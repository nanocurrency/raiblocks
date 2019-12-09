#include <nano/lib/logger_mt.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/lib/stats.hpp>
#include <nano/lib/threading.hpp>
#include <nano/lib/utility.hpp>
#include <nano/node/active_transactions.hpp>
#include <nano/node/confirmation_height_processor.hpp>
#include <nano/node/write_database_queue.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/ledger.hpp>

#include <boost/optional.hpp>

#include <cassert>
#include <numeric>

nano::confirmation_height_processor::confirmation_height_processor (nano::pending_confirmation_height & pending_confirmation_height_a, nano::ledger & ledger_a, nano::active_transactions & active_a, nano::write_database_queue & write_database_queue_a, std::chrono::milliseconds batch_separate_pending_min_time_a, nano::logger_mt & logger_a)
	: pending_confirmations (pending_confirmation_height_a)
	, ledger (ledger_a)
	, active (active_a)
	, logger (logger_a)
	, write_database_queue (write_database_queue_a)
	, batch_separate_pending_min_time (batch_separate_pending_min_time_a)
	, thread ([this]() {
		nano::thread_role::set (nano::thread_role::name::confirmation_height_processing);
		this->run ();
	})
{
}

nano::confirmation_height_processor::~confirmation_height_processor ()
{
	stop ();
}

void nano::confirmation_height_processor::stop ()
{
	stopped = true;
	condition.notify_one ();
	if (thread.joinable ())
	{
		thread.join ();
	}
}

void nano::confirmation_height_processor::run ()
{
	nano::unique_lock<std::mutex> lk (pending_confirmations.mutex);
	while (!stopped)
	{
		if (!paused && !pending_confirmations.pending.empty ())
		{
			pending_confirmations.current_hash = *pending_confirmations.pending.begin ();
			pending_confirmations.pending.erase (pending_confirmations.current_hash);
			// Copy the hash so can be used outside owning the lock
			auto current_pending_block = pending_confirmations.current_hash;
			lk.unlock ();
			if (pending_writes.empty ())
			{
				// Separate blocks which are pending confirmation height can be batched by a minimum processing time (to improve disk write performance), so make sure the slate is clean when a new batch is starting.
				confirmed_iterated_pairs.clear ();
				timer.restart ();
			}
			add_confirmation_height (current_pending_block);
			lk.lock ();
			pending_confirmations.current_hash = 0;
		}
		else
		{
			// If there are no blocks pending confirmation, then make sure we flush out the remaining writes
			if (!pending_writes.empty ())
			{
				lk.unlock ();
				auto scoped_write_guard = write_database_queue.wait (nano::writer::confirmation_height);
				write_pending (pending_writes);
				lk.lock ();
			}
			else
			{
				condition.wait (lk);
			}
		}
	}
}

void nano::confirmation_height_processor::pause ()
{
	paused = true;
}

void nano::confirmation_height_processor::unpause ()
{
	paused = false;
	condition.notify_one ();
}

void nano::confirmation_height_processor::add (nano::block_hash const & hash_a)
{
	{
		nano::lock_guard<std::mutex> lk (pending_confirmations.mutex);
		pending_confirmations.pending.insert (hash_a);
	}
	condition.notify_one ();
}

/**
 * For all the blocks below this height which have been implicitly confirmed check if they
 * are open/receive blocks, and if so follow the source blocks and iteratively repeat to genesis.
 * To limit write locking and to keep the confirmation height ledger correctly synced, confirmations are
 * written from the ground upwards in batches.
 */
void nano::confirmation_height_processor::add_confirmation_height (nano::block_hash const & hash_a)
{
	boost::optional<conf_height_details> receive_details;
	auto current = hash_a;
	assert (receive_source_pairs_size == 0);
	release_assert (receive_source_pairs.empty ());

	auto read_transaction (ledger.store.tx_begin_read ());
	auto last_iteration = false;
	// Traverse account chain and all sources for receive blocks iteratively
	do
	{
		if (!receive_source_pairs.empty ())
		{
			receive_details = receive_source_pairs.back ().receive_details;
			current = receive_source_pairs.back ().source_hash;
		}
		else
		{
			// If receive_details is set then this is the final iteration and we are back to the original chain.
			// We need to confirm any blocks below the original hash (incl self) and the first receive block
			// (if the original block is not already a receive)
			if (receive_details)
			{
				current = hash_a;
				receive_details = boost::none;
				last_iteration = true;
			}
		}

		auto block_height (ledger.store.block_account_height (read_transaction, current));
		nano::account account (ledger.store.block_account (read_transaction, current));
		uint64_t confirmation_height;
		release_assert (!ledger.store.confirmation_height_get (read_transaction, account, confirmation_height));
		auto iterated_height = confirmation_height;
		auto account_it = confirmed_iterated_pairs.find (account);
		if (account_it != confirmed_iterated_pairs.cend ())
		{
			if (account_it->second.confirmed_height > confirmation_height)
			{
				confirmation_height = account_it->second.confirmed_height;
				iterated_height = confirmation_height;
			}
			if (account_it->second.iterated_height > iterated_height)
			{
				iterated_height = account_it->second.iterated_height;
			}
		}

		if (!last_iteration && current == hash_a && confirmation_height >= block_height)
		{
			auto it = std::find_if (pending_writes.begin (), pending_writes.end (), [&hash_a](auto & conf_height_details) {
				auto it = std::find_if (conf_height_details.block_callbacks_required.begin (), conf_height_details.block_callbacks_required.end (), [&hash_a](auto & callback_data) {
					return callback_data.block->hash () == hash_a;
				});
				return (it != conf_height_details.block_callbacks_required.end ());
			});

			if (it == pending_writes.end ())
			{
				// This is a block which has been added to the processor but already has its confirmation height set (or about to be set)
				// Just need to perform active cleanup, no callbacks are needed.
				active.clear_block (hash_a);
			}
		}

		auto count_before_receive = receive_source_pairs.size ();
		std::vector<callback_data> block_callbacks_required;
		if (block_height > iterated_height)
		{
			if ((block_height - iterated_height) > 20000)
			{
				logger.always_log ("Iterating over a large account chain for setting confirmation height. The top block: ", current.to_string ());
			}

			collect_unconfirmed_receive_and_sources_for_account (block_height, iterated_height, current, account, read_transaction, block_callbacks_required);
		}

		// Exit early when the processor has been stopped, otherwise this function may take a
		// while (and hence keep the process running) if updating a long chain.
		if (stopped)
		{
			break;
		}

		// No longer need the read transaction
		read_transaction.reset ();

		// If this adds no more open or receive blocks, then we can now confirm this account as well as the linked open/receive block
		// Collect as pending any writes to the database and do them in bulk after a certain time.
		auto confirmed_receives_pending = (count_before_receive != receive_source_pairs.size ());
		if (!confirmed_receives_pending)
		{
			if (block_height > confirmation_height)
			{
				// Check whether the previous block has been seen. If so, the rest of sends below have already been seen so don't count them
				if (account_it != confirmed_iterated_pairs.cend ())
				{
					account_it->second.confirmed_height = block_height;
					if (block_height > iterated_height)
					{
						account_it->second.iterated_height = block_height;
					}
				}
				else
				{
					confirmed_iterated_pairs.emplace (account, confirmed_iterated_pair{ block_height, block_height });
				}

				pending_writes.emplace_back (account, current, block_height, block_height - confirmation_height, block_callbacks_required);
			}

			if (receive_details)
			{
				// Check whether the previous block has been seen. If so, the rest of sends below have already been seen so don't count them
				auto const & receive_account = receive_details->account;
				auto receive_account_it = confirmed_iterated_pairs.find (receive_account);
				if (receive_account_it != confirmed_iterated_pairs.cend ())
				{
					// Get current height
					auto current_height = receive_account_it->second.confirmed_height;
					receive_account_it->second.confirmed_height = receive_details->height;
					receive_details->num_blocks_confirmed = receive_details->height - current_height;
				}
				else
				{
					confirmed_iterated_pairs.emplace (receive_account, confirmed_iterated_pair{ receive_details->height, receive_details->height });
				}

				pending_writes.push_back (*receive_details);
			}

			if (!receive_source_pairs.empty ())
			{
				// Pop from the end
				receive_source_pairs.erase (receive_source_pairs.end () - 1);
				--receive_source_pairs_size;
			}
		}
		else if (block_height > iterated_height)
		{
			if (account_it != confirmed_iterated_pairs.cend ())
			{
				account_it->second.iterated_height = block_height;
			}
			else
			{
				confirmed_iterated_pairs.emplace (account, confirmed_iterated_pair{ confirmation_height, block_height });
			}
		}

		auto max_write_size_reached = (pending_writes.size () >= batch_write_size);
		// When there are a lot of pending confirmation height blocks, it is more efficient to
		// bulk some of them up to enable better write performance which becomes the bottleneck.
		auto min_time_exceeded = (timer.since_start () >= batch_separate_pending_min_time);
		auto finished_iterating = receive_source_pairs.empty ();
		auto no_pending = pending_confirmations.size () == 0;
		auto should_output = finished_iterating && (no_pending || min_time_exceeded);

		if ((max_write_size_reached || should_output) && !pending_writes.empty ())
		{
			if (write_database_queue.process (nano::writer::confirmation_height))
			{
				auto scoped_write_guard = write_database_queue.pop ();
				auto error = write_pending (pending_writes);
				// Don't set any more blocks as confirmed from the original hash if an inconsistency is found
				if (error)
				{
					break;
				}
			}
		}

		read_transaction.renew ();
	} while (!receive_source_pairs.empty () || current != hash_a);
}

/*
 * Returns true if there was an error in finding one of the blocks to write a confirmation height for, false otherwise
 */
bool nano::confirmation_height_processor::write_pending (std::deque<conf_height_details> & all_pending_a)
{
	auto total_pending_write_block_count = std::accumulate (all_pending_a.cbegin (), all_pending_a.cend (), uint64_t (0), [](uint64_t total, conf_height_details const & conf_height_details_a) {
		return total += conf_height_details_a.num_blocks_confirmed;
	});

	// Write in batches
	while (total_pending_write_block_count > 0)
	{
		uint64_t num_accounts_processed = 0;
		auto transaction (ledger.store.tx_begin_write ({}, { nano::tables::confirmation_height }));
		while (!all_pending_a.empty ())
		{
			const auto & pending = all_pending_a.front ();
			uint64_t confirmation_height;
			auto error = ledger.store.confirmation_height_get (transaction, pending.account, confirmation_height);
			release_assert (!error);
			if (pending.height > confirmation_height)
			{
#ifndef NDEBUG
				// Do more thorough checking in Debug mode, indicates programming error.
				nano::block_sideband sideband;
				auto block = ledger.store.block_get (transaction, pending.hash, &sideband);
				static nano::network_constants network_constants;
				assert (network_constants.is_test_network () || block != nullptr);
				assert (network_constants.is_test_network () || sideband.height == pending.height);
#else
				auto block = ledger.store.block_get (transaction, pending.hash);
#endif
				// Check that the block still exists as there may have been changes outside this processor.
				if (!block)
				{
					logger.always_log ("Failed to write confirmation height for: ", pending.hash.to_string ());
					ledger.stats.inc (nano::stat::type::confirmation_height, nano::stat::detail::invalid_block);
					receive_source_pairs.clear ();
					receive_source_pairs_size = 0;
					all_pending_a.clear ();
					return true;
				}

				for (auto & callback_data : pending.block_callbacks_required)
				{
					active.post_confirmation_height_set (transaction, callback_data.block, callback_data.sideband, callback_data.election_status_type);
				}

				ledger.stats.add (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in, pending.height - confirmation_height);
				assert (pending.num_blocks_confirmed == pending.height - confirmation_height);
				confirmation_height = pending.height;
				ledger.cemented_count += pending.num_blocks_confirmed;
				ledger.store.confirmation_height_put (transaction, pending.account, confirmation_height);
			}
			total_pending_write_block_count -= pending.num_blocks_confirmed;
			++num_accounts_processed;
			all_pending_a.erase (all_pending_a.begin ());

			if (num_accounts_processed >= batch_write_size)
			{
				// Commit changes periodically to reduce time holding write locks for long chains
				break;
			}
		}
	}
	assert (all_pending_a.empty ());
	return false;
}

void nano::confirmation_height_processor::collect_unconfirmed_receive_and_sources_for_account (uint64_t block_height_a, uint64_t confirmation_height_a, nano::block_hash const & hash_a, nano::account const & account_a, nano::read_transaction const & transaction_a, std::vector<callback_data> & block_callbacks_required)
{
	auto hash (hash_a);
	auto num_to_confirm = block_height_a - confirmation_height_a;

	// Store heights of blocks
	constexpr auto height_not_set = std::numeric_limits<uint64_t>::max ();
	auto next_height = height_not_set;
	while ((num_to_confirm > 0) && !hash.is_zero () && !stopped)
	{
		nano::block_sideband sideband;
		auto block (ledger.store.block_get (transaction_a, hash, &sideband));
		if (block)
		{
			if (!pending_confirmations.is_processing_block (hash))
			{
				auto election_status_type = active.confirm_block (transaction_a, block);
				if (election_status_type.is_initialized ())
				{
					block_callbacks_required.emplace_back (block, sideband, *election_status_type);
				}
			}
			else
			{
				// This block is the original which is having its confirmation height set on
				block_callbacks_required.emplace_back (block, sideband, nano::election_status_type::active_confirmed_quorum);
			}

			auto source (block->source ());
			if (source.is_zero ())
			{
				source = block->link ();
			}

			if (!source.is_zero () && !ledger.is_epoch_link (source) && ledger.store.source_exists (transaction_a, source))
			{
				auto block_height = confirmation_height_a + num_to_confirm;
				// Set the height for the receive block above (if there is one)
				if (next_height != height_not_set)
				{
					receive_source_pairs.back ().receive_details.num_blocks_confirmed = next_height - block_height;

					auto & receive_callbacks_required = receive_source_pairs.back ().receive_details.block_callbacks_required;

					// Don't include the last one as that belongs to the next recieve
					std::copy (block_callbacks_required.begin (), block_callbacks_required.end () - 1, std::back_inserter (receive_callbacks_required));
					block_callbacks_required = { block_callbacks_required.back () };
				}

				receive_source_pairs.emplace_back (conf_height_details{ account_a, hash, block_height, height_not_set, {} }, source);
				++receive_source_pairs_size;
				next_height = block_height;
			}

			hash = block->previous ();
		}

		// We could be traversing a very large account so we don't want to open read transactions for too long.
		if (num_to_confirm % batch_read_size == 0)
		{
			transaction_a.refresh ();
		}

		--num_to_confirm;
	}

	// Update the number of blocks confirmed by the last receive block
	if (!receive_source_pairs.empty ())
	{
		auto & last_receive_details = receive_source_pairs.back ().receive_details;
		last_receive_details.num_blocks_confirmed = last_receive_details.height - confirmation_height_a;
		last_receive_details.block_callbacks_required = block_callbacks_required;
	}
}

namespace nano
{
confirmation_height_processor::conf_height_details::conf_height_details (nano::account const & account_a, nano::block_hash const & hash_a, uint64_t height_a, uint64_t num_blocks_confirmed_a, std::vector<callback_data> const & block_callbacks_required_a)
	: account (account_a)
	, hash (hash_a)
	, height (height_a)
	, num_blocks_confirmed (num_blocks_confirmed_a)
	, block_callbacks_required (block_callbacks_required_a)
{
}

confirmation_height_processor::receive_source_pair::receive_source_pair (confirmation_height_processor::conf_height_details const & receive_details_a, const block_hash & source_a)
	: receive_details (receive_details_a)
	, source_hash (source_a)
{
}

confirmation_height_processor::confirmed_iterated_pair::confirmed_iterated_pair (uint64_t confirmed_height_a, uint64_t iterated_height_a)
	: confirmed_height (confirmed_height_a)
	, iterated_height (iterated_height_a)
{
}

confirmation_height_processor::callback_data::callback_data (std::shared_ptr<nano::block> const & block_a, nano::block_sideband const & sideband_a, nano::election_status_type election_status_type_a)
	: block (block_a)
	, sideband (sideband_a)
	, election_status_type (election_status_type_a)
{
}

std::unique_ptr<seq_con_info_component> collect_seq_con_info (confirmation_height_processor & confirmation_height_processor_a, const std::string & name_a)
{
	size_t receive_source_pairs_count = confirmation_height_processor_a.receive_source_pairs_size;
	auto composite = std::make_unique<seq_con_info_composite> (name_a);
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "receive_source_pairs", receive_source_pairs_count, sizeof (decltype (confirmation_height_processor_a.receive_source_pairs)::value_type) }));
	return composite;
}
}

size_t nano::pending_confirmation_height::size ()
{
	nano::lock_guard<std::mutex> lk (mutex);
	return pending.size ();
}

bool nano::pending_confirmation_height::is_processing_block (nano::block_hash const & hash_a)
{
	// First check the hash currently being processed
	nano::lock_guard<std::mutex> lk (mutex);
	if (!current_hash.is_zero () && current_hash == hash_a)
	{
		return true;
	}

	// Check remaining pending confirmations
	return pending.find (hash_a) != pending.cend ();
}

nano::block_hash nano::pending_confirmation_height::current ()
{
	nano::lock_guard<std::mutex> lk (mutex);
	return current_hash;
}

namespace nano
{
std::unique_ptr<seq_con_info_component> collect_seq_con_info (pending_confirmation_height & pending_confirmation_height_a, const std::string & name_a)
{
	size_t pending_count = pending_confirmation_height_a.size ();
	auto composite = std::make_unique<seq_con_info_composite> (name_a);
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "pending", pending_count, sizeof (nano::block_hash) }));
	return composite;
}
}
