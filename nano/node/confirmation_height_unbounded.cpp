#include <nano/lib/stats.hpp>
#include <nano/node/confirmation_height_unbounded.hpp>
#include <nano/node/write_database_queue.hpp>
#include <nano/secure/ledger.hpp>

#include <boost/format.hpp>

#include <numeric>

nano::confirmation_height_unbounded::confirmation_height_unbounded (nano::ledger & ledger_a, nano::write_database_queue & write_database_queue_a, std::chrono::milliseconds batch_separate_pending_min_time_a, nano::logger_mt & logger_a, std::atomic<bool> & stopped_a, nano::block_hash const & original_hash_a, uint64_t & batch_write_size_a, std::function<void(std::vector<std::shared_ptr<nano::block>> const &)> const & notify_observers_callback_a, std::function<void(nano::block_hash const &)> const & notify_block_already_cemented_observers_callback_a, std::function<uint64_t ()> const & awaiting_processing_size_callback_a) :
ledger (ledger_a),
write_database_queue (write_database_queue_a),
batch_separate_pending_min_time (batch_separate_pending_min_time_a),
logger (logger_a),
stopped (stopped_a),
original_hash (original_hash_a),
batch_write_size (batch_write_size_a),
notify_observers_callback (notify_observers_callback_a),
notify_block_already_cemented_observers_callback (notify_block_already_cemented_observers_callback_a),
awaiting_processing_size_callback (awaiting_processing_size_callback_a)
{
}

void nano::confirmation_height_unbounded::process ()
{
	if (pending_empty ())
	{
		clear_process_vars ();
		timer.restart ();
	}
	std::shared_ptr<conf_height_details> receive_details;
	auto current = original_hash;
	std::vector<nano::block_hash> orig_block_callback_data;

	std::vector<receive_source_pair> receive_source_pairs;
	release_assert (receive_source_pairs.empty ());

	bool first_iter = true;
	auto read_transaction (ledger.store.tx_begin_read ());

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
				current = original_hash;
				receive_details = nullptr;
			}
		}

		auto block (get_block_and_sideband (current, read_transaction));
		if (!block)
		{
			auto error_str = (boost::format ("Ledger mismatch trying to set confirmation height for block %1% (unbounded processor)") % current.to_string ()).str ();
			logger.always_log (error_str);
			std::cerr << error_str << std::endl;
		}
		release_assert (block);

		nano::account account (block->account ());
		if (account.is_zero ())
		{
			account = block->sideband ().account;
		}

		auto block_height = block->sideband ().height;
		uint64_t confirmation_height = 0;
		auto account_it = confirmed_iterated_pairs.find (account);
		if (account_it != confirmed_iterated_pairs.cend ())
		{
			confirmation_height = account_it->second.confirmed_height;
		}
		else
		{
			nano::confirmation_height_info confirmation_height_info;
			release_assert (!ledger.store.confirmation_height_get (read_transaction, account, confirmation_height_info));
			confirmation_height = confirmation_height_info.height;

			// This block was added to the confirmation height processor but is already confirmed
			if (first_iter && confirmation_height >= block_height && current == original_hash)
			{
				notify_block_already_cemented_observers_callback (original_hash);
			}
		}
		auto iterated_height = confirmation_height;
		if (account_it != confirmed_iterated_pairs.cend () && account_it->second.iterated_height > iterated_height)
		{
			iterated_height = account_it->second.iterated_height;
		}

		auto count_before_receive = receive_source_pairs.size ();
		std::vector<nano::block_hash> block_callback_datas_required;
		auto already_traversed = iterated_height >= block_height;
		if (!already_traversed)
		{
			collect_unconfirmed_receive_and_sources_for_account (block_height, iterated_height, current, account, read_transaction, receive_source_pairs, block_callback_datas_required, orig_block_callback_data);
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
			preparation_data preparation_data{ block_height, confirmation_height, iterated_height, account_it, account, receive_details, already_traversed, current, block_callback_datas_required, orig_block_callback_data };
			prepare_iterated_blocks_for_cementing (preparation_data);

			if (!receive_source_pairs.empty ())
			{
				// Pop from the end
				receive_source_pairs.erase (receive_source_pairs.end () - 1);
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
				confirmed_iterated_pairs.emplace (std::piecewise_construct, std::forward_as_tuple (account), std::forward_as_tuple (confirmation_height, block_height));
				++confirmed_iterated_pairs_size;
			}
		}

		auto max_write_size_reached = (pending_writes.size () >= confirmation_height::unbounded_cutoff);
		// When there are a lot of pending confirmation height blocks, it is more efficient to
		// bulk some of them up to enable better write performance which becomes the bottleneck.
		auto min_time_exceeded = (timer.since_start () >= batch_separate_pending_min_time);
		auto finished_iterating = receive_source_pairs.empty ();
		auto no_pending = awaiting_processing_size_callback () == 0;
		auto should_output = finished_iterating && (no_pending || min_time_exceeded);

		auto total_pending_write_block_count = std::accumulate (pending_writes.cbegin (), pending_writes.cend (), uint64_t (0), [](uint64_t total, conf_height_details const & receive_details_a) {
			return total += receive_details_a.num_blocks_confirmed;
		});
		auto force_write = total_pending_write_block_count > batch_write_size;

		if ((max_write_size_reached || should_output || force_write) && !pending_writes.empty ())
		{
			if (write_database_queue.process (nano::writer::confirmation_height))
			{
				auto scoped_write_guard = write_database_queue.pop ();
				cement_blocks (scoped_write_guard);
			}
			else if (force_write)
			{
				// Unbounded processor has grown too large, force a write
				auto scoped_write_guard = write_database_queue.wait (nano::writer::confirmation_height);
				cement_blocks (scoped_write_guard);
			}
		}

		first_iter = false;
		read_transaction.renew ();
	} while ((!receive_source_pairs.empty () || current != original_hash) && !stopped);
}

void nano::confirmation_height_unbounded::collect_unconfirmed_receive_and_sources_for_account (uint64_t block_height_a, uint64_t confirmation_height_a, nano::block_hash const & hash_a, nano::account const & account_a, nano::read_transaction const & transaction_a, std::vector<receive_source_pair> & receive_source_pairs_a, std::vector<nano::block_hash> & block_callback_data_a, std::vector<nano::block_hash> & orig_block_callback_data_a)
{
	auto hash (hash_a);
	auto num_to_confirm = block_height_a - confirmation_height_a;

	// Handle any sends above a receive
	auto is_original_block = (hash == original_hash);
	bool hit_receive = false;
	while ((num_to_confirm > 0) && !hash.is_zero () && !stopped)
	{
		auto block (get_block_and_sideband (hash, transaction_a));
		if (block)
		{
			auto source (block->source ());
			if (source.is_zero ())
			{
				source = block->link ();
			}

			if (!source.is_zero () && !ledger.is_epoch_link (source) && ledger.store.block_exists (transaction_a, source))
			{
				if (!hit_receive && !block_callback_data_a.empty ())
				{
					// Add the callbacks to the associated receive to retrieve later
					debug_assert (!receive_source_pairs_a.empty ());
					auto & last_receive_details = receive_source_pairs_a.back ().receive_details;
					last_receive_details->source_block_callback_data.assign (block_callback_data_a.begin (), block_callback_data_a.end ());
					block_callback_data_a.clear ();
				}

				is_original_block = false;
				hit_receive = true;

				auto block_height = confirmation_height_a + num_to_confirm;
				receive_source_pairs_a.emplace_back (std::make_shared<conf_height_details> (account_a, hash, block_height, 1, std::vector<nano::block_hash>{ hash }), source);
			}
			else if (is_original_block)
			{
				orig_block_callback_data_a.push_back (hash);
			}
			else
			{
				if (!hit_receive)
				{
					// This block is cemented via a recieve, as opposed to below a receive being cemented
					block_callback_data_a.push_back (hash);
				}
				else
				{
					// We have hit a receive before, add the block to it
					auto & last_receive_details = receive_source_pairs_a.back ().receive_details;
					++last_receive_details->num_blocks_confirmed;
					last_receive_details->block_callback_data.push_back (hash);

					implicit_receive_cemented_mapping[hash] = std::weak_ptr<conf_height_details> (last_receive_details);
					implicit_receive_cemented_mapping_size = implicit_receive_cemented_mapping.size ();
				}
			}

			hash = block->previous ();
		}

		--num_to_confirm;
	}
}

void nano::confirmation_height_unbounded::prepare_iterated_blocks_for_cementing (preparation_data & preparation_data_a)
{
	auto receive_details = preparation_data_a.receive_details;
	auto block_height = preparation_data_a.block_height;
	if (block_height > preparation_data_a.confirmation_height)
	{
		// Check whether the previous block has been seen. If so, the rest of sends below have already been seen so don't count them
		if (preparation_data_a.account_it != confirmed_iterated_pairs.cend ())
		{
			preparation_data_a.account_it->second.confirmed_height = block_height;
			if (block_height > preparation_data_a.iterated_height)
			{
				preparation_data_a.account_it->second.iterated_height = block_height;
			}
		}
		else
		{
			confirmed_iterated_pairs.emplace (std::piecewise_construct, std::forward_as_tuple (preparation_data_a.account), std::forward_as_tuple (block_height, block_height));
			++confirmed_iterated_pairs_size;
		}

		auto num_blocks_confirmed = block_height - preparation_data_a.confirmation_height;
		auto block_callback_data = preparation_data_a.block_callback_data;
		if (block_callback_data.empty ())
		{
			if (!receive_details)
			{
				block_callback_data = preparation_data_a.orig_block_callback_data;
			}
			else
			{
				debug_assert (receive_details);

				if (preparation_data_a.already_traversed && receive_details->source_block_callback_data.empty ())
				{
					// We are confirming a block which has already been traversed and found no associated receive details for it.
					auto & above_receive_details_w = implicit_receive_cemented_mapping[preparation_data_a.current];
					debug_assert (!above_receive_details_w.expired ());
					auto above_receive_details = above_receive_details_w.lock ();

					auto num_blocks_already_confirmed = above_receive_details->num_blocks_confirmed - (above_receive_details->height - preparation_data_a.confirmation_height);

					auto end_it = above_receive_details->block_callback_data.begin () + above_receive_details->block_callback_data.size () - (num_blocks_already_confirmed);
					auto start_it = end_it - num_blocks_confirmed;

					block_callback_data.assign (start_it, end_it);
				}
				else
				{
					block_callback_data = receive_details->source_block_callback_data;
				}

				auto num_to_remove = block_callback_data.size () - num_blocks_confirmed;
				block_callback_data.erase (std::next (block_callback_data.rbegin (), num_to_remove).base (), block_callback_data.end ());
				receive_details->source_block_callback_data.clear ();
			}
		}

		pending_writes.emplace_back (preparation_data_a.account, preparation_data_a.current, block_height, num_blocks_confirmed, block_callback_data);
		++pending_writes_size;
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
			auto const orig_num_blocks_confirmed = receive_details->num_blocks_confirmed;
			receive_details->num_blocks_confirmed = receive_details->height - current_height;

			// Get the difference and remove the callbacks
			auto block_callbacks_to_remove = orig_num_blocks_confirmed - receive_details->num_blocks_confirmed;
			receive_details->block_callback_data.erase (std::next (receive_details->block_callback_data.rbegin (), block_callbacks_to_remove).base (), receive_details->block_callback_data.end ());
			debug_assert (receive_details->block_callback_data.size () == receive_details->num_blocks_confirmed);
		}
		else
		{
			confirmed_iterated_pairs.emplace (std::piecewise_construct, std::forward_as_tuple (receive_account), std::forward_as_tuple (receive_details->height, receive_details->height));
			++confirmed_iterated_pairs_size;
		}

		pending_writes.push_back (*receive_details);
		++pending_writes_size;
	}
}

/*
 * Returns true if there was an error in finding one of the blocks to write a confirmation height for, false otherwise
 */
void nano::confirmation_height_unbounded::cement_blocks (nano::write_guard & scoped_write_guard_a)
{
	nano::timer<std::chrono::milliseconds> cemented_batch_timer;
	std::vector<std::shared_ptr<nano::block>> cemented_blocks;
	auto error = false;
	{
		auto transaction (ledger.store.tx_begin_write ({}, { nano::tables::confirmation_height }));
		cemented_batch_timer.start ();
		while (!pending_writes.empty ())
		{
			auto & pending = pending_writes.front ();
			nano::confirmation_height_info confirmation_height_info;
			error = ledger.store.confirmation_height_get (transaction, pending.account, confirmation_height_info);
			if (error)
			{
				auto error_str = (boost::format ("Failed to read confirmation height for account %1% when writing block %2% (unbounded processor)") % pending.account.to_account () % pending.hash.to_string ()).str ();
				logger.always_log (error_str);
				std::cerr << error_str << std::endl;
			}
			auto confirmation_height = confirmation_height_info.height;
			if (!error && pending.height > confirmation_height)
			{
				auto block = ledger.store.block_get (transaction, pending.hash);
				debug_assert (constants.network.is_dev_network () || block != nullptr);
				debug_assert (constants.network.is_dev_network () || block->sideband ().height == pending.height);

				if (!block)
				{
					auto error_str = (boost::format ("Failed to write confirmation height for block %1% (unbounded processor)") % pending.hash.to_string ()).str ();
					logger.always_log (error_str);
					std::cerr << error_str << std::endl;
					error = true;
					break;
				}
				ledger.stats.add (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in, pending.height - confirmation_height);
				ledger.stats.add (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_unbounded, nano::stat::dir::in, pending.height - confirmation_height);
				debug_assert (pending.num_blocks_confirmed == pending.height - confirmation_height);
				confirmation_height = pending.height;
				ledger.cache.cemented_count += pending.num_blocks_confirmed;
				ledger.store.confirmation_height_put (transaction, pending.account, { confirmation_height, pending.hash });

				// Reverse it so that the callbacks start from the lowest newly cemented block and move upwards
				std::reverse (pending.block_callback_data.begin (), pending.block_callback_data.end ());

				nano::lock_guard<std::mutex> guard (block_cache_mutex);
				std::transform (pending.block_callback_data.begin (), pending.block_callback_data.end (), std::back_inserter (cemented_blocks), [& block_cache = block_cache](auto const & hash_a) {
					debug_assert (block_cache.count (hash_a) == 1);
					return block_cache.at (hash_a);
				});
			}
			pending_writes.erase (pending_writes.begin ());
			--pending_writes_size;
		}
	}

	auto time_spent_cementing = cemented_batch_timer.since_start ().count ();
	if (time_spent_cementing > 50)
	{
		logger.always_log (boost::str (boost::format ("Cemented %1% blocks in %2% %3% (unbounded processor)") % cemented_blocks.size () % time_spent_cementing % cemented_batch_timer.unit ()));
	}

	scoped_write_guard_a.release ();
	notify_observers_callback (cemented_blocks);
	release_assert (!error);

	// Tests should check this already at the end, but not all blocks may have elections (e.g from manual calls to confirmation_height_processor::add), this should catch any inconsistencies on live/beta though
	if (!constants.network.is_dev_network ())
	{
		auto blocks_confirmed_stats = ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed);
		auto observer_stats = ledger.stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::all, nano::stat::dir::out);
		debug_assert (blocks_confirmed_stats == observer_stats);
	}
	debug_assert (pending_writes.empty ());
	debug_assert (pending_writes_size == 0);
	timer.restart ();
}

std::shared_ptr<nano::block> nano::confirmation_height_unbounded::get_block_and_sideband (nano::block_hash const & hash_a, nano::transaction const & transaction_a)
{
	nano::lock_guard<std::mutex> guard (block_cache_mutex);
	auto block_cache_it = block_cache.find (hash_a);
	if (block_cache_it != block_cache.cend ())
	{
		return block_cache_it->second;
	}
	else
	{
		auto block (ledger.store.block_get (transaction_a, hash_a));
		block_cache.emplace (hash_a, block);
		return block;
	}
}

bool nano::confirmation_height_unbounded::pending_empty () const
{
	return pending_writes.empty ();
}

void nano::confirmation_height_unbounded::clear_process_vars ()
{
	// Separate blocks which are pending confirmation height can be batched by a minimum processing time (to improve lmdb disk write performance),
	// so make sure the slate is clean when a new batch is starting.
	confirmed_iterated_pairs.clear ();
	confirmed_iterated_pairs_size = 0;
	implicit_receive_cemented_mapping.clear ();
	implicit_receive_cemented_mapping_size = 0;
	{
		nano::lock_guard<std::mutex> guard (block_cache_mutex);
		block_cache.clear ();
	}
}

bool nano::confirmation_height_unbounded::has_iterated_over_block (nano::block_hash const & hash_a) const
{
	nano::lock_guard<std::mutex> guard (block_cache_mutex);
	return block_cache.count (hash_a) == 1;
}

uint64_t nano::confirmation_height_unbounded::block_cache_size () const
{
	nano::lock_guard<std::mutex> guard (block_cache_mutex);
	return block_cache.size ();
}

nano::confirmation_height_unbounded::conf_height_details::conf_height_details (nano::account const & account_a, nano::block_hash const & hash_a, uint64_t height_a, uint64_t num_blocks_confirmed_a, std::vector<nano::block_hash> const & block_callback_data_a) :
account (account_a),
hash (hash_a),
height (height_a),
num_blocks_confirmed (num_blocks_confirmed_a),
block_callback_data (block_callback_data_a)
{
}

nano::confirmation_height_unbounded::receive_source_pair::receive_source_pair (std::shared_ptr<conf_height_details> const & receive_details_a, const block_hash & source_a) :
receive_details (receive_details_a),
source_hash (source_a)
{
}

nano::confirmation_height_unbounded::confirmed_iterated_pair::confirmed_iterated_pair (uint64_t confirmed_height_a, uint64_t iterated_height_a) :
confirmed_height (confirmed_height_a),
iterated_height (iterated_height_a)
{
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (confirmation_height_unbounded & confirmation_height_unbounded, const std::string & name_a)
{
	auto composite = std::make_unique<container_info_composite> (name_a);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "confirmed_iterated_pairs", confirmation_height_unbounded.confirmed_iterated_pairs_size, sizeof (decltype (confirmation_height_unbounded.confirmed_iterated_pairs)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "pending_writes", confirmation_height_unbounded.pending_writes_size, sizeof (decltype (confirmation_height_unbounded.pending_writes)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "implicit_receive_cemented_mapping", confirmation_height_unbounded.implicit_receive_cemented_mapping_size, sizeof (decltype (confirmation_height_unbounded.implicit_receive_cemented_mapping)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "block_cache", confirmation_height_unbounded.block_cache_size (), sizeof (decltype (confirmation_height_unbounded.block_cache)::value_type) }));
	return composite;
}
