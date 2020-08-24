#include <nano/lib/logger_mt.hpp>
#include <nano/lib/stats.hpp>
#include <nano/node/confirmation_height_bounded.hpp>
#include <nano/node/write_database_queue.hpp>
#include <nano/secure/ledger.hpp>

#include <boost/format.hpp>
#include <boost/optional.hpp>

#include <numeric>

nano::confirmation_height_bounded::confirmation_height_bounded (nano::ledger & ledger_a, nano::write_database_queue & write_database_queue_a, std::chrono::milliseconds batch_separate_pending_min_time_a, nano::logger_mt & logger_a, std::atomic<bool> & stopped_a, nano::block_hash const & original_hash_a, uint64_t & batch_write_size_a, std::function<void(std::vector<std::shared_ptr<nano::block>> const &)> const & notify_observers_callback_a, std::function<void(nano::block_hash const &)> const & notify_block_already_cemented_observers_callback_a, std::function<uint64_t ()> const & awaiting_processing_size_callback_a) :
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

// The next block hash to iterate over, the priority is as follows:
// 1 - The next block in the account chain for the last processed receive (if there is any)
// 2 - The next receive block which is closest to genesis
// 3 - The last checkpoint hit.
// 4 - The hash that was passed in originally. Either all checkpoints were exhausted (this can happen when there are many accounts to genesis)
//     or all other blocks have been processed.
nano::confirmation_height_bounded::top_and_next_hash nano::confirmation_height_bounded::get_next_block (boost::optional<top_and_next_hash> const & next_in_receive_chain_a, boost::circular_buffer_space_optimized<nano::block_hash> const & checkpoints_a, boost::circular_buffer_space_optimized<receive_source_pair> const & receive_source_pairs, boost::optional<receive_chain_details> & receive_details_a)
{
	top_and_next_hash next;
	if (next_in_receive_chain_a.is_initialized ())
	{
		next = *next_in_receive_chain_a;
	}
	else if (!receive_source_pairs.empty ())
	{
		auto next_receive_source_pair = receive_source_pairs.back ();
		receive_details_a = next_receive_source_pair.receive_details;
		next = { next_receive_source_pair.source_hash, receive_details_a->next, receive_details_a->height + 1 };
	}
	else if (!checkpoints_a.empty ())
	{
		next = { checkpoints_a.back (), boost::none, 0 };
	}
	else
	{
		next = { original_hash, boost::none, 0 };
	}

	return next;
}

void nano::confirmation_height_bounded::process ()
{
	if (pending_empty ())
	{
		clear_process_vars ();
		timer.restart ();
	}

	boost::optional<top_and_next_hash> next_in_receive_chain;
	boost::circular_buffer_space_optimized<nano::block_hash> checkpoints{ max_items };
	boost::circular_buffer_space_optimized<receive_source_pair> receive_source_pairs{ max_items };
	nano::block_hash current;
	bool first_iter = true;
	auto transaction (ledger.store.tx_begin_read ());
	do
	{
		boost::optional<receive_chain_details> receive_details;
		auto hash_to_process = get_next_block (next_in_receive_chain, checkpoints, receive_source_pairs, receive_details);
		current = hash_to_process.top;

		auto top_level_hash = current;
		auto block = ledger.store.block_get (transaction, current);
		if (!block)
		{
			auto error_str = (boost::format ("Ledger mismatch trying to set confirmation height for block %1% (bounded processor)") % current.to_string ()).str ();
			logger.always_log (error_str);
			std::cerr << error_str << std::endl;
		}
		release_assert (block);
		nano::account account (block->account ());
		if (account.is_zero ())
		{
			account = block->sideband ().account;
		}

		// Checks if we have encountered this account before but not commited changes yet, if so then update the cached confirmation height
		nano::confirmation_height_info confirmation_height_info;
		auto account_it = accounts_confirmed_info.find (account);
		if (account_it != accounts_confirmed_info.cend ())
		{
			confirmation_height_info.height = account_it->second.confirmed_height;
			confirmation_height_info.frontier = account_it->second.iterated_frontier;
		}
		else
		{
			auto error = ledger.store.confirmation_height_get (transaction, account, confirmation_height_info);
			(void)error;
			debug_assert (!error);
			// This block was added to the confirmation height processor but is already confirmed
			if (first_iter && confirmation_height_info.height >= block->sideband ().height && current == original_hash)
			{
				notify_block_already_cemented_observers_callback (original_hash);
			}
		}

		auto block_height = block->sideband ().height;
		bool already_cemented = confirmation_height_info.height >= block_height;

		// If we are not already at the bottom of the account chain (1 above cemented frontier) then find it
		if (!already_cemented && block_height - confirmation_height_info.height > 1)
		{
			if (block_height - confirmation_height_info.height == 2)
			{
				// If there is 1 uncemented block in-between this block and the cemented frontier,
				// we can just use the previous block to get the least unconfirmed hash.
				current = block->previous ();
				--block_height;
			}
			else if (!next_in_receive_chain.is_initialized ())
			{
				current = get_least_unconfirmed_hash_from_top_level (transaction, current, account, confirmation_height_info, block_height);
			}
			else
			{
				// Use the cached successor of the last receive which saves having to do more IO in get_least_unconfirmed_hash_from_top_level
				// as we already know what the next block we should process should be.
				current = *hash_to_process.next;
				block_height = hash_to_process.next_height;
			}
		}

		auto top_most_non_receive_block_hash = current;

		bool hit_receive = false;
		if (!already_cemented)
		{
			hit_receive = iterate (transaction, block_height, current, checkpoints, top_most_non_receive_block_hash, top_level_hash, receive_source_pairs, account);
		}

		// Exit early when the processor has been stopped, otherwise this function may take a
		// while (and hence keep the process running) if updating a long chain.
		if (stopped)
		{
			break;
		}

		// next_in_receive_chain can be modified when writing, so need to cache it here before resetting
		auto is_set = next_in_receive_chain.is_initialized ();
		next_in_receive_chain = boost::none;

		// Need to also handle the case where we are hitting receives where the sends below should be confirmed
		if (!hit_receive || (receive_source_pairs.size () == 1 && top_most_non_receive_block_hash != current))
		{
			preparation_data preparation_data{ transaction, top_most_non_receive_block_hash, already_cemented, checkpoints, account_it, confirmation_height_info, account, block_height, current, receive_details, next_in_receive_chain };
			prepare_iterated_blocks_for_cementing (preparation_data);

			// If used the top level, don't pop off the receive source pair because it wasn't used
			if (!is_set && !receive_source_pairs.empty ())
			{
				receive_source_pairs.pop_back ();
			}

			auto total_pending_write_block_count = std::accumulate (pending_writes.cbegin (), pending_writes.cend (), uint64_t (0), [](uint64_t total, auto const & write_details_a) {
				return total += write_details_a.top_height - write_details_a.bottom_height + 1;
			});

			auto max_batch_write_size_reached = (total_pending_write_block_count >= batch_write_size);
			// When there are a lot of pending confirmation height blocks, it is more efficient to
			// bulk some of them up to enable better write performance which becomes the bottleneck.
			auto min_time_exceeded = (timer.since_start () >= batch_separate_pending_min_time);
			auto finished_iterating = current == original_hash;
			auto non_awaiting_processing = awaiting_processing_size_callback () == 0;
			auto should_output = finished_iterating && (non_awaiting_processing || min_time_exceeded);
			auto force_write = pending_writes.size () >= pending_writes_max_size || accounts_confirmed_info.size () >= pending_writes_max_size;

			if ((max_batch_write_size_reached || should_output || force_write) && !pending_writes.empty ())
			{
				// If nothing is currently using the database write lock then write the cemented pending blocks otherwise continue iterating
				if (write_database_queue.process (nano::writer::confirmation_height))
				{
					auto scoped_write_guard = write_database_queue.pop ();
					cement_blocks (scoped_write_guard);
				}
				else if (force_write)
				{
					auto scoped_write_guard = write_database_queue.wait (nano::writer::confirmation_height);
					cement_blocks (scoped_write_guard);
				}
			}
		}

		first_iter = false;
		transaction.refresh ();
	} while ((!receive_source_pairs.empty () || current != original_hash) && !stopped);

	debug_assert (checkpoints.empty ());
}

nano::block_hash nano::confirmation_height_bounded::get_least_unconfirmed_hash_from_top_level (nano::transaction const & transaction_a, nano::block_hash const & hash_a, nano::account const & account_a, nano::confirmation_height_info const & confirmation_height_info_a, uint64_t & block_height_a)
{
	nano::block_hash least_unconfirmed_hash = hash_a;
	if (confirmation_height_info_a.height != 0)
	{
		if (block_height_a > confirmation_height_info_a.height)
		{
			auto block (ledger.store.block_get (transaction_a, confirmation_height_info_a.frontier));
			release_assert (block != nullptr);
			least_unconfirmed_hash = block->sideband ().successor;
			block_height_a = block->sideband ().height + 1;
		}
	}
	else
	{
		// No blocks have been confirmed, so the first block will be the open block
		nano::account_info account_info;
		release_assert (!ledger.store.account_get (transaction_a, account_a, account_info));
		least_unconfirmed_hash = account_info.open_block;
		block_height_a = 1;
	}
	return least_unconfirmed_hash;
}

bool nano::confirmation_height_bounded::iterate (nano::read_transaction const & transaction_a, uint64_t bottom_height_a, nano::block_hash const & bottom_hash_a, boost::circular_buffer_space_optimized<nano::block_hash> & checkpoints_a, nano::block_hash & top_most_non_receive_block_hash_a, nano::block_hash const & top_level_hash_a, boost::circular_buffer_space_optimized<receive_source_pair> & receive_source_pairs_a, nano::account const & account_a)
{
	bool reached_target = false;
	bool hit_receive = false;
	auto hash = bottom_hash_a;
	uint64_t num_blocks = 0;
	while (!hash.is_zero () && !reached_target && !stopped)
	{
		// Keep iterating upwards until we either reach the desired block or the second receive.
		// Once a receive is cemented, we can cement all blocks above it until the next receive, so store those details for later.
		++num_blocks;
		auto block = ledger.store.block_get (transaction_a, hash);
		auto source (block->source ());
		if (source.is_zero ())
		{
			source = block->link ();
		}

		if (!source.is_zero () && !ledger.is_epoch_link (source) && ledger.store.block_exists (transaction_a, source))
		{
			hit_receive = true;
			reached_target = true;
			auto const & sideband (block->sideband ());
			auto next = !sideband.successor.is_zero () && sideband.successor != top_level_hash_a ? boost::optional<nano::block_hash> (sideband.successor) : boost::none;
			receive_source_pairs_a.push_back ({ receive_chain_details{ account_a, sideband.height, hash, top_level_hash_a, next, bottom_height_a, bottom_hash_a }, source });
			// Store a checkpoint every max_items so that we can always traverse a long number of accounts to genesis
			if (receive_source_pairs_a.size () % max_items == 0)
			{
				checkpoints_a.push_back (top_level_hash_a);
			}
		}
		else
		{
			// Found a send/change/epoch block which isn't the desired top level
			top_most_non_receive_block_hash_a = hash;
			if (hash == top_level_hash_a)
			{
				reached_target = true;
			}
			else
			{
				hash = block->sideband ().successor;
			}
		}

		// We could be traversing a very large account so we don't want to open read transactions for too long.
		if ((num_blocks > 0) && num_blocks % batch_read_size == 0)
		{
			transaction_a.refresh ();
		}
	}

	return hit_receive;
}

// Once the path to genesis has been iterated to, we can begin to cement the lowest blocks in the accounts. This sets up
// the non-receive blocks which have been iterated for an account, and the associated receive block.
void nano::confirmation_height_bounded::prepare_iterated_blocks_for_cementing (preparation_data & preparation_data_a)
{
	if (!preparation_data_a.already_cemented)
	{
		// Add the non-receive blocks iterated for this account
		auto block_height = (ledger.store.block_account_height (preparation_data_a.transaction, preparation_data_a.top_most_non_receive_block_hash));
		if (block_height > preparation_data_a.confirmation_height_info.height)
		{
			confirmed_info confirmed_info_l{ block_height, preparation_data_a.top_most_non_receive_block_hash };
			if (preparation_data_a.account_it != accounts_confirmed_info.cend ())
			{
				preparation_data_a.account_it->second = confirmed_info_l;
			}
			else
			{
				accounts_confirmed_info.emplace (preparation_data_a.account, confirmed_info_l);
				++accounts_confirmed_info_size;
			}

			preparation_data_a.checkpoints.erase (std::remove (preparation_data_a.checkpoints.begin (), preparation_data_a.checkpoints.end (), preparation_data_a.top_most_non_receive_block_hash), preparation_data_a.checkpoints.end ());
			pending_writes.emplace_back (preparation_data_a.account, preparation_data_a.bottom_height, preparation_data_a.bottom_most, block_height, preparation_data_a.top_most_non_receive_block_hash);
			++pending_writes_size;
		}
	}

	// Add the receive block and all non-receive blocks above that one
	auto & receive_details = preparation_data_a.receive_details;
	if (receive_details)
	{
		auto receive_confirmed_info_it = accounts_confirmed_info.find (receive_details->account);
		if (receive_confirmed_info_it != accounts_confirmed_info.cend ())
		{
			auto & receive_confirmed_info = receive_confirmed_info_it->second;
			receive_confirmed_info.confirmed_height = receive_details->height;
			receive_confirmed_info.iterated_frontier = receive_details->hash;
		}
		else
		{
			accounts_confirmed_info.emplace (std::piecewise_construct, std::forward_as_tuple (receive_details->account), std::forward_as_tuple (receive_details->height, receive_details->hash));
			++accounts_confirmed_info_size;
		}

		if (receive_details->next.is_initialized ())
		{
			preparation_data_a.next_in_receive_chain = top_and_next_hash{ receive_details->top_level, receive_details->next, receive_details->height + 1 };
		}
		else
		{
			preparation_data_a.checkpoints.erase (std::remove (preparation_data_a.checkpoints.begin (), preparation_data_a.checkpoints.end (), receive_details->hash), preparation_data_a.checkpoints.end ());
		}

		pending_writes.emplace_back (receive_details->account, receive_details->bottom_height, receive_details->bottom_most, receive_details->height, receive_details->hash);
		++pending_writes_size;
	}
}

void nano::confirmation_height_bounded::cement_blocks (nano::write_guard & scoped_write_guard_a)
{
	// Will contain all blocks that have been cemented (bounded by batch_write_size)
	// and will get run through the cemented observer callback
	std::vector<std::shared_ptr<nano::block>> cemented_blocks;
	auto const maximum_batch_write_time = 250; // milliseconds
	auto const maximum_batch_write_time_increase_cutoff = maximum_batch_write_time - (maximum_batch_write_time / 5);
	auto const amount_to_change = batch_write_size / 10; // 10%
	auto const minimum_batch_write_size = 16384u;
	nano::timer<> cemented_batch_timer;
	auto error = false;
	{
		// This only writes to the confirmation_height table and is the only place to do so in a single process
		auto transaction (ledger.store.tx_begin_write ({}, { nano::tables::confirmation_height }));
		cemented_batch_timer.start ();
		// Cement all pending entries, each entry is specific to an account and contains the least amount
		// of blocks to retain consistent cementing across all account chains to genesis.
		while (!error && !pending_writes.empty ())
		{
			const auto & pending = pending_writes.front ();
			const auto & account = pending.account;

			auto write_confirmation_height = [&account, &ledger = ledger, &transaction](uint64_t num_blocks_cemented, uint64_t confirmation_height, nano::block_hash const & confirmed_frontier) {
#ifndef NDEBUG
				// Extra debug checks
				nano::confirmation_height_info confirmation_height_info;
				debug_assert (!ledger.store.confirmation_height_get (transaction, account, confirmation_height_info));
				auto block (ledger.store.block_get (transaction, confirmed_frontier));
				debug_assert (block != nullptr);
				debug_assert (block->sideband ().height == confirmation_height_info.height + num_blocks_cemented);
#endif
				ledger.store.confirmation_height_put (transaction, account, nano::confirmation_height_info{ confirmation_height, confirmed_frontier });
				ledger.cache.cemented_count += num_blocks_cemented;
				ledger.stats.add (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in, num_blocks_cemented);
				ledger.stats.add (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_bounded, nano::stat::dir::in, num_blocks_cemented);
			};

			nano::confirmation_height_info confirmation_height_info;
			error = ledger.store.confirmation_height_get (transaction, pending.account, confirmation_height_info);
			if (error)
			{
				auto error_str = (boost::format ("Failed to read confirmation height for account %1% (bounded processor)") % pending.account.to_account ()).str ();
				logger.always_log (error_str);
				std::cerr << error_str << std::endl;
			}

			// Some blocks need to be cemented at least
			if (!error && pending.top_height > confirmation_height_info.height)
			{
				// The highest hash which will be cemented
				nano::block_hash new_cemented_frontier;
				uint64_t num_blocks_confirmed = 0;
				uint64_t start_height = 0;
				if (pending.bottom_height > confirmation_height_info.height)
				{
					new_cemented_frontier = pending.bottom_hash;
					// If we are higher than the cemented frontier, we should be exactly 1 block above
					debug_assert (pending.bottom_height == confirmation_height_info.height + 1);
					num_blocks_confirmed = pending.top_height - pending.bottom_height + 1;
					start_height = pending.bottom_height;
				}
				else
				{
					auto block = ledger.store.block_get (transaction, confirmation_height_info.frontier);
					new_cemented_frontier = block->sideband ().successor;
					num_blocks_confirmed = pending.top_height - confirmation_height_info.height;
					start_height = confirmation_height_info.height + 1;
				}

				auto total_blocks_cemented = 0;
				auto block = ledger.store.block_get (transaction, new_cemented_frontier);

				// Cementing starts from the bottom of the chain and works upwards. This is because chains can have effectively
				// an infinite number of send/change blocks in a row. We don't want to hold the write transaction open for too long.
				for (auto num_blocks_iterated = 0; num_blocks_confirmed - num_blocks_iterated != 0; ++num_blocks_iterated)
				{
					if (!block)
					{
						auto error_str = (boost::format ("Failed to write confirmation height for block %1% (bounded processor)") % new_cemented_frontier.to_string ()).str ();
						logger.always_log (error_str);
						std::cerr << error_str << std::endl;
						// Undo any blocks about to be cemented from this account for this pending write.
						cemented_blocks.erase (cemented_blocks.end () - num_blocks_iterated, cemented_blocks.end ());
						error = true;
						break;
					}

					auto last_iteration = (num_blocks_confirmed - num_blocks_iterated) == 1;

					cemented_blocks.emplace_back (block);

					// Flush these callbacks and continue as we write in batches (ideally maximum 250ms) to not hold write db transaction for too long.
					// Include a tolerance to save having to potentially wait on the block processor if the number of blocks to cement is only a bit higher than the max.
					if (cemented_blocks.size () > batch_write_size + (batch_write_size / 10))
					{
						auto time_spent_cementing = cemented_batch_timer.since_start ().count ();
						auto num_blocks_cemented = num_blocks_iterated - total_blocks_cemented + 1;
						total_blocks_cemented += num_blocks_cemented;
						write_confirmation_height (num_blocks_cemented, start_height + total_blocks_cemented - 1, new_cemented_frontier);
						transaction.commit ();
						logger.always_log (boost::str (boost::format ("Cemented %1% blocks in %2% %3% (bounded processor)") % cemented_blocks.size () % time_spent_cementing % cemented_batch_timer.unit ()));

						// Update the maximum amount of blocks to write next time based on the time it took to cement this batch.
						if (!constants.network.is_dev_network ())
						{
							if (time_spent_cementing > maximum_batch_write_time)
							{
								// Reduce (unless we have hit a floor)
								batch_write_size = std::max<uint64_t> (minimum_batch_write_size, batch_write_size - amount_to_change);
							}
							else if (time_spent_cementing < maximum_batch_write_time_increase_cutoff)
							{
								// Increase amount of blocks written for next batch if the time for writing this one is sufficiently lower than the max time to warrant changing
								batch_write_size += amount_to_change;
							}
						}

						scoped_write_guard_a.release ();
						notify_observers_callback (cemented_blocks);
						cemented_blocks.clear ();

						// Only aquire transaction if there are blocks left
						if (!(last_iteration && pending_writes.size () == 1))
						{
							scoped_write_guard_a = write_database_queue.wait (nano::writer::confirmation_height);
							transaction.renew ();
						}
						cemented_batch_timer.restart ();
					}

					// Get the next block in the chain until we have reached the final desired one
					if (!last_iteration)
					{
						new_cemented_frontier = block->sideband ().successor;
						block = ledger.store.block_get (transaction, new_cemented_frontier);
					}
					else
					{
						// Confirm it is indeed the last one
						debug_assert (new_cemented_frontier == pending.top_hash);
					}
				}

				if (error)
				{
					// There was an error writing a block, do not process any more
					break;
				}

				auto num_blocks_cemented = num_blocks_confirmed - total_blocks_cemented;
				if (num_blocks_cemented > 0)
				{
					write_confirmation_height (num_blocks_cemented, pending.top_height, new_cemented_frontier);
				}
			}

			auto it = accounts_confirmed_info.find (pending.account);
			if (it != accounts_confirmed_info.cend () && it->second.confirmed_height == pending.top_height)
			{
				accounts_confirmed_info.erase (pending.account);
				--accounts_confirmed_info_size;
			}
			pending_writes.pop_front ();
			--pending_writes_size;
		}
	}
	auto time_spent_cementing = cemented_batch_timer.since_start ().count ();
	if (time_spent_cementing > 50)
	{
		logger.always_log (boost::str (boost::format ("Cemented %1% blocks in %2% %3% (bounded processor)") % cemented_blocks.size () % time_spent_cementing % cemented_batch_timer.unit ()));
	}

	// Scope guard could have been released earlier (0 cemented_blocks would indicate that)
	if (scoped_write_guard_a.is_owned () && !cemented_blocks.empty ())
	{
		scoped_write_guard_a.release ();
		notify_observers_callback (cemented_blocks);
	}
	release_assert (!error);
	// Tests should check this already at the end, but not all blocks may have elections (e.g from manual calls to confirmation_height_processor::add), this should catch any inconsistencies on live/beta though
	if (!constants.network.is_dev_network ())
	{
		// Bail if there was an error. This indicates that there was a fatal issue with the ledger
		// (the blocks probably got rolled back when they shouldn't have).
		auto blocks_confirmed_stats = ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed);
		auto observer_stats = ledger.stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::all, nano::stat::dir::out);
		debug_assert (blocks_confirmed_stats == observer_stats);

		// Lower batch_write_size if it took too long to write that amount.
		if (time_spent_cementing > maximum_batch_write_time)
		{
			// Reduce (unless we have hit a floor)
			batch_write_size = std::max<uint64_t> (minimum_batch_write_size, batch_write_size - amount_to_change);
		}
	}

	debug_assert (pending_writes.empty ());
	debug_assert (pending_writes_size == 0);
	timer.restart ();
}

bool nano::confirmation_height_bounded::pending_empty () const
{
	return pending_writes.empty ();
}

void nano::confirmation_height_bounded::clear_process_vars ()
{
	accounts_confirmed_info.clear ();
	accounts_confirmed_info_size = 0;
}

nano::confirmation_height_bounded::receive_chain_details::receive_chain_details (nano::account const & account_a, uint64_t height_a, nano::block_hash const & hash_a, nano::block_hash const & top_level_a, boost::optional<nano::block_hash> next_a, uint64_t bottom_height_a, nano::block_hash const & bottom_most_a) :
account (account_a),
height (height_a),
hash (hash_a),
top_level (top_level_a),
next (next_a),
bottom_height (bottom_height_a),
bottom_most (bottom_most_a)
{
}

nano::confirmation_height_bounded::write_details::write_details (nano::account const & account_a, uint64_t bottom_height_a, nano::block_hash const & bottom_hash_a, uint64_t top_height_a, nano::block_hash const & top_hash_a) :
account (account_a),
bottom_height (bottom_height_a),
bottom_hash (bottom_hash_a),
top_height (top_height_a),
top_hash (top_hash_a)
{
}

nano::confirmation_height_bounded::receive_source_pair::receive_source_pair (confirmation_height_bounded::receive_chain_details const & receive_details_a, const block_hash & source_a) :
receive_details (receive_details_a),
source_hash (source_a)
{
}

nano::confirmation_height_bounded::confirmed_info::confirmed_info (uint64_t confirmed_height_a, nano::block_hash const & iterated_frontier_a) :
confirmed_height (confirmed_height_a),
iterated_frontier (iterated_frontier_a)
{
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (confirmation_height_bounded & confirmation_height_bounded, const std::string & name_a)
{
	auto composite = std::make_unique<container_info_composite> (name_a);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "pending_writes", confirmation_height_bounded.pending_writes_size, sizeof (decltype (confirmation_height_bounded.pending_writes)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "accounts_confirmed_info", confirmation_height_bounded.accounts_confirmed_info_size, sizeof (decltype (confirmation_height_bounded.accounts_confirmed_info)::value_type) }));
	return composite;
}
