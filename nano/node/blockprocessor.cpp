#include <nano/lib/timer.hpp>
#include <nano/node/blockprocessor.hpp>
#include <nano/node/node.hpp>
#include <nano/secure/blockstore.hpp>

#include <boost/format.hpp>

#include <cassert>

std::chrono::milliseconds constexpr nano::block_processor::confirmation_request_delay;

nano::block_processor::block_processor (nano::node & node_a, nano::write_database_queue & write_database_queue_a)
	: generator (node_a.config, node_a.store, node_a.wallets, node_a.vote_processor, node_a.votes_cache, node_a.network)
	, stopped (false)
	, active (false)
	, next_log (std::chrono::steady_clock::now ())
	, node (node_a)
	, write_database_queue (write_database_queue_a)
{
}

nano::block_processor::~block_processor ()
{
	stop ();
}

void nano::block_processor::stop ()
{
	generator.stop ();
	{
		nano::lock_guard<std::mutex> lock (mutex);
		stopped = true;
	}
	condition.notify_all ();
}

void nano::block_processor::flush ()
{
	node.checker.flush ();
	nano::unique_lock<std::mutex> lock (mutex);
	while (!stopped && (have_blocks () || active))
	{
		condition.wait (lock);
	}
	blocks_filter.clear ();
}

size_t nano::block_processor::size ()
{
	nano::unique_lock<std::mutex> lock (mutex);
	return (blocks.size () + state_blocks.size () + forced.size ());
}

bool nano::block_processor::full ()
{
	return size () > node.flags.block_processor_full_size;
}

bool nano::block_processor::half_full ()
{
	return size () > node.flags.block_processor_full_size / 2;
}

void nano::block_processor::add (std::shared_ptr<nano::block> block_a, uint64_t origination)
{
	nano::unchecked_info info (block_a, 0, origination, nano::signature_verification::unknown);
	add (info);
}

void nano::block_processor::add (nano::unchecked_info const & info_a)
{
	if (!nano::work_validate (info_a.block->root (), info_a.block->block_work ()))
	{
		{
			auto hash (info_a.block->hash ());
			auto filter_hash (filter_item (hash, info_a.block->block_signature ()));
			nano::lock_guard<std::mutex> lock (mutex);
			if (blocks_filter.find (filter_hash) == blocks_filter.end () && rolled_back.get<1> ().find (hash) == rolled_back.get<1> ().end ())
			{
				if (info_a.verified == nano::signature_verification::unknown && (info_a.block->type () == nano::block_type::state || info_a.block->type () == nano::block_type::open || !info_a.account.is_zero ()))
				{
					state_blocks.push_back (info_a);
				}
				else
				{
					blocks.push_back (info_a);
				}
				blocks_filter.insert (filter_hash);
			}
		}
		condition.notify_all ();
	}
	else
	{
		node.logger.try_log ("nano::block_processor::add called for hash ", info_a.block->hash ().to_string (), " with invalid work ", nano::to_string_hex (info_a.block->block_work ()));
		assert (false && "nano::block_processor::add called with invalid work");
	}
}

void nano::block_processor::force (std::shared_ptr<nano::block> block_a)
{
	{
		nano::lock_guard<std::mutex> lock (mutex);
		forced.push_back (block_a);
	}
	condition.notify_all ();
}

void nano::block_processor::wait_write ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	awaiting_write = true;
}

void nano::block_processor::process_blocks ()
{
	nano::unique_lock<std::mutex> lock (mutex);
	while (!stopped)
	{
		if (have_blocks ())
		{
			active = true;
			lock.unlock ();
			process_batch (lock);
			lock.lock ();
			active = false;
		}
		else
		{
			condition.notify_all ();
			condition.wait (lock);
		}
	}
}

bool nano::block_processor::should_log (bool first_time)
{
	auto result (false);
	auto now (std::chrono::steady_clock::now ());
	if (first_time || next_log < now)
	{
		next_log = now + std::chrono::seconds (15);
		result = true;
	}
	return result;
}

bool nano::block_processor::have_blocks ()
{
	assert (!mutex.try_lock ());
	return !blocks.empty () || !forced.empty () || !state_blocks.empty ();
}

void nano::block_processor::verify_state_blocks (nano::unique_lock<std::mutex> & lock_a, size_t max_count)
{
	assert (!mutex.try_lock ());
	nano::timer<std::chrono::milliseconds> timer_l (nano::timer_state::started);
	std::deque<nano::unchecked_info> items;
	items.swap (state_blocks);
	lock_a.unlock ();
	if (!items.empty ())
	{
		auto size (items.size ());
		std::vector<nano::block_hash> hashes;
		hashes.reserve (size);
		std::vector<unsigned char const *> messages;
		messages.reserve (size);
		std::vector<size_t> lengths;
		lengths.reserve (size);
		std::vector<nano::account> accounts;
		accounts.reserve (size);
		std::vector<unsigned char const *> pub_keys;
		pub_keys.reserve (size);
		std::vector<nano::signature> blocks_signatures;
		blocks_signatures.reserve (size);
		std::vector<unsigned char const *> signatures;
		signatures.reserve (size);
		std::vector<int> verifications;
		verifications.resize (size, 0);
		for (auto i (0); i < size; ++i)
		{
			auto & item (items[i]);
			hashes.push_back (item.block->hash ());
			messages.push_back (hashes.back ().bytes.data ());
			lengths.push_back (sizeof (decltype (hashes)::value_type));
			nano::account account (item.block->account ());
			if (!item.block->link ().is_zero () && node.ledger.is_epoch_link (item.block->link ()))
			{
				account = node.ledger.epoch_signer (item.block->link ());
			}
			else if (!item.account.is_zero ())
			{
				account = item.account;
			}
			accounts.push_back (account);
			pub_keys.push_back (accounts.back ().bytes.data ());
			blocks_signatures.push_back (item.block->block_signature ());
			signatures.push_back (blocks_signatures.back ().bytes.data ());
		}
		nano::signature_check_set check = { size, messages.data (), lengths.data (), pub_keys.data (), signatures.data (), verifications.data () };
		node.checker.verify (check);
		lock_a.lock ();
		for (auto i (0); i < size; ++i)
		{
			assert (verifications[i] == 1 || verifications[i] == 0);
			auto & item (items.front ());
			if (!item.block->link ().is_zero () && node.ledger.is_epoch_link (item.block->link ()))
			{
				// Epoch blocks
				if (verifications[i] == 1)
				{
					item.verified = nano::signature_verification::valid_epoch;
					blocks.push_back (std::move (item));
				}
				else
				{
					// Possible regular state blocks with epoch link (send subtype)
					item.verified = nano::signature_verification::unknown;
					blocks.push_back (std::move (item));
				}
			}
			else if (verifications[i] == 1)
			{
				// Non epoch blocks
				item.verified = nano::signature_verification::valid;
				blocks.push_back (std::move (item));
			}
			else
			{
				blocks_filter.erase (filter_item (hashes[i], blocks_signatures[i]));
				requeue_invalid (hashes[i], item);
			}
			items.pop_front ();
		}
		if (node.config.logging.timing_logging ())
		{
			node.logger.try_log (boost::str (boost::format ("Batch verified %1% state blocks in %2% %3%") % size % timer_l.stop ().count () % timer_l.unit ()));
		}
	}
	else
	{
		lock_a.lock ();
	}
}

void nano::block_processor::process_batch (nano::unique_lock<std::mutex> & lock_a)
{
	nano::timer<std::chrono::milliseconds> timer_l;
	lock_a.lock ();
	timer_l.start ();
	// Limit state blocks verification time

	{
		if (!state_blocks.empty ())
		{
			size_t max_verification_batch (node.flags.block_processor_verification_size != 0 ? node.flags.block_processor_verification_size : 2048 * (node.config.signature_checker_threads + 1));
			while (!state_blocks.empty () && timer_l.before_deadline (std::chrono::seconds (2)))
			{
				verify_state_blocks (lock_a, max_verification_batch);
			}
		}
	}
	lock_a.unlock ();
	auto scoped_write_guard = write_database_queue.wait (nano::writer::process_batch);
	auto transaction (node.store.tx_begin_write ({ nano::tables::accounts, nano::tables::cached_counts, nano::tables::change_blocks, nano::tables::frontiers, nano::tables::open_blocks, nano::tables::pending, nano::tables::receive_blocks, nano::tables::representation, nano::tables::send_blocks, nano::tables::state_blocks, nano::tables::unchecked }, { nano::tables::confirmation_height }));
	timer_l.restart ();
	lock_a.lock ();
	// Processing blocks
	auto first_time (true);
	unsigned number_of_blocks_processed (0), number_of_forced_processed (0);
	while ((!blocks.empty () || !forced.empty ()) && (timer_l.before_deadline (node.config.block_processor_batch_max_time) || (number_of_blocks_processed < node.flags.block_processor_batch_size)) && !awaiting_write)
	{
		auto log_this_record (false);
		if (node.config.logging.timing_logging ())
		{
			if (should_log (first_time))
			{
				log_this_record = true;
			}
		}
		else
		{
			if (((blocks.size () + state_blocks.size () + forced.size ()) > 64 && should_log (false)))
			{
				log_this_record = true;
			}
		}

		if (log_this_record)
		{
			first_time = false;
			node.logger.always_log (boost::str (boost::format ("%1% blocks (+ %2% state blocks) (+ %3% forced) in processing queue") % blocks.size () % state_blocks.size () % forced.size ()));
		}
		nano::unchecked_info info;
		nano::block_hash hash (0);
		bool force (false);
		if (forced.empty ())
		{
			info = blocks.front ();
			blocks.pop_front ();
			hash = info.block->hash ();
			blocks_filter.erase (filter_item (hash, info.block->block_signature ()));
		}
		else
		{
			info = nano::unchecked_info (forced.front (), 0, nano::seconds_since_epoch (), nano::signature_verification::unknown);
			forced.pop_front ();
			hash = info.block->hash ();
			force = true;
			number_of_forced_processed++;
		}
		lock_a.unlock ();
		if (force)
		{
			auto successor (node.ledger.successor (transaction, info.block->qualified_root ()));
			if (successor != nullptr && successor->hash () != hash)
			{
				// Replace our block with the winner and roll back any dependent blocks
				node.logger.always_log (boost::str (boost::format ("Rolling back %1% and replacing with %2%") % successor->hash ().to_string () % hash.to_string ()));
				std::vector<std::shared_ptr<nano::block>> rollback_list;
				if (node.ledger.rollback (transaction, successor->hash (), rollback_list))
				{
					node.logger.always_log (nano::severity_level::error, boost::str (boost::format ("Failed to roll back %1% because it or a successor was confirmed") % successor->hash ().to_string ()));
				}
				else
				{
					node.logger.always_log (boost::str (boost::format ("%1% blocks rolled back") % rollback_list.size ()));
				}
				lock_a.lock ();
				// Prevent rolled back blocks second insertion
				auto inserted (rolled_back.insert (nano::rolled_hash{ std::chrono::steady_clock::now (), successor->hash () }));
				if (inserted.second)
				{
					// Possible election winner change
					rolled_back.get<1> ().erase (hash);
					// Prevent overflow
					if (rolled_back.size () > rolled_back_max)
					{
						rolled_back.erase (rolled_back.begin ());
					}
				}
				lock_a.unlock ();
				// Deleting from votes cache & wallet work watcher, stop active transaction
				for (auto & i : rollback_list)
				{
					node.votes_cache.remove (i->hash ());
					node.wallets.watcher->remove (i);
					node.active.erase (*i);
				}
			}
		}
		number_of_blocks_processed++;
		process_one (transaction, info);
		lock_a.lock ();
		/* Verify more state blocks if blocks deque is empty
		 Because verification is long process, avoid large deque verification inside of write transaction */
		if (blocks.empty () && !state_blocks.empty ())
		{
			verify_state_blocks (lock_a, 256 * (node.config.signature_checker_threads + 1));
		}
	}
	awaiting_write = false;
	lock_a.unlock ();

	if (node.config.logging.timing_logging () && number_of_blocks_processed != 0)
	{
		node.logger.always_log (boost::str (boost::format ("Processed %1% blocks (%2% blocks were forced) in %3% %4%") % number_of_blocks_processed % number_of_forced_processed % timer_l.stop ().count () % timer_l.unit ()));
	}
}

void nano::block_processor::process_live (nano::block_hash const & hash_a, std::shared_ptr<nano::block> block_a, const bool watch_work_a)
{
	// Start collecting quorum on block
	node.active.start (block_a, false);
	//add block to watcher if desired after block has been added to active
	if (watch_work_a)
	{
		node.wallets.watcher->add (block_a);
	}
	// Announce block contents to the network
	node.network.flood_block (block_a, false);
	if (node.config.enable_voting)
	{
		// Announce our weighted vote to the network
		generator.add (hash_a);
	}
}

nano::process_return nano::block_processor::process_one (nano::write_transaction const & transaction_a, nano::unchecked_info info_a, const bool watch_work_a)
{
	nano::process_return result;
	auto hash (info_a.block->hash ());
	result = node.ledger.process (transaction_a, *(info_a.block), info_a.verified);
	switch (result.code)
	{
		case nano::process_result::progress:
		{
			release_assert (info_a.account.is_zero () || info_a.account == result.account);
			if (node.config.logging.ledger_logging ())
			{
				std::string block;
				info_a.block->serialize_json (block, node.config.logging.single_line_record ());
				node.logger.try_log (boost::str (boost::format ("Processing block %1%: %2%") % hash.to_string () % block));
			}
			if (info_a.modified > nano::seconds_since_epoch () - 300 && node.block_arrival.recent (hash))
			{
				process_live (hash, info_a.block, watch_work_a);
			}
			queue_unchecked (transaction_a, hash);
			break;
		}
		case nano::process_result::gap_previous:
		{
			if (node.config.logging.ledger_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Gap previous for: %1%") % hash.to_string ()));
			}
			info_a.verified = result.verified;
			if (info_a.modified == 0)
			{
				info_a.modified = nano::seconds_since_epoch ();
			}
			node.store.unchecked_put (transaction_a, nano::unchecked_key (info_a.block->previous (), hash), info_a);
			node.gap_cache.add (hash);
			break;
		}
		case nano::process_result::gap_source:
		{
			if (node.config.logging.ledger_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Gap source for: %1%") % hash.to_string ()));
			}
			info_a.verified = result.verified;
			if (info_a.modified == 0)
			{
				info_a.modified = nano::seconds_since_epoch ();
			}
			node.store.unchecked_put (transaction_a, nano::unchecked_key (node.ledger.block_source (transaction_a, *(info_a.block)), hash), info_a);
			node.gap_cache.add (hash);
			break;
		}
		case nano::process_result::old:
		{
			if (node.config.logging.ledger_duplicate_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Old for: %1%") % hash.to_string ()));
			}
			if (!node.flags.fast_bootstrap)
			{
				queue_unchecked (transaction_a, hash);
			}
			node.active.update_difficulty (info_a.block, transaction_a);
			break;
		}
		case nano::process_result::bad_signature:
		{
			if (node.config.logging.ledger_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Bad signature for: %1%") % hash.to_string ()));
			}
			requeue_invalid (hash, info_a);
			break;
		}
		case nano::process_result::negative_spend:
		{
			if (node.config.logging.ledger_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Negative spend for: %1%") % hash.to_string ()));
			}
			break;
		}
		case nano::process_result::unreceivable:
		{
			if (node.config.logging.ledger_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Unreceivable for: %1%") % hash.to_string ()));
			}
			break;
		}
		case nano::process_result::fork:
		{
			node.process_fork (transaction_a, info_a.block);
			node.stats.inc (nano::stat::type::ledger, nano::stat::detail::fork, nano::stat::dir::in);
			if (node.config.logging.ledger_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Fork for: %1% root: %2%") % hash.to_string () % info_a.block->root ().to_string ()));
			}
			break;
		}
		case nano::process_result::opened_burn_account:
		{
			node.logger.always_log (boost::str (boost::format ("*** Rejecting open block for burn account ***: %1%") % hash.to_string ()));
			break;
		}
		case nano::process_result::balance_mismatch:
		{
			if (node.config.logging.ledger_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Balance mismatch for: %1%") % hash.to_string ()));
			}
			break;
		}
		case nano::process_result::representative_mismatch:
		{
			if (node.config.logging.ledger_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Representative mismatch for: %1%") % hash.to_string ()));
			}
			break;
		}
		case nano::process_result::block_position:
		{
			if (node.config.logging.ledger_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Block %1% cannot follow predecessor %2%") % hash.to_string () % info_a.block->previous ().to_string ()));
			}
			break;
		}
	}
	return result;
}

nano::process_return nano::block_processor::process_one (nano::write_transaction const & transaction_a, std::shared_ptr<nano::block> block_a, const bool watch_work_a)
{
	nano::unchecked_info info (block_a, block_a->account (), 0, nano::signature_verification::unknown);
	auto result (process_one (transaction_a, info, watch_work_a));
	return result;
}

void nano::block_processor::queue_unchecked (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a)
{
	auto unchecked_blocks (node.store.unchecked_get (transaction_a, hash_a));
	for (auto & info : unchecked_blocks)
	{
		if (!node.flags.fast_bootstrap)
		{
			node.store.unchecked_del (transaction_a, nano::unchecked_key (hash_a, info.block->hash ()));
		}
		add (info);
	}
	node.gap_cache.erase (hash_a);
}

nano::block_hash nano::block_processor::filter_item (nano::block_hash const & hash_a, nano::signature const & signature_a)
{
	static nano::random_constants constants;
	nano::block_hash result;
	blake2b_state state;
	blake2b_init (&state, sizeof (result.bytes));
	blake2b_update (&state, constants.not_an_account.bytes.data (), constants.not_an_account.bytes.size ());
	blake2b_update (&state, signature_a.bytes.data (), signature_a.bytes.size ());
	blake2b_update (&state, hash_a.bytes.data (), hash_a.bytes.size ());
	blake2b_final (&state, result.bytes.data (), sizeof (result.bytes));
	return result;
}

void nano::block_processor::requeue_invalid (nano::block_hash const & hash_a, nano::unchecked_info const & info_a)
{
	assert (hash_a == info_a.block->hash ());
	auto attempt (node.bootstrap_initiator.current_attempt ());
	if (attempt != nullptr && attempt->mode == nano::bootstrap_mode::lazy)
	{
		attempt->lazy_requeue (hash_a, info_a.block->previous (), info_a.confirmed);
	}
}
