#include <nano/lib/logger_mt.hpp>
#include <nano/lib/stats.hpp>
#include <nano/lib/threading.hpp>
#include <nano/lib/timer.hpp>
#include <nano/node/active_transactions.hpp>
#include <nano/node/node.hpp>
#include <nano/node/node_observers.hpp>
#include <nano/node/nodeconfig.hpp>
#include <nano/node/online_reps.hpp>
#include <nano/node/signatures.hpp>
#include <nano/node/vote_processor.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/ledger.hpp>

#include <boost/format.hpp>

nano::vote_processor::vote_processor (nano::signature_checker & checker_a, nano::active_transactions & active_a, nano::block_store & store_a, nano::node_observers & observers_a, nano::stat & stats_a, nano::node_config & config_a, nano::logger_mt & logger_a, nano::online_reps & online_reps_a, nano::ledger & ledger_a, nano::network_params & network_params_a)
	: checker (checker_a)
	, active (active_a)
	, store (store_a)
	, observers (observers_a)
	, stats (stats_a)
	, config (config_a)
	, logger (logger_a)
	, online_reps (online_reps_a)
	, ledger (ledger_a)
	, network_params (network_params_a)
	, started (false)
	, stopped (false)
	, is_active (false)
	, thread ([this]() {
		nano::thread_role::set (nano::thread_role::name::vote_processing);
		process_loop ();
	})
{
	nano::unique_lock<std::mutex> lock (mutex);
	condition.wait (lock, [& started = started] { return started; });
}

void nano::vote_processor::process_loop ()
{
	nano::timer<std::chrono::milliseconds> elapsed;
	bool log_this_iteration;

	nano::unique_lock<std::mutex> lock (mutex);
	started = true;

	lock.unlock ();
	condition.notify_all ();
	lock.lock ();

	while (!stopped)
	{
		if (!votes.empty ())
		{
			std::deque<std::pair<std::shared_ptr<nano::vote>, std::shared_ptr<nano::transport::channel>>> votes_l;
			votes_l.swap (votes);

			log_this_iteration = false;
			if (config.logging.network_logging () && votes_l.size () > 50)
			{
				/*
				 * Only log the timing information for this iteration if
				 * there are a sufficient number of items for it to be relevant
				 */
				log_this_iteration = true;
				elapsed.restart ();
			}
			is_active = true;
			lock.unlock ();
			verify_votes (votes_l);
			{
				nano::unique_lock<std::mutex> active_single_lock (active.mutex);
				auto transaction (store.tx_begin_read ());
				uint64_t count (1);
				for (auto & i : votes_l)
				{
					vote_blocking (transaction, i.first, i.second, true);
					// Free active_transactions mutex each 100 processed votes
					if (count % 100 == 0)
					{
						active_single_lock.unlock ();
						transaction.refresh ();
						active_single_lock.lock ();
					}
					count++;
				}
			}
			lock.lock ();
			is_active = false;

			lock.unlock ();
			condition.notify_all ();
			lock.lock ();

			if (log_this_iteration && elapsed.stop () > std::chrono::milliseconds (100))
			{
				logger.try_log (boost::str (boost::format ("Processed %1% votes in %2% milliseconds (rate of %3% votes per second)") % votes_l.size () % elapsed.value ().count () % ((votes_l.size () * 1000ULL) / elapsed.value ().count ())));
			}
		}
		else
		{
			condition.wait (lock);
		}
	}
}

void nano::vote_processor::vote (std::shared_ptr<nano::vote> vote_a, std::shared_ptr<nano::transport::channel> channel_a)
{
	nano::unique_lock<std::mutex> lock (mutex);
	if (!stopped)
	{
		bool process (false);
		/* Random early delection levels
		 Always process votes for test network (process = true)
		 Stop processing with max 144 * 1024 votes */
		if (!network_params.network.is_test_network ())
		{
			// Level 0 (< 0.1%)
			if (votes.size () < 96 * 1024)
			{
				process = true;
			}
			// Level 1 (0.1-1%)
			else if (votes.size () < 112 * 1024)
			{
				process = (representatives_1.find (vote_a->account) != representatives_1.end ());
			}
			// Level 2 (1-5%)
			else if (votes.size () < 128 * 1024)
			{
				process = (representatives_2.find (vote_a->account) != representatives_2.end ());
			}
			// Level 3 (> 5%)
			else if (votes.size () < 144 * 1024)
			{
				process = (representatives_3.find (vote_a->account) != representatives_3.end ());
			}
		}
		else
		{
			// Process for test network
			process = true;
		}
		if (process)
		{
			votes.push_back (std::make_pair (vote_a, channel_a));

			lock.unlock ();
			condition.notify_all ();
			lock.lock ();
		}
		else
		{
			stats.inc (nano::stat::type::vote, nano::stat::detail::vote_overflow);
		}
	}
}

void nano::vote_processor::verify_votes (std::deque<std::pair<std::shared_ptr<nano::vote>, std::shared_ptr<nano::transport::channel>>> & votes_a)
{
	auto size (votes_a.size ());
	std::vector<unsigned char const *> messages;
	messages.reserve (size);
	std::vector<nano::block_hash> hashes;
	hashes.reserve (size);
	std::vector<size_t> lengths (size, sizeof (nano::block_hash));
	std::vector<unsigned char const *> pub_keys;
	pub_keys.reserve (size);
	std::vector<unsigned char const *> signatures;
	signatures.reserve (size);
	std::vector<int> verifications;
	verifications.resize (size);
	for (auto & vote : votes_a)
	{
		hashes.push_back (vote.first->hash ());
		messages.push_back (hashes.back ().bytes.data ());
		pub_keys.push_back (vote.first->account.bytes.data ());
		signatures.push_back (vote.first->signature.bytes.data ());
	}
	nano::signature_check_set check = { size, messages.data (), lengths.data (), pub_keys.data (), signatures.data (), verifications.data () };
	checker.verify (check);
	std::remove_reference_t<decltype (votes_a)> result;
	auto i (0);
	for (auto & vote : votes_a)
	{
		assert (verifications[i] == 1 || verifications[i] == 0);
		if (verifications[i] == 1)
		{
			result.push_back (vote);
		}
		++i;
	}
	votes_a.swap (result);
}

// node.active.mutex lock required
nano::vote_code nano::vote_processor::vote_blocking (nano::transaction const & transaction_a, std::shared_ptr<nano::vote> vote_a, std::shared_ptr<nano::transport::channel> channel_a, bool validated)
{
	assert (!active.mutex.try_lock ());
	auto result (nano::vote_code::invalid);
	if (validated || !vote_a->validate ())
	{
		auto max_vote (store.vote_max (transaction_a, vote_a));
		result = nano::vote_code::replay;
		if (!active.vote (vote_a, true))
		{
			result = nano::vote_code::vote;
		}
		switch (result)
		{
			case nano::vote_code::vote:
				observers.vote.notify (vote_a, channel_a);
			case nano::vote_code::replay:
				// This tries to assist rep nodes that have lost track of their highest sequence number by replaying our highest known vote back to them
				// Only do this if the sequence number is significantly different to account for network reordering
				// Amplify attack considerations: We're sending out a confirm_ack in response to a confirm_ack for no net traffic increase
				if (max_vote->sequence > vote_a->sequence + 10000)
				{
					nano::confirm_ack confirm (max_vote);
					channel_a->send (confirm); // this is non essential traffic as it will be resolicited if not received
				}
				break;
			case nano::vote_code::invalid:
				assert (false);
				break;
		}
	}
	std::string status;
	switch (result)
	{
		case nano::vote_code::invalid:
			status = "Invalid";
			stats.inc (nano::stat::type::vote, nano::stat::detail::vote_invalid);
			break;
		case nano::vote_code::replay:
			status = "Replay";
			stats.inc (nano::stat::type::vote, nano::stat::detail::vote_replay);
			break;
		case nano::vote_code::vote:
			status = "Vote";
			stats.inc (nano::stat::type::vote, nano::stat::detail::vote_valid);
			break;
	}
	if (config.logging.vote_logging ())
	{
		logger.try_log (boost::str (boost::format ("Vote from: %1% sequence: %2% block(s): %3%status: %4%") % vote_a->account.to_account () % std::to_string (vote_a->sequence) % vote_a->hashes_string () % status));
	}
	return result;
}

void nano::vote_processor::stop ()
{
	{
		nano::lock_guard<std::mutex> lock (mutex);
		stopped = true;
	}
	condition.notify_all ();
	if (thread.joinable ())
	{
		thread.join ();
	}
}

void nano::vote_processor::flush ()
{
	nano::unique_lock<std::mutex> lock (mutex);
	while (is_active || !votes.empty ())
	{
		condition.wait (lock);
	}
}

void nano::vote_processor::calculate_weights ()
{
	nano::unique_lock<std::mutex> lock (mutex);
	if (!stopped)
	{
		representatives_1.clear ();
		representatives_2.clear ();
		representatives_3.clear ();
		auto supply (online_reps.online_stake ());
		auto rep_amounts = ledger.rep_weights.get_rep_amounts ();
		for (auto const & rep_amount : rep_amounts)
		{
			nano::account const & representative (rep_amount.first);
			auto weight (ledger.weight (representative));
			if (weight > supply / 1000) // 0.1% or above (level 1)
			{
				representatives_1.insert (representative);
				if (weight > supply / 100) // 1% or above (level 2)
				{
					representatives_2.insert (representative);
					if (weight > supply / 20) // 5% or above (level 3)
					{
						representatives_3.insert (representative);
					}
				}
			}
		}
	}
}

namespace nano
{
std::unique_ptr<seq_con_info_component> collect_seq_con_info (vote_processor & vote_processor, const std::string & name)
{
	size_t votes_count = 0;
	size_t representatives_1_count = 0;
	size_t representatives_2_count = 0;
	size_t representatives_3_count = 0;

	{
		nano::lock_guard<std::mutex> guard (vote_processor.mutex);
		votes_count = vote_processor.votes.size ();
		representatives_1_count = vote_processor.representatives_1.size ();
		representatives_2_count = vote_processor.representatives_2.size ();
		representatives_3_count = vote_processor.representatives_3.size ();
	}

	auto composite = std::make_unique<seq_con_info_composite> (name);
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "votes", votes_count, sizeof (decltype (vote_processor.votes)::value_type) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "representatives_1", representatives_1_count, sizeof (decltype (vote_processor.representatives_1)::value_type) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "representatives_2", representatives_2_count, sizeof (decltype (vote_processor.representatives_2)::value_type) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "representatives_3", representatives_3_count, sizeof (decltype (vote_processor.representatives_3)::value_type) }));
	return composite;
}
}
