#include <nano/lib/stats.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/active_transactions.hpp>
#include <nano/node/common.hpp>
#include <nano/node/network.hpp>
#include <nano/node/nodeconfig.hpp>
#include <nano/node/request_aggregator.hpp>
#include <nano/node/transport/udp.hpp>
#include <nano/node/voting.hpp>
#include <nano/node/wallet.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/ledger.hpp>
#include <nano/node/node.hpp>

nano::request_aggregator::request_aggregator (nano::node & node_a, nano::network_constants const & network_constants_a, nano::node_config const & config_a, nano::stat & stats_a, nano::vote_generator & generator_a, nano::local_vote_history & history_a, nano::ledger & ledger_a, nano::wallets & wallets_a, nano::active_transactions & active_a) :
node (node_a),
max_delay (network_constants_a.is_dev_network () ? 50 : 300),
small_delay (network_constants_a.is_dev_network () ? 10 : 50),
max_channel_requests (config_a.max_queued_requests),
stats (stats_a),
local_votes (history_a),
ledger (ledger_a),
wallets (wallets_a),
active (active_a),
generator (generator_a),
thread ([this]() { run (); })
{
	generator.set_reply_action ([this](std::shared_ptr<nano::vote> const & vote_a, std::shared_ptr<nano::transport::channel> const & channel_a) {
		this->reply_action (vote_a, channel_a);
	});
	nano::unique_lock<nano::mutex> lock (mutex);
	condition.wait (lock, [& started = started] { return started; });
}

void nano::request_aggregator::add (std::shared_ptr<nano::transport::channel> const & channel_a, std::vector<std::pair<nano::block_hash, nano::root>> const & hashes_roots_a)
{
	debug_assert (wallets.reps ().voting > 0);
	bool error = true;
	auto const endpoint (nano::transport::map_endpoint_to_v6 (channel_a->get_endpoint ()));
	nano::unique_lock<nano::mutex> lock (mutex);
	// Protecting from ever-increasing memory usage when request are consumed slower than generated
	// Reject request if the oldest request has not yet been processed after its deadline + a modest margin
	if (requests.empty () || (requests.get<tag_deadline> ().begin ()->deadline + 2 * this->max_delay > std::chrono::steady_clock::now ()))
	{
		auto & requests_by_endpoint (requests.get<tag_endpoint> ());
		auto existing (requests_by_endpoint.find (endpoint));
		if (existing == requests_by_endpoint.end ())
		{
			existing = requests_by_endpoint.emplace (channel_a).first;
		}
		requests_by_endpoint.modify (existing, [&hashes_roots_a, &channel_a, &error, this](channel_pool & pool_a) {
			// This extends the lifetime of the channel, which is acceptable up to max_delay
			pool_a.channel = channel_a;
			if (pool_a.hashes_roots.size () + hashes_roots_a.size () <= this->max_channel_requests)
			{
				error = false;
				auto new_deadline (std::min (pool_a.start + this->max_delay, std::chrono::steady_clock::now () + this->small_delay));
				pool_a.deadline = new_deadline;
				pool_a.hashes_roots.insert (pool_a.hashes_roots.begin (), hashes_roots_a.begin (), hashes_roots_a.end ());
			}
		});
		if (requests.size () == 1)
		{
			lock.unlock ();
			condition.notify_all ();
		}
	}
	stats.inc (nano::stat::type::aggregator, !error ? nano::stat::detail::aggregator_accepted : nano::stat::detail::aggregator_dropped);
}

void nano::request_aggregator::run ()
{
	nano::thread_role::set (nano::thread_role::name::request_aggregator);
	nano::unique_lock<nano::mutex> lock (mutex);
	started = true;
	lock.unlock ();
	condition.notify_all ();
	lock.lock ();
	while (!stopped)
	{
		if (!requests.empty ())
		{
			auto & requests_by_deadline (requests.get<tag_deadline> ());
			auto front (requests_by_deadline.begin ());
			if (front->deadline < std::chrono::steady_clock::now ())
			{
				// Store the channel and requests for processing after erasing this pool
				decltype (front->channel) channel{};
				decltype (front->hashes_roots) hashes_roots{};
				requests_by_deadline.modify (front, [&channel, &hashes_roots](channel_pool & pool) {
					channel.swap (pool.channel);
					hashes_roots.swap (pool.hashes_roots);
				});
				requests_by_deadline.erase (front);
				lock.unlock ();
				erase_duplicates (hashes_roots);
				auto const remaining = aggregate (hashes_roots, channel);
				if (!remaining.empty ())
				{
					// Generate votes for the remaining hashes
					auto const generated = generator.generate (remaining, channel);
					stats.add (nano::stat::type::requests, nano::stat::detail::requests_cannot_vote, stat::dir::in, remaining.size () - generated);
				}
				lock.lock ();
			}
			else
			{
				auto deadline = front->deadline;
				condition.wait_until (lock, deadline, [this, &deadline]() { return this->stopped || deadline < std::chrono::steady_clock::now (); });
			}
		}
		else
		{
			condition.wait_for (lock, small_delay, [this]() { return this->stopped || !this->requests.empty (); });
		}
	}
}

void nano::request_aggregator::stop ()
{
	{
		nano::lock_guard<nano::mutex> guard (mutex);
		stopped = true;
	}
	condition.notify_all ();
	if (thread.joinable ())
	{
		thread.join ();
	}
}

std::size_t nano::request_aggregator::size ()
{
	nano::unique_lock<nano::mutex> lock (mutex);
	return requests.size ();
}

bool nano::request_aggregator::empty ()
{
	return size () == 0;
}

void nano::request_aggregator::reply_action (std::shared_ptr<nano::vote> const & vote_a, std::shared_ptr<nano::transport::channel> const & channel_a) const
{
	nano::confirm_ack confirm (vote_a);
	channel_a->send (confirm);
}

void nano::request_aggregator::erase_duplicates (std::vector<std::pair<nano::block_hash, nano::root>> & requests_a) const
{
	std::sort (requests_a.begin (), requests_a.end (), [](auto const & pair1, auto const & pair2) {
		return pair1.first < pair2.first;
	});
	requests_a.erase (std::unique (requests_a.begin (), requests_a.end (), [](auto const & pair1, auto const & pair2) {
		return pair1.first == pair2.first;
	}),
	requests_a.end ());
}

std::vector<std::shared_ptr<nano::block>> nano::request_aggregator::aggregate (std::vector<std::pair<nano::block_hash, nano::root>> const & requests_a, std::shared_ptr<nano::transport::channel> & channel_a) const
{
	auto transaction (ledger.store.tx_begin_read ());
	size_t cached_hashes = 0;
	std::vector<std::shared_ptr<nano::block>> to_generate;
	std::vector<std::shared_ptr<nano::vote>> cached_votes;
	bool generate_vote (false);
	for (auto const & [hash, root] : requests_a)
	{
		// Check if block exist in active elections
		auto block = active.winner (hash);
		// Check if block exists in ledger
		if (block == nullptr)
		{
			block = ledger.store.block_get (transaction, hash);
		}

		// 1. Votes in cache
		auto find_votes (local_votes.votes (root, hash));
		if (!find_votes.empty ())
		{
			++cached_hashes;
			cached_votes.insert (cached_votes.end (), find_votes.begin (), find_votes.end ());
		}
		else
		{
			generate_vote = true;
			// 2. Ledger by root
			if (block == nullptr && !root.is_zero ())
			{
				// Search for block root
				auto successor (ledger.store.block_successor (transaction, root.as_block_hash ()));

				// Search for account root
				if (successor.is_zero ())
				{
					nano::account_info info;
					auto error (ledger.store.account_get (transaction, root.as_account (), info));
					if (!error)
					{
						successor = info.open_block;
					}
				}
				if (!successor.is_zero ())
				{
					auto successor_block = ledger.store.block_get (transaction, successor);
					debug_assert (successor_block != nullptr);
					block = std::move (successor_block);
					// 3. Votes in cache for successor
					auto find_successor_votes (local_votes.votes (root, successor));
					if (!find_successor_votes.empty ())
					{
						cached_votes.insert (cached_votes.end (), find_successor_votes.begin (), find_successor_votes.end ());
						generate_vote = false;
					}
				}
			}
		}

		if (block)
		{
			//If can vote and election does not exist then start an election
			if (!active.active(*block) && !node.block_confirmed_or_being_confirmed (transaction, block->hash ()) && ledger.dependents_confirmed (transaction, *block))
			{
				stats.inc (nano::stat::type::election, nano::stat::detail::election_request_aggregator, stat::dir::in);
				auto election = active.insert(block);
				if (election.inserted)
				{
					election.election->transition_active ();
				}
			}
			// Generate new vote
			if (generate_vote)
			{
				to_generate.push_back (block);
			}

			// Let the node know about the alternative block
			if (block->hash () != hash)
			{
				nano::publish publish (block);
				channel_a->send (publish);
			}
		}
		else
		{
			stats.inc (nano::stat::type::requests, nano::stat::detail::requests_unknown, stat::dir::in);
		}
	}
	// Unique votes
	std::sort (cached_votes.begin (), cached_votes.end ());
	cached_votes.erase (std::unique (cached_votes.begin (), cached_votes.end ()), cached_votes.end ());
	for (auto const & vote : cached_votes)
	{
		reply_action (vote, channel_a);
	}
	stats.add (nano::stat::type::requests, nano::stat::detail::requests_cached_hashes, stat::dir::in, cached_hashes);
	stats.add (nano::stat::type::requests, nano::stat::detail::requests_cached_votes, stat::dir::in, cached_votes.size ());
	return to_generate;
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (nano::request_aggregator & aggregator, std::string const & name)
{
	auto pools_count = aggregator.size ();
	auto sizeof_element = sizeof (decltype (aggregator.requests)::value_type);
	auto composite = std::make_unique<container_info_composite> (name);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "pools", pools_count, sizeof_element }));
	return composite;
}
