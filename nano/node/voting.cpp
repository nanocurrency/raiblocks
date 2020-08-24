#include "transport/udp.hpp"

#include <nano/lib/threading.hpp>
#include <nano/node/network.hpp>
#include <nano/node/nodeconfig.hpp>
#include <nano/node/vote_processor.hpp>
#include <nano/node/voting.hpp>
#include <nano/node/wallet.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/ledger.hpp>

#include <boost/variant/get.hpp>

#include <chrono>

void nano::local_vote_history::add (nano::root const & root_a, nano::block_hash const & hash_a, std::shared_ptr<nano::vote> const & vote_a)
{
	nano::lock_guard<std::mutex> guard (mutex);
	clean ();
	auto & history_by_root (history.get<tag_root> ());
	// Erase any vote that is not for this hash
	auto range (history_by_root.equal_range (root_a));
	for (auto i (range.first); i != range.second;)
	{
		if (i->hash != hash_a)
		{
			i = history_by_root.erase (i);
		}
		else
		{
			++i;
		}
	}
	auto result (history_by_root.emplace (root_a, hash_a, vote_a));
	debug_assert (result.second);
	debug_assert (std::all_of (history_by_root.equal_range (root_a).first, history_by_root.equal_range (root_a).second, [&hash_a](local_vote const & item_a) -> bool { return item_a.vote != nullptr && item_a.hash == hash_a; }));
}

void nano::local_vote_history::erase (nano::root const & root_a)
{
	nano::lock_guard<std::mutex> guard (mutex);
	auto & history_by_root (history.get<tag_root> ());
	auto range (history_by_root.equal_range (root_a));
	history_by_root.erase (range.first, range.second);
}

std::vector<std::shared_ptr<nano::vote>> nano::local_vote_history::votes (nano::root const & root_a) const
{
	nano::lock_guard<std::mutex> guard (mutex);
	std::vector<std::shared_ptr<nano::vote>> result;
	auto range (history.get<tag_root> ().equal_range (root_a));
	std::transform (range.first, range.second, std::back_inserter (result), [](auto const & entry) { return entry.vote; });
	return result;
}

std::vector<std::shared_ptr<nano::vote>> nano::local_vote_history::votes (nano::root const & root_a, nano::block_hash const & hash_a) const
{
	nano::lock_guard<std::mutex> guard (mutex);
	std::vector<std::shared_ptr<nano::vote>> result;
	auto range (history.get<tag_root> ().equal_range (root_a));
	// clang-format off
	nano::transform_if (range.first, range.second, std::back_inserter (result),
		[&hash_a](auto const & entry) { return entry.hash == hash_a; },
		[](auto const & entry) { return entry.vote; });
	// clang-format on
	return result;
}

bool nano::local_vote_history::exists (nano::root const & root_a) const
{
	nano::lock_guard<std::mutex> guard (mutex);
	return history.get<tag_root> ().find (root_a) != history.get<tag_root> ().end ();
}

void nano::local_vote_history::clean ()
{
	debug_assert (max_size > 0);
	auto & history_by_sequence (history.get<tag_sequence> ());
	while (history_by_sequence.size () > max_size)
	{
		history_by_sequence.erase (history_by_sequence.begin ());
	}
}

size_t nano::local_vote_history::size () const
{
	nano::lock_guard<std::mutex> guard (mutex);
	return history.size ();
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (nano::local_vote_history & history, const std::string & name)
{
	size_t history_count = history.size ();
	auto sizeof_element = sizeof (decltype (history.history)::value_type);
	auto composite = std::make_unique<container_info_composite> (name);
	/* This does not currently loop over each element inside the cache to get the sizes of the votes inside history*/
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "history", history_count, sizeof_element }));
	return composite;
}

nano::vote_generator::vote_generator (nano::node_config const & config_a, nano::ledger & ledger_a, nano::wallets & wallets_a, nano::vote_processor & vote_processor_a, nano::local_vote_history & history_a, nano::network & network_a) :
config (config_a),
ledger (ledger_a),
wallets (wallets_a),
vote_processor (vote_processor_a),
history (history_a),
network (network_a),
thread ([this]() { run (); })
{
	nano::unique_lock<std::mutex> lock (mutex);
	condition.wait (lock, [& started = started] { return started; });
}

void nano::vote_generator::add (nano::root const & root_a, nano::block_hash const & hash_a)
{
	auto transaction (ledger.store.tx_begin_read ());
	auto block (ledger.store.block_get (transaction, hash_a));
	if (block != nullptr && ledger.can_vote (transaction, *block))
	{
		nano::unique_lock<std::mutex> lock (mutex);
		hashes.emplace_back (root_a, hash_a);
		if (hashes.size () >= nano::network::confirm_ack_hashes_max)
		{
			lock.unlock ();
			condition.notify_all ();
		}
	}
}

void nano::vote_generator::stop ()
{
	nano::unique_lock<std::mutex> lock (mutex);
	stopped = true;

	lock.unlock ();
	condition.notify_all ();

	if (thread.joinable ())
	{
		thread.join ();
	}
}

void nano::vote_generator::send (nano::unique_lock<std::mutex> & lock_a)
{
	std::vector<nano::block_hash> hashes_l;
	std::vector<nano::root> roots;
	hashes_l.reserve (nano::network::confirm_ack_hashes_max);
	roots.reserve (nano::network::confirm_ack_hashes_max);
	while (!hashes.empty () && hashes_l.size () < nano::network::confirm_ack_hashes_max)
	{
		auto front (hashes.front ());
		hashes.pop_front ();
		roots.push_back (front.first);
		hashes_l.push_back (front.second);
	}
	lock_a.unlock ();
	{
		auto transaction (ledger.store.tx_begin_read ());
		wallets.foreach_representative ([this, &hashes_l, &roots, &transaction](nano::public_key const & pub_a, nano::raw_key const & prv_a) {
			auto vote (this->ledger.store.vote_generate (transaction, pub_a, prv_a, hashes_l));
			for (size_t i (0), n (hashes_l.size ()); i != n; ++i)
			{
				this->history.add (roots[i], hashes_l[i], vote);
			}
			this->network.flood_vote_pr (vote);
			this->network.flood_vote (vote, 2.0f);
			this->vote_processor.vote (vote, std::make_shared<nano::transport::channel_udp> (this->network.udp_channels, this->network.endpoint (), this->constants.protocol.protocol_version));
		});
	}
	lock_a.lock ();
}

void nano::vote_generator::run ()
{
	nano::thread_role::set (nano::thread_role::name::voting);
	nano::unique_lock<std::mutex> lock (mutex);
	started = true;
	lock.unlock ();
	condition.notify_all ();
	lock.lock ();
	while (!stopped)
	{
		if (hashes.size () >= nano::network::confirm_ack_hashes_max)
		{
			send (lock);
		}
		else
		{
			condition.wait_for (lock, config.vote_generator_delay, [this]() { return this->hashes.size () >= nano::network::confirm_ack_hashes_max; });
			if (hashes.size () >= config.vote_generator_threshold && hashes.size () < nano::network::confirm_ack_hashes_max)
			{
				condition.wait_for (lock, config.vote_generator_delay, [this]() { return this->hashes.size () >= nano::network::confirm_ack_hashes_max; });
			}
			if (!hashes.empty ())
			{
				send (lock);
			}
		}
	}
}

nano::vote_generator_session::vote_generator_session (nano::vote_generator & vote_generator_a) :
generator (vote_generator_a)
{
}

void nano::vote_generator_session::add (nano::root const & root_a, nano::block_hash const & hash_a)
{
	debug_assert (nano::thread_role::get () == nano::thread_role::name::request_loop);
	hashes.emplace_back (root_a, hash_a);
}

void nano::vote_generator_session::flush ()
{
	debug_assert (nano::thread_role::get () == nano::thread_role::name::request_loop);
	for (auto const & i : hashes)
	{
		generator.add (i.first, i.second);
	}
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (nano::vote_generator & vote_generator, const std::string & name)
{
	size_t hashes_count = 0;
	{
		nano::lock_guard<std::mutex> guard (vote_generator.mutex);
		hashes_count = vote_generator.hashes.size ();
	}
	auto sizeof_hashes_element = sizeof (decltype (vote_generator.hashes)::value_type);
	auto composite = std::make_unique<container_info_composite> (name);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "hashes", hashes_count, sizeof_hashes_element }));
	return composite;
}
