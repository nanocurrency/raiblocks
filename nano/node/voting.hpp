#pragma once

#include <nano/lib/locks.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/lib/utility.hpp>
#include <nano/node/wallet.hpp>
#include <nano/secure/common.hpp>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

namespace mi = boost::multi_index;

namespace nano
{
class ledger;
class network;
class node_config;
class stat;
class vote_processor;
class wallets;
namespace transport
{
	class channel;
}

class vote_spacing final
{
	class entry
	{
	public:
		nano::root root;
		std::chrono::steady_clock::time_point time;
		nano::block_hash hash;
	};

	boost::multi_index_container<entry,
	mi::indexed_by<
	mi::hashed_non_unique<mi::tag<class tag_root>,
	mi::member<entry, nano::root, &entry::root>>,
	mi::ordered_non_unique<mi::tag<class tag_time>,
	mi::member<entry, std::chrono::steady_clock::time_point, &entry::time>>>>
	recent;
	std::chrono::milliseconds const delay;
	void trim ();

public:
	vote_spacing (std::chrono::milliseconds const & delay) :
	delay{ delay }
	{
	}
	bool votable (nano::root const & root_a, nano::block_hash const & hash_a) const;
	void flag (nano::root const & root_a, nano::block_hash const & hash_a);
	size_t size () const;
};

class local_vote_history final
{
	class local_vote final
	{
	public:
		local_vote (nano::root const & root_a, nano::block_hash const & hash_a, std::shared_ptr<nano::vote> const & vote_a) :
		root (root_a),
		hash (hash_a),
		vote (vote_a)
		{
		}
		nano::root root;
		nano::block_hash hash;
		std::shared_ptr<nano::vote> vote;
	};

public:
	local_vote_history (nano::voting_constants const & constants) :
	constants{ constants }
	{
	}
	void add (nano::root const & root_a, nano::block_hash const & hash_a, std::shared_ptr<nano::vote> const & vote_a);
	void erase (nano::root const & root_a);

	std::vector<std::shared_ptr<nano::vote>> votes (nano::root const & root_a, nano::block_hash const & hash_a) const;
	bool exists (nano::root const &) const;
	size_t size () const;

private:
	// clang-format off
	boost::multi_index_container<local_vote,
	mi::indexed_by<
		mi::hashed_non_unique<mi::tag<class tag_root>,
			mi::member<local_vote, nano::root, &local_vote::root>>,
		mi::sequenced<mi::tag<class tag_sequence>>>>
	history;
	// clang-format on

	nano::voting_constants const & constants;
	void clean ();
	std::vector<std::shared_ptr<nano::vote>> votes (nano::root const & root_a) const;
	// Only used in Debug
	bool consistency_check (nano::root const &) const;
	mutable nano::mutex mutex;

	friend std::unique_ptr<container_info_component> collect_container_info (local_vote_history & history, std::string const & name);
	friend class local_vote_history_basic_Test;
};

std::unique_ptr<container_info_component> collect_container_info (local_vote_history & history, std::string const & name);

class vote_generator final
{
private:
	using candidate_t = std::pair<nano::root, nano::block_hash>;
	using request_t = std::pair<std::vector<candidate_t>, std::shared_ptr<nano::transport::channel>>;

public:
	vote_generator (nano::node_config const & config_a, nano::ledger & ledger_a, nano::wallets & wallets_a, nano::vote_processor & vote_processor_a, nano::local_vote_history & history_a, nano::network & network_a, nano::stat & stats_a);
	/** Queue items for vote generation, or broadcast votes already in cache */
	void add (nano::root const &, nano::block_hash const &);
	/** Queue blocks for vote generation, returning the number of successful candidates.*/
	size_t generate (std::vector<std::shared_ptr<nano::block>> const & blocks_a, std::shared_ptr<nano::transport::channel> const & channel_a);
	void set_reply_action (std::function<void(std::shared_ptr<nano::vote> const &, std::shared_ptr<nano::transport::channel> const &)>);
	void stop ();

private:
	void run ();
	void broadcast (nano::unique_lock<nano::mutex> &);
	void reply (nano::unique_lock<nano::mutex> &, request_t &&);
	void vote (std::vector<nano::block_hash> const &, std::vector<nano::root> const &, std::function<void(std::shared_ptr<nano::vote> const &)> const &);
	void broadcast_action (std::shared_ptr<nano::vote> const &) const;
	std::function<void(std::shared_ptr<nano::vote> const &, std::shared_ptr<nano::transport::channel> &)> reply_action; // must be set only during initialization by using set_reply_action
	nano::node_config const & config;
	nano::ledger & ledger;
	nano::wallets & wallets;
	nano::vote_processor & vote_processor;
	nano::local_vote_history & history;
	nano::vote_spacing spacing;
	nano::network & network;
	nano::stat & stats;
	mutable nano::mutex mutex;
	nano::condition_variable condition;
	static size_t constexpr max_requests{ 2048 };
	std::deque<request_t> requests;
	std::deque<candidate_t> candidates;
	nano::network_params network_params;
	std::atomic<bool> stopped{ false };
	bool started{ false };
	std::thread thread;

	friend std::unique_ptr<container_info_component> collect_container_info (vote_generator & vote_generator, std::string const & name);
};

std::unique_ptr<container_info_component> collect_container_info (vote_generator & generator, std::string const & name);

class vote_generator_session final
{
public:
	vote_generator_session (vote_generator & vote_generator_a);
	void add (nano::root const &, nano::block_hash const &);
	void flush ();

private:
	nano::vote_generator & generator;
	std::vector<std::pair<nano::root, nano::block_hash>> items;
};
}
