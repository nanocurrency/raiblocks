#pragma once

#include <nano/lib/locks.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/lib/utility.hpp>
#include <nano/secure/common.hpp>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

namespace nano
{
class block_store;
class network;
class node_config;
class vote_processor;
class votes_cache;
class wallets;

class vote_generator final
{
public:
	vote_generator (nano::node_config & config_a, nano::block_store & store_a, nano::wallets & wallets_a, nano::vote_processor & vote_processor_a, nano::votes_cache & votes_cache_a, nano::network & network_a);
	void add (nano::block_hash const &);
	void stop ();

private:
	void run ();
	void send (nano::unique_lock<std::mutex> &);
	nano::node_config & config;
	nano::block_store & store;
	nano::wallets & wallets;
	nano::vote_processor & vote_processor;
	nano::votes_cache & votes_cache;
	nano::network & network;
	std::mutex mutex;
	nano::condition_variable condition;
	std::deque<nano::block_hash> hashes;
	nano::network_params network_params;
	bool stopped{ false };
	bool started{ false };
	std::thread thread;

	friend std::unique_ptr<seq_con_info_component> collect_seq_con_info (vote_generator & vote_generator, const std::string & name);
};

std::unique_ptr<seq_con_info_component> collect_seq_con_info (vote_generator & vote_generator, const std::string & name);
class cached_votes final
{
public:
	std::chrono::steady_clock::time_point time;
	nano::block_hash hash;
	std::vector<std::shared_ptr<nano::vote>> votes;
};
class votes_cache final
{
public:
	void add (std::shared_ptr<nano::vote> const &);
	std::vector<std::shared_ptr<nano::vote>> find (nano::block_hash const &);
	void remove (nano::block_hash const &);

private:
	std::mutex cache_mutex;
	boost::multi_index_container<
		nano::cached_votes,
		boost::multi_index::indexed_by<
			boost::multi_index::ordered_non_unique<boost::multi_index::member<nano::cached_votes, std::chrono::steady_clock::time_point, &nano::cached_votes::time>>,
			boost::multi_index::hashed_unique<boost::multi_index::member<nano::cached_votes, nano::block_hash, &nano::cached_votes::hash>>>>
		cache;
	nano::network_params network_params;
	friend std::unique_ptr<seq_con_info_component> collect_seq_con_info (votes_cache & votes_cache, const std::string & name);
};

std::unique_ptr<seq_con_info_component> collect_seq_con_info (votes_cache & votes_cache, const std::string & name);
}
