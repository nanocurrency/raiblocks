#pragma once

#include <nano/node/common.hpp>

#include <boost/asio/spawn.hpp>

#include <deque>
#include <future>

namespace nano
{
class bootstrap_attempt;
class bootstrap_client;
class socket;
class frontier_req_client final : public std::enable_shared_from_this<nano::frontier_req_client>
{
public:
	explicit frontier_req_client (std::shared_ptr<nano::bootstrap_client>, std::shared_ptr<nano::bootstrap_attempt>);
	~frontier_req_client ();
	void run ();
	void receive_frontiers (boost::asio::yield_context yield);
	void unsynced (nano::block_hash const &, nano::block_hash const &);
	void next ();
	std::shared_ptr<nano::bootstrap_client> connection;
	std::shared_ptr<nano::bootstrap_attempt> attempt;
	nano::account current;
	nano::block_hash frontier;
	unsigned count;
	nano::account landing;
	nano::account faucet;
	std::chrono::steady_clock::time_point start_time;
	std::promise<bool> promise;
	/** A very rough estimate of the cost of `bulk_push`ing missing blocks */
	uint64_t bulk_push_cost;
	std::deque<std::pair<nano::account, nano::block_hash>> accounts;
	static size_t constexpr size_frontier = sizeof (nano::account) + sizeof (nano::block_hash);
};
class bootstrap_server;
class frontier_req;
class frontier_req_server final : public std::enable_shared_from_this<nano::frontier_req_server>
{
public:
	frontier_req_server (std::shared_ptr<nano::bootstrap_server> const &, std::unique_ptr<nano::frontier_req>);
	void send_next ();
	void sent_action (boost::system::error_code const &, size_t);
	void send_finished ();
	void no_block_sent (boost::system::error_code const &, size_t);
	void next ();
	std::shared_ptr<nano::bootstrap_server> connection;
	nano::account current;
	nano::block_hash frontier;
	std::unique_ptr<nano::frontier_req> request;
	size_t count;
	std::deque<std::pair<nano::account, nano::block_hash>> accounts;
};
}
