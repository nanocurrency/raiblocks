#pragma once

#include <nano/node/common.hpp>

#include <boost/asio/spawn.hpp>

#include <future>

namespace nano
{
class bootstrap_attempt;
class bootstrap_client;
class bulk_push_client final : public std::enable_shared_from_this<nano::bulk_push_client>
{
public:
	explicit bulk_push_client (std::shared_ptr<nano::bootstrap_client> const &, std::shared_ptr<nano::bootstrap_attempt> const &);
	~bulk_push_client ();
	void start ();
	void push ();
	void push_block (nano::block const &);
	void send_finished ();
	std::shared_ptr<nano::bootstrap_client> connection;
	std::shared_ptr<nano::bootstrap_attempt> attempt;
	std::promise<bool> promise;
	std::pair<nano::block_hash, nano::block_hash> current_target;
};
class bootstrap_server;
class bulk_push_server final : public std::enable_shared_from_this<nano::bulk_push_server>
{
public:
	explicit bulk_push_server (std::shared_ptr<nano::bootstrap_server> const &);
	void throttled_receive ();
	void receive (boost::asio::yield_context yield);
	void received_type (nano::block_type type_a, boost::asio::yield_context yield);
	void received_block (nano::block_type type_a, size_t size_a);
	std::shared_ptr<std::vector<uint8_t>> receive_buffer;
	std::shared_ptr<nano::bootstrap_server> connection;
};
}
