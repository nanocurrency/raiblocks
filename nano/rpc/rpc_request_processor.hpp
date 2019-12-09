#pragma once

#include <nano/lib/ipc_client.hpp>
#include <nano/lib/rpc_handler_interface.hpp>
#include <nano/lib/rpcconfig.hpp>
#include <nano/rpc/rpc.hpp>

#include <deque>

namespace nano
{
struct ipc_connection
{
	ipc_connection (nano::ipc::ipc_client && client_a, bool is_available_a)
		: client (std::move (client_a))
		, is_available (is_available_a)
	{
	}

	nano::ipc::ipc_client client;
	bool is_available{ false };
};

struct rpc_request
{
	rpc_request (const std::string & action_a, const std::string & body_a, std::function<void(std::string const &)> response_a)
		: action (action_a)
		, body (body_a)
		, response (response_a)
	{
	}

	std::string action;
	std::string body;
	std::function<void(std::string const &)> response;
};

class rpc_request_processor
{
public:
	rpc_request_processor (boost::asio::io_context & io_ctx, nano::rpc_config & rpc_config);
	~rpc_request_processor ();
	void stop ();
	void add (std::shared_ptr<rpc_request> request);
	std::function<void()> stop_callback;

private:
	void run ();
	void read_payload (std::shared_ptr<nano::ipc_connection> connection, std::shared_ptr<std::vector<uint8_t>> res, std::shared_ptr<nano::rpc_request> rpc_request);
	void try_reconnect_and_execute_request (std::shared_ptr<nano::ipc_connection> connection, nano::shared_const_buffer const & req, std::shared_ptr<std::vector<uint8_t>> res, std::shared_ptr<nano::rpc_request> rpc_request);
	void make_available (nano::ipc_connection & connection);

	std::vector<std::shared_ptr<nano::ipc_connection>> connections;
	std::mutex request_mutex;
	std::mutex connections_mutex;
	bool stopped{ false };
	std::deque<std::shared_ptr<nano::rpc_request>> requests;
	nano::condition_variable condition;
	const std::string ipc_address;
	const uint16_t ipc_port;
	std::thread thread;
};

class ipc_rpc_processor final : public nano::rpc_handler_interface
{
public:
	ipc_rpc_processor (boost::asio::io_context & io_ctx, nano::rpc_config & rpc_config)
		: rpc_request_processor (io_ctx, rpc_config)
	{
	}

	void process_request (std::string const & action_a, std::string const & body_a, std::function<void(std::string const &)> response_a) override
	{
		rpc_request_processor.add (std::make_shared<nano::rpc_request> (action_a, body_a, response_a));
	}

	void stop () override
	{
		rpc_request_processor.stop ();
	}

	void rpc_instance (nano::rpc & rpc) override
	{
		rpc_request_processor.stop_callback = [&rpc]() {
			rpc.stop ();
		};
	}

private:
	nano::rpc_request_processor rpc_request_processor;
};
}
