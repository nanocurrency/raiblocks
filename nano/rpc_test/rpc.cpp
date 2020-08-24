#include <nano/boost/beast/core/flat_buffer.hpp>
#include <nano/boost/beast/http.hpp>
#include <nano/lib/rpcconfig.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/ipc/ipc_server.hpp>
#include <nano/node/json_handler.hpp>
#include <nano/node/node_rpc_config.hpp>
#include <nano/node/testing.hpp>
#include <nano/rpc/rpc.hpp>
#include <nano/rpc/rpc_request_processor.hpp>
#include <nano/test_common/telemetry.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <algorithm>

using namespace std::chrono_literals;

namespace
{
class test_response
{
public:
	test_response (boost::property_tree::ptree const & request_a, boost::asio::io_context & io_ctx) :
	request (request_a),
	sock (io_ctx)
	{
	}

	test_response (boost::property_tree::ptree const & request_a, uint16_t port, boost::asio::io_context & io_ctx) :
	request (request_a),
	sock (io_ctx)
	{
		run (port);
	}

	void run (uint16_t port)
	{
		sock.async_connect (nano::tcp_endpoint (boost::asio::ip::address_v6::loopback (), port), [this](boost::system::error_code const & ec) {
			if (!ec)
			{
				std::stringstream ostream;
				boost::property_tree::write_json (ostream, request);
				req.method (boost::beast::http::verb::post);
				req.target ("/");
				req.version (11);
				ostream.flush ();
				req.body () = ostream.str ();
				req.prepare_payload ();
				boost::beast::http::async_write (sock, req, [this](boost::system::error_code const & ec, size_t bytes_transferred) {
					if (!ec)
					{
						boost::beast::http::async_read (sock, sb, resp, [this](boost::system::error_code const & ec, size_t bytes_transferred) {
							if (!ec)
							{
								std::stringstream body (resp.body ());
								try
								{
									boost::property_tree::read_json (body, json);
									status = 200;
								}
								catch (std::exception &)
								{
									status = 500;
								}
							}
							else
							{
								status = 400;
							};
						});
					}
					else
					{
						status = 600;
					}
				});
			}
			else
			{
				status = 400;
			}
		});
	}
	boost::property_tree::ptree const & request;
	boost::asio::ip::tcp::socket sock;
	boost::property_tree::ptree json;
	boost::beast::flat_buffer sb;
	boost::beast::http::request<boost::beast::http::string_body> req;
	boost::beast::http::response<boost::beast::http::string_body> resp;
	std::atomic<int> status{ 0 };
};

std::shared_ptr<nano::node> add_ipc_enabled_node (nano::system & system, nano::node_config & node_config)
{
	node_config.ipc_config.transport_tcp.enabled = true;
	node_config.ipc_config.transport_tcp.port = nano::get_available_port ();
	return system.add_node (node_config);
}

std::shared_ptr<nano::node> add_ipc_enabled_node (nano::system & system)
{
	nano::node_config node_config;
	return add_ipc_enabled_node (system, node_config);
}

void reset_confirmation_height (nano::block_store & store, nano::account const & account)
{
	auto transaction = store.tx_begin_write ();
	nano::confirmation_height_info confirmation_height_info;
	if (!store.confirmation_height_get (transaction, account, confirmation_height_info))
	{
		store.confirmation_height_clear (transaction, account, confirmation_height_info.height);
	}
}

void check_block_response_count (nano::system & system, nano::rpc & rpc, boost::property_tree::ptree & request, uint64_t size_count)
{
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ (size_count, response.json.get_child ("blocks").front ().second.size ());
}

class scoped_io_thread_name_change
{
public:
	scoped_io_thread_name_change ()
	{
		renew ();
	}

	~scoped_io_thread_name_change ()
	{
		reset ();
	}

	void reset ()
	{
		nano::thread_role::set (nano::thread_role::name::unknown);
	}

	void renew ()
	{
		nano::thread_role::set (nano::thread_role::name::io);
	}
};
}

TEST (rpc, wrapped_task)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);
	nano::node_rpc_config node_rpc_config;
	std::atomic<bool> response (false);
	auto response_handler_l ([&response](std::string const & response_a) {
		std::stringstream istream (response_a);
		boost::property_tree::ptree json_l;
		ASSERT_NO_THROW (boost::property_tree::read_json (istream, json_l));
		ASSERT_EQ (1, json_l.count ("error"));
		ASSERT_EQ ("Unable to parse JSON", json_l.get<std::string> ("error"));
		response = true;
	});
	auto handler_l (std::make_shared<nano::json_handler> (node, node_rpc_config, "", response_handler_l));
	auto task (handler_l->create_worker_task ([](std::shared_ptr<nano::json_handler>) {
		// Exception should get caught
		throw std::runtime_error ("");
	}));
	system.nodes[0]->worker.push_task (task);
	ASSERT_TIMELY (5s, response == true);
}

TEST (rpc, account_balance)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_balance");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string balance_text (response.json.get<std::string> ("balance"));
	ASSERT_EQ ("340282366920938463463374607431768211455", balance_text);
	std::string pending_text (response.json.get<std::string> ("pending"));
	ASSERT_EQ ("0", pending_text);
}

TEST (rpc, account_block_count)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_block_count");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string block_count_text (response.json.get<std::string> ("block_count"));
	ASSERT_EQ ("1", block_count_text);
}

TEST (rpc, account_create)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_create");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	test_response response0 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response0.status != 0);
	ASSERT_EQ (200, response0.status);
	auto account_text0 (response0.json.get<std::string> ("account"));
	nano::account account0;
	ASSERT_FALSE (account0.decode_account (account_text0));
	ASSERT_TRUE (system.wallet (0)->exists (account0));
	constexpr uint64_t max_index (std::numeric_limits<uint32_t>::max ());
	request.put ("index", max_index);
	test_response response1 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	auto account_text1 (response1.json.get<std::string> ("account"));
	nano::account account1;
	ASSERT_FALSE (account1.decode_account (account_text1));
	ASSERT_TRUE (system.wallet (0)->exists (account1));
	request.put ("index", max_index + 1);
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_EQ (std::error_code (nano::error_common::invalid_index).message (), response2.json.get<std::string> ("error"));
}

TEST (rpc, account_weight)
{
	nano::keypair key;
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::block_hash latest (node1.latest (nano::dev_genesis_key.pub));
	nano::change_block block (latest, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	ASSERT_EQ (nano::process_result::progress, node1.process (block).code);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_weight");
	request.put ("account", key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string balance_text (response.json.get<std::string> ("weight"));
	ASSERT_EQ ("340282366920938463463374607431768211455", balance_text);
}

TEST (rpc, wallet_contains)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_contains");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string exists_text (response.json.get<std::string> ("exists"));
	ASSERT_EQ ("1", exists_text);
}

TEST (rpc, wallet_doesnt_contain)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_contains");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string exists_text (response.json.get<std::string> ("exists"));
	ASSERT_EQ ("0", exists_text);
}

TEST (rpc, validate_account_number)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "validate_account_number");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	std::string exists_text (response.json.get<std::string> ("valid"));
	ASSERT_EQ ("1", exists_text);
}

TEST (rpc, validate_account_invalid)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	std::string account;
	nano::dev_genesis_key.pub.encode_account (account);
	account[0] ^= 0x1;
	boost::property_tree::ptree request;
	request.put ("action", "validate_account_number");
	request.put ("account", account);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string exists_text (response.json.get<std::string> ("valid"));
	ASSERT_EQ ("0", exists_text);
}

TEST (rpc, send)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev_genesis_key.pub.to_account ());
	request.put ("destination", nano::dev_genesis_key.pub.to_account ());
	request.put ("amount", "100");
	std::thread thread2 ([&system, node]() {
		ASSERT_TIMELY (10s, node->balance (nano::dev_genesis_key.pub) != nano::genesis_amount);
	});
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string block_text (response.json.get<std::string> ("block"));
	nano::block_hash block;
	ASSERT_FALSE (block.decode_hex (block_text));
	ASSERT_TRUE (node->ledger.block_exists (block));
	ASSERT_EQ (node->latest (nano::dev_genesis_key.pub), block);
	thread2.join ();
}

TEST (rpc, send_fail)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev_genesis_key.pub.to_account ());
	request.put ("destination", nano::dev_genesis_key.pub.to_account ());
	request.put ("amount", "100");
	std::atomic<bool> done (false);
	system.deadline_set (10s);
	std::thread thread2 ([&system, &done]() {
		ASSERT_TIMELY (10s, done);
	});
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response.status != 0);
	done = true;
	ASSERT_EQ (std::error_code (nano::error_common::account_not_found_wallet).message (), response.json.get<std::string> ("error"));
	thread2.join ();
}

TEST (rpc, send_work)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev_genesis_key.pub.to_account ());
	request.put ("destination", nano::dev_genesis_key.pub.to_account ());
	request.put ("amount", "100");
	request.put ("work", "1");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response.status != 0);
	ASSERT_EQ (std::error_code (nano::error_common::invalid_work).message (), response.json.get<std::string> ("error"));
	request.erase ("work");
	request.put ("work", nano::to_string_hex (*node->work_generate_blocking (node->latest (nano::dev_genesis_key.pub))));
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	std::string block_text (response2.json.get<std::string> ("block"));
	nano::block_hash block;
	ASSERT_FALSE (block.decode_hex (block_text));
	ASSERT_TRUE (node->ledger.block_exists (block));
	ASSERT_EQ (node->latest (nano::dev_genesis_key.pub), block);
}

TEST (rpc, send_work_disabled)
{
	nano::system system;
	nano::node_config node_config;
	node_config.work_threads = 0;
	auto & node = *add_ipc_enabled_node (system, node_config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node.wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev_genesis_key.pub.to_account ());
	request.put ("destination", nano::dev_genesis_key.pub.to_account ());
	request.put ("amount", "100");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_common::disabled_work_generation).message (), response.json.get<std::string> ("error"));
	}
}

TEST (rpc, send_idempotent)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev_genesis_key.pub.to_account ());
	request.put ("destination", nano::account (0).to_account ());
	request.put ("amount", (nano::genesis_amount - (nano::genesis_amount / 4)).convert_to<std::string> ());
	request.put ("id", "123abc");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string block_text (response.json.get<std::string> ("block"));
	nano::block_hash block;
	ASSERT_FALSE (block.decode_hex (block_text));
	ASSERT_TRUE (node->ledger.block_exists (block));
	ASSERT_EQ (node->balance (nano::dev_genesis_key.pub), nano::genesis_amount / 4);
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_EQ ("", response2.json.get<std::string> ("error", ""));
	ASSERT_EQ (block_text, response2.json.get<std::string> ("block"));
	ASSERT_EQ (node->balance (nano::dev_genesis_key.pub), nano::genesis_amount / 4);
	request.erase ("id");
	request.put ("id", "456def");
	test_response response3 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response3.status != 0);
	ASSERT_EQ (200, response3.status);
	ASSERT_EQ (std::error_code (nano::error_common::insufficient_balance).message (), response3.json.get<std::string> ("error"));
}

TEST (rpc, send_epoch_2)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);

	// Upgrade the genesis account to epoch 2
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (node, nano::epoch::epoch_1));
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (node, nano::epoch::epoch_2));

	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv, false);

	auto target_difficulty = nano::work_threshold (nano::work_version::work_1, nano::block_details (nano::epoch::epoch_2, true, false, false));
	ASSERT_LT (node.env.constants.network.publish_thresholds.entry, target_difficulty);
	auto min_difficulty = node.env.constants.network.publish_thresholds.entry;

	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node.wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev_genesis_key.pub.to_account ());
	request.put ("destination", nano::keypair ().pub.to_account ());
	request.put ("amount", "1");

	// Test that the correct error is given if there is insufficient work
	auto insufficient = system.work_generate_limited (nano::genesis_hash, min_difficulty, target_difficulty);
	request.put ("work", nano::to_string_hex (insufficient));
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::error_code ec (nano::error_common::invalid_work);
		ASSERT_EQ (1, response.json.count ("error"));
		ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, stop)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "stop");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	;
}

TEST (rpc, wallet_add)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::keypair key1;
	std::string key_text;
	key1.prv.data.encode_hex (key_text);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_add");
	request.put ("key", key_text);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string account_text1 (response.json.get<std::string> ("account"));
	ASSERT_EQ (account_text1, key1.pub.to_account ());
	ASSERT_TRUE (system.wallet (0)->exists (key1.pub));
}

TEST (rpc, wallet_password_valid)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "password_valid");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string account_text1 (response.json.get<std::string> ("valid"));
	ASSERT_EQ (account_text1, "1");
}

TEST (rpc, wallet_password_change)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "password_change");
	request.put ("password", "test");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string account_text1 (response.json.get<std::string> ("changed"));
	ASSERT_EQ (account_text1, "1");
	scoped_thread_name_io.reset ();
	auto transaction (system.wallet (0)->wallets.tx_begin_write ());
	ASSERT_TRUE (system.wallet (0)->store.valid_password (transaction));
	ASSERT_TRUE (system.wallet (0)->enter_password (transaction, ""));
	ASSERT_FALSE (system.wallet (0)->store.valid_password (transaction));
	ASSERT_FALSE (system.wallet (0)->enter_password (transaction, "test"));
	ASSERT_TRUE (system.wallet (0)->store.valid_password (transaction));
}

TEST (rpc, wallet_password_enter)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::raw_key password_l;
	password_l.data.clear ();
	system.deadline_set (10s);
	while (password_l.data == 0)
	{
		ASSERT_NO_ERROR (system.poll ());
		system.wallet (0)->store.password.value (password_l);
	}
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "password_enter");
	request.put ("password", "");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string account_text1 (response.json.get<std::string> ("valid"));
	ASSERT_EQ (account_text1, "1");
}

TEST (rpc, wallet_representative)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_representative");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string account_text1 (response.json.get<std::string> ("representative"));
	ASSERT_EQ (account_text1, nano::genesis_account.to_account ());
}

TEST (rpc, wallet_representative_set)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	nano::keypair key;
	request.put ("action", "wallet_representative_set");
	request.put ("representative", key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto transaction (node->wallets.tx_begin_read ());
	ASSERT_EQ (key.pub, node->wallets.items.begin ()->second->store.representative (transaction));
}

TEST (rpc, wallet_representative_set_force)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	nano::keypair key;
	request.put ("action", "wallet_representative_set");
	request.put ("representative", key.pub.to_account ());
	request.put ("update_existing_accounts", true);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	{
		auto transaction (node->wallets.tx_begin_read ());
		ASSERT_EQ (key.pub, node->wallets.items.begin ()->second->store.representative (transaction));
	}
	nano::account representative (0);
	while (representative != key.pub)
	{
		auto transaction (node->store.tx_begin_read ());
		nano::account_info info;
		if (!node->store.account_get (transaction, nano::dev_genesis_key.pub, info))
		{
			representative = info.representative;
		}
		ASSERT_NO_ERROR (system.poll ());
	}
}

TEST (rpc, account_list)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key2;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key2.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "account_list");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & accounts_node (response.json.get_child ("accounts"));
	std::vector<nano::account> accounts;
	for (auto i (accounts_node.begin ()), j (accounts_node.end ()); i != j; ++i)
	{
		auto account (i->second.get<std::string> (""));
		nano::account number;
		ASSERT_FALSE (number.decode_account (account));
		accounts.push_back (number);
	}
	ASSERT_EQ (2, accounts.size ());
	for (auto i (accounts.begin ()), j (accounts.end ()); i != j; ++i)
	{
		ASSERT_TRUE (system.wallet (0)->exists (*i));
	}
}

TEST (rpc, wallet_key_valid)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_key_valid");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string exists_text (response.json.get<std::string> ("valid"));
	ASSERT_EQ ("1", exists_text);
}

TEST (rpc, wallet_create)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_create");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string wallet_text (response.json.get<std::string> ("wallet"));
	nano::wallet_id wallet_id;
	ASSERT_FALSE (wallet_id.decode_hex (wallet_text));
	ASSERT_NE (node->wallets.items.end (), node->wallets.items.find (wallet_id));
}

TEST (rpc, wallet_create_seed)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::raw_key seed;
	nano::random_pool::generate_block (seed.data.bytes.data (), seed.data.bytes.size ());
	auto prv = nano::deterministic_key (seed, 0);
	auto pub (nano::pub_key (prv));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_create");
	request.put ("seed", seed.data.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string wallet_text (response.json.get<std::string> ("wallet"));
	nano::wallet_id wallet_id;
	ASSERT_FALSE (wallet_id.decode_hex (wallet_text));
	auto existing (node->wallets.items.find (wallet_id));
	ASSERT_NE (node->wallets.items.end (), existing);
	{
		auto transaction (node->wallets.tx_begin_read ());
		nano::raw_key seed0;
		existing->second->store.seed (seed0, transaction);
		ASSERT_EQ (seed, seed0);
	}
	auto account_text (response.json.get<std::string> ("last_restored_account"));
	nano::account account;
	ASSERT_FALSE (account.decode_account (account_text));
	ASSERT_TRUE (existing->second->exists (account));
	ASSERT_EQ (pub, account);
	ASSERT_EQ ("1", response.json.get<std::string> ("restored_count"));
}

TEST (rpc, wallet_export)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_export");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string wallet_json (response.json.get<std::string> ("json"));
	bool error (false);
	scoped_thread_name_io.reset ();
	auto transaction (node->wallets.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store store (error, kdf, transaction, nano::genesis_account, 1, "0", wallet_json);
	ASSERT_FALSE (error);
	ASSERT_TRUE (store.exists (transaction, nano::dev_genesis_key.pub));
}

TEST (rpc, wallet_destroy)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	auto wallet_id (node->wallets.items.begin ()->first);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_destroy");
	request.put ("wallet", wallet_id.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ (node->wallets.items.end (), node->wallets.items.find (wallet_id));
}

TEST (rpc, account_move)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto wallet_id (node->wallets.items.begin ()->first);
	auto destination (system.wallet (0));
	destination->insert_adhoc (nano::dev_genesis_key.prv);
	nano::keypair key;
	auto source_id = nano::random_wallet_id ();
	auto source (node->wallets.create (source_id));
	source->insert_adhoc (key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_move");
	request.put ("wallet", wallet_id.to_string ());
	request.put ("source", source_id.to_string ());
	boost::property_tree::ptree keys;
	boost::property_tree::ptree entry;
	entry.put ("", key.pub.to_account ());
	keys.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", keys);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ ("1", response.json.get<std::string> ("moved"));
	ASSERT_TRUE (destination->exists (key.pub));
	ASSERT_TRUE (destination->exists (nano::dev_genesis_key.pub));
	auto transaction (node->wallets.tx_begin_read ());
	ASSERT_EQ (source->store.end (), source->store.begin (transaction));
}

TEST (rpc, block)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "block");
	request.put ("hash", node->latest (nano::genesis_account).to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto contents (response.json.get<std::string> ("contents"));
	ASSERT_FALSE (contents.empty ());
	ASSERT_TRUE (response.json.get<bool> ("confirmed")); // Genesis block is confirmed by default
}

TEST (rpc, block_account)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::genesis genesis;
	boost::property_tree::ptree request;
	request.put ("action", "block_account");
	request.put ("hash", genesis.hash ().to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string account_text (response.json.get<std::string> ("account"));
	nano::account account;
	ASSERT_FALSE (account.decode_account (account_text));
}

TEST (rpc, chain)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::keypair key;
	auto genesis (node->latest (nano::dev_genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, 1));
	ASSERT_NE (nullptr, block);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "chain");
	request.put ("block", block->hash ().to_string ());
	request.put ("count", std::to_string (std::numeric_limits<uint64_t>::max ()));
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & blocks_node (response.json.get_child ("blocks"));
	std::vector<nano::block_hash> blocks;
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (2, blocks.size ());
	ASSERT_EQ (block->hash (), blocks[0]);
	ASSERT_EQ (genesis, blocks[1]);
}

TEST (rpc, chain_limit)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::keypair key;
	auto genesis (node->latest (nano::dev_genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, 1));
	ASSERT_NE (nullptr, block);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "chain");
	request.put ("block", block->hash ().to_string ());
	request.put ("count", 1);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & blocks_node (response.json.get_child ("blocks"));
	std::vector<nano::block_hash> blocks;
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, blocks.size ());
	ASSERT_EQ (block->hash (), blocks[0]);
}

TEST (rpc, chain_offset)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::keypair key;
	auto genesis (node->latest (nano::dev_genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, 1));
	ASSERT_NE (nullptr, block);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "chain");
	request.put ("block", block->hash ().to_string ());
	request.put ("count", std::to_string (std::numeric_limits<uint64_t>::max ()));
	request.put ("offset", 1);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & blocks_node (response.json.get_child ("blocks"));
	std::vector<nano::block_hash> blocks;
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, blocks.size ());
	ASSERT_EQ (genesis, blocks[0]);
}

TEST (rpc, frontier)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	std::unordered_map<nano::account, nano::block_hash> source;
	{
		auto transaction (node->store.tx_begin_write ());
		for (auto i (0); i < 1000; ++i)
		{
			nano::keypair key;
			nano::block_hash hash;
			nano::random_pool::generate_block (hash.bytes.data (), hash.bytes.size ());
			source[key.pub] = hash;
			node->store.confirmation_height_put (transaction, key.pub, { 0, nano::block_hash (0) });
			node->store.account_put (transaction, key.pub, nano::account_info (hash, 0, 0, 0, 0, 0, nano::epoch::epoch_0));
		}
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::keypair key;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "frontiers");
	request.put ("account", nano::account (0).to_account ());
	request.put ("count", std::to_string (std::numeric_limits<uint64_t>::max ()));
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & frontiers_node (response.json.get_child ("frontiers"));
	std::unordered_map<nano::account, nano::block_hash> frontiers;
	for (auto i (frontiers_node.begin ()), j (frontiers_node.end ()); i != j; ++i)
	{
		nano::account account;
		account.decode_account (i->first);
		nano::block_hash frontier;
		frontier.decode_hex (i->second.get<std::string> (""));
		frontiers[account] = frontier;
	}
	ASSERT_EQ (1, frontiers.erase (nano::dev_genesis_key.pub));
	ASSERT_EQ (source, frontiers);
}

TEST (rpc, frontier_limited)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	std::unordered_map<nano::account, nano::block_hash> source;
	{
		auto transaction (node->store.tx_begin_write ());
		for (auto i (0); i < 1000; ++i)
		{
			nano::keypair key;
			nano::block_hash hash;
			nano::random_pool::generate_block (hash.bytes.data (), hash.bytes.size ());
			source[key.pub] = hash;
			node->store.confirmation_height_put (transaction, key.pub, { 0, nano::block_hash (0) });
			node->store.account_put (transaction, key.pub, nano::account_info (hash, 0, 0, 0, 0, 0, nano::epoch::epoch_0));
		}
	}

	scoped_io_thread_name_change scoped_thread_name_io;
	nano::keypair key;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "frontiers");
	request.put ("account", nano::account (0).to_account ());
	request.put ("count", std::to_string (100));
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & frontiers_node (response.json.get_child ("frontiers"));
	ASSERT_EQ (100, frontiers_node.size ());
}

TEST (rpc, frontier_startpoint)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	std::unordered_map<nano::account, nano::block_hash> source;
	{
		auto transaction (node->store.tx_begin_write ());
		for (auto i (0); i < 1000; ++i)
		{
			nano::keypair key;
			nano::block_hash hash;
			nano::random_pool::generate_block (hash.bytes.data (), hash.bytes.size ());
			source[key.pub] = hash;
			node->store.confirmation_height_put (transaction, key.pub, { 0, nano::block_hash (0) });
			node->store.account_put (transaction, key.pub, nano::account_info (hash, 0, 0, 0, 0, 0, nano::epoch::epoch_0));
		}
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::keypair key;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "frontiers");
	request.put ("account", source.begin ()->first.to_account ());
	request.put ("count", std::to_string (1));
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & frontiers_node (response.json.get_child ("frontiers"));
	ASSERT_EQ (1, frontiers_node.size ());
	ASSERT_EQ (source.begin ()->first.to_account (), frontiers_node.begin ()->first);
}

TEST (rpc, history)
{
	nano::system system;
	auto node0 = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto change (system.wallet (0)->change_action (nano::dev_genesis_key.pub, nano::dev_genesis_key.pub));
	ASSERT_NE (nullptr, change);
	auto send (system.wallet (0)->send_action (nano::dev_genesis_key.pub, nano::dev_genesis_key.pub, node0->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send);
	auto receive (system.wallet (0)->receive_action (*send, nano::dev_genesis_key.pub, node0->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, receive);
	nano::genesis genesis;
	nano::state_block usend (nano::genesis_account, node0->latest (nano::genesis_account), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node0->work_generate_blocking (node0->latest (nano::genesis_account)));
	nano::state_block ureceive (nano::genesis_account, usend.hash (), nano::genesis_account, nano::genesis_amount, usend.hash (), nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node0->work_generate_blocking (usend.hash ()));
	nano::state_block uchange (nano::genesis_account, ureceive.hash (), nano::keypair ().pub, nano::genesis_amount, 0, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node0->work_generate_blocking (ureceive.hash ()));
	{
		auto transaction (node0->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, usend).code);
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, ureceive).code);
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, uchange).code);
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node0, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node0->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "history");
	request.put ("hash", uchange.hash ().to_string ());
	request.put ("count", 100);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::vector<std::tuple<std::string, std::string, std::string, std::string>> history_l;
	auto & history_node (response.json.get_child ("history"));
	for (auto i (history_node.begin ()), n (history_node.end ()); i != n; ++i)
	{
		history_l.push_back (std::make_tuple (i->second.get<std::string> ("type"), i->second.get<std::string> ("account"), i->second.get<std::string> ("amount"), i->second.get<std::string> ("hash")));
	}
	ASSERT_EQ (5, history_l.size ());
	ASSERT_EQ ("receive", std::get<0> (history_l[0]));
	ASSERT_EQ (ureceive.hash ().to_string (), std::get<3> (history_l[0]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[0]));
	ASSERT_EQ (nano::Gxrb_ratio.convert_to<std::string> (), std::get<2> (history_l[0]));
	ASSERT_EQ (5, history_l.size ());
	ASSERT_EQ ("send", std::get<0> (history_l[1]));
	ASSERT_EQ (usend.hash ().to_string (), std::get<3> (history_l[1]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[1]));
	ASSERT_EQ (nano::Gxrb_ratio.convert_to<std::string> (), std::get<2> (history_l[1]));
	ASSERT_EQ ("receive", std::get<0> (history_l[2]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[2]));
	ASSERT_EQ (node0->config.receive_minimum.to_string_dec (), std::get<2> (history_l[2]));
	ASSERT_EQ (receive->hash ().to_string (), std::get<3> (history_l[2]));
	ASSERT_EQ ("send", std::get<0> (history_l[3]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[3]));
	ASSERT_EQ (node0->config.receive_minimum.to_string_dec (), std::get<2> (history_l[3]));
	ASSERT_EQ (send->hash ().to_string (), std::get<3> (history_l[3]));
	ASSERT_EQ ("receive", std::get<0> (history_l[4]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[4]));
	ASSERT_EQ (nano::genesis_amount.convert_to<std::string> (), std::get<2> (history_l[4]));
	ASSERT_EQ (genesis.hash ().to_string (), std::get<3> (history_l[4]));
}

TEST (rpc, account_history)
{
	nano::system system;
	auto node0 = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto change (system.wallet (0)->change_action (nano::dev_genesis_key.pub, nano::dev_genesis_key.pub));
	ASSERT_NE (nullptr, change);
	auto send (system.wallet (0)->send_action (nano::dev_genesis_key.pub, nano::dev_genesis_key.pub, node0->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send);
	auto receive (system.wallet (0)->receive_action (*send, nano::dev_genesis_key.pub, node0->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, receive);
	nano::genesis genesis;
	nano::state_block usend (nano::genesis_account, node0->latest (nano::genesis_account), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node0->work_generate_blocking (node0->latest (nano::genesis_account)));
	nano::state_block ureceive (nano::genesis_account, usend.hash (), nano::genesis_account, nano::genesis_amount, usend.hash (), nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node0->work_generate_blocking (usend.hash ()));
	nano::state_block uchange (nano::genesis_account, ureceive.hash (), nano::keypair ().pub, nano::genesis_amount, 0, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node0->work_generate_blocking (ureceive.hash ()));
	{
		auto transaction (node0->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, usend).code);
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, ureceive).code);
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, uchange).code);
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node0, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node0->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	{
		boost::property_tree::ptree request;
		request.put ("action", "account_history");
		request.put ("account", nano::genesis_account.to_account ());
		request.put ("count", 100);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string>> history_l;
		auto & history_node (response.json.get_child ("history"));
		for (auto i (history_node.begin ()), n (history_node.end ()); i != n; ++i)
		{
			history_l.push_back (std::make_tuple (i->second.get<std::string> ("type"), i->second.get<std::string> ("account"), i->second.get<std::string> ("amount"), i->second.get<std::string> ("hash"), i->second.get<std::string> ("height")));
		}

		ASSERT_EQ (5, history_l.size ());
		ASSERT_EQ ("receive", std::get<0> (history_l[0]));
		ASSERT_EQ (ureceive.hash ().to_string (), std::get<3> (history_l[0]));
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[0]));
		ASSERT_EQ (nano::Gxrb_ratio.convert_to<std::string> (), std::get<2> (history_l[0]));
		ASSERT_EQ ("6", std::get<4> (history_l[0])); // change block (height 7) is skipped by account_history since "raw" is not set
		ASSERT_EQ ("send", std::get<0> (history_l[1]));
		ASSERT_EQ (usend.hash ().to_string (), std::get<3> (history_l[1]));
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[1]));
		ASSERT_EQ (nano::Gxrb_ratio.convert_to<std::string> (), std::get<2> (history_l[1]));
		ASSERT_EQ ("5", std::get<4> (history_l[1]));
		ASSERT_EQ ("receive", std::get<0> (history_l[2]));
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[2]));
		ASSERT_EQ (node0->config.receive_minimum.to_string_dec (), std::get<2> (history_l[2]));
		ASSERT_EQ (receive->hash ().to_string (), std::get<3> (history_l[2]));
		ASSERT_EQ ("4", std::get<4> (history_l[2]));
		ASSERT_EQ ("send", std::get<0> (history_l[3]));
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[3]));
		ASSERT_EQ (node0->config.receive_minimum.to_string_dec (), std::get<2> (history_l[3]));
		ASSERT_EQ (send->hash ().to_string (), std::get<3> (history_l[3]));
		ASSERT_EQ ("3", std::get<4> (history_l[3]));
		ASSERT_EQ ("receive", std::get<0> (history_l[4]));
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[4]));
		ASSERT_EQ (nano::genesis_amount.convert_to<std::string> (), std::get<2> (history_l[4]));
		ASSERT_EQ (genesis.hash ().to_string (), std::get<3> (history_l[4]));
		ASSERT_EQ ("1", std::get<4> (history_l[4])); // change block (height 2) is skipped
	}
	// Test count and reverse
	{
		boost::property_tree::ptree request;
		request.put ("action", "account_history");
		request.put ("account", nano::genesis_account.to_account ());
		request.put ("reverse", true);
		request.put ("count", 1);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & history_node (response.json.get_child ("history"));
		ASSERT_EQ (1, history_node.size ());
		ASSERT_EQ ("1", history_node.begin ()->second.get<std::string> ("height"));
		ASSERT_EQ (change->hash ().to_string (), response.json.get<std::string> ("next"));
	}

	// Test filtering
	scoped_thread_name_io.reset ();
	auto account2 (system.wallet (0)->deterministic_insert ());
	auto send2 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, account2, node0->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send2);
	auto receive2 (system.wallet (0)->receive_action (*send2, account2, node0->config.receive_minimum.number ()));
	scoped_thread_name_io.renew ();
	// Test filter for send state blocks
	ASSERT_NE (nullptr, receive2);
	{
		boost::property_tree::ptree request;
		request.put ("action", "account_history");
		request.put ("account", nano::dev_genesis_key.pub.to_account ());
		boost::property_tree::ptree other_account;
		other_account.put ("", account2.to_account ());
		boost::property_tree::ptree filtered_accounts;
		filtered_accounts.push_back (std::make_pair ("", other_account));
		request.add_child ("account_filter", filtered_accounts);
		request.put ("count", 100);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		auto history_node (response.json.get_child ("history"));
		ASSERT_EQ (history_node.size (), 2);
	}
	// Test filter for receive state blocks
	{
		boost::property_tree::ptree request;
		request.put ("action", "account_history");
		request.put ("account", account2.to_account ());
		boost::property_tree::ptree other_account;
		other_account.put ("", nano::dev_genesis_key.pub.to_account ());
		boost::property_tree::ptree filtered_accounts;
		filtered_accounts.push_back (std::make_pair ("", other_account));
		request.add_child ("account_filter", filtered_accounts);
		request.put ("count", 100);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		auto history_node (response.json.get_child ("history"));
		ASSERT_EQ (history_node.size (), 1);
	}
}

TEST (rpc, history_count)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto change (system.wallet (0)->change_action (nano::dev_genesis_key.pub, nano::dev_genesis_key.pub));
	ASSERT_NE (nullptr, change);
	auto send (system.wallet (0)->send_action (nano::dev_genesis_key.pub, nano::dev_genesis_key.pub, node->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send);
	auto receive (system.wallet (0)->receive_action (*send, nano::dev_genesis_key.pub, node->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, receive);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "history");
	request.put ("hash", receive->hash ().to_string ());
	request.put ("count", 1);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & history_node (response.json.get_child ("history"));
	ASSERT_EQ (1, history_node.size ());
}

TEST (rpc, process_block)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::keypair key;
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_TIMELY (10s, node1.latest (nano::dev_genesis_key.pub) == send.hash ());
		std::string send_hash (response.json.get<std::string> ("hash"));
		ASSERT_EQ (send.hash ().to_string (), send_hash);
	}
	request.put ("json_block", true);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::error_code ec (nano::error_blocks::invalid_block);
		ASSERT_EQ (ec.message (), response.json.get<std::string> ("error"));
	}
}

TEST (rpc, process_json_block)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::keypair key;
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "process");
	boost::property_tree::ptree block_node;
	send.serialize_json (block_node);
	request.add_child ("block", block_node);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::error_code ec (nano::error_blocks::invalid_block);
		ASSERT_EQ (ec.message (), response.json.get<std::string> ("error"));
	}
	request.put ("json_block", true);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_TIMELY (10s, node1.latest (nano::dev_genesis_key.pub) == send.hash ());
		std::string send_hash (response.json.get<std::string> ("hash"));
		ASSERT_EQ (send.hash ().to_string (), send_hash);
	}
}

TEST (rpc, process_block_with_work_watcher)
{
	nano::system system;
	nano::node_config node_config;
	node_config.enable_voting = false;
	node_config.work_watcher_period = 1s;
	node_config.max_work_generate_multiplier = 1e6;
	auto & node1 = *add_ipc_enabled_node (system, node_config);
	nano::keypair key;
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	auto send (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, latest, nano::dev_genesis_key.pub, nano::genesis_amount - 100, nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (latest)));
	auto difficulty1 (send->difficulty ());
	auto multiplier1 = nano::normalized_multiplier (nano::difficulty::to_multiplier (difficulty1, node1.env.constants.network.publish_thresholds.epoch_1), node1.env.constants.network.publish_thresholds.epoch_1);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "process");
	request.put ("watch_work", true);
	std::string json;
	send->serialize_json (json);
	request.put ("block", json);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_TIMELY (10s, node1.latest (nano::dev_genesis_key.pub) == send->hash ());
	system.deadline_set (10s);
	auto updated (false);
	double updated_multiplier;
	while (!updated)
	{
		nano::unique_lock<std::mutex> lock (node1.active.mutex);
		//fill multipliers_cb and update active difficulty;
		for (auto i (0); i < node1.active.multipliers_cb.size (); i++)
		{
			node1.active.multipliers_cb.push_back (multiplier1 * (1 + i / 100.));
		}
		node1.active.update_active_multiplier (lock);
		auto const existing (node1.active.roots.find (send->qualified_root ()));
		//if existing is junk the block has been confirmed already
		ASSERT_NE (existing, node1.active.roots.end ());
		updated = existing->multiplier != multiplier1;
		updated_multiplier = existing->multiplier;
		lock.unlock ();
		ASSERT_NO_ERROR (system.poll ());
	}
	ASSERT_GT (updated_multiplier, multiplier1);

	// Try without enable_control which watch_work requires if set to true
	{
		nano::rpc_config rpc_config (nano::get_available_port (), false);
		rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
		nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
		nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
		rpc.start ();
		boost::property_tree::ptree request;
		request.put ("action", "process");
		request.put ("watch_work", true);
		std::string json;
		send->serialize_json (json);
		request.put ("block", json);
		{
			test_response response (request, rpc.config.port, system.env.ctx);
			ASSERT_TIMELY (5s, response.status != 0);
			ASSERT_EQ (200, response.status);
			std::error_code ec (nano::error_rpc::rpc_control_disabled);
			ASSERT_EQ (ec.message (), response.json.get<std::string> ("error"));
		}

		// Check no enable_control error message is present when not watching work
		request.put ("watch_work", false);
		{
			test_response response (request, rpc.config.port, system.env.ctx);
			ASSERT_TIMELY (5s, response.status != 0);
			ASSERT_EQ (200, response.status);
			std::error_code ec (nano::error_rpc::rpc_control_disabled);
			ASSERT_NE (ec.message (), response.json.get<std::string> ("error"));
		}
	}
}

TEST (rpc, process_block_no_work)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::keypair key;
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	send.block_work_set (0);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_FALSE (response.json.get<std::string> ("error", "").empty ());
}

TEST (rpc, process_republish)
{
	nano::system system (2);
	auto & node1 (*system.nodes[0]);
	auto & node2 (*system.nodes[1]);
	auto & node3 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::keypair key;
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node3.work_generate_blocking (latest));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node3, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node3.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_TIMELY (10s, node2.latest (nano::dev_genesis_key.pub) == send.hash ());
}

TEST (rpc, process_subtype_send)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	system.add_node ();
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::keypair key;
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::state_block send (nano::genesis_account, latest, nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	request.put ("subtype", "receive");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::error_code ec (nano::error_rpc::invalid_subtype_balance);
	ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "change");
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_EQ (response2.json.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "send");
	test_response response3 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response3.status != 0);
	ASSERT_EQ (200, response3.status);
	ASSERT_EQ (send.hash ().to_string (), response3.json.get<std::string> ("hash"));
	ASSERT_TIMELY (10s, system.nodes[1]->latest (nano::dev_genesis_key.pub) == send.hash ());
}

TEST (rpc, process_subtype_open)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	auto & node2 = *system.add_node ();
	nano::keypair key;
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::state_block send (nano::genesis_account, latest, nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	ASSERT_EQ (nano::process_result::progress, node1.process (send).code);
	ASSERT_EQ (nano::process_result::progress, node2.process (send).code);
	scoped_io_thread_name_change scoped_thread_name_io;
	node1.active.insert (std::make_shared<nano::state_block> (send));
	nano::state_block open (key.pub, 0, key.pub, nano::Gxrb_ratio, send.hash (), key.prv, key.pub, *node1.work_generate_blocking (key.pub));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	open.serialize_json (json);
	request.put ("block", json);
	request.put ("subtype", "send");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::error_code ec (nano::error_rpc::invalid_subtype_balance);
	ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "epoch");
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_EQ (response2.json.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "open");
	test_response response3 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response3.status != 0);
	ASSERT_EQ (200, response3.status);
	ASSERT_EQ (open.hash ().to_string (), response3.json.get<std::string> ("hash"));
	ASSERT_TIMELY (10s, node2.latest (key.pub) == open.hash ());
}

TEST (rpc, process_subtype_receive)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	auto & node2 = *system.add_node ();
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::state_block send (nano::genesis_account, latest, nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	ASSERT_EQ (nano::process_result::progress, node1.process (send).code);
	ASSERT_EQ (nano::process_result::progress, node2.process (send).code);
	scoped_io_thread_name_change scoped_thread_name_io;
	node1.active.insert (std::make_shared<nano::state_block> (send));
	nano::state_block receive (nano::dev_genesis_key.pub, send.hash (), nano::dev_genesis_key.pub, nano::genesis_amount, send.hash (), nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (send.hash ()));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	receive.serialize_json (json);
	request.put ("block", json);
	request.put ("subtype", "send");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::error_code ec (nano::error_rpc::invalid_subtype_balance);
	ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "open");
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ec = nano::error_rpc::invalid_subtype_previous;
	ASSERT_EQ (response2.json.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "receive");
	test_response response3 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response3.status != 0);
	ASSERT_EQ (200, response3.status);
	ASSERT_EQ (receive.hash ().to_string (), response3.json.get<std::string> ("hash"));
	ASSERT_TIMELY (10s, node2.latest (nano::dev_genesis_key.pub) == receive.hash ());
}

TEST (rpc, process_ledger_insufficient_work)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);
	ASSERT_LT (node.env.constants.network.publish_thresholds.entry, node.env.constants.network.publish_thresholds.epoch_1);
	auto latest (node.latest (nano::dev_genesis_key.pub));
	auto min_difficulty = node.env.constants.network.publish_thresholds.entry;
	auto max_difficulty = node.env.constants.network.publish_thresholds.epoch_1;
	nano::state_block send (nano::genesis_account, latest, nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, system.work_generate_limited (latest, min_difficulty, max_difficulty));
	ASSERT_LT (send.difficulty (), max_difficulty);
	ASSERT_GE (send.difficulty (), min_difficulty);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	request.put ("subtype", "send");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::error_code ec (nano::error_process::insufficient_work);
	ASSERT_EQ (1, response.json.count ("error"));
	ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
}

// Ensure that processing an old block with updated work floods it to peers
TEST (rpc, process_difficulty_update_flood)
{
	nano::system system (1);
	auto & node_passive = *system.nodes[0];
	auto & node = *add_ipc_enabled_node (system);

	auto latest (node.latest (nano::dev_genesis_key.pub));
	nano::state_block send (nano::genesis_account, latest, nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node.work_generate_blocking (latest));

	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);

	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();

	boost::property_tree::ptree request;
	request.put ("action", "process");
	// Must not watch work, otherwise the work watcher could update the block and flood it, whereas we want to ensure flooding happens on demand, without the work watcher
	request.put ("watch_work", false);
	{
		std::string json;
		send.serialize_json (json);
		request.put ("block", json);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (0, response.json.count ("error"));
	}

	ASSERT_TIMELY (5s, node_passive.active.size () == 1 && node_passive.block (send.hash ()) != nullptr);

	// Update block work
	node.work_generate_blocking (send, send.difficulty ());
	auto expected_multiplier = nano::normalized_multiplier (nano::difficulty::to_multiplier (send.difficulty (), nano::work_threshold (send.work_version (), nano::block_details (nano::epoch::epoch_0, true, false, false))), node.env.constants.network.publish_thresholds.epoch_1);

	{
		std::string json;
		send.serialize_json (json);
		request.put ("block", json);
		std::error_code ec (nano::error_process::old);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	}

	// Ensure the difficulty update occurs in both nodes
	ASSERT_NO_ERROR (system.poll_until_true (5s, [&node, &node_passive, &send, expected_multiplier] {
		nano::lock_guard<std::mutex> guard (node.active.mutex);
		auto const existing (node.active.roots.find (send.qualified_root ()));
		EXPECT_NE (existing, node.active.roots.end ());

		nano::lock_guard<std::mutex> guard_passive (node_passive.active.mutex);
		auto const existing_passive (node_passive.active.roots.find (send.qualified_root ()));
		EXPECT_NE (existing_passive, node_passive.active.roots.end ());

		bool updated = existing->multiplier == expected_multiplier;
		bool updated_passive = existing_passive->multiplier == expected_multiplier;

		return updated && updated_passive;
	}));
}

TEST (rpc, keepalive)
{
	nano::system system;
	auto node0 = add_ipc_enabled_node (system);
	auto node1 (std::make_shared<nano::node> (system.env, nano::node_config{}));
	node1->start ();
	system.nodes.push_back (node1);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node0, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node0->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "keepalive");
	auto address (boost::str (boost::format ("%1%") % node1->network.endpoint ().address ()));
	auto port (boost::str (boost::format ("%1%") % node1->network.endpoint ().port ()));
	request.put ("address", address);
	request.put ("port", port);
	ASSERT_EQ (nullptr, node0->network.udp_channels.channel (node1->network.endpoint ()));
	ASSERT_EQ (0, node0->network.size ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	system.deadline_set (10s);
	while (node0->network.find_channel (node1->network.endpoint ()) == nullptr)
	{
		ASSERT_EQ (0, node0->network.size ());
		ASSERT_NO_ERROR (system.poll ());
	}
	node1->stop ();
}

TEST (rpc, payment_init)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto wallet_id = nano::random_wallet_id ();
	auto wallet (node1->wallets.create (wallet_id));
	ASSERT_TRUE (node1->wallets.items.find (wallet_id) != node1->wallets.items.end ());
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "payment_init");
	request.put ("wallet", wallet_id.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ ("Ready", response.json.get<std::string> ("status"));
}

TEST (rpc, payment_begin_end)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto wallet_id = nano::random_wallet_id ();
	auto wallet (node1->wallets.create (wallet_id));
	ASSERT_TRUE (node1->wallets.items.find (wallet_id) != node1->wallets.items.end ());
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "payment_begin");
	request1.put ("wallet", wallet_id.to_string ());
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	auto account_text (response1.json.get<std::string> ("account"));
	nano::account account;
	ASSERT_FALSE (account.decode_account (account_text));
	ASSERT_TRUE (wallet->exists (account));
	nano::root root1;
	{
		auto transaction (node1->store.tx_begin_read ());
		root1 = node1->ledger.latest_root (transaction, account);
	}
	uint64_t work (0);
	while (nano::work_difficulty (nano::work_version::work_1, root1, work) >= nano::work_threshold_base (nano::work_version::work_1))
	{
		++work;
		ASSERT_LT (work, 50);
	}
	system.deadline_set (10s);
	while (nano::work_difficulty (nano::work_version::work_1, root1, work) < node1->default_difficulty (nano::work_version::work_1))
	{
		auto ec = system.poll ();
		auto transaction (wallet->wallets.tx_begin_read ());
		ASSERT_FALSE (wallet->store.work_get (transaction, account, work));
		ASSERT_NO_ERROR (ec);
	}
	ASSERT_EQ (wallet->free_accounts.end (), wallet->free_accounts.find (account));
	boost::property_tree::ptree request2;
	request2.put ("action", "payment_end");
	request2.put ("wallet", wallet_id.to_string ());
	request2.put ("account", account.to_account ());
	test_response response2 (request2, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_TRUE (wallet->exists (account));
	ASSERT_NE (wallet->free_accounts.end (), wallet->free_accounts.find (account));
	rpc.stop ();
	system.stop ();
}

TEST (rpc, payment_end_nonempty)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto transaction (node1->wallets.tx_begin_read ());
	system.wallet (0)->init_free_accounts (transaction);
	auto wallet_id (node1->wallets.items.begin ()->first);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "payment_end");
	request1.put ("wallet", wallet_id.to_string ());
	request1.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_FALSE (response1.json.get<std::string> ("error", "").empty ());
}

TEST (rpc, payment_zero_balance)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto transaction (node1->wallets.tx_begin_read ());
	system.wallet (0)->init_free_accounts (transaction);
	auto wallet_id (node1->wallets.items.begin ()->first);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "payment_begin");
	request1.put ("wallet", wallet_id.to_string ());
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	auto account_text (response1.json.get<std::string> ("account"));
	nano::account account;
	ASSERT_FALSE (account.decode_account (account_text));
	ASSERT_NE (nano::dev_genesis_key.pub, account);
}

TEST (rpc, payment_begin_reuse)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto wallet_id = nano::random_wallet_id ();
	auto wallet (node1->wallets.create (wallet_id));
	ASSERT_TRUE (node1->wallets.items.find (wallet_id) != node1->wallets.items.end ());
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "payment_begin");
	request1.put ("wallet", wallet_id.to_string ());
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	auto account_text (response1.json.get<std::string> ("account"));
	nano::account account;
	ASSERT_FALSE (account.decode_account (account_text));
	ASSERT_TRUE (wallet->exists (account));
	ASSERT_EQ (wallet->free_accounts.end (), wallet->free_accounts.find (account));
	boost::property_tree::ptree request2;
	request2.put ("action", "payment_end");
	request2.put ("wallet", wallet_id.to_string ());
	request2.put ("account", account.to_account ());
	test_response response2 (request2, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_TRUE (wallet->exists (account));
	ASSERT_NE (wallet->free_accounts.end (), wallet->free_accounts.find (account));
	test_response response3 (request1, rpc.config.port, system.env.ctx);
	system.deadline_set (5s);
	ASSERT_TIMELY (10s, response3.status != 0);
	ASSERT_EQ (200, response3.status);
	auto account2_text (response1.json.get<std::string> ("account"));
	nano::account account2;
	ASSERT_FALSE (account2.decode_account (account2_text));
	ASSERT_EQ (account, account2);
}

TEST (rpc, payment_begin_locked)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto wallet_id = nano::random_wallet_id ();
	auto wallet (node1->wallets.create (wallet_id));
	{
		auto transaction (wallet->wallets.tx_begin_write ());
		wallet->store.rekey (transaction, "1");
		ASSERT_TRUE (wallet->store.attempt_password (transaction, ""));
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	ASSERT_TRUE (node1->wallets.items.find (wallet_id) != node1->wallets.items.end ());
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "payment_begin");
	request1.put ("wallet", wallet_id.to_string ());
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_FALSE (response1.json.get<std::string> ("error", "").empty ());
}

TEST (rpc, payment_wait)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "payment_wait");
	request1.put ("account", key.pub.to_account ());
	request1.put ("amount", nano::amount (nano::Mxrb_ratio).to_string_dec ());
	request1.put ("timeout", "100");
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ ("nothing", response1.json.get<std::string> ("status"));
	request1.put ("timeout", "100000");
	scoped_thread_name_io.reset ();
	system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Mxrb_ratio);
	system.env.alarm.add (std::chrono::steady_clock::now () + std::chrono::milliseconds (500), [&]() {
		system.nodes.front ()->worker.push_task ([&]() {
			system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Mxrb_ratio);
		});
	});
	scoped_thread_name_io.renew ();
	test_response response2 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_EQ ("success", response2.json.get<std::string> ("status"));
	request1.put ("amount", nano::amount (nano::Mxrb_ratio * 2).to_string_dec ());
	test_response response3 (request1, rpc.config.port, system.env.ctx);
	system.deadline_set (5s);
	ASSERT_TIMELY (10s, response3.status != 0);
	ASSERT_EQ (200, response3.status);
	ASSERT_EQ ("success", response2.json.get<std::string> ("status"));
}

TEST (rpc, peers)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::node_config config;
	system.add_node ();
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::endpoint endpoint (boost::asio::ip::make_address_v6 ("fc00::1"), 4000);
	node->network.udp_channels.insert (endpoint, node->env.constants.protocol.protocol_version);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "peers");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & peers_node (response.json.get_child ("peers"));
	ASSERT_EQ (2, peers_node.size ());
	ASSERT_EQ (std::to_string (node->env.constants.protocol.protocol_version), peers_node.get<std::string> ((boost::format ("[::1]:%1%") % config.peering_port).str ()));
	// Previously "[::ffff:80.80.80.80]:4000", but IPv4 address cause "No such node thrown in the test body" issue with peers_node.get
	std::stringstream endpoint_text;
	endpoint_text << endpoint;
	ASSERT_EQ (std::to_string (node->env.constants.protocol.protocol_version), peers_node.get<std::string> (endpoint_text.str ()));
}

TEST (rpc, peers_node_id)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::node_config config;
	system.add_node (config);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::endpoint endpoint (boost::asio::ip::make_address_v6 ("fc00::1"), 4000);
	node->network.udp_channels.insert (endpoint, node->env.constants.protocol.protocol_version);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "peers");
	request.put ("peer_details", true);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & peers_node (response.json.get_child ("peers"));
	ASSERT_EQ (2, peers_node.size ());
	auto tree1 (peers_node.get_child ((boost::format ("[::1]:%1%") % config.peering_port).str ()));
	ASSERT_EQ (std::to_string (node->env.constants.protocol.protocol_version), tree1.get<std::string> ("protocol_version"));
	ASSERT_EQ (system.nodes[1]->node_id.pub.to_node_id (), tree1.get<std::string> ("node_id"));
	std::stringstream endpoint_text;
	endpoint_text << endpoint;
	auto tree2 (peers_node.get_child (endpoint_text.str ()));
	ASSERT_EQ (std::to_string (node->env.constants.protocol.protocol_version), tree2.get<std::string> ("protocol_version"));
	ASSERT_EQ ("", tree2.get<std::string> ("node_id"));
}

TEST (rpc, pending)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key1;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto block1 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key1.pub, 100));
	scoped_io_thread_name_change scoped_thread_name_io;
	ASSERT_TIMELY (5s, !node->active.active (*block1));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "pending");
	request.put ("account", key1.pub.to_account ());
	request.put ("count", "100");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & blocks_node (response.json.get_child ("blocks"));
		ASSERT_EQ (1, blocks_node.size ());
		nano::block_hash hash (blocks_node.begin ()->second.get<std::string> (""));
		ASSERT_EQ (block1->hash (), hash);
	}
	request.put ("sorting", "true"); // Sorting test
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & blocks_node (response.json.get_child ("blocks"));
		ASSERT_EQ (1, blocks_node.size ());
		nano::block_hash hash (blocks_node.begin ()->first);
		ASSERT_EQ (block1->hash (), hash);
		std::string amount (blocks_node.begin ()->second.get<std::string> (""));
		ASSERT_EQ ("100", amount);
	}
	request.put ("threshold", "100"); // Threshold test
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & blocks_node (response.json.get_child ("blocks"));
		ASSERT_EQ (1, blocks_node.size ());
		std::unordered_map<nano::block_hash, nano::uint128_union> blocks;
		for (auto i (blocks_node.begin ()), j (blocks_node.end ()); i != j; ++i)
		{
			nano::block_hash hash;
			hash.decode_hex (i->first);
			nano::uint128_union amount;
			amount.decode_dec (i->second.get<std::string> (""));
			blocks[hash] = amount;
			boost::optional<std::string> source (i->second.get_optional<std::string> ("source"));
			ASSERT_FALSE (source.is_initialized ());
			boost::optional<uint8_t> min_version (i->second.get_optional<uint8_t> ("min_version"));
			ASSERT_FALSE (min_version.is_initialized ());
		}
		ASSERT_EQ (blocks[block1->hash ()], 100);
	}
	request.put ("threshold", "101");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & blocks_node (response.json.get_child ("blocks"));
		ASSERT_EQ (0, blocks_node.size ());
	}
	request.put ("threshold", "0");
	request.put ("source", "true");
	request.put ("min_version", "true");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & blocks_node (response.json.get_child ("blocks"));
		ASSERT_EQ (1, blocks_node.size ());
		std::unordered_map<nano::block_hash, nano::uint128_union> amounts;
		std::unordered_map<nano::block_hash, nano::account> sources;
		for (auto i (blocks_node.begin ()), j (blocks_node.end ()); i != j; ++i)
		{
			nano::block_hash hash;
			hash.decode_hex (i->first);
			amounts[hash].decode_dec (i->second.get<std::string> ("amount"));
			sources[hash].decode_account (i->second.get<std::string> ("source"));
			ASSERT_EQ (i->second.get<uint8_t> ("min_version"), 0);
		}
		ASSERT_EQ (amounts[block1->hash ()], 100);
		ASSERT_EQ (sources[block1->hash ()], nano::dev_genesis_key.pub);
	}

	request.put ("account", key1.pub.to_account ());
	request.put ("source", "false");
	request.put ("min_version", "false");

	auto check_block_response_count = [&system, &request, &rpc](size_t size) {
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);

		ASSERT_EQ (200, response.status);
		ASSERT_EQ (size, response.json.get_child ("blocks").size ());
	};

	request.put ("include_only_confirmed", "true");
	check_block_response_count (1);
	scoped_thread_name_io.reset ();
	reset_confirmation_height (system.nodes.front ()->store, block1->account ());
	scoped_thread_name_io.renew ();
	check_block_response_count (0);
}

TEST (rpc, pending_burn)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::account burn (0);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto block1 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, burn, 100));
	scoped_io_thread_name_change scoped_thread_name_io;
	ASSERT_TIMELY (5s, !node->active.active (*block1));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "pending");
	request.put ("account", burn.to_account ());
	request.put ("count", "100");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & blocks_node (response.json.get_child ("blocks"));
		ASSERT_EQ (1, blocks_node.size ());
		nano::block_hash hash (blocks_node.begin ()->second.get<std::string> (""));
		ASSERT_EQ (block1->hash (), hash);
	}
}

TEST (rpc, search_pending)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto wallet (node->wallets.items.begin ()->first.to_string ());
	auto latest (node->latest (nano::dev_genesis_key.pub));
	nano::send_block block (latest, nano::dev_genesis_key.pub, nano::genesis_amount - node->config.receive_minimum.number (), nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node->work_generate_blocking (latest));
	{
		auto transaction (node->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, block).code);
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "search_pending");
	request.put ("wallet", wallet);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_TIMELY (10s, node->balance (nano::dev_genesis_key.pub) == nano::genesis_amount);
}

TEST (rpc, version)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "version");
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ ("1", response1.json.get<std::string> ("rpc_version"));
	ASSERT_EQ (200, response1.status);
	{
		auto transaction (node1->store.tx_begin_read ());
		ASSERT_EQ (std::to_string (node1->store.version_get (transaction)), response1.json.get<std::string> ("store_version"));
	}
	ASSERT_EQ (std::to_string (node1->env.constants.protocol.protocol_version), response1.json.get<std::string> ("protocol_version"));
	ASSERT_EQ (boost::str (boost::format ("Nano %1%") % NANO_VERSION_STRING), response1.json.get<std::string> ("node_vendor"));
	ASSERT_EQ (node1->store.vendor_get (), response1.json.get<std::string> ("store_vendor"));
	auto network_label (node1->env.constants.network.get_current_network_as_string ());
	ASSERT_EQ (network_label, response1.json.get<std::string> ("network"));
	auto genesis_open (node1->latest (nano::dev_genesis_key.pub));
	ASSERT_EQ (genesis_open.to_string (), response1.json.get<std::string> ("network_identifier"));
	ASSERT_EQ (BUILD_INFO, response1.json.get<std::string> ("build_info"));
	auto headers (response1.resp.base ());
	auto allow (headers.at ("Allow"));
	auto content_type (headers.at ("Content-Type"));
	auto access_control_allow_origin (headers.at ("Access-Control-Allow-Origin"));
	auto access_control_allow_methods (headers.at ("Access-Control-Allow-Methods"));
	auto access_control_allow_headers (headers.at ("Access-Control-Allow-Headers"));
	auto connection (headers.at ("Connection"));
	ASSERT_EQ ("POST, OPTIONS", allow);
	ASSERT_EQ ("application/json", content_type);
	ASSERT_EQ ("*", access_control_allow_origin);
	ASSERT_EQ (allow, access_control_allow_methods);
	ASSERT_EQ ("Accept, Accept-Language, Content-Language, Content-Type", access_control_allow_headers);
	ASSERT_EQ ("close", connection);
}

TEST (rpc, work_generate)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::block_hash hash (1);
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	auto verify_response = [node, &rpc, &system](auto & request, auto & hash) {
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (hash.to_string (), response.json.get<std::string> ("hash"));
		auto work_text (response.json.get<std::string> ("work"));
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (work_text, work));
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		auto response_difficulty_text (response.json.get<std::string> ("difficulty"));
		uint64_t response_difficulty;
		ASSERT_FALSE (nano::from_string_hex (response_difficulty_text, response_difficulty));
		ASSERT_EQ (result_difficulty, response_difficulty);
		auto multiplier = response.json.get<double> ("multiplier");
		ASSERT_NEAR (nano::difficulty::to_multiplier (result_difficulty, node->default_difficulty (nano::work_version::work_1)), multiplier, 1e-6);
	};
	verify_response (request, hash);
	request.put ("use_peers", "true");
	verify_response (request, hash);
}

TEST (rpc, work_generate_difficulty)
{
	nano::system system;
	nano::node_config node_config;
	node_config.max_work_generate_multiplier = 1000;
	auto node = add_ipc_enabled_node (system, node_config);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::block_hash hash (1);
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	{
		uint64_t difficulty (0xfff0000000000000);
		request.put ("difficulty", nano::to_string_hex (difficulty));
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto work_text (response.json.get<std::string> ("work"));
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (work_text, work));
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		auto response_difficulty_text (response.json.get<std::string> ("difficulty"));
		uint64_t response_difficulty;
		ASSERT_FALSE (nano::from_string_hex (response_difficulty_text, response_difficulty));
		ASSERT_EQ (result_difficulty, response_difficulty);
		auto multiplier = response.json.get<double> ("multiplier");
		// Expected multiplier from base threshold, not from the given difficulty
		ASSERT_NEAR (nano::difficulty::to_multiplier (result_difficulty, node->default_difficulty (nano::work_version::work_1)), multiplier, 1e-10);
		ASSERT_GE (result_difficulty, difficulty);
	}
	{
		uint64_t difficulty (0xffff000000000000);
		request.put ("difficulty", nano::to_string_hex (difficulty));
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (20s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto work_text (response.json.get<std::string> ("work"));
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (work_text, work));
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		ASSERT_GE (result_difficulty, difficulty);
	}
	{
		uint64_t difficulty (node->max_work_generate_difficulty (nano::work_version::work_1) + 1);
		request.put ("difficulty", nano::to_string_hex (difficulty));
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::error_code ec (nano::error_rpc::difficulty_limit);
		ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, work_generate_multiplier)
{
	nano::system system;
	nano::node_config node_config;
	node_config.max_work_generate_multiplier = 100;
	auto node = add_ipc_enabled_node (system, node_config);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::block_hash hash (1);
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	{
		// When both difficulty and multiplier are given, should use multiplier
		// Give base difficulty and very high multiplier to test
		request.put ("difficulty", nano::to_string_hex (0xff00000000000000));
		double multiplier{ 100.0 };
		request.put ("multiplier", multiplier);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto work_text (response.json.get_optional<std::string> ("work"));
		ASSERT_TRUE (work_text.is_initialized ());
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (*work_text, work));
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		auto response_difficulty_text (response.json.get<std::string> ("difficulty"));
		uint64_t response_difficulty;
		ASSERT_FALSE (nano::from_string_hex (response_difficulty_text, response_difficulty));
		ASSERT_EQ (result_difficulty, response_difficulty);
		auto result_multiplier = response.json.get<double> ("multiplier");
		ASSERT_GE (result_multiplier, multiplier);
	}
	{
		request.put ("multiplier", -1.5);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::error_code ec (nano::error_rpc::bad_multiplier_format);
		ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	}
	{
		double max_multiplier (nano::difficulty::to_multiplier (node->max_work_generate_difficulty (nano::work_version::work_1), node->default_difficulty (nano::work_version::work_1)));
		request.put ("multiplier", max_multiplier + 1);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::error_code ec (nano::error_rpc::difficulty_limit);
		ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, work_generate_epoch_2)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto epoch1 = system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1);
	ASSERT_NE (nullptr, epoch1);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	auto verify_response = [node, &rpc, &system](auto & request, nano::block_hash const & hash, uint64_t & out_difficulty) {
		request.put ("hash", hash.to_string ());
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto work_text (response.json.get<std::string> ("work"));
		uint64_t work{ 0 };
		ASSERT_FALSE (nano::from_string_hex (work_text, work));
		out_difficulty = nano::work_difficulty (nano::work_version::work_1, hash, work);
	};
	request.put ("action", "work_generate");
	// Before upgrading to epoch 2 should use epoch_1 difficulty as default
	{
		unsigned const max_tries = 30;
		uint64_t difficulty{ 0 };
		unsigned tries = 0;
		while (++tries < max_tries)
		{
			verify_response (request, epoch1->hash (), difficulty);
			if (difficulty < node->env.constants.network.publish_thresholds.base)
			{
				break;
			}
		}
		ASSERT_LT (tries, max_tries);
	}
	// After upgrading, should always use the higher difficulty by default
	ASSERT_EQ (node->env.constants.network.publish_thresholds.epoch_2, node->env.constants.network.publish_thresholds.base);
	scoped_thread_name_io.reset ();
	auto epoch2 = system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2);
	ASSERT_NE (nullptr, epoch2);
	scoped_thread_name_io.renew ();
	{
		for (auto i = 0; i < 5; ++i)
		{
			uint64_t difficulty{ 0 };
			verify_response (request, epoch1->hash (), difficulty);
			ASSERT_GE (difficulty, node->env.constants.network.publish_thresholds.base);
		}
	}
}

TEST (rpc, work_generate_block_high)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::keypair key;
	nano::state_block block (key.pub, 0, nano::dev_genesis_key.pub, nano::Gxrb_ratio, 123, key.prv, key.pub, *node->work_generate_blocking (key.pub));
	nano::block_hash hash (block.root ());
	auto block_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, block.block_work ()));
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	request.put ("json_block", "true");
	boost::property_tree::ptree json;
	block.serialize_json (json);
	request.add_child ("block", json);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (1, response.json.count ("error"));
		ASSERT_EQ (std::error_code (nano::error_rpc::block_work_enough).message (), response.json.get<std::string> ("error"));
	}
}

TEST (rpc, work_generate_block_low)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::keypair key;
	nano::state_block block (key.pub, 0, nano::dev_genesis_key.pub, nano::Gxrb_ratio, 123, key.prv, key.pub, 0);
	auto threshold (node->default_difficulty (block.work_version ()));
	block.block_work_set (system.work_generate_limited (block.root (), threshold, nano::difficulty::from_multiplier (node->config.max_work_generate_multiplier / 10, threshold)));
	nano::block_hash hash (block.root ());
	auto block_difficulty (block.difficulty ());
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	request.put ("difficulty", nano::to_string_hex (block_difficulty + 1));
	request.put ("json_block", "false");
	std::string json;
	block.serialize_json (json);
	request.put ("block", json);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto work_text (response.json.get_optional<std::string> ("work"));
		ASSERT_TRUE (work_text.is_initialized ());
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (*work_text, work));
		ASSERT_NE (block.block_work (), work);
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		auto response_difficulty_text (response.json.get<std::string> ("difficulty"));
		uint64_t response_difficulty;
		ASSERT_FALSE (nano::from_string_hex (response_difficulty_text, response_difficulty));
		ASSERT_EQ (result_difficulty, response_difficulty);
		ASSERT_LT (block_difficulty, result_difficulty);
	}
}

TEST (rpc, work_generate_block_root_mismatch)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::keypair key;
	nano::state_block block (key.pub, 0, nano::dev_genesis_key.pub, nano::Gxrb_ratio, 123, key.prv, key.pub, *node->work_generate_blocking (key.pub));
	nano::block_hash hash (1);
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	request.put ("json_block", "false");
	std::string json;
	block.serialize_json (json);
	request.put ("block", json);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (1, response.json.count ("error"));
		ASSERT_EQ (std::error_code (nano::error_rpc::block_root_mismatch).message (), response.json.get<std::string> ("error"));
	}
}

TEST (rpc, work_generate_block_ledger_epoch_2)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto epoch1 = system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1);
	ASSERT_NE (nullptr, epoch1);
	auto epoch2 = system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2);
	ASSERT_NE (nullptr, epoch2);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto send_block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::state_block block (key.pub, 0, nano::dev_genesis_key.pub, nano::Gxrb_ratio, send_block->hash (), key.prv, key.pub, 0);
	auto threshold (nano::work_threshold (block.work_version (), nano::block_details (nano::epoch::epoch_2, false, true, false)));
	block.block_work_set (system.work_generate_limited (block.root (), 1, threshold - 1));
	nano::block_hash hash (block.root ());
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	request.put ("json_block", "false");
	std::string json;
	block.serialize_json (json);
	request.put ("block", json);
	bool finished (false);
	auto iteration (0);
	while (!finished)
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto work_text (response.json.get_optional<std::string> ("work"));
		ASSERT_TRUE (work_text.is_initialized ());
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (*work_text, work));
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		auto response_difficulty_text (response.json.get<std::string> ("difficulty"));
		uint64_t response_difficulty;
		ASSERT_FALSE (nano::from_string_hex (response_difficulty_text, response_difficulty));
		ASSERT_EQ (result_difficulty, response_difficulty);
		ASSERT_GE (result_difficulty, node->env.constants.network.publish_thresholds.epoch_2_receive);
		finished = result_difficulty < node->env.constants.network.publish_thresholds.epoch_1;
		ASSERT_LT (++iteration, 200);
	}
}

TEST (rpc, work_cancel)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::block_hash hash1 (1);
	boost::property_tree::ptree request1;
	request1.put ("action", "work_cancel");
	request1.put ("hash", hash1.to_string ());
	std::atomic<bool> done (false);
	system.deadline_set (10s);
	while (!done)
	{
		system.env.work.generate (nano::work_version::work_1, hash1, node1.env.constants.network.publish_thresholds.base, [&done](boost::optional<uint64_t> work_a) {
			done = !work_a;
		});
		test_response response1 (request1, rpc.config.port, system.env.ctx);
		std::error_code ec;
		ASSERT_TIMELY (10s, response1.status != 0);
		ASSERT_EQ (200, response1.status);
		ASSERT_NO_ERROR (ec);
	}
}

TEST (rpc, work_peer_bad)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	auto & node2 = *system.add_node ();
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	node2.config.work_peers.push_back (std::make_pair (boost::asio::ip::address_v6::any ().to_string (), 0));
	nano::block_hash hash1 (1);
	std::atomic<uint64_t> work (0);
	node2.work_generate (nano::work_version::work_1, hash1, node2.env.constants.network.publish_thresholds.base, [&work](boost::optional<uint64_t> work_a) {
		ASSERT_TRUE (work_a.is_initialized ());
		work = *work_a;
	});
	ASSERT_TIMELY (5s, nano::work_difficulty (nano::work_version::work_1, hash1, work) >= nano::work_threshold_base (nano::work_version::work_1));
}

TEST (rpc, work_peer_one)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	auto & node2 = *system.add_node ();
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	node2.config.work_peers.push_back (std::make_pair (node1.network.endpoint ().address ().to_string (), rpc.config.port));
	nano::keypair key1;
	std::atomic<uint64_t> work (0);
	node2.work_generate (nano::work_version::work_1, key1.pub, node1.env.constants.network.publish_thresholds.base, [&work](boost::optional<uint64_t> work_a) {
		ASSERT_TRUE (work_a.is_initialized ());
		work = *work_a;
	});
	ASSERT_TIMELY (5s, nano::work_difficulty (nano::work_version::work_1, key1.pub, work) >= nano::work_threshold_base (nano::work_version::work_1));
}

TEST (rpc, work_peer_many)
{
	nano::system system1 (1);
	nano::system system2;
	nano::system system3 (1);
	nano::system system4 (1);
	auto & node1 (*system1.nodes[0]);
	auto & node2 = *add_ipc_enabled_node (system2);
	auto & node3 = *add_ipc_enabled_node (system3);
	auto & node4 = *add_ipc_enabled_node (system4);
	nano::keypair key;
	nano::rpc_config config2 (nano::get_available_port (), true);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server2 (node2, node_rpc_config);
	nano::ipc_rpc_processor ipc_rpc_processor2 (system2.env.ctx, config2);
	nano::rpc rpc2 (system2.env.ctx, config2, ipc_rpc_processor2);
	rpc2.start ();
	nano::rpc_config config3 (nano::get_available_port (), true);
	nano::ipc::ipc_server ipc_server3 (node3, node_rpc_config);
	nano::ipc_rpc_processor ipc_rpc_processor3 (system3.env.ctx, config3);
	nano::rpc rpc3 (system3.env.ctx, config3, ipc_rpc_processor3);
	rpc3.start ();
	nano::rpc_config config4 (nano::get_available_port (), true);
	nano::ipc::ipc_server ipc_server4 (node4, node_rpc_config);
	nano::ipc_rpc_processor ipc_rpc_processor4 (system4.env.ctx, config4);
	nano::rpc rpc4 (system2.env.ctx, config4, ipc_rpc_processor4);
	rpc4.start ();
	node1.config.work_peers.push_back (std::make_pair (node2.network.endpoint ().address ().to_string (), rpc2.config.port));
	node1.config.work_peers.push_back (std::make_pair (node3.network.endpoint ().address ().to_string (), rpc3.config.port));
	node1.config.work_peers.push_back (std::make_pair (node4.network.endpoint ().address ().to_string (), rpc4.config.port));

	std::array<std::atomic<uint64_t>, 10> works;
	for (auto i (0); i < works.size (); ++i)
	{
		nano::keypair key1;
		node1.work_generate (nano::work_version::work_1, key1.pub, node1.env.constants.network.publish_thresholds.base, [& work = works[i]](boost::optional<uint64_t> work_a) {
			work = *work_a;
		});
		while (nano::work_difficulty (nano::work_version::work_1, key1.pub, works[i]) < nano::work_threshold_base (nano::work_version::work_1))
		{
			system1.poll ();
			system2.poll ();
			system3.poll ();
			system4.poll ();
		}
	}
	node1.stop ();
}

TEST (rpc, work_version_invalid)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::block_hash hash (1);
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	request.put ("version", "work_invalid");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (1, response.json.count ("error"));
		ASSERT_EQ (std::error_code (nano::error_rpc::bad_work_version).message (), response.json.get<std::string> ("error"));
	}
	request.put ("action", "work_validate");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (1, response.json.count ("error"));
		ASSERT_EQ (std::error_code (nano::error_rpc::bad_work_version).message (), response.json.get<std::string> ("error"));
	}
}

TEST (rpc, block_count)
{
	{
		nano::system system;
		auto & node1 = *add_ipc_enabled_node (system);
		scoped_io_thread_name_change scoped_thread_name_io;
		nano::node_rpc_config node_rpc_config;
		nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
		nano::rpc_config rpc_config (nano::get_available_port (), true);
		rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
		nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
		nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
		rpc.start ();
		boost::property_tree::ptree request1;
		request1.put ("action", "block_count");
		{
			test_response response1 (request1, rpc.config.port, system.env.ctx);
			ASSERT_TIMELY (10s, response1.status != 0);
			ASSERT_EQ (200, response1.status);
			ASSERT_EQ ("1", response1.json.get<std::string> ("count"));
			ASSERT_EQ ("0", response1.json.get<std::string> ("unchecked"));
			ASSERT_EQ ("1", response1.json.get<std::string> ("cemented"));
		}
	}

	// Should be able to get all counts even when enable_control is false.
	{
		nano::system system;
		auto & node1 = *add_ipc_enabled_node (system);
		nano::node_rpc_config node_rpc_config;
		nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
		nano::rpc_config rpc_config;
		rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
		nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
		nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
		rpc.start ();
		boost::property_tree::ptree request1;
		request1.put ("action", "block_count");
		{
			test_response response1 (request1, rpc.config.port, system.env.ctx);
			ASSERT_TIMELY (10s, response1.status != 0);
			ASSERT_EQ (200, response1.status);
			ASSERT_EQ ("1", response1.json.get<std::string> ("count"));
			ASSERT_EQ ("0", response1.json.get<std::string> ("unchecked"));
			ASSERT_EQ ("1", response1.json.get<std::string> ("cemented"));
		}
	}
}

TEST (rpc, frontier_count)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "frontier_count");
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ ("1", response1.json.get<std::string> ("count"));
}

TEST (rpc, account_count)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "account_count");
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ ("1", response1.json.get<std::string> ("count"));
}

TEST (rpc, available_supply)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "available_supply");
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ ("0", response1.json.get<std::string> ("available"));
	scoped_thread_name_io.reset ();
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::keypair key;
	auto block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, 1));
	scoped_thread_name_io.renew ();
	test_response response2 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_EQ ("1", response2.json.get<std::string> ("available"));
	scoped_thread_name_io.reset ();
	auto block2 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, 0, 100)); // Sending to burning 0 account
	scoped_thread_name_io.renew ();
	test_response response3 (request1, rpc.config.port, system.env.ctx);
	system.deadline_set (5s);
	ASSERT_TIMELY (10s, response3.status != 0);
	ASSERT_EQ (200, response3.status);
	ASSERT_EQ ("1", response3.json.get<std::string> ("available"));
}

TEST (rpc, mrai_to_raw)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "mrai_to_raw");
	request1.put ("amount", "1");
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ (nano::Mxrb_ratio.convert_to<std::string> (), response1.json.get<std::string> ("amount"));
}

TEST (rpc, mrai_from_raw)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "mrai_from_raw");
	request1.put ("amount", nano::Mxrb_ratio.convert_to<std::string> ());
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ ("1", response1.json.get<std::string> ("amount"));
}

TEST (rpc, krai_to_raw)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "krai_to_raw");
	request1.put ("amount", "1");
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ (nano::kxrb_ratio.convert_to<std::string> (), response1.json.get<std::string> ("amount"));
}

TEST (rpc, krai_from_raw)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "krai_from_raw");
	request1.put ("amount", nano::kxrb_ratio.convert_to<std::string> ());
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ ("1", response1.json.get<std::string> ("amount"));
}

TEST (rpc, nano_to_raw)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "nano_to_raw");
	request1.put ("amount", "1");
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ (nano::xrb_ratio.convert_to<std::string> (), response1.json.get<std::string> ("amount"));
}

TEST (rpc, nano_from_raw)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request1;
	request1.put ("action", "nano_from_raw");
	request1.put ("amount", nano::xrb_ratio.convert_to<std::string> ());
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ ("1", response1.json.get<std::string> ("amount"));
}

TEST (rpc, account_representative)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("account", nano::genesis_account.to_account ());
	request.put ("action", "account_representative");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string account_text1 (response.json.get<std::string> ("representative"));
	ASSERT_EQ (account_text1, nano::genesis_account.to_account ());
}

TEST (rpc, account_representative_set)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	nano::keypair rep;
	request.put ("account", nano::genesis_account.to_account ());
	request.put ("representative", rep.pub.to_account ());
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("action", "account_representative_set");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string block_text1 (response.json.get<std::string> ("block"));
	nano::block_hash hash;
	ASSERT_FALSE (hash.decode_hex (block_text1));
	ASSERT_FALSE (hash.is_zero ());
	auto transaction (node->store.tx_begin_read ());
	ASSERT_TRUE (node->store.block_exists (transaction, hash));
	ASSERT_EQ (rep.pub, node->store.block_get (transaction, hash)->representative ());
}

TEST (rpc, account_representative_set_work_disabled)
{
	nano::system system;
	nano::node_config node_config;
	node_config.work_threads = 0;
	auto & node = *add_ipc_enabled_node (system, node_config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	nano::keypair rep;
	request.put ("account", nano::genesis_account.to_account ());
	request.put ("representative", rep.pub.to_account ());
	request.put ("wallet", node.wallets.items.begin ()->first.to_string ());
	request.put ("action", "account_representative_set");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_common::disabled_work_generation).message (), response.json.get<std::string> ("error"));
	}
}

TEST (rpc, account_representative_set_epoch_2)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);

	// Upgrade the genesis account to epoch 2
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (node, nano::epoch::epoch_1));
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (node, nano::epoch::epoch_2));

	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv, false);

	auto target_difficulty = nano::work_threshold (nano::work_version::work_1, nano::block_details (nano::epoch::epoch_2, false, false, false));
	ASSERT_LT (node.env.constants.network.publish_thresholds.entry, target_difficulty);
	auto min_difficulty = node.env.constants.network.publish_thresholds.entry;

	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node.wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "account_representative_set");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	request.put ("representative", nano::keypair ().pub.to_account ());

	// Test that the correct error is given if there is insufficient work
	auto insufficient = system.work_generate_limited (nano::genesis_hash, min_difficulty, target_difficulty);
	request.put ("work", nano::to_string_hex (insufficient));
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::error_code ec (nano::error_common::invalid_work);
		ASSERT_EQ (1, response.json.count ("error"));
		ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, bootstrap)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	nano::system system1 (1);
	auto latest (system1.nodes[0]->latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, nano::genesis_account, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system1.nodes[0]->work_generate_blocking (latest));
	{
		auto transaction (system1.nodes[0]->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, system1.nodes[0]->ledger.process (transaction, send).code);
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system0.env.ctx, rpc_config);
	nano::rpc rpc (system0.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "bootstrap");
	request.put ("address", "::ffff:127.0.0.1");
	request.put ("port", system1.nodes[0]->network.endpoint ().port ());
	test_response response (request, rpc.config.port, system0.env.ctx);
	while (response.status == 0)
	{
		system0.poll ();
	}
	system1.deadline_set (10s);
	while (system0.nodes[0]->latest (nano::genesis_account) != system1.nodes[0]->latest (nano::genesis_account))
	{
		ASSERT_NO_ERROR (system0.poll ());
		ASSERT_NO_ERROR (system1.poll ());
	}
}

TEST (rpc, account_remove)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	auto key1 (system0.wallet (0)->deterministic_insert ());
	scoped_io_thread_name_change scoped_thread_name_io;
	ASSERT_TRUE (system0.wallet (0)->exists (key1));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system0.env.ctx, rpc_config);
	nano::rpc rpc (system0.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_remove");
	request.put ("wallet", system0.nodes[0]->wallets.items.begin ()->first.to_string ());
	request.put ("account", key1.to_account ());
	test_response response (request, rpc.config.port, system0.env.ctx);
	while (response.status == 0)
	{
		system0.poll ();
	}
	ASSERT_FALSE (system0.wallet (0)->exists (key1));
}

TEST (rpc, representatives)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system0.env.ctx, rpc_config);
	nano::rpc rpc (system0.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "representatives");
	test_response response (request, rpc.config.port, system0.env.ctx);
	while (response.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response.status);
	auto & representatives_node (response.json.get_child ("representatives"));
	std::vector<nano::account> representatives;
	for (auto i (representatives_node.begin ()), n (representatives_node.end ()); i != n; ++i)
	{
		nano::account account;
		ASSERT_FALSE (account.decode_account (i->first));
		representatives.push_back (account);
	}
	ASSERT_EQ (1, representatives.size ());
	ASSERT_EQ (nano::genesis_account, representatives[0]);
}

// wallet_seed is only available over IPC's unsafe encoding, and when running on test network
TEST (rpc, wallet_seed)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::raw_key seed;
	{
		auto transaction (node->wallets.tx_begin_read ());
		system.wallet (0)->store.seed (seed, transaction);
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_seed");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	test_response response (request, rpc_config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	{
		std::string seed_text (response.json.get<std::string> ("seed"));
		ASSERT_EQ (seed.data.to_string (), seed_text);
	}
}

TEST (rpc, wallet_change_seed)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	nano::raw_key seed;
	nano::random_pool::generate_block (seed.data.bytes.data (), seed.data.bytes.size ());
	{
		auto transaction (system0.nodes[0]->wallets.tx_begin_read ());
		nano::raw_key seed0;
		nano::random_pool::generate_block (seed0.data.bytes.data (), seed0.data.bytes.size ());
		system0.wallet (0)->store.seed (seed0, transaction);
		ASSERT_NE (seed, seed0);
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	auto prv = nano::deterministic_key (seed, 0);
	auto pub (nano::pub_key (prv));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system0.env.ctx, rpc_config);
	nano::rpc rpc (system0.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_change_seed");
	request.put ("wallet", system0.nodes[0]->wallets.items.begin ()->first.to_string ());
	request.put ("seed", seed.data.to_string ());
	test_response response (request, rpc.config.port, system0.env.ctx);
	system0.deadline_set (5s);
	while (response.status == 0)
	{
		ASSERT_NO_ERROR (system0.poll ());
	}
	ASSERT_EQ (200, response.status);
	{
		auto transaction (system0.nodes[0]->wallets.tx_begin_read ());
		nano::raw_key seed0;
		system0.wallet (0)->store.seed (seed0, transaction);
		ASSERT_EQ (seed, seed0);
	}
	auto account_text (response.json.get<std::string> ("last_restored_account"));
	nano::account account;
	ASSERT_FALSE (account.decode_account (account_text));
	ASSERT_TRUE (system0.wallet (0)->exists (account));
	ASSERT_EQ (pub, account);
	ASSERT_EQ ("1", response.json.get<std::string> ("restored_count"));
}

TEST (rpc, wallet_frontiers)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	system0.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system0.env.ctx, rpc_config);
	nano::rpc rpc (system0.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_frontiers");
	request.put ("wallet", system0.nodes[0]->wallets.items.begin ()->first.to_string ());
	test_response response (request, rpc.config.port, system0.env.ctx);
	while (response.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response.status);
	auto & frontiers_node (response.json.get_child ("frontiers"));
	std::vector<nano::account> frontiers;
	for (auto i (frontiers_node.begin ()), n (frontiers_node.end ()); i != n; ++i)
	{
		frontiers.push_back (nano::account (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, frontiers.size ());
	ASSERT_EQ (system0.nodes[0]->latest (nano::genesis_account), frontiers[0]);
}

TEST (rpc, work_validate)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	nano::block_hash hash (1);
	uint64_t work1 (*node1.work_generate_blocking (hash));
	boost::property_tree::ptree request;
	request.put ("action", "work_validate");
	request.put ("hash", hash.to_string ());
	request.put ("work", nano::to_string_hex (work1));
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (0, response.json.count ("valid"));
		ASSERT_TRUE (response.json.get<bool> ("valid_all"));
		ASSERT_TRUE (response.json.get<bool> ("valid_receive"));
		std::string difficulty_text (response.json.get<std::string> ("difficulty"));
		uint64_t difficulty;
		ASSERT_FALSE (nano::from_string_hex (difficulty_text, difficulty));
		ASSERT_GE (difficulty, node1.default_difficulty (nano::work_version::work_1));
		double multiplier (response.json.get<double> ("multiplier"));
		ASSERT_NEAR (multiplier, nano::difficulty::to_multiplier (difficulty, node1.default_difficulty (nano::work_version::work_1)), 1e-6);
	}
	uint64_t work2 (0);
	request.put ("work", nano::to_string_hex (work2));
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (0, response.json.count ("valid"));
		ASSERT_FALSE (response.json.get<bool> ("valid_all"));
		ASSERT_FALSE (response.json.get<bool> ("valid_receive"));
		std::string difficulty_text (response.json.get<std::string> ("difficulty"));
		uint64_t difficulty;
		ASSERT_FALSE (nano::from_string_hex (difficulty_text, difficulty));
		ASSERT_GE (node1.default_difficulty (nano::work_version::work_1), difficulty);
		double multiplier (response.json.get<double> ("multiplier"));
		ASSERT_NEAR (multiplier, nano::difficulty::to_multiplier (difficulty, node1.default_difficulty (nano::work_version::work_1)), 1e-6);
	}
	auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work1));
	ASSERT_GE (result_difficulty, node1.default_difficulty (nano::work_version::work_1));
	request.put ("work", nano::to_string_hex (work1));
	request.put ("difficulty", nano::to_string_hex (result_difficulty));
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_TRUE (response.json.get<bool> ("valid"));
		ASSERT_TRUE (response.json.get<bool> ("valid_all"));
		ASSERT_TRUE (response.json.get<bool> ("valid_receive"));
	}
	uint64_t difficulty4 (0xfff0000000000000);
	request.put ("work", nano::to_string_hex (work1));
	request.put ("difficulty", nano::to_string_hex (difficulty4));
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (result_difficulty >= difficulty4, response.json.get<bool> ("valid"));
		ASSERT_EQ (result_difficulty >= node1.default_difficulty (nano::work_version::work_1), response.json.get<bool> ("valid_all"));
		ASSERT_EQ (result_difficulty >= node1.env.constants.network.publish_thresholds.epoch_2_receive, response.json.get<bool> ("valid_all"));
	}
	uint64_t work3 (*node1.work_generate_blocking (hash, difficulty4));
	request.put ("work", nano::to_string_hex (work3));
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_TRUE (response.json.get<bool> ("valid"));
		ASSERT_TRUE (response.json.get<bool> ("valid_all"));
		ASSERT_TRUE (response.json.get<bool> ("valid_receive"));
	}
}

TEST (rpc, work_validate_epoch_2)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto epoch1 = system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1);
	ASSERT_NE (nullptr, epoch1);
	ASSERT_EQ (node->env.constants.network.publish_thresholds.epoch_2, node->env.constants.network.publish_thresholds.base);
	auto work = system.work_generate_limited (epoch1->hash (), node->env.constants.network.publish_thresholds.epoch_1, node->env.constants.network.publish_thresholds.base);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "work_validate");
	request.put ("hash", epoch1->hash ().to_string ());
	request.put ("work", nano::to_string_hex (work));
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (0, response.json.count ("valid"));
		ASSERT_TRUE (response.json.get<bool> ("valid_all"));
		ASSERT_TRUE (response.json.get<bool> ("valid_receive"));
		std::string difficulty_text (response.json.get<std::string> ("difficulty"));
		uint64_t difficulty{ 0 };
		ASSERT_FALSE (nano::from_string_hex (difficulty_text, difficulty));
		double multiplier (response.json.get<double> ("multiplier"));
		ASSERT_NEAR (multiplier, nano::difficulty::to_multiplier (difficulty, node->env.constants.network.publish_thresholds.epoch_1), 1e-6);
	};
	// After upgrading, the higher difficulty is used to validate and calculate the multiplier
	scoped_thread_name_io.reset ();
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));
	scoped_thread_name_io.renew ();
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (0, response.json.count ("valid"));
		ASSERT_FALSE (response.json.get<bool> ("valid_all"));
		ASSERT_TRUE (response.json.get<bool> ("valid_receive"));
		std::string difficulty_text (response.json.get<std::string> ("difficulty"));
		uint64_t difficulty{ 0 };
		ASSERT_FALSE (nano::from_string_hex (difficulty_text, difficulty));
		double multiplier (response.json.get<double> ("multiplier"));
		ASSERT_NEAR (multiplier, nano::difficulty::to_multiplier (difficulty, node->default_difficulty (nano::work_version::work_1)), 1e-6);
	};
}

TEST (rpc, successors)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::keypair key;
	auto genesis (node->latest (nano::dev_genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, 1));
	ASSERT_NE (nullptr, block);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "successors");
	request.put ("block", genesis.to_string ());
	request.put ("count", std::to_string (std::numeric_limits<uint64_t>::max ()));
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & blocks_node (response.json.get_child ("blocks"));
	std::vector<nano::block_hash> blocks;
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (2, blocks.size ());
	ASSERT_EQ (genesis, blocks[0]);
	ASSERT_EQ (block->hash (), blocks[1]);
	// RPC chain "reverse" option
	request.put ("action", "chain");
	request.put ("reverse", "true");
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_EQ (response.json, response2.json);
}

TEST (rpc, bootstrap_any)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	nano::system system1 (1);
	auto latest (system1.nodes[0]->latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, nano::genesis_account, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system1.nodes[0]->work_generate_blocking (latest));
	{
		auto transaction (system1.nodes[0]->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, system1.nodes[0]->ledger.process (transaction, send).code);
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system0.env.ctx, rpc_config);
	nano::rpc rpc (system0.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "bootstrap_any");
	test_response response (request, rpc.config.port, system0.env.ctx);
	while (response.status == 0)
	{
		system0.poll ();
	}
	std::string success (response.json.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
}

TEST (rpc, republish)
{
	nano::system system;
	nano::keypair key;
	nano::genesis genesis;
	auto & node1 = *add_ipc_enabled_node (system);
	system.add_node ();
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	node1.process (send);
	nano::open_block open (send.hash (), key.pub, key.pub, key.prv, key.pub, *node1.work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1.process (open).code);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "republish");
	request.put ("hash", send.hash ().to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_TIMELY (10s, system.nodes[1]->balance (nano::dev_genesis_key.pub) != nano::genesis_amount);
	auto & blocks_node (response.json.get_child ("blocks"));
	std::vector<nano::block_hash> blocks;
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, blocks.size ());
	ASSERT_EQ (send.hash (), blocks[0]);

	request.put ("hash", genesis.hash ().to_string ());
	request.put ("count", 1);
	test_response response1 (request, rpc.config.port, system.env.ctx);
	system.deadline_set (5s);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	blocks_node = response1.json.get_child ("blocks");
	blocks.clear ();
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, blocks.size ());
	ASSERT_EQ (genesis.hash (), blocks[0]);

	request.put ("hash", open.hash ().to_string ());
	request.put ("sources", 2);
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	blocks_node = response2.json.get_child ("blocks");
	blocks.clear ();
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (3, blocks.size ());
	ASSERT_EQ (genesis.hash (), blocks[0]);
	ASSERT_EQ (send.hash (), blocks[1]);
	ASSERT_EQ (open.hash (), blocks[2]);
}

TEST (rpc, deterministic_key)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	nano::raw_key seed;
	{
		auto transaction (system0.nodes[0]->wallets.tx_begin_read ());
		system0.wallet (0)->store.seed (seed, transaction);
	}
	nano::account account0 (system0.wallet (0)->deterministic_insert ());
	nano::account account1 (system0.wallet (0)->deterministic_insert ());
	nano::account account2 (system0.wallet (0)->deterministic_insert ());
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system0.env.ctx, rpc_config);
	nano::rpc rpc (system0.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "deterministic_key");
	request.put ("seed", seed.data.to_string ());
	request.put ("index", "0");
	test_response response0 (request, rpc.config.port, system0.env.ctx);
	while (response0.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response0.status);
	std::string validate_text (response0.json.get<std::string> ("account"));
	ASSERT_EQ (account0.to_account (), validate_text);
	request.put ("index", "2");
	test_response response1 (request, rpc.config.port, system0.env.ctx);
	while (response1.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response1.status);
	validate_text = response1.json.get<std::string> ("account");
	ASSERT_NE (account1.to_account (), validate_text);
	ASSERT_EQ (account2.to_account (), validate_text);
}

TEST (rpc, accounts_balances)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "accounts_balances");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", nano::dev_genesis_key.pub.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", peers_l);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	for (auto & balances : response.json.get_child ("balances"))
	{
		std::string account_text (balances.first);
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), account_text);
		std::string balance_text (balances.second.get<std::string> ("balance"));
		ASSERT_EQ ("340282366920938463463374607431768211455", balance_text);
		std::string pending_text (balances.second.get<std::string> ("pending"));
		ASSERT_EQ ("0", pending_text);
	}
}

TEST (rpc, accounts_frontiers)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "accounts_frontiers");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", nano::dev_genesis_key.pub.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", peers_l);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	for (auto & frontiers : response.json.get_child ("frontiers"))
	{
		std::string account_text (frontiers.first);
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), account_text);
		std::string frontier_text (frontiers.second.get<std::string> (""));
		ASSERT_EQ (node->latest (nano::genesis_account), frontier_text);
	}
}

TEST (rpc, accounts_pending)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key1;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto block1 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key1.pub, 100));
	scoped_io_thread_name_change scoped_thread_name_io;
	ASSERT_TIMELY (10s, !node->active.active (*block1));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "accounts_pending");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", key1.pub.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", peers_l);
	request.put ("count", "100");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		for (auto & blocks : response.json.get_child ("blocks"))
		{
			std::string account_text (blocks.first);
			ASSERT_EQ (key1.pub.to_account (), account_text);
			nano::block_hash hash1 (blocks.second.begin ()->second.get<std::string> (""));
			ASSERT_EQ (block1->hash (), hash1);
		}
	}
	request.put ("sorting", "true"); // Sorting test
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		for (auto & blocks : response.json.get_child ("blocks"))
		{
			std::string account_text (blocks.first);
			ASSERT_EQ (key1.pub.to_account (), account_text);
			nano::block_hash hash1 (blocks.second.begin ()->first);
			ASSERT_EQ (block1->hash (), hash1);
			std::string amount (blocks.second.begin ()->second.get<std::string> (""));
			ASSERT_EQ ("100", amount);
		}
	}
	request.put ("threshold", "100"); // Threshold test
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::unordered_map<nano::block_hash, nano::uint128_union> blocks;
		for (auto & pending : response.json.get_child ("blocks"))
		{
			std::string account_text (pending.first);
			ASSERT_EQ (key1.pub.to_account (), account_text);
			for (auto i (pending.second.begin ()), j (pending.second.end ()); i != j; ++i)
			{
				nano::block_hash hash;
				hash.decode_hex (i->first);
				nano::uint128_union amount;
				amount.decode_dec (i->second.get<std::string> (""));
				blocks[hash] = amount;
				boost::optional<std::string> source (i->second.get_optional<std::string> ("source"));
				ASSERT_FALSE (source.is_initialized ());
			}
		}
		ASSERT_EQ (blocks[block1->hash ()], 100);
	}
	request.put ("source", "true");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::unordered_map<nano::block_hash, nano::uint128_union> amounts;
		std::unordered_map<nano::block_hash, nano::account> sources;
		for (auto & pending : response.json.get_child ("blocks"))
		{
			std::string account_text (pending.first);
			ASSERT_EQ (key1.pub.to_account (), account_text);
			for (auto i (pending.second.begin ()), j (pending.second.end ()); i != j; ++i)
			{
				nano::block_hash hash;
				hash.decode_hex (i->first);
				amounts[hash].decode_dec (i->second.get<std::string> ("amount"));
				sources[hash].decode_account (i->second.get<std::string> ("source"));
			}
		}
		ASSERT_EQ (amounts[block1->hash ()], 100);
		ASSERT_EQ (sources[block1->hash ()], nano::dev_genesis_key.pub);
	}

	request.put ("include_only_confirmed", "true");
	check_block_response_count (system, rpc, request, 1);
	scoped_thread_name_io.reset ();
	reset_confirmation_height (system.nodes.front ()->store, block1->account ());
	scoped_thread_name_io.renew ();
	check_block_response_count (system, rpc, request, 0);
}

TEST (rpc, blocks)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "blocks");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", node->latest (nano::genesis_account).to_string ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("hashes", peers_l);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	for (auto & blocks : response.json.get_child ("blocks"))
	{
		std::string hash_text (blocks.first);
		ASSERT_EQ (node->latest (nano::genesis_account).to_string (), hash_text);
		std::string blocks_text (blocks.second.get<std::string> (""));
		ASSERT_FALSE (blocks_text.empty ());
	}
}

TEST (rpc, wallet_info)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (key.prv);
	auto send (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, 1));
	nano::account account (system.wallet (0)->deterministic_insert ());
	{
		auto transaction (node->wallets.tx_begin_write ());
		system.wallet (0)->store.erase (transaction, account);
	}
	account = system.wallet (0)->deterministic_insert ();
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_info");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string balance_text (response.json.get<std::string> ("balance"));
	ASSERT_EQ ("340282366920938463463374607431768211454", balance_text);
	std::string pending_text (response.json.get<std::string> ("pending"));
	ASSERT_EQ ("1", pending_text);
	std::string count_text (response.json.get<std::string> ("accounts_count"));
	ASSERT_EQ ("3", count_text);
	std::string adhoc_count (response.json.get<std::string> ("adhoc_count"));
	ASSERT_EQ ("2", adhoc_count);
	std::string deterministic_count (response.json.get<std::string> ("deterministic_count"));
	ASSERT_EQ ("1", deterministic_count);
	std::string index_text (response.json.get<std::string> ("deterministic_index"));
	ASSERT_EQ ("2", index_text);
}

TEST (rpc, wallet_balances)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	system0.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system0.env.ctx, rpc_config);
	nano::rpc rpc (system0.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_balances");
	request.put ("wallet", system0.nodes[0]->wallets.items.begin ()->first.to_string ());
	test_response response (request, rpc.config.port, system0.env.ctx);
	while (response.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response.status);
	for (auto & balances : response.json.get_child ("balances"))
	{
		std::string account_text (balances.first);
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), account_text);
		std::string balance_text (balances.second.get<std::string> ("balance"));
		ASSERT_EQ ("340282366920938463463374607431768211455", balance_text);
		std::string pending_text (balances.second.get<std::string> ("pending"));
		ASSERT_EQ ("0", pending_text);
	}
	nano::keypair key;
	scoped_thread_name_io.reset ();
	system0.wallet (0)->insert_adhoc (key.prv);
	auto send (system0.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, 1));
	scoped_thread_name_io.renew ();
	request.put ("threshold", "2");
	test_response response1 (request, rpc.config.port, system0.env.ctx);
	while (response1.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response1.status);
	for (auto & balances : response1.json.get_child ("balances"))
	{
		std::string account_text (balances.first);
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), account_text);
		std::string balance_text (balances.second.get<std::string> ("balance"));
		ASSERT_EQ ("340282366920938463463374607431768211454", balance_text);
		std::string pending_text (balances.second.get<std::string> ("pending"));
		ASSERT_EQ ("0", pending_text);
	}
}

TEST (rpc, pending_exists)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key1;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto hash0 (node->latest (nano::genesis_account));
	auto block1 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key1.pub, 100));
	scoped_io_thread_name_change scoped_thread_name_io;
	ASSERT_TIMELY (5s, !node->active.active (*block1));
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;

	auto pending_exists = [&system, &request, &rpc](const char * exists_a) {
		test_response response0 (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response0.status != 0);
		ASSERT_EQ (200, response0.status);
		std::string exists_text (response0.json.get<std::string> ("exists"));
		ASSERT_EQ (exists_a, exists_text);
	};

	request.put ("action", "pending_exists");
	request.put ("hash", hash0.to_string ());
	pending_exists ("0");

	request.put ("hash", block1->hash ().to_string ());
	pending_exists ("1");

	request.put ("include_only_confirmed", "true");
	pending_exists ("1");
	scoped_thread_name_io.reset ();
	reset_confirmation_height (system.nodes.front ()->store, block1->account ());
	scoped_thread_name_io.renew ();
	pending_exists ("0");
}

TEST (rpc, wallet_pending)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	nano::keypair key1;
	system0.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system0.wallet (0)->insert_adhoc (key1.prv);
	auto block1 (system0.wallet (0)->send_action (nano::dev_genesis_key.pub, key1.pub, 100));
	auto iterations (0);
	scoped_io_thread_name_change scoped_thread_name_io;
	while (system0.nodes[0]->active.active (*block1))
	{
		system0.poll ();
		++iterations;
		ASSERT_LT (iterations, 200);
	}
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system0.env.ctx, rpc_config);
	nano::rpc rpc (system0.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_pending");
	request.put ("wallet", system0.nodes[0]->wallets.items.begin ()->first.to_string ());
	request.put ("count", "100");
	test_response response (request, rpc.config.port, system0.env.ctx);
	while (response.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response.status);
	ASSERT_EQ (1, response.json.get_child ("blocks").size ());
	for (auto & pending : response.json.get_child ("blocks"))
	{
		std::string account_text (pending.first);
		ASSERT_EQ (key1.pub.to_account (), account_text);
		nano::block_hash hash1 (pending.second.begin ()->second.get<std::string> (""));
		ASSERT_EQ (block1->hash (), hash1);
	}
	request.put ("threshold", "100"); // Threshold test
	test_response response0 (request, rpc.config.port, system0.env.ctx);
	while (response0.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response0.status);
	std::unordered_map<nano::block_hash, nano::uint128_union> blocks;
	ASSERT_EQ (1, response0.json.get_child ("blocks").size ());
	for (auto & pending : response0.json.get_child ("blocks"))
	{
		std::string account_text (pending.first);
		ASSERT_EQ (key1.pub.to_account (), account_text);
		for (auto i (pending.second.begin ()), j (pending.second.end ()); i != j; ++i)
		{
			nano::block_hash hash;
			hash.decode_hex (i->first);
			nano::uint128_union amount;
			amount.decode_dec (i->second.get<std::string> (""));
			blocks[hash] = amount;
			boost::optional<std::string> source (i->second.get_optional<std::string> ("source"));
			ASSERT_FALSE (source.is_initialized ());
			boost::optional<uint8_t> min_version (i->second.get_optional<uint8_t> ("min_version"));
			ASSERT_FALSE (min_version.is_initialized ());
		}
	}
	ASSERT_EQ (blocks[block1->hash ()], 100);
	request.put ("threshold", "101");
	test_response response1 (request, rpc.config.port, system0.env.ctx);
	while (response1.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response1.status);
	auto & pending1 (response1.json.get_child ("blocks"));
	ASSERT_EQ (0, pending1.size ());
	request.put ("threshold", "0");
	request.put ("source", "true");
	request.put ("min_version", "true");
	test_response response2 (request, rpc.config.port, system0.env.ctx);
	while (response2.status == 0)
	{
		system0.poll ();
	}
	ASSERT_EQ (200, response2.status);
	std::unordered_map<nano::block_hash, nano::uint128_union> amounts;
	std::unordered_map<nano::block_hash, nano::account> sources;
	ASSERT_EQ (1, response0.json.get_child ("blocks").size ());
	for (auto & pending : response2.json.get_child ("blocks"))
	{
		std::string account_text (pending.first);
		ASSERT_EQ (key1.pub.to_account (), account_text);
		for (auto i (pending.second.begin ()), j (pending.second.end ()); i != j; ++i)
		{
			nano::block_hash hash;
			hash.decode_hex (i->first);
			amounts[hash].decode_dec (i->second.get<std::string> ("amount"));
			sources[hash].decode_account (i->second.get<std::string> ("source"));
			ASSERT_EQ (i->second.get<uint8_t> ("min_version"), 0);
		}
	}
	ASSERT_EQ (amounts[block1->hash ()], 100);
	ASSERT_EQ (sources[block1->hash ()], nano::dev_genesis_key.pub);

	request.put ("include_only_confirmed", "true");
	check_block_response_count (system0, rpc, request, 1);
	scoped_thread_name_io.reset ();
	reset_confirmation_height (system0.nodes.front ()->store, block1->account ());
	scoped_thread_name_io.renew ();
	{
		test_response response (request, rpc.config.port, system0.env.ctx);
		system0.deadline_set (5s);
		while (response.status == 0)
		{
			ASSERT_NO_ERROR (system0.poll ());
		}
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (0, response.json.get_child ("blocks").size ());
	}
}

TEST (rpc, receive_minimum)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "receive_minimum");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string amount (response.json.get<std::string> ("amount"));
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), amount);
}

TEST (rpc, receive_minimum_set)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "receive_minimum_set");
	request.put ("amount", "100");
	ASSERT_NE (node->config.receive_minimum.to_string_dec (), "100");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string success (response.json.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), "100");
}

TEST (rpc, work_get)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->work_cache_blocking (nano::dev_genesis_key.pub, node->latest (nano::dev_genesis_key.pub));
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "work_get");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string work_text (response.json.get<std::string> ("work"));
	uint64_t work (1);
	auto transaction (node->wallets.tx_begin_read ());
	node->wallets.items.begin ()->second->store.work_get (transaction, nano::genesis_account, work);
	ASSERT_EQ (nano::to_string_hex (work), work_text);
}

TEST (rpc, wallet_work_get)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->work_cache_blocking (nano::dev_genesis_key.pub, node->latest (nano::dev_genesis_key.pub));
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_work_get");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto transaction (node->wallets.tx_begin_read ());
	for (auto & works : response.json.get_child ("works"))
	{
		std::string account_text (works.first);
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), account_text);
		std::string work_text (works.second.get<std::string> (""));
		uint64_t work (1);
		node->wallets.items.begin ()->second->store.work_get (transaction, nano::genesis_account, work);
		ASSERT_EQ (nano::to_string_hex (work), work_text);
	}
}

TEST (rpc, work_set)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	uint64_t work0 (100);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "work_set");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	request.put ("work", nano::to_string_hex (work0));
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string success (response.json.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
	uint64_t work1 (1);
	auto transaction (node->wallets.tx_begin_read ());
	node->wallets.items.begin ()->second->store.work_get (transaction, nano::genesis_account, work1);
	ASSERT_EQ (work1, work0);
}

TEST (rpc, search_pending_all)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto latest (node->latest (nano::dev_genesis_key.pub));
	nano::send_block block (latest, nano::dev_genesis_key.pub, nano::genesis_amount - node->config.receive_minimum.number (), nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node->work_generate_blocking (latest));
	{
		auto transaction (node->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, block).code);
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "search_pending_all");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_TIMELY (10s, node->balance (nano::dev_genesis_key.pub) == nano::genesis_amount);
}

TEST (rpc, wallet_republish)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::genesis genesis;
	nano::keypair key;
	while (key.pub < nano::dev_genesis_key.pub)
	{
		nano::keypair key1;
		key.pub = key1.pub;
		key.prv.data = key1.prv.data;
	}
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	node1.process (send);
	nano::open_block open (send.hash (), key.pub, key.pub, key.prv, key.pub, *node1.work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1.process (open).code);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_republish");
	request.put ("wallet", node1.wallets.items.begin ()->first.to_string ());
	request.put ("count", 1);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & blocks_node (response.json.get_child ("blocks"));
	std::vector<nano::block_hash> blocks;
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (2, blocks.size ());
	ASSERT_EQ (send.hash (), blocks[0]);
	ASSERT_EQ (open.hash (), blocks[1]);
}

TEST (rpc, delegators)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	node1.process (send);
	nano::open_block open (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, *node1.work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1.process (open).code);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "delegators");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & delegators_node (response.json.get_child ("delegators"));
	boost::property_tree::ptree delegators;
	for (auto i (delegators_node.begin ()), n (delegators_node.end ()); i != n; ++i)
	{
		delegators.put ((i->first), (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (2, delegators.size ());
	ASSERT_EQ ("100", delegators.get<std::string> (nano::dev_genesis_key.pub.to_account ()));
	ASSERT_EQ ("340282366920938463463374607431768211355", delegators.get<std::string> (key.pub.to_account ()));
}

TEST (rpc, delegators_count)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	node1.process (send);
	nano::open_block open (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, *node1.work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1.process (open).code);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "delegators_count");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string count (response.json.get<std::string> ("count"));
	ASSERT_EQ ("2", count);
}

TEST (rpc, account_info)
{
	nano::system system;
	nano::keypair key;
	nano::genesis genesis;

	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();

	boost::property_tree::ptree request;
	request.put ("action", "account_info");
	request.put ("account", nano::account ().to_account ());

	// Test for a non existing account
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);

		auto error (response.json.get_optional<std::string> ("error"));
		ASSERT_TRUE (error.is_initialized ());
		ASSERT_EQ (error.get (), std::error_code (nano::error_common::account_not_found).message ());
	}

	scoped_thread_name_io.reset ();
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	node1.process (send);
	auto time (nano::seconds_since_epoch ());
	{
		auto transaction = node1.store.tx_begin_write ();
		node1.store.confirmation_height_put (transaction, nano::dev_genesis_key.pub, { 1, genesis.hash () });
	}
	scoped_thread_name_io.renew ();

	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);

		ASSERT_EQ (200, response.status);
		std::string frontier (response.json.get<std::string> ("frontier"));
		ASSERT_EQ (send.hash ().to_string (), frontier);
		std::string open_block (response.json.get<std::string> ("open_block"));
		ASSERT_EQ (genesis.hash ().to_string (), open_block);
		std::string representative_block (response.json.get<std::string> ("representative_block"));
		ASSERT_EQ (genesis.hash ().to_string (), representative_block);
		std::string balance (response.json.get<std::string> ("balance"));
		ASSERT_EQ ("100", balance);
		std::string modified_timestamp (response.json.get<std::string> ("modified_timestamp"));
		ASSERT_LT (std::abs ((long)time - stol (modified_timestamp)), 5);
		std::string block_count (response.json.get<std::string> ("block_count"));
		ASSERT_EQ ("2", block_count);
		std::string confirmation_height (response.json.get<std::string> ("confirmation_height"));
		ASSERT_EQ ("1", confirmation_height);
		std::string confirmation_height_frontier (response.json.get<std::string> ("confirmation_height_frontier"));
		ASSERT_EQ (genesis.hash ().to_string (), confirmation_height_frontier);
		ASSERT_EQ (0, response.json.get<uint8_t> ("account_version"));
		boost::optional<std::string> weight (response.json.get_optional<std::string> ("weight"));
		ASSERT_FALSE (weight.is_initialized ());
		boost::optional<std::string> pending (response.json.get_optional<std::string> ("pending"));
		ASSERT_FALSE (pending.is_initialized ());
		boost::optional<std::string> representative (response.json.get_optional<std::string> ("representative"));
		ASSERT_FALSE (representative.is_initialized ());
	}

	// Test for optional values
	request.put ("weight", "true");
	request.put ("pending", "1");
	request.put ("representative", "1");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		std::string weight2 (response.json.get<std::string> ("weight"));
		ASSERT_EQ ("100", weight2);
		std::string pending2 (response.json.get<std::string> ("pending"));
		ASSERT_EQ ("0", pending2);
		std::string representative2 (response.json.get<std::string> ("representative"));
		ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), representative2);
	}
}

/** Make sure we can use json block literals instead of string as input */
TEST (rpc, json_block_input)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (key.prv);
	nano::state_block send (nano::genesis_account, node1.latest (nano::dev_genesis_key.pub), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, 0);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "sign");
	request.put ("json_block", "true");
	std::string wallet;
	node1.wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("account", key.pub.to_account ());
	boost::property_tree::ptree json;
	send.serialize_json (json);
	request.add_child ("block", json);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response.status != 0);
	ASSERT_EQ (200, response.status);

	bool json_error{ false };
	nano::state_block block (json_error, response.json.get_child ("block"));
	ASSERT_FALSE (json_error);

	ASSERT_FALSE (nano::validate_message (key.pub, send.hash (), block.block_signature ()));
	ASSERT_NE (block.block_signature (), send.block_signature ());
	ASSERT_EQ (block.hash (), send.hash ());
}

/** Make sure we can receive json block literals instead of string as output */
TEST (rpc, json_block_output)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	node1.process (send);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "block_info");
	request.put ("json_block", "true");
	request.put ("hash", send.hash ().to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);

	// Make sure contents contains a valid JSON subtree instread of stringified json
	bool json_error{ false };
	nano::send_block send_from_json (json_error, response.json.get_child ("contents"));
	ASSERT_FALSE (json_error);
}

TEST (rpc, blocks_info)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	auto check_blocks = [node](test_response & response) {
		for (auto & blocks : response.json.get_child ("blocks"))
		{
			std::string hash_text (blocks.first);
			ASSERT_EQ (node->latest (nano::genesis_account).to_string (), hash_text);
			std::string account_text (blocks.second.get<std::string> ("block_account"));
			ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), account_text);
			std::string amount_text (blocks.second.get<std::string> ("amount"));
			ASSERT_EQ (nano::genesis_amount.convert_to<std::string> (), amount_text);
			std::string blocks_text (blocks.second.get<std::string> ("contents"));
			ASSERT_FALSE (blocks_text.empty ());
			boost::optional<std::string> pending (blocks.second.get_optional<std::string> ("pending"));
			ASSERT_FALSE (pending.is_initialized ());
			boost::optional<std::string> source (blocks.second.get_optional<std::string> ("source_account"));
			ASSERT_FALSE (source.is_initialized ());
			std::string balance_text (blocks.second.get<std::string> ("balance"));
			ASSERT_EQ (nano::genesis_amount.convert_to<std::string> (), balance_text);
			ASSERT_TRUE (blocks.second.get<bool> ("confirmed")); // Genesis block is confirmed by default
		}
	};
	boost::property_tree::ptree request;
	request.put ("action", "blocks_info");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree hashes;
	entry.put ("", node->latest (nano::genesis_account).to_string ());
	hashes.push_back (std::make_pair ("", entry));
	request.add_child ("hashes", hashes);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		check_blocks (response);
	}
	std::string random_hash = nano::block_hash ().to_string ();
	entry.put ("", random_hash);
	hashes.push_back (std::make_pair ("", entry));
	request.erase ("hashes");
	request.add_child ("hashes", hashes);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response.json.get<std::string> ("error"));
	}
	request.put ("include_not_found", "true");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		check_blocks (response);
		auto & blocks_not_found (response.json.get_child ("blocks_not_found"));
		ASSERT_EQ (1, blocks_not_found.size ());
		ASSERT_EQ (random_hash, blocks_not_found.begin ()->second.get<std::string> (""));
	}
	request.put ("source", "true");
	request.put ("pending", "1");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		for (auto & blocks : response.json.get_child ("blocks"))
		{
			std::string source (blocks.second.get<std::string> ("source_account"));
			ASSERT_EQ ("0", source);
			std::string pending (blocks.second.get<std::string> ("pending"));
			ASSERT_EQ ("0", pending);
		}
	}
}

TEST (rpc, blocks_info_subtype)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto send (system.wallet (0)->send_action (nano::dev_genesis_key.pub, nano::dev_genesis_key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send);
	auto receive (system.wallet (0)->receive_action (*send, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, receive);
	auto change (system.wallet (0)->change_action (nano::dev_genesis_key.pub, key.pub));
	ASSERT_NE (nullptr, change);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "blocks_info");
	boost::property_tree::ptree hashes;
	boost::property_tree::ptree entry;
	entry.put ("", send->hash ().to_string ());
	hashes.push_back (std::make_pair ("", entry));
	entry.put ("", receive->hash ().to_string ());
	hashes.push_back (std::make_pair ("", entry));
	entry.put ("", change->hash ().to_string ());
	hashes.push_back (std::make_pair ("", entry));
	request.add_child ("hashes", hashes);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	auto & blocks (response.json.get_child ("blocks"));
	ASSERT_EQ (3, blocks.size ());
	auto send_subtype (blocks.get_child (send->hash ().to_string ()).get<std::string> ("subtype"));
	ASSERT_EQ (send_subtype, "send");
	auto receive_subtype (blocks.get_child (receive->hash ().to_string ()).get<std::string> ("subtype"));
	ASSERT_EQ (receive_subtype, "receive");
	auto change_subtype (blocks.get_child (change->hash ().to_string ()).get<std::string> ("subtype"));
	ASSERT_EQ (change_subtype, "change");
}

TEST (rpc, work_peers_all)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "work_peer_add");
	request.put ("address", "::1");
	request.put ("port", "0");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string success (response.json.get<std::string> ("success", ""));
	ASSERT_TRUE (success.empty ());
	boost::property_tree::ptree request1;
	request1.put ("action", "work_peers");
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	auto & peers_node (response1.json.get_child ("work_peers"));
	std::vector<std::string> peers;
	for (auto i (peers_node.begin ()), n (peers_node.end ()); i != n; ++i)
	{
		peers.push_back (i->second.get<std::string> (""));
	}
	ASSERT_EQ (1, peers.size ());
	ASSERT_EQ ("::1:0", peers[0]);
	boost::property_tree::ptree request2;
	request2.put ("action", "work_peers_clear");
	test_response response2 (request2, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	success = response2.json.get<std::string> ("success", "");
	ASSERT_TRUE (success.empty ());
	test_response response3 (request1, rpc.config.port, system.env.ctx);
	system.deadline_set (5s);
	ASSERT_TIMELY (10s, response3.status != 0);
	ASSERT_EQ (200, response3.status);
	peers_node = response3.json.get_child ("work_peers");
	ASSERT_EQ (0, peers_node.size ());
}

TEST (rpc, ledger)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	nano::genesis genesis;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto & node1 (*system.nodes[0]);
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	auto genesis_balance (nano::genesis_amount);
	auto send_amount (genesis_balance - 100);
	genesis_balance -= send_amount;
	nano::send_block send (latest, key.pub, genesis_balance, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	node1.process (send);
	nano::open_block open (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, *node1.work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1.process (open).code);
	auto time (nano::seconds_since_epoch ());
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "ledger");
	request.put ("sorting", true);
	request.put ("count", "1");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		for (auto & account : response.json.get_child ("accounts"))
		{
			std::string account_text (account.first);
			ASSERT_EQ (key.pub.to_account (), account_text);
			std::string frontier (account.second.get<std::string> ("frontier"));
			ASSERT_EQ (open.hash ().to_string (), frontier);
			std::string open_block (account.second.get<std::string> ("open_block"));
			ASSERT_EQ (open.hash ().to_string (), open_block);
			std::string representative_block (account.second.get<std::string> ("representative_block"));
			ASSERT_EQ (open.hash ().to_string (), representative_block);
			std::string balance_text (account.second.get<std::string> ("balance"));
			ASSERT_EQ (send_amount.convert_to<std::string> (), balance_text);
			std::string modified_timestamp (account.second.get<std::string> ("modified_timestamp"));
			ASSERT_LT (std::abs ((long)time - stol (modified_timestamp)), 5);
			std::string block_count (account.second.get<std::string> ("block_count"));
			ASSERT_EQ ("1", block_count);
			boost::optional<std::string> weight (account.second.get_optional<std::string> ("weight"));
			ASSERT_FALSE (weight.is_initialized ());
			boost::optional<std::string> pending (account.second.get_optional<std::string> ("pending"));
			ASSERT_FALSE (pending.is_initialized ());
			boost::optional<std::string> representative (account.second.get_optional<std::string> ("representative"));
			ASSERT_FALSE (representative.is_initialized ());
		}
	}
	// Test for optional values
	request.put ("weight", true);
	request.put ("pending", true);
	request.put ("representative", true);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		for (auto & account : response.json.get_child ("accounts"))
		{
			boost::optional<std::string> weight (account.second.get_optional<std::string> ("weight"));
			ASSERT_TRUE (weight.is_initialized ());
			ASSERT_EQ ("0", weight.get ());
			boost::optional<std::string> pending (account.second.get_optional<std::string> ("pending"));
			ASSERT_TRUE (pending.is_initialized ());
			ASSERT_EQ ("0", pending.get ());
			boost::optional<std::string> representative (account.second.get_optional<std::string> ("representative"));
			ASSERT_TRUE (representative.is_initialized ());
			ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), representative.get ());
		}
	}
	// Test threshold
	request.put ("count", 2);
	request.put ("threshold", genesis_balance + 1);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		auto & accounts (response.json.get_child ("accounts"));
		ASSERT_EQ (1, accounts.size ());
		auto account (accounts.begin ());
		ASSERT_EQ (key.pub.to_account (), account->first);
		std::string balance_text (account->second.get<std::string> ("balance"));
		ASSERT_EQ (send_amount.convert_to<std::string> (), balance_text);
	}
	auto send2_amount (50);
	genesis_balance -= send2_amount;
	nano::send_block send2 (send.hash (), key.pub, genesis_balance, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (send.hash ()));
	scoped_thread_name_io.reset ();
	node1.process (send2);
	scoped_thread_name_io.renew ();
	// When asking for pending, pending amount is taken into account for threshold so the account must show up
	request.put ("count", 2);
	request.put ("threshold", (send_amount + send2_amount).convert_to<std::string> ());
	request.put ("pending", true);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		auto & accounts (response.json.get_child ("accounts"));
		ASSERT_EQ (1, accounts.size ());
		auto account (accounts.begin ());
		ASSERT_EQ (key.pub.to_account (), account->first);
		std::string balance_text (account->second.get<std::string> ("balance"));
		ASSERT_EQ (send_amount.convert_to<std::string> (), balance_text);
		std::string pending_text (account->second.get<std::string> ("pending"));
		ASSERT_EQ (std::to_string (send2_amount), pending_text);
	}
}

TEST (rpc, accounts_create)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "accounts_create");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("count", "8");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & accounts (response.json.get_child ("accounts"));
	for (auto i (accounts.begin ()), n (accounts.end ()); i != n; ++i)
	{
		std::string account_text (i->second.get<std::string> (""));
		nano::account account;
		ASSERT_FALSE (account.decode_account (account_text));
		ASSERT_TRUE (system.wallet (0)->exists (account));
	}
	ASSERT_EQ (8, accounts.size ());
}

TEST (rpc, block_create)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	nano::genesis genesis;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	auto send_work = *node1.work_generate_blocking (latest);
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, send_work);
	auto open_work = *node1.work_generate_blocking (key.pub);
	nano::open_block open (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, open_work);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "send");
	request.put ("wallet", node1.wallets.items.begin ()->first.to_string ());
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	request.put ("previous", latest.to_string ());
	request.put ("amount", "340282366920938463463374607431768211355");
	request.put ("destination", key.pub.to_account ());
	request.put ("work", nano::to_string_hex (send_work));
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string send_hash (response.json.get<std::string> ("hash"));
	ASSERT_EQ (send.hash ().to_string (), send_hash);
	std::string send_difficulty (response.json.get<std::string> ("difficulty"));
	ASSERT_EQ (nano::to_string_hex (send.difficulty ()), send_difficulty);
	auto send_text (response.json.get<std::string> ("block"));
	boost::property_tree::ptree block_l;
	std::stringstream block_stream (send_text);
	boost::property_tree::read_json (block_stream, block_l);
	auto send_block (nano::deserialize_block_json (block_l));
	ASSERT_EQ (send.hash (), send_block->hash ());
	scoped_thread_name_io.reset ();
	node1.process (send);
	scoped_thread_name_io.renew ();
	boost::property_tree::ptree request1;
	request1.put ("action", "block_create");
	request1.put ("type", "open");
	std::string key_text;
	key.prv.data.encode_hex (key_text);
	request1.put ("key", key_text);
	request1.put ("representative", nano::dev_genesis_key.pub.to_account ());
	request1.put ("source", send.hash ().to_string ());
	request1.put ("work", nano::to_string_hex (open_work));
	test_response response1 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	std::string open_hash (response1.json.get<std::string> ("hash"));
	ASSERT_EQ (open.hash ().to_string (), open_hash);
	auto open_text (response1.json.get<std::string> ("block"));
	std::stringstream block_stream1 (open_text);
	boost::property_tree::read_json (block_stream1, block_l);
	auto open_block (nano::deserialize_block_json (block_l));
	ASSERT_EQ (open.hash (), open_block->hash ());
	scoped_thread_name_io.reset ();
	ASSERT_EQ (nano::process_result::progress, node1.process (open).code);
	scoped_thread_name_io.renew ();
	request1.put ("representative", key.pub.to_account ());
	test_response response2 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	std::string open2_hash (response2.json.get<std::string> ("hash"));
	ASSERT_NE (open.hash ().to_string (), open2_hash); // different blocks with wrong representative
	auto change_work = *node1.work_generate_blocking (open.hash ());
	nano::change_block change (open.hash (), key.pub, key.prv, key.pub, change_work);
	request1.put ("type", "change");
	request1.put ("work", nano::to_string_hex (change_work));
	test_response response4 (request1, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response4.status != 0);
	ASSERT_EQ (200, response4.status);
	std::string change_hash (response4.json.get<std::string> ("hash"));
	ASSERT_EQ (change.hash ().to_string (), change_hash);
	auto change_text (response4.json.get<std::string> ("block"));
	std::stringstream block_stream4 (change_text);
	boost::property_tree::read_json (block_stream4, block_l);
	auto change_block (nano::deserialize_block_json (block_l));
	ASSERT_EQ (change.hash (), change_block->hash ());
	scoped_thread_name_io.reset ();
	ASSERT_EQ (nano::process_result::progress, node1.process (change).code);
	nano::send_block send2 (send.hash (), key.pub, 0, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (send.hash ()));
	ASSERT_EQ (nano::process_result::progress, node1.process (send2).code);
	scoped_thread_name_io.renew ();
	boost::property_tree::ptree request2;
	request2.put ("action", "block_create");
	request2.put ("type", "receive");
	request2.put ("wallet", node1.wallets.items.begin ()->first.to_string ());
	request2.put ("account", key.pub.to_account ());
	request2.put ("source", send2.hash ().to_string ());
	request2.put ("previous", change.hash ().to_string ());
	request2.put ("work", nano::to_string_hex (*node1.work_generate_blocking (change.hash ())));
	test_response response5 (request2, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response5.status != 0);
	ASSERT_EQ (200, response5.status);
	std::string receive_hash (response4.json.get<std::string> ("hash"));
	auto receive_text (response5.json.get<std::string> ("block"));
	std::stringstream block_stream5 (change_text);
	boost::property_tree::read_json (block_stream5, block_l);
	auto receive_block (nano::deserialize_block_json (block_l));
	ASSERT_EQ (receive_hash, receive_block->hash ().to_string ());
	node1.process_active (std::move (receive_block));
	latest = node1.latest (key.pub);
	ASSERT_EQ (receive_hash, latest.to_string ());
}

TEST (rpc, block_create_state)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	nano::genesis genesis;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	request.put ("previous", genesis.hash ().to_string ());
	request.put ("representative", nano::dev_genesis_key.pub.to_account ());
	request.put ("balance", (nano::genesis_amount - nano::Gxrb_ratio).convert_to<std::string> ());
	request.put ("link", key.pub.to_account ());
	request.put ("work", nano::to_string_hex (*node->work_generate_blocking (genesis.hash ())));
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string state_hash (response.json.get<std::string> ("hash"));
	auto state_text (response.json.get<std::string> ("block"));
	std::stringstream block_stream (state_text);
	boost::property_tree::ptree block_l;
	boost::property_tree::read_json (block_stream, block_l);
	auto state_block (nano::deserialize_block_json (block_l));
	ASSERT_NE (nullptr, state_block);
	ASSERT_EQ (nano::block_type::state, state_block->type ());
	ASSERT_EQ (state_hash, state_block->hash ().to_string ());
	scoped_thread_name_io.reset ();
	auto process_result (node->process (*state_block));
	ASSERT_EQ (nano::process_result::progress, process_result.code);
}

TEST (rpc, block_create_state_open)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	nano::genesis genesis;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto send_block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("key", key.prv.data.to_string ());
	request.put ("account", key.pub.to_account ());
	request.put ("previous", 0);
	request.put ("representative", nano::dev_genesis_key.pub.to_account ());
	request.put ("balance", nano::Gxrb_ratio.convert_to<std::string> ());
	request.put ("link", send_block->hash ().to_string ());
	request.put ("work", nano::to_string_hex (*node->work_generate_blocking (key.pub)));
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string state_hash (response.json.get<std::string> ("hash"));
	auto state_text (response.json.get<std::string> ("block"));
	std::stringstream block_stream (state_text);
	boost::property_tree::ptree block_l;
	boost::property_tree::read_json (block_stream, block_l);
	auto state_block (nano::deserialize_block_json (block_l));
	ASSERT_NE (nullptr, state_block);
	ASSERT_EQ (nano::block_type::state, state_block->type ());
	ASSERT_EQ (state_hash, state_block->hash ().to_string ());
	auto difficulty (state_block->difficulty ());
	ASSERT_GT (difficulty, nano::work_threshold (state_block->work_version (), nano::block_details (nano::epoch::epoch_0, false, true, false)));
	ASSERT_TRUE (node->latest (key.pub).is_zero ());
	scoped_thread_name_io.reset ();
	auto process_result (node->process (*state_block));
	ASSERT_EQ (nano::process_result::progress, process_result.code);
	ASSERT_EQ (state_block->sideband ().details.epoch, nano::epoch::epoch_0);
	ASSERT_TRUE (state_block->sideband ().details.is_receive);
	ASSERT_FALSE (node->latest (key.pub).is_zero ());
}

// Missing "work" parameter should cause work to be generated for us.
TEST (rpc, block_create_state_request_work)
{
	nano::genesis genesis;

	// Test work generation for state blocks both with and without previous (in the latter
	// case, the account will be used for work generation)
	std::vector<std::string> previous_test_input{ genesis.hash ().to_string (), std::string ("0") };
	for (auto previous : previous_test_input)
	{
		nano::system system;
		auto node = add_ipc_enabled_node (system);
		nano::keypair key;
		nano::genesis genesis;
		system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
		scoped_io_thread_name_change scoped_thread_name_io;
		boost::property_tree::ptree request;
		request.put ("action", "block_create");
		request.put ("type", "state");
		request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
		request.put ("account", nano::dev_genesis_key.pub.to_account ());
		request.put ("representative", nano::dev_genesis_key.pub.to_account ());
		request.put ("balance", (nano::genesis_amount - nano::Gxrb_ratio).convert_to<std::string> ());
		request.put ("link", key.pub.to_account ());
		request.put ("previous", previous);
		nano::node_rpc_config node_rpc_config;
		nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
		nano::rpc_config rpc_config (nano::get_available_port (), true);
		rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
		nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
		nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
		rpc.start ();
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		boost::property_tree::ptree block_l;
		std::stringstream block_stream (response.json.get<std::string> ("block"));
		boost::property_tree::read_json (block_stream, block_l);
		auto block (nano::deserialize_block_json (block_l));
		ASSERT_NE (nullptr, block);
		ASSERT_GE (block->difficulty (), node->default_difficulty (nano::work_version::work_1));
	}
}

TEST (rpc, block_create_open_epoch_v2)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	nano::genesis genesis;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1));
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));
	auto send_block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("key", key.prv.data.to_string ());
	request.put ("account", key.pub.to_account ());
	request.put ("previous", 0);
	request.put ("representative", nano::dev_genesis_key.pub.to_account ());
	request.put ("balance", nano::Gxrb_ratio.convert_to<std::string> ());
	request.put ("link", send_block->hash ().to_string ());
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string state_hash (response.json.get<std::string> ("hash"));
	auto state_text (response.json.get<std::string> ("block"));
	std::stringstream block_stream (state_text);
	boost::property_tree::ptree block_l;
	boost::property_tree::read_json (block_stream, block_l);
	auto state_block (nano::deserialize_block_json (block_l));
	ASSERT_NE (nullptr, state_block);
	ASSERT_EQ (nano::block_type::state, state_block->type ());
	ASSERT_EQ (state_hash, state_block->hash ().to_string ());
	auto difficulty (state_block->difficulty ());
	ASSERT_GT (difficulty, nano::work_threshold (state_block->work_version (), nano::block_details (nano::epoch::epoch_2, false, true, false)));
	ASSERT_TRUE (node->latest (key.pub).is_zero ());
	scoped_thread_name_io.reset ();
	auto process_result (node->process (*state_block));
	ASSERT_EQ (nano::process_result::progress, process_result.code);
	ASSERT_EQ (state_block->sideband ().details.epoch, nano::epoch::epoch_2);
	ASSERT_TRUE (state_block->sideband ().details.is_receive);
	ASSERT_FALSE (node->latest (key.pub).is_zero ());
}

TEST (rpc, block_create_receive_epoch_v2)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	nano::genesis genesis;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1));
	auto send_block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	nano::state_block open (key.pub, 0, nano::dev_genesis_key.pub, nano::Gxrb_ratio, send_block->hash (), key.prv, key.pub, *node->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node->process (open).code);
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));
	auto send_block_2 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("key", key.prv.data.to_string ());
	request.put ("account", key.pub.to_account ());
	request.put ("previous", open.hash ().to_string ());
	request.put ("representative", nano::dev_genesis_key.pub.to_account ());
	request.put ("balance", (2 * nano::Gxrb_ratio).convert_to<std::string> ());
	request.put ("link", send_block_2->hash ().to_string ());
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string state_hash (response.json.get<std::string> ("hash"));
	auto state_text (response.json.get<std::string> ("block"));
	std::stringstream block_stream (state_text);
	boost::property_tree::ptree block_l;
	boost::property_tree::read_json (block_stream, block_l);
	auto state_block (nano::deserialize_block_json (block_l));
	ASSERT_NE (nullptr, state_block);
	ASSERT_EQ (nano::block_type::state, state_block->type ());
	ASSERT_EQ (state_hash, state_block->hash ().to_string ());
	auto difficulty (state_block->difficulty ());
	ASSERT_GT (difficulty, nano::work_threshold (state_block->work_version (), nano::block_details (nano::epoch::epoch_2, false, true, false)));
	scoped_thread_name_io.reset ();
	auto process_result (node->process (*state_block));
	ASSERT_EQ (nano::process_result::progress, process_result.code);
	ASSERT_EQ (state_block->sideband ().details.epoch, nano::epoch::epoch_2);
	ASSERT_TRUE (state_block->sideband ().details.is_receive);
	ASSERT_FALSE (node->latest (key.pub).is_zero ());
}

TEST (rpc, block_create_send_epoch_v2)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	nano::genesis genesis;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1));
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));
	auto send_block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	nano::state_block open (key.pub, 0, nano::dev_genesis_key.pub, nano::Gxrb_ratio, send_block->hash (), key.prv, key.pub, *node->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node->process (open).code);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("key", key.prv.data.to_string ());
	request.put ("account", key.pub.to_account ());
	request.put ("previous", open.hash ().to_string ());
	request.put ("representative", nano::dev_genesis_key.pub.to_account ());
	request.put ("balance", 0);
	request.put ("link", nano::dev_genesis_key.pub.to_string ());
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string state_hash (response.json.get<std::string> ("hash"));
	auto state_text (response.json.get<std::string> ("block"));
	std::stringstream block_stream (state_text);
	boost::property_tree::ptree block_l;
	boost::property_tree::read_json (block_stream, block_l);
	auto state_block (nano::deserialize_block_json (block_l));
	ASSERT_NE (nullptr, state_block);
	ASSERT_EQ (nano::block_type::state, state_block->type ());
	ASSERT_EQ (state_hash, state_block->hash ().to_string ());
	auto difficulty (state_block->difficulty ());
	ASSERT_GT (difficulty, nano::work_threshold (state_block->work_version (), nano::block_details (nano::epoch::epoch_2, true, false, false)));
	scoped_thread_name_io.reset ();
	auto process_result (node->process (*state_block));
	ASSERT_EQ (nano::process_result::progress, process_result.code);
	ASSERT_EQ (state_block->sideband ().details.epoch, nano::epoch::epoch_2);
	ASSERT_TRUE (state_block->sideband ().details.is_send);
	ASSERT_FALSE (node->latest (key.pub).is_zero ());
}

TEST (rpc, block_hash)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "block_hash");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string send_hash (response.json.get<std::string> ("hash"));
	ASSERT_EQ (send.hash ().to_string (), send_hash);
}

TEST (rpc, wallet_lock)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	{
		auto transaction (system.wallet (0)->wallets.tx_begin_read ());
		ASSERT_TRUE (system.wallet (0)->store.valid_password (transaction));
	}
	request.put ("wallet", wallet);
	request.put ("action", "wallet_lock");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string account_text1 (response.json.get<std::string> ("locked"));
	ASSERT_EQ (account_text1, "1");
	auto transaction (system.wallet (0)->wallets.tx_begin_read ());
	ASSERT_FALSE (system.wallet (0)->store.valid_password (transaction));
}

TEST (rpc, wallet_locked)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_locked");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string account_text1 (response.json.get<std::string> ("locked"));
	ASSERT_EQ (account_text1, "0");
}

TEST (rpc, wallet_create_fail)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	// lmdb_max_dbs should be removed once the wallet store is refactored to support more wallets.
	for (int i = 0; i < 127; i++)
	{
		node->wallets.create (nano::random_wallet_id ());
	}
	rpc.start ();
	scoped_io_thread_name_change scoped_thread_name_io;
	boost::property_tree::ptree request;
	request.put ("action", "wallet_create");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (std::error_code (nano::error_common::wallet_lmdb_max_dbs).message (), response.json.get<std::string> ("error"));
}

TEST (rpc, wallet_ledger)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	nano::genesis genesis;
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1.latest (nano::dev_genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node1.work_generate_blocking (latest));
	node1.process (send);
	nano::open_block open (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, *node1.work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1.process (open).code);
	auto time (nano::seconds_since_epoch ());
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_ledger");
	request.put ("wallet", node1.wallets.items.begin ()->first.to_string ());
	request.put ("sorting", "1");
	request.put ("count", "1");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	for (auto & accounts : response.json.get_child ("accounts"))
	{
		std::string account_text (accounts.first);
		ASSERT_EQ (key.pub.to_account (), account_text);
		std::string frontier (accounts.second.get<std::string> ("frontier"));
		ASSERT_EQ (open.hash ().to_string (), frontier);
		std::string open_block (accounts.second.get<std::string> ("open_block"));
		ASSERT_EQ (open.hash ().to_string (), open_block);
		std::string representative_block (accounts.second.get<std::string> ("representative_block"));
		ASSERT_EQ (open.hash ().to_string (), representative_block);
		std::string balance_text (accounts.second.get<std::string> ("balance"));
		ASSERT_EQ ("340282366920938463463374607431768211355", balance_text);
		std::string modified_timestamp (accounts.second.get<std::string> ("modified_timestamp"));
		ASSERT_LT (std::abs ((long)time - stol (modified_timestamp)), 5);
		std::string block_count (accounts.second.get<std::string> ("block_count"));
		ASSERT_EQ ("1", block_count);
		boost::optional<std::string> weight (accounts.second.get_optional<std::string> ("weight"));
		ASSERT_FALSE (weight.is_initialized ());
		boost::optional<std::string> pending (accounts.second.get_optional<std::string> ("pending"));
		ASSERT_FALSE (pending.is_initialized ());
		boost::optional<std::string> representative (accounts.second.get_optional<std::string> ("representative"));
		ASSERT_FALSE (representative.is_initialized ());
	}
	// Test for optional values
	request.put ("weight", "true");
	request.put ("pending", "1");
	request.put ("representative", "false");
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	for (auto & accounts : response2.json.get_child ("accounts"))
	{
		boost::optional<std::string> weight (accounts.second.get_optional<std::string> ("weight"));
		ASSERT_TRUE (weight.is_initialized ());
		ASSERT_EQ ("0", weight.get ());
		boost::optional<std::string> pending (accounts.second.get_optional<std::string> ("pending"));
		ASSERT_TRUE (pending.is_initialized ());
		ASSERT_EQ ("0", pending.get ());
		boost::optional<std::string> representative (accounts.second.get_optional<std::string> ("representative"));
		ASSERT_FALSE (representative.is_initialized ());
	}
}

TEST (rpc, wallet_add_watch)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_add_watch");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", nano::dev_genesis_key.pub.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", peers_l);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string success (response.json.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
	ASSERT_TRUE (system.wallet (0)->exists (nano::dev_genesis_key.pub));

	// Make sure using special wallet key as pubkey fails
	nano::public_key bad_key (1);
	entry.put ("", bad_key.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.erase ("accounts");
	request.add_child ("accounts", peers_l);

	test_response response_error (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response_error.status != 0);
	ASSERT_EQ (200, response_error.status);
	std::error_code ec (nano::error_common::bad_public_key);
	ASSERT_EQ (response_error.json.get<std::string> ("error"), ec.message ());
}

TEST (rpc, online_reps)
{
	nano::system system (1);
	auto node1 (system.nodes[0]);
	auto node2 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	ASSERT_TRUE (node2->online_reps.online_stake () == node2->config.online_weight_minimum.number ());
	auto send_block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	scoped_io_thread_name_change scoped_thread_name_io;
	ASSERT_TIMELY (10s, !node2->online_reps.list ().empty ());
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*system.nodes[1], node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node2->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "representatives_online");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto representatives (response.json.get_child ("representatives"));
	auto item (representatives.begin ());
	ASSERT_NE (representatives.end (), item);
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), item->second.get<std::string> (""));
	boost::optional<std::string> weight (item->second.get_optional<std::string> ("weight"));
	ASSERT_FALSE (weight.is_initialized ());
	ASSERT_TIMELY (5s, node2->block (send_block->hash ()));
	//Test weight option
	request.put ("weight", "true");
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	auto representatives2 (response2.json.get_child ("representatives"));
	auto item2 (representatives2.begin ());
	ASSERT_NE (representatives2.end (), item2);
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), item2->first);
	auto weight2 (item2->second.get<std::string> ("weight"));
	ASSERT_EQ (node2->weight (nano::dev_genesis_key.pub).convert_to<std::string> (), weight2);
	//Test accounts filter
	scoped_thread_name_io.reset ();
	auto new_rep (system.wallet (1)->deterministic_insert ());
	auto send (system.wallet (0)->send_action (nano::dev_genesis_key.pub, new_rep, node1->config.receive_minimum.number ()));
	scoped_thread_name_io.renew ();
	ASSERT_NE (nullptr, send);
	ASSERT_TIMELY (10s, node2->block (send->hash ()));
	scoped_thread_name_io.reset ();
	auto receive (system.wallet (1)->receive_action (*send, new_rep, node1->config.receive_minimum.number ()));
	scoped_thread_name_io.renew ();
	ASSERT_NE (nullptr, receive);
	ASSERT_TIMELY (5s, node2->block (receive->hash ()));
	scoped_thread_name_io.reset ();
	auto change (system.wallet (0)->change_action (nano::dev_genesis_key.pub, new_rep));
	scoped_thread_name_io.renew ();
	ASSERT_NE (nullptr, change);
	ASSERT_TIMELY (5s, node2->block (change->hash ()));
	ASSERT_TIMELY (5s, node2->online_reps.list ().size () == 2);
	boost::property_tree::ptree child_rep;
	child_rep.put ("", new_rep.to_account ());
	boost::property_tree::ptree filtered_accounts;
	filtered_accounts.push_back (std::make_pair ("", child_rep));
	request.add_child ("accounts", filtered_accounts);
	test_response response3 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response3.status != 0);
	auto representatives3 (response3.json.get_child ("representatives"));
	auto item3 (representatives3.begin ());
	ASSERT_NE (representatives3.end (), item3);
	ASSERT_EQ (new_rep.to_account (), item3->first);
	ASSERT_EQ (representatives3.size (), 1);
	node2->stop ();
}

TEST (rpc, confirmation_height_currently_processing)
{
	nano::system system;
	nano::node_config node_config;
	node_config.flags.force_use_write_database_queue = true;
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;

	auto node = add_ipc_enabled_node (system, node_config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);

	auto previous_genesis_chain_hash = node->latest (nano::dev_genesis_key.pub);
	{
		auto transaction = node->store.tx_begin_write ();
		nano::keypair key1;
		nano::send_block send (previous_genesis_chain_hash, key1.pub, nano::genesis_amount - nano::Gxrb_ratio - 1, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (previous_genesis_chain_hash));
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
		previous_genesis_chain_hash = send.hash ();
	}

	scoped_io_thread_name_change scoped_thread_name_io;

	std::shared_ptr<nano::block> frontier;
	{
		auto transaction = node->store.tx_begin_read ();
		frontier = node->store.block_get (transaction, previous_genesis_chain_hash);
	}

	boost::property_tree::ptree request;
	request.put ("action", "confirmation_height_currently_processing");

	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();

	// Begin process for confirming the block (and setting confirmation height)
	{
		// Write guard prevents the confirmation height processor writing the blocks, so that we can inspect contents during the response
		auto write_guard = node->write_database_queue.wait (nano::writer::testing);
		node->block_confirm (frontier);

		ASSERT_TIMELY (10s, node->confirmation_height_processor.current () == frontier->hash ());

		// Make the request
		{
			test_response response (request, rpc.config.port, system.env.ctx);
			ASSERT_TIMELY (10s, response.status != 0);
			ASSERT_EQ (200, response.status);
			auto hash (response.json.get<std::string> ("hash"));
			ASSERT_EQ (frontier->hash ().to_string (), hash);
		}
	}

	// Wait until confirmation has been set and not processing anything
	ASSERT_TIMELY (10s, node->confirmation_height_processor.current ().is_zero () && node->confirmation_height_processor.awaiting_processing_size () == 0);

	// Make the same request, it should now return an error
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::error_code ec (nano::error_rpc::confirmation_height_not_processing);
		ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, confirmation_history)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	ASSERT_TRUE (node->active.list_recently_cemented ().empty ());
	auto block (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	scoped_io_thread_name_change scoped_thread_name_io;
	ASSERT_TIMELY (10s, !node->active.list_recently_cemented ().empty ());
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "confirmation_history");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto representatives (response.json.get_child ("confirmations"));
	auto item (representatives.begin ());
	ASSERT_NE (representatives.end (), item);
	auto hash (item->second.get<std::string> ("hash"));
	auto tally (item->second.get<std::string> ("tally"));
	ASSERT_EQ (1, item->second.count ("duration"));
	ASSERT_EQ (1, item->second.count ("time"));
	ASSERT_EQ (1, item->second.count ("request_count"));
	ASSERT_EQ (1, item->second.count ("voters"));
	ASSERT_GE (1, item->second.get<unsigned> ("blocks"));
	ASSERT_EQ (block->hash ().to_string (), hash);
	nano::amount tally_num;
	tally_num.decode_dec (tally);
	debug_assert (tally_num == nano::genesis_amount || tally_num == (nano::genesis_amount - nano::Gxrb_ratio));
	system.stop ();
}

TEST (rpc, confirmation_history_hash)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	ASSERT_TRUE (node->active.list_recently_cemented ().empty ());
	auto send1 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	auto send2 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	auto send3 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, nano::Gxrb_ratio));
	scoped_io_thread_name_change scoped_thread_name_io;
	ASSERT_TIMELY (10s, node->active.list_recently_cemented ().size () == 3);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "confirmation_history");
	request.put ("hash", send2->hash ().to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto representatives (response.json.get_child ("confirmations"));
	ASSERT_EQ (representatives.size (), 1);
	auto item (representatives.begin ());
	ASSERT_NE (representatives.end (), item);
	auto hash (item->second.get<std::string> ("hash"));
	auto tally (item->second.get<std::string> ("tally"));
	ASSERT_FALSE (item->second.get<std::string> ("duration", "").empty ());
	ASSERT_FALSE (item->second.get<std::string> ("time", "").empty ());
	ASSERT_EQ (send2->hash ().to_string (), hash);
	nano::amount tally_num;
	tally_num.decode_dec (tally);
	debug_assert (tally_num == nano::genesis_amount || tally_num == (nano::genesis_amount - nano::Gxrb_ratio) || tally_num == (nano::genesis_amount - 2 * nano::Gxrb_ratio) || tally_num == (nano::genesis_amount - 3 * nano::Gxrb_ratio));
	system.stop ();
}

TEST (rpc, block_confirm)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::genesis genesis;
	auto send1 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, genesis.hash (), nano::dev_genesis_key.pub, nano::genesis_amount - nano::Gxrb_ratio, nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *node->work_generate_blocking (genesis.hash ())));
	{
		auto transaction (node->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send1).code);
	}
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "block_confirm");
	request.put ("hash", send1->hash ().to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ ("1", response.json.get<std::string> ("started"));
}

TEST (rpc, block_confirm_absent)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "block_confirm");
	request.put ("hash", "0");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response.json.get<std::string> ("error"));
}

TEST (rpc, block_confirm_confirmed)
{
	nano::system system (1);
	auto path (nano::unique_path ());
	nano::node_config config;
	config.callback_address = "localhost";
	config.callback_port = nano::get_available_port ();
	config.callback_target = "/";
	config.logging.init (path);
	auto node = add_ipc_enabled_node (system, config);
	nano::genesis genesis;
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, genesis.hash ()));
	}
	ASSERT_EQ (0, node->stats.count (nano::stat::type::error, nano::stat::detail::http_callback, nano::stat::dir::out));
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "block_confirm");
	request.put ("hash", genesis.hash ().to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ ("1", response.json.get<std::string> ("started"));
	// Check confirmation history
	auto confirmed (node->active.list_recently_cemented ());
	ASSERT_EQ (1, confirmed.size ());
	ASSERT_EQ (nano::genesis_hash, confirmed.begin ()->winner->hash ());
	// Check callback
	ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::error, nano::stat::detail::http_callback, nano::stat::dir::out) != 0);
	// Callback result is error because callback target port isn't listening
	ASSERT_EQ (1, node->stats.count (nano::stat::type::error, nano::stat::detail::http_callback, nano::stat::dir::out));
	node->stop ();
}

TEST (rpc, node_id)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "node_id");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ (node->node_id.prv.data.to_string (), response.json.get<std::string> ("private"));
	ASSERT_EQ (node->node_id.pub.to_account (), response.json.get<std::string> ("as_account"));
	ASSERT_EQ (node->node_id.pub.to_node_id (), response.json.get<std::string> ("node_id"));
}

TEST (rpc, stats_clear)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	node->stats.inc (nano::stat::type::ledger, nano::stat::dir::in);
	ASSERT_EQ (1, node->stats.count (nano::stat::type::ledger, nano::stat::dir::in));
	boost::property_tree::ptree request;
	request.put ("action", "stats_clear");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	std::string success (response.json.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
	ASSERT_EQ (0, node->stats.count (nano::stat::type::ledger, nano::stat::dir::in));
	ASSERT_LE (node->stats.last_reset ().count (), 5);
}

TEST (rpc, unchecked)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);
	nano::keypair key;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	auto open (std::make_shared<nano::state_block> (key.pub, 0, key.pub, 1, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub)));
	auto open2 (std::make_shared<nano::state_block> (key.pub, 0, key.pub, 2, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub)));
	node.process_active (open);
	node.process_active (open2);
	node.block_processor.flush ();
	boost::property_tree::ptree request;
	request.put ("action", "unchecked");
	request.put ("count", 2);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & blocks (response.json.get_child ("blocks"));
		ASSERT_EQ (2, blocks.size ());
		ASSERT_EQ (1, blocks.count (open->hash ().to_string ()));
		ASSERT_EQ (1, blocks.count (open2->hash ().to_string ()));
	}
	request.put ("json_block", true);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & blocks (response.json.get_child ("blocks"));
		ASSERT_EQ (2, blocks.size ());
		auto & open_block (blocks.get_child (open->hash ().to_string ()));
		ASSERT_EQ ("state", open_block.get<std::string> ("type"));
	}
}

TEST (rpc, unchecked_get)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);
	nano::keypair key;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	auto open (std::make_shared<nano::state_block> (key.pub, 0, key.pub, 1, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub)));
	node.process_active (open);
	node.block_processor.flush ();
	boost::property_tree::ptree request;
	request.put ("action", "unchecked_get");
	request.put ("hash", open->hash ().to_string ());
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (1, response.json.count ("contents"));
		auto timestamp (response.json.get<uint64_t> ("modified_timestamp"));
		ASSERT_LE (timestamp, nano::seconds_since_epoch ());
	}
	request.put ("json_block", true);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & contents (response.json.get_child ("contents"));
		ASSERT_EQ ("state", contents.get<std::string> ("type"));
		auto timestamp (response.json.get<uint64_t> ("modified_timestamp"));
		ASSERT_LE (timestamp, nano::seconds_since_epoch ());
	}
}

TEST (rpc, unchecked_clear)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);
	nano::keypair key;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	auto open (std::make_shared<nano::state_block> (key.pub, 0, key.pub, 1, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub)));
	node.process_active (open);
	node.block_processor.flush ();
	boost::property_tree::ptree request;
	{
		ASSERT_EQ (node.store.unchecked_count (node.store.tx_begin_read ()), 1);
	}
	request.put ("action", "unchecked_clear");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);

	ASSERT_TIMELY (10s, node.store.unchecked_count (node.store.tx_begin_read ()) == 0);
}

TEST (rpc, unopened)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::account account1 (1), account2 (account1.number () + 1);
	auto genesis (node->latest (nano::dev_genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto send (system.wallet (0)->send_action (nano::dev_genesis_key.pub, account1, 1));
	ASSERT_NE (nullptr, send);
	auto send2 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, account2, 10));
	ASSERT_NE (nullptr, send2);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	{
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & accounts (response.json.get_child ("accounts"));
		ASSERT_EQ (2, accounts.size ());
		ASSERT_EQ ("1", accounts.get<std::string> (account1.to_account ()));
		ASSERT_EQ ("10", accounts.get<std::string> (account2.to_account ()));
	}
	{
		// starting at second account should get a single result
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		request.put ("account", account2.to_account ());
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & accounts (response.json.get_child ("accounts"));
		ASSERT_EQ (1, accounts.size ());
		ASSERT_EQ ("10", accounts.get<std::string> (account2.to_account ()));
	}
	{
		// starting at third account should get no results
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		request.put ("account", nano::account (account2.number () + 1).to_account ());
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & accounts (response.json.get_child ("accounts"));
		ASSERT_EQ (0, accounts.size ());
	}
	{
		// using count=1 should get a single result
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		request.put ("count", "1");
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & accounts (response.json.get_child ("accounts"));
		ASSERT_EQ (1, accounts.size ());
		ASSERT_EQ ("1", accounts.get<std::string> (account1.to_account ()));
	}
	{
		// using threshold at 5 should get a single result
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		request.put ("threshold", 5);
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & accounts (response.json.get_child ("accounts"));
		ASSERT_EQ (1, accounts.size ());
		ASSERT_EQ ("10", accounts.get<std::string> (account2.to_account ()));
	}
}

TEST (rpc, unopened_burn)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto genesis (node->latest (nano::dev_genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto send (system.wallet (0)->send_action (nano::dev_genesis_key.pub, nano::burn_account, 1));
	ASSERT_NE (nullptr, send);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "unopened");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & accounts (response.json.get_child ("accounts"));
	ASSERT_EQ (0, accounts.size ());
}

TEST (rpc, unopened_no_accounts)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "unopened");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto & accounts (response.json.get_child ("accounts"));
	ASSERT_EQ (0, accounts.size ());
}

TEST (rpc, uptime)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "uptime");
	std::this_thread::sleep_for (std::chrono::seconds (1));
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_LE (1, response.json.get<int> ("seconds"));
}

TEST (rpc, wallet_history)
{
	nano::system system;
	nano::node_config node_config;
	node_config.enable_voting = false;
	auto node = add_ipc_enabled_node (system, node_config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto timestamp1 (nano::seconds_since_epoch ());
	auto send (system.wallet (0)->send_action (nano::dev_genesis_key.pub, nano::dev_genesis_key.pub, node->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send);
	auto timestamp2 (nano::seconds_since_epoch ());
	auto receive (system.wallet (0)->receive_action (*send, nano::dev_genesis_key.pub, node->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, receive);
	nano::keypair key;
	auto timestamp3 (nano::seconds_since_epoch ());
	auto send2 (system.wallet (0)->send_action (nano::dev_genesis_key.pub, key.pub, node->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send2);
	system.deadline_set (10s);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "wallet_history");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string, std::string>> history_l;
	auto & history_node (response.json.get_child ("history"));
	for (auto i (history_node.begin ()), n (history_node.end ()); i != n; ++i)
	{
		history_l.push_back (std::make_tuple (i->second.get<std::string> ("type"), i->second.get<std::string> ("account"), i->second.get<std::string> ("amount"), i->second.get<std::string> ("hash"), i->second.get<std::string> ("block_account"), i->second.get<std::string> ("local_timestamp")));
	}
	ASSERT_EQ (4, history_l.size ());
	ASSERT_EQ ("send", std::get<0> (history_l[0]));
	ASSERT_EQ (key.pub.to_account (), std::get<1> (history_l[0]));
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), std::get<2> (history_l[0]));
	ASSERT_EQ (send2->hash ().to_string (), std::get<3> (history_l[0]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<4> (history_l[0]));
	ASSERT_EQ (std::to_string (timestamp3), std::get<5> (history_l[0]));
	ASSERT_EQ ("receive", std::get<0> (history_l[1]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[1]));
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), std::get<2> (history_l[1]));
	ASSERT_EQ (receive->hash ().to_string (), std::get<3> (history_l[1]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<4> (history_l[1]));
	ASSERT_EQ (std::to_string (timestamp2), std::get<5> (history_l[1]));
	ASSERT_EQ ("send", std::get<0> (history_l[2]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[2]));
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), std::get<2> (history_l[2]));
	ASSERT_EQ (send->hash ().to_string (), std::get<3> (history_l[2]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<4> (history_l[2]));
	ASSERT_EQ (std::to_string (timestamp1), std::get<5> (history_l[2]));
	// Genesis block
	ASSERT_EQ ("receive", std::get<0> (history_l[3]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<1> (history_l[3]));
	ASSERT_EQ (nano::genesis_amount.convert_to<std::string> (), std::get<2> (history_l[3]));
	ASSERT_EQ (nano::genesis_hash.to_string (), std::get<3> (history_l[3]));
	ASSERT_EQ (nano::dev_genesis_key.pub.to_account (), std::get<4> (history_l[3]));
}

TEST (rpc, sign_hash)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	nano::state_block send (nano::genesis_account, node1.latest (nano::dev_genesis_key.pub), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, 0);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "sign");
	request.put ("hash", send.hash ().to_string ());
	request.put ("key", key.prv.data.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::error_code ec (nano::error_rpc::sign_hash_disabled);
	ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	node_rpc_config.enable_sign_hash = true;
	test_response response2 (request, rpc.config.port, system.env.ctx);
	system.deadline_set (10s);
	ASSERT_TIMELY (10s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	nano::signature signature;
	std::string signature_text (response2.json.get<std::string> ("signature"));
	ASSERT_FALSE (signature.decode_hex (signature_text));
	ASSERT_FALSE (nano::validate_message (key.pub, send.hash (), signature));
}

TEST (rpc, sign_block)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (key.prv);
	nano::state_block send (nano::genesis_account, node1.latest (nano::dev_genesis_key.pub), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, 0);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "sign");
	std::string wallet;
	node1.wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("account", key.pub.to_account ());
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response.status != 0);
	ASSERT_EQ (200, response.status);
	auto contents (response.json.get<std::string> ("block"));
	boost::property_tree::ptree block_l;
	std::stringstream block_stream (contents);
	boost::property_tree::read_json (block_stream, block_l);
	auto block (nano::deserialize_block_json (block_l));
	ASSERT_FALSE (nano::validate_message (key.pub, send.hash (), block->block_signature ()));
	ASSERT_NE (block->block_signature (), send.block_signature ());
	ASSERT_EQ (block->hash (), send.hash ());
}

TEST (rpc, memory_stats)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);

	// Preliminary test adding to the vote uniquer and checking json output is correct
	nano::keypair key;
	auto block (std::make_shared<nano::state_block> (0, 0, 0, 0, 0, key.prv, key.pub, 0));
	std::vector<nano::block_hash> hashes;
	hashes.push_back (block->hash ());
	auto vote (std::make_shared<nano::vote> (key.pub, key.prv, 0, hashes));
	node->vote_uniquer.unique (vote);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "stats");
	request.put ("type", "objects");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);

		ASSERT_EQ (response.json.get_child ("node").get_child ("vote_uniquer").get_child ("votes").get<std::string> ("count"), "1");
	}

	request.put ("type", "database");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_TRUE (!response.json.empty ());
	}
}

TEST (rpc, block_confirmed)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "block_info");
	request.put ("hash", "bad_hash1337");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ (std::error_code (nano::error_blocks::invalid_block_hash).message (), response.json.get<std::string> ("error"));

	request.put ("hash", "0");
	test_response response1 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response1.json.get<std::string> ("error"));

	scoped_thread_name_io.reset ();
	nano::keypair key;

	// Open an account directly in the ledger
	{
		auto transaction = node->store.tx_begin_write ();
		nano::block_hash latest (node->latest (nano::dev_genesis_key.pub));
		nano::send_block send1 (latest, key.pub, 300, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (latest));
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);

		nano::open_block open1 (send1.hash (), nano::genesis_account, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub));
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open1).code);
	}
	scoped_thread_name_io.renew ();

	// This should not be confirmed
	nano::block_hash latest (node->latest (nano::dev_genesis_key.pub));
	request.put ("hash", latest.to_string ());
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);

	ASSERT_EQ (200, response2.status);
	ASSERT_FALSE (response2.json.get<bool> ("confirmed"));

	// Create and process a new send block
	auto send = std::make_shared<nano::send_block> (latest, key.pub, 10, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (latest));
	node->process_active (send);
	node->block_processor.flush ();
	node->block_confirm (send);
	{
		auto election = node->active.election (send->qualified_root ());
		ASSERT_NE (nullptr, election);
		nano::lock_guard<std::mutex> guard (node->active.mutex);
		election->confirm_once ();
	}

	// Wait until the confirmation height has been set
	ASSERT_TIMELY (10s, node->ledger.block_confirmed (node->store.tx_begin_read (), send->hash ()) && !node->confirmation_height_processor.is_processing_block (send->hash ()));

	// Requesting confirmation for this should now succeed
	request.put ("hash", send->hash ().to_string ());
	test_response response3 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response3.status != 0);
	ASSERT_EQ (200, response3.status);
	ASSERT_TRUE (response3.json.get<bool> ("confirmed"));
}

TEST (rpc, database_txn_tracker)
{
	// Don't test this with the rocksdb backend
	auto use_rocksdb_str = std::getenv ("TEST_USE_ROCKSDB");
	if (use_rocksdb_str && boost::lexical_cast<int> (use_rocksdb_str) == 1)
	{
		return;
	}

	// First try when database tracking is disabled
	{
		nano::system system;
		auto node = add_ipc_enabled_node (system);
		scoped_io_thread_name_change scoped_thread_name_io;
		nano::node_rpc_config node_rpc_config;
		nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
		nano::rpc_config rpc_config (nano::get_available_port (), true);
		rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
		nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
		nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
		rpc.start ();

		boost::property_tree::ptree request;
		request.put ("action", "database_txn_tracker");
		{
			test_response response (request, rpc.config.port, system.env.ctx);
			ASSERT_TIMELY (5s, response.status != 0);
			ASSERT_EQ (200, response.status);
			std::error_code ec (nano::error_common::tracking_not_enabled);
			ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
		}
	}

	// Now try enabling it but with invalid amounts
	nano::system system;
	nano::node_config node_config;
	node_config.diagnostics_config.txn_tracking.enable = true;
	auto node = add_ipc_enabled_node (system, node_config);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();

	boost::property_tree::ptree request;
	auto check_not_correct_amount = [&system, &request, &rpc_port = rpc.config.port]() {
		test_response response (request, rpc_port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		std::error_code ec (nano::error_common::invalid_amount);
		ASSERT_EQ (response.json.get<std::string> ("error"), ec.message ());
	};

	request.put ("action", "database_txn_tracker");
	request.put ("min_read_time", "not a time");
	check_not_correct_amount ();

	// Read is valid now, but write isn't
	request.put ("min_read_time", "1000");
	request.put ("min_write_time", "bad time");
	check_not_correct_amount ();

	// Now try where times are large unattainable numbers
	request.put ("min_read_time", "1000000");
	request.put ("min_write_time", "1000000");

	std::promise<void> keep_txn_alive_promise;
	std::promise<void> txn_created_promise;
	std::thread thread ([& store = node->store, &keep_txn_alive_promise, &txn_created_promise]() {
		// Use rpc_process_container as a placeholder as this thread is only instantiated by the daemon so won't be used
		nano::thread_role::set (nano::thread_role::name::rpc_process_container);

		// Create a read transaction to test
		auto read_tx = store.tx_begin_read ();
		// Sleep so that the read transaction has been alive for at least 1 seconds. A write lock is not used in this test as it can cause a deadlock with
		// other writes done in the background
		std::this_thread::sleep_for (1s);
		txn_created_promise.set_value ();
		keep_txn_alive_promise.get_future ().wait ();
	});

	txn_created_promise.get_future ().wait ();

	// Adjust minimum read time so that it can detect the read transaction being opened
	request.put ("min_read_time", "1000");
	test_response response (request, rpc.config.port, system.env.ctx);
	// It can take a long time to generate stack traces
	ASSERT_TIMELY (60s, response.status != 0);
	ASSERT_EQ (200, response.status);
	keep_txn_alive_promise.set_value ();
	std::vector<std::tuple<std::string, std::string, std::string, std::vector<std::tuple<std::string, std::string, std::string, std::string>>>> json_l;
	auto & json_node (response.json.get_child ("txn_tracking"));
	for (auto & stat : json_node)
	{
		auto & stack_trace = stat.second.get_child ("stacktrace");
		std::vector<std::tuple<std::string, std::string, std::string, std::string>> frames_json_l;
		for (auto & frame : stack_trace)
		{
			frames_json_l.emplace_back (frame.second.get<std::string> ("name"), frame.second.get<std::string> ("address"), frame.second.get<std::string> ("source_file"), frame.second.get<std::string> ("source_line"));
		}

		json_l.emplace_back (stat.second.get<std::string> ("thread"), stat.second.get<std::string> ("time_held_open"), stat.second.get<std::string> ("write"), std::move (frames_json_l));
	}

	ASSERT_EQ (1, json_l.size ());
	auto thread_name = nano::thread_role::get_string (nano::thread_role::name::rpc_process_container);
	// Should only have a read transaction
	ASSERT_EQ (thread_name, std::get<0> (json_l.front ()));
	ASSERT_LE (1000u, boost::lexical_cast<unsigned> (std::get<1> (json_l.front ())));
	ASSERT_EQ ("false", std::get<2> (json_l.front ()));
	// Due to results being different for different compilers/build options we cannot reliably check the contents.
	// The best we can do is just check that there are entries.
	ASSERT_TRUE (!std::get<3> (json_l.front ()).empty ());
	thread.join ();
}

TEST (rpc, active_difficulty)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	// "Start" epoch 2
	node->ledger.cache.epoch_2_started = true;
	ASSERT_EQ (node->default_difficulty (nano::work_version::work_1), node->env.constants.network.publish_thresholds.epoch_2);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "active_difficulty");
	nano::unique_lock<std::mutex> lock (node->active.mutex);
	node->active.multipliers_cb.push_front (1.5);
	node->active.multipliers_cb.push_front (4.2);
	// Also pushes 1.0 to the front of multipliers_cb
	node->active.update_active_multiplier (lock);
	lock.unlock ();
	auto trend_size (node->active.multipliers_cb.size ());
	ASSERT_NE (0, trend_size);
	auto expected_multiplier{ (1.5 + 4.2 + (trend_size - 2) * 1) / trend_size };
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto network_minimum_text (response.json.get<std::string> ("network_minimum"));
		uint64_t network_minimum;
		ASSERT_FALSE (nano::from_string_hex (network_minimum_text, network_minimum));
		ASSERT_EQ (node->default_difficulty (nano::work_version::work_1), network_minimum);
		auto multiplier (response.json.get<double> ("multiplier"));
		ASSERT_NEAR (expected_multiplier, multiplier, 1e-6);
		auto network_current_text (response.json.get<std::string> ("network_current"));
		uint64_t network_current;
		ASSERT_FALSE (nano::from_string_hex (network_current_text, network_current));
		ASSERT_EQ (nano::difficulty::from_multiplier (expected_multiplier, node->default_difficulty (nano::work_version::work_1)), network_current);
		ASSERT_EQ (response.json.not_found (), response.json.find ("difficulty_trend"));
	}
	// Test include_trend optional
	request.put ("include_trend", true);
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		auto trend_opt (response.json.get_child_optional ("difficulty_trend"));
		ASSERT_TRUE (trend_opt.is_initialized ());
		auto & trend (trend_opt.get ());
		ASSERT_EQ (trend_size, trend.size ());

		system.deadline_set (5s);
		bool done = false;
		while (!done)
		{
			// Look for the sequence 4.2, 1.5; we don't know where as the active transaction request loop may prepend values concurrently
			double values[2]{ 4.2, 1.5 };
			auto it = std::search (trend.begin (), trend.end (), values, values + 2, [](auto a, double b) {
				return a.second.template get<double> ("") == b;
			});
			done = it != trend.end ();
			ASSERT_NO_ERROR (system.poll ());
		}
	}
}

// This is mainly to check for threading issues with TSAN
TEST (rpc, simultaneous_calls)
{
	// This tests simulatenous calls to the same node in different threads
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::thread_runner runner (system.env.ctx, node->config.io_threads);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	rpc_config.rpc_process.num_ipc_connections = 8;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_block_count");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());

	constexpr auto num = 100;
	std::array<std::unique_ptr<test_response>, num> test_responses;
	for (int i = 0; i < num; ++i)
	{
		test_responses[i] = std::make_unique<test_response> (request, system.env.ctx);
	}

	std::promise<void> promise;
	std::atomic<int> count{ num };
	for (int i = 0; i < num; ++i)
	{
		std::thread ([&test_responses, &promise, &count, i, port = rpc.config.port]() {
			test_responses[i]->run (port);
			if (--count == 0)
			{
				promise.set_value ();
			}
		})
		.detach ();
	}

	promise.get_future ().wait ();

	ASSERT_TIMELY (60s, std::all_of (test_responses.begin (), test_responses.end (), [](const auto & test_response) { return test_response->status != 0; }));

	for (int i = 0; i < num; ++i)
	{
		ASSERT_EQ (200, test_responses[i]->status);
		std::string block_count_text (test_responses[i]->json.get<std::string> ("block_count"));
		ASSERT_EQ ("1", block_count_text);
	}
	rpc.stop ();
	system.stop ();
	ipc_server.stop ();
	system.env.ctx.stop ();
	runner.join ();
}

// This tests that the inprocess RPC (i.e without using IPC) works correctly
TEST (rpc, in_process)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::inprocess_rpc_handler inprocess_rpc_handler (*node, ipc_server, node_rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, inprocess_rpc_handler);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_balance");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	std::string balance_text (response.json.get<std::string> ("balance"));
	ASSERT_EQ ("340282366920938463463374607431768211455", balance_text);
	std::string pending_text (response.json.get<std::string> ("pending"));
	ASSERT_EQ ("0", pending_text);
}

TEST (rpc_config, serialization)
{
	nano::rpc_config config1;
	config1.address = boost::asio::ip::address_v6::any ().to_string ();
	config1.port = 10;
	config1.enable_control = true;
	config1.max_json_depth = 10;
	config1.rpc_process.io_threads = 2;
	config1.rpc_process.ipc_address = boost::asio::ip::address_v6::any ().to_string ();
	config1.rpc_process.ipc_port = 2000;
	config1.rpc_process.num_ipc_connections = 99;
	nano::jsonconfig tree;
	config1.serialize_json (tree);
	nano::rpc_config config2;
	ASSERT_NE (config2.address, config1.address);
	ASSERT_NE (config2.port, config1.port);
	ASSERT_NE (config2.enable_control, config1.enable_control);
	ASSERT_NE (config2.max_json_depth, config1.max_json_depth);
	ASSERT_NE (config2.rpc_process.io_threads, config1.rpc_process.io_threads);
	ASSERT_NE (config2.rpc_process.ipc_address, config1.rpc_process.ipc_address);
	ASSERT_NE (config2.rpc_process.ipc_port, config1.rpc_process.ipc_port);
	ASSERT_NE (config2.rpc_process.num_ipc_connections, config1.rpc_process.num_ipc_connections);
	bool upgraded{ false };
	config2.deserialize_json (upgraded, tree);
	ASSERT_EQ (config2.address, config1.address);
	ASSERT_EQ (config2.port, config1.port);
	ASSERT_EQ (config2.enable_control, config1.enable_control);
	ASSERT_EQ (config2.max_json_depth, config1.max_json_depth);
	ASSERT_EQ (config2.rpc_process.io_threads, config1.rpc_process.io_threads);
	ASSERT_EQ (config2.rpc_process.ipc_address, config1.rpc_process.ipc_address);
	ASSERT_EQ (config2.rpc_process.ipc_port, config1.rpc_process.ipc_port);
	ASSERT_EQ (config2.rpc_process.num_ipc_connections, config1.rpc_process.num_ipc_connections);
}

TEST (rpc, deprecated_account_format)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_info");
	request.put ("account", nano::dev_genesis_key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	boost::optional<std::string> deprecated_account_format (response.json.get_optional<std::string> ("deprecated_account_format"));
	ASSERT_FALSE (deprecated_account_format.is_initialized ());
	std::string account_text (nano::dev_genesis_key.pub.to_account ());
	account_text[4] = '-';
	request.put ("account", account_text);
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	std::string frontier (response.json.get<std::string> ("frontier"));
	ASSERT_EQ (nano::genesis_hash.to_string (), frontier);
	boost::optional<std::string> deprecated_account_format2 (response2.json.get_optional<std::string> ("deprecated_account_format"));
	ASSERT_TRUE (deprecated_account_format2.is_initialized ());
}

TEST (rpc, epoch_upgrade)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key1, key2, key3;
	nano::keypair epoch_signer (nano::dev_genesis_key);
	auto send1 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, nano::genesis_hash, nano::dev_genesis_key.pub, nano::genesis_amount - 1, key1.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (nano::genesis_hash))); // to opened account
	ASSERT_EQ (nano::process_result::progress, node->process (*send1).code);
	auto send2 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, send1->hash (), nano::dev_genesis_key.pub, nano::genesis_amount - 2, key2.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send1->hash ()))); // to unopened account (pending)
	ASSERT_EQ (nano::process_result::progress, node->process (*send2).code);
	auto send3 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, send2->hash (), nano::dev_genesis_key.pub, nano::genesis_amount - 3, 0, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send2->hash ()))); // to burn (0)
	ASSERT_EQ (nano::process_result::progress, node->process (*send3).code);
	nano::account max_account (std::numeric_limits<nano::uint256_t>::max ());
	auto send4 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, send3->hash (), nano::dev_genesis_key.pub, nano::genesis_amount - 4, max_account, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send3->hash ()))); // to max account
	ASSERT_EQ (nano::process_result::progress, node->process (*send4).code);
	auto open (std::make_shared<nano::state_block> (key1.pub, 0, key1.pub, 1, send1->hash (), key1.prv, key1.pub, *system.env.work.generate (key1.pub)));
	ASSERT_EQ (nano::process_result::progress, node->process (*open).code);
	// Check accounts epochs
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (2, node->store.account_count (transaction));
		for (auto i (node->store.latest_begin (transaction)); i != node->store.latest_end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_0);
		}
	}
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "epoch_upgrade");
	request.put ("epoch", 1);
	request.put ("key", epoch_signer.prv.data.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ ("1", response.json.get<std::string> ("started"));
	test_response response_fail (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response_fail.status != 0);
	ASSERT_EQ (200, response_fail.status);
	ASSERT_EQ ("0", response_fail.json.get<std::string> ("started"));
	ASSERT_TIMELY (10s, 4 == node->store.account_count (node->store.tx_begin_read ()));
	// Check upgrade
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (4, node->store.account_count (transaction));
		for (auto i (node->store.latest_begin (transaction)); i != node->store.latest_end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_1);
		}
		ASSERT_TRUE (node->store.account_exists (transaction, key1.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, key2.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, std::numeric_limits<nano::uint256_t>::max ()));
		ASSERT_FALSE (node->store.account_exists (transaction, 0));
	}

	// Epoch 2 upgrade
	auto genesis_latest (node->latest (nano::dev_genesis_key.pub));
	auto send5 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, genesis_latest, nano::dev_genesis_key.pub, nano::genesis_amount - 5, 0, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (genesis_latest))); // to burn (0)
	ASSERT_EQ (nano::process_result::progress, node->process (*send5).code);
	auto send6 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, send5->hash (), nano::dev_genesis_key.pub, nano::genesis_amount - 6, key1.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send5->hash ()))); // to key1 (again)
	ASSERT_EQ (nano::process_result::progress, node->process (*send6).code);
	auto key1_latest (node->latest (key1.pub));
	auto send7 (std::make_shared<nano::state_block> (key1.pub, key1_latest, key1.pub, 0, key3.pub, key1.prv, key1.pub, *system.env.work.generate (key1_latest))); // to key3
	ASSERT_EQ (nano::process_result::progress, node->process (*send7).code);
	{
		// Check pending entry
		auto transaction (node->store.tx_begin_read ());
		nano::pending_info info;
		ASSERT_FALSE (node->store.pending_get (transaction, nano::pending_key (key3.pub, send7->hash ()), info));
		ASSERT_EQ (nano::epoch::epoch_1, info.epoch);
	}

	request.put ("epoch", 2);
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_EQ ("1", response2.json.get<std::string> ("started"));
	ASSERT_TIMELY (10s, 5 == node->store.account_count (node->store.tx_begin_read ()));
	// Check upgrade
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (5, node->store.account_count (transaction));
		for (auto i (node->store.latest_begin (transaction)); i != node->store.latest_end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_2);
		}
		ASSERT_TRUE (node->store.account_exists (transaction, key1.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, key2.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, key3.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, std::numeric_limits<nano::uint256_t>::max ()));
		ASSERT_FALSE (node->store.account_exists (transaction, 0));
	}
}

TEST (rpc, epoch_upgrade_multithreaded)
{
	nano::system system;
	nano::node_config node_config;
	node_config.work_threads = 4;
	auto node = add_ipc_enabled_node (system, node_config);
	nano::keypair key1, key2, key3;
	nano::keypair epoch_signer (nano::dev_genesis_key);
	auto send1 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, nano::genesis_hash, nano::dev_genesis_key.pub, nano::genesis_amount - 1, key1.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (nano::genesis_hash))); // to opened account
	ASSERT_EQ (nano::process_result::progress, node->process (*send1).code);
	auto send2 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, send1->hash (), nano::dev_genesis_key.pub, nano::genesis_amount - 2, key2.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send1->hash ()))); // to unopened account (pending)
	ASSERT_EQ (nano::process_result::progress, node->process (*send2).code);
	auto send3 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, send2->hash (), nano::dev_genesis_key.pub, nano::genesis_amount - 3, 0, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send2->hash ()))); // to burn (0)
	ASSERT_EQ (nano::process_result::progress, node->process (*send3).code);
	nano::account max_account (std::numeric_limits<nano::uint256_t>::max ());
	auto send4 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, send3->hash (), nano::dev_genesis_key.pub, nano::genesis_amount - 4, max_account, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send3->hash ()))); // to max account
	ASSERT_EQ (nano::process_result::progress, node->process (*send4).code);
	auto open (std::make_shared<nano::state_block> (key1.pub, 0, key1.pub, 1, send1->hash (), key1.prv, key1.pub, *system.env.work.generate (key1.pub)));
	ASSERT_EQ (nano::process_result::progress, node->process (*open).code);
	// Check accounts epochs
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (2, node->store.account_count (transaction));
		for (auto i (node->store.latest_begin (transaction)); i != node->store.latest_end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_0);
		}
	}
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "epoch_upgrade");
	request.put ("threads", 2);
	request.put ("epoch", 1);
	request.put ("key", epoch_signer.prv.data.to_string ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	ASSERT_EQ ("1", response.json.get<std::string> ("started"));
	ASSERT_TIMELY (5s, 4 == node->store.account_count (node->store.tx_begin_read ()));
	// Check upgrade
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (4, node->store.account_count (transaction));
		for (auto i (node->store.latest_begin (transaction)); i != node->store.latest_end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_1);
		}
		ASSERT_TRUE (node->store.account_exists (transaction, key1.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, key2.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, std::numeric_limits<nano::uint256_t>::max ()));
		ASSERT_FALSE (node->store.account_exists (transaction, 0));
	}

	// Epoch 2 upgrade
	auto genesis_latest (node->latest (nano::dev_genesis_key.pub));
	auto send5 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, genesis_latest, nano::dev_genesis_key.pub, nano::genesis_amount - 5, 0, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (genesis_latest))); // to burn (0)
	ASSERT_EQ (nano::process_result::progress, node->process (*send5).code);
	auto send6 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, send5->hash (), nano::dev_genesis_key.pub, nano::genesis_amount - 6, key1.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send5->hash ()))); // to key1 (again)
	ASSERT_EQ (nano::process_result::progress, node->process (*send6).code);
	auto key1_latest (node->latest (key1.pub));
	auto send7 (std::make_shared<nano::state_block> (key1.pub, key1_latest, key1.pub, 0, key3.pub, key1.prv, key1.pub, *system.env.work.generate (key1_latest))); // to key3
	ASSERT_EQ (nano::process_result::progress, node->process (*send7).code);
	{
		// Check pending entry
		auto transaction (node->store.tx_begin_read ());
		nano::pending_info info;
		ASSERT_FALSE (node->store.pending_get (transaction, nano::pending_key (key3.pub, send7->hash ()), info));
		ASSERT_EQ (nano::epoch::epoch_1, info.epoch);
	}

	request.put ("epoch", 2);
	test_response response2 (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response2.status != 0);
	ASSERT_EQ (200, response2.status);
	ASSERT_EQ ("1", response2.json.get<std::string> ("started"));
	ASSERT_TIMELY (5s, 5 == node->store.account_count (node->store.tx_begin_read ()));
	// Check upgrade
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (5, node->store.account_count (transaction));
		for (auto i (node->store.latest_begin (transaction)); i != node->store.latest_end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_2);
		}
		ASSERT_TRUE (node->store.account_exists (transaction, key1.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, key2.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, key3.pub));
		ASSERT_TRUE (node->store.account_exists (transaction, std::numeric_limits<nano::uint256_t>::max ()));
		ASSERT_FALSE (node->store.account_exists (transaction, 0));
	}
}

TEST (rpc, account_lazy_start)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_legacy_bootstrap = true;
	auto node1 = system.add_node (config);
	nano::keypair key;
	// Generating test chain
	auto send1 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, nano::genesis_hash, nano::dev_genesis_key.pub, nano::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (nano::genesis_hash)));
	ASSERT_EQ (nano::process_result::progress, node1->process (*send1).code);
	auto open (std::make_shared<nano::open_block> (send1->hash (), key.pub, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub)));
	ASSERT_EQ (nano::process_result::progress, node1->process (*open).code);

	// Start lazy bootstrap with account
	nano::node_config node_config;
	node_config.ipc_config.transport_tcp.enabled = true;
	node_config.ipc_config.transport_tcp.port = nano::get_available_port ();
	node_config.flags = config.flags;
	auto node2 = system.add_node (node_config);
	node2->network.udp_channels.insert (node1->network.endpoint (), node1->env.constants.protocol.protocol_version);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node2, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node_config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_info");
	request.put ("account", key.pub.to_account ());
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (5s, response.status != 0);
	ASSERT_EQ (200, response.status);
	boost::optional<std::string> account_error (response.json.get_optional<std::string> ("error"));
	ASSERT_TRUE (account_error.is_initialized ());

	// Check processed blocks
	ASSERT_TIMELY (10s, !node2->bootstrap_initiator.in_progress ());
	node2->block_processor.flush ();
	ASSERT_TRUE (node2->ledger.block_exists (send1->hash ()));
	ASSERT_TRUE (node2->ledger.block_exists (open->hash ()));
}

TEST (rpc, receive)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);
	auto wallet = system.wallet (0);
	std::string wallet_text;
	node.wallets.items.begin ()->first.encode_hex (wallet_text);
	wallet->insert_adhoc (nano::dev_genesis_key.prv);
	nano::keypair key1;
	wallet->insert_adhoc (key1.prv);
	auto send1 (wallet->send_action (nano::dev_genesis_key.pub, key1.pub, node.config.receive_minimum.number (), *node.work_generate_blocking (nano::genesis_hash)));
	ASSERT_TIMELY (5s, node.balance (nano::dev_genesis_key.pub) != nano::genesis_amount);
	ASSERT_TIMELY (10s, !node.store.account_exists (node.store.tx_begin_read (), key1.pub));
	// Send below minimum receive amount
	auto send2 (wallet->send_action (nano::dev_genesis_key.pub, key1.pub, node.config.receive_minimum.number () - 1, *node.work_generate_blocking (send1->hash ())));
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "receive");
	request.put ("wallet", wallet_text);
	request.put ("account", key1.pub.to_account ());
	request.put ("block", send2->hash ().to_string ());
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto receive_text (response.json.get<std::string> ("block"));
		nano::account_info info;
		ASSERT_FALSE (node.store.account_get (node.store.tx_begin_read (), key1.pub, info));
		ASSERT_EQ (info.head, receive_text);
	}
	// Trying to receive the same block should fail with unreceivable
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_process::unreceivable).message (), response.json.get<std::string> ("error"));
	}
	// Trying to receive a non-existing block should fail
	request.put ("block", nano::block_hash (send2->hash ().number () + 1).to_string ());
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response.json.get<std::string> ("error"));
	}
}

TEST (rpc, receive_unopened)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);
	auto wallet = system.wallet (0);
	std::string wallet_text;
	node.wallets.items.begin ()->first.encode_hex (wallet_text);
	wallet->insert_adhoc (nano::dev_genesis_key.prv);
	// Test receiving for unopened account
	nano::keypair key1;
	auto send1 (wallet->send_action (nano::dev_genesis_key.pub, key1.pub, node.config.receive_minimum.number () - 1, *node.work_generate_blocking (nano::genesis_hash)));
	ASSERT_TIMELY (5s, !node.balance (nano::dev_genesis_key.pub) != nano::genesis_amount);
	ASSERT_FALSE (node.store.account_exists (node.store.tx_begin_read (), key1.pub));
	ASSERT_TRUE (node.store.block_exists (node.store.tx_begin_read (), send1->hash ()));
	wallet->insert_adhoc (key1.prv); // should not auto receive, amount sent was lower than minimum
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "receive");
	request.put ("wallet", wallet_text);
	request.put ("account", key1.pub.to_account ());
	request.put ("block", send1->hash ().to_string ());
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto receive_text (response.json.get<std::string> ("block"));
		nano::account_info info;
		ASSERT_FALSE (node.store.account_get (node.store.tx_begin_read (), key1.pub, info));
		ASSERT_EQ (info.head, info.open_block);
		ASSERT_EQ (info.head.to_string (), receive_text);
		ASSERT_EQ (info.representative, nano::dev_genesis_key.pub);
	}
	scoped_thread_name_io.reset ();

	// Test receiving for an unopened with a different wallet representative
	nano::keypair key2;
	auto prev_amount (node.balance (nano::dev_genesis_key.pub));
	auto send2 (wallet->send_action (nano::dev_genesis_key.pub, key2.pub, node.config.receive_minimum.number () - 1, *node.work_generate_blocking (send1->hash ())));
	ASSERT_TIMELY (5s, !node.balance (nano::dev_genesis_key.pub) != prev_amount);
	ASSERT_FALSE (node.store.account_exists (node.store.tx_begin_read (), key2.pub));
	ASSERT_TRUE (node.store.block_exists (node.store.tx_begin_read (), send2->hash ()));
	nano::public_key rep;
	wallet->store.representative_set (node.wallets.tx_begin_write (), rep);
	wallet->insert_adhoc (key2.prv); // should not auto receive, amount sent was lower than minimum
	scoped_thread_name_io.renew ();
	request.put ("account", key2.pub.to_account ());
	request.put ("block", send2->hash ().to_string ());
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto receive_text (response.json.get<std::string> ("block"));
		nano::account_info info;
		ASSERT_FALSE (node.store.account_get (node.store.tx_begin_read (), key2.pub, info));
		ASSERT_EQ (info.head, info.open_block);
		ASSERT_EQ (info.head.to_string (), receive_text);
		ASSERT_EQ (info.representative, rep);
	}
}

TEST (rpc, receive_work_disabled)
{
	nano::system system;
	nano::node_config config;
	auto & worker_node = *system.add_node (config);
	config.work_threads = 0;
	auto & node = *add_ipc_enabled_node (system, config);
	auto wallet = system.wallet (1);
	std::string wallet_text;
	node.wallets.items.begin ()->first.encode_hex (wallet_text);
	wallet->insert_adhoc (nano::dev_genesis_key.prv);
	nano::keypair key1;
	nano::genesis genesis;
	ASSERT_TRUE (worker_node.work_generation_enabled ());
	auto send1 (wallet->send_action (nano::dev_genesis_key.pub, key1.pub, node.config.receive_minimum.number () - 1, *worker_node.work_generate_blocking (genesis.hash ()), false));
	ASSERT_TRUE (send1 != nullptr);
	ASSERT_TIMELY (5s, node.balance (nano::dev_genesis_key.pub) != nano::genesis_amount);
	ASSERT_FALSE (node.store.account_exists (node.store.tx_begin_read (), key1.pub));
	ASSERT_TRUE (node.store.block_exists (node.store.tx_begin_read (), send1->hash ()));
	wallet->insert_adhoc (key1.prv);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "receive");
	request.put ("wallet", wallet_text);
	request.put ("account", key1.pub.to_account ());
	request.put ("block", send1->hash ().to_string ());
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_common::disabled_work_generation).message (), response.json.get<std::string> ("error"));
	}
}

TEST (rpc, telemetry_single)
{
	nano::system system (1);
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();

	// Wait until peers are stored as they are done in the background
	auto peers_stored = false;
	ASSERT_TIMELY (10s, node1.store.peer_count (node1.store.tx_begin_read ()) != 0);

	// Missing port
	boost::property_tree::ptree request;
	auto node = system.nodes.front ();
	request.put ("action", "telemetry");
	request.put ("address", "not_a_valid_address");

	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_rpc::requires_port_and_address).message (), response.json.get<std::string> ("error"));
	}

	// Missing address
	request.erase ("address");
	request.put ("port", 65);

	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_rpc::requires_port_and_address).message (), response.json.get<std::string> ("error"));
	}

	// Try with invalid address
	request.put ("address", "not_a_valid_address");
	request.put ("port", 65);

	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_common::invalid_ip_address).message (), response.json.get<std::string> ("error"));
	}

	// Then invalid port
	request.put ("address", (boost::format ("%1%") % node->network.endpoint ().address ()).str ());
	request.put ("port", "invalid port");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_common::invalid_port).message (), response.json.get<std::string> ("error"));
	}

	// Use correctly formed address and port
	request.put ("port", node->network.endpoint ().port ());
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);

		nano::jsonconfig config (response.json);
		nano::telemetry_data telemetry_data;
		auto const should_ignore_identification_metrics = false;
		ASSERT_FALSE (telemetry_data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data (telemetry_data, node->env.constants, node->config.bandwidth_limit, node->active.active_difficulty (), node->node_id);
	}
}

TEST (rpc, telemetry_all)
{
	nano::system system (1);
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();

	// Wait until peers are stored as they are done in the background
	ASSERT_TIMELY (10s, node1.store.peer_count (node1.store.tx_begin_read ()) != 0);

	// First need to set up the cached data
	std::atomic<bool> done{ false };
	auto node = system.nodes.front ();
	node1.telemetry->get_metrics_single_peer_async (node1.network.find_channel (node->network.endpoint ()), [&done](nano::telemetry_data_response const & telemetry_data_response_a) {
		ASSERT_FALSE (telemetry_data_response_a.error);
		done = true;
	});

	ASSERT_TIMELY (10s, done);

	boost::property_tree::ptree request;
	request.put ("action", "telemetry");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		nano::jsonconfig config (response.json);
		nano::telemetry_data telemetry_data;
		auto const should_ignore_identification_metrics = true;
		ASSERT_FALSE (telemetry_data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data_excluding_signature (telemetry_data, node->env.constants, node->config.bandwidth_limit, node->active.active_difficulty ());
		ASSERT_FALSE (response.json.get_optional<std::string> ("node_id").is_initialized ());
		ASSERT_FALSE (response.json.get_optional<std::string> ("signature").is_initialized ());
	}

	request.put ("raw", "true");
	test_response response (request, rpc.config.port, system.env.ctx);
	ASSERT_TIMELY (10s, response.status != 0);
	ASSERT_EQ (200, response.status);

	// This may fail if the response has taken longer than the cache cutoff time.
	auto & all_metrics = response.json.get_child ("metrics");
	auto & metrics = all_metrics.front ().second;
	ASSERT_EQ (1, all_metrics.size ());

	nano::jsonconfig config (metrics);
	nano::telemetry_data data;
	auto const should_ignore_identification_metrics = false;
	ASSERT_FALSE (data.deserialize_json (config, should_ignore_identification_metrics));
	nano::compare_default_telemetry_response_data (data, node->env.constants, node->config.bandwidth_limit, node->active.active_difficulty (), node->node_id);

	ASSERT_EQ (node->network.endpoint ().address ().to_string (), metrics.get<std::string> ("address"));
	ASSERT_EQ (node->network.endpoint ().port (), metrics.get<uint16_t> ("port"));
	ASSERT_TRUE (node1.network.find_node_id (data.node_id));
}

// Also tests all forms of ipv4/ipv6
TEST (rpc, telemetry_self)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();

	// Just to have peer count at 1
	node1.network.udp_channels.insert (nano::endpoint (boost::asio::ip::make_address_v6 ("::1"), nano::get_available_port ()), 0);

	boost::property_tree::ptree request;
	request.put ("action", "telemetry");
	request.put ("address", "::1");
	request.put ("port", node1.network.endpoint ().port ());
	auto const should_ignore_identification_metrics = false;
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		nano::telemetry_data data;
		nano::jsonconfig config (response.json);
		ASSERT_FALSE (data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data (data, node1.env.constants, node1.config.bandwidth_limit, node1.active.active_difficulty (), node1.node_id);
	}

	request.put ("address", "[::1]");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		nano::telemetry_data data;
		nano::jsonconfig config (response.json);
		ASSERT_FALSE (data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data (data, node1.env.constants, node1.config.bandwidth_limit, node1.active.active_difficulty (), node1.node_id);
	}

	request.put ("address", "127.0.0.1");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		nano::telemetry_data data;
		nano::jsonconfig config (response.json);
		ASSERT_FALSE (data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data (data, node1.env.constants, node1.config.bandwidth_limit, node1.active.active_difficulty (), node1.node_id);
	}

	// Incorrect port should fail
	request.put ("port", "0");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (10s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (std::error_code (nano::error_rpc::peer_not_found).message (), response.json.get<std::string> ("error"));
	}
}

TEST (rpc, confirmation_active)
{
	nano::system system;
	nano::node_config node_config;
	node_config.ipc_config.transport_tcp.enabled = true;
	node_config.ipc_config.transport_tcp.port = nano::get_available_port ();
	node_config.flags.disable_request_loop = true;
	auto & node1 (*system.add_node (node_config));
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();

	nano::genesis genesis;
	auto send1 (std::make_shared<nano::send_block> (genesis.hash (), nano::public_key (), nano::genesis_amount - 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (genesis.hash ())));
	auto send2 (std::make_shared<nano::send_block> (send1->hash (), nano::public_key (), nano::genesis_amount - 200, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send1->hash ())));
	node1.process_active (send1);
	node1.process_active (send2);
	nano::blocks_confirm (node1, { send1, send2 });
	ASSERT_EQ (2, node1.active.size ());
	{
		nano::lock_guard<std::mutex> guard (node1.active.mutex);
		auto info (node1.active.roots.find (send1->qualified_root ()));
		ASSERT_NE (node1.active.roots.end (), info);
		info->election->confirm_once ();
	}

	boost::property_tree::ptree request;
	request.put ("action", "confirmation_active");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		auto & confirmations (response.json.get_child ("confirmations"));
		ASSERT_EQ (1, confirmations.size ());
		ASSERT_EQ (send2->qualified_root ().to_string (), confirmations.front ().second.get<std::string> (""));
		ASSERT_EQ (1, response.json.get<unsigned> ("unconfirmed"));
		ASSERT_EQ (1, response.json.get<unsigned> ("confirmed"));
	}
}

TEST (rpc, confirmation_info)
{
	nano::system system;
	auto & node1 = *add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (node1, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node1.config.ipc_config.transport_tcp.port;
	nano::ipc_rpc_processor ipc_rpc_processor (system.env.ctx, rpc_config);
	nano::rpc rpc (system.env.ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();

	nano::genesis genesis;
	auto send (std::make_shared<nano::send_block> (genesis.hash (), nano::public_key (), nano::genesis_amount - 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (genesis.hash ())));
	node1.process_active (send);
	node1.block_processor.flush ();
	ASSERT_FALSE (node1.active.empty ());

	boost::property_tree::ptree request;
	request.put ("action", "confirmation_info");
	request.put ("root", send->qualified_root ().to_string ());
	request.put ("representatives", "true");
	request.put ("json_block", "true");
	{
		test_response response (request, rpc.config.port, system.env.ctx);
		ASSERT_TIMELY (5s, response.status != 0);
		ASSERT_EQ (200, response.status);
		ASSERT_EQ (1, response.json.count ("announcements"));
		ASSERT_EQ (1, response.json.get<unsigned> ("voters"));
		ASSERT_EQ (send->hash ().to_string (), response.json.get<std::string> ("last_winner"));
		auto & blocks (response.json.get_child ("blocks"));
		ASSERT_EQ (1, blocks.size ());
		auto & representatives (blocks.front ().second.get_child ("representatives"));
		ASSERT_EQ (1, representatives.size ());
		ASSERT_EQ (0, response.json.get<unsigned> ("total_tally"));
	}
}
