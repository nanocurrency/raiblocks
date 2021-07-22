#include <nano/boost/beast/core/flat_buffer.hpp>
#include <nano/boost/beast/http.hpp>
#include <nano/lib/rpcconfig.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/ipc/ipc_server.hpp>
#include <nano/node/json_handler.hpp>
#include <nano/node/node_rpc_config.hpp>
#include <nano/rpc/rpc.hpp>
#include <nano/rpc/rpc_request_processor.hpp>
#include <nano/test_common/system.hpp>
#include <nano/test_common/telemetry.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <algorithm>
#include <tuple>

using namespace std::chrono_literals;

namespace
{
class test_response
{
public:
	test_response (boost::property_tree::ptree const & request_a, boost::asio::io_context & io_ctx_a) :
		request (request_a),
		sock (io_ctx_a)
	{
	}

	test_response (boost::property_tree::ptree const & request_a, uint16_t port_a, boost::asio::io_context & io_ctx_a) :
		request (request_a),
		sock (io_ctx_a)
	{
		run (port_a);
	}

	void run (uint16_t port_a)
	{
		sock.async_connect (nano::tcp_endpoint (boost::asio::ip::address_v6::loopback (), port_a), [this] (boost::system::error_code const & ec) {
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
				boost::beast::http::async_write (sock, req, [this] (boost::system::error_code const & ec, size_t bytes_transferred) {
					if (!ec)
					{
						boost::beast::http::async_read (sock, sb, resp, [this] (boost::system::error_code const & ec, size_t bytes_transferred) {
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
							}
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

std::shared_ptr<nano::node> add_ipc_enabled_node (nano::system & system, nano::node_config & node_config, nano::node_flags const & node_flags)
{
	node_config.ipc_config.transport_tcp.enabled = true;
	node_config.ipc_config.transport_tcp.port = nano::get_available_port ();
	return system.add_node (node_config, node_flags);
}

std::shared_ptr<nano::node> add_ipc_enabled_node (nano::system & system, nano::node_config & node_config)
{
	return add_ipc_enabled_node (system, node_config, nano::node_flags ());
}

std::shared_ptr<nano::node> add_ipc_enabled_node (nano::system & system)
{
	nano::node_config node_config (nano::get_available_port (), system.logging);
	return add_ipc_enabled_node (system, node_config);
}

void reset_confirmation_height (nano::store & store, nano::account const & account)
{
	auto transaction = store.tx_begin_write ();
	nano::confirmation_height_info confirmation_height_info;
	if (!store.confirmation_height.get (transaction, account, confirmation_height_info))
	{
		store.confirmation_height.clear (transaction, account);
	}
}

void wait_response_impl (nano::system & system, std::shared_ptr<nano::rpc> const & rpc, boost::property_tree::ptree & request, const std::chrono::duration<double, std::nano> & time, boost::property_tree::ptree & response_json)
{
	test_response response (request, rpc->config.port, system.io_ctx);
	ASSERT_TIMELY (time, response.status != 0);
	ASSERT_EQ (200, response.status);
	response_json = response.json;
}

boost::property_tree::ptree wait_response (nano::system & system, std::shared_ptr<nano::rpc> const & rpc, boost::property_tree::ptree & request, const std::chrono::duration<double, std::nano> & time = 5s)
{
	boost::property_tree::ptree response_json;
	wait_response_impl (system, rpc, request, time, response_json);
	return response_json;
}

void check_block_response_count (nano::system & system, std::shared_ptr<nano::rpc> const & rpc, boost::property_tree::ptree & request, uint64_t size_count)
{
	auto response (wait_response (system, rpc, request));
	auto & blocks = response.get_child ("blocks");
	if (size_count > 0)
	{
		ASSERT_EQ (size_count, blocks.front ().second.size ());
	}
	else
	{
		ASSERT_TRUE (blocks.empty ());
	}
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

class rpc_context
{
public:
	rpc_context (std::unique_ptr<nano::ipc::ipc_server> & ipc_server_a, std::unique_ptr<nano::ipc_rpc_processor> & ipc_rpc_processor_a, std::unique_ptr<nano::node_rpc_config> & node_rpc_config_a, std::unique_ptr<scoped_io_thread_name_change> & io_scope_a)
	{
		ipc_server = std::move (ipc_server_a);
		ipc_rpc_processor = std::move (ipc_rpc_processor_a);
		node_rpc_config = std::move (node_rpc_config_a);
		io_scope = std::move (io_scope_a);
	}

	std::unique_ptr<nano::ipc::ipc_server> ipc_server;
	std::unique_ptr<nano::ipc_rpc_processor> ipc_rpc_processor;
	std::unique_ptr<nano::node_rpc_config> node_rpc_config;
	std::unique_ptr<scoped_io_thread_name_change> io_scope;
};

std::tuple<std::shared_ptr<nano::rpc>, std::unique_ptr<rpc_context>> add_rpc (nano::system & system, std::shared_ptr<nano::node> const & node_a)
{
	auto scoped_thread_name_io (std::make_unique<scoped_io_thread_name_change> ());
	auto node_rpc_config (std::make_unique<nano::node_rpc_config> ());
	auto ipc_server (std::make_unique<nano::ipc::ipc_server> (*node_a, *node_rpc_config));
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node_a->config.ipc_config.transport_tcp.port;
	auto ipc_rpc_processor (std::make_unique<nano::ipc_rpc_processor> (system.io_ctx, rpc_config));
	auto rpc (std::make_shared<nano::rpc> (system.io_ctx, rpc_config, *ipc_rpc_processor));
	rpc->start ();
	auto rpc_ctx (std::make_unique<rpc_context> (ipc_server, ipc_rpc_processor, node_rpc_config, scoped_thread_name_io));
	return std::make_tuple (rpc, std::move (rpc_ctx));
}
}

TEST (rpc, wrapped_task)
{
	nano::system system;
	auto & node = *add_ipc_enabled_node (system);
	nano::node_rpc_config node_rpc_config;
	std::atomic<bool> response (false);
	auto response_handler_l ([&response] (std::string const & response_a) {
		std::stringstream istream (response_a);
		boost::property_tree::ptree json_l;
		ASSERT_NO_THROW (boost::property_tree::read_json (istream, json_l));
		ASSERT_EQ (1, json_l.count ("error"));
		ASSERT_EQ ("Unable to parse JSON", json_l.get<std::string> ("error"));
		response = true;
	});
	auto handler_l (std::make_shared<nano::json_handler> (node, node_rpc_config, "", response_handler_l));
	auto task (handler_l->create_worker_task ([] (std::shared_ptr<nano::json_handler> const &) {
		// Exception should get caught
		throw std::runtime_error ("");
	}));
	system.nodes[0]->workers.push_task (task);
	ASSERT_TIMELY (5s, response == true);
}

TEST (rpc, account_balance)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);

	// Add a send block (which will add a pending entry too) for the genesis account
	nano::state_block_builder builder;

	auto send1 = builder.make_block ()
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - 1)
				 .link (nano::dev::genesis_key.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (nano::dev::genesis->hash ()))
				 .build ();

	ASSERT_EQ (nano::process_result::progress, node->process (*send1).code);

	auto [rpc, rpc_ctx] = add_rpc (system, node);

	boost::property_tree::ptree request;
	request.put ("action", "account_balance");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());

	// The send and pending should be unconfirmed
	{
		auto response (wait_response (system, rpc, request));
		std::string balance_text (response.get<std::string> ("balance"));
		ASSERT_EQ ("340282366920938463463374607431768211455", balance_text);
		std::string pending_text (response.get<std::string> ("pending"));
		ASSERT_EQ ("0", pending_text);
	}

	request.put ("include_only_confirmed", false);
	{
		auto response (wait_response (system, rpc, request));
		std::string balance_text (response.get<std::string> ("balance"));
		ASSERT_EQ ("340282366920938463463374607431768211454", balance_text);
		std::string pending_text (response.get<std::string> ("pending"));
		ASSERT_EQ ("1", pending_text);
	}
}

TEST (rpc, account_block_count)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, context] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "account_block_count");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	std::string block_count_text (response.get<std::string> ("block_count"));
	ASSERT_EQ ("1", block_count_text);
}

TEST (rpc, account_create)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "account_create");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	auto response0 (wait_response (system, rpc, request));
	auto account_text0 (response0.get<std::string> ("account"));
	nano::account account0;
	ASSERT_FALSE (account0.decode_account (account_text0));
	ASSERT_TRUE (system.wallet (0)->exists (account0));
	constexpr uint64_t max_index (std::numeric_limits<uint32_t>::max ());
	request.put ("index", max_index);
	auto response1 (wait_response (system, rpc, request, 10s));
	auto account_text1 (response1.get<std::string> ("account"));
	nano::account account1;
	ASSERT_FALSE (account1.decode_account (account_text1));
	ASSERT_TRUE (system.wallet (0)->exists (account1));
	request.put ("index", max_index + 1);
	auto response2 (wait_response (system, rpc, request));
	ASSERT_EQ (std::error_code (nano::error_common::invalid_index).message (), response2.get<std::string> ("error"));
}

TEST (rpc, account_weight)
{
	nano::keypair key;
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::block_hash latest (node1->latest (nano::dev::genesis_key.pub));
	nano::change_block block (latest, key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	ASSERT_EQ (nano::process_result::progress, node1->process (block).code);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "account_weight");
	request.put ("account", key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	std::string balance_text (response.get<std::string> ("weight"));
	ASSERT_EQ ("340282366920938463463374607431768211455", balance_text);
}

TEST (rpc, wallet_contains)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_contains");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	std::string exists_text (response.get<std::string> ("exists"));
	ASSERT_EQ ("1", exists_text);
}

TEST (rpc, wallet_doesnt_contain)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_contains");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	std::string exists_text (response.get<std::string> ("exists"));
	ASSERT_EQ ("0", exists_text);
}

TEST (rpc, validate_account_number)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "validate_account_number");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	std::string exists_text (response.get<std::string> ("valid"));
	ASSERT_EQ ("1", exists_text);
}

TEST (rpc, validate_account_invalid)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	std::string account;
	nano::dev::genesis_key.pub.encode_account (account);
	account[0] ^= 0x1;
	boost::property_tree::ptree request;
	request.put ("action", "validate_account_number");
	request.put ("account", account);
	auto response (wait_response (system, rpc, request));
	std::string exists_text (response.get<std::string> ("valid"));
	ASSERT_EQ ("0", exists_text);
}

TEST (rpc, send)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev::genesis_key.pub.to_account ());
	request.put ("destination", nano::dev::genesis_key.pub.to_account ());
	request.put ("amount", "100");
	ASSERT_EQ (node->balance (nano::dev::genesis_key.pub), nano::dev::genesis_amount);
	auto response (wait_response (system, rpc, request, 10s));
	std::string block_text (response.get<std::string> ("block"));
	nano::block_hash block;
	ASSERT_FALSE (block.decode_hex (block_text));
	ASSERT_TRUE (node->ledger.block_or_pruned_exists (block));
	ASSERT_EQ (node->latest (nano::dev::genesis_key.pub), block);
	ASSERT_NE (node->balance (nano::dev::genesis_key.pub), nano::dev::genesis_amount);
}

TEST (rpc, send_fail)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev::genesis_key.pub.to_account ());
	request.put ("destination", nano::dev::genesis_key.pub.to_account ());
	request.put ("amount", "100");
	auto response (wait_response (system, rpc, request, 10s));
	ASSERT_EQ (std::error_code (nano::error_common::account_not_found_wallet).message (), response.get<std::string> ("error"));
}

TEST (rpc, send_work)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev::genesis_key.pub.to_account ());
	request.put ("destination", nano::dev::genesis_key.pub.to_account ());
	request.put ("amount", "100");
	request.put ("work", "1");
	auto response (wait_response (system, rpc, request, 10s));
	ASSERT_EQ (std::error_code (nano::error_common::invalid_work).message (), response.get<std::string> ("error"));
	request.erase ("work");
	request.put ("work", nano::to_string_hex (*node->work_generate_blocking (node->latest (nano::dev::genesis_key.pub))));
	auto response2 (wait_response (system, rpc, request, 10s));
	std::string block_text (response2.get<std::string> ("block"));
	nano::block_hash block;
	ASSERT_FALSE (block.decode_hex (block_text));
	ASSERT_TRUE (node->ledger.block_or_pruned_exists (block));
	ASSERT_EQ (node->latest (nano::dev::genesis_key.pub), block);
}

TEST (rpc, send_work_disabled)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.work_threads = 0;
	auto node = add_ipc_enabled_node (system, node_config);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev::genesis_key.pub.to_account ());
	request.put ("destination", nano::dev::genesis_key.pub.to_account ());
	request.put ("amount", "100");
	auto response (wait_response (system, rpc, request, 10s));
	ASSERT_EQ (std::error_code (nano::error_common::disabled_work_generation).message (), response.get<std::string> ("error"));
}

TEST (rpc, send_idempotent)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev::genesis_key.pub.to_account ());
	request.put ("destination", nano::account (0).to_account ());
	request.put ("amount", (nano::dev::genesis_amount - (nano::dev::genesis_amount / 4)).convert_to<std::string> ());
	request.put ("id", "123abc");
	auto response (wait_response (system, rpc, request));
	std::string block_text (response.get<std::string> ("block"));
	nano::block_hash block;
	ASSERT_FALSE (block.decode_hex (block_text));
	ASSERT_TRUE (node->ledger.block_or_pruned_exists (block));
	ASSERT_EQ (node->balance (nano::dev::genesis_key.pub), nano::dev::genesis_amount / 4);
	auto response2 (wait_response (system, rpc, request));
	ASSERT_EQ ("", response2.get<std::string> ("error", ""));
	ASSERT_EQ (block_text, response2.get<std::string> ("block"));
	ASSERT_EQ (node->balance (nano::dev::genesis_key.pub), nano::dev::genesis_amount / 4);
	request.erase ("id");
	request.put ("id", "456def");
	auto response3 (wait_response (system, rpc, request));
	ASSERT_EQ (std::error_code (nano::error_common::insufficient_balance).message (), response3.get<std::string> ("error"));
}

TEST (rpc, send_epoch_2)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);

	// Upgrade the genesis account to epoch 2
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1));
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));

	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv, false);

	auto target_difficulty = nano::work_threshold (nano::work_version::work_1, nano::block_details (nano::epoch::epoch_2, true, false, false));
	ASSERT_LT (node->network_params.network.publish_thresholds.entry, target_difficulty);
	auto min_difficulty = node->network_params.network.publish_thresholds.entry;

	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "send");
	request.put ("source", nano::dev::genesis_key.pub.to_account ());
	request.put ("destination", nano::keypair ().pub.to_account ());
	request.put ("amount", "1");

	// Test that the correct error is given if there is insufficient work
	auto insufficient = system.work_generate_limited (nano::dev::genesis->hash (), min_difficulty, target_difficulty);
	request.put ("work", nano::to_string_hex (insufficient));
	{
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_common::invalid_work);
		ASSERT_EQ (1, response.count ("error"));
		ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, send_ipc_random_id)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	std::atomic<bool> got_request{ false };
	rpc_ctx->node_rpc_config->set_request_callback ([&got_request] (boost::property_tree::ptree const & request_a) {
		EXPECT_TRUE (request_a.count ("id"));
		got_request = true;
	});
	boost::property_tree::ptree request;
	request.put ("action", "send");
	auto response (wait_response (system, rpc, request, 10s));
	ASSERT_EQ (1, response.count ("error"));
	ASSERT_EQ ("Unable to parse JSON", response.get<std::string> ("error"));
	ASSERT_TRUE (got_request);
}

TEST (rpc, stop)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "stop");
	auto response (wait_response (system, rpc, request));
}

TEST (rpc, wallet_add)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::keypair key1;
	std::string key_text;
	key1.prv.encode_hex (key_text);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_add");
	request.put ("key", key_text);
	auto response (wait_response (system, rpc, request));
	std::string account_text1 (response.get<std::string> ("account"));
	ASSERT_EQ (account_text1, key1.pub.to_account ());
	ASSERT_TRUE (system.wallet (0)->exists (key1.pub));
}

TEST (rpc, wallet_password_valid)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "password_valid");
	auto response (wait_response (system, rpc, request));
	std::string account_text1 (response.get<std::string> ("valid"));
	ASSERT_EQ (account_text1, "1");
}

TEST (rpc, wallet_password_change)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "password_change");
	request.put ("password", "test");
	auto response (wait_response (system, rpc, request));
	std::string account_text1 (response.get<std::string> ("changed"));
	ASSERT_EQ (account_text1, "1");
	rpc_ctx->io_scope->reset ();
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::raw_key password_l;
	password_l.clear ();
	system.deadline_set (10s);
	while (password_l == 0)
	{
		ASSERT_NO_ERROR (system.poll ());
		system.wallet (0)->store.password.value (password_l);
	}
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "password_enter");
	request.put ("password", "");
	auto response (wait_response (system, rpc, request));
	std::string account_text1 (response.get<std::string> ("valid"));
	ASSERT_EQ (account_text1, "1");
}

TEST (rpc, wallet_representative)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_representative");
	auto response (wait_response (system, rpc, request));
	std::string account_text1 (response.get<std::string> ("representative"));
	ASSERT_EQ (account_text1, nano::dev::genesis->account ().to_account ());
}

TEST (rpc, wallet_representative_set)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	nano::keypair key;
	request.put ("action", "wallet_representative_set");
	request.put ("representative", key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	auto transaction (node->wallets.tx_begin_read ());
	ASSERT_EQ (key.pub, node->wallets.items.begin ()->second->store.representative (transaction));
}

TEST (rpc, wallet_representative_set_force)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	nano::keypair key;
	request.put ("action", "wallet_representative_set");
	request.put ("representative", key.pub.to_account ());
	request.put ("update_existing_accounts", true);
	auto response (wait_response (system, rpc, request));
	{
		auto transaction (node->wallets.tx_begin_read ());
		ASSERT_EQ (key.pub, node->wallets.items.begin ()->second->store.representative (transaction));
	}
	nano::account representative (0);
	while (representative != key.pub)
	{
		auto transaction (node->store.tx_begin_read ());
		nano::account_info info;
		if (!node->store.account.get (transaction, nano::dev::genesis_key.pub, info))
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->insert_adhoc (key2.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "account_list");
	auto response (wait_response (system, rpc, request));
	auto & accounts_node (response.get_child ("accounts"));
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_key_valid");
	auto response (wait_response (system, rpc, request));
	std::string exists_text (response.get<std::string> ("valid"));
	ASSERT_EQ ("1", exists_text);
}

TEST (rpc, wallet_create)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_create");
	auto response (wait_response (system, rpc, request));
	std::string wallet_text (response.get<std::string> ("wallet"));
	nano::wallet_id wallet_id;
	ASSERT_FALSE (wallet_id.decode_hex (wallet_text));
	ASSERT_NE (node->wallets.items.end (), node->wallets.items.find (wallet_id));
}

TEST (rpc, wallet_create_seed)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::raw_key seed;
	nano::random_pool::generate_block (seed.bytes.data (), seed.bytes.size ());
	auto prv = nano::deterministic_key (seed, 0);
	auto pub (nano::pub_key (prv));
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_create");
	request.put ("seed", seed.to_string ());
	auto response (wait_response (system, rpc, request, 10s));
	std::string wallet_text (response.get<std::string> ("wallet"));
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
	auto account_text (response.get<std::string> ("last_restored_account"));
	nano::account account;
	ASSERT_FALSE (account.decode_account (account_text));
	ASSERT_TRUE (existing->second->exists (account));
	ASSERT_EQ (pub, account);
	ASSERT_EQ ("1", response.get<std::string> ("restored_count"));
}

TEST (rpc, wallet_export)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_export");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	auto response (wait_response (system, rpc, request));
	std::string wallet_json (response.get<std::string> ("json"));
	bool error (false);
	rpc_ctx->io_scope->reset ();
	auto transaction (node->wallets.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store store (error, kdf, transaction, nano::dev::genesis->account (), 1, "0", wallet_json);
	ASSERT_FALSE (error);
	ASSERT_TRUE (store.exists (transaction, nano::dev::genesis_key.pub));
}

TEST (rpc, wallet_destroy)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	auto wallet_id (node->wallets.items.begin ()->first);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_destroy");
	request.put ("wallet", wallet_id.to_string ());
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ (node->wallets.items.end (), node->wallets.items.find (wallet_id));
}

TEST (rpc, account_move)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto wallet_id (node->wallets.items.begin ()->first);
	auto destination (system.wallet (0));
	destination->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	auto source_id = nano::random_wallet_id ();
	auto source (node->wallets.create (source_id));
	source->insert_adhoc (key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "account_move");
	request.put ("wallet", wallet_id.to_string ());
	request.put ("source", source_id.to_string ());
	boost::property_tree::ptree keys;
	boost::property_tree::ptree entry;
	entry.put ("", key.pub.to_account ());
	keys.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", keys);
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ ("1", response.get<std::string> ("moved"));
	ASSERT_TRUE (destination->exists (key.pub));
	ASSERT_TRUE (destination->exists (nano::dev::genesis_key.pub));
	auto transaction (node->wallets.tx_begin_read ());
	ASSERT_EQ (source->store.end (), source->store.begin (transaction));
}

TEST (rpc, block)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block");
	request.put ("hash", node->latest (nano::dev::genesis->account ()).to_string ());
	auto response (wait_response (system, rpc, request));
	auto contents (response.get<std::string> ("contents"));
	ASSERT_FALSE (contents.empty ());
	ASSERT_TRUE (response.get<bool> ("confirmed")); // Genesis block is confirmed by default
}

TEST (rpc, block_account)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_account");
	request.put ("hash", nano::dev::genesis->hash ().to_string ());
	auto response (wait_response (system, rpc, request));
	std::string account_text (response.get<std::string> ("account"));
	nano::account account;
	ASSERT_FALSE (account.decode_account (account_text));
}

TEST (rpc, chain)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	auto genesis (node->latest (nano::dev::genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, 1));
	ASSERT_NE (nullptr, block);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "chain");
	request.put ("block", block->hash ().to_string ());
	request.put ("count", std::to_string (std::numeric_limits<uint64_t>::max ()));
	auto response (wait_response (system, rpc, request));
	auto & blocks_node (response.get_child ("blocks"));
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	auto genesis (node->latest (nano::dev::genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, 1));
	ASSERT_NE (nullptr, block);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "chain");
	request.put ("block", block->hash ().to_string ());
	request.put ("count", 1);
	auto response (wait_response (system, rpc, request));
	auto & blocks_node (response.get_child ("blocks"));
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	auto genesis (node->latest (nano::dev::genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, 1));
	ASSERT_NE (nullptr, block);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "chain");
	request.put ("block", block->hash ().to_string ());
	request.put ("count", std::to_string (std::numeric_limits<uint64_t>::max ()));
	request.put ("offset", 1);
	auto response (wait_response (system, rpc, request));
	auto & blocks_node (response.get_child ("blocks"));
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
			node->store.confirmation_height.put (transaction, key.pub, { 0, nano::block_hash (0) });
			node->store.account.put (transaction, key.pub, nano::account_info (hash, 0, 0, 0, 0, 0, nano::epoch::epoch_0));
		}
	}
	nano::keypair key;
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "frontiers");
	request.put ("account", nano::account (0).to_account ());
	request.put ("count", std::to_string (std::numeric_limits<uint64_t>::max ()));
	auto response (wait_response (system, rpc, request));
	auto & frontiers_node (response.get_child ("frontiers"));
	std::unordered_map<nano::account, nano::block_hash> frontiers;
	for (auto i (frontiers_node.begin ()), j (frontiers_node.end ()); i != j; ++i)
	{
		nano::account account;
		account.decode_account (i->first);
		nano::block_hash frontier;
		frontier.decode_hex (i->second.get<std::string> (""));
		frontiers[account] = frontier;
	}
	ASSERT_EQ (1, frontiers.erase (nano::dev::genesis_key.pub));
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
			node->store.confirmation_height.put (transaction, key.pub, { 0, nano::block_hash (0) });
			node->store.account.put (transaction, key.pub, nano::account_info (hash, 0, 0, 0, 0, 0, nano::epoch::epoch_0));
		}
	}
	nano::keypair key;
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "frontiers");
	request.put ("account", nano::account (0).to_account ());
	request.put ("count", std::to_string (100));
	auto response (wait_response (system, rpc, request));
	auto & frontiers_node (response.get_child ("frontiers"));
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
			node->store.confirmation_height.put (transaction, key.pub, { 0, nano::block_hash (0) });
			node->store.account.put (transaction, key.pub, nano::account_info (hash, 0, 0, 0, 0, 0, nano::epoch::epoch_0));
		}
	}
	nano::keypair key;
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "frontiers");
	request.put ("account", source.begin ()->first.to_account ());
	request.put ("count", std::to_string (1));
	auto response (wait_response (system, rpc, request));
	auto & frontiers_node (response.get_child ("frontiers"));
	ASSERT_EQ (1, frontiers_node.size ());
	ASSERT_EQ (source.begin ()->first.to_account (), frontiers_node.begin ()->first);
}

TEST (rpc, history)
{
	nano::system system;
	auto node0 = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto change (system.wallet (0)->change_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub));
	ASSERT_NE (nullptr, change);
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub, node0->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send);
	auto receive (system.wallet (0)->receive_action (send->hash (), nano::dev::genesis_key.pub, node0->config.receive_minimum.number (), send->link ().as_account ()));
	ASSERT_NE (nullptr, receive);
	nano::state_block usend (nano::dev::genesis->account (), node0->latest (nano::dev::genesis->account ()), nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis->account (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work_generate_blocking (node0->latest (nano::dev::genesis->account ())));
	nano::state_block ureceive (nano::dev::genesis->account (), usend.hash (), nano::dev::genesis->account (), nano::dev::genesis_amount, usend.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work_generate_blocking (usend.hash ()));
	nano::state_block uchange (nano::dev::genesis->account (), ureceive.hash (), nano::keypair ().pub, nano::dev::genesis_amount, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work_generate_blocking (ureceive.hash ()));
	{
		auto transaction (node0->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, usend).code);
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, ureceive).code);
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, uchange).code);
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node0);
	boost::property_tree::ptree request;
	request.put ("action", "history");
	request.put ("hash", uchange.hash ().to_string ());
	request.put ("count", 100);
	auto response (wait_response (system, rpc, request));
	std::vector<std::tuple<std::string, std::string, std::string, std::string>> history_l;
	auto & history_node (response.get_child ("history"));
	for (auto i (history_node.begin ()), n (history_node.end ()); i != n; ++i)
	{
		history_l.push_back (std::make_tuple (i->second.get<std::string> ("type"), i->second.get<std::string> ("account"), i->second.get<std::string> ("amount"), i->second.get<std::string> ("hash")));
	}
	ASSERT_EQ (5, history_l.size ());
	ASSERT_EQ ("receive", std::get<0> (history_l[0]));
	ASSERT_EQ (ureceive.hash ().to_string (), std::get<3> (history_l[0]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[0]));
	ASSERT_EQ (nano::Gxrb_ratio.convert_to<std::string> (), std::get<2> (history_l[0]));
	ASSERT_EQ (5, history_l.size ());
	ASSERT_EQ ("send", std::get<0> (history_l[1]));
	ASSERT_EQ (usend.hash ().to_string (), std::get<3> (history_l[1]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[1]));
	ASSERT_EQ (nano::Gxrb_ratio.convert_to<std::string> (), std::get<2> (history_l[1]));
	ASSERT_EQ ("receive", std::get<0> (history_l[2]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[2]));
	ASSERT_EQ (node0->config.receive_minimum.to_string_dec (), std::get<2> (history_l[2]));
	ASSERT_EQ (receive->hash ().to_string (), std::get<3> (history_l[2]));
	ASSERT_EQ ("send", std::get<0> (history_l[3]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[3]));
	ASSERT_EQ (node0->config.receive_minimum.to_string_dec (), std::get<2> (history_l[3]));
	ASSERT_EQ (send->hash ().to_string (), std::get<3> (history_l[3]));
	ASSERT_EQ ("receive", std::get<0> (history_l[4]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[4]));
	ASSERT_EQ (nano::dev::genesis_amount.convert_to<std::string> (), std::get<2> (history_l[4]));
	ASSERT_EQ (nano::dev::genesis->hash ().to_string (), std::get<3> (history_l[4]));
}

TEST (rpc, account_history)
{
	nano::system system;
	auto node0 = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto change (system.wallet (0)->change_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub));
	ASSERT_NE (nullptr, change);
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub, node0->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send);
	auto receive (system.wallet (0)->receive_action (send->hash (), nano::dev::genesis_key.pub, node0->config.receive_minimum.number (), send->link ().as_account ()));
	ASSERT_NE (nullptr, receive);
	nano::state_block usend (nano::dev::genesis->account (), node0->latest (nano::dev::genesis->account ()), nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis->account (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work_generate_blocking (node0->latest (nano::dev::genesis->account ())));
	nano::state_block ureceive (nano::dev::genesis->account (), usend.hash (), nano::dev::genesis->account (), nano::dev::genesis_amount, usend.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work_generate_blocking (usend.hash ()));
	nano::state_block uchange (nano::dev::genesis->account (), ureceive.hash (), nano::keypair ().pub, nano::dev::genesis_amount, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work_generate_blocking (ureceive.hash ()));
	{
		auto transaction (node0->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, usend).code);
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, ureceive).code);
		ASSERT_EQ (nano::process_result::progress, node0->ledger.process (transaction, uchange).code);
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node0);
	{
		boost::property_tree::ptree request;
		request.put ("action", "account_history");
		request.put ("account", nano::dev::genesis->account ().to_account ());
		request.put ("count", 100);
		auto response (wait_response (system, rpc, request, 10s));
		std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string>> history_l;
		auto & history_node (response.get_child ("history"));
		for (auto i (history_node.begin ()), n (history_node.end ()); i != n; ++i)
		{
			history_l.push_back (std::make_tuple (i->second.get<std::string> ("type"), i->second.get<std::string> ("account"), i->second.get<std::string> ("amount"), i->second.get<std::string> ("hash"), i->second.get<std::string> ("height")));
		}

		ASSERT_EQ (5, history_l.size ());
		ASSERT_EQ ("receive", std::get<0> (history_l[0]));
		ASSERT_EQ (ureceive.hash ().to_string (), std::get<3> (history_l[0]));
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[0]));
		ASSERT_EQ (nano::Gxrb_ratio.convert_to<std::string> (), std::get<2> (history_l[0]));
		ASSERT_EQ ("6", std::get<4> (history_l[0])); // change block (height 7) is skipped by account_history since "raw" is not set
		ASSERT_EQ ("send", std::get<0> (history_l[1]));
		ASSERT_EQ (usend.hash ().to_string (), std::get<3> (history_l[1]));
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[1]));
		ASSERT_EQ (nano::Gxrb_ratio.convert_to<std::string> (), std::get<2> (history_l[1]));
		ASSERT_EQ ("5", std::get<4> (history_l[1]));
		ASSERT_EQ ("receive", std::get<0> (history_l[2]));
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[2]));
		ASSERT_EQ (node0->config.receive_minimum.to_string_dec (), std::get<2> (history_l[2]));
		ASSERT_EQ (receive->hash ().to_string (), std::get<3> (history_l[2]));
		ASSERT_EQ ("4", std::get<4> (history_l[2]));
		ASSERT_EQ ("send", std::get<0> (history_l[3]));
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[3]));
		ASSERT_EQ (node0->config.receive_minimum.to_string_dec (), std::get<2> (history_l[3]));
		ASSERT_EQ (send->hash ().to_string (), std::get<3> (history_l[3]));
		ASSERT_EQ ("3", std::get<4> (history_l[3]));
		ASSERT_EQ ("receive", std::get<0> (history_l[4]));
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[4]));
		ASSERT_EQ (nano::dev::genesis_amount.convert_to<std::string> (), std::get<2> (history_l[4]));
		ASSERT_EQ (nano::dev::genesis->hash ().to_string (), std::get<3> (history_l[4]));
		ASSERT_EQ ("1", std::get<4> (history_l[4])); // change block (height 2) is skipped
	}
	// Test count and reverse
	{
		boost::property_tree::ptree request;
		request.put ("action", "account_history");
		request.put ("account", nano::dev::genesis->account ().to_account ());
		request.put ("reverse", true);
		request.put ("count", 1);
		auto response (wait_response (system, rpc, request, 10s));
		auto & history_node (response.get_child ("history"));
		ASSERT_EQ (1, history_node.size ());
		ASSERT_EQ ("1", history_node.begin ()->second.get<std::string> ("height"));
		ASSERT_EQ (change->hash ().to_string (), response.get<std::string> ("next"));
	}

	// Test filtering
	rpc_ctx->io_scope->reset ();
	auto account2 (system.wallet (0)->deterministic_insert ());
	auto send2 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, account2, node0->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send2);
	auto receive2 (system.wallet (0)->receive_action (send2->hash (), account2, node0->config.receive_minimum.number (), send2->link ().as_account ()));
	rpc_ctx->io_scope->renew ();
	// Test filter for send state blocks
	ASSERT_NE (nullptr, receive2);
	{
		boost::property_tree::ptree request;
		request.put ("action", "account_history");
		request.put ("account", nano::dev::genesis_key.pub.to_account ());
		boost::property_tree::ptree other_account;
		other_account.put ("", account2.to_account ());
		boost::property_tree::ptree filtered_accounts;
		filtered_accounts.push_back (std::make_pair ("", other_account));
		request.add_child ("account_filter", filtered_accounts);
		request.put ("count", 100);
		auto response (wait_response (system, rpc, request));
		auto history_node (response.get_child ("history"));
		ASSERT_EQ (history_node.size (), 2);
	}
	// Test filter for receive state blocks
	{
		boost::property_tree::ptree request;
		request.put ("action", "account_history");
		request.put ("account", account2.to_account ());
		boost::property_tree::ptree other_account;
		other_account.put ("", nano::dev::genesis_key.pub.to_account ());
		boost::property_tree::ptree filtered_accounts;
		filtered_accounts.push_back (std::make_pair ("", other_account));
		request.add_child ("account_filter", filtered_accounts);
		request.put ("count", 100);
		auto response (wait_response (system, rpc, request));
		auto history_node (response.get_child ("history"));
		ASSERT_EQ (history_node.size (), 1);
	}
}

TEST (rpc, history_count)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto change (system.wallet (0)->change_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub));
	ASSERT_NE (nullptr, change);
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub, node->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send);
	auto receive (system.wallet (0)->receive_action (send->hash (), nano::dev::genesis_key.pub, node->config.receive_minimum.number (), send->link ().as_account ()));
	ASSERT_NE (nullptr, receive);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "history");
	request.put ("hash", receive->hash ().to_string ());
	request.put ("count", 1);
	auto response (wait_response (system, rpc, request));
	auto & history_node (response.get_child ("history"));
	ASSERT_EQ (1, history_node.size ());
}

TEST (rpc, history_pruning)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.enable_voting = false; // Remove after allowing pruned voting
	nano::node_flags node_flags;
	node_flags.enable_pruning = true;
	auto node0 = add_ipc_enabled_node (system, node_config, node_flags);
	auto change (std::make_shared<nano::change_block> (nano::dev::genesis->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work.generate (nano::dev::genesis->hash ())));
	node0->process_active (change);
	auto send (std::make_shared<nano::send_block> (change->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - node0->config.receive_minimum.number (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work.generate (change->hash ())));
	node0->process_active (send);
	auto receive (std::make_shared<nano::receive_block> (send->hash (), send->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work.generate (send->hash ())));
	node0->process_active (receive);
	auto usend (std::make_shared<nano::state_block> (nano::dev::genesis->account (), receive->hash (), nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis->account (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work_generate_blocking (receive->hash ())));
	auto ureceive (std::make_shared<nano::state_block> (nano::dev::genesis->account (), usend->hash (), nano::dev::genesis->account (), nano::dev::genesis_amount, usend->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work_generate_blocking (usend->hash ())));
	auto uchange (std::make_shared<nano::state_block> (nano::dev::genesis->account (), ureceive->hash (), nano::keypair ().pub, nano::dev::genesis_amount, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node0->work_generate_blocking (ureceive->hash ())));
	node0->process_active (usend);
	node0->process_active (ureceive);
	node0->process_active (uchange);
	node0->block_processor.flush ();
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	// Confirm last block to prune previous
	{
		auto election = node0->active.election (change->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
	}
	ASSERT_TIMELY (2s, node0->block_confirmed (change->hash ()) && node0->active.active (send->qualified_root ()));
	{
		auto election = node0->active.election (send->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
	}
	ASSERT_TIMELY (2s, node0->block_confirmed (send->hash ()) && node0->active.active (receive->qualified_root ()));
	{
		auto election = node0->active.election (receive->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
	}
	ASSERT_TIMELY (2s, node0->block_confirmed (receive->hash ()) && node0->active.active (usend->qualified_root ()));
	{
		auto election = node0->active.election (usend->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
	}
	ASSERT_TIMELY (2s, node0->block_confirmed (usend->hash ()) && node0->active.active (ureceive->qualified_root ()));
	{
		auto election = node0->active.election (ureceive->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
	}
	ASSERT_TIMELY (2s, node0->block_confirmed (ureceive->hash ()) && node0->active.active (uchange->qualified_root ()));
	{
		auto election = node0->active.election (uchange->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
	}
	ASSERT_TIMELY (2s, node0->active.empty () && node0->block_confirmed (uchange->hash ()));
	ASSERT_TIMELY (2s, node0->ledger.cache.cemented_count == 7 && node0->confirmation_height_processor.current ().is_zero () && node0->confirmation_height_processor.awaiting_processing_size () == 0);
	// Pruning action
	{
		auto transaction (node0->store.tx_begin_write ());
		ASSERT_EQ (1, node0->ledger.pruning_action (transaction, change->hash (), 1));
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node0);
	boost::property_tree::ptree request;
	request.put ("action", "history");
	request.put ("hash", send->hash ().to_string ());
	request.put ("count", 100);
	auto response (wait_response (system, rpc, request));
	std::vector<std::tuple<std::string, std::string, std::string, std::string>> history_l;
	auto & history_node (response.get_child ("history"));
	for (auto i (history_node.begin ()), n (history_node.end ()); i != n; ++i)
	{
		history_l.push_back (std::make_tuple (i->second.get<std::string> ("type"), i->second.get<std::string> ("account", "-1"), i->second.get<std::string> ("amount", "-1"), i->second.get<std::string> ("hash")));
		boost::optional<std::string> amount (i->second.get_optional<std::string> ("amount"));
		ASSERT_FALSE (amount.is_initialized ()); // Cannot calculate amount
	}
	ASSERT_EQ (1, history_l.size ());
	ASSERT_EQ ("send", std::get<0> (history_l[0]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[0]));
	ASSERT_EQ ("-1", std::get<2> (history_l[0]));
	ASSERT_EQ (send->hash ().to_string (), std::get<3> (history_l[0]));
	// Pruning action
	{
		rpc_ctx->io_scope->reset ();
		auto transaction (node0->store.tx_begin_write ());
		ASSERT_EQ (1, node0->ledger.pruning_action (transaction, send->hash (), 1));
		rpc_ctx->io_scope->renew ();
	}
	boost::property_tree::ptree request2;
	request2.put ("action", "history");
	request2.put ("hash", receive->hash ().to_string ());
	request2.put ("count", 100);
	auto response2 (wait_response (system, rpc, request2));
	history_l.clear ();
	auto & history_node2 (response2.get_child ("history"));
	for (auto i (history_node2.begin ()), n (history_node2.end ()); i != n; ++i)
	{
		history_l.push_back (std::make_tuple (i->second.get<std::string> ("type"), i->second.get<std::string> ("account", "-1"), i->second.get<std::string> ("amount", "-1"), i->second.get<std::string> ("hash")));
		boost::optional<std::string> amount (i->second.get_optional<std::string> ("amount"));
		ASSERT_FALSE (amount.is_initialized ()); // Cannot calculate amount
		boost::optional<std::string> account (i->second.get_optional<std::string> ("account"));
		ASSERT_FALSE (account.is_initialized ()); // Cannot find source account
	}
	ASSERT_EQ (1, history_l.size ());
	ASSERT_EQ ("receive", std::get<0> (history_l[0]));
	ASSERT_EQ ("-1", std::get<1> (history_l[0]));
	ASSERT_EQ ("-1", std::get<2> (history_l[0]));
	ASSERT_EQ (receive->hash ().to_string (), std::get<3> (history_l[0]));
	// Pruning action
	{
		rpc_ctx->io_scope->reset ();
		auto transaction (node0->store.tx_begin_write ());
		ASSERT_EQ (1, node0->ledger.pruning_action (transaction, receive->hash (), 1));
		rpc_ctx->io_scope->renew ();
	}
	boost::property_tree::ptree request3;
	request3.put ("action", "history");
	request3.put ("hash", uchange->hash ().to_string ());
	request3.put ("count", 100);
	auto response3 (wait_response (system, rpc, request3));
	history_l.clear ();
	auto & history_node3 (response3.get_child ("history"));
	for (auto i (history_node3.begin ()), n (history_node3.end ()); i != n; ++i)
	{
		history_l.push_back (std::make_tuple (i->second.get<std::string> ("type"), i->second.get<std::string> ("account", "-1"), i->second.get<std::string> ("amount", "-1"), i->second.get<std::string> ("hash")));
	}
	ASSERT_EQ (2, history_l.size ());
	ASSERT_EQ ("receive", std::get<0> (history_l[0]));
	ASSERT_EQ (ureceive->hash ().to_string (), std::get<3> (history_l[0]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[0]));
	ASSERT_EQ (nano::Gxrb_ratio.convert_to<std::string> (), std::get<2> (history_l[0]));
	ASSERT_EQ ("unknown", std::get<0> (history_l[1]));
	ASSERT_EQ ("-1", std::get<1> (history_l[1]));
	ASSERT_EQ ("-1", std::get<2> (history_l[1]));
	ASSERT_EQ (usend->hash ().to_string (), std::get<3> (history_l[1]));
}

TEST (rpc, process_block)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_TIMELY (10s, node1->latest (nano::dev::genesis_key.pub) == send.hash ());
		std::string send_hash (response.get<std::string> ("hash"));
		ASSERT_EQ (send.hash ().to_string (), send_hash);
	}
	request.put ("json_block", true);
	{
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_blocks::invalid_block);
		ASSERT_EQ (ec.message (), response.get<std::string> ("error"));
	}
}

TEST (rpc, process_json_block)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	boost::property_tree::ptree request;
	request.put ("action", "process");
	boost::property_tree::ptree block_node;
	send.serialize_json (block_node);
	request.add_child ("block", block_node);
	{
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_blocks::invalid_block);
		ASSERT_EQ (ec.message (), response.get<std::string> ("error"));
	}
	request.put ("json_block", true);
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_TIMELY (10s, node1->latest (nano::dev::genesis_key.pub) == send.hash ());
		std::string send_hash (response.get<std::string> ("hash"));
		ASSERT_EQ (send.hash ().to_string (), send_hash);
	}
}

TEST (rpc, process_block_async)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	boost::property_tree::ptree request;
	request.put ("action", "process");
	request.put ("async", "true");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	request.put ("json_block", true);
	{
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_blocks::invalid_block);
		ASSERT_EQ (ec.message (), response.get<std::string> ("error"));
	}
	request.put ("json_block", false);
	{
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_common::is_not_state_block);
		ASSERT_EQ (ec.message (), response.get<std::string> ("error"));
	}

	auto state_send (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, latest, nano::dev::genesis_key.pub, nano::dev::genesis_amount - 100, nano::dev::genesis_key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest)));
	std::string json1;
	state_send->serialize_json (json1);
	request.put ("block", json1);
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ ("1", response.get<std::string> ("started"));
		ASSERT_TIMELY (10s, node1->latest (nano::dev::genesis_key.pub) == state_send->hash ());
	}
}

TEST (rpc, process_block_no_work)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	send.block_work_set (0);
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	auto response (wait_response (system, rpc, request));
	ASSERT_FALSE (response.get<std::string> ("error", "").empty ());
}

TEST (rpc, process_republish)
{
	nano::system system (2);
	auto & node1 (*system.nodes[0]);
	auto & node2 (*system.nodes[1]);
	auto node3 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node3);
	nano::keypair key;
	auto latest (node1.latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node3->work_generate_blocking (latest));
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	auto response (wait_response (system, rpc, request));
	ASSERT_TIMELY (10s, node2.latest (nano::dev::genesis_key.pub) == send.hash ());
}

TEST (rpc, process_subtype_send)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	system.add_node ();
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::state_block send (nano::dev::genesis->account (), latest, nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	request.put ("subtype", "receive");
	auto response (wait_response (system, rpc, request));
	std::error_code ec (nano::error_rpc::invalid_subtype_balance);
	ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "change");
	auto response2 (wait_response (system, rpc, request));
	ASSERT_EQ (response2.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "send");
	auto response3 (wait_response (system, rpc, request));
	ASSERT_EQ (send.hash ().to_string (), response3.get<std::string> ("hash"));
	ASSERT_TIMELY (10s, system.nodes[1]->latest (nano::dev::genesis_key.pub) == send.hash ());
}

TEST (rpc, process_subtype_open)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto & node2 = *system.add_node ();
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	auto send = std::make_shared<nano::state_block> (nano::dev::genesis->account (), latest, nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	ASSERT_EQ (nano::process_result::progress, node1->process (*send).code);
	ASSERT_EQ (nano::process_result::progress, node2.process (*send).code);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	node1->scheduler.manual (send);
	nano::state_block open (key.pub, 0, key.pub, nano::Gxrb_ratio, send->hash (), key.prv, key.pub, *node1->work_generate_blocking (key.pub));
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	open.serialize_json (json);
	request.put ("block", json);
	request.put ("subtype", "send");
	auto response (wait_response (system, rpc, request));
	std::error_code ec (nano::error_rpc::invalid_subtype_balance);
	ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "epoch");
	auto response2 (wait_response (system, rpc, request));
	ASSERT_EQ (response2.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "open");
	auto response3 (wait_response (system, rpc, request));
	ASSERT_EQ (open.hash ().to_string (), response3.get<std::string> ("hash"));
	ASSERT_TIMELY (10s, node2.latest (key.pub) == open.hash ());
}

TEST (rpc, process_subtype_receive)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto & node2 = *system.add_node ();
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	auto send = std::make_shared<nano::state_block> (nano::dev::genesis->account (), latest, nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	ASSERT_EQ (nano::process_result::progress, node1->process (*send).code);
	ASSERT_EQ (nano::process_result::progress, node2.process (*send).code);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	node1->scheduler.manual (send);
	nano::state_block receive (nano::dev::genesis_key.pub, send->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount, send->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (send->hash ()));
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	receive.serialize_json (json);
	request.put ("block", json);
	request.put ("subtype", "send");
	auto response (wait_response (system, rpc, request));
	std::error_code ec (nano::error_rpc::invalid_subtype_balance);
	ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "open");
	auto response2 (wait_response (system, rpc, request));
	ec = nano::error_rpc::invalid_subtype_previous;
	ASSERT_EQ (response2.get<std::string> ("error"), ec.message ());
	request.put ("subtype", "receive");
	auto response3 (wait_response (system, rpc, request));
	ASSERT_EQ (receive.hash ().to_string (), response3.get<std::string> ("hash"));
	ASSERT_TIMELY (10s, node2.latest (nano::dev::genesis_key.pub) == receive.hash ());
}

TEST (rpc, process_ledger_insufficient_work)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	ASSERT_LT (node->network_params.network.publish_thresholds.entry, node->network_params.network.publish_thresholds.epoch_1);
	auto latest (node->latest (nano::dev::genesis_key.pub));
	auto min_difficulty = node->network_params.network.publish_thresholds.entry;
	auto max_difficulty = node->network_params.network.publish_thresholds.epoch_1;
	nano::state_block send (nano::dev::genesis->account (), latest, nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, system.work_generate_limited (latest, min_difficulty, max_difficulty));
	ASSERT_LT (send.difficulty (), max_difficulty);
	ASSERT_GE (send.difficulty (), min_difficulty);
	boost::property_tree::ptree request;
	request.put ("action", "process");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	request.put ("subtype", "send");
	auto response (wait_response (system, rpc, request));
	std::error_code ec (nano::error_process::insufficient_work);
	ASSERT_EQ (1, response.count ("error"));
	ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
}

TEST (rpc, keepalive)
{
	nano::system system;
	auto node0 = add_ipc_enabled_node (system);
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	node1->start ();
	system.nodes.push_back (node1);
	auto [rpc, rpc_ctx] = add_rpc (system, node0);
	boost::property_tree::ptree request;
	request.put ("action", "keepalive");
	auto address (boost::str (boost::format ("%1%") % node1->network.endpoint ().address ()));
	auto port (boost::str (boost::format ("%1%") % node1->network.endpoint ().port ()));
	request.put ("address", address);
	request.put ("port", port);
	ASSERT_EQ (nullptr, node0->network.udp_channels.channel (node1->network.endpoint ()));
	ASSERT_EQ (0, node0->network.size ());
	auto response (wait_response (system, rpc, request));
	system.deadline_set (10s);
	while (node0->network.find_channel (node1->network.endpoint ()) == nullptr)
	{
		ASSERT_EQ (0, node0->network.size ());
		ASSERT_NO_ERROR (system.poll ());
	}
	node1->stop ();
}

TEST (rpc, peers)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto port = nano::get_available_port ();
	system.add_node (nano::node_config (port, system.logging));
	nano::endpoint endpoint (boost::asio::ip::make_address_v6 ("fc00::1"), 4000);
	node->network.udp_channels.insert (endpoint, node->network_params.protocol.protocol_version);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "peers");
	auto response (wait_response (system, rpc, request));
	auto & peers_node (response.get_child ("peers"));
	ASSERT_EQ (2, peers_node.size ());
	ASSERT_EQ (std::to_string (node->network_params.protocol.protocol_version), peers_node.get<std::string> ((boost::format ("[::1]:%1%") % port).str ()));
	// Previously "[::ffff:80.80.80.80]:4000", but IPv4 address cause "No such node thrown in the test body" issue with peers_node.get
	std::stringstream endpoint_text;
	endpoint_text << endpoint;
	ASSERT_EQ (std::to_string (node->network_params.protocol.protocol_version), peers_node.get<std::string> (endpoint_text.str ()));
}

TEST (rpc, peers_node_id)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto port = nano::get_available_port ();
	system.add_node (nano::node_config (port, system.logging));
	nano::endpoint endpoint (boost::asio::ip::make_address_v6 ("fc00::1"), 4000);
	node->network.udp_channels.insert (endpoint, node->network_params.protocol.protocol_version);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "peers");
	request.put ("peer_details", true);
	auto response (wait_response (system, rpc, request));
	auto & peers_node (response.get_child ("peers"));
	ASSERT_EQ (2, peers_node.size ());
	auto tree1 (peers_node.get_child ((boost::format ("[::1]:%1%") % port).str ()));
	ASSERT_EQ (std::to_string (node->network_params.protocol.protocol_version), tree1.get<std::string> ("protocol_version"));
	ASSERT_EQ (system.nodes[1]->node_id.pub.to_node_id (), tree1.get<std::string> ("node_id"));
	std::stringstream endpoint_text;
	endpoint_text << endpoint;
	auto tree2 (peers_node.get_child (endpoint_text.str ()));
	ASSERT_EQ (std::to_string (node->network_params.protocol.protocol_version), tree2.get<std::string> ("protocol_version"));
	ASSERT_EQ ("", tree2.get<std::string> ("node_id"));
}

TEST (rpc, pending)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key1;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto block1 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key1.pub, 100));
	node->scheduler.flush ();
	ASSERT_TIMELY (5s, !node->active.active (*block1));
	ASSERT_TIMELY (5s, node->ledger.cache.cemented_count == 2 && node->confirmation_height_processor.current ().is_zero () && node->confirmation_height_processor.awaiting_processing_size () == 0);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "pending");
	request.put ("account", key1.pub.to_account ());
	request.put ("count", "100");
	{
		auto response (wait_response (system, rpc, request));
		auto & blocks_node (response.get_child ("blocks"));
		ASSERT_EQ (1, blocks_node.size ());
		nano::block_hash hash (blocks_node.begin ()->second.get<std::string> (""));
		ASSERT_EQ (block1->hash (), hash);
	}
	request.put ("sorting", "true"); // Sorting test
	{
		auto response (wait_response (system, rpc, request));
		auto & blocks_node (response.get_child ("blocks"));
		ASSERT_EQ (1, blocks_node.size ());
		nano::block_hash hash (blocks_node.begin ()->first);
		ASSERT_EQ (block1->hash (), hash);
		std::string amount (blocks_node.begin ()->second.get<std::string> (""));
		ASSERT_EQ ("100", amount);
	}
	request.put ("threshold", "100"); // Threshold test
	{
		auto response (wait_response (system, rpc, request));
		auto & blocks_node (response.get_child ("blocks"));
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
		auto response (wait_response (system, rpc, request, 10s));
		auto & blocks_node (response.get_child ("blocks"));
		ASSERT_EQ (0, blocks_node.size ());
	}
	request.put ("threshold", "0");
	request.put ("source", "true");
	request.put ("min_version", "true");
	{
		auto response (wait_response (system, rpc, request));
		auto & blocks_node (response.get_child ("blocks"));
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
		ASSERT_EQ (sources[block1->hash ()], nano::dev::genesis_key.pub);
	}

	request.put ("account", key1.pub.to_account ());
	request.put ("source", "false");
	request.put ("min_version", "false");

	auto check_block_response_count_l = [&system, &request, rpc = rpc] (size_t size) {
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (size, response.get_child ("blocks").size ());
	};

	check_block_response_count_l (1);
	rpc_ctx->io_scope->reset ();
	reset_confirmation_height (system.nodes.front ()->store, block1->account ());
	rpc_ctx->io_scope->renew ();
	check_block_response_count_l (0);
	request.put ("include_only_confirmed", "false");
	rpc_ctx->io_scope->renew ();
	check_block_response_count_l (1);
	request.put ("include_only_confirmed", "true");

	// Sorting with a smaller count than total should give absolute sorted amounts
	rpc_ctx->io_scope->reset ();
	node->store.confirmation_height.put (node->store.tx_begin_write (), nano::dev::genesis_key.pub, { 2, block1->hash () });
	auto block2 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key1.pub, 200));
	auto block3 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key1.pub, 300));
	auto block4 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key1.pub, 400));
	rpc_ctx->io_scope->renew ();

	ASSERT_TIMELY (10s, node->ledger.account_pending (node->store.tx_begin_read (), key1.pub) == 1000);
	ASSERT_TIMELY (5s, !node->active.active (*block4));
	ASSERT_TIMELY (5s, node->block_confirmed (block4->hash ()));

	request.put ("count", "2");

	{
		auto response (wait_response (system, rpc, request));
		auto & blocks_node (response.get_child ("blocks"));
		ASSERT_EQ (2, blocks_node.size ());
		nano::block_hash hash (blocks_node.begin ()->first);
		nano::block_hash hash1 ((++blocks_node.begin ())->first);
		ASSERT_EQ (block4->hash (), hash);
		ASSERT_EQ (block3->hash (), hash1);
	}
}

TEST (rpc, pending_burn)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::account burn (0);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto block1 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, burn, 100));
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	node->scheduler.flush ();
	ASSERT_TIMELY (5s, !node->active.active (*block1));
	ASSERT_TIMELY (5s, node->ledger.cache.cemented_count == 2 && node->confirmation_height_processor.current ().is_zero () && node->confirmation_height_processor.awaiting_processing_size () == 0);
	boost::property_tree::ptree request;
	request.put ("action", "pending");
	request.put ("account", burn.to_account ());
	request.put ("count", "100");
	{
		auto response (wait_response (system, rpc, request));
		auto & blocks_node (response.get_child ("blocks"));
		ASSERT_EQ (1, blocks_node.size ());
		nano::block_hash hash (blocks_node.begin ()->second.get<std::string> (""));
		ASSERT_EQ (block1->hash (), hash);
	}
}

TEST (rpc, search_pending)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto wallet (node->wallets.items.begin ()->first.to_string ());
	auto latest (node->latest (nano::dev::genesis_key.pub));
	nano::send_block block (latest, nano::dev::genesis_key.pub, nano::dev::genesis_amount - node->config.receive_minimum.number (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node->work_generate_blocking (latest));
	{
		auto transaction (node->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, block).code);
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "search_pending");
	request.put ("wallet", wallet);
	auto response (wait_response (system, rpc, request));
	ASSERT_TIMELY (10s, node->balance (nano::dev::genesis_key.pub) == nano::dev::genesis_amount);
}

TEST (rpc, version)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "version");
	test_response response1 (request1, rpc->config.port, system.io_ctx);
	ASSERT_TIMELY (5s, response1.status != 0);
	ASSERT_EQ (200, response1.status);
	ASSERT_EQ ("1", response1.json.get<std::string> ("rpc_version"));
	{
		auto transaction (node1->store.tx_begin_read ());
		ASSERT_EQ (std::to_string (node1->store.version.get (transaction)), response1.json.get<std::string> ("store_version"));
	}
	ASSERT_EQ (std::to_string (node1->network_params.protocol.protocol_version), response1.json.get<std::string> ("protocol_version"));
	ASSERT_EQ (boost::str (boost::format ("Nano %1%") % NANO_VERSION_STRING), response1.json.get<std::string> ("node_vendor"));
	ASSERT_EQ (node1->store.vendor_get (), response1.json.get<std::string> ("store_vendor"));
	auto network_label (node1->network_params.network.get_current_network_as_string ());
	ASSERT_EQ (network_label, response1.json.get<std::string> ("network"));
	auto genesis_open (node1->latest (nano::dev::genesis_key.pub));
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::block_hash hash (1);
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	auto verify_response = [node, rpc = rpc, &system] (auto & request, auto & hash) {
		test_response response (request, rpc->config.port, system.io_ctx);
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
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.max_work_generate_multiplier = 1000;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::block_hash hash (1);
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	{
		uint64_t difficulty (0xfff0000000000000);
		request.put ("difficulty", nano::to_string_hex (difficulty));
		auto response (wait_response (system, rpc, request, 10s));
		auto work_text (response.get<std::string> ("work"));
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (work_text, work));
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		auto response_difficulty_text (response.get<std::string> ("difficulty"));
		uint64_t response_difficulty;
		ASSERT_FALSE (nano::from_string_hex (response_difficulty_text, response_difficulty));
		ASSERT_EQ (result_difficulty, response_difficulty);
		auto multiplier = response.get<double> ("multiplier");
		// Expected multiplier from base threshold, not from the given difficulty
		ASSERT_NEAR (nano::difficulty::to_multiplier (result_difficulty, node->default_difficulty (nano::work_version::work_1)), multiplier, 1e-10);
		ASSERT_GE (result_difficulty, difficulty);
	}
	{
		uint64_t difficulty (0xffff000000000000);
		request.put ("difficulty", nano::to_string_hex (difficulty));
		auto response (wait_response (system, rpc, request));
		auto work_text (response.get<std::string> ("work"));
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (work_text, work));
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		ASSERT_GE (result_difficulty, difficulty);
	}
	{
		uint64_t difficulty (node->max_work_generate_difficulty (nano::work_version::work_1) + 1);
		request.put ("difficulty", nano::to_string_hex (difficulty));
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_rpc::difficulty_limit);
		ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, work_generate_multiplier)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.max_work_generate_multiplier = 100;
	auto node = add_ipc_enabled_node (system, node_config);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
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
		auto response (wait_response (system, rpc, request, 10s));
		auto work_text (response.get_optional<std::string> ("work"));
		ASSERT_TRUE (work_text.is_initialized ());
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (*work_text, work));
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		auto response_difficulty_text (response.get<std::string> ("difficulty"));
		uint64_t response_difficulty;
		ASSERT_FALSE (nano::from_string_hex (response_difficulty_text, response_difficulty));
		ASSERT_EQ (result_difficulty, response_difficulty);
		auto result_multiplier = response.get<double> ("multiplier");
		ASSERT_GE (result_multiplier, multiplier);
	}
	{
		request.put ("multiplier", -1.5);
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_rpc::bad_multiplier_format);
		ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	}
	{
		double max_multiplier (nano::difficulty::to_multiplier (node->max_work_generate_difficulty (nano::work_version::work_1), node->default_difficulty (nano::work_version::work_1)));
		request.put ("multiplier", max_multiplier + 1);
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_rpc::difficulty_limit);
		ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, work_generate_block_high)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::keypair key;
	nano::state_block block (key.pub, 0, nano::dev::genesis_key.pub, nano::Gxrb_ratio, 123, key.prv, key.pub, *node->work_generate_blocking (key.pub));
	nano::block_hash hash (block.root ().as_block_hash ());
	auto block_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, block.block_work ()));
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	request.put ("json_block", "true");
	boost::property_tree::ptree json;
	block.serialize_json (json);
	request.add_child ("block", json);
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (1, response.count ("error"));
		ASSERT_EQ (std::error_code (nano::error_rpc::block_work_enough).message (), response.get<std::string> ("error"));
	}
}

TEST (rpc, work_generate_block_low)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::keypair key;
	nano::state_block block (key.pub, 0, nano::dev::genesis_key.pub, nano::Gxrb_ratio, 123, key.prv, key.pub, 0);
	auto threshold (node->default_difficulty (block.work_version ()));
	block.block_work_set (system.work_generate_limited (block.root ().as_block_hash (), threshold, nano::difficulty::from_multiplier (node->config.max_work_generate_multiplier / 10, threshold)));
	nano::block_hash hash (block.root ().as_block_hash ());
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
		auto response (wait_response (system, rpc, request, 10s));
		auto work_text (response.get_optional<std::string> ("work"));
		ASSERT_TRUE (work_text.is_initialized ());
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (*work_text, work));
		ASSERT_NE (block.block_work (), work);
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		auto response_difficulty_text (response.get<std::string> ("difficulty"));
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::keypair key;
	nano::state_block block (key.pub, 0, nano::dev::genesis_key.pub, nano::Gxrb_ratio, 123, key.prv, key.pub, *node->work_generate_blocking (key.pub));
	nano::block_hash hash (1);
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	request.put ("json_block", "false");
	std::string json;
	block.serialize_json (json);
	request.put ("block", json);
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (1, response.count ("error"));
		ASSERT_EQ (std::error_code (nano::error_rpc::block_root_mismatch).message (), response.get<std::string> ("error"));
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto send_block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	nano::state_block block (key.pub, 0, nano::dev::genesis_key.pub, nano::Gxrb_ratio, send_block->hash (), key.prv, key.pub, 0);
	auto threshold (nano::work_threshold (block.work_version (), nano::block_details (nano::epoch::epoch_2, false, true, false)));
	block.block_work_set (system.work_generate_limited (block.root ().as_block_hash (), 1, threshold - 1));
	nano::block_hash hash (block.root ().as_block_hash ());
	auto [rpc, rpc_ctx] = add_rpc (system, node);
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
		auto response (wait_response (system, rpc, request, 10s));
		auto work_text (response.get_optional<std::string> ("work"));
		ASSERT_TRUE (work_text.is_initialized ());
		uint64_t work;
		ASSERT_FALSE (nano::from_string_hex (*work_text, work));
		auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work));
		auto response_difficulty_text (response.get<std::string> ("difficulty"));
		uint64_t response_difficulty;
		ASSERT_FALSE (nano::from_string_hex (response_difficulty_text, response_difficulty));
		ASSERT_EQ (result_difficulty, response_difficulty);
		ASSERT_GE (result_difficulty, node->network_params.network.publish_thresholds.epoch_2_receive);
		finished = result_difficulty < node->network_params.network.publish_thresholds.epoch_1;
		ASSERT_LT (++iteration, 200);
	}
}

TEST (rpc, work_cancel)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	nano::block_hash hash1 (1);
	boost::property_tree::ptree request1;
	request1.put ("action", "work_cancel");
	request1.put ("hash", hash1.to_string ());
	std::atomic<bool> done (false);
	system.deadline_set (10s);
	while (!done)
	{
		system.work.generate (nano::work_version::work_1, hash1, node1->network_params.network.publish_thresholds.base, [&done] (boost::optional<uint64_t> work_a) {
			done = !work_a;
		});
		auto response1 (wait_response (system, rpc, request1));
		std::error_code ec;
		ASSERT_NO_ERROR (ec);
		std::string success (response1.get<std::string> ("success"));
		ASSERT_TRUE (success.empty ());
	}
}

TEST (rpc, work_peer_bad)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto & node2 = *system.add_node ();
	node2.config.work_peers.push_back (std::make_pair (boost::asio::ip::address_v6::any ().to_string (), 0));
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	nano::block_hash hash1 (1);
	std::atomic<uint64_t> work (0);
	node2.work_generate (nano::work_version::work_1, hash1, node2.network_params.network.publish_thresholds.base, [&work] (boost::optional<uint64_t> work_a) {
		ASSERT_TRUE (work_a.is_initialized ());
		work = *work_a;
	});
	ASSERT_TIMELY (5s, nano::work_difficulty (nano::work_version::work_1, hash1, work) >= nano::work_threshold_base (nano::work_version::work_1));
}

TEST (rpc, work_peer_one)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto & node2 = *system.add_node ();
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	node2.config.work_peers.push_back (std::make_pair (node1->network.endpoint ().address ().to_string (), rpc->config.port));
	nano::keypair key1;
	std::atomic<uint64_t> work (0);
	node2.work_generate (nano::work_version::work_1, key1.pub, node1->network_params.network.publish_thresholds.base, [&work] (boost::optional<uint64_t> work_a) {
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
	auto node2 = add_ipc_enabled_node (system2);
	auto node3 = add_ipc_enabled_node (system3);
	auto node4 = add_ipc_enabled_node (system4);
	auto [rpc2, rpc_ctx_2] = add_rpc (system2, node2);
	auto [rpc3, rpc_ctx_3] = add_rpc (system3, node3);
	auto [rpc4, rpc_ctx_4] = add_rpc (system4, node4);
	node1.config.work_peers.push_back (std::make_pair (node2->network.endpoint ().address ().to_string (), rpc2->config.port));
	node1.config.work_peers.push_back (std::make_pair (node3->network.endpoint ().address ().to_string (), rpc3->config.port));
	node1.config.work_peers.push_back (std::make_pair (node4->network.endpoint ().address ().to_string (), rpc4->config.port));

	std::array<std::atomic<uint64_t>, 10> works;
	for (auto i (0); i < works.size (); ++i)
	{
		nano::keypair key1;
		node1.work_generate (nano::work_version::work_1, key1.pub, node1.network_params.network.publish_thresholds.base, [&work = works[i]] (boost::optional<uint64_t> work_a) {
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::block_hash hash (1);
	boost::property_tree::ptree request;
	request.put ("action", "work_generate");
	request.put ("hash", hash.to_string ());
	request.put ("version", "work_invalid");
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (1, response.count ("error"));
		ASSERT_EQ (std::error_code (nano::error_rpc::bad_work_version).message (), response.get<std::string> ("error"));
	}
	request.put ("action", "work_validate");
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (1, response.count ("error"));
		ASSERT_EQ (std::error_code (nano::error_rpc::bad_work_version).message (), response.get<std::string> ("error"));
	}
}

TEST (rpc, block_count)
{
	{
		nano::system system;
		auto node1 = add_ipc_enabled_node (system);
		auto [rpc, rpc_ctx] = add_rpc (system, node1);
		boost::property_tree::ptree request1;
		request1.put ("action", "block_count");
		{
			auto response1 (wait_response (system, rpc, request1));
			ASSERT_EQ ("1", response1.get<std::string> ("count"));
			ASSERT_EQ ("0", response1.get<std::string> ("unchecked"));
			ASSERT_EQ ("1", response1.get<std::string> ("cemented"));
		}
	}

	// Should be able to get all counts even when enable_control is false.
	{
		nano::system system;
		auto node1 = add_ipc_enabled_node (system);
		auto [rpc, rpc_ctx] = add_rpc (system, node1);
		boost::property_tree::ptree request1;
		request1.put ("action", "block_count");
		{
			auto response1 (wait_response (system, rpc, request1));
			ASSERT_EQ ("1", response1.get<std::string> ("count"));
			ASSERT_EQ ("0", response1.get<std::string> ("unchecked"));
			ASSERT_EQ ("1", response1.get<std::string> ("cemented"));
		}
	}
}

TEST (rpc, block_count_pruning)
{
	nano::system system;
	auto & node0 = *system.add_node ();
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.enable_voting = false; // Remove after allowing pruned voting
	nano::node_flags node_flags;
	node_flags.enable_pruning = true;
	auto node1 = add_ipc_enabled_node (system, node_config, node_flags);
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	auto send1 (std::make_shared<nano::send_block> (latest, nano::dev::genesis_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest)));
	node1->process_active (send1);
	auto receive1 (std::make_shared<nano::receive_block> (send1->hash (), send1->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (send1->hash ())));
	node1->process_active (receive1);
	node1->block_processor.flush ();
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_TIMELY (5s, node1->ledger.cache.cemented_count == 3 && node1->confirmation_height_processor.current ().is_zero () && node1->confirmation_height_processor.awaiting_processing_size () == 0);
	// Pruning action
	{
		auto transaction (node1->store.tx_begin_write ());
		ASSERT_EQ (1, node1->ledger.pruning_action (transaction, send1->hash (), 1));
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "block_count");
	{
		auto response1 (wait_response (system, rpc, request1));
		ASSERT_EQ ("3", response1.get<std::string> ("count"));
		ASSERT_EQ ("0", response1.get<std::string> ("unchecked"));
		ASSERT_EQ ("3", response1.get<std::string> ("cemented"));
		ASSERT_EQ ("2", response1.get<std::string> ("full"));
		ASSERT_EQ ("1", response1.get<std::string> ("pruned"));
	}
}

TEST (rpc, frontier_count)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "frontier_count");
	auto response1 (wait_response (system, rpc, request1));
	ASSERT_EQ ("1", response1.get<std::string> ("count"));
}

TEST (rpc, account_count)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "account_count");
	auto response1 (wait_response (system, rpc, request1));
	ASSERT_EQ ("1", response1.get<std::string> ("count"));
}

TEST (rpc, available_supply)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "available_supply");
	auto response1 (wait_response (system, rpc, request1));
	ASSERT_EQ ("0", response1.get<std::string> ("available"));
	rpc_ctx->io_scope->reset ();
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	auto block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, 1));
	rpc_ctx->io_scope->renew ();
	auto response2 (wait_response (system, rpc, request1));
	ASSERT_EQ ("1", response2.get<std::string> ("available"));
	rpc_ctx->io_scope->reset ();
	auto block2 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, 0, 100)); // Sending to burning 0 account
	rpc_ctx->io_scope->renew ();
	auto response3 (wait_response (system, rpc, request1, 10s));
	ASSERT_EQ ("1", response3.get<std::string> ("available"));
}

TEST (rpc, mrai_to_raw)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "mrai_to_raw");
	request1.put ("amount", "1");
	auto response1 (wait_response (system, rpc, request1));
	ASSERT_EQ (nano::Mxrb_ratio.convert_to<std::string> (), response1.get<std::string> ("amount"));
}

TEST (rpc, mrai_from_raw)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "mrai_from_raw");
	request1.put ("amount", nano::Mxrb_ratio.convert_to<std::string> ());
	auto response1 (wait_response (system, rpc, request1));
	ASSERT_EQ ("1", response1.get<std::string> ("amount"));
}

TEST (rpc, krai_to_raw)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "krai_to_raw");
	request1.put ("amount", "1");
	auto response1 (wait_response (system, rpc, request1));
	ASSERT_EQ (nano::kxrb_ratio.convert_to<std::string> (), response1.get<std::string> ("amount"));
}

TEST (rpc, krai_from_raw)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "krai_from_raw");
	request1.put ("amount", nano::kxrb_ratio.convert_to<std::string> ());
	auto response1 (wait_response (system, rpc, request1));
	ASSERT_EQ ("1", response1.get<std::string> ("amount"));
}

TEST (rpc, nano_to_raw)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "nano_to_raw");
	request1.put ("amount", "1");
	auto response1 (wait_response (system, rpc, request1));
	ASSERT_EQ (nano::xrb_ratio.convert_to<std::string> (), response1.get<std::string> ("amount"));
}

TEST (rpc, nano_from_raw)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request1;
	request1.put ("action", "nano_from_raw");
	request1.put ("amount", nano::xrb_ratio.convert_to<std::string> ());
	auto response1 (wait_response (system, rpc, request1));
	ASSERT_EQ ("1", response1.get<std::string> ("amount"));
}

TEST (rpc, account_representative)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("account", nano::dev::genesis->account ().to_account ());
	request.put ("action", "account_representative");
	auto response (wait_response (system, rpc, request));
	std::string account_text1 (response.get<std::string> ("representative"));
	ASSERT_EQ (account_text1, nano::dev::genesis->account ().to_account ());
}

TEST (rpc, account_representative_set)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	nano::keypair rep;
	request.put ("account", nano::dev::genesis->account ().to_account ());
	request.put ("representative", rep.pub.to_account ());
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("action", "account_representative_set");
	auto response (wait_response (system, rpc, request));
	std::string block_text1 (response.get<std::string> ("block"));
	nano::block_hash hash;
	ASSERT_FALSE (hash.decode_hex (block_text1));
	ASSERT_FALSE (hash.is_zero ());
	auto transaction (node->store.tx_begin_read ());
	ASSERT_TRUE (node->store.block.exists (transaction, hash));
	ASSERT_EQ (rep.pub, node->store.block.get (transaction, hash)->representative ());
}

TEST (rpc, account_representative_set_work_disabled)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.work_threads = 0;
	auto node = add_ipc_enabled_node (system, node_config);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	nano::keypair rep;
	request.put ("account", nano::dev::genesis->account ().to_account ());
	request.put ("representative", rep.pub.to_account ());
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("action", "account_representative_set");
	{
		auto response (wait_response (system, rpc, request, 10s));
		ASSERT_EQ (std::error_code (nano::error_common::disabled_work_generation).message (), response.get<std::string> ("error"));
	}
}

TEST (rpc, account_representative_set_epoch_2)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);

	// Upgrade the genesis account to epoch 2
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1));
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));

	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv, false);

	auto target_difficulty = nano::work_threshold (nano::work_version::work_1, nano::block_details (nano::epoch::epoch_2, false, false, false));
	ASSERT_LT (node->network_params.network.publish_thresholds.entry, target_difficulty);
	auto min_difficulty = node->network_params.network.publish_thresholds.entry;

	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "account_representative_set");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	request.put ("representative", nano::keypair ().pub.to_account ());

	// Test that the correct error is given if there is insufficient work
	auto insufficient = system.work_generate_limited (nano::dev::genesis->hash (), min_difficulty, target_difficulty);
	request.put ("work", nano::to_string_hex (insufficient));
	{
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_common::invalid_work);
		ASSERT_EQ (1, response.count ("error"));
		ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, bootstrap)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	nano::system system1 (1);
	auto node1 = system1.nodes[0];
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, nano::dev::genesis->account (), 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	{
		auto transaction (node1->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node1->ledger.process (transaction, send).code);
	}
	auto [rpc, rpc_ctx] = add_rpc (system0, node);
	boost::property_tree::ptree request;
	request.put ("action", "bootstrap");
	request.put ("address", "::ffff:127.0.0.1");
	request.put ("port", node1->network.endpoint ().port ());
	test_response response (request, rpc->config.port, system0.io_ctx);
	while (response.status == 0)
	{
		system0.poll ();
	}
	system1.deadline_set (10s);
	while (node->latest (nano::dev::genesis->account ()) != node1->latest (nano::dev::genesis->account ()))
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
	ASSERT_TRUE (system0.wallet (0)->exists (key1));
	auto [rpc, rpc_ctx] = add_rpc (system0, node);
	boost::property_tree::ptree request;
	request.put ("action", "account_remove");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("account", key1.to_account ());
	auto response (wait_response (system0, rpc, request));
	ASSERT_FALSE (system0.wallet (0)->exists (key1));
}

TEST (rpc, representatives)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	auto [rpc, rpc_ctx] = add_rpc (system0, node);
	boost::property_tree::ptree request;
	request.put ("action", "representatives");
	auto response (wait_response (system0, rpc, request));
	auto & representatives_node (response.get_child ("representatives"));
	std::vector<nano::account> representatives;
	for (auto i (representatives_node.begin ()), n (representatives_node.end ()); i != n; ++i)
	{
		nano::account account;
		ASSERT_FALSE (account.decode_account (i->first));
		representatives.push_back (account);
	}
	ASSERT_EQ (1, representatives.size ());
	ASSERT_EQ (nano::dev::genesis->account (), representatives[0]);
}

// wallet_seed is only available over IPC's unsafe encoding, and when running on test network
TEST (rpc, wallet_seed)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::raw_key seed;
	{
		auto transaction (node->wallets.tx_begin_read ());
		system.wallet (0)->store.seed (seed, transaction);
	}
	boost::property_tree::ptree request;
	request.put ("action", "wallet_seed");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	auto response (wait_response (system, rpc, request));
	{
		std::string seed_text (response.get<std::string> ("seed"));
		ASSERT_EQ (seed.to_string (), seed_text);
	}
}

TEST (rpc, wallet_change_seed)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	auto [rpc, rpc_ctx] = add_rpc (system0, node);
	nano::raw_key seed;
	nano::random_pool::generate_block (seed.bytes.data (), seed.bytes.size ());
	{
		auto transaction (node->wallets.tx_begin_read ());
		nano::raw_key seed0;
		nano::random_pool::generate_block (seed0.bytes.data (), seed0.bytes.size ());
		system0.wallet (0)->store.seed (seed0, transaction);
		ASSERT_NE (seed, seed0);
	}
	auto prv = nano::deterministic_key (seed, 0);
	auto pub (nano::pub_key (prv));
	boost::property_tree::ptree request;
	request.put ("action", "wallet_change_seed");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("seed", seed.to_string ());
	auto response (wait_response (system0, rpc, request));
	{
		auto transaction (node->wallets.tx_begin_read ());
		nano::raw_key seed0;
		system0.wallet (0)->store.seed (seed0, transaction);
		ASSERT_EQ (seed, seed0);
	}
	auto account_text (response.get<std::string> ("last_restored_account"));
	nano::account account;
	ASSERT_FALSE (account.decode_account (account_text));
	ASSERT_TRUE (system0.wallet (0)->exists (account));
	ASSERT_EQ (pub, account);
	ASSERT_EQ ("1", response.get<std::string> ("restored_count"));
}

TEST (rpc, wallet_frontiers)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	system0.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system0, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_frontiers");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	auto response (wait_response (system0, rpc, request));
	auto & frontiers_node (response.get_child ("frontiers"));
	std::vector<nano::account> frontiers;
	for (auto i (frontiers_node.begin ()), n (frontiers_node.end ()); i != n; ++i)
	{
		frontiers.push_back (nano::account (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, frontiers.size ());
	ASSERT_EQ (node->latest (nano::dev::genesis->account ()), frontiers[0]);
}

TEST (rpc, work_validate)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	nano::block_hash hash (1);
	uint64_t work1 (*node1->work_generate_blocking (hash));
	boost::property_tree::ptree request;
	request.put ("action", "work_validate");
	request.put ("hash", hash.to_string ());
	request.put ("work", nano::to_string_hex (work1));
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (0, response.count ("valid"));
		ASSERT_TRUE (response.get<bool> ("valid_all"));
		ASSERT_TRUE (response.get<bool> ("valid_receive"));
		std::string difficulty_text (response.get<std::string> ("difficulty"));
		uint64_t difficulty;
		ASSERT_FALSE (nano::from_string_hex (difficulty_text, difficulty));
		ASSERT_GE (difficulty, node1->default_difficulty (nano::work_version::work_1));
		double multiplier (response.get<double> ("multiplier"));
		ASSERT_NEAR (multiplier, nano::difficulty::to_multiplier (difficulty, node1->default_difficulty (nano::work_version::work_1)), 1e-6);
	}
	uint64_t work2 (0);
	request.put ("work", nano::to_string_hex (work2));
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (0, response.count ("valid"));
		ASSERT_FALSE (response.get<bool> ("valid_all"));
		ASSERT_FALSE (response.get<bool> ("valid_receive"));
		std::string difficulty_text (response.get<std::string> ("difficulty"));
		uint64_t difficulty;
		ASSERT_FALSE (nano::from_string_hex (difficulty_text, difficulty));
		ASSERT_GE (node1->default_difficulty (nano::work_version::work_1), difficulty);
		double multiplier (response.get<double> ("multiplier"));
		ASSERT_NEAR (multiplier, nano::difficulty::to_multiplier (difficulty, node1->default_difficulty (nano::work_version::work_1)), 1e-6);
	}
	auto result_difficulty (nano::work_difficulty (nano::work_version::work_1, hash, work1));
	ASSERT_GE (result_difficulty, node1->default_difficulty (nano::work_version::work_1));
	request.put ("work", nano::to_string_hex (work1));
	request.put ("difficulty", nano::to_string_hex (result_difficulty));
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_TRUE (response.get<bool> ("valid"));
		ASSERT_TRUE (response.get<bool> ("valid_all"));
		ASSERT_TRUE (response.get<bool> ("valid_receive"));
	}
	uint64_t difficulty4 (0xfff0000000000000);
	request.put ("work", nano::to_string_hex (work1));
	request.put ("difficulty", nano::to_string_hex (difficulty4));
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (result_difficulty >= difficulty4, response.get<bool> ("valid"));
		ASSERT_EQ (result_difficulty >= node1->default_difficulty (nano::work_version::work_1), response.get<bool> ("valid_all"));
		ASSERT_EQ (result_difficulty >= node1->network_params.network.publish_thresholds.epoch_2_receive, response.get<bool> ("valid_all"));
	}
	uint64_t work3 (*node1->work_generate_blocking (hash, difficulty4));
	request.put ("work", nano::to_string_hex (work3));
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_TRUE (response.get<bool> ("valid"));
		ASSERT_TRUE (response.get<bool> ("valid_all"));
		ASSERT_TRUE (response.get<bool> ("valid_receive"));
	}
}

TEST (rpc, work_validate_epoch_2)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto epoch1 = system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1);
	ASSERT_NE (nullptr, epoch1);
	ASSERT_EQ (node->network_params.network.publish_thresholds.epoch_2, node->network_params.network.publish_thresholds.base);
	auto work = system.work_generate_limited (epoch1->hash (), node->network_params.network.publish_thresholds.epoch_1, node->network_params.network.publish_thresholds.base);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "work_validate");
	request.put ("hash", epoch1->hash ().to_string ());
	request.put ("work", nano::to_string_hex (work));
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (0, response.count ("valid"));
		ASSERT_FALSE (response.get<bool> ("valid_all"));
		ASSERT_TRUE (response.get<bool> ("valid_receive"));
		std::string difficulty_text (response.get<std::string> ("difficulty"));
		uint64_t difficulty{ 0 };
		ASSERT_FALSE (nano::from_string_hex (difficulty_text, difficulty));
		double multiplier (response.get<double> ("multiplier"));
		ASSERT_NEAR (multiplier, nano::difficulty::to_multiplier (difficulty, node->network_params.network.publish_thresholds.epoch_2), 1e-6);
	};
	// After upgrading, the higher difficulty is used to validate and calculate the multiplier
	rpc_ctx->io_scope->reset ();
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));
	rpc_ctx->io_scope->renew ();
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (0, response.count ("valid"));
		ASSERT_FALSE (response.get<bool> ("valid_all"));
		ASSERT_TRUE (response.get<bool> ("valid_receive"));
		std::string difficulty_text (response.get<std::string> ("difficulty"));
		uint64_t difficulty{ 0 };
		ASSERT_FALSE (nano::from_string_hex (difficulty_text, difficulty));
		double multiplier (response.get<double> ("multiplier"));
		ASSERT_NEAR (multiplier, nano::difficulty::to_multiplier (difficulty, node->default_difficulty (nano::work_version::work_1)), 1e-6);
	};
}

TEST (rpc, successors)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	auto genesis (node->latest (nano::dev::genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, 1));
	ASSERT_NE (nullptr, block);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "successors");
	request.put ("block", genesis.to_string ());
	request.put ("count", std::to_string (std::numeric_limits<uint64_t>::max ()));
	auto response (wait_response (system, rpc, request));
	auto & blocks_node (response.get_child ("blocks"));
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
	auto response2 (wait_response (system, rpc, request, 10s));
	ASSERT_EQ (response, response2);
}

TEST (rpc, bootstrap_any)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	nano::system system1 (1);
	auto latest (system1.nodes[0]->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, nano::dev::genesis->account (), 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system1.nodes[0]->work_generate_blocking (latest));
	{
		auto transaction (system1.nodes[0]->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, system1.nodes[0]->ledger.process (transaction, send).code);
	}
	auto [rpc, rpc_ctx] = add_rpc (system0, node);
	boost::property_tree::ptree request;
	request.put ("action", "bootstrap_any");
	auto response (wait_response (system0, rpc, request));
	std::string success (response.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
}

TEST (rpc, republish)
{
	nano::system system;
	nano::keypair key;
	auto node1 = add_ipc_enabled_node (system);
	system.add_node ();
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	node1->process (send);
	nano::open_block open (send.hash (), key.pub, key.pub, key.prv, key.pub, *node1->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1->process (open).code);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "republish");
	request.put ("hash", send.hash ().to_string ());
	auto response (wait_response (system, rpc, request));
	ASSERT_TIMELY (10s, system.nodes[1]->balance (nano::dev::genesis_key.pub) != nano::dev::genesis_amount);
	auto & blocks_node (response.get_child ("blocks"));
	std::vector<nano::block_hash> blocks;
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, blocks.size ());
	ASSERT_EQ (send.hash (), blocks[0]);

	request.put ("hash", nano::dev::genesis->hash ().to_string ());
	request.put ("count", 1);
	auto response1 (wait_response (system, rpc, request));
	blocks_node = response1.get_child ("blocks");
	blocks.clear ();
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, blocks.size ());
	ASSERT_EQ (nano::dev::genesis->hash (), blocks[0]);

	request.put ("hash", open.hash ().to_string ());
	request.put ("sources", 2);
	auto response2 (wait_response (system, rpc, request));
	blocks_node = response2.get_child ("blocks");
	blocks.clear ();
	for (auto i (blocks_node.begin ()), n (blocks_node.end ()); i != n; ++i)
	{
		blocks.push_back (nano::block_hash (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (3, blocks.size ());
	ASSERT_EQ (nano::dev::genesis->hash (), blocks[0]);
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
	auto [rpc, rpc_ctx] = add_rpc (system0, node);
	boost::property_tree::ptree request;
	request.put ("action", "deterministic_key");
	request.put ("seed", seed.to_string ());
	request.put ("index", "0");
	auto response0 (wait_response (system0, rpc, request));
	std::string validate_text (response0.get<std::string> ("account"));
	ASSERT_EQ (account0.to_account (), validate_text);
	request.put ("index", "2");
	auto response1 (wait_response (system0, rpc, request));
	validate_text = response1.get<std::string> ("account");
	ASSERT_NE (account1.to_account (), validate_text);
	ASSERT_EQ (account2.to_account (), validate_text);
}

TEST (rpc, accounts_balances)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "accounts_balances");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", nano::dev::genesis_key.pub.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", peers_l);
	auto response (wait_response (system, rpc, request));
	for (auto & balances : response.get_child ("balances"))
	{
		std::string account_text (balances.first);
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), account_text);
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "accounts_frontiers");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", nano::dev::genesis_key.pub.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", peers_l);
	auto response (wait_response (system, rpc, request));
	for (auto & frontiers : response.get_child ("frontiers"))
	{
		std::string account_text (frontiers.first);
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), account_text);
		std::string frontier_text (frontiers.second.get<std::string> (""));
		ASSERT_EQ (node->latest (nano::dev::genesis->account ()), nano::block_hash{ frontier_text });
	}
}

TEST (rpc, accounts_pending)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key1;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto block1 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key1.pub, 100));
	node->scheduler.flush ();
	ASSERT_TIMELY (5s, !node->active.active (*block1));
	ASSERT_TIMELY (5s, node->ledger.cache.cemented_count == 2 && node->confirmation_height_processor.current ().is_zero () && node->confirmation_height_processor.awaiting_processing_size () == 0);

	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "accounts_pending");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", key1.pub.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", peers_l);
	request.put ("count", "100");
	{
		auto response (wait_response (system, rpc, request));
		for (auto & blocks : response.get_child ("blocks"))
		{
			std::string account_text (blocks.first);
			ASSERT_EQ (key1.pub.to_account (), account_text);
			nano::block_hash hash1 (blocks.second.begin ()->second.get<std::string> (""));
			ASSERT_EQ (block1->hash (), hash1);
		}
	}
	request.put ("sorting", "true"); // Sorting test
	{
		auto response (wait_response (system, rpc, request));
		for (auto & blocks : response.get_child ("blocks"))
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
		auto response (wait_response (system, rpc, request));
		std::unordered_map<nano::block_hash, nano::uint128_union> blocks;
		for (auto & pending : response.get_child ("blocks"))
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
		auto response (wait_response (system, rpc, request));
		std::unordered_map<nano::block_hash, nano::uint128_union> amounts;
		std::unordered_map<nano::block_hash, nano::account> sources;
		for (auto & pending : response.get_child ("blocks"))
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
		ASSERT_EQ (sources[block1->hash ()], nano::dev::genesis_key.pub);
	}

	check_block_response_count (system, rpc, request, 1);
	rpc_ctx->io_scope->reset ();
	reset_confirmation_height (system.nodes.front ()->store, block1->account ());
	rpc_ctx->io_scope->renew ();
	check_block_response_count (system, rpc, request, 0);
	request.put ("include_only_confirmed", "false");
	rpc_ctx->io_scope->renew ();
	check_block_response_count (system, rpc, request, 1);
}

TEST (rpc, blocks)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "blocks");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", node->latest (nano::dev::genesis->account ()).to_string ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("hashes", peers_l);
	auto response (wait_response (system, rpc, request));
	for (auto & blocks : response.get_child ("blocks"))
	{
		std::string hash_text (blocks.first);
		ASSERT_EQ (node->latest (nano::dev::genesis->account ()).to_string (), hash_text);
		std::string blocks_text (blocks.second.get<std::string> (""));
		ASSERT_FALSE (blocks_text.empty ());
	}
}

TEST (rpc, wallet_info)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.enable_voting = true;
	auto node = add_ipc_enabled_node (system, node_config);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (key.prv);

	// at first, 1 block and 1 confirmed -- the genesis
	ASSERT_EQ (1, node->ledger.cache.block_count);
	ASSERT_EQ (1, node->ledger.cache.cemented_count);

	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	// after the send, expect 2 blocks immediately, then 2 confirmed in a timely manner,
	// and finally 3 blocks and 3 confirmed after the wallet generates the receive block for this send
	ASSERT_EQ (2, node->ledger.cache.block_count);
	ASSERT_TIMELY (5s, 2 == node->ledger.cache.cemented_count);
	ASSERT_TIMELY (5s, 3 == node->ledger.cache.block_count && 3 == node->ledger.cache.cemented_count);

	// do another send to be able to expect some "pending" down below
	auto send2 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, 1));
	ASSERT_EQ (4, node->ledger.cache.block_count);

	nano::account account (system.wallet (0)->deterministic_insert ());
	{
		auto transaction (node->wallets.tx_begin_write ());
		system.wallet (0)->store.erase (transaction, account);
	}
	account = system.wallet (0)->deterministic_insert ();
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_info");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	auto response (wait_response (system, rpc, request));
	std::string balance_text (response.get<std::string> ("balance"));
	ASSERT_EQ ("340282366920938463463374607431768211454", balance_text);
	std::string pending_text (response.get<std::string> ("pending"));
	ASSERT_EQ ("1", pending_text);
	std::string count_text (response.get<std::string> ("accounts_count"));
	ASSERT_EQ ("3", count_text);
	std::string block_count_text (response.get<std::string> ("accounts_block_count"));
	ASSERT_EQ ("4", block_count_text);
	std::string cemented_block_count_text (response.get<std::string> ("accounts_cemented_block_count"));
	ASSERT_EQ ("3", cemented_block_count_text);
	std::string adhoc_count (response.get<std::string> ("adhoc_count"));
	ASSERT_EQ ("2", adhoc_count);
	std::string deterministic_count (response.get<std::string> ("deterministic_count"));
	ASSERT_EQ ("1", deterministic_count);
	std::string index_text (response.get<std::string> ("deterministic_index"));
	ASSERT_EQ ("2", index_text);
}

TEST (rpc, wallet_balances)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	system0.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system0, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_balances");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	auto response (wait_response (system0, rpc, request));
	for (auto & balances : response.get_child ("balances"))
	{
		std::string account_text (balances.first);
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), account_text);
		std::string balance_text (balances.second.get<std::string> ("balance"));
		ASSERT_EQ ("340282366920938463463374607431768211455", balance_text);
		std::string pending_text (balances.second.get<std::string> ("pending"));
		ASSERT_EQ ("0", pending_text);
	}
	nano::keypair key;
	rpc_ctx->io_scope->reset ();
	system0.wallet (0)->insert_adhoc (key.prv);
	auto send (system0.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, 1));
	rpc_ctx->io_scope->renew ();
	request.put ("threshold", "2");
	auto response1 (wait_response (system0, rpc, request));
	for (auto & balances : response1.get_child ("balances"))
	{
		std::string account_text (balances.first);
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), account_text);
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto hash0 (node->latest (nano::dev::genesis->account ()));
	auto block1 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key1.pub, 100));
	node->scheduler.flush ();
	ASSERT_TIMELY (5s, !node->active.active (*block1));
	ASSERT_TIMELY (5s, node->ledger.cache.cemented_count == 2 && node->confirmation_height_processor.current ().is_zero () && node->confirmation_height_processor.awaiting_processing_size () == 0);

	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;

	auto pending_exists = [&system, &request, rpc = rpc] (const char * exists_a) {
		auto response0 (wait_response (system, rpc, request));
		std::string exists_text (response0.get<std::string> ("exists"));
		ASSERT_EQ (exists_a, exists_text);
	};

	request.put ("action", "pending_exists");
	request.put ("hash", hash0.to_string ());
	pending_exists ("0");

	node->store.pending.exists (node->store.tx_begin_read (), nano::pending_key (nano::dev::genesis_key.pub, block1->hash ()));
	request.put ("hash", block1->hash ().to_string ());
	pending_exists ("1");

	pending_exists ("1");
	rpc_ctx->io_scope->reset ();
	reset_confirmation_height (node->store, block1->account ());
	rpc_ctx->io_scope->renew ();
	pending_exists ("0");
	request.put ("include_only_confirmed", "false");
	rpc_ctx->io_scope->renew ();
	pending_exists ("1");
}

TEST (rpc, wallet_pending)
{
	nano::system system0;
	auto node = add_ipc_enabled_node (system0);
	nano::keypair key1;
	system0.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system0.wallet (0)->insert_adhoc (key1.prv);
	auto iterations (0);
	auto block1 (system0.wallet (0)->send_action (nano::dev::genesis_key.pub, key1.pub, 100));
	node->scheduler.flush ();
	while (node->active.active (*block1) || node->ledger.cache.cemented_count < 2 || !node->confirmation_height_processor.current ().is_zero () || node->confirmation_height_processor.awaiting_processing_size () != 0)
	{
		system0.poll ();
		++iterations;
		ASSERT_LT (iterations, 200);
	}

	auto [rpc, rpc_ctx] = add_rpc (system0, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_pending");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("count", "100");
	auto response (wait_response (system0, rpc, request));
	ASSERT_EQ (1, response.get_child ("blocks").size ());
	for (auto & pending : response.get_child ("blocks"))
	{
		std::string account_text (pending.first);
		ASSERT_EQ (key1.pub.to_account (), account_text);
		nano::block_hash hash1 (pending.second.begin ()->second.get<std::string> (""));
		ASSERT_EQ (block1->hash (), hash1);
	}
	request.put ("threshold", "100"); // Threshold test
	auto response0 (wait_response (system0, rpc, request));
	std::unordered_map<nano::block_hash, nano::uint128_union> blocks;
	ASSERT_EQ (1, response0.get_child ("blocks").size ());
	for (auto & pending : response0.get_child ("blocks"))
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
	auto response1 (wait_response (system0, rpc, request));
	auto & pending1 (response1.get_child ("blocks"));
	ASSERT_EQ (0, pending1.size ());
	request.put ("threshold", "0");
	request.put ("source", "true");
	request.put ("min_version", "true");
	auto response2 (wait_response (system0, rpc, request));
	std::unordered_map<nano::block_hash, nano::uint128_union> amounts;
	std::unordered_map<nano::block_hash, nano::account> sources;
	ASSERT_EQ (1, response2.get_child ("blocks").size ());
	for (auto & pending : response2.get_child ("blocks"))
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
	ASSERT_EQ (sources[block1->hash ()], nano::dev::genesis_key.pub);

	check_block_response_count (system0, rpc, request, 1);
	rpc_ctx->io_scope->reset ();
	reset_confirmation_height (system0.nodes.front ()->store, block1->account ());
	rpc_ctx->io_scope->renew ();
	check_block_response_count (system0, rpc, request, 0);
	request.put ("include_only_confirmed", "false");
	rpc_ctx->io_scope->renew ();
	check_block_response_count (system0, rpc, request, 1);
}

TEST (rpc, receive_minimum)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "receive_minimum");
	auto response (wait_response (system, rpc, request));
	std::string amount (response.get<std::string> ("amount"));
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), amount);
}

TEST (rpc, receive_minimum_set)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "receive_minimum_set");
	request.put ("amount", "100");
	ASSERT_NE (node->config.receive_minimum.to_string_dec (), "100");
	auto response (wait_response (system, rpc, request));
	std::string success (response.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), "100");
}

TEST (rpc, work_get)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->work_cache_blocking (nano::dev::genesis_key.pub, node->latest (nano::dev::genesis_key.pub));
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "work_get");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	std::string work_text (response.get<std::string> ("work"));
	uint64_t work (1);
	auto transaction (node->wallets.tx_begin_read ());
	node->wallets.items.begin ()->second->store.work_get (transaction, nano::dev::genesis->account (), work);
	ASSERT_EQ (nano::to_string_hex (work), work_text);
}

TEST (rpc, wallet_work_get)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->work_cache_blocking (nano::dev::genesis_key.pub, node->latest (nano::dev::genesis_key.pub));
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_work_get");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	auto response (wait_response (system, rpc, request));
	auto transaction (node->wallets.tx_begin_read ());
	for (auto & works : response.get_child ("works"))
	{
		std::string account_text (works.first);
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), account_text);
		std::string work_text (works.second.get<std::string> (""));
		uint64_t work (1);
		node->wallets.items.begin ()->second->store.work_get (transaction, nano::dev::genesis->account (), work);
		ASSERT_EQ (nano::to_string_hex (work), work_text);
	}
}

TEST (rpc, work_set)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	uint64_t work0 (100);
	boost::property_tree::ptree request;
	request.put ("action", "work_set");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	request.put ("work", nano::to_string_hex (work0));
	auto response (wait_response (system, rpc, request));
	std::string success (response.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
	uint64_t work1 (1);
	auto transaction (node->wallets.tx_begin_read ());
	node->wallets.items.begin ()->second->store.work_get (transaction, nano::dev::genesis->account (), work1);
	ASSERT_EQ (work1, work0);
}

TEST (rpc, search_pending_all)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto latest (node->latest (nano::dev::genesis_key.pub));
	nano::send_block block (latest, nano::dev::genesis_key.pub, nano::dev::genesis_amount - node->config.receive_minimum.number (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node->work_generate_blocking (latest));
	{
		auto transaction (node->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, block).code);
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "search_pending_all");
	auto response (wait_response (system, rpc, request));
	ASSERT_TIMELY (10s, node->balance (nano::dev::genesis_key.pub) == nano::dev::genesis_amount);
}

TEST (rpc, wallet_republish)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	while (key.pub < nano::dev::genesis_key.pub)
	{
		nano::keypair key1;
		key.pub = key1.pub;
		key.prv = key1.prv;
	}
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	node1->process (send);
	nano::open_block open (send.hash (), key.pub, key.pub, key.prv, key.pub, *node1->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1->process (open).code);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_republish");
	request.put ("wallet", node1->wallets.items.begin ()->first.to_string ());
	request.put ("count", 1);
	auto response (wait_response (system, rpc, request));
	auto & blocks_node (response.get_child ("blocks"));
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
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	node1->process (send);
	nano::open_block open (send.hash (), nano::dev::genesis_key.pub, key.pub, key.prv, key.pub, *node1->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1->process (open).code);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "delegators");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	auto & delegators_node (response.get_child ("delegators"));
	boost::property_tree::ptree delegators;
	for (auto i (delegators_node.begin ()), n (delegators_node.end ()); i != n; ++i)
	{
		delegators.put ((i->first), (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (2, delegators.size ());
	ASSERT_EQ ("100", delegators.get<std::string> (nano::dev::genesis_key.pub.to_account ()));
	ASSERT_EQ ("340282366920938463463374607431768211355", delegators.get<std::string> (key.pub.to_account ()));
}

TEST (rpc, delegators_parameters)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	node1->process (send);
	nano::open_block open (send.hash (), nano::dev::genesis_key.pub, key.pub, key.prv, key.pub, *node1->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1->process (open).code);

	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	// Test with "count" = 2
	boost::property_tree::ptree request;
	request.put ("action", "delegators");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	request.put ("count", 2);
	auto response (wait_response (system, rpc, request));
	auto & delegators_node (response.get_child ("delegators"));
	boost::property_tree::ptree delegators;
	for (auto i (delegators_node.begin ()), n (delegators_node.end ()); i != n; ++i)
	{
		delegators.put ((i->first), (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (2, delegators.size ());
	ASSERT_EQ ("100", delegators.get<std::string> (nano::dev::genesis_key.pub.to_account ()));
	ASSERT_EQ ("340282366920938463463374607431768211355", delegators.get<std::string> (key.pub.to_account ()));

	// Test with "count" = 1
	request.put ("count", 1);
	auto response2 (wait_response (system, rpc, request));
	auto & delegators_node2 (response2.get_child ("delegators"));
	boost::property_tree::ptree delegators2;
	for (auto i (delegators_node2.begin ()), n (delegators_node2.end ()); i != n; ++i)
	{
		delegators2.put ((i->first), (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, delegators2.size ());
	// What is first in ledger by public key?
	if (nano::dev::genesis_key.pub.number () < key.pub.number ())
	{
		ASSERT_EQ ("100", delegators2.get<std::string> (nano::dev::genesis_key.pub.to_account ()));
	}
	else
	{
		ASSERT_EQ ("340282366920938463463374607431768211355", delegators2.get<std::string> (key.pub.to_account ()));
	}

	// Test with "threshold"
	request.put ("count", 1024);
	request.put ("threshold", 101); // higher than remaining genesis balance
	auto response3 (wait_response (system, rpc, request));
	auto & delegators_node3 (response3.get_child ("delegators"));
	boost::property_tree::ptree delegators3;
	for (auto i (delegators_node3.begin ()), n (delegators_node3.end ()); i != n; ++i)
	{
		delegators3.put ((i->first), (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, delegators3.size ());
	ASSERT_EQ ("340282366920938463463374607431768211355", delegators3.get<std::string> (key.pub.to_account ()));

	// Test with "start" before last account
	request.put ("threshold", 0);
	auto last_account (key.pub);
	if (nano::dev::genesis_key.pub.number () > key.pub.number ())
	{
		last_account = nano::dev::genesis_key.pub;
	}
	request.put ("start", nano::account (last_account.number () - 1).to_account ());

	auto response4 (wait_response (system, rpc, request));
	auto & delegators_node4 (response4.get_child ("delegators"));
	boost::property_tree::ptree delegators4;
	for (auto i (delegators_node4.begin ()), n (delegators_node4.end ()); i != n; ++i)
	{
		delegators4.put ((i->first), (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (1, delegators4.size ());
	boost::optional<std::string> balance (delegators4.get_optional<std::string> (last_account.to_account ()));
	ASSERT_TRUE (balance.is_initialized ());

	// Test with "start" equal to last account
	request.put ("start", last_account.to_account ());
	auto response5 (wait_response (system, rpc, request));
	auto & delegators_node5 (response5.get_child ("delegators"));
	boost::property_tree::ptree delegators5;
	for (auto i (delegators_node5.begin ()), n (delegators_node5.end ()); i != n; ++i)
	{
		delegators5.put ((i->first), (i->second.get<std::string> ("")));
	}
	ASSERT_EQ (0, delegators5.size ());
}

TEST (rpc, delegators_count)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	node1->process (send);
	nano::open_block open (send.hash (), nano::dev::genesis_key.pub, key.pub, key.prv, key.pub, *node1->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1->process (open).code);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "delegators_count");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	std::string count (response.get<std::string> ("count"));
	ASSERT_EQ ("2", count);
}

TEST (rpc, account_info)
{
	nano::system system;
	nano::keypair key;

	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);

	boost::property_tree::ptree request;
	request.put ("action", "account_info");
	request.put ("account", nano::account ().to_account ());

	// Test for a non existing account
	{
		auto response (wait_response (system, rpc, request));

		auto error (response.get_optional<std::string> ("error"));
		ASSERT_TRUE (error.is_initialized ());
		ASSERT_EQ (error.get (), std::error_code (nano::error_common::account_not_found).message ());
	}

	rpc_ctx->io_scope->reset ();
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	node1->process (send);
	auto time (nano::seconds_since_epoch ());
	{
		auto transaction = node1->store.tx_begin_write ();
		node1->store.confirmation_height.put (transaction, nano::dev::genesis_key.pub, { 1, nano::dev::genesis->hash () });
	}
	rpc_ctx->io_scope->renew ();

	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	{
		auto response (wait_response (system, rpc, request));
		std::string frontier (response.get<std::string> ("frontier"));
		ASSERT_EQ (send.hash ().to_string (), frontier);
		std::string open_block (response.get<std::string> ("open_block"));
		ASSERT_EQ (nano::dev::genesis->hash ().to_string (), open_block);
		std::string representative_block (response.get<std::string> ("representative_block"));
		ASSERT_EQ (nano::dev::genesis->hash ().to_string (), representative_block);
		std::string balance (response.get<std::string> ("balance"));
		ASSERT_EQ ("100", balance);
		std::string modified_timestamp (response.get<std::string> ("modified_timestamp"));
		ASSERT_LT (std::abs ((long)time - stol (modified_timestamp)), 5);
		std::string block_count (response.get<std::string> ("block_count"));
		ASSERT_EQ ("2", block_count);
		std::string confirmation_height (response.get<std::string> ("confirmation_height"));
		ASSERT_EQ ("1", confirmation_height);
		std::string confirmation_height_frontier (response.get<std::string> ("confirmation_height_frontier"));
		ASSERT_EQ (nano::dev::genesis->hash ().to_string (), confirmation_height_frontier);
		ASSERT_EQ (0, response.get<uint8_t> ("account_version"));
		boost::optional<std::string> weight (response.get_optional<std::string> ("weight"));
		ASSERT_FALSE (weight.is_initialized ());
		boost::optional<std::string> pending (response.get_optional<std::string> ("pending"));
		ASSERT_FALSE (pending.is_initialized ());
		boost::optional<std::string> representative (response.get_optional<std::string> ("representative"));
		ASSERT_FALSE (representative.is_initialized ());
	}

	// Test for optional values
	request.put ("weight", "true");
	request.put ("pending", "1");
	request.put ("representative", "1");
	{
		auto response (wait_response (system, rpc, request));
		std::string weight2 (response.get<std::string> ("weight"));
		ASSERT_EQ ("100", weight2);
		std::string pending2 (response.get<std::string> ("pending"));
		ASSERT_EQ ("0", pending2);
		std::string representative2 (response.get<std::string> ("representative"));
		ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), representative2);
	}

	// Test for confirmed only blocks
	rpc_ctx->io_scope->reset ();
	nano::keypair key1;
	{
		latest = node1->latest (nano::dev::genesis_key.pub);
		nano::send_block send1 (latest, key1.pub, 50, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
		node1->process (send1);
		nano::send_block send2 (send1.hash (), key1.pub, 25, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (send1.hash ()));
		node1->process (send2);

		nano::state_block state_change (nano::dev::genesis_key.pub, send2.hash (), key1.pub, 25, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (send2.hash ()));
		node1->process (state_change);

		nano::open_block open (send1.hash (), nano::dev::genesis_key.pub, key1.pub, key1.prv, key1.pub, *node1->work_generate_blocking (key1.pub));
		node1->process (open);
	}

	rpc_ctx->io_scope->renew ();

	{
		auto response (wait_response (system, rpc, request));
		std::string balance (response.get<std::string> ("balance"));
		ASSERT_EQ ("25", balance);
	}

	request.put ("include_confirmed", true);
	{
		auto response (wait_response (system, rpc, request));
		auto balance (response.get<std::string> ("balance"));
		ASSERT_EQ ("25", balance);
		auto confirmed_balance (response.get<std::string> ("confirmed_balance"));
		ASSERT_EQ ("340282366920938463463374607431768211455", confirmed_balance);

		auto representative (response.get<std::string> ("representative"));
		ASSERT_EQ (representative, key1.pub.to_account ());

		auto confirmed_representative (response.get<std::string> ("confirmed_representative"));
		ASSERT_EQ (confirmed_representative, nano::dev::genesis_key.pub.to_account ());

		auto confirmed_frontier (response.get<std::string> ("confirmed_frontier"));
		ASSERT_EQ (nano::dev::genesis->hash ().to_string (), confirmed_frontier);

		auto confirmed_height (response.get<uint64_t> ("confirmed_height"));
		ASSERT_EQ (1, confirmed_height);
	}

	request.put ("account", key1.pub.to_account ());
	{
		auto response (wait_response (system, rpc, request));
		std::string pending (response.get<std::string> ("pending"));
		ASSERT_EQ ("25", pending);
		std::string confirmed_pending (response.get<std::string> ("confirmed_pending"));
		ASSERT_EQ ("0", confirmed_pending);
	}

	request.put ("include_confirmed", false);
	{
		auto response (wait_response (system, rpc, request));
		std::string pending (response.get<std::string> ("pending"));
		ASSERT_EQ ("25", pending);

		// These fields shouldn't exist
		auto confirmed_balance (response.get_optional<std::string> ("confirmed_balance"));
		ASSERT_FALSE (confirmed_balance.is_initialized ());

		auto confirmed_pending (response.get_optional<std::string> ("confirmed_pending"));
		ASSERT_FALSE (confirmed_pending.is_initialized ());

		auto confirmed_representative (response.get_optional<std::string> ("confirmed_representative"));
		ASSERT_FALSE (confirmed_representative.is_initialized ());

		auto confirmed_frontier (response.get_optional<std::string> ("confirmed_frontier"));
		ASSERT_FALSE (confirmed_frontier.is_initialized ());

		auto confirmed_height (response.get_optional<uint64_t> ("confirmed_height"));
		ASSERT_FALSE (confirmed_height.is_initialized ());
	}
}

/** Make sure we can use json block literals instead of string as input */
TEST (rpc, json_block_input)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (key.prv);
	nano::state_block send (nano::dev::genesis->account (), node1->latest (nano::dev::genesis_key.pub), nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, 0);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "sign");
	request.put ("json_block", "true");
	std::string wallet;
	node1->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("account", key.pub.to_account ());
	boost::property_tree::ptree json;
	send.serialize_json (json);
	request.add_child ("block", json);
	auto response (wait_response (system, rpc, request, 10s));

	bool json_error{ false };
	nano::state_block block (json_error, response.get_child ("block"));
	ASSERT_FALSE (json_error);

	ASSERT_FALSE (nano::validate_message (key.pub, send.hash (), block.block_signature ()));
	ASSERT_NE (block.block_signature (), send.block_signature ());
	ASSERT_EQ (block.hash (), send.hash ());
}

/** Make sure we can receive json block literals instead of string as output */
TEST (rpc, json_block_output)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	node1->process (send);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "block_info");
	request.put ("json_block", "true");
	request.put ("hash", send.hash ().to_string ());
	auto response (wait_response (system, rpc, request));

	// Make sure contents contains a valid JSON subtree instread of stringified json
	bool json_error{ false };
	nano::send_block send_from_json (json_error, response.get_child ("contents"));
	ASSERT_FALSE (json_error);
}

TEST (rpc, blocks_info)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	auto check_blocks = [node] (boost::property_tree::ptree & response) {
		for (auto & blocks : response.get_child ("blocks"))
		{
			std::string hash_text (blocks.first);
			ASSERT_EQ (node->latest (nano::dev::genesis->account ()).to_string (), hash_text);
			std::string account_text (blocks.second.get<std::string> ("block_account"));
			ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), account_text);
			std::string amount_text (blocks.second.get<std::string> ("amount"));
			ASSERT_EQ (nano::dev::genesis_amount.convert_to<std::string> (), amount_text);
			std::string blocks_text (blocks.second.get<std::string> ("contents"));
			ASSERT_FALSE (blocks_text.empty ());
			boost::optional<std::string> pending (blocks.second.get_optional<std::string> ("pending"));
			ASSERT_FALSE (pending.is_initialized ());
			boost::optional<std::string> source (blocks.second.get_optional<std::string> ("source_account"));
			ASSERT_FALSE (source.is_initialized ());
			std::string balance_text (blocks.second.get<std::string> ("balance"));
			ASSERT_EQ (nano::dev::genesis_amount.convert_to<std::string> (), balance_text);
			ASSERT_TRUE (blocks.second.get<bool> ("confirmed")); // Genesis block is confirmed by default
			std::string successor_text (blocks.second.get<std::string> ("successor"));
			ASSERT_EQ (nano::block_hash (0).to_string (), successor_text); // Genesis block doesn't have successor yet
		}
	};
	boost::property_tree::ptree request;
	request.put ("action", "blocks_info");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree hashes;
	entry.put ("", node->latest (nano::dev::genesis->account ()).to_string ());
	hashes.push_back (std::make_pair ("", entry));
	request.add_child ("hashes", hashes);
	{
		auto response (wait_response (system, rpc, request));
		check_blocks (response);
	}
	std::string random_hash = nano::block_hash ().to_string ();
	entry.put ("", random_hash);
	hashes.push_back (std::make_pair ("", entry));
	request.erase ("hashes");
	request.add_child ("hashes", hashes);
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response.get<std::string> ("error"));
	}
	request.put ("include_not_found", "true");
	{
		auto response (wait_response (system, rpc, request));
		check_blocks (response);
		auto & blocks_not_found (response.get_child ("blocks_not_found"));
		ASSERT_EQ (1, blocks_not_found.size ());
		ASSERT_EQ (random_hash, blocks_not_found.begin ()->second.get<std::string> (""));
	}
	request.put ("source", "true");
	request.put ("pending", "1");
	{
		auto response (wait_response (system, rpc, request));
		for (auto & blocks : response.get_child ("blocks"))
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
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send);
	auto receive (system.wallet (0)->receive_action (send->hash (), key.pub, nano::Gxrb_ratio, send->link ().as_account ()));
	ASSERT_NE (nullptr, receive);
	auto change (system.wallet (0)->change_action (nano::dev::genesis_key.pub, key.pub));
	ASSERT_NE (nullptr, change);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
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
	auto response (wait_response (system, rpc, request));
	auto & blocks (response.get_child ("blocks"));
	ASSERT_EQ (3, blocks.size ());
	auto send_subtype (blocks.get_child (send->hash ().to_string ()).get<std::string> ("subtype"));
	ASSERT_EQ (send_subtype, "send");
	auto receive_subtype (blocks.get_child (receive->hash ().to_string ()).get<std::string> ("subtype"));
	ASSERT_EQ (receive_subtype, "receive");
	auto change_subtype (blocks.get_child (change->hash ().to_string ()).get<std::string> ("subtype"));
	ASSERT_EQ (change_subtype, "change");
	// Successor fields
	auto send_successor (blocks.get_child (send->hash ().to_string ()).get<std::string> ("successor"));
	ASSERT_EQ (send_successor, receive->hash ().to_string ());
	auto receive_successor (blocks.get_child (receive->hash ().to_string ()).get<std::string> ("successor"));
	ASSERT_EQ (receive_successor, change->hash ().to_string ());
	auto change_successor (blocks.get_child (change->hash ().to_string ()).get<std::string> ("successor"));
	ASSERT_EQ (change_successor, nano::block_hash (0).to_string ()); // Change block doesn't have successor yet
}

TEST (rpc, block_info_successor)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	node1->process (send);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "block_info");
	request.put ("hash", latest.to_string ());
	auto response (wait_response (system, rpc, request));

	// Make sure send block is successor of genesis
	std::string successor_text (response.get<std::string> ("successor"));
	ASSERT_EQ (successor_text, send.hash ().to_string ());
	std::string account_text (response.get<std::string> ("block_account"));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), account_text);
	std::string amount_text (response.get<std::string> ("amount"));
	ASSERT_EQ (nano::dev::genesis_amount.convert_to<std::string> (), amount_text);
}

TEST (rpc, block_info_pruning)
{
	nano::system system;
	nano::node_config node_config0 (nano::get_available_port (), system.logging);
	node_config0.receive_minimum = nano::dev::genesis_amount; // Prevent auto-receive & receive1 block conflicts
	auto & node0 = *system.add_node (node_config0);
	nano::node_config node_config1 (nano::get_available_port (), system.logging);
	node_config1.enable_voting = false; // Remove after allowing pruned voting
	nano::node_flags node_flags;
	node_flags.enable_pruning = true;
	auto node1 = add_ipc_enabled_node (system, node_config1, node_flags);
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	auto send1 (std::make_shared<nano::send_block> (latest, nano::dev::genesis_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest)));
	node1->process_active (send1);
	auto receive1 (std::make_shared<nano::receive_block> (send1->hash (), send1->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (send1->hash ())));
	node1->process_active (receive1);
	node1->block_processor.flush ();
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_TIMELY (5s, node1->ledger.cache.cemented_count == 3 && node1->confirmation_height_processor.current ().is_zero () && node1->confirmation_height_processor.awaiting_processing_size () == 0);
	// Pruning action
	{
		auto transaction (node1->store.tx_begin_write ());
		ASSERT_EQ (1, node1->ledger.pruning_action (transaction, send1->hash (), 1));
		ASSERT_TRUE (node1->store.block.exists (transaction, receive1->hash ()));
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	// Pruned block
	boost::property_tree::ptree request;
	request.put ("action", "block_info");
	request.put ("hash", send1->hash ().to_string ());
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response.get<std::string> ("error"));
	// Existing block with previous pruned
	boost::property_tree::ptree request2;
	request2.put ("action", "block_info");
	request2.put ("json_block", "true");
	request2.put ("hash", receive1->hash ().to_string ());
	auto response2 (wait_response (system, rpc, request2));
	std::string account_text (response2.get<std::string> ("block_account"));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), account_text);
	boost::optional<std::string> amount (response2.get_optional<std::string> ("amount"));
	ASSERT_FALSE (amount.is_initialized ()); // Cannot calculate amount
	bool json_error{ false };
	nano::receive_block receive_from_json (json_error, response2.get_child ("contents"));
	ASSERT_FALSE (json_error);
	ASSERT_EQ (receive1->full_hash (), receive_from_json.full_hash ());
	std::string balance_text (response2.get<std::string> ("balance"));
	ASSERT_EQ (nano::dev::genesis_amount.convert_to<std::string> (), balance_text);
	ASSERT_TRUE (response2.get<bool> ("confirmed"));
	std::string successor_text (response2.get<std::string> ("successor"));
	ASSERT_EQ (successor_text, nano::block_hash (0).to_string ()); // receive1 block doesn't have successor yet
}

TEST (rpc, pruned_exists)
{
	nano::system system;
	nano::node_config node_config0 (nano::get_available_port (), system.logging);
	node_config0.receive_minimum = nano::dev::genesis_amount; // Prevent auto-receive & receive1 block conflicts
	auto & node0 = *system.add_node (node_config0);
	nano::node_config node_config1 (nano::get_available_port (), system.logging);
	node_config1.enable_voting = false; // Remove after allowing pruned voting
	nano::node_flags node_flags;
	node_flags.enable_pruning = true;
	auto node1 = add_ipc_enabled_node (system, node_config1, node_flags);
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	auto send1 (std::make_shared<nano::send_block> (latest, nano::dev::genesis_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest)));
	node1->process_active (send1);
	auto receive1 (std::make_shared<nano::receive_block> (send1->hash (), send1->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (send1->hash ())));
	node1->process_active (receive1);
	node1->block_processor.flush ();
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_TIMELY (5s, node1->ledger.cache.cemented_count == 3 && node1->confirmation_height_processor.current ().is_zero () && node1->confirmation_height_processor.awaiting_processing_size () == 0);
	// Pruning action
	{
		auto transaction (node1->store.tx_begin_write ());
		ASSERT_EQ (1, node1->ledger.pruning_action (transaction, send1->hash (), 1));
		ASSERT_TRUE (node1->store.block.exists (transaction, receive1->hash ()));
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	// Pruned block
	boost::property_tree::ptree request;
	request.put ("action", "pruned_exists");
	request.put ("hash", send1->hash ().to_string ());
	auto response (wait_response (system, rpc, request));
	ASSERT_TRUE (response.get<bool> ("exists"));
	// Existing block with previous pruned
	boost::property_tree::ptree request2;
	request2.put ("action", "pruned_exists");
	request2.put ("hash", receive1->hash ().to_string ());
	auto response2 (wait_response (system, rpc, request2));
	ASSERT_FALSE (response2.get<bool> ("exists"));
}

TEST (rpc, work_peers_all)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "work_peer_add");
	request.put ("address", "::1");
	request.put ("port", "0");
	auto response (wait_response (system, rpc, request));
	std::string success (response.get<std::string> ("success", ""));
	ASSERT_TRUE (success.empty ());
	boost::property_tree::ptree request1;
	request1.put ("action", "work_peers");
	auto response1 (wait_response (system, rpc, request1));
	auto & peers_node (response1.get_child ("work_peers"));
	std::vector<std::string> peers;
	for (auto i (peers_node.begin ()), n (peers_node.end ()); i != n; ++i)
	{
		peers.push_back (i->second.get<std::string> (""));
	}
	ASSERT_EQ (1, peers.size ());
	ASSERT_EQ ("::1:0", peers[0]);
	boost::property_tree::ptree request2;
	request2.put ("action", "work_peers_clear");
	auto response2 (wait_response (system, rpc, request2));
	success = response2.get<std::string> ("success", "");
	ASSERT_TRUE (success.empty ());
	auto response3 (wait_response (system, rpc, request1, 10s));
	peers_node = response3.get_child ("work_peers");
	ASSERT_EQ (0, peers_node.size ());
}

TEST (rpc, ledger)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	auto latest (node->latest (nano::dev::genesis_key.pub));
	auto genesis_balance (nano::dev::genesis_amount);
	auto send_amount (genesis_balance - 100);
	genesis_balance -= send_amount;
	nano::send_block send (latest, key.pub, genesis_balance, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node->work_generate_blocking (latest));
	node->process (send);
	nano::open_block open (send.hash (), nano::dev::genesis_key.pub, key.pub, key.prv, key.pub, *node->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node->process (open).code);
	auto time (nano::seconds_since_epoch ());
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "ledger");
	request.put ("sorting", true);
	request.put ("count", "1");
	{
		auto response (wait_response (system, rpc, request));
		for (auto & account : response.get_child ("accounts"))
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
		auto response (wait_response (system, rpc, request));
		for (auto & account : response.get_child ("accounts"))
		{
			boost::optional<std::string> weight (account.second.get_optional<std::string> ("weight"));
			ASSERT_TRUE (weight.is_initialized ());
			ASSERT_EQ ("0", weight.get ());
			boost::optional<std::string> pending (account.second.get_optional<std::string> ("pending"));
			ASSERT_TRUE (pending.is_initialized ());
			ASSERT_EQ ("0", pending.get ());
			boost::optional<std::string> representative (account.second.get_optional<std::string> ("representative"));
			ASSERT_TRUE (representative.is_initialized ());
			ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), representative.get ());
		}
	}
	// Test threshold
	request.put ("count", 2);
	request.put ("threshold", genesis_balance + 1);
	{
		auto response (wait_response (system, rpc, request));
		auto & accounts (response.get_child ("accounts"));
		ASSERT_EQ (1, accounts.size ());
		auto account (accounts.begin ());
		ASSERT_EQ (key.pub.to_account (), account->first);
		std::string balance_text (account->second.get<std::string> ("balance"));
		ASSERT_EQ (send_amount.convert_to<std::string> (), balance_text);
	}
	auto send2_amount (50);
	genesis_balance -= send2_amount;
	nano::send_block send2 (send.hash (), key.pub, genesis_balance, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node->work_generate_blocking (send.hash ()));
	rpc_ctx->io_scope->reset ();
	node->process (send2);
	rpc_ctx->io_scope->renew ();
	// When asking for pending, pending amount is taken into account for threshold so the account must show up
	request.put ("count", 2);
	request.put ("threshold", (send_amount + send2_amount).convert_to<std::string> ());
	request.put ("pending", true);
	{
		auto response (wait_response (system, rpc, request));
		auto & accounts (response.get_child ("accounts"));
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "accounts_create");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("count", "8");
	auto response (wait_response (system, rpc, request));
	auto & accounts (response.get_child ("accounts"));
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
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	auto send_work = *node1->work_generate_blocking (latest);
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, send_work);
	auto open_work = *node1->work_generate_blocking (key.pub);
	nano::open_block open (send.hash (), nano::dev::genesis_key.pub, key.pub, key.prv, key.pub, open_work);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "send");
	request.put ("wallet", node1->wallets.items.begin ()->first.to_string ());
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	request.put ("previous", latest.to_string ());
	request.put ("amount", "340282366920938463463374607431768211355");
	request.put ("destination", key.pub.to_account ());
	request.put ("work", nano::to_string_hex (send_work));
	auto response (wait_response (system, rpc, request));
	std::string send_hash (response.get<std::string> ("hash"));
	ASSERT_EQ (send.hash ().to_string (), send_hash);
	std::string send_difficulty (response.get<std::string> ("difficulty"));
	ASSERT_EQ (nano::to_string_hex (send.difficulty ()), send_difficulty);
	auto send_text (response.get<std::string> ("block"));
	boost::property_tree::ptree block_l;
	std::stringstream block_stream (send_text);
	boost::property_tree::read_json (block_stream, block_l);
	auto send_block (nano::deserialize_block_json (block_l));
	ASSERT_EQ (send.hash (), send_block->hash ());
	rpc_ctx->io_scope->reset ();
	node1->process (send);
	rpc_ctx->io_scope->renew ();
	boost::property_tree::ptree request1;
	request1.put ("action", "block_create");
	request1.put ("type", "open");
	std::string key_text;
	key.prv.encode_hex (key_text);
	request1.put ("key", key_text);
	request1.put ("representative", nano::dev::genesis_key.pub.to_account ());
	request1.put ("source", send.hash ().to_string ());
	request1.put ("work", nano::to_string_hex (open_work));
	auto response1 (wait_response (system, rpc, request1));
	std::string open_hash (response1.get<std::string> ("hash"));
	ASSERT_EQ (open.hash ().to_string (), open_hash);
	auto open_text (response1.get<std::string> ("block"));
	std::stringstream block_stream1 (open_text);
	boost::property_tree::read_json (block_stream1, block_l);
	auto open_block (nano::deserialize_block_json (block_l));
	ASSERT_EQ (open.hash (), open_block->hash ());
	rpc_ctx->io_scope->reset ();
	ASSERT_EQ (nano::process_result::progress, node1->process (open).code);
	rpc_ctx->io_scope->renew ();
	request1.put ("representative", key.pub.to_account ());
	auto response2 (wait_response (system, rpc, request1));
	std::string open2_hash (response2.get<std::string> ("hash"));
	ASSERT_NE (open.hash ().to_string (), open2_hash); // different blocks with wrong representative
	auto change_work = *node1->work_generate_blocking (open.hash ());
	nano::change_block change (open.hash (), key.pub, key.prv, key.pub, change_work);
	request1.put ("type", "change");
	request1.put ("work", nano::to_string_hex (change_work));
	auto response4 (wait_response (system, rpc, request1));
	std::string change_hash (response4.get<std::string> ("hash"));
	ASSERT_EQ (change.hash ().to_string (), change_hash);
	auto change_text (response4.get<std::string> ("block"));
	std::stringstream block_stream4 (change_text);
	boost::property_tree::read_json (block_stream4, block_l);
	auto change_block (nano::deserialize_block_json (block_l));
	ASSERT_EQ (change.hash (), change_block->hash ());
	rpc_ctx->io_scope->reset ();
	ASSERT_EQ (nano::process_result::progress, node1->process (change).code);
	nano::send_block send2 (send.hash (), key.pub, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (send.hash ()));
	ASSERT_EQ (nano::process_result::progress, node1->process (send2).code);
	rpc_ctx->io_scope->renew ();
	boost::property_tree::ptree request2;
	request2.put ("action", "block_create");
	request2.put ("type", "receive");
	request2.put ("wallet", node1->wallets.items.begin ()->first.to_string ());
	request2.put ("account", key.pub.to_account ());
	request2.put ("source", send2.hash ().to_string ());
	request2.put ("previous", change.hash ().to_string ());
	request2.put ("work", nano::to_string_hex (*node1->work_generate_blocking (change.hash ())));
	auto response5 (wait_response (system, rpc, request2));
	std::string receive_hash (response4.get<std::string> ("hash"));
	auto receive_text (response5.get<std::string> ("block"));
	std::stringstream block_stream5 (change_text);
	boost::property_tree::read_json (block_stream5, block_l);
	auto receive_block (nano::deserialize_block_json (block_l));
	ASSERT_EQ (receive_hash, receive_block->hash ().to_string ());
	node1->process_active (std::move (receive_block));
	latest = node1->latest (key.pub);
	ASSERT_EQ (receive_hash, latest.to_string ());
}

TEST (rpc, block_create_state)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	request.put ("previous", nano::dev::genesis->hash ().to_string ());
	request.put ("representative", nano::dev::genesis_key.pub.to_account ());
	request.put ("balance", (nano::dev::genesis_amount - nano::Gxrb_ratio).convert_to<std::string> ());
	request.put ("link", key.pub.to_account ());
	request.put ("work", nano::to_string_hex (*node->work_generate_blocking (nano::dev::genesis->hash ())));
	auto response (wait_response (system, rpc, request));
	std::string state_hash (response.get<std::string> ("hash"));
	auto state_text (response.get<std::string> ("block"));
	std::stringstream block_stream (state_text);
	boost::property_tree::ptree block_l;
	boost::property_tree::read_json (block_stream, block_l);
	auto state_block (nano::deserialize_block_json (block_l));
	ASSERT_NE (nullptr, state_block);
	ASSERT_EQ (nano::block_type::state, state_block->type ());
	ASSERT_EQ (state_hash, state_block->hash ().to_string ());
	rpc_ctx->io_scope->reset ();
	auto process_result (node->process (*state_block));
	ASSERT_EQ (nano::process_result::progress, process_result.code);
}

TEST (rpc, block_create_state_open)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto send_block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("key", key.prv.to_string ());
	request.put ("account", key.pub.to_account ());
	request.put ("previous", 0);
	request.put ("representative", nano::dev::genesis_key.pub.to_account ());
	request.put ("balance", nano::Gxrb_ratio.convert_to<std::string> ());
	request.put ("link", send_block->hash ().to_string ());
	request.put ("work", nano::to_string_hex (*node->work_generate_blocking (key.pub)));
	auto response (wait_response (system, rpc, request));
	std::string state_hash (response.get<std::string> ("hash"));
	auto state_text (response.get<std::string> ("block"));
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
	rpc_ctx->io_scope->reset ();
	auto process_result (node->process (*state_block));
	ASSERT_EQ (nano::process_result::progress, process_result.code);
	ASSERT_EQ (state_block->sideband ().details.epoch, nano::epoch::epoch_0);
	ASSERT_TRUE (state_block->sideband ().details.is_receive);
	ASSERT_FALSE (node->latest (key.pub).is_zero ());
}

// Missing "work" parameter should cause work to be generated for us.
TEST (rpc, block_create_state_request_work)
{
	// Test work generation for state blocks both with and without previous (in the latter
	// case, the account will be used for work generation)
	std::unique_ptr<nano::state_block> epoch2;
	{
		nano::system system (1);
		system.upgrade_genesis_epoch (*system.nodes.front (), nano::epoch::epoch_1);
		epoch2 = system.upgrade_genesis_epoch (*system.nodes.front (), nano::epoch::epoch_2);
	}

	std::vector<std::string> previous_test_input{ epoch2->hash ().to_string (), std::string ("0") };
	for (auto previous : previous_test_input)
	{
		nano::system system;
		auto node = add_ipc_enabled_node (system);
		nano::keypair key;
		system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
		auto [rpc, rpc_ctx] = add_rpc (system, node);
		boost::property_tree::ptree request;
		request.put ("action", "block_create");
		request.put ("type", "state");
		request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
		request.put ("account", nano::dev::genesis_key.pub.to_account ());
		request.put ("representative", nano::dev::genesis_key.pub.to_account ());
		request.put ("balance", (nano::dev::genesis_amount - nano::Gxrb_ratio).convert_to<std::string> ());
		request.put ("link", key.pub.to_account ());
		request.put ("previous", previous);
		auto response (wait_response (system, rpc, request));
		boost::property_tree::ptree block_l;
		std::stringstream block_stream (response.get<std::string> ("block"));
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1));
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));
	auto send_block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("key", key.prv.to_string ());
	request.put ("account", key.pub.to_account ());
	request.put ("previous", 0);
	request.put ("representative", nano::dev::genesis_key.pub.to_account ());
	request.put ("balance", nano::Gxrb_ratio.convert_to<std::string> ());
	request.put ("link", send_block->hash ().to_string ());
	auto response (wait_response (system, rpc, request));
	std::string state_hash (response.get<std::string> ("hash"));
	auto state_text (response.get<std::string> ("block"));
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
	rpc_ctx->io_scope->reset ();
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1));
	auto send_block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	nano::state_block open (key.pub, 0, nano::dev::genesis_key.pub, nano::Gxrb_ratio, send_block->hash (), key.prv, key.pub, *node->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node->process (open).code);
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));
	auto send_block_2 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("key", key.prv.to_string ());
	request.put ("account", key.pub.to_account ());
	request.put ("previous", open.hash ().to_string ());
	request.put ("representative", nano::dev::genesis_key.pub.to_account ());
	request.put ("balance", (2 * nano::Gxrb_ratio).convert_to<std::string> ());
	request.put ("link", send_block_2->hash ().to_string ());
	auto response (wait_response (system, rpc, request));
	std::string state_hash (response.get<std::string> ("hash"));
	auto state_text (response.get<std::string> ("block"));
	std::stringstream block_stream (state_text);
	boost::property_tree::ptree block_l;
	boost::property_tree::read_json (block_stream, block_l);
	auto state_block (nano::deserialize_block_json (block_l));
	ASSERT_NE (nullptr, state_block);
	ASSERT_EQ (nano::block_type::state, state_block->type ());
	ASSERT_EQ (state_hash, state_block->hash ().to_string ());
	auto difficulty (state_block->difficulty ());
	ASSERT_GT (difficulty, nano::work_threshold (state_block->work_version (), nano::block_details (nano::epoch::epoch_2, false, true, false)));
	rpc_ctx->io_scope->reset ();
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
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_1));
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (*node, nano::epoch::epoch_2));
	auto send_block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	nano::state_block open (key.pub, 0, nano::dev::genesis_key.pub, nano::Gxrb_ratio, send_block->hash (), key.prv, key.pub, *node->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node->process (open).code);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_create");
	request.put ("type", "state");
	request.put ("key", key.prv.to_string ());
	request.put ("account", key.pub.to_account ());
	request.put ("previous", open.hash ().to_string ());
	request.put ("representative", nano::dev::genesis_key.pub.to_account ());
	request.put ("balance", 0);
	request.put ("link", nano::dev::genesis_key.pub.to_string ());
	auto response (wait_response (system, rpc, request));
	std::string state_hash (response.get<std::string> ("hash"));
	auto state_text (response.get<std::string> ("block"));
	std::stringstream block_stream (state_text);
	boost::property_tree::ptree block_l;
	boost::property_tree::read_json (block_stream, block_l);
	auto state_block (nano::deserialize_block_json (block_l));
	ASSERT_NE (nullptr, state_block);
	ASSERT_EQ (nano::block_type::state, state_block->type ());
	ASSERT_EQ (state_hash, state_block->hash ().to_string ());
	auto difficulty (state_block->difficulty ());
	ASSERT_GT (difficulty, nano::work_threshold (state_block->work_version (), nano::block_details (nano::epoch::epoch_2, true, false, false)));
	rpc_ctx->io_scope->reset ();
	auto process_result (node->process (*state_block));
	ASSERT_EQ (nano::process_result::progress, process_result.code);
	ASSERT_EQ (state_block->sideband ().details.epoch, nano::epoch::epoch_2);
	ASSERT_TRUE (state_block->sideband ().details.is_send);
	ASSERT_FALSE (node->latest (key.pub).is_zero ());
}

TEST (rpc, block_hash)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	nano::keypair key;
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	boost::property_tree::ptree request;
	request.put ("action", "block_hash");
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	auto response (wait_response (system, rpc, request));
	std::string send_hash (response.get<std::string> ("hash"));
	ASSERT_EQ (send.hash ().to_string (), send_hash);
}

TEST (rpc, wallet_lock)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	{
		auto transaction (system.wallet (0)->wallets.tx_begin_read ());
		ASSERT_TRUE (system.wallet (0)->store.valid_password (transaction));
	}
	request.put ("wallet", wallet);
	request.put ("action", "wallet_lock");
	auto response (wait_response (system, rpc, request));
	std::string account_text1 (response.get<std::string> ("locked"));
	ASSERT_EQ (account_text1, "1");
	auto transaction (system.wallet (0)->wallets.tx_begin_read ());
	ASSERT_FALSE (system.wallet (0)->store.valid_password (transaction));
}

TEST (rpc, wallet_locked)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_locked");
	auto response (wait_response (system, rpc, request));
	std::string account_text1 (response.get<std::string> ("locked"));
	ASSERT_EQ (account_text1, "0");
}

TEST (rpc, wallet_create_fail)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	// lmdb_max_dbs should be removed once the wallet store is refactored to support more wallets.
	for (int i = 0; i < 127; i++)
	{
		node->wallets.create (nano::random_wallet_id ());
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_create");
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ (std::error_code (nano::error_common::wallet_lmdb_max_dbs).message (), response.get<std::string> ("error"));
}

TEST (rpc, wallet_ledger)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (key.prv);
	auto latest (node1->latest (nano::dev::genesis_key.pub));
	nano::send_block send (latest, key.pub, 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node1->work_generate_blocking (latest));
	node1->process (send);
	nano::open_block open (send.hash (), nano::dev::genesis_key.pub, key.pub, key.prv, key.pub, *node1->work_generate_blocking (key.pub));
	ASSERT_EQ (nano::process_result::progress, node1->process (open).code);
	auto time (nano::seconds_since_epoch ());
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_ledger");
	request.put ("wallet", node1->wallets.items.begin ()->first.to_string ());
	request.put ("sorting", "1");
	request.put ("count", "1");
	auto response (wait_response (system, rpc, request));
	for (auto & accounts : response.get_child ("accounts"))
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
	auto response2 (wait_response (system, rpc, request));
	for (auto & accounts : response2.get_child ("accounts"))
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	std::string wallet;
	node->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("action", "wallet_add_watch");
	boost::property_tree::ptree entry;
	boost::property_tree::ptree peers_l;
	entry.put ("", nano::dev::genesis_key.pub.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.add_child ("accounts", peers_l);
	auto response (wait_response (system, rpc, request));
	std::string success (response.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
	ASSERT_TRUE (system.wallet (0)->exists (nano::dev::genesis_key.pub));

	// Make sure using special wallet key as pubkey fails
	nano::public_key bad_key (1);
	entry.put ("", bad_key.to_account ());
	peers_l.push_back (std::make_pair ("", entry));
	request.erase ("accounts");
	request.add_child ("accounts", peers_l);

	auto response_error (wait_response (system, rpc, request));
	std::error_code ec (nano::error_common::bad_public_key);
	ASSERT_EQ (response_error.get<std::string> ("error"), ec.message ());
}

TEST (rpc, online_reps)
{
	nano::system system (1);
	auto node1 (system.nodes[0]);
	auto node2 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_EQ (node2->online_reps.online (), 0);
	auto send_block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_NE (nullptr, send_block);
	ASSERT_TIMELY (10s, !node2->online_reps.list ().empty ());
	ASSERT_EQ (node2->online_reps.online (), nano::dev::genesis_amount - nano::Gxrb_ratio);
	auto [rpc, rpc_ctx] = add_rpc (system, node2);
	boost::property_tree::ptree request;
	request.put ("action", "representatives_online");
	auto response (wait_response (system, rpc, request));
	auto representatives (response.get_child ("representatives"));
	auto item (representatives.begin ());
	ASSERT_NE (representatives.end (), item);
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), item->second.get<std::string> (""));
	boost::optional<std::string> weight (item->second.get_optional<std::string> ("weight"));
	ASSERT_FALSE (weight.is_initialized ());
	ASSERT_TIMELY (5s, node2->block (send_block->hash ()));
	//Test weight option
	request.put ("weight", "true");
	auto response2 (wait_response (system, rpc, request));
	auto representatives2 (response2.get_child ("representatives"));
	auto item2 (representatives2.begin ());
	ASSERT_NE (representatives2.end (), item2);
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), item2->first);
	auto weight2 (item2->second.get<std::string> ("weight"));
	ASSERT_EQ (node2->weight (nano::dev::genesis_key.pub).convert_to<std::string> (), weight2);
	//Test accounts filter
	rpc_ctx->io_scope->reset ();
	auto new_rep (system.wallet (1)->deterministic_insert ());
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, new_rep, node1->config.receive_minimum.number ()));
	rpc_ctx->io_scope->renew ();
	ASSERT_NE (nullptr, send);
	ASSERT_TIMELY (10s, node2->block (send->hash ()));
	rpc_ctx->io_scope->reset ();
	auto receive (system.wallet (1)->receive_action (send->hash (), new_rep, node1->config.receive_minimum.number (), send->link ().as_account ()));
	rpc_ctx->io_scope->renew ();
	ASSERT_NE (nullptr, receive);
	ASSERT_TIMELY (5s, node2->block (receive->hash ()));
	rpc_ctx->io_scope->reset ();
	auto change (system.wallet (0)->change_action (nano::dev::genesis_key.pub, new_rep));
	rpc_ctx->io_scope->renew ();
	ASSERT_NE (nullptr, change);
	ASSERT_TIMELY (5s, node2->block (change->hash ()));
	ASSERT_TIMELY (5s, node2->online_reps.list ().size () == 2);
	boost::property_tree::ptree child_rep;
	child_rep.put ("", new_rep.to_account ());
	boost::property_tree::ptree filtered_accounts;
	filtered_accounts.push_back (std::make_pair ("", child_rep));
	request.add_child ("accounts", filtered_accounts);
	auto response3 (wait_response (system, rpc, request, 10s));
	auto representatives3 (response3.get_child ("representatives"));
	auto item3 (representatives3.begin ());
	ASSERT_NE (representatives3.end (), item3);
	ASSERT_EQ (new_rep.to_account (), item3->first);
	ASSERT_EQ (representatives3.size (), 1);
	node2->stop ();
}

TEST (rpc, confirmation_height_currently_processing)
{
	nano::system system;
	nano::node_flags node_flags;
	node_flags.force_use_write_database_queue = true;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;

	auto node = add_ipc_enabled_node (system, node_config, node_flags);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);

	auto previous_genesis_chain_hash = node->latest (nano::dev::genesis_key.pub);
	{
		auto transaction = node->store.tx_begin_write ();
		nano::keypair key1;
		nano::send_block send (previous_genesis_chain_hash, key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio - 1, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (previous_genesis_chain_hash));
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
		previous_genesis_chain_hash = send.hash ();
	}

	std::shared_ptr<nano::block> frontier;
	{
		auto transaction = node->store.tx_begin_read ();
		frontier = node->store.block.get (transaction, previous_genesis_chain_hash);
	}

	boost::property_tree::ptree request;
	request.put ("action", "confirmation_height_currently_processing");

	auto [rpc, rpc_ctx] = add_rpc (system, node);

	// Begin process for confirming the block (and setting confirmation height)
	{
		// Write guard prevents the confirmation height processor writing the blocks, so that we can inspect contents during the response
		auto write_guard = node->write_database_queue.wait (nano::writer::testing);
		node->block_confirm (frontier);

		ASSERT_TIMELY (10s, node->confirmation_height_processor.current () == frontier->hash ());

		// Make the request
		{
			auto response (wait_response (system, rpc, request, 10s));
			auto hash (response.get<std::string> ("hash"));
			ASSERT_EQ (frontier->hash ().to_string (), hash);
		}
	}

	// Wait until confirmation has been set and not processing anything
	ASSERT_TIMELY (10s, node->confirmation_height_processor.current ().is_zero () && node->confirmation_height_processor.awaiting_processing_size () == 0);

	// Make the same request, it should now return an error
	{
		auto response (wait_response (system, rpc, request, 10s));
		std::error_code ec (nano::error_rpc::confirmation_height_not_processing);
		ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	}
}

TEST (rpc, confirmation_history)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_TRUE (node->active.list_recently_cemented ().empty ());
	auto block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_TIMELY (10s, !node->active.list_recently_cemented ().empty ());
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "confirmation_history");
	auto response (wait_response (system, rpc, request));
	auto representatives (response.get_child ("confirmations"));
	auto item (representatives.begin ());
	ASSERT_NE (representatives.end (), item);
	auto hash (item->second.get<std::string> ("hash"));
	auto tally (item->second.get<std::string> ("tally"));
	auto final_tally (item->second.get<std::string> ("final"));
	ASSERT_EQ (1, item->second.count ("duration"));
	ASSERT_EQ (1, item->second.count ("time"));
	ASSERT_EQ (1, item->second.count ("request_count"));
	ASSERT_EQ (1, item->second.count ("voters"));
	ASSERT_GE (1U, item->second.get<unsigned> ("blocks"));
	ASSERT_EQ (block->hash ().to_string (), hash);
	nano::amount tally_num;
	tally_num.decode_dec (tally);
	debug_assert (tally_num == nano::dev::genesis_amount || tally_num == (nano::dev::genesis_amount - nano::Gxrb_ratio));
	system.stop ();
}

TEST (rpc, confirmation_history_hash)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_TRUE (node->active.list_recently_cemented ().empty ());
	auto send1 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	auto send2 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	auto send3 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, nano::Gxrb_ratio));
	ASSERT_TIMELY (10s, node->active.list_recently_cemented ().size () == 3);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "confirmation_history");
	request.put ("hash", send2->hash ().to_string ());
	auto response (wait_response (system, rpc, request));
	auto representatives (response.get_child ("confirmations"));
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
	debug_assert (tally_num == nano::dev::genesis_amount || tally_num == (nano::dev::genesis_amount - nano::Gxrb_ratio) || tally_num == (nano::dev::genesis_amount - 2 * nano::Gxrb_ratio) || tally_num == (nano::dev::genesis_amount - 3 * nano::Gxrb_ratio));
	system.stop ();
}

TEST (rpc, block_confirm)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto send1 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, nano::dev::genesis->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *node->work_generate_blocking (nano::dev::genesis->hash ())));
	{
		auto transaction (node->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send1).code);
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_confirm");
	request.put ("hash", send1->hash ().to_string ());
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ ("1", response.get<std::string> ("started"));
}

TEST (rpc, block_confirm_absent)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_confirm");
	request.put ("hash", "0");
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response.get<std::string> ("error"));
}

TEST (rpc, block_confirm_confirmed)
{
	nano::system system (1);
	auto path (nano::unique_path ());
	nano::node_config config;
	config.peering_port = nano::get_available_port ();
	config.callback_address = "localhost";
	config.callback_port = nano::get_available_port ();
	config.callback_target = "/";
	config.logging.init (path);
	auto node = add_ipc_enabled_node (system, config);
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, nano::dev::genesis->hash ()));
	}
	ASSERT_EQ (0, node->stats.count (nano::stat::type::error, nano::stat::detail::http_callback, nano::stat::dir::out));
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_confirm");
	request.put ("hash", nano::dev::genesis->hash ().to_string ());
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ ("1", response.get<std::string> ("started"));
	// Check confirmation history
	auto confirmed (node->active.list_recently_cemented ());
	ASSERT_EQ (1, confirmed.size ());
	ASSERT_EQ (nano::dev::genesis->hash (), confirmed.begin ()->winner->hash ());
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "node_id");
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ (node->node_id.prv.to_string (), response.get<std::string> ("private"));
	ASSERT_EQ (node->node_id.pub.to_account (), response.get<std::string> ("as_account"));
	ASSERT_EQ (node->node_id.pub.to_node_id (), response.get<std::string> ("node_id"));
}

TEST (rpc, stats_clear)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::keypair key;
	node->stats.inc (nano::stat::type::ledger, nano::stat::dir::in);
	ASSERT_EQ (1, node->stats.count (nano::stat::type::ledger, nano::stat::dir::in));
	boost::property_tree::ptree request;
	request.put ("action", "stats_clear");
	auto response (wait_response (system, rpc, request));
	std::string success (response.get<std::string> ("success"));
	ASSERT_TRUE (success.empty ());
	ASSERT_EQ (0, node->stats.count (nano::stat::type::ledger, nano::stat::dir::in));
	ASSERT_LE (node->stats.last_reset ().count (), 5);
}

TEST (rpc, unchecked)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::keypair key;
	auto open (std::make_shared<nano::state_block> (key.pub, 0, key.pub, 1, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	auto open2 (std::make_shared<nano::state_block> (key.pub, 0, key.pub, 2, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	node->process_active (open);
	node->process_active (open2);
	node->block_processor.flush ();
	boost::property_tree::ptree request;
	request.put ("action", "unchecked");
	request.put ("count", 2);
	{
		auto response (wait_response (system, rpc, request));
		auto & blocks (response.get_child ("blocks"));
		ASSERT_EQ (2, blocks.size ());
		ASSERT_EQ (1, blocks.count (open->hash ().to_string ()));
		ASSERT_EQ (1, blocks.count (open2->hash ().to_string ()));
	}
	request.put ("json_block", true);
	{
		auto response (wait_response (system, rpc, request));
		auto & blocks (response.get_child ("blocks"));
		ASSERT_EQ (2, blocks.size ());
		auto & open_block (blocks.get_child (open->hash ().to_string ()));
		ASSERT_EQ ("state", open_block.get<std::string> ("type"));
	}
}

TEST (rpc, unchecked_get)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::keypair key;
	auto open (std::make_shared<nano::state_block> (key.pub, 0, key.pub, 1, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	node->process_active (open);
	node->block_processor.flush ();
	boost::property_tree::ptree request;
	request.put ("action", "unchecked_get");
	request.put ("hash", open->hash ().to_string ());
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (1, response.count ("contents"));
		auto timestamp (response.get<uint64_t> ("modified_timestamp"));
		ASSERT_LE (timestamp, nano::seconds_since_epoch ());
	}
	request.put ("json_block", true);
	{
		auto response (wait_response (system, rpc, request));
		auto & contents (response.get_child ("contents"));
		ASSERT_EQ ("state", contents.get<std::string> ("type"));
		auto timestamp (response.get<uint64_t> ("modified_timestamp"));
		ASSERT_LE (timestamp, nano::seconds_since_epoch ());
	}
}

TEST (rpc, unchecked_clear)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	nano::keypair key;
	auto open (std::make_shared<nano::state_block> (key.pub, 0, key.pub, 1, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	node->process_active (open);
	node->block_processor.flush ();
	boost::property_tree::ptree request;
	{
		ASSERT_EQ (node->store.unchecked.count (node->store.tx_begin_read ()), 1);
	}
	request.put ("action", "unchecked_clear");
	auto response (wait_response (system, rpc, request));

	ASSERT_TIMELY (10s, node->store.unchecked.count (node->store.tx_begin_read ()) == 0);
}

TEST (rpc, unopened)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::account account1 (1), account2 (account1.number () + 1);
	auto genesis (node->latest (nano::dev::genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, account1, 1));
	ASSERT_NE (nullptr, send);
	auto send2 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, account2, 10));
	ASSERT_NE (nullptr, send2);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	{
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		auto response (wait_response (system, rpc, request));
		auto & accounts (response.get_child ("accounts"));
		ASSERT_EQ (2, accounts.size ());
		ASSERT_EQ ("1", accounts.get<std::string> (account1.to_account ()));
		ASSERT_EQ ("10", accounts.get<std::string> (account2.to_account ()));
	}
	{
		// starting at second account should get a single result
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		request.put ("account", account2.to_account ());
		auto response (wait_response (system, rpc, request));
		auto & accounts (response.get_child ("accounts"));
		ASSERT_EQ (1, accounts.size ());
		ASSERT_EQ ("10", accounts.get<std::string> (account2.to_account ()));
	}
	{
		// starting at third account should get no results
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		request.put ("account", nano::account (account2.number () + 1).to_account ());
		auto response (wait_response (system, rpc, request));
		auto & accounts (response.get_child ("accounts"));
		ASSERT_EQ (0, accounts.size ());
	}
	{
		// using count=1 should get a single result
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		request.put ("count", "1");
		auto response (wait_response (system, rpc, request));
		auto & accounts (response.get_child ("accounts"));
		ASSERT_EQ (1, accounts.size ());
		ASSERT_EQ ("1", accounts.get<std::string> (account1.to_account ()));
	}
	{
		// using threshold at 5 should get a single result
		boost::property_tree::ptree request;
		request.put ("action", "unopened");
		request.put ("threshold", 5);
		auto response (wait_response (system, rpc, request));
		auto & accounts (response.get_child ("accounts"));
		ASSERT_EQ (1, accounts.size ());
		ASSERT_EQ ("10", accounts.get<std::string> (account2.to_account ()));
	}
}

TEST (rpc, unopened_burn)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto genesis (node->latest (nano::dev::genesis_key.pub));
	ASSERT_FALSE (genesis.is_zero ());
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, nano::dev::constants.burn_account, 1));
	ASSERT_NE (nullptr, send);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "unopened");
	auto response (wait_response (system, rpc, request));
	auto & accounts (response.get_child ("accounts"));
	ASSERT_EQ (0, accounts.size ());
}

TEST (rpc, unopened_no_accounts)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "unopened");
	auto response (wait_response (system, rpc, request));
	auto & accounts (response.get_child ("accounts"));
	ASSERT_EQ (0, accounts.size ());
}

TEST (rpc, uptime)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "uptime");
	std::this_thread::sleep_for (std::chrono::seconds (1));
	auto response (wait_response (system, rpc, request));
	ASSERT_LE (1, response.get<int> ("seconds"));
}

TEST (rpc, wallet_history)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.enable_voting = false;
	auto node = add_ipc_enabled_node (system, node_config);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto timestamp1 (nano::seconds_since_epoch ());
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub, node->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send);
	auto timestamp2 (nano::seconds_since_epoch ());
	auto receive (system.wallet (0)->receive_action (send->hash (), nano::dev::genesis_key.pub, node->config.receive_minimum.number (), send->link ().as_account ()));
	ASSERT_NE (nullptr, receive);
	nano::keypair key;
	auto timestamp3 (nano::seconds_since_epoch ());
	auto send2 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key.pub, node->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send2);
	system.deadline_set (10s);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "wallet_history");
	request.put ("wallet", node->wallets.items.begin ()->first.to_string ());
	auto response (wait_response (system, rpc, request));
	std::vector<std::tuple<std::string, std::string, std::string, std::string, std::string, std::string>> history_l;
	auto & history_node (response.get_child ("history"));
	for (auto i (history_node.begin ()), n (history_node.end ()); i != n; ++i)
	{
		history_l.push_back (std::make_tuple (i->second.get<std::string> ("type"), i->second.get<std::string> ("account"), i->second.get<std::string> ("amount"), i->second.get<std::string> ("hash"), i->second.get<std::string> ("block_account"), i->second.get<std::string> ("local_timestamp")));
	}
	ASSERT_EQ (4, history_l.size ());
	ASSERT_EQ ("send", std::get<0> (history_l[0]));
	ASSERT_EQ (key.pub.to_account (), std::get<1> (history_l[0]));
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), std::get<2> (history_l[0]));
	ASSERT_EQ (send2->hash ().to_string (), std::get<3> (history_l[0]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<4> (history_l[0]));
	ASSERT_EQ (std::to_string (timestamp3), std::get<5> (history_l[0]));
	ASSERT_EQ ("receive", std::get<0> (history_l[1]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[1]));
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), std::get<2> (history_l[1]));
	ASSERT_EQ (receive->hash ().to_string (), std::get<3> (history_l[1]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<4> (history_l[1]));
	ASSERT_EQ (std::to_string (timestamp2), std::get<5> (history_l[1]));
	ASSERT_EQ ("send", std::get<0> (history_l[2]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[2]));
	ASSERT_EQ (node->config.receive_minimum.to_string_dec (), std::get<2> (history_l[2]));
	ASSERT_EQ (send->hash ().to_string (), std::get<3> (history_l[2]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<4> (history_l[2]));
	ASSERT_EQ (std::to_string (timestamp1), std::get<5> (history_l[2]));
	// Genesis block
	ASSERT_EQ ("receive", std::get<0> (history_l[3]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<1> (history_l[3]));
	ASSERT_EQ (nano::dev::genesis_amount.convert_to<std::string> (), std::get<2> (history_l[3]));
	ASSERT_EQ (nano::dev::genesis->hash ().to_string (), std::get<3> (history_l[3]));
	ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), std::get<4> (history_l[3]));
}

TEST (rpc, sign_hash)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	nano::state_block send (nano::dev::genesis->account (), node1->latest (nano::dev::genesis_key.pub), nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, 0);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "sign");
	request.put ("hash", send.hash ().to_string ());
	request.put ("key", key.prv.to_string ());
	auto response (wait_response (system, rpc, request, 10s));
	std::error_code ec (nano::error_rpc::sign_hash_disabled);
	ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
	rpc_ctx->node_rpc_config->enable_sign_hash = true;
	auto response2 (wait_response (system, rpc, request, 10s));
	nano::signature signature;
	std::string signature_text (response2.get<std::string> ("signature"));
	ASSERT_FALSE (signature.decode_hex (signature_text));
	ASSERT_FALSE (nano::validate_message (key.pub, send.hash (), signature));
}

TEST (rpc, sign_block)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	nano::keypair key;
	system.wallet (0)->insert_adhoc (key.prv);
	nano::state_block send (nano::dev::genesis->account (), node1->latest (nano::dev::genesis_key.pub), nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, 0);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);
	boost::property_tree::ptree request;
	request.put ("action", "sign");
	std::string wallet;
	node1->wallets.items.begin ()->first.encode_hex (wallet);
	request.put ("wallet", wallet);
	request.put ("account", key.pub.to_account ());
	std::string json;
	send.serialize_json (json);
	request.put ("block", json);
	auto response (wait_response (system, rpc, request, 10s));
	auto contents (response.get<std::string> ("block"));
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);

	// Preliminary test adding to the vote uniquer and checking json output is correct
	nano::keypair key;
	auto block (std::make_shared<nano::state_block> (0, 0, 0, 0, 0, key.prv, key.pub, 0));
	std::vector<nano::block_hash> hashes;
	hashes.push_back (block->hash ());
	auto vote (std::make_shared<nano::vote> (key.pub, key.prv, 0, hashes));
	node->vote_uniquer.unique (vote);
	boost::property_tree::ptree request;
	request.put ("action", "stats");
	request.put ("type", "objects");
	{
		auto response (wait_response (system, rpc, request));

		ASSERT_EQ (response.get_child ("node").get_child ("vote_uniquer").get_child ("votes").get<std::string> ("count"), "1");
	}

	request.put ("type", "database");
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_TRUE (!response.empty ());
	}
}

TEST (rpc, block_confirmed)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "block_info");
	request.put ("hash", "bad_hash1337");
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ (std::error_code (nano::error_blocks::invalid_block_hash).message (), response.get<std::string> ("error"));

	request.put ("hash", "0");
	auto response1 (wait_response (system, rpc, request));
	ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response1.get<std::string> ("error"));

	rpc_ctx->io_scope->reset ();
	nano::keypair key;

	// Open an account directly in the ledger
	{
		auto transaction = node->store.tx_begin_write ();
		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));
		nano::send_block send1 (latest, key.pub, 300, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);

		nano::open_block open1 (send1.hash (), nano::dev::genesis->account (), key.pub, key.prv, key.pub, *system.work.generate (key.pub));
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open1).code);
	}
	rpc_ctx->io_scope->renew ();

	// This should not be confirmed
	nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));
	request.put ("hash", latest.to_string ());
	auto response2 (wait_response (system, rpc, request));
	ASSERT_FALSE (response2.get<bool> ("confirmed"));

	// Create and process a new send block
	auto send = std::make_shared<nano::send_block> (latest, key.pub, 10, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
	node->process_active (send);
	node->block_processor.flush ();
	node->block_confirm (send);
	auto election = node->active.election (send->qualified_root ());
	ASSERT_NE (nullptr, election);
	election->force_confirm ();

	// Wait until the confirmation height has been set
	ASSERT_TIMELY (10s, node->ledger.block_confirmed (node->store.tx_begin_read (), send->hash ()) && !node->confirmation_height_processor.is_processing_block (send->hash ()));

	// Requesting confirmation for this should now succeed
	request.put ("hash", send->hash ().to_string ());
	auto response3 (wait_response (system, rpc, request));
	ASSERT_TRUE (response3.get<bool> ("confirmed"));
}

TEST (rpc, database_txn_tracker)
{
	if (nano::rocksdb_config::using_rocksdb_in_tests ())
	{
		// Don't test this in rocksdb mode
		return;
	}

	// First try when database tracking is disabled
	{
		nano::system system;
		auto node = add_ipc_enabled_node (system);
		auto [rpc, rpc_ctx] = add_rpc (system, node);

		boost::property_tree::ptree request;
		request.put ("action", "database_txn_tracker");
		{
			auto response (wait_response (system, rpc, request));
			std::error_code ec (nano::error_common::tracking_not_enabled);
			ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
		}
	}

	// Now try enabling it but with invalid amounts
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.diagnostics_config.txn_tracking.enable = true;
	auto node = add_ipc_enabled_node (system, node_config);
	auto [rpc, rpc_ctx] = add_rpc (system, node);

	boost::property_tree::ptree request;
	auto check_not_correct_amount = [&system, &request, rpc = rpc] () {
		auto response (wait_response (system, rpc, request));
		std::error_code ec (nano::error_common::invalid_amount);
		ASSERT_EQ (response.get<std::string> ("error"), ec.message ());
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
	std::thread thread ([&store = node->store, &keep_txn_alive_promise, &txn_created_promise] () {
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
	// It can take a long time to generate stack traces
	auto response (wait_response (system, rpc, request, 60s));
	keep_txn_alive_promise.set_value ();
	std::vector<std::tuple<std::string, std::string, std::string, std::vector<std::tuple<std::string, std::string, std::string, std::string>>>> json_l;
	auto & json_node (response.get_child ("txn_tracking"));
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	ASSERT_EQ (node->default_difficulty (nano::work_version::work_1), node->network_params.network.publish_thresholds.epoch_2);
	boost::property_tree::ptree request;
	request.put ("action", "active_difficulty");
	auto expected_multiplier{ 1.0 };
	{
		auto response (wait_response (system, rpc, request));
		auto network_minimum_text (response.get<std::string> ("network_minimum"));
		uint64_t network_minimum;
		ASSERT_FALSE (nano::from_string_hex (network_minimum_text, network_minimum));
		ASSERT_EQ (node->default_difficulty (nano::work_version::work_1), network_minimum);
		auto network_receive_minimum_text (response.get<std::string> ("network_receive_minimum"));
		uint64_t network_receive_minimum;
		ASSERT_FALSE (nano::from_string_hex (network_receive_minimum_text, network_receive_minimum));
		ASSERT_EQ (node->default_receive_difficulty (nano::work_version::work_1), network_receive_minimum);
		auto multiplier (response.get<double> ("multiplier"));
		ASSERT_NEAR (expected_multiplier, multiplier, 1e-6);
		auto network_current_text (response.get<std::string> ("network_current"));
		uint64_t network_current;
		ASSERT_FALSE (nano::from_string_hex (network_current_text, network_current));
		ASSERT_EQ (nano::difficulty::from_multiplier (expected_multiplier, node->default_difficulty (nano::work_version::work_1)), network_current);
		auto network_receive_current_text (response.get<std::string> ("network_receive_current"));
		uint64_t network_receive_current;
		ASSERT_FALSE (nano::from_string_hex (network_receive_current_text, network_receive_current));
		auto network_receive_current_multiplier (nano::difficulty::to_multiplier (network_receive_current, network_receive_minimum));
		auto network_receive_current_normalized_multiplier (nano::normalized_multiplier (network_receive_current_multiplier, network_receive_minimum));
		ASSERT_NEAR (network_receive_current_normalized_multiplier, multiplier, 1e-6);
		ASSERT_EQ (response.not_found (), response.find ("difficulty_trend"));
	}
	// Test include_trend optional
	request.put ("include_trend", true);
	{
		auto response (wait_response (system, rpc, request));
		auto trend_opt (response.get_child_optional ("difficulty_trend"));
		ASSERT_TRUE (trend_opt.is_initialized ());
		auto & trend (trend_opt.get ());
		ASSERT_EQ (1, trend.size ());
	}
}

// This is mainly to check for threading issues with TSAN
TEST (rpc, simultaneous_calls)
{
	// This tests simulatenous calls to the same node in different threads
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	scoped_io_thread_name_change scoped_thread_name_io;
	nano::thread_runner runner (system.io_ctx, node->config.io_threads);
	nano::node_rpc_config node_rpc_config;
	nano::ipc::ipc_server ipc_server (*node, node_rpc_config);
	nano::rpc_config rpc_config (nano::get_available_port (), true);
	rpc_config.rpc_process.ipc_port = node->config.ipc_config.transport_tcp.port;
	rpc_config.rpc_process.num_ipc_connections = 8;
	nano::ipc_rpc_processor ipc_rpc_processor (system.io_ctx, rpc_config);
	nano::rpc rpc (system.io_ctx, rpc_config, ipc_rpc_processor);
	rpc.start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_block_count");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());

	constexpr auto num = 100;
	std::array<std::unique_ptr<test_response>, num> test_responses;
	for (int i = 0; i < num; ++i)
	{
		test_responses[i] = std::make_unique<test_response> (request, system.io_ctx);
	}

	std::promise<void> promise;
	std::atomic<int> count{ num };
	for (int i = 0; i < num; ++i)
	{
		std::thread ([&test_responses, &promise, &count, i, port = rpc.config.port] () {
			test_responses[i]->run (port);
			if (--count == 0)
			{
				promise.set_value ();
			}
		})
		.detach ();
	}

	promise.get_future ().wait ();

	ASSERT_TIMELY (60s, std::all_of (test_responses.begin (), test_responses.end (), [] (const auto & test_response) { return test_response->status != 0; }));

	for (int i = 0; i < num; ++i)
	{
		ASSERT_EQ (200, test_responses[i]->status);
		std::string block_count_text (test_responses[i]->json.get<std::string> ("block_count"));
		ASSERT_EQ ("1", block_count_text);
	}
	rpc.stop ();
	system.stop ();
	ipc_server.stop ();
	system.io_ctx.stop ();
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
	auto rpc (std::make_shared<nano::rpc> (system.io_ctx, rpc_config, inprocess_rpc_handler));
	rpc->start ();
	boost::property_tree::ptree request;
	request.put ("action", "account_balance");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	std::string balance_text (response.get<std::string> ("balance"));
	ASSERT_EQ ("340282366920938463463374607431768211455", balance_text);
	std::string pending_text (response.get<std::string> ("pending"));
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
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "account_info");
	request.put ("account", nano::dev::genesis_key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	boost::optional<std::string> deprecated_account_format (response.get_optional<std::string> ("deprecated_account_format"));
	ASSERT_FALSE (deprecated_account_format.is_initialized ());
	std::string account_text (nano::dev::genesis_key.pub.to_account ());
	account_text[4] = '-';
	request.put ("account", account_text);
	auto response2 (wait_response (system, rpc, request));
	std::string frontier (response2.get<std::string> ("frontier"));
	ASSERT_EQ (nano::dev::genesis->hash ().to_string (), frontier);
	boost::optional<std::string> deprecated_account_format2 (response2.get_optional<std::string> ("deprecated_account_format"));
	ASSERT_TRUE (deprecated_account_format2.is_initialized ());
}

TEST (rpc, epoch_upgrade)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	nano::keypair key1, key2, key3;
	nano::keypair epoch_signer (nano::dev::genesis_key);
	auto send1 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, nano::dev::genesis->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 1, key1.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (nano::dev::genesis->hash ()))); // to opened account
	ASSERT_EQ (nano::process_result::progress, node->process (*send1).code);
	auto send2 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, send1->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 2, key2.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1->hash ()))); // to unopened account (pending)
	ASSERT_EQ (nano::process_result::progress, node->process (*send2).code);
	auto send3 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, send2->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 3, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send2->hash ()))); // to burn (0)
	ASSERT_EQ (nano::process_result::progress, node->process (*send3).code);
	nano::account max_account (std::numeric_limits<nano::uint256_t>::max ());
	auto send4 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, send3->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 4, max_account, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send3->hash ()))); // to max account
	ASSERT_EQ (nano::process_result::progress, node->process (*send4).code);
	auto open (std::make_shared<nano::state_block> (key1.pub, 0, key1.pub, 1, send1->hash (), key1.prv, key1.pub, *system.work.generate (key1.pub)));
	ASSERT_EQ (nano::process_result::progress, node->process (*open).code);
	// Check accounts epochs
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (2, node->store.account.count (transaction));
		for (auto i (node->store.account.begin (transaction)); i != node->store.account.end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_0);
		}
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "epoch_upgrade");
	request.put ("epoch", 1);
	request.put ("key", epoch_signer.prv.to_string ());
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ ("1", response.get<std::string> ("started"));
	auto response_fail (wait_response (system, rpc, request));
	ASSERT_EQ ("0", response_fail.get<std::string> ("started"));
	ASSERT_TIMELY (10s, 4 == node->store.account.count (node->store.tx_begin_read ()));
	// Check upgrade
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (4, node->store.account.count (transaction));
		for (auto i (node->store.account.begin (transaction)); i != node->store.account.end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_1);
		}
		ASSERT_TRUE (node->store.account.exists (transaction, key1.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, key2.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, std::numeric_limits<nano::uint256_t>::max ()));
		ASSERT_FALSE (node->store.account.exists (transaction, 0));
	}

	rpc_ctx->io_scope->reset ();
	// Epoch 2 upgrade
	auto genesis_latest (node->latest (nano::dev::genesis_key.pub));
	auto send5 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, genesis_latest, nano::dev::genesis_key.pub, nano::dev::genesis_amount - 5, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (genesis_latest))); // to burn (0)
	ASSERT_EQ (nano::process_result::progress, node->process (*send5).code);
	auto send6 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, send5->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 6, key1.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send5->hash ()))); // to key1 (again)
	ASSERT_EQ (nano::process_result::progress, node->process (*send6).code);
	auto key1_latest (node->latest (key1.pub));
	auto send7 (std::make_shared<nano::state_block> (key1.pub, key1_latest, key1.pub, 0, key3.pub, key1.prv, key1.pub, *system.work.generate (key1_latest))); // to key3
	ASSERT_EQ (nano::process_result::progress, node->process (*send7).code);
	{
		// Check pending entry
		auto transaction (node->store.tx_begin_read ());
		nano::pending_info info;
		ASSERT_FALSE (node->store.pending.get (transaction, nano::pending_key (key3.pub, send7->hash ()), info));
		ASSERT_EQ (nano::epoch::epoch_1, info.epoch);
	}

	rpc_ctx->io_scope->renew ();
	request.put ("epoch", 2);
	auto response2 (wait_response (system, rpc, request));
	ASSERT_EQ ("1", response2.get<std::string> ("started"));
	ASSERT_TIMELY (10s, 5 == node->store.account.count (node->store.tx_begin_read ()));
	// Check upgrade
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (5, node->store.account.count (transaction));
		for (auto i (node->store.account.begin (transaction)); i != node->store.account.end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_2);
		}
		ASSERT_TRUE (node->store.account.exists (transaction, key1.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, key2.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, key3.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, std::numeric_limits<nano::uint256_t>::max ()));
		ASSERT_FALSE (node->store.account.exists (transaction, 0));
	}
}

TEST (rpc, epoch_upgrade_multithreaded)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.work_threads = 4;
	auto node = add_ipc_enabled_node (system, node_config);
	nano::keypair key1, key2, key3;
	nano::keypair epoch_signer (nano::dev::genesis_key);
	auto send1 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, nano::dev::genesis->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 1, key1.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (nano::dev::genesis->hash ()))); // to opened account
	ASSERT_EQ (nano::process_result::progress, node->process (*send1).code);
	auto send2 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, send1->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 2, key2.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1->hash ()))); // to unopened account (pending)
	ASSERT_EQ (nano::process_result::progress, node->process (*send2).code);
	auto send3 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, send2->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 3, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send2->hash ()))); // to burn (0)
	ASSERT_EQ (nano::process_result::progress, node->process (*send3).code);
	nano::account max_account (std::numeric_limits<nano::uint256_t>::max ());
	auto send4 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, send3->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 4, max_account, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send3->hash ()))); // to max account
	ASSERT_EQ (nano::process_result::progress, node->process (*send4).code);
	auto open (std::make_shared<nano::state_block> (key1.pub, 0, key1.pub, 1, send1->hash (), key1.prv, key1.pub, *system.work.generate (key1.pub)));
	ASSERT_EQ (nano::process_result::progress, node->process (*open).code);
	// Check accounts epochs
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (2, node->store.account.count (transaction));
		for (auto i (node->store.account.begin (transaction)); i != node->store.account.end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_0);
		}
	}
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "epoch_upgrade");
	request.put ("threads", 2);
	request.put ("epoch", 1);
	request.put ("key", epoch_signer.prv.to_string ());
	auto response (wait_response (system, rpc, request));
	ASSERT_EQ ("1", response.get<std::string> ("started"));
	ASSERT_TIMELY (5s, 4 == node->store.account.count (node->store.tx_begin_read ()));
	// Check upgrade
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (4, node->store.account.count (transaction));
		for (auto i (node->store.account.begin (transaction)); i != node->store.account.end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_1);
		}
		ASSERT_TRUE (node->store.account.exists (transaction, key1.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, key2.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, std::numeric_limits<nano::uint256_t>::max ()));
		ASSERT_FALSE (node->store.account.exists (transaction, 0));
	}

	rpc_ctx->io_scope->reset ();
	// Epoch 2 upgrade
	auto genesis_latest (node->latest (nano::dev::genesis_key.pub));
	auto send5 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, genesis_latest, nano::dev::genesis_key.pub, nano::dev::genesis_amount - 5, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (genesis_latest))); // to burn (0)
	ASSERT_EQ (nano::process_result::progress, node->process (*send5).code);
	auto send6 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, send5->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 6, key1.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send5->hash ()))); // to key1 (again)
	ASSERT_EQ (nano::process_result::progress, node->process (*send6).code);
	auto key1_latest (node->latest (key1.pub));
	auto send7 (std::make_shared<nano::state_block> (key1.pub, key1_latest, key1.pub, 0, key3.pub, key1.prv, key1.pub, *system.work.generate (key1_latest))); // to key3
	ASSERT_EQ (nano::process_result::progress, node->process (*send7).code);
	{
		// Check pending entry
		auto transaction (node->store.tx_begin_read ());
		nano::pending_info info;
		ASSERT_FALSE (node->store.pending.get (transaction, nano::pending_key (key3.pub, send7->hash ()), info));
		ASSERT_EQ (nano::epoch::epoch_1, info.epoch);
	}

	rpc_ctx->io_scope->renew ();
	request.put ("epoch", 2);
	auto response2 (wait_response (system, rpc, request));
	ASSERT_EQ ("1", response2.get<std::string> ("started"));
	ASSERT_TIMELY (5s, 5 == node->store.account.count (node->store.tx_begin_read ()));
	// Check upgrade
	{
		auto transaction (node->store.tx_begin_read ());
		ASSERT_EQ (5, node->store.account.count (transaction));
		for (auto i (node->store.account.begin (transaction)); i != node->store.account.end (); ++i)
		{
			nano::account_info info (i->second);
			ASSERT_EQ (info.epoch (), nano::epoch::epoch_2);
		}
		ASSERT_TRUE (node->store.account.exists (transaction, key1.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, key2.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, key3.pub));
		ASSERT_TRUE (node->store.account.exists (transaction, std::numeric_limits<nano::uint256_t>::max ()));
		ASSERT_FALSE (node->store.account.exists (transaction, 0));
	}
}

TEST (rpc, account_lazy_start)
{
	nano::system system;
	nano::node_flags node_flags;
	node_flags.disable_legacy_bootstrap = true;
	auto node1 = system.add_node (node_flags);
	nano::keypair key;
	// Generating test chain
	auto send1 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, nano::dev::genesis->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, key.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (nano::dev::genesis->hash ())));
	ASSERT_EQ (nano::process_result::progress, node1->process (*send1).code);
	auto open (std::make_shared<nano::open_block> (send1->hash (), key.pub, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	ASSERT_EQ (nano::process_result::progress, node1->process (*open).code);

	// Start lazy bootstrap with account
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.ipc_config.transport_tcp.enabled = true;
	node_config.ipc_config.transport_tcp.port = nano::get_available_port ();
	auto node2 = system.add_node (node_config, node_flags);
	node2->network.udp_channels.insert (node1->network.endpoint (), node1->network_params.protocol.protocol_version);
	auto [rpc, rpc_ctx] = add_rpc (system, node2);
	boost::property_tree::ptree request;
	request.put ("action", "account_info");
	request.put ("account", key.pub.to_account ());
	auto response (wait_response (system, rpc, request));
	boost::optional<std::string> account_error (response.get_optional<std::string> ("error"));
	ASSERT_TRUE (account_error.is_initialized ());

	// Check processed blocks
	ASSERT_TIMELY (10s, !node2->bootstrap_initiator.in_progress ());
	node2->block_processor.flush ();
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send1->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (open->hash ()));
}

TEST (rpc, receive)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto wallet = system.wallet (0);
	std::string wallet_text;
	node->wallets.items.begin ()->first.encode_hex (wallet_text);
	wallet->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key1;
	wallet->insert_adhoc (key1.prv);
	auto send1 (wallet->send_action (nano::dev::genesis_key.pub, key1.pub, node->config.receive_minimum.number (), *node->work_generate_blocking (nano::dev::genesis->hash ())));
	ASSERT_TIMELY (5s, node->balance (nano::dev::genesis_key.pub) != nano::dev::genesis_amount);
	ASSERT_TIMELY (10s, !node->store.account.exists (node->store.tx_begin_read (), key1.pub));
	// Send below minimum receive amount
	auto send2 (wallet->send_action (nano::dev::genesis_key.pub, key1.pub, node->config.receive_minimum.number () - 1, *node->work_generate_blocking (send1->hash ())));
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "receive");
	request.put ("wallet", wallet_text);
	request.put ("account", key1.pub.to_account ());
	request.put ("block", send2->hash ().to_string ());
	{
		auto response (wait_response (system, rpc, request));
		auto receive_text (response.get<std::string> ("block"));
		nano::account_info info;
		ASSERT_FALSE (node->store.account.get (node->store.tx_begin_read (), key1.pub, info));
		ASSERT_EQ (info.head, nano::block_hash{ receive_text });
	}
	// Trying to receive the same block should fail with unreceivable
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (std::error_code (nano::error_process::unreceivable).message (), response.get<std::string> ("error"));
	}
	// Trying to receive a non-existing block should fail
	request.put ("block", nano::block_hash (send2->hash ().number () + 1).to_string ());
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response.get<std::string> ("error"));
	}
}

TEST (rpc, receive_unopened)
{
	nano::system system;
	auto node = add_ipc_enabled_node (system);
	auto wallet = system.wallet (0);
	std::string wallet_text;
	node->wallets.items.begin ()->first.encode_hex (wallet_text);
	wallet->insert_adhoc (nano::dev::genesis_key.prv);
	// Test receiving for unopened account
	nano::keypair key1;
	auto send1 (wallet->send_action (nano::dev::genesis_key.pub, key1.pub, node->config.receive_minimum.number () - 1, *node->work_generate_blocking (nano::dev::genesis->hash ())));
	ASSERT_TIMELY (5s, !node->balance (nano::dev::genesis_key.pub) != nano::dev::genesis_amount);
	ASSERT_FALSE (node->store.account.exists (node->store.tx_begin_read (), key1.pub));
	ASSERT_TRUE (node->store.block.exists (node->store.tx_begin_read (), send1->hash ()));
	wallet->insert_adhoc (key1.prv); // should not auto receive, amount sent was lower than minimum
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "receive");
	request.put ("wallet", wallet_text);
	request.put ("account", key1.pub.to_account ());
	request.put ("block", send1->hash ().to_string ());
	{
		auto response (wait_response (system, rpc, request));
		auto receive_text (response.get<std::string> ("block"));
		nano::account_info info;
		ASSERT_FALSE (node->store.account.get (node->store.tx_begin_read (), key1.pub, info));
		ASSERT_EQ (info.head, info.open_block);
		ASSERT_EQ (info.head.to_string (), receive_text);
		ASSERT_EQ (info.representative, nano::dev::genesis_key.pub);
	}
	rpc_ctx->io_scope->reset ();

	// Test receiving for an unopened with a different wallet representative
	nano::keypair key2;
	auto prev_amount (node->balance (nano::dev::genesis_key.pub));
	auto send2 (wallet->send_action (nano::dev::genesis_key.pub, key2.pub, node->config.receive_minimum.number () - 1, *node->work_generate_blocking (send1->hash ())));
	ASSERT_TIMELY (5s, !node->balance (nano::dev::genesis_key.pub) != prev_amount);
	ASSERT_FALSE (node->store.account.exists (node->store.tx_begin_read (), key2.pub));
	ASSERT_TRUE (node->store.block.exists (node->store.tx_begin_read (), send2->hash ()));
	nano::public_key rep;
	wallet->store.representative_set (node->wallets.tx_begin_write (), rep);
	wallet->insert_adhoc (key2.prv); // should not auto receive, amount sent was lower than minimum
	rpc_ctx->io_scope->renew ();
	request.put ("account", key2.pub.to_account ());
	request.put ("block", send2->hash ().to_string ());
	{
		auto response (wait_response (system, rpc, request));
		auto receive_text (response.get<std::string> ("block"));
		nano::account_info info;
		ASSERT_FALSE (node->store.account.get (node->store.tx_begin_read (), key2.pub, info));
		ASSERT_EQ (info.head, info.open_block);
		ASSERT_EQ (info.head.to_string (), receive_text);
		ASSERT_EQ (info.representative, rep);
	}
}

TEST (rpc, receive_work_disabled)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	auto & worker_node = *system.add_node (config);
	config.peering_port = nano::get_available_port ();
	config.work_threads = 0;
	auto node = add_ipc_enabled_node (system, config);
	auto wallet = system.wallet (1);
	std::string wallet_text;
	node->wallets.items.begin ()->first.encode_hex (wallet_text);
	wallet->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key1;
	ASSERT_TRUE (worker_node.work_generation_enabled ());
	auto send1 (wallet->send_action (nano::dev::genesis_key.pub, key1.pub, node->config.receive_minimum.number () - 1, *worker_node.work_generate_blocking (nano::dev::genesis->hash ()), false));
	ASSERT_TRUE (send1 != nullptr);
	ASSERT_TIMELY (5s, node->balance (nano::dev::genesis_key.pub) != nano::dev::genesis_amount);
	ASSERT_FALSE (node->store.account.exists (node->store.tx_begin_read (), key1.pub));
	ASSERT_TRUE (node->store.block.exists (node->store.tx_begin_read (), send1->hash ()));
	wallet->insert_adhoc (key1.prv);
	auto [rpc, rpc_ctx] = add_rpc (system, node);
	boost::property_tree::ptree request;
	request.put ("action", "receive");
	request.put ("wallet", wallet_text);
	request.put ("account", key1.pub.to_account ());
	request.put ("block", send1->hash ().to_string ());
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (std::error_code (nano::error_common::disabled_work_generation).message (), response.get<std::string> ("error"));
	}
}

TEST (rpc, receive_pruned)
{
	nano::system system;
	auto & node1 = *system.add_node ();
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.enable_voting = false; // Remove after allowing pruned voting
	nano::node_flags node_flags;
	node_flags.enable_pruning = true;
	auto node2 = add_ipc_enabled_node (system, node_config, node_flags);
	auto wallet1 = system.wallet (0);
	auto wallet2 = system.wallet (1);
	std::string wallet_text;
	node2->wallets.items.begin ()->first.encode_hex (wallet_text);
	wallet1->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key1;
	wallet2->insert_adhoc (key1.prv);
	auto send1 (wallet1->send_action (nano::dev::genesis_key.pub, key1.pub, node2->config.receive_minimum.number (), *node2->work_generate_blocking (nano::dev::genesis->hash ())));
	ASSERT_TIMELY (5s, node2->balance (nano::dev::genesis_key.pub) != nano::dev::genesis_amount);
	ASSERT_TIMELY (10s, !node2->store.account.exists (node2->store.tx_begin_read (), key1.pub));
	// Send below minimum receive amount
	auto send2 (wallet1->send_action (nano::dev::genesis_key.pub, key1.pub, node2->config.receive_minimum.number () - 1, *node2->work_generate_blocking (send1->hash ())));
	// Extra send frontier
	auto send3 (wallet1->send_action (nano::dev::genesis_key.pub, key1.pub, node2->config.receive_minimum.number (), *node2->work_generate_blocking (send1->hash ())));
	// Pruning
	ASSERT_TIMELY (5s, node2->ledger.cache.cemented_count == 6 && node2->confirmation_height_processor.current ().is_zero () && node2->confirmation_height_processor.awaiting_processing_size () == 0);
	{
		auto transaction (node2->store.tx_begin_write ());
		ASSERT_EQ (2, node2->ledger.pruning_action (transaction, send2->hash (), 1));
	}
	ASSERT_EQ (2, node2->ledger.cache.pruned_count);
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send1->hash ()));
	ASSERT_FALSE (node2->store.block.exists (node2->store.tx_begin_read (), send1->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send2->hash ()));
	ASSERT_FALSE (node2->store.block.exists (node2->store.tx_begin_read (), send2->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send3->hash ()));

	auto [rpc, rpc_ctx] = add_rpc (system, node2);
	boost::property_tree::ptree request;
	request.put ("action", "receive");
	request.put ("wallet", wallet_text);
	request.put ("account", key1.pub.to_account ());
	request.put ("block", send2->hash ().to_string ());
	{
		auto response (wait_response (system, rpc, request));
		auto receive_text (response.get<std::string> ("block"));
		nano::account_info info;
		ASSERT_FALSE (node2->store.account.get (node2->store.tx_begin_read (), key1.pub, info));
		ASSERT_EQ (info.head, nano::block_hash{ receive_text });
	}
	// Trying to receive the same block should fail with unreceivable
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (std::error_code (nano::error_process::unreceivable).message (), response.get<std::string> ("error"));
	}
	// Trying to receive a non-existing block should fail
	request.put ("block", nano::block_hash (send2->hash ().number () + 1).to_string ());
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (std::error_code (nano::error_blocks::not_found).message (), response.get<std::string> ("error"));
	}
}

TEST (rpc, telemetry_single)
{
	nano::system system (1);
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);

	// Wait until peers are stored as they are done in the background
	auto peers_stored = false;
	ASSERT_TIMELY (10s, node1->store.peer.count (node1->store.tx_begin_read ()) != 0);

	// Missing port
	boost::property_tree::ptree request;
	auto node = system.nodes.front ();
	request.put ("action", "telemetry");
	request.put ("address", "not_a_valid_address");

	{
		auto response (wait_response (system, rpc, request, 10s));
		ASSERT_EQ (std::error_code (nano::error_rpc::requires_port_and_address).message (), response.get<std::string> ("error"));
	}

	// Missing address
	request.erase ("address");
	request.put ("port", 65);

	{
		auto response (wait_response (system, rpc, request, 10s));
		ASSERT_EQ (std::error_code (nano::error_rpc::requires_port_and_address).message (), response.get<std::string> ("error"));
	}

	// Try with invalid address
	request.put ("address", "not_a_valid_address");
	request.put ("port", 65);

	{
		auto response (wait_response (system, rpc, request, 10s));
		ASSERT_EQ (std::error_code (nano::error_common::invalid_ip_address).message (), response.get<std::string> ("error"));
	}

	// Then invalid port
	request.put ("address", (boost::format ("%1%") % node->network.endpoint ().address ()).str ());
	request.put ("port", "invalid port");
	{
		auto response (wait_response (system, rpc, request, 10s));
		ASSERT_EQ (std::error_code (nano::error_common::invalid_port).message (), response.get<std::string> ("error"));
	}

	// Use correctly formed address and port
	request.put ("port", node->network.endpoint ().port ());
	{
		auto response (wait_response (system, rpc, request, 10s));

		nano::jsonconfig config (response);
		nano::telemetry_data telemetry_data;
		auto const should_ignore_identification_metrics = false;
		ASSERT_FALSE (telemetry_data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data (telemetry_data, node->network_params, node->config.bandwidth_limit, node->default_difficulty (nano::work_version::work_1), node->node_id);
	}
}

TEST (rpc, telemetry_all)
{
	nano::system system (1);
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);

	// Wait until peers are stored as they are done in the background
	ASSERT_TIMELY (10s, node1->store.peer.count (node1->store.tx_begin_read ()) != 0);

	// First need to set up the cached data
	std::atomic<bool> done{ false };
	auto node = system.nodes.front ();
	node1->telemetry->get_metrics_single_peer_async (node1->network.find_channel (node->network.endpoint ()), [&done] (nano::telemetry_data_response const & telemetry_data_response_a) {
		ASSERT_FALSE (telemetry_data_response_a.error);
		done = true;
	});

	ASSERT_TIMELY (10s, done);

	boost::property_tree::ptree request;
	request.put ("action", "telemetry");
	{
		auto response (wait_response (system, rpc, request, 10s));
		nano::jsonconfig config (response);
		nano::telemetry_data telemetry_data;
		auto const should_ignore_identification_metrics = true;
		ASSERT_FALSE (telemetry_data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data_excluding_signature (telemetry_data, node->network_params, node->config.bandwidth_limit, node->default_difficulty (nano::work_version::work_1));
		ASSERT_FALSE (response.get_optional<std::string> ("node_id").is_initialized ());
		ASSERT_FALSE (response.get_optional<std::string> ("signature").is_initialized ());
	}

	request.put ("raw", "true");
	auto response (wait_response (system, rpc, request, 10s));

	// This may fail if the response has taken longer than the cache cutoff time.
	auto & all_metrics = response.get_child ("metrics");
	auto & metrics = all_metrics.front ().second;
	ASSERT_EQ (1, all_metrics.size ());

	nano::jsonconfig config (metrics);
	nano::telemetry_data data;
	auto const should_ignore_identification_metrics = false;
	ASSERT_FALSE (data.deserialize_json (config, should_ignore_identification_metrics));
	nano::compare_default_telemetry_response_data (data, node->network_params, node->config.bandwidth_limit, node->default_difficulty (nano::work_version::work_1), node->node_id);

	ASSERT_EQ (node->network.endpoint ().address ().to_string (), metrics.get<std::string> ("address"));
	ASSERT_EQ (node->network.endpoint ().port (), metrics.get<uint16_t> ("port"));
	ASSERT_TRUE (node1->network.find_node_id (data.node_id));
}

// Also tests all forms of ipv4/ipv6
TEST (rpc, telemetry_self)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);

	// Just to have peer count at 1
	node1->network.udp_channels.insert (nano::endpoint (boost::asio::ip::make_address_v6 ("::1"), nano::get_available_port ()), 0);

	boost::property_tree::ptree request;
	request.put ("action", "telemetry");
	request.put ("address", "::1");
	request.put ("port", node1->network.endpoint ().port ());
	auto const should_ignore_identification_metrics = false;
	{
		auto response (wait_response (system, rpc, request, 10s));
		nano::telemetry_data data;
		nano::jsonconfig config (response);
		ASSERT_FALSE (data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data (data, node1->network_params, node1->config.bandwidth_limit, node1->default_difficulty (nano::work_version::work_1), node1->node_id);
	}

	request.put ("address", "[::1]");
	{
		auto response (wait_response (system, rpc, request, 10s));
		nano::telemetry_data data;
		nano::jsonconfig config (response);
		ASSERT_FALSE (data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data (data, node1->network_params, node1->config.bandwidth_limit, node1->default_difficulty (nano::work_version::work_1), node1->node_id);
	}

	request.put ("address", "127.0.0.1");
	{
		auto response (wait_response (system, rpc, request, 10s));
		nano::telemetry_data data;
		nano::jsonconfig config (response);
		ASSERT_FALSE (data.deserialize_json (config, should_ignore_identification_metrics));
		nano::compare_default_telemetry_response_data (data, node1->network_params, node1->config.bandwidth_limit, node1->default_difficulty (nano::work_version::work_1), node1->node_id);
	}

	// Incorrect port should fail
	request.put ("port", "0");
	{
		auto response (wait_response (system, rpc, request, 10s));
		ASSERT_EQ (std::error_code (nano::error_rpc::peer_not_found).message (), response.get<std::string> ("error"));
	}
}

TEST (rpc, confirmation_active)
{
	nano::system system;
	nano::node_config node_config;
	node_config.ipc_config.transport_tcp.enabled = true;
	node_config.ipc_config.transport_tcp.port = nano::get_available_port ();
	nano::node_flags node_flags;
	node_flags.disable_request_loop = true;
	auto node1 (system.add_node (node_config, node_flags));
	auto [rpc, rpc_ctx] = add_rpc (system, node1);

	auto send1 (std::make_shared<nano::send_block> (nano::dev::genesis->hash (), nano::public_key (), nano::dev::genesis_amount - 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (nano::dev::genesis->hash ())));
	auto send2 (std::make_shared<nano::send_block> (send1->hash (), nano::public_key (), nano::dev::genesis_amount - 200, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1->hash ())));
	node1->process_active (send1);
	node1->process_active (send2);
	nano::blocks_confirm (*node1, { send1, send2 });
	ASSERT_EQ (2, node1->active.size ());
	auto election (node1->active.election (send1->qualified_root ()));
	ASSERT_NE (nullptr, election);
	election->force_confirm ();

	boost::property_tree::ptree request;
	request.put ("action", "confirmation_active");
	{
		auto response (wait_response (system, rpc, request));
		auto & confirmations (response.get_child ("confirmations"));
		ASSERT_EQ (1, confirmations.size ());
		ASSERT_EQ (send2->qualified_root ().to_string (), confirmations.front ().second.get<std::string> (""));
		ASSERT_EQ (1, response.get<unsigned> ("unconfirmed"));
		ASSERT_EQ (1, response.get<unsigned> ("confirmed"));
	}
}

TEST (rpc, confirmation_info)
{
	nano::system system;
	auto node1 = add_ipc_enabled_node (system);
	auto [rpc, rpc_ctx] = add_rpc (system, node1);

	auto send (std::make_shared<nano::send_block> (nano::dev::genesis->hash (), nano::public_key (), nano::dev::genesis_amount - 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (nano::dev::genesis->hash ())));
	node1->process_active (send);
	node1->block_processor.flush ();
	node1->scheduler.flush ();
	ASSERT_FALSE (node1->active.empty ());

	boost::property_tree::ptree request;
	request.put ("action", "confirmation_info");
	request.put ("root", send->qualified_root ().to_string ());
	request.put ("representatives", "true");
	request.put ("json_block", "true");
	{
		auto response (wait_response (system, rpc, request));
		ASSERT_EQ (1, response.count ("announcements"));
		ASSERT_EQ (1, response.get<unsigned> ("voters"));
		ASSERT_EQ (send->hash ().to_string (), response.get<std::string> ("last_winner"));
		auto & blocks (response.get_child ("blocks"));
		ASSERT_EQ (1, blocks.size ());
		auto & representatives (blocks.front ().second.get_child ("representatives"));
		ASSERT_EQ (1, representatives.size ());
		ASSERT_EQ (0, response.get<unsigned> ("total_tally"));
	}
}
