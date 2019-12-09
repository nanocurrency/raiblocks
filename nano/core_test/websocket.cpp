#include <nano/boost/asio/connect.hpp>
#include <nano/boost/asio/ip/tcp.hpp>
#include <nano/boost/beast/core.hpp>
#include <nano/boost/beast/websocket.hpp>
#include <nano/core_test/testutil.hpp>
#include <nano/node/testing.hpp>
#include <nano/node/websocket.hpp>

#include <gtest/gtest.h>

#include <boost/property_tree/json_parser.hpp>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace
{
/** This variable must be set to false before setting up every thread that makes a websocket test call (and needs ack), to be safe */
std::atomic<bool> ack_ready{ false };

/** An optionally blocking websocket client for testing */
boost::optional<std::string> websocket_test_call (std::string host, std::string port, std::string message_a, bool await_ack, bool await_response, const std::chrono::seconds response_deadline = 5s)
{
	if (await_ack)
	{
		ack_ready = false;
	}

	boost::optional<std::string> ret;
	boost::asio::io_context ioc;
	boost::asio::ip::tcp::resolver resolver{ ioc };
	auto ws (std::make_shared<boost::beast::websocket::stream<boost::asio::ip::tcp::socket>> (ioc));

	auto const results = resolver.resolve (host, port);
	boost::asio::connect (ws->next_layer (), results.begin (), results.end ());

	ws->handshake (host, "/");
	ws->text (true);
	ws->write (boost::asio::buffer (message_a));

	if (await_ack)
	{
		boost::beast::flat_buffer buffer;
		ws->read (buffer);
		ack_ready = true;
	}

	if (await_response)
	{
		assert (response_deadline > 0s);
		auto buffer (std::make_shared<boost::beast::flat_buffer> ());
		ws->async_read (*buffer, [&ret, ws, buffer](boost::beast::error_code const & ec, std::size_t const n) {
			if (!ec)
			{
				std::ostringstream res;
				res << beast_buffers (buffer->data ());
				ret = res.str ();
			}
		});
		ioc.run_one_for (response_deadline);
	}

	if (ws->is_open ())
	{
		ws->async_close (boost::beast::websocket::close_code::normal, [ws](boost::beast::error_code const & ec) {
			// A synchronous close usually hangs in tests when the server's io_context stops looping
			// An async_close solves this problem
		});
	}
	return ret;
}
}

/** Tests clients subscribing multiple times or unsubscribing without a subscription */
TEST (websocket, subscription_edge)
{
	nano::system system (1);
	nano::node_config config (nano::get_available_port (), system.logging);
	nano::node_flags node_flags;
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), system.alarm, config, system.work, node_flags));
	node1->start ();
	system.nodes.push_back (node1);

	ASSERT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));

	// First subscription
	{
		ack_ready = false;
		std::thread subscription_thread ([config]() {
			websocket_test_call ("::1", std::to_string (config.websocket_config.port), R"json({"action": "subscribe", "topic": "confirmation", "ack": true})json", true, false);
		});
		system.deadline_set (5s);
		while (!ack_ready)
		{
			ASSERT_NO_ERROR (system.poll ());
		}
		subscription_thread.join ();
		ASSERT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
	}

	// Second subscription, should not increase subscriber count, only update the subscription
	{
		ack_ready = false;
		std::thread subscription_thread ([config]() {
			websocket_test_call ("::1", std::to_string (config.websocket_config.port), R"json({"action": "subscribe", "topic": "confirmation", "ack": true})json", true, false);
		});
		system.deadline_set (5s);
		while (!ack_ready)
		{
			ASSERT_NO_ERROR (system.poll ());
		}
		subscription_thread.join ();
		ASSERT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
	}

	// First unsub
	{
		ack_ready = false;
		std::thread unsub_thread ([config]() {
			websocket_test_call ("::1", std::to_string (config.websocket_config.port), R"json({"action": "unsubscribe", "topic": "confirmation", "ack": true})json", true, false);
		});
		system.deadline_set (5s);
		while (!ack_ready)
		{
			ASSERT_NO_ERROR (system.poll ());
		}
		unsub_thread.join ();
		ASSERT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
	}

	// Second unsub, should acknowledge but not decrease subscriber count
	{
		ack_ready = false;
		std::thread unsub_thread ([config]() {
			websocket_test_call ("::1", std::to_string (config.websocket_config.port), R"json({"action": "unsubscribe", "topic": "confirmation", "ack": true})json", true, false);
		});
		system.deadline_set (5s);
		while (!ack_ready)
		{
			ASSERT_NO_ERROR (system.poll ());
		}
		unsub_thread.join ();
		ASSERT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
	}

	node1->stop ();
}

// Test client subscribing to changes in active_difficulty
TEST (websocket, active_difficulty)
{
	nano::system system (1);
	nano::node_config config (nano::get_available_port (), system.logging);
	nano::node_flags node_flags;
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), system.alarm, config, system.work, node_flags));
	node1->start ();
	system.nodes.push_back (node1);

	ASSERT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::active_difficulty));

	// Subscribe to active_difficulty and wait for response asynchronously
	ack_ready = false;
	auto client_task = ([config]() -> boost::optional<std::string> {
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port), R"json({"action": "subscribe", "topic": "active_difficulty", "ack": true})json", true, true);
		return response;
	});
	auto client_future = std::async (std::launch::async, client_task);

	// Wait for acknowledge
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ASSERT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::active_difficulty));

	// Fake history records to force trended_active_difficulty change
	{
		nano::unique_lock<std::mutex> lock (node1->active.mutex);
		node1->active.multipliers_cb.push_front (10.);
	}

	// Wait to receive the active_difficulty message
	system.deadline_set (5s);
	while (client_future.wait_for (std::chrono::seconds (0)) != std::future_status::ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	// Check active_difficulty response
	auto response = client_future.get ();
	ASSERT_TRUE (response);
	std::stringstream stream;
	stream << response;
	boost::property_tree::ptree event;
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "active_difficulty");

	auto message_contents = event.get_child ("message");
	uint64_t network_minimum;
	nano::from_string_hex (message_contents.get<std::string> ("network_minimum"), network_minimum);
	ASSERT_EQ (network_minimum, node1->network_params.network.publish_threshold);

	uint64_t network_current;
	nano::from_string_hex (message_contents.get<std::string> ("network_current"), network_current);
	ASSERT_EQ (network_current, node1->active.active_difficulty ());

	double multiplier = message_contents.get<double> ("multiplier");
	ASSERT_NEAR (multiplier, nano::difficulty::to_multiplier (node1->active.active_difficulty (), node1->network_params.network.publish_threshold), 1e-6);

	node1->stop ();
}

/** Subscribes to block confirmations, confirms a block and then awaits websocket notification */
TEST (websocket, confirmation)
{
	nano::system system (1);
	nano::node_config config (nano::get_available_port (), system.logging);
	nano::node_flags node_flags;
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), system.alarm, config, system.work, node_flags));
	node1->wallets.create (nano::random_wallet_id ());
	node1->start ();
	system.nodes.push_back (node1);

	// Start websocket test-client in a separate thread
	ack_ready = false;
	std::atomic<bool> confirmation_event_received{ false };
	ASSERT_FALSE (node1->websocket_server->any_subscriber (nano::websocket::topic::confirmation));
	std::thread client_thread ([&confirmation_event_received, config]() {
		// This will expect two results: the acknowledgement of the subscription
		// and then the block confirmation message
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port),
			R"json({"action": "subscribe", "topic": "confirmation", "ack": true})json", true, true);
		ASSERT_TRUE (response);
		boost::property_tree::ptree event;
		std::stringstream stream;
		stream << response.get ();
		boost::property_tree::read_json (stream, event);
		ASSERT_EQ (event.get<std::string> ("topic"), "confirmation");
		confirmation_event_received = true;
	});

	// Wait for the subscription to be acknowledged
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	ASSERT_TRUE (node1->websocket_server->any_subscriber (nano::websocket::topic::confirmation));

	nano::keypair key;
	system.wallet (1)->insert_adhoc (nano::test_genesis_key.prv);
	auto balance = nano::genesis_amount;
	auto send_amount = node1->config.online_weight_minimum.number () + 1;
	// Quick-confirm a block, legacy blocks should work without filtering
	{
		nano::block_hash previous (node1->latest (nano::test_genesis_key.pub));
		balance -= send_amount;
		auto send (std::make_shared<nano::send_block> (previous, key.pub, balance, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *system.work.generate (previous)));
		node1->process_active (send);
	}

	// Wait for the confirmation to be received
	system.deadline_set (5s);
	while (!confirmation_event_received)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;
	client_thread.join ();

	std::atomic<bool> unsubscribe_ack_received{ false };
	std::thread client_thread_2 ([&unsubscribe_ack_received, config]() {
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port),
			R"json({"action": "subscribe", "topic": "confirmation", "ack": true})json", true, true);
		ASSERT_TRUE (response);
		boost::property_tree::ptree event;
		std::stringstream stream;
		stream << response.get ();
		boost::property_tree::read_json (stream, event);
		ASSERT_EQ (event.get<std::string> ("topic"), "confirmation");

		// Unsubscribe action, expects an acknowledge but no response follows
		websocket_test_call ("::1", std::to_string (config.websocket_config.port),
			R"json({"action": "unsubscribe", "topic": "confirmation", "ack": true})json", true, true, 1s);
		unsubscribe_ack_received = true;
	});

	// Wait for the subscription to be acknowledged
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	// Quick confirm a state block
	{
		nano::block_hash previous (node1->latest (nano::test_genesis_key.pub));
		balance -= send_amount;
		auto send (std::make_shared<nano::state_block> (nano::test_genesis_key.pub, previous, nano::test_genesis_key.pub, balance, key.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *system.work.generate (previous)));
		node1->process_active (send);
	}

	// Wait for the unsubscribe action to be acknowledged
	system.deadline_set (5s);
	while (!unsubscribe_ack_received)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;
	client_thread_2.join ();

	node1->stop ();
}

/** Tests getting notification of an erased election */
TEST (websocket, stopped_election)
{
	nano::system system (1);
	nano::node_config config (nano::get_available_port (), system.logging);
	nano::node_flags node_flags;
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), system.alarm, config, system.work, node_flags));
	node1->wallets.create (nano::random_wallet_id ());
	node1->start ();
	system.nodes.push_back (node1);

	// Start websocket test-client in a separate thread
	ack_ready = false;
	std::atomic<bool> client_thread_finished{ false };
	ASSERT_FALSE (node1->websocket_server->any_subscriber (nano::websocket::topic::confirmation));
	std::thread client_thread ([&client_thread_finished, config]() {
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port),
			R"json({"action": "subscribe", "topic": "stopped_election", "ack": "true"})json", true, true, 5s);

		ASSERT_TRUE (response);
		boost::property_tree::ptree event;
		std::stringstream stream;
		stream << response.get ();
		boost::property_tree::read_json (stream, event);
		ASSERT_EQ (event.get<std::string> ("topic"), "stopped_election");
		client_thread_finished = true;
	});

	// Wait for subscribe acknowledgement
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	// Create election, then erase it, causing a websocket message to be emitted
	nano::keypair key1;
	nano::genesis genesis;
	auto send1 (std::make_shared<nano::send_block> (genesis.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *system.work.generate (genesis.hash ())));
	nano::publish publish1 (send1);
	auto channel1 (node1->network.udp_channels.create (node1->network.endpoint ()));
	node1->network.process_message (publish1, channel1);
	node1->block_processor.flush ();
	node1->active.erase (*send1);

	// Wait for subscribe acknowledgement
	system.deadline_set (5s);
	while (!client_thread_finished)
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	client_thread.join ();
	node1->stop ();
}

/** Tests the filtering options of block confirmations */
TEST (websocket, confirmation_options)
{
	nano::system system (1);
	nano::node_config config (nano::get_available_port (), system.logging);
	nano::node_flags node_flags;
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), system.alarm, config, system.work, node_flags));
	node1->wallets.create (nano::random_wallet_id ());
	node1->start ();
	system.nodes.push_back (node1);

	// Start websocket test-client in a separate thread
	ack_ready = false;
	std::atomic<bool> client_thread_finished{ false };
	ASSERT_FALSE (node1->websocket_server->any_subscriber (nano::websocket::topic::confirmation));
	std::thread client_thread ([&client_thread_finished, config]() {
		// Subscribe initially with a specific invalid account
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port),
			R"json({"action": "subscribe", "topic": "confirmation", "ack": "true", "options": {"confirmation_type": "active_quorum", "accounts": ["xrb_invalid"]}})json", true, true, 1s);

		ASSERT_FALSE (response);
		client_thread_finished = true;
	});

	// Wait for subscribe acknowledgement
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	// Confirm a state block for an in-wallet account
	system.wallet (1)->insert_adhoc (nano::test_genesis_key.prv);
	nano::keypair key;
	auto balance = nano::genesis_amount;
	auto send_amount = node1->config.online_weight_minimum.number () + 1;
	nano::block_hash previous (node1->latest (nano::test_genesis_key.pub));
	{
		balance -= send_amount;
		auto send (std::make_shared<nano::state_block> (nano::test_genesis_key.pub, previous, nano::test_genesis_key.pub, balance, key.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *system.work.generate (previous)));
		node1->process_active (send);
		previous = send->hash ();
	}

	// Wait for client thread to finish, no confirmation message should be received with given filter
	system.deadline_set (5s);
	while (!client_thread_finished)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	std::atomic<bool> client_thread_2_finished{ false };
	std::thread client_thread_2 ([&client_thread_2_finished, config]() {
		// Re-subscribe with options for all local wallet accounts
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port),
			R"json({"action": "subscribe", "topic": "confirmation", "ack": "true", "options": {"confirmation_type": "active_quorum", "all_local_accounts": "true", "include_election_info": "true"}})json", true, true);

		ASSERT_TRUE (response);
		boost::property_tree::ptree event;
		std::stringstream stream;
		stream << response.get ();
		boost::property_tree::read_json (stream, event);
		ASSERT_EQ (event.get<std::string> ("topic"), "confirmation");
		try
		{
			boost::property_tree::ptree election_info = event.get_child ("message.election_info");
			auto tally (election_info.get<std::string> ("tally"));
			auto time (election_info.get<std::string> ("time"));
			// Duration and request count may be zero on testnet, so we only check that they're present
			ASSERT_EQ (1, election_info.count ("duration"));
			ASSERT_EQ (1, election_info.count ("request_count"));
			ASSERT_EQ (1, election_info.count ("voters"));
			ASSERT_GE (1, election_info.get<unsigned> ("blocks"));
			// Make sure tally and time are non-zero.
			ASSERT_NE ("0", tally);
			ASSERT_NE ("0", time);
		}
		catch (std::runtime_error const & ex)
		{
			FAIL () << ex.what ();
		}

		client_thread_2_finished = true;
	});

	node1->block_processor.flush ();
	// Wait for the subscribe action to be acknowledged
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	ASSERT_TRUE (node1->websocket_server->any_subscriber (nano::websocket::topic::confirmation));

	// Quick-confirm another block
	{
		balance -= send_amount;
		auto send (std::make_shared<nano::state_block> (nano::test_genesis_key.pub, previous, nano::test_genesis_key.pub, balance, key.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *system.work.generate (previous)));
		node1->process_active (send);
		previous = send->hash ();
	}

	node1->block_processor.flush ();
	// Wait for confirmation message
	system.deadline_set (5s);
	while (!client_thread_2_finished)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	std::atomic<bool> client_thread_3_finished{ false };
	std::thread client_thread_3 ([&client_thread_3_finished, config]() {
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port),
			R"json({"action": "subscribe", "topic": "confirmation", "ack": "true", "options": {"confirmation_type": "active_quorum", "all_local_accounts": "true"}})json", true, true, 1s);

		ASSERT_FALSE (response);
		client_thread_3_finished = true;
	});

	// Confirm a legacy block
	// When filtering options are enabled, legacy blocks are always filtered
	{
		balance -= send_amount;
		auto send (std::make_shared<nano::send_block> (previous, key.pub, balance, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *system.work.generate (previous)));
		node1->process_active (send);
		previous = send->hash ();
	}

	node1->block_processor.flush ();
	// Wait for client thread to finish, no confirmation message should be received
	system.deadline_set (5s);
	while (!client_thread_3_finished)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	client_thread.join ();
	client_thread_2.join ();
	client_thread_3.join ();
	node1->stop ();
}

/** Subscribes to votes, sends a block and awaits websocket notification of a vote arrival */
TEST (websocket, vote)
{
	nano::system system (1);
	nano::node_config config (nano::get_available_port (), system.logging);
	nano::node_flags node_flags;
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), system.alarm, config, system.work, node_flags));
	node1->wallets.create (nano::random_wallet_id ());
	node1->start ();
	system.nodes.push_back (node1);

	// Start websocket test-client in a separate thread
	ack_ready = false;
	std::atomic<bool> client_thread_finished{ false };
	ASSERT_FALSE (node1->websocket_server->any_subscriber (nano::websocket::topic::vote));
	std::thread client_thread ([&client_thread_finished, config]() {
		// This will expect two results: the acknowledgement of the subscription
		// and then the vote message
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port),
			R"json({"action": "subscribe", "topic": "vote", "ack": true})json", true, true);

		ASSERT_TRUE (response);
		boost::property_tree::ptree event;
		std::stringstream stream;
		stream << response;
		boost::property_tree::read_json (stream, event);
		ASSERT_EQ (event.get<std::string> ("topic"), "vote");
		client_thread_finished = true;
	});

	// Wait for the subscription to be acknowledged
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	ASSERT_TRUE (node1->websocket_server->any_subscriber (nano::websocket::topic::vote));

	// Quick-confirm a block
	nano::keypair key;
	system.wallet (1)->insert_adhoc (nano::test_genesis_key.prv);
	nano::block_hash previous (node1->latest (nano::test_genesis_key.pub));
	auto send (std::make_shared<nano::state_block> (nano::test_genesis_key.pub, previous, nano::test_genesis_key.pub, nano::genesis_amount - (node1->config.online_weight_minimum.number () + 1), key.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *system.work.generate (previous)));
	node1->process_active (send);

	// Wait for the websocket client to receive the vote message
	system.deadline_set (5s);
	while (!client_thread_finished)
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	client_thread.join ();
	node1->stop ();
}

/** Tests vote subscription options */
TEST (websocket, vote_options)
{
	nano::system system (1);
	nano::node_config config (nano::get_available_port (), system.logging);
	nano::node_flags node_flags;
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), system.alarm, config, system.work, node_flags));
	node1->wallets.create (nano::random_wallet_id ());
	node1->start ();
	system.nodes.push_back (node1);

	// Start websocket test-client in a separate thread
	ack_ready = false;
	std::atomic<bool> client_thread_finished{ false };
	ASSERT_FALSE (node1->websocket_server->any_subscriber (nano::websocket::topic::vote));
	std::thread client_thread ([&client_thread_finished, config]() {
		std::ostringstream data;
		data << R"json({"action": "subscribe", "topic": "vote", "ack": true, "options": {"representatives": [")json"
			 << nano::test_genesis_key.pub.to_account ()
			 << R"json("]}})json";
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port), data.str (), true, true);

		ASSERT_TRUE (response);
		boost::property_tree::ptree event;
		std::stringstream stream;
		stream << response;
		boost::property_tree::read_json (stream, event);
		ASSERT_EQ (event.get<std::string> ("topic"), "vote");
		client_thread_finished = true;
	});

	// Wait for the subscription to be acknowledged
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	ASSERT_TRUE (node1->websocket_server->any_subscriber (nano::websocket::topic::vote));

	// Quick-confirm a block
	nano::keypair key;
	auto balance = nano::genesis_amount;
	system.wallet (1)->insert_adhoc (nano::test_genesis_key.prv);
	auto send_amount = node1->config.online_weight_minimum.number () + 1;
	auto confirm_block = [&]() {
		nano::block_hash previous (node1->latest (nano::test_genesis_key.pub));
		balance -= send_amount;
		auto send (std::make_shared<nano::state_block> (nano::test_genesis_key.pub, previous, nano::test_genesis_key.pub, balance, key.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *system.work.generate (previous)));
		node1->process_active (send);
	};
	confirm_block ();

	// Wait for the websocket client to receive the vote message
	system.deadline_set (5s);
	while (!client_thread_finished || node1->websocket_server->any_subscriber (nano::websocket::topic::vote))
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	std::atomic<bool> client_thread_2_finished{ false };
	std::thread client_thread_2 ([&client_thread_2_finished, config]() {
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port),
			R"json({"action": "subscribe", "topic": "vote", "ack": true, "options": {"representatives": ["xrb_invalid"]}})json", true, true, 1s);

		// No response expected given the filter
		ASSERT_FALSE (response);
		client_thread_2_finished = true;
	});

	// Wait for the subscription to be acknowledged
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ack_ready = false;

	ASSERT_TRUE (node1->websocket_server->any_subscriber (nano::websocket::topic::vote));

	// Confirm another block
	confirm_block ();

	// No response expected
	system.deadline_set (5s);
	while (!client_thread_2_finished)
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	client_thread.join ();
	client_thread_2.join ();
	node1->stop ();
}

// Test client subscribing to notifications for work generation
TEST (websocket, work)
{
	nano::system system (1);
	nano::node_config config (nano::get_available_port (), system.logging);
	nano::node_flags node_flags;
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), system.alarm, config, system.work, node_flags));
	node1->start ();
	system.nodes.push_back (node1);

	ASSERT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::work));

	// Subscribe to work and wait for response asynchronously
	ack_ready = false;
	auto client_task = ([config]() -> boost::optional<std::string> {
		auto response = websocket_test_call ("::1", std::to_string (config.websocket_config.port), R"json({"action": "subscribe", "topic": "work", "ack": true})json", true, true);
		return response;
	});
	auto client_future = std::async (std::launch::async, client_task);

	// Wait for acknowledge
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	ASSERT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::work));

	// Generate work
	nano::block_hash hash{ 1 };
	auto work (node1->work_generate_blocking (hash));
	ASSERT_TRUE (work.is_initialized ());

	// Wait for the work notification
	system.deadline_set (5s);
	while (client_future.wait_for (std::chrono::seconds (0)) != std::future_status::ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	// Check the work notification message
	auto response = client_future.get ();
	ASSERT_TRUE (response);
	std::stringstream stream;
	stream << response;
	boost::property_tree::ptree event;
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "work");

	auto & contents = event.get_child ("message");
	ASSERT_EQ (contents.get<std::string> ("success"), "true");
	ASSERT_LT (contents.get<unsigned> ("duration"), 10000);

	ASSERT_EQ (1, contents.count ("request"));
	auto & request = contents.get_child ("request");
	ASSERT_EQ (request.get<std::string> ("hash"), hash.to_string ());
	ASSERT_EQ (request.get<std::string> ("difficulty"), nano::to_string_hex (node1->network_params.network.publish_threshold));
	ASSERT_EQ (request.get<double> ("multiplier"), 1.0);

	ASSERT_EQ (1, contents.count ("result"));
	auto & result = contents.get_child ("result");
	uint64_t result_difficulty;
	nano::from_string_hex (result.get<std::string> ("difficulty"), result_difficulty);
	ASSERT_GE (result_difficulty, node1->network_params.network.publish_threshold);
	ASSERT_NEAR (result.get<double> ("multiplier"), nano::difficulty::to_multiplier (result_difficulty, node1->network_params.network.publish_threshold), 1e-6);
	ASSERT_EQ (result.get<std::string> ("work"), nano::to_string_hex (work.get ()));

	ASSERT_EQ (1, contents.count ("bad_peers"));
	auto & bad_peers = contents.get_child ("bad_peers");
	ASSERT_TRUE (bad_peers.empty ());

	ASSERT_EQ (contents.get<std::string> ("reason"), "");
}

/** Tests clients subscribing multiple times or unsubscribing without a subscription */
TEST (websocket, ws_keepalive)
{
	nano::system system (1);
	nano::node_config config (nano::get_available_port (), system.logging);
	nano::node_flags node_flags;
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), system.alarm, config, system.work, node_flags));
	node1->start ();
	system.nodes.push_back (node1);
	ack_ready = false;
	std::thread subscription_thread ([config]() {
		websocket_test_call ("::1", std::to_string (config.websocket_config.port), R"json({"action": "ping"})json", true, false);
	});
	system.deadline_set (5s);
	while (!ack_ready)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	subscription_thread.join ();
}