#include <nano/core_test/fakes/websocket_client.hpp>
#include <nano/node/websocket.hpp>
#include <nano/test_common/system.hpp>
#include <nano/test_common/telemetry.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

#include <boost/property_tree/json_parser.hpp>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

using namespace std::chrono_literals;

// Tests clients subscribing multiple times or unsubscribing without a subscription
TEST (websocket, subscription_edge)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	ASSERT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));

	auto task = ([config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "confirmation", "ack": true})json");
		client.await_ack ();
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		client.send_message (R"json({"action": "subscribe", "topic": "confirmation", "ack": true})json");
		client.await_ack ();
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		client.send_message (R"json({"action": "unsubscribe", "topic": "confirmation", "ack": true})json");
		client.await_ack ();
		EXPECT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		client.send_message (R"json({"action": "unsubscribe", "topic": "confirmation", "ack": true})json");
		client.await_ack ();
		EXPECT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
	});
	auto future = std::async (std::launch::async, task);

	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);
}

// Subscribes to block confirmations, confirms a block and then awaits websocket notification
TEST (websocket, confirmation)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	std::atomic<bool> ack_ready{ false };
	std::atomic<bool> unsubscribed{ false };
	auto task = ([&ack_ready, &unsubscribed, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "confirmation", "ack": true})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		auto response = client.get_response ();
		EXPECT_TRUE (response);
		boost::property_tree::ptree event;
		std::stringstream stream;
		stream << response.get ();
		boost::property_tree::read_json (stream, event);
		EXPECT_EQ (event.get<std::string> ("topic"), "confirmation");
		client.send_message (R"json({"action": "unsubscribe", "topic": "confirmation", "ack": true})json");
		client.await_ack ();
		unsubscribed = true;
		EXPECT_FALSE (client.get_response (1s));
	});
	auto future = std::async (std::launch::async, task);

	ASSERT_TIMELY (5s, ack_ready);

	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto balance = nano::dev::genesis_amount;
	auto send_amount = node1->online_reps.delta () + 1;
	// Quick-confirm a block, legacy blocks should work without filtering
	{
		nano::block_hash previous (node1->latest (nano::dev::genesis_key.pub));
		balance -= send_amount;
		auto send (std::make_shared<nano::send_block> (previous, key.pub, balance, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (previous)));
		node1->process_active (send);
	}

	ASSERT_TIMELY (5s, unsubscribed);

	// Quick confirm a state block
	{
		nano::state_block_builder builder;
		nano::block_hash previous (node1->latest (nano::dev::genesis_key.pub));
		balance -= send_amount;
		auto send = builder
					.account (nano::dev::genesis_key.pub)
					.previous (previous)
					.representative (nano::dev::genesis_key.pub)
					.balance (balance)
					.link (key.pub)
					.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
					.work (*system.work.generate (previous))
					.build_shared ();

		node1->process_active (send);
	}

	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);
}

// Tests getting notification of an erased election
TEST (websocket, stopped_election)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	std::atomic<bool> ack_ready{ false };
	auto task = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "stopped_election", "ack": "true"})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::stopped_election));
		return client.get_response ();
	});
	auto future = std::async (std::launch::async, task);

	ASSERT_TIMELY (5s, ack_ready);

	// Create election, then erase it, causing a websocket message to be emitted
	nano::keypair key1;
	auto send1 (std::make_shared<nano::send_block> (nano::dev::genesis->hash (), key1.pub, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (nano::dev::genesis->hash ())));
	nano::publish publish1 (send1);
	auto channel1 (node1->network.udp_channels.create (node1->network.endpoint ()));
	node1->network.inbound (publish1, channel1);
	node1->block_processor.flush ();
	ASSERT_TIMELY (1s, node1->active.election (send1->qualified_root ()));
	node1->active.erase (*send1);

	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);

	auto response = future.get ();
	ASSERT_TRUE (response);
	boost::property_tree::ptree event;
	std::stringstream stream;
	stream << response.get ();
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "stopped_election");
}

// Tests the filtering options of block confirmations
TEST (websocket, confirmation_options)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	std::atomic<bool> ack_ready{ false };
	auto task1 = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "confirmation", "ack": "true", "options": {"confirmation_type": "active_quorum", "accounts": ["xrb_invalid"]}})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		auto response = client.get_response (1s);
		EXPECT_FALSE (response);
	});
	auto future1 = std::async (std::launch::async, task1);

	ASSERT_TIMELY (5s, ack_ready);

	// Confirm a state block for an in-wallet account
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	auto balance = nano::dev::genesis_amount;
	auto send_amount = node1->online_reps.delta () + 1;
	nano::block_hash previous (node1->latest (nano::dev::genesis_key.pub));
	{
		balance -= send_amount;
		nano::state_block_builder builder;
		auto send = builder
					.account (nano::dev::genesis_key.pub)
					.previous (previous)
					.representative (nano::dev::genesis_key.pub)
					.balance (balance)
					.link (key.pub)
					.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
					.work (*system.work.generate (previous))
					.build_shared ();

		node1->process_active (send);
		previous = send->hash ();
	}

	ASSERT_TIMELY (5s, future1.wait_for (0s) == std::future_status::ready);

	ack_ready = false;
	auto task2 = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "confirmation", "ack": "true", "options": {"confirmation_type": "active_quorum", "all_local_accounts": "true", "include_election_info": "true"}})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		return client.get_response ();
	});
	auto future2 = std::async (std::launch::async, task2);

	ASSERT_TIMELY (10s, ack_ready);

	// Quick-confirm another block
	{
		balance -= send_amount;
		nano::state_block_builder builder;
		auto send = builder
					.account (nano::dev::genesis_key.pub)
					.previous (previous)
					.representative (nano::dev::genesis_key.pub)
					.balance (balance)
					.link (key.pub)
					.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
					.work (*system.work.generate (previous))
					.build_shared ();

		node1->process_active (send);
		previous = send->hash ();
	}

	ASSERT_TIMELY (5s, future2.wait_for (0s) == std::future_status::ready);

	auto response2 = future2.get ();
	ASSERT_TRUE (response2);
	boost::property_tree::ptree event;
	std::stringstream stream;
	stream << response2.get ();
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "confirmation");
	try
	{
		boost::property_tree::ptree election_info = event.get_child ("message.election_info");
		auto tally (election_info.get<std::string> ("tally"));
		auto final_tally (election_info.get<std::string> ("final"));
		auto time (election_info.get<std::string> ("time"));
		// Duration and request count may be zero on devnet, so we only check that they're present
		ASSERT_EQ (1, election_info.count ("duration"));
		ASSERT_EQ (1, election_info.count ("request_count"));
		ASSERT_EQ (1, election_info.count ("voters"));
		ASSERT_GE (1U, election_info.get<unsigned> ("blocks"));
		// Make sure tally and time are non-zero.
		ASSERT_NE ("0", tally);
		ASSERT_NE ("0", time);
		auto votes_l (election_info.get_child_optional ("votes"));
		ASSERT_FALSE (votes_l.is_initialized ());
	}
	catch (std::runtime_error const & ex)
	{
		FAIL () << ex.what ();
	}

	ack_ready = false;
	auto task3 = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "confirmation", "ack": "true", "options": {"confirmation_type": "active_quorum", "all_local_accounts": "true"}})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		auto response = client.get_response (1s);
		EXPECT_FALSE (response);
	});
	auto future3 = std::async (std::launch::async, task3);

	ASSERT_TIMELY (5s, ack_ready);

	// Confirm a legacy block
	// When filtering options are enabled, legacy blocks are always filtered
	{
		balance -= send_amount;
		auto send (std::make_shared<nano::send_block> (previous, key.pub, balance, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (previous)));
		node1->process_active (send);
		previous = send->hash ();
	}

	ASSERT_TIMELY (5s, future3.wait_for (0s) == std::future_status::ready);
}

TEST (websocket, confirmation_options_votes)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	std::atomic<bool> ack_ready{ false };
	auto task1 = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "confirmation", "ack": "true", "options": {"confirmation_type": "active_quorum", "include_election_info_with_votes": "true", "include_block": "false"}})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		return client.get_response ();
	});
	auto future1 = std::async (std::launch::async, task1);

	ASSERT_TIMELY (10s, ack_ready);

	// Confirm a state block for an in-wallet account
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	auto balance = nano::dev::genesis_amount;
	auto send_amount = node1->config.online_weight_minimum.number () + 1;
	nano::block_hash previous (node1->latest (nano::dev::genesis_key.pub));
	{
		nano::state_block_builder builder;
		balance -= send_amount;
		auto send = builder
					.account (nano::dev::genesis_key.pub)
					.previous (previous)
					.representative (nano::dev::genesis_key.pub)
					.balance (balance)
					.link (key.pub)
					.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
					.work (*system.work.generate (previous))
					.build_shared ();

		node1->process_active (send);
		previous = send->hash ();
	}

	ASSERT_TIMELY (5s, future1.wait_for (0s) == std::future_status::ready);

	auto response1 = future1.get ();
	ASSERT_TRUE (response1);
	boost::property_tree::ptree event;
	std::stringstream stream;
	stream << response1.get ();
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "confirmation");
	try
	{
		boost::property_tree::ptree election_info = event.get_child ("message.election_info");
		auto tally (election_info.get<std::string> ("tally"));
		auto time (election_info.get<std::string> ("time"));
		// Duration and request count may be zero on devnet, so we only check that they're present
		ASSERT_EQ (1, election_info.count ("duration"));
		ASSERT_EQ (1, election_info.count ("request_count"));
		ASSERT_EQ (1, election_info.count ("voters"));
		ASSERT_GE (1U, election_info.get<unsigned> ("blocks"));
		// Make sure tally and time are non-zero.
		ASSERT_NE ("0", tally);
		ASSERT_NE ("0", time);
		auto votes_l (election_info.get_child_optional ("votes"));
		ASSERT_TRUE (votes_l.is_initialized ());
		ASSERT_EQ (1, votes_l.get ().size ());
		for (auto & vote : votes_l.get ())
		{
			std::string representative (vote.second.get<std::string> ("representative"));
			ASSERT_EQ (nano::dev::genesis_key.pub.to_account (), representative);
			std::string timestamp (vote.second.get<std::string> ("timestamp"));
			ASSERT_NE ("0", timestamp);
			std::string hash (vote.second.get<std::string> ("hash"));
			ASSERT_EQ (node1->latest (nano::dev::genesis_key.pub).to_string (), hash);
			std::string weight (vote.second.get<std::string> ("weight"));
			ASSERT_EQ (node1->balance (nano::dev::genesis_key.pub).convert_to<std::string> (), weight);
		}
	}
	catch (std::runtime_error const & ex)
	{
		FAIL () << ex.what ();
	}
}

// Tests updating options of block confirmations
TEST (websocket, confirmation_options_update)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	std::atomic<bool> added{ false };
	std::atomic<bool> deleted{ false };
	auto task = ([&added, &deleted, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		// Subscribe initially with empty options, everything will be filtered
		client.send_message (R"json({"action": "subscribe", "topic": "confirmation", "ack": "true", "options": {}})json");
		client.await_ack ();
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		// Now update filter with an account and wait for a response
		std::string add_message = boost::str (boost::format (R"json({"action": "update", "topic": "confirmation", "ack": "true", "options": {"accounts_add": ["%1%"]}})json") % nano::dev::genesis_key.pub.to_account ());
		client.send_message (add_message);
		client.await_ack ();
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		added = true;
		EXPECT_TRUE (client.get_response ());
		// Update the filter again, removing the account
		std::string delete_message = boost::str (boost::format (R"json({"action": "update", "topic": "confirmation", "ack": "true", "options": {"accounts_del": ["%1%"]}})json") % nano::dev::genesis_key.pub.to_account ());
		client.send_message (delete_message);
		client.await_ack ();
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::confirmation));
		deleted = true;
		EXPECT_FALSE (client.get_response (1s));
	});
	auto future = std::async (std::launch::async, task);

	// Wait for update acknowledgement
	ASSERT_TIMELY (5s, added);

	// Confirm a block
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key;
	nano::state_block_builder builder;
	auto previous (node1->latest (nano::dev::genesis_key.pub));
	auto send = builder
				.account (nano::dev::genesis_key.pub)
				.previous (previous)
				.representative (nano::dev::genesis_key.pub)
				.balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				.link (key.pub)
				.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				.work (*system.work.generate (previous))
				.build_shared ();

	node1->process_active (send);

	// Wait for delete acknowledgement
	ASSERT_TIMELY (5s, deleted);

	// Confirm another block
	previous = send->hash ();
	auto send2 = builder
				 .make_block ()
				 .account (nano::dev::genesis_key.pub)
				 .previous (previous)
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - 2 * nano::Gxrb_ratio)
				 .link (key.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (previous))
				 .build_shared ();

	node1->process_active (send2);

	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);
}

// Subscribes to votes, sends a block and awaits websocket notification of a vote arrival
TEST (websocket, vote)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	std::atomic<bool> ack_ready{ false };
	auto task = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "vote", "ack": true})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::vote));
		return client.get_response ();
	});
	auto future = std::async (std::launch::async, task);

	ASSERT_TIMELY (5s, ack_ready);

	// Quick-confirm a block
	nano::keypair key;
	nano::state_block_builder builder;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::block_hash previous (node1->latest (nano::dev::genesis_key.pub));
	auto send = builder
				.account (nano::dev::genesis_key.pub)
				.previous (previous)
				.representative (nano::dev::genesis_key.pub)
				.balance (nano::dev::genesis_amount - (node1->online_reps.delta () + 1))
				.link (key.pub)
				.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				.work (*system.work.generate (previous))
				.build_shared ();

	node1->process_active (send);

	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);

	auto response = future.get ();
	ASSERT_TRUE (response);
	boost::property_tree::ptree event;
	std::stringstream stream;
	stream << response;
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "vote");
}

// Tests vote subscription options - vote type
TEST (websocket, vote_options_type)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	std::atomic<bool> ack_ready{ false };
	auto task = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "vote", "ack": true, "options": {"include_replays": "true", "include_indeterminate": "false"}})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::vote));
		return client.get_response ();
	});
	auto future = std::async (std::launch::async, task);

	ASSERT_TIMELY (5s, ack_ready);

	// Custom made votes for simplicity
	auto vote (std::make_shared<nano::vote> (nano::dev::genesis_key.pub, nano::dev::genesis_key.prv, 0, nano::dev::genesis));
	nano::websocket::message_builder builder;
	auto msg (builder.vote_received (vote, nano::vote_code::replay));
	node1->websocket_server->broadcast (msg);

	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);

	auto response = future.get ();
	ASSERT_TRUE (response);
	boost::property_tree::ptree event;
	std::stringstream stream;
	stream << response;
	boost::property_tree::read_json (stream, event);
	auto message_contents = event.get_child ("message");
	ASSERT_EQ (1, message_contents.count ("type"));
	ASSERT_EQ ("replay", message_contents.get<std::string> ("type"));
}

// Tests vote subscription options - list of representatives
TEST (websocket, vote_options_representatives)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	std::atomic<bool> ack_ready{ false };
	auto task1 = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		std::string message = boost::str (boost::format (R"json({"action": "subscribe", "topic": "vote", "ack": "true", "options": {"representatives": ["%1%"]}})json") % nano::dev::genesis_key.pub.to_account ());
		client.send_message (message);
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::vote));
		auto response = client.get_response ();
		EXPECT_TRUE (response);
		boost::property_tree::ptree event;
		std::stringstream stream;
		stream << response;
		boost::property_tree::read_json (stream, event);
		EXPECT_EQ (event.get<std::string> ("topic"), "vote");
	});
	auto future1 = std::async (std::launch::async, task1);

	ASSERT_TIMELY (5s, ack_ready);

	// Quick-confirm a block
	nano::keypair key;
	auto balance = nano::dev::genesis_amount;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto send_amount = node1->online_reps.delta () + 1;
	auto confirm_block = [&] () {
		nano::block_hash previous (node1->latest (nano::dev::genesis_key.pub));
		balance -= send_amount;
		nano::state_block_builder builder;
		auto send = builder
					.account (nano::dev::genesis_key.pub)
					.previous (previous)
					.representative (nano::dev::genesis_key.pub)
					.balance (balance)
					.link (key.pub)
					.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
					.work (*system.work.generate (previous))
					.build_shared ();
		node1->process_active (send);
	};
	confirm_block ();

	ASSERT_TIMELY (5s, future1.wait_for (0s) == std::future_status::ready);

	ack_ready = false;
	auto task2 = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "vote", "ack": "true", "options": {"representatives": ["xrb_invalid"]}})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::vote));
		auto response = client.get_response ();
		// A list of invalid representatives is the same as no filter
		EXPECT_TRUE (response);
	});
	auto future2 = std::async (std::launch::async, task2);

	// Wait for the subscription to be acknowledged
	ASSERT_TIMELY (5s, ack_ready);

	// Confirm another block
	confirm_block ();

	ASSERT_TIMELY (5s, future2.wait_for (0s) == std::future_status::ready);
}

// Test client subscribing to notifications for work generation
TEST (websocket, work)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	ASSERT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::work));

	// Subscribe to work and wait for response asynchronously
	std::atomic<bool> ack_ready{ false };
	auto task = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "work", "ack": true})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::work));
		return client.get_response ();
	});
	auto future = std::async (std::launch::async, task);

	// Wait for acknowledge
	ASSERT_TIMELY (5s, ack_ready);
	ASSERT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::work));

	// Generate work
	nano::block_hash hash{ 1 };
	auto work (node1->work_generate_blocking (hash));
	ASSERT_TRUE (work.is_initialized ());

	// Wait for the work notification
	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);

	// Check the work notification message
	auto response = future.get ();
	ASSERT_TRUE (response);
	std::stringstream stream;
	stream << response;
	boost::property_tree::ptree event;
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "work");

	auto & contents = event.get_child ("message");
	ASSERT_EQ (contents.get<std::string> ("success"), "true");
	ASSERT_LT (contents.get<unsigned> ("duration"), 10000U);

	ASSERT_EQ (1, contents.count ("request"));
	auto & request = contents.get_child ("request");
	ASSERT_EQ (request.get<std::string> ("version"), nano::to_string (nano::work_version::work_1));
	ASSERT_EQ (request.get<std::string> ("hash"), hash.to_string ());
	ASSERT_EQ (request.get<std::string> ("difficulty"), nano::to_string_hex (node1->default_difficulty (nano::work_version::work_1)));
	ASSERT_EQ (request.get<double> ("multiplier"), 1.0);

	ASSERT_EQ (1, contents.count ("result"));
	auto & result = contents.get_child ("result");
	uint64_t result_difficulty;
	nano::from_string_hex (result.get<std::string> ("difficulty"), result_difficulty);
	ASSERT_GE (result_difficulty, node1->default_difficulty (nano::work_version::work_1));
	ASSERT_NEAR (result.get<double> ("multiplier"), nano::difficulty::to_multiplier (result_difficulty, node1->default_difficulty (nano::work_version::work_1)), 1e-6);
	ASSERT_EQ (result.get<std::string> ("work"), nano::to_string_hex (work.get ()));

	ASSERT_EQ (1, contents.count ("bad_peers"));
	auto & bad_peers = contents.get_child ("bad_peers");
	ASSERT_TRUE (bad_peers.empty ());

	ASSERT_EQ (contents.get<std::string> ("reason"), "");
}

// Test client subscribing to notifications for bootstrap
TEST (websocket, bootstrap)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	ASSERT_EQ (0, node1->websocket_server->subscriber_count (nano::websocket::topic::bootstrap));

	// Subscribe to bootstrap and wait for response asynchronously
	std::atomic<bool> ack_ready{ false };
	auto task = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "bootstrap", "ack": true})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::bootstrap));
		return client.get_response ();
	});
	auto future = std::async (std::launch::async, task);

	// Wait for acknowledge
	ASSERT_TIMELY (5s, ack_ready);

	// Start bootstrap attempt
	node1->bootstrap_initiator.bootstrap (true, "123abc");
	ASSERT_TIMELY (5s, nullptr == node1->bootstrap_initiator.current_attempt ());

	// Wait for the bootstrap notification
	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);

	// Check the bootstrap notification message
	auto response = future.get ();
	ASSERT_TRUE (response);
	std::stringstream stream;
	stream << response;
	boost::property_tree::ptree event;
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "bootstrap");

	auto & contents = event.get_child ("message");
	ASSERT_EQ (contents.get<std::string> ("reason"), "started");
	ASSERT_EQ (contents.get<std::string> ("id"), "123abc");
	ASSERT_EQ (contents.get<std::string> ("mode"), "legacy");

	// Wait for bootstrap finish
	ASSERT_TIMELY (5s, !node1->bootstrap_initiator.in_progress ());
}

TEST (websocket, bootstrap_exited)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	// Start bootstrap, exit after subscription
	std::atomic<bool> bootstrap_started{ false };
	nano::util::counted_completion subscribed_completion (1);
	std::thread bootstrap_thread ([node1, &system, &bootstrap_started, &subscribed_completion] () {
		std::shared_ptr<nano::bootstrap_attempt> attempt;
		while (attempt == nullptr)
		{
			std::this_thread::sleep_for (50ms);
			node1->bootstrap_initiator.bootstrap (true, "123abc");
			attempt = node1->bootstrap_initiator.current_attempt ();
		}
		ASSERT_NE (nullptr, attempt);
		bootstrap_started = true;
		EXPECT_FALSE (subscribed_completion.await_count_for (5s));
	});

	// Wait for bootstrap start
	ASSERT_TIMELY (5s, bootstrap_started);

	// Subscribe to bootstrap and wait for response asynchronously
	std::atomic<bool> ack_ready{ false };
	auto task = ([&ack_ready, config, &node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "bootstrap", "ack": true})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::bootstrap));
		return client.get_response ();
	});
	auto future = std::async (std::launch::async, task);

	// Wait for acknowledge
	ASSERT_TIMELY (5s, ack_ready);

	// Wait for the bootstrap notification
	subscribed_completion.increment ();
	bootstrap_thread.join ();
	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);

	// Check the bootstrap notification message
	auto response = future.get ();
	ASSERT_TRUE (response);
	std::stringstream stream;
	stream << response;
	boost::property_tree::ptree event;
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "bootstrap");

	auto & contents = event.get_child ("message");
	ASSERT_EQ (contents.get<std::string> ("reason"), "exited");
	ASSERT_EQ (contents.get<std::string> ("id"), "123abc");
	ASSERT_EQ (contents.get<std::string> ("mode"), "legacy");
	ASSERT_EQ (contents.get<unsigned> ("total_blocks"), 0U);
	ASSERT_LT (contents.get<unsigned> ("duration"), 15000U);
}

// Tests sending keepalive
TEST (websocket, ws_keepalive)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	auto task = ([config] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "ping"})json");
		client.await_ack ();
	});
	auto future = std::async (std::launch::async, task);

	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);
}

// Tests sending telemetry
TEST (websocket, telemetry)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	nano::node_flags node_flags;
	node_flags.disable_initial_telemetry_requests = true;
	node_flags.disable_ongoing_telemetry_requests = true;
	auto node1 (system.add_node (config, node_flags));
	config.peering_port = nano::get_available_port ();
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node2 (system.add_node (config, node_flags));

	wait_peer_connections (system);

	std::atomic<bool> done{ false };
	auto task = ([config = node1->config, &node1, &done] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "telemetry", "ack": true})json");
		client.await_ack ();
		done = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::telemetry));
		return client.get_response ();
	});

	auto future = std::async (std::launch::async, task);

	ASSERT_TIMELY (10s, done);

	node1->telemetry->get_metrics_single_peer_async (node1->network.find_channel (node2->network.endpoint ()), [] (auto const & response_a) {
		ASSERT_FALSE (response_a.error);
	});

	ASSERT_TIMELY (10s, future.wait_for (0s) == std::future_status::ready);

	// Check the telemetry notification message
	auto response = future.get ();

	std::stringstream stream;
	stream << response;
	boost::property_tree::ptree event;
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "telemetry");

	auto & contents = event.get_child ("message");
	nano::jsonconfig telemetry_contents (contents);
	nano::telemetry_data telemetry_data;
	telemetry_data.deserialize_json (telemetry_contents, false);
	compare_default_telemetry_response_data (telemetry_data, node2->network_params, node2->config.bandwidth_limit, node2->default_difficulty (nano::work_version::work_1), node2->node_id);

	ASSERT_EQ (contents.get<std::string> ("address"), node2->network.endpoint ().address ().to_string ());
	ASSERT_EQ (contents.get<uint16_t> ("port"), node2->network.endpoint ().port ());

	// Other node should have no subscribers
	EXPECT_EQ (0, node2->websocket_server->subscriber_count (nano::websocket::topic::telemetry));
}

TEST (websocket, new_unconfirmed_block)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.websocket_config.enabled = true;
	config.websocket_config.port = nano::get_available_port ();
	auto node1 (system.add_node (config));

	std::atomic<bool> ack_ready{ false };
	auto task = ([&ack_ready, config, node1] () {
		fake_websocket_client client (config.websocket_config.port);
		client.send_message (R"json({"action": "subscribe", "topic": "new_unconfirmed_block", "ack": "true"})json");
		client.await_ack ();
		ack_ready = true;
		EXPECT_EQ (1, node1->websocket_server->subscriber_count (nano::websocket::topic::new_unconfirmed_block));
		return client.get_response ();
	});
	auto future = std::async (std::launch::async, task);

	ASSERT_TIMELY (5s, ack_ready);

	nano::state_block_builder builder;
	// Process a new block
	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - 1)
				 .link (nano::dev::genesis_key.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (nano::dev::genesis->hash ()))
				 .build_shared ();

	ASSERT_EQ (nano::process_result::progress, node1->process_local (send1).code);

	ASSERT_TIMELY (5s, future.wait_for (0s) == std::future_status::ready);

	// Check the response
	boost::optional<std::string> response = future.get ();
	ASSERT_TRUE (response);
	std::stringstream stream;
	stream << response;
	boost::property_tree::ptree event;
	boost::property_tree::read_json (stream, event);
	ASSERT_EQ (event.get<std::string> ("topic"), "new_unconfirmed_block");

	auto message_contents = event.get_child ("message");
	ASSERT_EQ ("state", message_contents.get<std::string> ("type"));
	ASSERT_EQ ("send", message_contents.get<std::string> ("subtype"));
}
