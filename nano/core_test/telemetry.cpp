#include <nano/node/telemetry.hpp>
#include <nano/node/testing.hpp>
#include <nano/test_common/telemetry.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

#include <numeric>

using namespace std::chrono_literals;

TEST (telemetry, consolidate_data)
{
	auto time = 1582117035109;

	// Pick specific values so that we can check both mode and average are working correctly
	nano::telemetry_data data;
	data.account_count = 2;
	data.block_count = 1;
	data.cemented_count = 1;
	data.protocol_version = 12;
	data.peer_count = 2;
	data.bandwidth_cap = 100;
	data.unchecked_count = 3;
	data.uptime = 6;
	data.genesis_block = nano::block_hash (3);
	data.major_version = 20;
	data.minor_version = 1;
	data.patch_version = 4;
	data.pre_release_version = 6;
	data.maker = 2;
	data.timestamp = std::chrono::system_clock::time_point (std::chrono::milliseconds (time));
	data.active_difficulty = 2;

	nano::telemetry_data data1;
	data1.account_count = 5;
	data1.block_count = 7;
	data1.cemented_count = 4;
	data1.protocol_version = 11;
	data1.peer_count = 5;
	data1.bandwidth_cap = 0;
	data1.unchecked_count = 1;
	data1.uptime = 10;
	data1.genesis_block = nano::block_hash (4);
	data1.major_version = 10;
	data1.minor_version = 2;
	data1.patch_version = 3;
	data1.pre_release_version = 6;
	data1.maker = 2;
	data1.timestamp = std::chrono::system_clock::time_point (std::chrono::milliseconds (time + 1));
	data1.active_difficulty = 3;

	nano::telemetry_data data2;
	data2.account_count = 3;
	data2.block_count = 3;
	data2.cemented_count = 2;
	data2.protocol_version = 11;
	data2.peer_count = 4;
	data2.bandwidth_cap = 0;
	data2.unchecked_count = 2;
	data2.uptime = 3;
	data2.genesis_block = nano::block_hash (4);
	data2.major_version = 20;
	data2.minor_version = 1;
	data2.patch_version = 4;
	data2.pre_release_version = 6;
	data2.maker = 2;
	data2.timestamp = std::chrono::system_clock::time_point (std::chrono::milliseconds (time));
	data2.active_difficulty = 2;

	std::vector<nano::telemetry_data> all_data{ data, data1, data2 };

	auto consolidated_telemetry_data = nano::consolidate_telemetry_data (all_data);
	ASSERT_EQ (consolidated_telemetry_data.account_count, 3);
	ASSERT_EQ (consolidated_telemetry_data.block_count, 3);
	ASSERT_EQ (consolidated_telemetry_data.cemented_count, 2);
	ASSERT_EQ (consolidated_telemetry_data.protocol_version, 11);
	ASSERT_EQ (consolidated_telemetry_data.peer_count, 3);
	ASSERT_EQ (consolidated_telemetry_data.bandwidth_cap, 0);
	ASSERT_EQ (consolidated_telemetry_data.unchecked_count, 2);
	ASSERT_EQ (consolidated_telemetry_data.uptime, 6);
	ASSERT_EQ (consolidated_telemetry_data.genesis_block, nano::block_hash (4));
	ASSERT_EQ (consolidated_telemetry_data.major_version, 20);
	ASSERT_EQ (consolidated_telemetry_data.minor_version, 1);
	ASSERT_EQ (consolidated_telemetry_data.patch_version, 4);
	ASSERT_EQ (consolidated_telemetry_data.pre_release_version, 6);
	ASSERT_EQ (consolidated_telemetry_data.maker, 2);
	ASSERT_EQ (consolidated_telemetry_data.timestamp, std::chrono::system_clock::time_point (std::chrono::milliseconds (time)));
	ASSERT_EQ (consolidated_telemetry_data.active_difficulty, 2);

	// Modify the metrics which may be either the mode or averages to ensure all are tested.
	all_data[2].bandwidth_cap = 53;
	all_data[2].protocol_version = 13;
	all_data[2].genesis_block = nano::block_hash (3);
	all_data[2].major_version = 10;
	all_data[2].minor_version = 2;
	all_data[2].patch_version = 3;
	all_data[2].pre_release_version = 6;
	all_data[2].maker = 2;

	auto consolidated_telemetry_data1 = nano::consolidate_telemetry_data (all_data);
	ASSERT_EQ (consolidated_telemetry_data1.major_version, 10);
	ASSERT_EQ (consolidated_telemetry_data1.minor_version, 2);
	ASSERT_EQ (consolidated_telemetry_data1.patch_version, 3);
	ASSERT_EQ (consolidated_telemetry_data1.pre_release_version, 6);
	ASSERT_EQ (consolidated_telemetry_data1.maker, 2);
	ASSERT_TRUE (consolidated_telemetry_data1.protocol_version == 11 || consolidated_telemetry_data1.protocol_version == 12 || consolidated_telemetry_data1.protocol_version == 13);
	ASSERT_EQ (consolidated_telemetry_data1.bandwidth_cap, 51);
	ASSERT_EQ (consolidated_telemetry_data1.genesis_block, nano::block_hash (3));

	// Test equality operator
	ASSERT_FALSE (consolidated_telemetry_data == consolidated_telemetry_data1);
	ASSERT_EQ (consolidated_telemetry_data, consolidated_telemetry_data);
}

TEST (telemetry, consolidate_data_remove_outliers)
{
	nano::telemetry_data data;
	data.account_count = 2;
	data.block_count = 1;
	data.cemented_count = 1;
	data.protocol_version = 12;
	data.peer_count = 2;
	data.bandwidth_cap = 100;
	data.unchecked_count = 3;
	data.uptime = 6;
	data.genesis_block = nano::block_hash (3);
	data.major_version = 20;
	data.minor_version = 1;
	data.patch_version = 5;
	data.pre_release_version = 2;
	data.maker = 1;
	data.timestamp = std::chrono::system_clock::time_point (100ms);
	data.active_difficulty = 10;

	// Insert 20 of these, and 2 outliers at the lower and upper bounds which should get removed
	std::vector<nano::telemetry_data> all_data (20, data);

	// Insert some outliers
	nano::telemetry_data lower_bound_outlier_data;
	lower_bound_outlier_data.account_count = 1;
	lower_bound_outlier_data.block_count = 0;
	lower_bound_outlier_data.cemented_count = 0;
	lower_bound_outlier_data.protocol_version = 11;
	lower_bound_outlier_data.peer_count = 0;
	lower_bound_outlier_data.bandwidth_cap = 8;
	lower_bound_outlier_data.unchecked_count = 1;
	lower_bound_outlier_data.uptime = 2;
	lower_bound_outlier_data.genesis_block = nano::block_hash (2);
	lower_bound_outlier_data.major_version = 11;
	lower_bound_outlier_data.minor_version = 1;
	lower_bound_outlier_data.patch_version = 1;
	lower_bound_outlier_data.pre_release_version = 1;
	lower_bound_outlier_data.maker = 1;
	lower_bound_outlier_data.timestamp = std::chrono::system_clock::time_point (1ms);
	lower_bound_outlier_data.active_difficulty = 1;
	all_data.push_back (lower_bound_outlier_data);
	all_data.push_back (lower_bound_outlier_data);

	nano::telemetry_data upper_bound_outlier_data;
	upper_bound_outlier_data.account_count = 99;
	upper_bound_outlier_data.block_count = 99;
	upper_bound_outlier_data.cemented_count = 99;
	upper_bound_outlier_data.protocol_version = 99;
	upper_bound_outlier_data.peer_count = 99;
	upper_bound_outlier_data.bandwidth_cap = 999;
	upper_bound_outlier_data.unchecked_count = 99;
	upper_bound_outlier_data.uptime = 999;
	upper_bound_outlier_data.genesis_block = nano::block_hash (99);
	upper_bound_outlier_data.major_version = 99;
	upper_bound_outlier_data.minor_version = 9;
	upper_bound_outlier_data.patch_version = 9;
	upper_bound_outlier_data.pre_release_version = 9;
	upper_bound_outlier_data.maker = 9;
	upper_bound_outlier_data.timestamp = std::chrono::system_clock::time_point (999ms);
	upper_bound_outlier_data.active_difficulty = 99;
	all_data.push_back (upper_bound_outlier_data);
	all_data.push_back (upper_bound_outlier_data);

	auto consolidated_telemetry_data = nano::consolidate_telemetry_data (all_data);
	ASSERT_EQ (data, consolidated_telemetry_data);
}

TEST (telemetry, signatures)
{
	nano::keypair node_id;
	nano::telemetry_data data;
	data.node_id = node_id.pub;
	data.major_version = 20;
	data.minor_version = 1;
	data.patch_version = 5;
	data.pre_release_version = 2;
	data.maker = 1;
	data.timestamp = std::chrono::system_clock::time_point (100ms);
	data.sign (node_id);
	ASSERT_FALSE (data.validate_signature (nano::telemetry_data::size));
	auto signature = data.signature;
	// Check that the signature is different if changing a piece of data
	data.maker = 2;
	data.sign (node_id);
	ASSERT_NE (data.signature, signature);
}

TEST (telemetry, no_peers)
{
	nano::system system (1);

	auto responses = system.nodes[0]->telemetry->get_metrics ();
	ASSERT_TRUE (responses.empty ());
}

TEST (telemetry, basic)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_ongoing_telemetry_requests = true;
	config.flags.disable_initial_telemetry_requests = true;
	auto node_client = system.add_node (config);
	auto node_server = system.add_node (config);

	wait_peer_connections (system);

	// Request telemetry metrics
	nano::telemetry_data telemetry_data;
	auto server_endpoint = node_server->network.endpoint ();
	auto channel = node_client->network.find_channel (node_server->network.endpoint ());
	{
		std::atomic<bool> done{ false };
		node_client->telemetry->get_metrics_single_peer_async (channel, [&done, &server_endpoint, &telemetry_data](nano::telemetry_data_response const & response_a) {
			ASSERT_FALSE (response_a.error);
			ASSERT_EQ (server_endpoint, response_a.endpoint);
			telemetry_data = response_a.telemetry_data;
			done = true;
		});

		ASSERT_TIMELY (10s, done);
	}

	// Check the metrics are correct
	nano::compare_default_telemetry_response_data (telemetry_data, node_server->env.constants, node_server->config.bandwidth_limit, node_server->active.active_difficulty (), node_server->node_id);

	// Call again straight away. It should use the cache
	{
		std::atomic<bool> done{ false };
		node_client->telemetry->get_metrics_single_peer_async (channel, [&done, &telemetry_data](nano::telemetry_data_response const & response_a) {
			ASSERT_EQ (telemetry_data, response_a.telemetry_data);
			ASSERT_FALSE (response_a.error);
			done = true;
		});

		ASSERT_TIMELY (10s, done);
	}

	// Wait the cache period and check cache is not used
	std::this_thread::sleep_for (nano::telemetry_cache_cutoffs::dev);

	std::atomic<bool> done{ false };
	node_client->telemetry->get_metrics_single_peer_async (channel, [&done, &telemetry_data](nano::telemetry_data_response const & response_a) {
		ASSERT_NE (telemetry_data, response_a.telemetry_data);
		ASSERT_FALSE (response_a.error);
		done = true;
	});

	ASSERT_TIMELY (10s, done);
}

TEST (telemetry, receive_from_non_listening_channel)
{
	nano::system system;
	auto node = system.add_node ();
	nano::telemetry_ack message (nano::telemetry_data{});
	node->network.process_message (message, node->network.udp_channels.create (node->network.endpoint ()));
	// We have not sent a telemetry_req message to this endpoint, so shouldn't count telemetry_ack received from it.
	ASSERT_EQ (node->telemetry->telemetry_data_size (), 0);
}

TEST (telemetry, over_udp)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_tcp_realtime = true;
	config.flags.disable_udp = false;
	auto node_client = system.add_node (config);
	auto node_server = system.add_node (config);

	wait_peer_connections (system);

	std::atomic<bool> done{ false };
	auto channel = node_client->network.find_channel (node_server->network.endpoint ());
	node_client->telemetry->get_metrics_single_peer_async (channel, [&done, &node_server](nano::telemetry_data_response const & response_a) {
		ASSERT_FALSE (response_a.error);
		nano::compare_default_telemetry_response_data (response_a.telemetry_data, node_server->env.constants, node_server->config.bandwidth_limit, node_server->active.active_difficulty (), node_server->node_id);
		done = true;
	});

	ASSERT_TIMELY (10s, done);

	// Check channels are indeed udp
	ASSERT_EQ (1, node_client->network.size ());
	auto list1 (node_client->network.list (2));
	ASSERT_EQ (node_server->network.endpoint (), list1[0]->get_endpoint ());
	ASSERT_EQ (nano::transport::transport_type::udp, list1[0]->get_type ());
	ASSERT_EQ (1, node_server->network.size ());
	auto list2 (node_server->network.list (2));
	ASSERT_EQ (node_client->network.endpoint (), list2[0]->get_endpoint ());
	ASSERT_EQ (nano::transport::transport_type::udp, list2[0]->get_type ());
}

TEST (telemetry, invalid_channel)
{
	nano::system system (2);

	auto node_client = system.nodes.front ();
	auto node_server = system.nodes.back ();

	std::atomic<bool> done{ false };
	node_client->telemetry->get_metrics_single_peer_async (nullptr, [&done](nano::telemetry_data_response const & response_a) {
		ASSERT_TRUE (response_a.error);
		done = true;
	});

	ASSERT_TIMELY (10s, done);
}

TEST (telemetry, blocking_request)
{
	nano::system system (2);

	auto node_client = system.nodes.front ();
	auto node_server = system.nodes.back ();

	wait_peer_connections (system);

	// Request telemetry metrics
	std::atomic<bool> done{ false };
	std::function<void()> call_system_poll;
	std::promise<void> promise;
	call_system_poll = [&call_system_poll, &worker = node_client->worker, &done, &system, &promise]() {
		if (!done)
		{
			ASSERT_NO_ERROR (system.poll ());
			worker.push_task (call_system_poll);
		}
		else
		{
			promise.set_value ();
		}
	};

	// Keep pushing system.polls in another thread (worker), because we will be blocking this thread and unable to do so.
	system.deadline_set (10s);
	node_client->worker.push_task (call_system_poll);

	// Now try single request metric
	auto telemetry_data_response = node_client->telemetry->get_metrics_single_peer (node_client->network.find_channel (node_server->network.endpoint ()));
	ASSERT_FALSE (telemetry_data_response.error);
	nano::compare_default_telemetry_response_data (telemetry_data_response.telemetry_data, node_server->env.constants, node_server->config.bandwidth_limit, node_server->active.active_difficulty (), node_server->node_id);

	done = true;
	promise.get_future ().wait ();
}

TEST (telemetry, disconnects)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_initial_telemetry_requests = true;
	auto node_client = system.add_node (config);
	auto node_server = system.add_node (config);

	wait_peer_connections (system);

	// Try and request metrics from a node which is turned off but a channel is not closed yet
	auto channel = node_client->network.find_channel (node_server->network.endpoint ());
	node_server->stop ();
	ASSERT_TRUE (channel);

	std::atomic<bool> done{ false };
	node_client->telemetry->get_metrics_single_peer_async (channel, [&done](nano::telemetry_data_response const & response_a) {
		ASSERT_TRUE (response_a.error);
		done = true;
	});

	ASSERT_TIMELY (10s, done);
}

TEST (telemetry, dos_tcp)
{
	// Confirm that telemetry_reqs are not processed
	nano::system system;
	nano::node_config config;
	config.flags.disable_initial_telemetry_requests = true;
	config.flags.disable_ongoing_telemetry_requests = true;
	auto node_client = system.add_node (config);
	auto node_server = system.add_node (config);

	wait_peer_connections (system);

	nano::telemetry_req message;
	auto channel = node_client->network.tcp_channels.find_channel (nano::transport::map_endpoint_to_tcp (node_server->network.endpoint ()));
	channel->send (message, [](boost::system::error_code const & ec, size_t size_a) {
		ASSERT_FALSE (ec);
	});

	ASSERT_TIMELY (10s, 1 == node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));

	auto orig = std::chrono::steady_clock::now ();
	for (int i = 0; i < 10; ++i)
	{
		channel->send (message, [](boost::system::error_code const & ec, size_t size_a) {
			ASSERT_FALSE (ec);
		});
	}

	ASSERT_TIMELY (10s, (nano::telemetry_cache_cutoffs::dev + orig) <= std::chrono::steady_clock::now ());

	// Should process no more telemetry_req messages
	ASSERT_EQ (1, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));

	// Now spam messages waiting for it to be processed
	while (node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in) == 1)
	{
		channel->send (message);
		ASSERT_NO_ERROR (system.poll ());
	}
}

TEST (telemetry, dos_udp)
{
	// Confirm that telemetry_reqs are not processed
	nano::system system;
	nano::node_config config;
	config.flags.disable_udp = false;
	config.flags.disable_tcp_realtime = true;
	config.flags.disable_initial_telemetry_requests = true;
	config.flags.disable_ongoing_telemetry_requests = true;
	auto node_client = system.add_node (config);
	auto node_server = system.add_node (config);

	wait_peer_connections (system);

	nano::telemetry_req message;
	auto channel (node_client->network.udp_channels.create (node_server->network.endpoint ()));
	channel->send (message, [](boost::system::error_code const & ec, size_t size_a) {
		ASSERT_FALSE (ec);
	});

	ASSERT_TIMELY (20s, 1 == node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));

	auto orig = std::chrono::steady_clock::now ();
	for (int i = 0; i < 10; ++i)
	{
		channel->send (message, [](boost::system::error_code const & ec, size_t size_a) {
			ASSERT_FALSE (ec);
		});
	}

	ASSERT_TIMELY (20s, (nano::telemetry_cache_cutoffs::dev + orig) <= std::chrono::steady_clock::now ());

	// Should process no more telemetry_req messages
	ASSERT_EQ (1, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));

	// Now spam messages waiting for it to be processed
	system.deadline_set (20s);
	while (node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in) == 1)
	{
		channel->send (message);
		ASSERT_NO_ERROR (system.poll ());
	}
}

TEST (telemetry, disable_metrics)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_initial_telemetry_requests = true;
	auto node_client = system.add_node (config);
	config.flags.disable_providing_telemetry_metrics = true;
	auto node_server = system.add_node (config);

	wait_peer_connections (system);

	// Try and request metrics from a node which is turned off but a channel is not closed yet
	auto channel = node_client->network.find_channel (node_server->network.endpoint ());
	ASSERT_TRUE (channel);

	std::atomic<bool> done{ false };
	node_client->telemetry->get_metrics_single_peer_async (channel, [&done](nano::telemetry_data_response const & response_a) {
		ASSERT_TRUE (response_a.error);
		done = true;
	});

	ASSERT_TIMELY (10s, done);

	// It should still be able to receive metrics though
	done = false;
	auto channel1 = node_server->network.find_channel (node_client->network.endpoint ());
	node_server->telemetry->get_metrics_single_peer_async (channel1, [&done, node_client](nano::telemetry_data_response const & response_a) {
		ASSERT_FALSE (response_a.error);
		nano::compare_default_telemetry_response_data (response_a.telemetry_data, node_client->env.constants, node_client->config.bandwidth_limit, node_client->active.active_difficulty (), node_client->node_id);
		done = true;
	});

	ASSERT_TIMELY (10s, done);
}

namespace nano
{
TEST (telemetry, remove_peer_different_genesis)
{
	nano::system system0 (1);
	auto node0 (system0.nodes[0]);
	ASSERT_EQ (0, node0->network.size ());
	nano::system system1;
	// Change genesis block to something else in this test (this is the reference telemetry processing uses).
	// Possible TSAN issue in the future if something else uses this, but will only appear in tests.
	system1.env.constants.ledger.genesis_hash = nano::block_hash ("0");
	auto node1 (std::make_shared<nano::node> (system1.env, nano::node_config{}));
	ASSERT_NE (node0->env.constants.ledger.genesis_hash, node1->env.constants.ledger.genesis_hash);
	node1->start ();
	system0.nodes.push_back (node1);
	node0->network.merge_peer (node1->network.endpoint ());
	node1->network.merge_peer (node0->network.endpoint ());
	system0.deadline_set (10s);
	system1.deadline_set (10s);
	while (!(node0->stats.count (nano::stat::type::telemetry, nano::stat::detail::different_genesis_hash) != 0 && node1->stats.count (nano::stat::type::telemetry, nano::stat::detail::different_genesis_hash) != 0))
	{
		ASSERT_NO_ERROR (system0.poll ());
		ASSERT_NO_ERROR (system1.poll ());
	}

	system0.deadline_set (1s);
	system1.deadline_set (1s);
	while (0 != node0->network.size ())
	{
		ASSERT_NO_ERROR (system0.poll ());
		ASSERT_NO_ERROR (system1.poll ());
	}
	system0.deadline_set (1s);
	system1.deadline_set (1s);
	while (0 != node1->network.size ())
	{
		ASSERT_NO_ERROR (system0.poll ());
		ASSERT_NO_ERROR (system1.poll ());
	}
	ASSERT_EQ (node0->stats.count (nano::stat::type::message, nano::stat::detail::node_id_handshake, nano::stat::dir::out), 1);
	ASSERT_EQ (node1->stats.count (nano::stat::type::message, nano::stat::detail::node_id_handshake, nano::stat::dir::out), 1);

	nano::lock_guard<std::mutex> guard (node0->network.excluded_peers.mutex);
	ASSERT_EQ (1, node0->network.excluded_peers.peers.get<nano::peer_exclusion::tag_endpoint> ().count (node1->network.endpoint ().address ()));
	ASSERT_EQ (1, node1->network.excluded_peers.peers.get<nano::peer_exclusion::tag_endpoint> ().count (node0->network.endpoint ().address ()));
}

// Peer exclusion is only fully supported for TCP-only nodes; peers can still reconnect through UDP
TEST (telemetry, remove_peer_different_genesis_udp)
{
	nano::node_config config;
	config.flags.disable_udp = false;
	config.flags.disable_tcp_realtime = true;
	config.flags.disable_ongoing_telemetry_requests = true;
	nano::system system0 (1, nano::transport::transport_type::udp, config);
	auto node0 (system0.nodes[0]);
	ASSERT_EQ (0, node0->network.size ());
	nano::system system1;
	auto node1 (std::make_shared<nano::node> (system1.env, config));
	node1->env.constants.ledger.genesis_hash = nano::block_hash ("0");
	node1->start ();
	system1.nodes.push_back (node1);
	auto channel0 (std::make_shared<nano::transport::channel_udp> (node1->network.udp_channels, node0->network.endpoint (), node0->env.constants.protocol.protocol_version));
	auto channel1 (std::make_shared<nano::transport::channel_udp> (node0->network.udp_channels, node1->network.endpoint (), node1->env.constants.protocol.protocol_version));
	node0->network.send_keepalive (channel1);
	node1->network.send_keepalive (channel0);

	system0.deadline_set (10s);
	system1.deadline_set (10s);
	while (!(node0->network.udp_channels.size () != 0 && node1->network.udp_channels.size () != 0))
	{
		ASSERT_NO_ERROR (system0.poll ());
		ASSERT_NO_ERROR (system1.poll ());
	}
	ASSERT_EQ (node0->network.tcp_channels.size (), 0);
	ASSERT_EQ (node1->network.tcp_channels.size (), 0);

	std::atomic<bool> done0{ false };
	std::atomic<bool> done1{ false };

	node0->telemetry->get_metrics_single_peer_async (channel1, [&done0](nano::telemetry_data_response const & response_a) {
		done0 = true;
	});

	node1->telemetry->get_metrics_single_peer_async (channel0, [&done1](nano::telemetry_data_response const & response_a) {
		done1 = true;
	});

	system0.deadline_set (10s);
	system1.deadline_set (10s);
	while (!(done0 && done1))
	{
		ASSERT_NO_ERROR (system0.poll ());
		ASSERT_NO_ERROR (system1.poll ());
	}

	ASSERT_EQ (node0->network.tcp_channels.size (), 0);
	ASSERT_EQ (node1->network.tcp_channels.size (), 0);

	nano::lock_guard<std::mutex> guard (node0->network.excluded_peers.mutex);
	ASSERT_EQ (1, node0->network.excluded_peers.peers.get<nano::peer_exclusion::tag_endpoint> ().count (node1->network.endpoint ().address ()));
	ASSERT_EQ (1, node1->network.excluded_peers.peers.get<nano::peer_exclusion::tag_endpoint> ().count (node0->network.endpoint ().address ()));
}

TEST (telemetry, remove_peer_invalid_signature)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_udp = false;
	config.flags.disable_initial_telemetry_requests = true;
	config.flags.disable_ongoing_telemetry_requests = true;
	auto node = system.add_node (config);

	auto channel = node->network.udp_channels.create (node->network.endpoint ());
	channel->set_node_id (node->node_id.pub);
	// (Implementation detail) So that messages are not just discarded when requests were not sent.
	node->telemetry->recent_or_initial_request_telemetry_data.emplace (channel->get_endpoint (), nano::telemetry_data (), std::chrono::steady_clock::now (), true);

	auto telemetry_data = nano::local_telemetry_data (node->store, node->ledger.cache, node->network, node->config.bandwidth_limit, node->env.constants, node->startup_time, node->active.active_difficulty (), node->node_id);
	// Change anything so that the signed message is incorrect
	telemetry_data.block_count = 0;
	auto telemetry_ack = nano::telemetry_ack (telemetry_data);
	node->network.process_message (telemetry_ack, channel);

	ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::telemetry, nano::stat::detail::invalid_signature) > 0);
	ASSERT_NO_ERROR (system.poll_until_true (3s, [&node, address = channel->get_endpoint ().address ()]() -> bool {
		nano::lock_guard<std::mutex> guard (node->network.excluded_peers.mutex);
		return node->network.excluded_peers.peers.get<nano::peer_exclusion::tag_endpoint> ().count (address);
	}));
}
}
