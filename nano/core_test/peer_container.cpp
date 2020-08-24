#include <nano/node/testing.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

TEST (peer_container, empty_peers)
{
	nano::system system (1);
	nano::network & network (system.nodes[0]->network);
	system.nodes[0]->network.cleanup (std::chrono::steady_clock::now ());
	ASSERT_EQ (0, network.size ());
}

TEST (peer_container, no_recontact)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::network & network (node1.network);
	auto observed_peer (0);
	auto observed_disconnect (false);
	nano::endpoint endpoint1 (boost::asio::ip::address_v6::loopback (), 10000);
	ASSERT_EQ (0, network.size ());
	network.channel_observer = [&observed_peer](std::shared_ptr<nano::transport::channel>) { ++observed_peer; };
	node1.network.disconnect_observer = [&observed_disconnect]() { observed_disconnect = true; };
	auto channel (network.udp_channels.insert (endpoint1, node1.env.constants.protocol.protocol_version));
	ASSERT_EQ (1, network.size ());
	ASSERT_EQ (channel, network.udp_channels.insert (endpoint1, node1.env.constants.protocol.protocol_version));
	node1.network.cleanup (std::chrono::steady_clock::now () + std::chrono::seconds (5));
	ASSERT_TRUE (network.empty ());
	ASSERT_EQ (1, observed_peer);
	ASSERT_TRUE (observed_disconnect);
}

TEST (peer_container, no_self_incoming)
{
	nano::system system (1);
	ASSERT_EQ (nullptr, system.nodes[0]->network.udp_channels.insert (system.nodes[0]->network.endpoint (), 0));
	ASSERT_TRUE (system.nodes[0]->network.empty ());
}

TEST (peer_container, reserved_peers_no_contact)
{
	nano::system system (1);
	auto & channels (system.nodes[0]->network.udp_channels);
	ASSERT_EQ (nullptr, channels.insert (nano::endpoint (boost::asio::ip::address_v6::v4_mapped (boost::asio::ip::address_v4 (0x00000001)), 10000), 0));
	ASSERT_EQ (nullptr, channels.insert (nano::endpoint (boost::asio::ip::address_v6::v4_mapped (boost::asio::ip::address_v4 (0xc0000201)), 10000), 0));
	ASSERT_EQ (nullptr, channels.insert (nano::endpoint (boost::asio::ip::address_v6::v4_mapped (boost::asio::ip::address_v4 (0xc6336401)), 10000), 0));
	ASSERT_EQ (nullptr, channels.insert (nano::endpoint (boost::asio::ip::address_v6::v4_mapped (boost::asio::ip::address_v4 (0xcb007101)), 10000), 0));
	ASSERT_EQ (nullptr, channels.insert (nano::endpoint (boost::asio::ip::address_v6::v4_mapped (boost::asio::ip::address_v4 (0xe9fc0001)), 10000), 0));
	ASSERT_EQ (nullptr, channels.insert (nano::endpoint (boost::asio::ip::address_v6::v4_mapped (boost::asio::ip::address_v4 (0xf0000001)), 10000), 0));
	ASSERT_EQ (nullptr, channels.insert (nano::endpoint (boost::asio::ip::address_v6::v4_mapped (boost::asio::ip::address_v4 (0xffffffff)), 10000), 0));
	ASSERT_EQ (0, system.nodes[0]->network.size ());
}

TEST (peer_container, split)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	auto now (std::chrono::steady_clock::now ());
	nano::endpoint endpoint1 (boost::asio::ip::address_v6::loopback (), 100);
	nano::endpoint endpoint2 (boost::asio::ip::address_v6::loopback (), 101);
	auto channel1 (node1.network.udp_channels.insert (endpoint1, 0));
	ASSERT_NE (nullptr, channel1);
	node1.network.udp_channels.modify (channel1, [&now](auto channel) {
		channel->set_last_packet_received (now - std::chrono::seconds (1));
	});
	auto channel2 (node1.network.udp_channels.insert (endpoint2, 0));
	ASSERT_NE (nullptr, channel2);
	node1.network.udp_channels.modify (channel2, [&now](auto channel) {
		channel->set_last_packet_received (now + std::chrono::seconds (1));
	});
	ASSERT_EQ (2, node1.network.size ());
	ASSERT_EQ (2, node1.network.udp_channels.size ());
	node1.network.cleanup (now);
	ASSERT_EQ (1, node1.network.size ());
	ASSERT_EQ (1, node1.network.udp_channels.size ());
	auto list (node1.network.list (1));
	ASSERT_EQ (endpoint2, list[0]->get_endpoint ());
}

TEST (channels, fill_random_clear)
{
	nano::system system (1);
	std::array<nano::endpoint, 8> target;
	std::fill (target.begin (), target.end (), nano::endpoint (boost::asio::ip::address_v6::loopback (), 10000));
	system.nodes[0]->network.random_fill (target);
	ASSERT_TRUE (std::all_of (target.begin (), target.end (), [](nano::endpoint const & endpoint_a) { return endpoint_a == nano::endpoint (boost::asio::ip::address_v6::any (), 0); }));
}

TEST (channels, fill_random_full)
{
	nano::system system (1);
	for (uint16_t i (0u); i < 100u; ++i)
	{
		system.nodes[0]->network.udp_channels.insert (nano::endpoint (boost::asio::ip::address_v6::loopback (), i), 0);
	}
	std::array<nano::endpoint, 8> target;
	std::fill (target.begin (), target.end (), nano::endpoint (boost::asio::ip::address_v6::loopback (), 10000));
	system.nodes[0]->network.random_fill (target);
	ASSERT_TRUE (std::none_of (target.begin (), target.end (), [](nano::endpoint const & endpoint_a) { return endpoint_a == nano::endpoint (boost::asio::ip::address_v6::loopback (), 10000); }));
}

TEST (channels, fill_random_part)
{
	nano::system system (1);
	std::array<nano::endpoint, 8> target;
	auto half (target.size () / 2);
	for (auto i (0); i < half; ++i)
	{
		system.nodes[0]->network.udp_channels.insert (nano::endpoint (boost::asio::ip::address_v6::loopback (), i + 1), 0);
	}
	std::fill (target.begin (), target.end (), nano::endpoint (boost::asio::ip::address_v6::loopback (), 10000));
	system.nodes[0]->network.random_fill (target);
	ASSERT_TRUE (std::none_of (target.begin (), target.begin () + half, [](nano::endpoint const & endpoint_a) { return endpoint_a == nano::endpoint (boost::asio::ip::address_v6::loopback (), 10000); }));
	ASSERT_TRUE (std::none_of (target.begin (), target.begin () + half, [](nano::endpoint const & endpoint_a) { return endpoint_a == nano::endpoint (boost::asio::ip::address_v6::loopback (), 0); }));
	ASSERT_TRUE (std::all_of (target.begin () + half, target.end (), [](nano::endpoint const & endpoint_a) { return endpoint_a == nano::endpoint (boost::asio::ip::address_v6::any (), 0); }));
}

TEST (peer_container, list_fanout)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	ASSERT_EQ (0, node.network.size ());
	ASSERT_EQ (0.0, node.network.size_sqrt ());
	ASSERT_EQ (0, node.network.fanout ());
	auto list1 (node.network.list (node.network.fanout ()));
	ASSERT_TRUE (list1.empty ());
	auto add_peer = [&node](const uint16_t port_a) {
		ASSERT_NE (nullptr, node.network.udp_channels.insert (nano::endpoint (boost::asio::ip::address_v6::loopback (), port_a), node.env.constants.protocol.protocol_version));
	};
	add_peer (9998);
	ASSERT_EQ (1, node.network.size ());
	ASSERT_EQ (1.f, node.network.size_sqrt ());
	ASSERT_EQ (1, node.network.fanout ());
	auto list2 (node.network.list (node.network.fanout ()));
	ASSERT_EQ (1, list2.size ());
	add_peer (9999);
	ASSERT_EQ (2, node.network.size ());
	ASSERT_EQ (std::sqrt (2.f), node.network.size_sqrt ());
	ASSERT_EQ (2, node.network.fanout ());
	auto list3 (node.network.list (node.network.fanout ()));
	ASSERT_EQ (2, list3.size ());
	for (auto i (0); i < 1000; ++i)
	{
		add_peer (10000 + i);
	}
	ASSERT_EQ (1002, node.network.size ());
	ASSERT_EQ (std::sqrt (1002.f), node.network.size_sqrt ());
	size_t expected_size (std::ceil (std::sqrt (1002.f)));
	ASSERT_EQ (expected_size, node.network.fanout ());
	auto list4 (node.network.list (node.network.fanout ()));
	ASSERT_EQ (expected_size, list4.size ());
}

// Test to make sure we don't repeatedly send keepalive messages to nodes that aren't responding
TEST (peer_container, reachout)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_udp = false;
	auto & node1 = *system.add_node (config);
	nano::endpoint endpoint0 (boost::asio::ip::address_v6::loopback (), nano::get_available_port ());
	// Make sure having been contacted by them already indicates we shouldn't reach out
	node1.network.udp_channels.insert (endpoint0, node1.env.constants.protocol.protocol_version);
	ASSERT_TRUE (node1.network.reachout (endpoint0));
	nano::endpoint endpoint1 (boost::asio::ip::address_v6::loopback (), nano::get_available_port ());
	ASSERT_FALSE (node1.network.reachout (endpoint1));
	// Reaching out to them once should signal we shouldn't reach out again.
	ASSERT_TRUE (node1.network.reachout (endpoint1));
	// Make sure we don't purge new items
	node1.network.cleanup (std::chrono::steady_clock::now () - std::chrono::seconds (10));
	ASSERT_TRUE (node1.network.reachout (endpoint1));
	// Make sure we purge old items
	node1.network.cleanup (std::chrono::steady_clock::now () + std::chrono::seconds (10));
	ASSERT_FALSE (node1.network.reachout (endpoint1));
}

TEST (peer_container, depeer)
{
	nano::system system (1);
	nano::endpoint endpoint0 (boost::asio::ip::address_v6::loopback (), nano::get_available_port ());
	nano::keepalive message;
	message.header.version_using = 1;
	auto bytes (message.to_bytes (false));
	nano::message_buffer buffer = { bytes->data (), bytes->size (), endpoint0 };
	system.nodes[0]->network.udp_channels.receive_action (&buffer);
	ASSERT_EQ (1, system.nodes[0]->stats.count (nano::stat::type::udp, nano::stat::detail::outdated_version));
}
