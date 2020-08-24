#include <nano/lib/jsonconfig.hpp>
#include <nano/node/confirmation_solicitor.hpp>
#include <nano/node/testing.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

TEST (confirmation_solicitor, batches)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_request_loop = true;
	config.flags.disable_rep_crawler = true;
	config.flags.disable_udp = false;
	auto & node1 = *system.add_node (config);
	config.flags.disable_request_loop = true;
	auto & node2 = *system.add_node (config);
	auto channel1 (node2.network.udp_channels.create (node1.network.endpoint ()));
	// Solicitor will only solicit from this representative
	nano::representative representative (nano::dev_genesis_key.pub, nano::genesis_amount, channel1);
	std::vector<nano::representative> representatives{ representative };
	nano::confirmation_solicitor solicitor (node2.network, node2.env.constants.network);
	solicitor.prepare (representatives);
	// Ensure the representatives are correct
	ASSERT_EQ (1, representatives.size ());
	ASSERT_EQ (channel1, representatives.front ().channel);
	ASSERT_EQ (nano::dev_genesis_key.pub, representatives.front ().account);
	ASSERT_TIMELY (3s, node2.network.size () == 1);
	auto send (std::make_shared<nano::send_block> (nano::genesis_hash, nano::keypair ().pub, nano::genesis_amount - 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (nano::genesis_hash)));
	send->sideband_set ({});
	{
		nano::lock_guard<std::mutex> guard (node2.active.mutex);
		for (size_t i (0); i < nano::network::confirm_req_hashes_max; ++i)
		{
			auto election (std::make_shared<nano::election> (node2, send, nullptr, false));
			ASSERT_FALSE (solicitor.add (*election));
		}
		ASSERT_EQ (1, solicitor.max_confirm_req_batches);
		// Reached the maximum amount of requests for the channel
		auto election (std::make_shared<nano::election> (node2, send, nullptr, false));
		ASSERT_TRUE (solicitor.add (*election));
		// Broadcasting should be immediate
		ASSERT_EQ (0, node2.stats.count (nano::stat::type::message, nano::stat::detail::publish, nano::stat::dir::out));
		ASSERT_FALSE (solicitor.broadcast (*election));
	}
	// One publish through directed broadcasting and another through random flooding
	ASSERT_EQ (2, node2.stats.count (nano::stat::type::message, nano::stat::detail::publish, nano::stat::dir::out));
	solicitor.flush ();
	ASSERT_EQ (1, node2.stats.count (nano::stat::type::message, nano::stat::detail::confirm_req, nano::stat::dir::out));
}

TEST (confirmation_solicitor, different_hash)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_request_loop = true;
	config.flags.disable_rep_crawler = true;
	config.flags.disable_udp = false;
	auto & node1 = *system.add_node (config);
	auto & node2 = *system.add_node (config);
	auto channel1 (node2.network.udp_channels.create (node1.network.endpoint ()));
	// Solicitor will only solicit from this representative
	nano::representative representative (nano::dev_genesis_key.pub, nano::genesis_amount, channel1);
	std::vector<nano::representative> representatives{ representative };
	nano::confirmation_solicitor solicitor (node2.network, node2.env.constants.network);
	solicitor.prepare (representatives);
	// Ensure the representatives are correct
	ASSERT_EQ (1, representatives.size ());
	ASSERT_EQ (channel1, representatives.front ().channel);
	ASSERT_EQ (nano::dev_genesis_key.pub, representatives.front ().account);
	ASSERT_TIMELY (3s, node2.network.size () == 1);
	auto send (std::make_shared<nano::send_block> (nano::genesis_hash, nano::keypair ().pub, nano::genesis_amount - 100, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (nano::genesis_hash)));
	send->sideband_set ({});
	{
		nano::lock_guard<std::mutex> guard (node2.active.mutex);
		auto election (std::make_shared<nano::election> (node2, send, nullptr, false));
		// Add a vote for something else, not the winner
		election->last_votes[representative.account] = { std::chrono::steady_clock::now (), 1, 1 };
		// Ensure the request and broadcast goes through
		ASSERT_FALSE (solicitor.add (*election));
		ASSERT_FALSE (solicitor.broadcast (*election));
	}
	// One publish through directed broadcasting and another through random flooding
	ASSERT_EQ (2, node2.stats.count (nano::stat::type::message, nano::stat::detail::publish, nano::stat::dir::out));
	solicitor.flush ();
	ASSERT_EQ (1, node2.stats.count (nano::stat::type::message, nano::stat::detail::confirm_req, nano::stat::dir::out));
}
