#include <nano/lib/jsonconfig.hpp>
#include <nano/node/testing.hpp>
#include <nano/node/vote_processor.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

TEST (vote_processor, codes)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair key;
	auto vote (std::make_shared<nano::vote> (key.pub, key.prv, 1, std::vector<nano::block_hash>{ genesis.open->hash () }));
	auto vote_invalid = std::make_shared<nano::vote> (*vote);
	vote_invalid->signature.bytes[0] ^= 1;
	auto channel (std::make_shared<nano::transport::channel_udp> (node.network.udp_channels, node.network.endpoint (), node.env.constants.protocol.protocol_version));

	// Invalid signature
	ASSERT_EQ (nano::vote_code::invalid, node.vote_processor.vote_blocking (vote_invalid, channel, false));

	// Hint of pre-validation
	ASSERT_NE (nano::vote_code::invalid, node.vote_processor.vote_blocking (vote_invalid, channel, true));

	// No ongoing election
	ASSERT_EQ (nano::vote_code::indeterminate, node.vote_processor.vote_blocking (vote, channel));

	// First vote from an account for an ongoing election
	genesis.open->sideband_set (nano::block_sideband (nano::genesis_account, 0, nano::genesis_amount, 1, nano::seconds_since_epoch (), nano::epoch::epoch_0, false, false, false, nano::epoch::epoch_0));
	ASSERT_TRUE (node.active.insert (genesis.open).inserted);
	ASSERT_EQ (nano::vote_code::vote, node.vote_processor.vote_blocking (vote, channel));

	// Processing the same vote is a replay
	ASSERT_EQ (nano::vote_code::replay, node.vote_processor.vote_blocking (vote, channel));

	// Invalid takes precedence
	ASSERT_EQ (nano::vote_code::invalid, node.vote_processor.vote_blocking (vote_invalid, channel));

	// A higher sequence is not a replay
	++vote->sequence;
	ASSERT_EQ (nano::vote_code::invalid, node.vote_processor.vote_blocking (vote, channel));
	vote->signature = nano::sign_message (key.prv, key.pub, vote->hash ());
	ASSERT_EQ (nano::vote_code::vote, node.vote_processor.vote_blocking (vote, channel));

	// Once the election is removed (confirmed / dropped) the vote is again indeterminate
	node.active.erase (*genesis.open);
	ASSERT_EQ (nano::vote_code::indeterminate, node.vote_processor.vote_blocking (vote, channel));
}

TEST (vote_processor, flush)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	nano::genesis genesis;
	auto vote (std::make_shared<nano::vote> (nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, 1, std::vector<nano::block_hash>{ genesis.open->hash () }));
	auto channel (std::make_shared<nano::transport::channel_udp> (node.network.udp_channels, node.network.endpoint (), node.env.constants.protocol.protocol_version));
	for (unsigned i = 0; i < 2000; ++i)
	{
		auto new_vote (std::make_shared<nano::vote> (*vote));
		node.vote_processor.vote (new_vote, channel);
		++vote->sequence; // invalidates votes without signing again
	}
	node.vote_processor.flush ();
	ASSERT_TRUE (node.vote_processor.empty ());
}

TEST (vote_processor, invalid_signature)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair key;
	auto vote (std::make_shared<nano::vote> (key.pub, key.prv, 1, std::vector<nano::block_hash>{ genesis.open->hash () }));
	auto vote_invalid = std::make_shared<nano::vote> (*vote);
	vote_invalid->signature.bytes[0] ^= 1;
	auto channel (std::make_shared<nano::transport::channel_udp> (node.network.udp_channels, node.network.endpoint (), node.env.constants.protocol.protocol_version));

	genesis.open->sideband_set (nano::block_sideband (nano::genesis_account, 0, nano::genesis_amount, 1, nano::seconds_since_epoch (), nano::epoch::epoch_0, false, false, false, nano::epoch::epoch_0));
	auto election (node.active.insert (genesis.open));
	ASSERT_TRUE (election.election && election.inserted);
	ASSERT_EQ (1, election.election->last_votes.size ());
	node.vote_processor.vote (vote_invalid, channel);
	node.vote_processor.flush ();
	ASSERT_EQ (1, election.election->last_votes.size ());
	node.vote_processor.vote (vote, channel);
	node.vote_processor.flush ();
	ASSERT_EQ (2, election.election->last_votes.size ());
}

TEST (vote_processor, no_capacity)
{
	nano::system system;
	nano::node_config config;
	config.flags.vote_processor_capacity = 0;
	auto & node (*system.add_node (config));
	nano::genesis genesis;
	nano::keypair key;
	auto vote (std::make_shared<nano::vote> (key.pub, key.prv, 1, std::vector<nano::block_hash>{ genesis.open->hash () }));
	auto channel (std::make_shared<nano::transport::channel_udp> (node.network.udp_channels, node.network.endpoint (), node.env.constants.protocol.protocol_version));
	ASSERT_TRUE (node.vote_processor.vote (vote, channel));
}

TEST (vote_processor, overflow)
{
	nano::system system;
	nano::node_config config;
	config.flags.vote_processor_capacity = 1;
	auto & node (*system.add_node (config));
	nano::genesis genesis;
	nano::keypair key;
	auto vote (std::make_shared<nano::vote> (key.pub, key.prv, 1, std::vector<nano::block_hash>{ genesis.open->hash () }));
	auto channel (std::make_shared<nano::transport::channel_udp> (node.network.udp_channels, node.network.endpoint (), node.env.constants.protocol.protocol_version));

	// No way to lock the processor, but queueing votes in quick succession must result in overflow
	size_t not_processed{ 0 };
	size_t const total{ 1000 };
	for (unsigned i = 0; i < total; ++i)
	{
		if (node.vote_processor.vote (vote, channel))
		{
			++not_processed;
		}
	}
	ASSERT_GT (not_processed, 0);
	ASSERT_LT (not_processed, total);
	ASSERT_EQ (not_processed, node.stats.count (nano::stat::type::vote, nano::stat::detail::vote_overflow));
}

namespace nano
{
TEST (vote_processor, weights)
{
	nano::system system (4);
	auto & node (*system.nodes[0]);

	// Create representatives of different weight levels
	// The online stake will be the minimum configurable due to online_reps sampling in tests
	auto const online = node.config.online_weight_minimum.number ();
	auto const level0 = online / 5000; // 0.02%
	auto const level1 = online / 500; // 0.2%
	auto const level2 = online / 50; // 2%

	nano::keypair key0;
	nano::keypair key1;
	nano::keypair key2;

	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (1)->insert_adhoc (key0.prv);
	system.wallet (2)->insert_adhoc (key1.prv);
	system.wallet (3)->insert_adhoc (key2.prv);
	system.wallet (1)->store.representative_set (system.nodes[1]->wallets.tx_begin_write (), key0.pub);
	system.wallet (2)->store.representative_set (system.nodes[2]->wallets.tx_begin_write (), key1.pub);
	system.wallet (3)->store.representative_set (system.nodes[3]->wallets.tx_begin_write (), key2.pub);
	system.wallet (0)->send_sync (nano::dev_genesis_key.pub, key0.pub, level0);
	system.wallet (0)->send_sync (nano::dev_genesis_key.pub, key1.pub, level1);
	system.wallet (0)->send_sync (nano::dev_genesis_key.pub, key2.pub, level2);

	// Wait for representatives
	ASSERT_TIMELY (10s, node.ledger.cache.rep_weights.get_rep_amounts ().size () == 4);
	node.vote_processor.calculate_weights ();

	ASSERT_EQ (node.vote_processor.representatives_1.end (), node.vote_processor.representatives_1.find (key0.pub));
	ASSERT_EQ (node.vote_processor.representatives_2.end (), node.vote_processor.representatives_2.find (key0.pub));
	ASSERT_EQ (node.vote_processor.representatives_3.end (), node.vote_processor.representatives_3.find (key0.pub));

	ASSERT_NE (node.vote_processor.representatives_1.end (), node.vote_processor.representatives_1.find (key1.pub));
	ASSERT_EQ (node.vote_processor.representatives_2.end (), node.vote_processor.representatives_2.find (key1.pub));
	ASSERT_EQ (node.vote_processor.representatives_3.end (), node.vote_processor.representatives_3.find (key1.pub));

	ASSERT_NE (node.vote_processor.representatives_1.end (), node.vote_processor.representatives_1.find (key2.pub));
	ASSERT_NE (node.vote_processor.representatives_2.end (), node.vote_processor.representatives_2.find (key2.pub));
	ASSERT_EQ (node.vote_processor.representatives_3.end (), node.vote_processor.representatives_3.find (key2.pub));

	ASSERT_NE (node.vote_processor.representatives_1.end (), node.vote_processor.representatives_1.find (nano::dev_genesis_key.pub));
	ASSERT_NE (node.vote_processor.representatives_2.end (), node.vote_processor.representatives_2.find (nano::dev_genesis_key.pub));
	ASSERT_NE (node.vote_processor.representatives_3.end (), node.vote_processor.representatives_3.find (nano::dev_genesis_key.pub));
}
}

TEST (vote_processor, no_broadcast_local)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_request_loop = true;
	auto & node (*system.add_node (config));
	system.add_node (config);
	nano::block_builder builder;
	std::error_code ec;
	// Reduce the weight of genesis to 2x default min voting weight
	nano::keypair key;
	std::shared_ptr<nano::block> send = builder.state ()
	                                    .account (nano::dev_genesis_key.pub)
	                                    .representative (nano::dev_genesis_key.pub)
	                                    .previous (nano::genesis_hash)
	                                    .balance (2 * node.config.vote_minimum.number ())
	                                    .link (key.pub)
	                                    .sign (nano::dev_genesis_key.prv, nano::dev_genesis_key.pub)
	                                    .work (*system.env.work.generate (nano::genesis_hash))
	                                    .build (ec);
	ASSERT_FALSE (ec);
	ASSERT_EQ (nano::process_result::progress, node.process_local (send).code);
	ASSERT_EQ (2 * node.config.vote_minimum.number (), node.weight (nano::dev_genesis_key.pub));
	// Insert account in wallet
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	node.wallets.compute_reps ();
	ASSERT_TRUE (node.wallets.reps ().exists (nano::dev_genesis_key.pub));
	ASSERT_FALSE (node.wallets.reps ().have_half_rep ());
	// Process a vote
	auto vote (node.store.vote_generate (node.store.tx_begin_read (), nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, { send->hash () }));
	ASSERT_EQ (nano::vote_code::vote, node.active.vote (vote));
	// Make sure the vote was processed
	auto election (node.active.election (send->qualified_root ()));
	ASSERT_NE (nullptr, election);
	auto existing (election->last_votes.find (nano::dev_genesis_key.pub));
	ASSERT_NE (election->last_votes.end (), existing);
	ASSERT_EQ (vote->sequence, existing->second.sequence);
	// Ensure the vote, from a local representative, was not broadcast on processing - it should be flooded on generation instead
	ASSERT_EQ (0, node.stats.count (nano::stat::type::message, nano::stat::detail::confirm_ack, nano::stat::dir::out));
	ASSERT_EQ (1, node.stats.count (nano::stat::type::message, nano::stat::detail::publish, nano::stat::dir::out));

	// Repeat test with no representative
	// Erase account from the wallet
	system.wallet (0)->store.erase (node.wallets.tx_begin_write (), nano::dev_genesis_key.pub);
	node.wallets.compute_reps ();
	ASSERT_FALSE (node.wallets.reps ().exists (nano::dev_genesis_key.pub));

	std::shared_ptr<nano::block> send2 = builder.state ()
	                                     .account (nano::dev_genesis_key.pub)
	                                     .representative (nano::dev_genesis_key.pub)
	                                     .previous (send->hash ())
	                                     .balance (node.config.vote_minimum)
	                                     .link (key.pub)
	                                     .sign (nano::dev_genesis_key.prv, nano::dev_genesis_key.pub)
	                                     .work (*system.env.work.generate (send->hash ()))
	                                     .build (ec);
	ASSERT_FALSE (ec);
	ASSERT_EQ (nano::process_result::progress, node.process_local (send2).code);
	ASSERT_EQ (node.config.vote_minimum, node.weight (nano::dev_genesis_key.pub));
	node.block_confirm (send2);
	// Process a vote
	auto vote2 (node.store.vote_generate (node.store.tx_begin_read (), nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, { send2->hash () }));
	ASSERT_EQ (nano::vote_code::vote, node.active.vote (vote2));
	// Make sure the vote was processed
	auto election2 (node.active.election (send2->qualified_root ()));
	ASSERT_NE (nullptr, election2);
	auto existing2 (election2->last_votes.find (nano::dev_genesis_key.pub));
	ASSERT_NE (election2->last_votes.end (), existing2);
	ASSERT_EQ (vote2->sequence, existing2->second.sequence);
	// Ensure the vote was broadcast
	ASSERT_EQ (1, node.stats.count (nano::stat::type::message, nano::stat::detail::confirm_ack, nano::stat::dir::out));
	ASSERT_EQ (2, node.stats.count (nano::stat::type::message, nano::stat::detail::publish, nano::stat::dir::out));

	// Repeat test with a PR in the wallet
	// Increase the genesis weight again
	std::shared_ptr<nano::block> open = builder.state ()
	                                    .account (key.pub)
	                                    .representative (nano::dev_genesis_key.pub)
	                                    .previous (0)
	                                    .balance (nano::genesis_amount - 2 * node.config.vote_minimum.number ())
	                                    .link (send->hash ())
	                                    .sign (key.prv, key.pub)
	                                    .work (*system.env.work.generate (key.pub))
	                                    .build (ec);
	ASSERT_FALSE (ec);
	ASSERT_EQ (nano::process_result::progress, node.process_local (open).code);
	ASSERT_EQ (nano::genesis_amount - node.config.vote_minimum.number (), node.weight (nano::dev_genesis_key.pub));
	node.block_confirm (open);
	// Insert account in wallet
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	node.wallets.compute_reps ();
	ASSERT_TRUE (node.wallets.reps ().exists (nano::dev_genesis_key.pub));
	ASSERT_TRUE (node.wallets.reps ().have_half_rep ());
	// Process a vote
	auto vote3 (node.store.vote_generate (node.store.tx_begin_read (), nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, { open->hash () }));
	ASSERT_EQ (nano::vote_code::vote, node.active.vote (vote3));
	// Make sure the vote was processed
	auto election3 (node.active.election (open->qualified_root ()));
	ASSERT_NE (nullptr, election3);
	auto existing3 (election3->last_votes.find (nano::dev_genesis_key.pub));
	ASSERT_NE (election3->last_votes.end (), existing3);
	ASSERT_EQ (vote3->sequence, existing3->second.sequence);
	// Ensure the vote wass not broadcasst
	ASSERT_EQ (1, node.stats.count (nano::stat::type::message, nano::stat::detail::confirm_ack, nano::stat::dir::out));
	ASSERT_EQ (3, node.stats.count (nano::stat::type::message, nano::stat::detail::publish, nano::stat::dir::out));
}
