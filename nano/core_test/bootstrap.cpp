#include <nano/node/bootstrap/bootstrap_frontier.hpp>
#include <nano/node/bootstrap/bootstrap_lazy.hpp>
#include <nano/test_common/system.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

// If the account doesn't exist, current == end so there's no iteration
TEST (bulk_pull, no_address)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::bulk_pull> ();
	req->start = 1;
	req->end = 2;
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::bulk_pull_server> (connection, std::move (req)));
	ASSERT_EQ (request->current, request->request->end);
	ASSERT_TRUE (request->current.is_zero ());
}

TEST (bulk_pull, genesis_to_end)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::bulk_pull> ();
	req->start = nano::dev::genesis_key.pub;
	req->end.clear ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::bulk_pull_server> (connection, std::move (req)));
	ASSERT_EQ (system.nodes[0]->latest (nano::dev::genesis_key.pub), request->current);
	ASSERT_EQ (request->request->end, request->request->end);
}

// If we can't find the end block, send everything
TEST (bulk_pull, no_end)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::bulk_pull> ();
	req->start = nano::dev::genesis_key.pub;
	req->end = 1;
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::bulk_pull_server> (connection, std::move (req)));
	ASSERT_EQ (system.nodes[0]->latest (nano::dev::genesis_key.pub), request->current);
	ASSERT_TRUE (request->request->end.is_zero ());
}

TEST (bulk_pull, end_not_owned)
{
	nano::system system (1);
	nano::keypair key2;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	ASSERT_NE (nullptr, system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, 100));
	nano::block_hash latest (system.nodes[0]->latest (nano::dev::genesis_key.pub));
	nano::open_block open (0, 1, 2, nano::keypair ().prv, 4, 5);
	open.hashables.account = key2.pub;
	open.hashables.representative = key2.pub;
	open.hashables.source = latest;
	open.refresh ();
	open.signature = nano::sign_message (key2.prv, key2.pub, open.hash ());
	system.nodes[0]->work_generate_blocking (open);
	ASSERT_EQ (nano::process_result::progress, system.nodes[0]->process (open).code);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::bulk_pull> ();
	req->start = key2.pub;
	req->end = nano::dev::genesis->hash ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::bulk_pull_server> (connection, std::move (req)));
	ASSERT_EQ (request->current, request->request->end);
}

TEST (bulk_pull, none)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::bulk_pull> ();
	req->start = nano::dev::genesis_key.pub;
	req->end = nano::dev::genesis->hash ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::bulk_pull_server> (connection, std::move (req)));
	auto block (request->get_next ());
	ASSERT_EQ (nullptr, block);
}

TEST (bulk_pull, get_next_on_open)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::bulk_pull> ();
	req->start = nano::dev::genesis_key.pub;
	req->end.clear ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::bulk_pull_server> (connection, std::move (req)));
	auto block (request->get_next ());
	ASSERT_NE (nullptr, block);
	ASSERT_TRUE (block->previous ().is_zero ());
	ASSERT_FALSE (connection->requests.empty ());
	ASSERT_EQ (request->current, request->request->end);
}

TEST (bulk_pull, by_block)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::bulk_pull> ();
	req->start = nano::dev::genesis->hash ();
	req->end.clear ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::bulk_pull_server> (connection, std::move (req)));
	auto block (request->get_next ());
	ASSERT_NE (nullptr, block);
	ASSERT_EQ (block->hash (), nano::dev::genesis->hash ());

	block = request->get_next ();
	ASSERT_EQ (nullptr, block);
}

TEST (bulk_pull, by_block_single)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::bulk_pull> ();
	req->start = nano::dev::genesis->hash ();
	req->end = nano::dev::genesis->hash ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::bulk_pull_server> (connection, std::move (req)));
	auto block (request->get_next ());
	ASSERT_NE (nullptr, block);
	ASSERT_EQ (block->hash (), nano::dev::genesis->hash ());

	block = request->get_next ();
	ASSERT_EQ (nullptr, block);
}

TEST (bulk_pull, count_limit)
{
	nano::system system (1);
	auto node0 (system.nodes[0]);

	auto send1 (std::make_shared<nano::send_block> (node0->latest (nano::dev::genesis_key.pub), nano::dev::genesis_key.pub, 1, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (node0->latest (nano::dev::genesis_key.pub))));
	ASSERT_EQ (nano::process_result::progress, node0->process (*send1).code);
	auto receive1 (std::make_shared<nano::receive_block> (send1->hash (), send1->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1->hash ())));
	ASSERT_EQ (nano::process_result::progress, node0->process (*receive1).code);

	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*node0), node0));
	auto req = std::make_unique<nano::bulk_pull> ();
	req->start = receive1->hash ();
	req->set_count_present (true);
	req->count = 2;
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::bulk_pull_server> (connection, std::move (req)));

	ASSERT_EQ (request->max_count, 2);
	ASSERT_EQ (request->sent_count, 0);

	auto block (request->get_next ());
	ASSERT_NE (nullptr, block);
	ASSERT_EQ (receive1->hash (), block->hash ());

	block = request->get_next ();
	ASSERT_EQ (send1->hash (), block->hash ());

	block = request->get_next ();
	ASSERT_EQ (nullptr, block);
}

TEST (bootstrap_processor, DISABLED_process_none)
{
	nano::system system (1);
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	ASSERT_FALSE (node1->init_error ());
	auto done (false);
	node1->bootstrap_initiator.bootstrap (system.nodes[0]->network.endpoint (), false);
	while (!done)
	{
		system.io_ctx.run_one ();
	}
	node1->stop ();
}

// Bootstrap can pull one basic block
TEST (bootstrap_processor, process_one)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	node_config.enable_voting = false;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node0 = system.add_node (node_config, node_flags);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub, 100));
	ASSERT_NE (nullptr, send);

	node_config.peering_port = nano::get_available_port ();
	node_flags.disable_rep_crawler = true;
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), node_config, system.work, node_flags));
	nano::block_hash hash1 (node0->latest (nano::dev::genesis_key.pub));
	nano::block_hash hash2 (node1->latest (nano::dev::genesis_key.pub));
	ASSERT_NE (hash1, hash2);
	node1->bootstrap_initiator.bootstrap (node0->network.endpoint (), false);
	ASSERT_NE (node1->latest (nano::dev::genesis_key.pub), node0->latest (nano::dev::genesis_key.pub));
	ASSERT_TIMELY (10s, node1->latest (nano::dev::genesis_key.pub) == node0->latest (nano::dev::genesis_key.pub));
	ASSERT_EQ (0, node1->active.size ());
	node1->stop ();
}

TEST (bootstrap_processor, process_two)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node0 (system.add_node (config, node_flags));
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::block_hash hash1 (node0->latest (nano::dev::genesis_key.pub));
	ASSERT_NE (nullptr, system.wallet (0)->send_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub, 50));
	nano::block_hash hash2 (node0->latest (nano::dev::genesis_key.pub));
	ASSERT_NE (nullptr, system.wallet (0)->send_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub, 50));
	nano::block_hash hash3 (node0->latest (nano::dev::genesis_key.pub));
	ASSERT_NE (hash1, hash2);
	ASSERT_NE (hash1, hash3);
	ASSERT_NE (hash2, hash3);

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	ASSERT_FALSE (node1->init_error ());
	node1->bootstrap_initiator.bootstrap (node0->network.endpoint (), false);
	ASSERT_NE (node1->latest (nano::dev::genesis_key.pub), node0->latest (nano::dev::genesis_key.pub));
	ASSERT_TIMELY (10s, node1->latest (nano::dev::genesis_key.pub) == node0->latest (nano::dev::genesis_key.pub));
	node1->stop ();
}

// Bootstrap can pull universal blocks
TEST (bootstrap_processor, process_state)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node0 (system.add_node (config, node_flags));
	nano::state_block_builder builder;

	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto block1 = builder
				  .account (nano::dev::genesis_key.pub)
				  .previous (node0->latest (nano::dev::genesis_key.pub))
				  .representative (nano::dev::genesis_key.pub)
				  .balance (nano::dev::genesis_amount - 100)
				  .link (nano::dev::genesis_key.pub)
				  .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				  .work (0)
				  .build_shared ();
	auto block2 = builder
				  .make_block ()
				  .account (nano::dev::genesis_key.pub)
				  .previous (block1->hash ())
				  .representative (nano::dev::genesis_key.pub)
				  .balance (nano::dev::genesis_amount)
				  .link (block1->hash ())
				  .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				  .work (0)
				  .build_shared ();

	node0->work_generate_blocking (*block1);
	node0->work_generate_blocking (*block2);
	node0->process (*block1);
	node0->process (*block2);

	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work, node_flags));
	ASSERT_EQ (node0->latest (nano::dev::genesis_key.pub), block2->hash ());
	ASSERT_NE (node1->latest (nano::dev::genesis_key.pub), block2->hash ());
	node1->bootstrap_initiator.bootstrap (node0->network.endpoint (), false);
	ASSERT_NE (node1->latest (nano::dev::genesis_key.pub), node0->latest (nano::dev::genesis_key.pub));
	ASSERT_TIMELY (10s, node1->latest (nano::dev::genesis_key.pub) == node0->latest (nano::dev::genesis_key.pub));
	ASSERT_TIMELY (10s, node1->active.empty ());
	node1->stop ();
}

TEST (bootstrap_processor, process_new)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node1 (system.add_node (config, node_flags));
	config.peering_port = nano::get_available_port ();
	auto node2 (system.add_node (config, node_flags));
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key2;
	system.wallet (1)->insert_adhoc (key2.prv);
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, node1->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send);
	ASSERT_TIMELY (10s, !node1->balance (key2.pub).is_zero ());
	auto receive (node2->block (node2->latest (key2.pub)));
	ASSERT_NE (nullptr, receive);
	nano::uint128_t balance1 (node1->balance (nano::dev::genesis_key.pub));
	nano::uint128_t balance2 (node1->balance (key2.pub));
	ASSERT_TIMELY (10s, node1->block_confirmed (send->hash ()) && node1->block_confirmed (receive->hash ()) && node1->active.empty () && node2->active.empty ()); // All blocks should be propagated & confirmed

	auto node3 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	ASSERT_FALSE (node3->init_error ());
	node3->bootstrap_initiator.bootstrap (node1->network.endpoint (), false);
	ASSERT_TIMELY (10s, node3->balance (key2.pub) == balance2);
	ASSERT_EQ (balance1, node3->balance (nano::dev::genesis_key.pub));
	node3->stop ();
}

TEST (bootstrap_processor, pull_diamond)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node0 (system.add_node (config, node_flags));
	nano::keypair key;
	auto send1 (std::make_shared<nano::send_block> (node0->latest (nano::dev::genesis_key.pub), key.pub, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (node0->latest (nano::dev::genesis_key.pub))));
	ASSERT_EQ (nano::process_result::progress, node0->process (*send1).code);
	auto open (std::make_shared<nano::open_block> (send1->hash (), 1, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	ASSERT_EQ (nano::process_result::progress, node0->process (*open).code);
	auto send2 (std::make_shared<nano::send_block> (open->hash (), nano::dev::genesis_key.pub, std::numeric_limits<nano::uint128_t>::max () - 100, key.prv, key.pub, *system.work.generate (open->hash ())));
	ASSERT_EQ (nano::process_result::progress, node0->process (*send2).code);
	auto receive (std::make_shared<nano::receive_block> (send1->hash (), send2->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1->hash ())));
	ASSERT_EQ (nano::process_result::progress, node0->process (*receive).code);
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	ASSERT_FALSE (node1->init_error ());
	node1->bootstrap_initiator.bootstrap (node0->network.endpoint (), false);
	ASSERT_TIMELY (10s, node1->balance (nano::dev::genesis_key.pub) == 100);
	ASSERT_EQ (100, node1->balance (nano::dev::genesis_key.pub));
	node1->stop ();
}

TEST (bootstrap_processor, DISABLED_pull_requeue_network_error)
{
	// Bootstrap attempt stopped before requeue & then cannot be found in attempts list
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node1 (system.add_node (config, node_flags));
	config.peering_port = nano::get_available_port ();
	auto node2 (system.add_node (config, node_flags));
	nano::keypair key1;

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (nano::dev::genesis->hash ()))
				 .build_shared ();

	node1->bootstrap_initiator.bootstrap (node2->network.endpoint ());
	auto attempt (node1->bootstrap_initiator.current_attempt ());
	ASSERT_NE (nullptr, attempt);
	ASSERT_TIMELY (2s, attempt->frontiers_received);
	// Add non-existing pull & stop remote peer
	{
		nano::unique_lock<nano::mutex> lock (node1->bootstrap_initiator.connections->mutex);
		ASSERT_FALSE (attempt->stopped);
		++attempt->pulling;
		node1->bootstrap_initiator.connections->pulls.emplace_back (nano::dev::genesis_key.pub, send1->hash (), nano::dev::genesis->hash (), attempt->incremental_id);
		node1->bootstrap_initiator.connections->request_pull (lock);
		node2->stop ();
	}
	ASSERT_TIMELY (5s, attempt == nullptr || attempt->requeued_pulls == 1);
	ASSERT_EQ (0, node1->stats.count (nano::stat::type::bootstrap, nano::stat::detail::bulk_pull_failed_account, nano::stat::dir::in)); // Requeue is not increasing failed attempts
}

TEST (bootstrap_processor, push_diamond)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto node0 (system.add_node (config));
	nano::keypair key;
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	ASSERT_FALSE (node1->init_error ());
	auto wallet1 (node1->wallets.create (100));
	wallet1->insert_adhoc (nano::dev::genesis_key.prv);
	wallet1->insert_adhoc (key.prv);
	auto send1 (std::make_shared<nano::send_block> (node0->latest (nano::dev::genesis_key.pub), key.pub, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (node0->latest (nano::dev::genesis_key.pub))));
	ASSERT_EQ (nano::process_result::progress, node1->process (*send1).code);
	auto open (std::make_shared<nano::open_block> (send1->hash (), 1, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	ASSERT_EQ (nano::process_result::progress, node1->process (*open).code);
	auto send2 (std::make_shared<nano::send_block> (open->hash (), nano::dev::genesis_key.pub, std::numeric_limits<nano::uint128_t>::max () - 100, key.prv, key.pub, *system.work.generate (open->hash ())));
	ASSERT_EQ (nano::process_result::progress, node1->process (*send2).code);
	auto receive (std::make_shared<nano::receive_block> (send1->hash (), send2->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1->hash ())));
	ASSERT_EQ (nano::process_result::progress, node1->process (*receive).code);
	node1->bootstrap_initiator.bootstrap (node0->network.endpoint (), false);
	ASSERT_TIMELY (10s, node0->balance (nano::dev::genesis_key.pub) == 100);
	ASSERT_EQ (100, node0->balance (nano::dev::genesis_key.pub));
	node1->stop ();
}

TEST (bootstrap_processor, push_diamond_pruning)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto node0 (system.add_node (config));
	nano::keypair key;
	config.peering_port = nano::get_available_port ();
	config.enable_voting = false; // Remove after allowing pruned voting
	nano::node_flags node_flags;
	node_flags.enable_pruning = true;
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), config, system.work, node_flags, 1));
	ASSERT_FALSE (node1->init_error ());
	auto latest (node0->latest (nano::dev::genesis_key.pub));
	auto send1 (std::make_shared<nano::send_block> (latest, key.pub, 0, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest)));
	ASSERT_EQ (nano::process_result::progress, node1->process (*send1).code);
	auto open (std::make_shared<nano::open_block> (send1->hash (), 1, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	ASSERT_EQ (nano::process_result::progress, node1->process (*open).code);
	// 1st bootstrap
	node1->bootstrap_initiator.bootstrap (node0->network.endpoint (), false);
	ASSERT_TIMELY (10s, node0->balance (key.pub) == nano::dev::genesis_amount);
	// Process more blocks & prune old
	auto send2 (std::make_shared<nano::send_block> (open->hash (), nano::dev::genesis_key.pub, std::numeric_limits<nano::uint128_t>::max () - 100, key.prv, key.pub, *system.work.generate (open->hash ())));
	ASSERT_EQ (nano::process_result::progress, node1->process (*send2).code);
	auto receive (std::make_shared<nano::receive_block> (send1->hash (), send2->hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1->hash ())));
	ASSERT_EQ (nano::process_result::progress, node1->process (*receive).code);
	{
		auto transaction (node1->store.tx_begin_write ());
		ASSERT_EQ (1, node1->ledger.pruning_action (transaction, send1->hash (), 2));
		ASSERT_EQ (1, node1->ledger.pruning_action (transaction, open->hash (), 1));
		ASSERT_TRUE (node1->store.block.exists (transaction, latest));
		ASSERT_FALSE (node1->store.block.exists (transaction, send1->hash ()));
		ASSERT_TRUE (node1->store.pruned.exists (transaction, send1->hash ()));
		ASSERT_FALSE (node1->store.block.exists (transaction, open->hash ()));
		ASSERT_TRUE (node1->store.pruned.exists (transaction, open->hash ()));
		ASSERT_TRUE (node1->store.block.exists (transaction, send2->hash ()));
		ASSERT_TRUE (node1->store.block.exists (transaction, receive->hash ()));
		ASSERT_EQ (2, node1->ledger.cache.pruned_count);
		ASSERT_EQ (5, node1->ledger.cache.block_count);
	}
	// 2nd bootstrap
	node1->bootstrap_initiator.bootstrap (node0->network.endpoint (), false);
	ASSERT_TIMELY (10s, node0->balance (nano::dev::genesis_key.pub) == 100);
	ASSERT_EQ (100, node0->balance (nano::dev::genesis_key.pub));
	node1->stop ();
}

TEST (bootstrap_processor, push_one)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto node0 (system.add_node (config));
	nano::keypair key1;
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	auto wallet (node1->wallets.create (nano::random_wallet_id ()));
	ASSERT_NE (nullptr, wallet);
	wallet->insert_adhoc (nano::dev::genesis_key.prv);
	nano::uint128_t balance1 (node1->balance (nano::dev::genesis_key.pub));
	auto send (wallet->send_action (nano::dev::genesis_key.pub, key1.pub, 100));
	ASSERT_NE (nullptr, send);
	ASSERT_NE (balance1, node1->balance (nano::dev::genesis_key.pub));
	node1->bootstrap_initiator.bootstrap (node0->network.endpoint (), false);
	ASSERT_TIMELY (10s, node0->balance (nano::dev::genesis_key.pub) != balance1);
	node1->stop ();
}

TEST (bootstrap_processor, lazy_hash)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node0 (system.add_node (config, node_flags));
	nano::keypair key1;
	nano::keypair key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*node0->work_generate_blocking (nano::dev::genesis->hash ()))
				 .build_shared ();
	auto receive1 = builder
					.make_block ()
					.account (key1.pub)
					.previous (0)
					.representative (key1.pub)
					.balance (nano::Gxrb_ratio)
					.link (send1->hash ())
					.sign (key1.prv, key1.pub)
					.work (*node0->work_generate_blocking (key1.pub))
					.build_shared ();
	auto send2 = builder
				 .make_block ()
				 .account (key1.pub)
				 .previous (receive1->hash ())
				 .representative (key1.pub)
				 .balance (0)
				 .link (key2.pub)
				 .sign (key1.prv, key1.pub)
				 .work (*node0->work_generate_blocking (receive1->hash ()))
				 .build_shared ();
	auto receive2 = builder
					.make_block ()
					.account (key2.pub)
					.previous (0)
					.representative (key2.pub)
					.balance (nano::Gxrb_ratio)
					.link (send2->hash ())
					.sign (key2.prv, key2.pub)
					.work (*node0->work_generate_blocking (key2.pub))
					.build_shared ();

	// Processing test chain
	node0->block_processor.add (send1);
	node0->block_processor.add (receive1);
	node0->block_processor.add (send2);
	node0->block_processor.add (receive2);
	node0->block_processor.flush ();
	// Start lazy bootstrap with last block in chain known
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	node1->network.udp_channels.insert (node0->network.endpoint (), node1->network_params.protocol.protocol_version);
	node1->bootstrap_initiator.bootstrap_lazy (receive2->hash (), true);
	{
		auto lazy_attempt (node1->bootstrap_initiator.current_lazy_attempt ());
		ASSERT_NE (nullptr, lazy_attempt);
		ASSERT_EQ (receive2->hash ().to_string (), lazy_attempt->id);
	}
	// Check processed blocks
	ASSERT_TIMELY (10s, node1->balance (key2.pub) != 0);
	node1->stop ();
}

TEST (bootstrap_processor, lazy_hash_bootstrap_id)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node0 (system.add_node (config, node_flags));
	nano::keypair key1;
	nano::keypair key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*node0->work_generate_blocking (nano::dev::genesis->hash ()))
				 .build_shared ();
	auto receive1 = builder
					.make_block ()
					.account (key1.pub)
					.previous (0)
					.representative (key1.pub)
					.balance (nano::Gxrb_ratio)
					.link (send1->hash ())
					.sign (key1.prv, key1.pub)
					.work (*node0->work_generate_blocking (key1.pub))
					.build_shared ();
	auto send2 = builder
				 .make_block ()
				 .account (key1.pub)
				 .previous (receive1->hash ())
				 .representative (key1.pub)
				 .balance (0)
				 .link (key2.pub)
				 .sign (key1.prv, key1.pub)
				 .work (*node0->work_generate_blocking (receive1->hash ()))
				 .build_shared ();
	auto receive2 = builder
					.make_block ()
					.account (key2.pub)
					.previous (0)
					.representative (key2.pub)
					.balance (nano::Gxrb_ratio)
					.link (send2->hash ())
					.sign (key2.prv, key2.pub)
					.work (*node0->work_generate_blocking (key2.pub))
					.build_shared ();

	// Processing test chain
	node0->block_processor.add (send1);
	node0->block_processor.add (receive1);
	node0->block_processor.add (send2);
	node0->block_processor.add (receive2);
	node0->block_processor.flush ();
	// Start lazy bootstrap with last block in chain known
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	node1->network.udp_channels.insert (node0->network.endpoint (), node1->network_params.protocol.protocol_version);
	node1->bootstrap_initiator.bootstrap_lazy (receive2->hash (), true, true, "123456");
	{
		auto lazy_attempt (node1->bootstrap_initiator.current_lazy_attempt ());
		ASSERT_NE (nullptr, lazy_attempt);
		ASSERT_EQ ("123456", lazy_attempt->id);
	}
	// Check processed blocks
	ASSERT_TIMELY (10s, node1->balance (key2.pub) != 0);
	node1->stop ();
}

TEST (bootstrap_processor, lazy_hash_pruning)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	config.enable_voting = false; // Remove after allowing pruned voting
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.enable_pruning = true;
	auto node0 (system.add_node (config, node_flags));
	nano::keypair key1;
	nano::keypair key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (nano::dev::genesis_key.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*node0->work_generate_blocking (nano::dev::genesis->hash ()))
				 .build_shared ();
	auto receive1 = builder
					.make_block ()
					.account (nano::dev::genesis_key.pub)
					.previous (send1->hash ())
					.representative (nano::dev::genesis_key.pub)
					.balance (nano::dev::genesis_amount)
					.link (send1->hash ())
					.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
					.work (*node0->work_generate_blocking (send1->hash ()))
					.build_shared ();
	auto change1 = builder
				   .make_block ()
				   .account (nano::dev::genesis_key.pub)
				   .previous (receive1->hash ())
				   .representative (key1.pub)
				   .balance (nano::dev::genesis_amount)
				   .link (0)
				   .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				   .work (*node0->work_generate_blocking (receive1->hash ()))
				   .build_shared ();
	auto change2 = builder
				   .make_block ()
				   .account (nano::dev::genesis_key.pub)
				   .previous (change1->hash ())
				   .representative (key2.pub)
				   .balance (nano::dev::genesis_amount)
				   .link (0)
				   .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				   .work (*node0->work_generate_blocking (change1->hash ()))
				   .build_shared ();
	auto send2 = builder
				 .make_block ()
				 .account (nano::dev::genesis_key.pub)
				 .previous (change2->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*node0->work_generate_blocking (change2->hash ()))
				 .build_shared ();
	auto receive2 = builder
					.make_block ()
					.account (key1.pub)
					.previous (0)
					.representative (key1.pub)
					.balance (nano::Gxrb_ratio)
					.link (send2->hash ())
					.sign (key1.prv, key1.pub)
					.work (*node0->work_generate_blocking (key1.pub))
					.build_shared ();
	auto send3 = builder
				 .make_block ()
				 .account (key1.pub)
				 .previous (receive2->hash ())
				 .representative (key1.pub)
				 .balance (0)
				 .link (key2.pub)
				 .sign (key1.prv, key1.pub)
				 .work (*node0->work_generate_blocking (receive2->hash ()))
				 .build_shared ();
	auto receive3 = builder
					.make_block ()
					.account (key2.pub)
					.previous (0)
					.representative (key2.pub)
					.balance (nano::Gxrb_ratio)
					.link (send3->hash ())
					.sign (key2.prv, key2.pub)
					.work (*node0->work_generate_blocking (key2.pub))
					.build_shared ();

	// Processing test chain
	node0->block_processor.add (send1);
	node0->block_processor.add (receive1);
	node0->block_processor.add (change1);
	node0->block_processor.add (change2);
	node0->block_processor.add (send2);
	node0->block_processor.add (receive2);
	node0->block_processor.add (send3);
	node0->block_processor.add (receive3);
	node0->block_processor.flush ();
	ASSERT_EQ (9, node0->ledger.cache.block_count);
	// Processing chain to prune for node1
	config.peering_port = nano::get_available_port ();
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), config, system.work, node_flags, 1));
	node1->process_active (send1);
	node1->process_active (receive1);
	node1->process_active (change1);
	node1->process_active (change2);
	// Confirm last block to prune previous
	nano::blocks_confirm (*node1, { send1, receive1, change1, change2 }, true);
	ASSERT_TIMELY (10s, node1->block_confirmed (send1->hash ()) && node1->block_confirmed (receive1->hash ()) && node1->block_confirmed (change1->hash ()) && node1->block_confirmed (change2->hash ()) && node1->active.empty ());
	ASSERT_EQ (5, node1->ledger.cache.block_count);
	ASSERT_EQ (5, node1->ledger.cache.cemented_count);
	// Pruning action
	node1->ledger_pruning (2, false, false);
	ASSERT_EQ (9, node0->ledger.cache.block_count);
	ASSERT_EQ (0, node0->ledger.cache.pruned_count);
	ASSERT_EQ (5, node1->ledger.cache.block_count);
	ASSERT_EQ (3, node1->ledger.cache.pruned_count);
	// Start lazy bootstrap with last block in chain known
	node1->network.udp_channels.insert (node0->network.endpoint (), node1->network_params.protocol.protocol_version);
	node1->bootstrap_initiator.bootstrap_lazy (receive3->hash (), true);
	// Check processed blocks
	ASSERT_TIMELY (10s, node1->ledger.cache.block_count == 9);
	ASSERT_TIMELY (10s, node1->balance (key2.pub) != 0);
	ASSERT_TIMELY (10s, !node1->bootstrap_initiator.in_progress ());
	node1->stop ();
}

TEST (bootstrap_processor, lazy_max_pull_count)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node0 (system.add_node (config, node_flags));
	nano::keypair key1;
	nano::keypair key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*node0->work_generate_blocking (nano::dev::genesis->hash ()))
				 .build_shared ();
	auto receive1 = builder
					.make_block ()
					.account (key1.pub)
					.previous (0)
					.representative (key1.pub)
					.balance (nano::Gxrb_ratio)
					.link (send1->hash ())
					.sign (key1.prv, key1.pub)
					.work (*node0->work_generate_blocking (key1.pub))
					.build_shared ();
	auto send2 = builder
				 .make_block ()
				 .account (key1.pub)
				 .previous (receive1->hash ())
				 .representative (key1.pub)
				 .balance (0)
				 .link (key2.pub)
				 .sign (key1.prv, key1.pub)
				 .work (*node0->work_generate_blocking (receive1->hash ()))
				 .build_shared ();
	auto receive2 = builder
					.make_block ()
					.account (key2.pub)
					.previous (0)
					.representative (key2.pub)
					.balance (nano::Gxrb_ratio)
					.link (send2->hash ())
					.sign (key2.prv, key2.pub)
					.work (*node0->work_generate_blocking (key2.pub))
					.build_shared ();
	auto change1 = builder
				   .make_block ()
				   .account (key2.pub)
				   .previous (receive2->hash ())
				   .representative (key1.pub)
				   .balance (nano::Gxrb_ratio)
				   .link (0)
				   .sign (key2.prv, key2.pub)
				   .work (*node0->work_generate_blocking (receive2->hash ()))
				   .build_shared ();
	auto change2 = builder
				   .make_block ()
				   .account (key2.pub)
				   .previous (change1->hash ())
				   .representative (nano::dev::genesis_key.pub)
				   .balance (nano::Gxrb_ratio)
				   .link (0)
				   .sign (key2.prv, key2.pub)
				   .work (*node0->work_generate_blocking (change1->hash ()))
				   .build_shared ();
	auto change3 = builder
				   .make_block ()
				   .account (key2.pub)
				   .previous (change2->hash ())
				   .representative (key2.pub)
				   .balance (nano::Gxrb_ratio)
				   .link (0)
				   .sign (key2.prv, key2.pub)
				   .work (*node0->work_generate_blocking (change2->hash ()))
				   .build_shared ();
	// Processing test chain
	node0->block_processor.add (send1);
	node0->block_processor.add (receive1);
	node0->block_processor.add (send2);
	node0->block_processor.add (receive2);
	node0->block_processor.add (change1);
	node0->block_processor.add (change2);
	node0->block_processor.add (change3);
	node0->block_processor.flush ();
	// Start lazy bootstrap with last block in chain known
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	node1->network.udp_channels.insert (node0->network.endpoint (), node1->network_params.protocol.protocol_version);
	node1->bootstrap_initiator.bootstrap_lazy (change3->hash ());
	// Check processed blocks
	ASSERT_TIMELY (10s, node1->block (change3->hash ()));

	node1->stop ();
}

TEST (bootstrap_processor, lazy_unclear_state_link)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.disable_legacy_bootstrap = true;
	auto node1 = system.add_node (config, node_flags);
	nano::keypair key;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (nano::dev::genesis->hash ()))
				 .build_shared ();
	ASSERT_EQ (nano::process_result::progress, node1->process (*send1).code);
	auto send2 = builder
				 .make_block ()
				 .account (nano::dev::genesis_key.pub)
				 .previous (send1->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - 2 * nano::Gxrb_ratio)
				 .link (key.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (send1->hash ()))
				 .build_shared ();
	ASSERT_EQ (nano::process_result::progress, node1->process (*send2).code);
	auto open (std::make_shared<nano::open_block> (send1->hash (), key.pub, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	ASSERT_EQ (nano::process_result::progress, node1->process (*open).code);
	auto receive = builder
				   .make_block ()
				   .account (key.pub)
				   .previous (open->hash ())
				   .representative (key.pub)
				   .balance (2 * nano::Gxrb_ratio)
				   .link (send2->hash ())
				   .sign (key.prv, key.pub)
				   .work (*system.work.generate (open->hash ()))
				   .build_shared ();
	ASSERT_EQ (nano::process_result::progress, node1->process (*receive).code);
	// Start lazy bootstrap with last block in chain known
	auto node2 = system.add_node (nano::node_config (nano::get_available_port (), system.logging), node_flags);
	node2->network.udp_channels.insert (node1->network.endpoint (), node1->network_params.protocol.protocol_version);
	node2->bootstrap_initiator.bootstrap_lazy (receive->hash ());
	// Check processed blocks
	ASSERT_TIMELY (10s, !node2->bootstrap_initiator.in_progress ());
	node2->block_processor.flush ();
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send1->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send2->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (open->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (receive->hash ()));
	ASSERT_EQ (0, node2->stats.count (nano::stat::type::bootstrap, nano::stat::detail::bulk_pull_failed_account, nano::stat::dir::in));
}

TEST (bootstrap_processor, lazy_unclear_state_link_not_existing)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.disable_legacy_bootstrap = true;
	auto node1 = system.add_node (config, node_flags);
	nano::keypair key, key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (nano::dev::genesis->hash ()))
				 .build_shared ();
	ASSERT_EQ (nano::process_result::progress, node1->process (*send1).code);
	auto open (std::make_shared<nano::open_block> (send1->hash (), key.pub, key.pub, key.prv, key.pub, *system.work.generate (key.pub)));
	ASSERT_EQ (nano::process_result::progress, node1->process (*open).code);
	auto send2 = builder
				 .make_block ()
				 .account (key.pub)
				 .previous (open->hash ())
				 .representative (key.pub)
				 .balance (0)
				 .link (key2.pub)
				 .sign (key.prv, key.pub)
				 .work (*system.work.generate (open->hash ()))
				 .build_shared ();
	ASSERT_EQ (nano::process_result::progress, node1->process (*send2).code);

	// Start lazy bootstrap with last block in chain known
	auto node2 = system.add_node (nano::node_config (nano::get_available_port (), system.logging), node_flags);
	node2->network.udp_channels.insert (node1->network.endpoint (), node1->network_params.protocol.protocol_version);
	node2->bootstrap_initiator.bootstrap_lazy (send2->hash ());
	// Check processed blocks
	ASSERT_TIMELY (15s, !node2->bootstrap_initiator.in_progress ());
	node2->block_processor.flush ();
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send1->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (open->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send2->hash ()));
	ASSERT_EQ (1, node2->stats.count (nano::stat::type::bootstrap, nano::stat::detail::bulk_pull_failed_account, nano::stat::dir::in));
}

TEST (bootstrap_processor, DISABLED_lazy_destinations)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.disable_legacy_bootstrap = true;
	auto node1 = system.add_node (config, node_flags);
	nano::keypair key1, key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (nano::dev::genesis->hash ()))
				 .build_shared ();
	ASSERT_EQ (nano::process_result::progress, node1->process (*send1).code);
	auto send2 = builder
				 .make_block ()
				 .account (nano::dev::genesis_key.pub)
				 .previous (send1->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - 2 * nano::Gxrb_ratio)
				 .link (key2.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (send1->hash ()))
				 .build_shared ();
	ASSERT_EQ (nano::process_result::progress, node1->process (*send2).code);
	auto open (std::make_shared<nano::open_block> (send1->hash (), key1.pub, key1.pub, key1.prv, key1.pub, *system.work.generate (key1.pub)));
	ASSERT_EQ (nano::process_result::progress, node1->process (*open).code);
	auto state_open = builder
					  .make_block ()
					  .account (key2.pub)
					  .previous (0)
					  .representative (key2.pub)
					  .balance (nano::Gxrb_ratio)
					  .link (send2->hash ())
					  .sign (key2.prv, key2.pub)
					  .work (*system.work.generate (key2.pub))
					  .build_shared ();
	ASSERT_EQ (nano::process_result::progress, node1->process (*state_open).code);

	// Start lazy bootstrap with last block in sender chain
	auto node2 = system.add_node (nano::node_config (nano::get_available_port (), system.logging), node_flags);
	node2->network.udp_channels.insert (node1->network.endpoint (), node1->network_params.protocol.protocol_version);
	node2->bootstrap_initiator.bootstrap_lazy (send2->hash ());
	// Check processed blocks
	ASSERT_TIMELY (10s, !node2->bootstrap_initiator.in_progress ());
	node2->block_processor.flush ();
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send1->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send2->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (open->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (state_open->hash ()));
}

TEST (bootstrap_processor, lazy_pruning_missing_block)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	config.enable_voting = false; // Remove after allowing pruned voting
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.disable_legacy_bootstrap = true;
	node_flags.enable_pruning = true;
	auto node1 = system.add_node (config, node_flags);
	nano::keypair key1, key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (nano::dev::genesis->hash ()))
				 .build_shared ();
	node1->process_active (send1);
	auto send2 = builder
				 .make_block ()
				 .account (nano::dev::genesis_key.pub)
				 .previous (send1->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - 2 * nano::Gxrb_ratio)
				 .link (key2.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*system.work.generate (send1->hash ()))
				 .build_shared ();
	node1->process_active (send2);
	auto open (std::make_shared<nano::open_block> (send1->hash (), key1.pub, key1.pub, key1.prv, key1.pub, *system.work.generate (key1.pub)));
	node1->process_active (open);
	auto state_open = builder
					  .make_block ()
					  .account (key2.pub)
					  .previous (0)
					  .representative (key2.pub)
					  .balance (nano::Gxrb_ratio)
					  .link (send2->hash ())
					  .sign (key2.prv, key2.pub)
					  .work (*system.work.generate (key2.pub))
					  .build_shared ();

	node1->process_active (state_open);
	// Confirm last block to prune previous
	nano::blocks_confirm (*node1, { send1, send2, open, state_open }, true);
	ASSERT_TIMELY (10s, node1->block_confirmed (send1->hash ()) && node1->block_confirmed (send2->hash ()) && node1->block_confirmed (open->hash ()) && node1->block_confirmed (state_open->hash ()) && node1->active.empty ());
	ASSERT_EQ (5, node1->ledger.cache.block_count);
	ASSERT_EQ (5, node1->ledger.cache.cemented_count);
	// Pruning action
	node1->ledger_pruning (2, false, false);
	ASSERT_EQ (5, node1->ledger.cache.block_count);
	ASSERT_EQ (1, node1->ledger.cache.pruned_count);
	ASSERT_TRUE (node1->ledger.block_or_pruned_exists (send1->hash ())); // true for pruned
	ASSERT_TRUE (node1->ledger.block_or_pruned_exists (send2->hash ()));
	ASSERT_TRUE (node1->ledger.block_or_pruned_exists (open->hash ()));
	ASSERT_TRUE (node1->ledger.block_or_pruned_exists (state_open->hash ()));
	// Start lazy bootstrap with last block in sender chain
	config.peering_port = nano::get_available_port ();
	auto node2 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), config, system.work, node_flags, 1));
	node2->network.udp_channels.insert (node1->network.endpoint (), node1->network_params.protocol.protocol_version);
	node2->bootstrap_initiator.bootstrap_lazy (send2->hash ());
	// Check processed blocks
	auto lazy_attempt (node2->bootstrap_initiator.current_lazy_attempt ());
	ASSERT_NE (nullptr, lazy_attempt);
	ASSERT_TIMELY (5s, lazy_attempt == nullptr || lazy_attempt->stopped || lazy_attempt->requeued_pulls >= 4);
	// Some blocks cannot be retrieved from pruned node
	node2->block_processor.flush ();
	ASSERT_EQ (1, node2->ledger.cache.block_count);
	ASSERT_FALSE (node2->ledger.block_or_pruned_exists (send1->hash ()));
	ASSERT_FALSE (node2->ledger.block_or_pruned_exists (send2->hash ()));
	ASSERT_FALSE (node2->ledger.block_or_pruned_exists (open->hash ()));
	ASSERT_FALSE (node2->ledger.block_or_pruned_exists (state_open->hash ()));
	{
		auto transaction (node2->store.tx_begin_read ());
		ASSERT_TRUE (node2->store.unchecked.exists (transaction, nano::unchecked_key (send2->root ().as_block_hash (), send2->hash ())));
	}
	// Insert missing block
	node2->process_active (send1);
	node2->block_processor.flush ();
	ASSERT_TIMELY (10s, !node2->bootstrap_initiator.in_progress ());
	node2->block_processor.flush ();
	ASSERT_EQ (3, node2->ledger.cache.block_count);
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send1->hash ()));
	ASSERT_TRUE (node2->ledger.block_or_pruned_exists (send2->hash ()));
	ASSERT_FALSE (node2->ledger.block_or_pruned_exists (open->hash ()));
	ASSERT_FALSE (node2->ledger.block_or_pruned_exists (state_open->hash ()));
	node2->stop ();
}

TEST (bootstrap_processor, lazy_cancel)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node0 (system.add_node (config, node_flags));
	nano::keypair key1;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*node0->work_generate_blocking (nano::dev::genesis->hash ()))
				 .build_shared ();

	// Start lazy bootstrap with last block in chain known
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	node1->network.udp_channels.insert (node0->network.endpoint (), node1->network_params.protocol.protocol_version);
	node1->bootstrap_initiator.bootstrap_lazy (send1->hash (), true); // Start "confirmed" block bootstrap
	{
		auto lazy_attempt (node1->bootstrap_initiator.current_lazy_attempt ());
		ASSERT_NE (nullptr, lazy_attempt);
		ASSERT_EQ (send1->hash ().to_string (), lazy_attempt->id);
	}
	// Cancel failing lazy bootstrap
	ASSERT_TIMELY (10s, !node1->bootstrap_initiator.in_progress ());
	node1->stop ();
}

TEST (bootstrap_processor, wallet_lazy_frontier)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.disable_legacy_bootstrap = true;
	auto node0 = system.add_node (config, node_flags);
	nano::keypair key1;
	nano::keypair key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*node0->work_generate_blocking (nano::dev::genesis->hash ()))
				 .build_shared ();
	auto receive1 = builder
					.make_block ()
					.account (key1.pub)
					.previous (0)
					.representative (key1.pub)
					.balance (nano::Gxrb_ratio)
					.link (send1->hash ())
					.sign (key1.prv, key1.pub)
					.work (*node0->work_generate_blocking (key1.pub))
					.build_shared ();
	auto send2 = builder
				 .make_block ()
				 .account (key1.pub)
				 .previous (receive1->hash ())
				 .representative (key1.pub)
				 .balance (0)
				 .link (key2.pub)
				 .sign (key1.prv, key1.pub)
				 .work (*node0->work_generate_blocking (receive1->hash ()))
				 .build_shared ();
	auto receive2 = builder
					.make_block ()
					.account (key2.pub)
					.previous (0)
					.representative (key2.pub)
					.balance (nano::Gxrb_ratio)
					.link (send2->hash ())
					.sign (key2.prv, key2.pub)
					.work (*node0->work_generate_blocking (key2.pub))
					.build_shared ();

	// Processing test chain
	node0->block_processor.add (send1);
	node0->block_processor.add (receive1);
	node0->block_processor.add (send2);
	node0->block_processor.add (receive2);
	node0->block_processor.flush ();
	// Start wallet lazy bootstrap
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	node1->network.udp_channels.insert (node0->network.endpoint (), node1->network_params.protocol.protocol_version);
	auto wallet (node1->wallets.create (nano::random_wallet_id ()));
	ASSERT_NE (nullptr, wallet);
	wallet->insert_adhoc (key2.prv);
	node1->bootstrap_wallet ();
	{
		auto wallet_attempt (node1->bootstrap_initiator.current_wallet_attempt ());
		ASSERT_NE (nullptr, wallet_attempt);
		ASSERT_EQ (key2.pub.to_account (), wallet_attempt->id);
	}
	// Check processed blocks
	ASSERT_TIMELY (10s, node1->ledger.block_or_pruned_exists (receive2->hash ()));
	node1->stop ();
}

TEST (bootstrap_processor, wallet_lazy_pending)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.disable_legacy_bootstrap = true;
	auto node0 = system.add_node (config, node_flags);
	nano::keypair key1;
	nano::keypair key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*node0->work_generate_blocking (nano::dev::genesis->hash ()))
				 .build_shared ();
	auto receive1 = builder
					.make_block ()
					.account (key1.pub)
					.previous (0)
					.representative (key1.pub)
					.balance (nano::Gxrb_ratio)
					.link (send1->hash ())
					.sign (key1.prv, key1.pub)
					.work (*node0->work_generate_blocking (key1.pub))
					.build_shared ();
	auto send2 = builder
				 .make_block ()
				 .account (key1.pub)
				 .previous (receive1->hash ())
				 .representative (key1.pub)
				 .balance (0)
				 .link (key2.pub)
				 .sign (key1.prv, key1.pub)
				 .work (*node0->work_generate_blocking (receive1->hash ()))
				 .build_shared ();

	// Processing test chain
	node0->block_processor.add (send1);
	node0->block_processor.add (receive1);
	node0->block_processor.add (send2);
	node0->block_processor.flush ();
	// Start wallet lazy bootstrap
	auto node1 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	node1->network.udp_channels.insert (node0->network.endpoint (), node1->network_params.protocol.protocol_version);
	auto wallet (node1->wallets.create (nano::random_wallet_id ()));
	ASSERT_NE (nullptr, wallet);
	wallet->insert_adhoc (key2.prv);
	node1->bootstrap_wallet ();
	// Check processed blocks
	ASSERT_TIMELY (10s, node1->ledger.block_or_pruned_exists (send2->hash ()));
	node1->stop ();
}

TEST (bootstrap_processor, multiple_attempts)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	auto node1 = system.add_node (config, node_flags);
	nano::keypair key1;
	nano::keypair key2;
	// Generating test chain

	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (*node1->work_generate_blocking (nano::dev::genesis->hash ()))
				 .build_shared ();
	auto receive1 = builder
					.make_block ()
					.account (key1.pub)
					.previous (0)
					.representative (key1.pub)
					.balance (nano::Gxrb_ratio)
					.link (send1->hash ())
					.sign (key1.prv, key1.pub)
					.work (*node1->work_generate_blocking (key1.pub))
					.build_shared ();
	auto send2 = builder
				 .make_block ()
				 .account (key1.pub)
				 .previous (receive1->hash ())
				 .representative (key1.pub)
				 .balance (0)
				 .link (key2.pub)
				 .sign (key1.prv, key1.pub)
				 .work (*node1->work_generate_blocking (receive1->hash ()))
				 .build_shared ();
	auto receive2 = builder
					.make_block ()
					.account (key2.pub)
					.previous (0)
					.representative (key2.pub)
					.balance (nano::Gxrb_ratio)
					.link (send2->hash ())
					.sign (key2.prv, key2.pub)
					.work (*node1->work_generate_blocking (key2.pub))
					.build_shared ();

	// Processing test chain
	node1->block_processor.add (send1);
	node1->block_processor.add (receive1);
	node1->block_processor.add (send2);
	node1->block_processor.add (receive2);
	node1->block_processor.flush ();
	// Start 2 concurrent bootstrap attempts
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.bootstrap_initiator_threads = 3;
	auto node2 (std::make_shared<nano::node> (system.io_ctx, nano::unique_path (), node_config, system.work));
	node2->network.udp_channels.insert (node1->network.endpoint (), node2->network_params.protocol.protocol_version);
	node2->bootstrap_initiator.bootstrap_lazy (receive2->hash (), true);
	node2->bootstrap_initiator.bootstrap ();
	auto lazy_attempt (node2->bootstrap_initiator.current_lazy_attempt ());
	auto legacy_attempt (node2->bootstrap_initiator.current_attempt ());
	ASSERT_TIMELY (5s, lazy_attempt->started && legacy_attempt->started);
	// Check that both bootstrap attempts are running & not finished
	ASSERT_FALSE (lazy_attempt->stopped);
	ASSERT_FALSE (legacy_attempt->stopped);
	ASSERT_GE (node2->bootstrap_initiator.attempts.size (), 2);
	// Check processed blocks
	ASSERT_TIMELY (10s, node2->balance (key2.pub) != 0);
	// Check attempts finish
	ASSERT_TIMELY (5s, node2->bootstrap_initiator.attempts.size () == 0);
	node2->stop ();
}

TEST (frontier_req_response, DISABLED_destruction)
{
	{
		std::shared_ptr<nano::frontier_req_server> hold; // Destructing tcp acceptor on non-existent io_context
		{
			nano::system system (1);
			auto connection (std::make_shared<nano::bootstrap_server> (nullptr, system.nodes[0]));
			auto req = std::make_unique<nano::frontier_req> ();
			req->start.clear ();
			req->age = std::numeric_limits<decltype (req->age)>::max ();
			req->count = std::numeric_limits<decltype (req->count)>::max ();
			connection->requests.push (std::unique_ptr<nano::message>{});
			hold = std::make_shared<nano::frontier_req_server> (connection, std::move (req));
		}
	}
	ASSERT_TRUE (true);
}

TEST (frontier_req, begin)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::frontier_req> ();
	req->start.clear ();
	req->age = std::numeric_limits<decltype (req->age)>::max ();
	req->count = std::numeric_limits<decltype (req->count)>::max ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::frontier_req_server> (connection, std::move (req)));
	ASSERT_EQ (nano::dev::genesis_key.pub, request->current);
	ASSERT_EQ (nano::dev::genesis->hash (), request->frontier);
}

TEST (frontier_req, end)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::frontier_req> ();
	req->start = nano::dev::genesis_key.pub.number () + 1;
	req->age = std::numeric_limits<decltype (req->age)>::max ();
	req->count = std::numeric_limits<decltype (req->count)>::max ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::frontier_req_server> (connection, std::move (req)));
	ASSERT_TRUE (request->current.is_zero ());
}

TEST (frontier_req, count)
{
	nano::system system (1);
	auto node1 = system.nodes[0];
	// Public key FB93... after genesis in accounts table
	nano::keypair key1 ("ED5AE0A6505B14B67435C29FD9FEEBC26F597D147BC92F6D795FFAD7AFD3D967");
	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key1.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (0)
				 .build_shared ();
	node1->work_generate_blocking (*send1);
	ASSERT_EQ (nano::process_result::progress, node1->process (*send1).code);
	auto receive1 = builder
					.make_block ()
					.account (key1.pub)
					.previous (0)
					.representative (nano::dev::genesis_key.pub)
					.balance (nano::Gxrb_ratio)
					.link (send1->hash ())
					.sign (key1.prv, key1.pub)
					.work (0)
					.build_shared ();
	node1->work_generate_blocking (*receive1);
	ASSERT_EQ (nano::process_result::progress, node1->process (*receive1).code);

	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*node1), node1));
	auto req = std::make_unique<nano::frontier_req> ();
	req->start.clear ();
	req->age = std::numeric_limits<decltype (req->age)>::max ();
	req->count = 1;
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::frontier_req_server> (connection, std::move (req)));
	ASSERT_EQ (nano::dev::genesis_key.pub, request->current);
	ASSERT_EQ (send1->hash (), request->frontier);
}

TEST (frontier_req, time_bound)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::frontier_req> ();
	req->start.clear ();
	req->age = 1;
	req->count = std::numeric_limits<decltype (req->count)>::max ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::frontier_req_server> (connection, std::move (req)));
	ASSERT_EQ (nano::dev::genesis_key.pub, request->current);
	// Wait 2 seconds until age of account will be > 1 seconds
	std::this_thread::sleep_for (std::chrono::milliseconds (2100));
	auto req2 (std::make_unique<nano::frontier_req> ());
	req2->start.clear ();
	req2->age = 1;
	req2->count = std::numeric_limits<decltype (req2->count)>::max ();
	auto connection2 (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	connection2->requests.push (std::unique_ptr<nano::message>{});
	auto request2 (std::make_shared<nano::frontier_req_server> (connection, std::move (req2)));
	ASSERT_TRUE (request2->current.is_zero ());
}

TEST (frontier_req, time_cutoff)
{
	nano::system system (1);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	auto req = std::make_unique<nano::frontier_req> ();
	req->start.clear ();
	req->age = 3;
	req->count = std::numeric_limits<decltype (req->count)>::max ();
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::frontier_req_server> (connection, std::move (req)));
	ASSERT_EQ (nano::dev::genesis_key.pub, request->current);
	ASSERT_EQ (nano::dev::genesis->hash (), request->frontier);
	// Wait 4 seconds until age of account will be > 3 seconds
	std::this_thread::sleep_for (std::chrono::milliseconds (4100));
	auto req2 (std::make_unique<nano::frontier_req> ());
	req2->start.clear ();
	req2->age = 3;
	req2->count = std::numeric_limits<decltype (req2->count)>::max ();
	auto connection2 (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));
	connection2->requests.push (std::unique_ptr<nano::message>{});
	auto request2 (std::make_shared<nano::frontier_req_server> (connection, std::move (req2)));
	ASSERT_TRUE (request2->frontier.is_zero ());
}

TEST (frontier_req, confirmed_frontier)
{
	nano::system system (1);
	auto node1 = system.nodes[0];
	nano::raw_key priv_key;
	// Public key before genesis in accounts table
	while (nano::pub_key (priv_key).number () >= nano::dev::genesis_key.pub.number ())
	{
		priv_key = nano::keypair ().prv;
	}
	nano::keypair key_before_genesis (priv_key.to_string ());
	// Public key after genesis in accounts table
	while (nano::pub_key (priv_key).number () <= nano::dev::genesis_key.pub.number ())
	{
		priv_key = nano::keypair ().prv;
	}
	nano::keypair key_after_genesis (priv_key.to_string ());
	nano::state_block_builder builder;

	auto send1 = builder
				 .account (nano::dev::genesis_key.pub)
				 .previous (nano::dev::genesis->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - nano::Gxrb_ratio)
				 .link (key_before_genesis.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (0)
				 .build_shared ();
	node1->work_generate_blocking (*send1);
	ASSERT_EQ (nano::process_result::progress, node1->process (*send1).code);
	auto send2 = builder
				 .make_block ()
				 .account (nano::dev::genesis_key.pub)
				 .previous (send1->hash ())
				 .representative (nano::dev::genesis_key.pub)
				 .balance (nano::dev::genesis_amount - 2 * nano::Gxrb_ratio)
				 .link (key_after_genesis.pub)
				 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				 .work (0)
				 .build_shared ();
	node1->work_generate_blocking (*send2);
	ASSERT_EQ (nano::process_result::progress, node1->process (*send2).code);
	auto receive1 = builder
					.make_block ()
					.account (key_before_genesis.pub)
					.previous (0)
					.representative (nano::dev::genesis_key.pub)
					.balance (nano::Gxrb_ratio)
					.link (send1->hash ())
					.sign (key_before_genesis.prv, key_before_genesis.pub)
					.work (0)
					.build_shared ();
	node1->work_generate_blocking (*receive1);
	ASSERT_EQ (nano::process_result::progress, node1->process (*receive1).code);
	auto receive2 = builder
					.make_block ()
					.account (key_after_genesis.pub)
					.previous (0)
					.representative (nano::dev::genesis_key.pub)
					.balance (nano::Gxrb_ratio)
					.link (send2->hash ())
					.sign (key_after_genesis.prv, key_after_genesis.pub)
					.work (0)
					.build_shared ();
	node1->work_generate_blocking (*receive2);
	ASSERT_EQ (nano::process_result::progress, node1->process (*receive2).code);

	// Request for all accounts (confirmed only)
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*node1), node1));
	auto req = std::make_unique<nano::frontier_req> ();
	req->start.clear ();
	req->age = std::numeric_limits<decltype (req->age)>::max ();
	req->count = std::numeric_limits<decltype (req->count)>::max ();
	ASSERT_FALSE (req->header.frontier_req_is_only_confirmed_present ());
	req->header.flag_set (nano::message_header::frontier_req_only_confirmed);
	ASSERT_TRUE (req->header.frontier_req_is_only_confirmed_present ());
	connection->requests.push (std::unique_ptr<nano::message>{});
	auto request (std::make_shared<nano::frontier_req_server> (connection, std::move (req)));
	ASSERT_EQ (nano::dev::genesis_key.pub, request->current);
	ASSERT_EQ (nano::dev::genesis->hash (), request->frontier);

	// Request starting with account before genesis (confirmed only)
	auto connection2 (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*node1), node1));
	auto req2 = std::make_unique<nano::frontier_req> ();
	req2->start = key_before_genesis.pub;
	req2->age = std::numeric_limits<decltype (req2->age)>::max ();
	req2->count = std::numeric_limits<decltype (req2->count)>::max ();
	ASSERT_FALSE (req2->header.frontier_req_is_only_confirmed_present ());
	req2->header.flag_set (nano::message_header::frontier_req_only_confirmed);
	ASSERT_TRUE (req2->header.frontier_req_is_only_confirmed_present ());
	connection2->requests.push (std::unique_ptr<nano::message>{});
	auto request2 (std::make_shared<nano::frontier_req_server> (connection2, std::move (req2)));
	ASSERT_EQ (nano::dev::genesis_key.pub, request2->current);
	ASSERT_EQ (nano::dev::genesis->hash (), request2->frontier);

	// Request starting with account after genesis (confirmed only)
	auto connection3 (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*node1), node1));
	auto req3 = std::make_unique<nano::frontier_req> ();
	req3->start = key_after_genesis.pub;
	req3->age = std::numeric_limits<decltype (req3->age)>::max ();
	req3->count = std::numeric_limits<decltype (req3->count)>::max ();
	ASSERT_FALSE (req3->header.frontier_req_is_only_confirmed_present ());
	req3->header.flag_set (nano::message_header::frontier_req_only_confirmed);
	ASSERT_TRUE (req3->header.frontier_req_is_only_confirmed_present ());
	connection3->requests.push (std::unique_ptr<nano::message>{});
	auto request3 (std::make_shared<nano::frontier_req_server> (connection3, std::move (req3)));
	ASSERT_TRUE (request3->current.is_zero ());
	ASSERT_TRUE (request3->frontier.is_zero ());

	// Request for all accounts (unconfirmed blocks)
	auto connection4 (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*node1), node1));
	auto req4 = std::make_unique<nano::frontier_req> ();
	req4->start.clear ();
	req4->age = std::numeric_limits<decltype (req4->age)>::max ();
	req4->count = std::numeric_limits<decltype (req4->count)>::max ();
	ASSERT_FALSE (req4->header.frontier_req_is_only_confirmed_present ());
	connection4->requests.push (std::unique_ptr<nano::message>{});
	auto request4 (std::make_shared<nano::frontier_req_server> (connection4, std::move (req4)));
	ASSERT_EQ (key_before_genesis.pub, request4->current);
	ASSERT_EQ (receive1->hash (), request4->frontier);

	// Request starting with account after genesis (unconfirmed blocks)
	auto connection5 (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*node1), node1));
	auto req5 = std::make_unique<nano::frontier_req> ();
	req5->start = key_after_genesis.pub;
	req5->age = std::numeric_limits<decltype (req5->age)>::max ();
	req5->count = std::numeric_limits<decltype (req5->count)>::max ();
	ASSERT_FALSE (req5->header.frontier_req_is_only_confirmed_present ());
	connection5->requests.push (std::unique_ptr<nano::message>{});
	auto request5 (std::make_shared<nano::frontier_req_server> (connection5, std::move (req5)));
	ASSERT_EQ (key_after_genesis.pub, request5->current);
	ASSERT_EQ (receive2->hash (), request5->frontier);

	// Confirm account before genesis (confirmed only)
	nano::blocks_confirm (*node1, { send1, receive1 }, true);
	ASSERT_TIMELY (5s, node1->block_confirmed (send1->hash ()) && node1->block_confirmed (receive1->hash ()));
	auto connection6 (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*node1), node1));
	auto req6 = std::make_unique<nano::frontier_req> ();
	req6->start = key_before_genesis.pub;
	req6->age = std::numeric_limits<decltype (req6->age)>::max ();
	req6->count = std::numeric_limits<decltype (req6->count)>::max ();
	ASSERT_FALSE (req6->header.frontier_req_is_only_confirmed_present ());
	req6->header.flag_set (nano::message_header::frontier_req_only_confirmed);
	ASSERT_TRUE (req6->header.frontier_req_is_only_confirmed_present ());
	connection6->requests.push (std::unique_ptr<nano::message>{});
	auto request6 (std::make_shared<nano::frontier_req_server> (connection6, std::move (req6)));
	ASSERT_EQ (key_before_genesis.pub, request6->current);
	ASSERT_EQ (receive1->hash (), request6->frontier);

	// Confirm account after genesis (confirmed only)
	nano::blocks_confirm (*node1, { send2, receive2 }, true);
	ASSERT_TIMELY (5s, node1->block_confirmed (send2->hash ()) && node1->block_confirmed (receive2->hash ()));
	auto connection7 (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*node1), node1));
	auto req7 = std::make_unique<nano::frontier_req> ();
	req7->start = key_after_genesis.pub;
	req7->age = std::numeric_limits<decltype (req7->age)>::max ();
	req7->count = std::numeric_limits<decltype (req7->count)>::max ();
	ASSERT_FALSE (req7->header.frontier_req_is_only_confirmed_present ());
	req7->header.flag_set (nano::message_header::frontier_req_only_confirmed);
	ASSERT_TRUE (req7->header.frontier_req_is_only_confirmed_present ());
	connection7->requests.push (std::unique_ptr<nano::message>{});
	auto request7 (std::make_shared<nano::frontier_req_server> (connection7, std::move (req7)));
	ASSERT_EQ (key_after_genesis.pub, request7->current);
	ASSERT_EQ (receive2->hash (), request7->frontier);
}

TEST (bulk, genesis)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.disable_lazy_bootstrap = true;
	auto node1 = system.add_node (config, node_flags);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto node2 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	ASSERT_FALSE (node2->init_error ());
	nano::block_hash latest1 (node1->latest (nano::dev::genesis_key.pub));
	nano::block_hash latest2 (node2->latest (nano::dev::genesis_key.pub));
	ASSERT_EQ (latest1, latest2);
	nano::keypair key2;
	auto send (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, 100));
	ASSERT_NE (nullptr, send);
	nano::block_hash latest3 (node1->latest (nano::dev::genesis_key.pub));
	ASSERT_NE (latest1, latest3);

	node2->bootstrap_initiator.bootstrap (node1->network.endpoint (), false);
	ASSERT_TIMELY (10s, node2->latest (nano::dev::genesis_key.pub) == node1->latest (nano::dev::genesis_key.pub));
	ASSERT_EQ (node2->latest (nano::dev::genesis_key.pub), node1->latest (nano::dev::genesis_key.pub));
	node2->stop ();
}

TEST (bulk, offline_send)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.disable_lazy_bootstrap = true;
	auto node1 = system.add_node (config, node_flags);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto node2 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work));
	ASSERT_FALSE (node2->init_error ());
	node2->start ();
	system.nodes.push_back (node2);
	nano::keypair key2;
	auto wallet (node2->wallets.create (nano::random_wallet_id ()));
	wallet->insert_adhoc (key2.prv);
	auto send1 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, node1->config.receive_minimum.number ()));
	ASSERT_NE (nullptr, send1);
	ASSERT_NE (std::numeric_limits<nano::uint256_t>::max (), node1->balance (nano::dev::genesis_key.pub));
	node1->block_processor.flush ();
	node1->scheduler.flush ();
	// Wait to finish election background tasks
	ASSERT_TIMELY (10s, node1->active.empty ());
	ASSERT_TIMELY (10s, node1->block_confirmed (send1->hash ()));
	// Initiate bootstrap
	node2->bootstrap_initiator.bootstrap (node1->network.endpoint ());
	// Nodes should find each other
	system.deadline_set (10s);
	do
	{
		ASSERT_NO_ERROR (system.poll ());
	} while (node1->network.empty () || node2->network.empty ());
	// Send block arrival via bootstrap
	ASSERT_TIMELY (10s, node2->balance (nano::dev::genesis_key.pub) != std::numeric_limits<nano::uint256_t>::max ());
	// Receiving send block
	ASSERT_TIMELY (20s, node2->balance (key2.pub) == node1->config.receive_minimum.number ());
	node2->stop ();
}

TEST (bulk, genesis_pruning)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	config.enable_voting = false; // Remove after allowing pruned voting
	nano::node_flags node_flags;
	node_flags.disable_bootstrap_bulk_push_client = true;
	node_flags.disable_lazy_bootstrap = true;
	node_flags.disable_ongoing_bootstrap = true;
	node_flags.enable_pruning = true;
	auto node1 = system.add_node (config, node_flags);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	node_flags.enable_pruning = false;
	auto node2 (std::make_shared<nano::node> (system.io_ctx, nano::get_available_port (), nano::unique_path (), system.logging, system.work, node_flags));
	ASSERT_FALSE (node2->init_error ());
	nano::block_hash latest1 (node1->latest (nano::dev::genesis_key.pub));
	nano::block_hash latest2 (node2->latest (nano::dev::genesis_key.pub));
	ASSERT_EQ (latest1, latest2);
	nano::keypair key2;
	auto send1 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, 100));
	ASSERT_NE (nullptr, send1);
	auto send2 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, 100));
	ASSERT_NE (nullptr, send2);
	auto send3 (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, 100));
	ASSERT_NE (nullptr, send3);
	{
		auto transaction (node1->wallets.tx_begin_write ());
		system.wallet (0)->store.erase (transaction, nano::dev::genesis_key.pub);
	}
	nano::block_hash latest3 (node1->latest (nano::dev::genesis_key.pub));
	ASSERT_NE (latest1, latest3);
	ASSERT_EQ (send3->hash (), latest3);
	// Confirm last block to prune previous
	{
		auto election = node1->active.election (send1->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
	}
	ASSERT_TIMELY (2s, node1->block_confirmed (send1->hash ()) && node1->active.active (send2->qualified_root ()));
	ASSERT_EQ (0, node1->ledger.cache.pruned_count);
	{
		auto election = node1->active.election (send2->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
	}
	ASSERT_TIMELY (2s, node1->block_confirmed (send2->hash ()) && node1->active.active (send3->qualified_root ()));
	ASSERT_EQ (0, node1->ledger.cache.pruned_count);
	{
		auto election = node1->active.election (send3->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
	}
	ASSERT_TIMELY (2s, node1->active.empty () && node1->block_confirmed (send3->hash ()));
	node1->ledger_pruning (2, false, false);
	ASSERT_EQ (2, node1->ledger.cache.pruned_count);
	ASSERT_EQ (4, node1->ledger.cache.block_count);
	ASSERT_TRUE (node1->ledger.block_or_pruned_exists (send1->hash ())); // true for pruned
	ASSERT_TRUE (node1->ledger.block_or_pruned_exists (send2->hash ())); // true for pruned
	ASSERT_TRUE (node1->ledger.block_or_pruned_exists (send3->hash ()));
	// Bootstrap with missing blocks for node2
	node2->bootstrap_initiator.bootstrap (node1->network.endpoint (), false);
	node2->network.merge_peer (node1->network.endpoint ());
	ASSERT_TIMELY (25s, node2->stats.count (nano::stat::type::bootstrap, nano::stat::detail::initiate, nano::stat::dir::out) >= 1 && !node2->bootstrap_initiator.in_progress ());
	// node2 still missing blocks
	ASSERT_EQ (1, node2->ledger.cache.block_count);
	{
		auto transaction (node2->store.tx_begin_write ());
		node2->store.unchecked.clear (transaction);
	}
	// Insert pruned blocks
	node2->process_active (send1);
	node2->process_active (send2);
	node2->block_processor.flush ();
	ASSERT_EQ (3, node2->ledger.cache.block_count);
	// New bootstrap
	ASSERT_TIMELY (5s, node2->bootstrap_initiator.connections->connections_count == 0);
	node2->bootstrap_initiator.bootstrap (node1->network.endpoint (), false);
	ASSERT_TIMELY (10s, node2->latest (nano::dev::genesis_key.pub) == node1->latest (nano::dev::genesis_key.pub));
	ASSERT_EQ (node2->latest (nano::dev::genesis_key.pub), node1->latest (nano::dev::genesis_key.pub));
	node2->stop ();
}

TEST (bulk_pull_account, basics)
{
	nano::system system (1);
	system.nodes[0]->config.receive_minimum = 20;
	nano::keypair key1;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	system.wallet (0)->insert_adhoc (key1.prv);
	auto send1 (system.wallet (0)->send_action (nano::dev::genesis->account (), key1.pub, 25));
	auto send2 (system.wallet (0)->send_action (nano::dev::genesis->account (), key1.pub, 10));
	auto send3 (system.wallet (0)->send_action (nano::dev::genesis->account (), key1.pub, 2));
	ASSERT_TIMELY (5s, system.nodes[0]->balance (key1.pub) == 25);
	auto connection (std::make_shared<nano::bootstrap_server> (std::make_shared<nano::socket> (*system.nodes[0]), system.nodes[0]));

	{
		auto req = std::make_unique<nano::bulk_pull_account> ();
		req->account = key1.pub;
		req->minimum_amount = 5;
		req->flags = nano::bulk_pull_account_flags ();
		connection->requests.push (std::unique_ptr<nano::message>{});
		auto request (std::make_shared<nano::bulk_pull_account_server> (connection, std::move (req)));
		ASSERT_FALSE (request->invalid_request);
		ASSERT_FALSE (request->pending_include_address);
		ASSERT_FALSE (request->pending_address_only);
		ASSERT_EQ (request->current_key.account, key1.pub);
		ASSERT_EQ (request->current_key.hash, 0);
		auto block_data (request->get_next ());
		ASSERT_EQ (send2->hash (), block_data.first.get ()->hash);
		ASSERT_EQ (nano::uint128_union (10), block_data.second.get ()->amount);
		ASSERT_EQ (nano::dev::genesis->account (), block_data.second.get ()->source);
		ASSERT_EQ (nullptr, request->get_next ().first.get ());
	}

	{
		auto req = std::make_unique<nano::bulk_pull_account> ();
		req->account = key1.pub;
		req->minimum_amount = 0;
		req->flags = nano::bulk_pull_account_flags::pending_address_only;
		auto request (std::make_shared<nano::bulk_pull_account_server> (connection, std::move (req)));
		ASSERT_TRUE (request->pending_address_only);
		auto block_data (request->get_next ());
		ASSERT_NE (nullptr, block_data.first.get ());
		ASSERT_NE (nullptr, block_data.second.get ());
		ASSERT_EQ (nano::dev::genesis->account (), block_data.second.get ()->source);
		block_data = request->get_next ();
		ASSERT_EQ (nullptr, block_data.first.get ());
		ASSERT_EQ (nullptr, block_data.second.get ());
	}
}
