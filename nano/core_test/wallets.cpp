#include <nano/node/testing.hpp>
#include <nano/secure/versioning.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

TEST (wallets, open_create)
{
	nano::system system (1);
	bool error (false);
	nano::wallets wallets (error, *system.nodes[0]);
	ASSERT_FALSE (error);
	ASSERT_EQ (1, wallets.items.size ()); // it starts out with a default wallet
	auto id = nano::random_wallet_id ();
	ASSERT_EQ (nullptr, wallets.open (id));
	auto wallet (wallets.create (id));
	ASSERT_NE (nullptr, wallet);
	ASSERT_EQ (wallet, wallets.open (id));
}

TEST (wallets, open_existing)
{
	nano::system system (1);
	auto id (nano::random_wallet_id ());
	{
		bool error (false);
		nano::wallets wallets (error, *system.nodes[0]);
		ASSERT_FALSE (error);
		ASSERT_EQ (1, wallets.items.size ());
		auto wallet (wallets.create (id));
		ASSERT_NE (nullptr, wallet);
		ASSERT_EQ (wallet, wallets.open (id));
		nano::raw_key password;
		password.data.clear ();
		system.deadline_set (10s);
		while (password.data == 0)
		{
			ASSERT_NO_ERROR (system.poll ());
			wallet->store.password.value (password);
		}
	}
	{
		bool error (false);
		nano::wallets wallets (error, *system.nodes[0]);
		ASSERT_FALSE (error);
		ASSERT_EQ (2, wallets.items.size ());
		ASSERT_NE (nullptr, wallets.open (id));
	}
}

TEST (wallets, remove)
{
	nano::system system (1);
	nano::wallet_id one (1);
	{
		bool error (false);
		nano::wallets wallets (error, *system.nodes[0]);
		ASSERT_FALSE (error);
		ASSERT_EQ (1, wallets.items.size ());
		auto wallet (wallets.create (one));
		ASSERT_NE (nullptr, wallet);
		ASSERT_EQ (2, wallets.items.size ());
		wallets.destroy (one);
		ASSERT_EQ (1, wallets.items.size ());
	}
	{
		bool error (false);
		nano::wallets wallets (error, *system.nodes[0]);
		ASSERT_FALSE (error);
		ASSERT_EQ (1, wallets.items.size ());
	}
}

// Keeps breaking whenever we add new DBs
TEST (wallets, DISABLED_wallet_create_max)
{
	nano::system system (1);
	bool error (false);
	nano::wallets wallets (error, *system.nodes[0]);
	const int nonWalletDbs = 19;
	for (int i = 0; i < system.nodes[0]->config.deprecated_lmdb_max_dbs - nonWalletDbs; i++)
	{
		auto wallet_id = nano::random_wallet_id ();
		auto wallet = wallets.create (wallet_id);
		auto existing = wallets.items.find (wallet_id);
		ASSERT_TRUE (existing != wallets.items.end ());
		nano::raw_key seed;
		seed.data = 0;
		auto transaction (system.nodes[0]->store.tx_begin_write ());
		existing->second->store.seed_set (transaction, seed);
	}
	auto wallet_id = nano::random_wallet_id ();
	wallets.create (wallet_id);
	auto existing = wallets.items.find (wallet_id);
	ASSERT_TRUE (existing == wallets.items.end ());
}

TEST (wallets, reload)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::wallet_id one (1);
	bool error (false);
	ASSERT_FALSE (error);
	ASSERT_EQ (1, node1.wallets.items.size ());
	{
		nano::lock_guard<std::mutex> lock_wallet (node1.wallets.mutex);
		nano::environment env{ node1.path };
		nano::node_flags flags;
		auto error{ env.apply_overrides (flags, nano::environment::purpose::inactive) };
		nano::inactive_node node (env, flags);
		auto wallet (node.node->wallets.create (one));
		ASSERT_NE (wallet, nullptr);
	}
	ASSERT_TIMELY (5s, node1.wallets.open (one) != nullptr);
	ASSERT_EQ (2, node1.wallets.items.size ());
}

TEST (wallets, vote_minimum)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::keypair key1;
	nano::keypair key2;
	nano::genesis genesis;
	nano::state_block send1 (nano::dev_genesis_key.pub, genesis.hash (), nano::dev_genesis_key.pub, std::numeric_limits<nano::uint128_t>::max () - node1.config.vote_minimum.number (), key1.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, node1.process (send1).code);
	nano::state_block open1 (key1.pub, 0, key1.pub, node1.config.vote_minimum.number (), send1.hash (), key1.prv, key1.pub, *system.env.work.generate (key1.pub));
	ASSERT_EQ (nano::process_result::progress, node1.process (open1).code);
	// send2 with amount vote_minimum - 1 (not voting representative)
	nano::state_block send2 (nano::dev_genesis_key.pub, send1.hash (), nano::dev_genesis_key.pub, std::numeric_limits<nano::uint128_t>::max () - 2 * node1.config.vote_minimum.number () + 1, key2.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, node1.process (send2).code);
	nano::state_block open2 (key2.pub, 0, key2.pub, node1.config.vote_minimum.number () - 1, send2.hash (), key2.prv, key2.pub, *system.env.work.generate (key2.pub));
	ASSERT_EQ (nano::process_result::progress, node1.process (open2).code);
	auto wallet (node1.wallets.items.begin ()->second);
	ASSERT_EQ (0, wallet->representatives.size ());
	wallet->insert_adhoc (nano::dev_genesis_key.prv);
	wallet->insert_adhoc (key1.prv);
	wallet->insert_adhoc (key2.prv);
	node1.wallets.compute_reps ();
	ASSERT_EQ (2, wallet->representatives.size ());
}

TEST (wallets, exists)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	nano::keypair key1;
	nano::keypair key2;
	{
		auto transaction (node.wallets.tx_begin_read ());
		ASSERT_FALSE (node.wallets.exists (transaction, key1.pub));
		ASSERT_FALSE (node.wallets.exists (transaction, key2.pub));
	}
	system.wallet (0)->insert_adhoc (key1.prv);
	{
		auto transaction (node.wallets.tx_begin_read ());
		ASSERT_TRUE (node.wallets.exists (transaction, key1.pub));
		ASSERT_FALSE (node.wallets.exists (transaction, key2.pub));
	}
	system.wallet (0)->insert_adhoc (key2.prv);
	{
		auto transaction (node.wallets.tx_begin_read ());
		ASSERT_TRUE (node.wallets.exists (transaction, key1.pub));
		ASSERT_TRUE (node.wallets.exists (transaction, key2.pub));
	}
}
