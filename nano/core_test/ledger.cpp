#include <nano/core_test/testutil.hpp>
#include <nano/lib/stats.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/election.hpp>
#include <nano/node/testing.hpp>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

// Init returns an error if it can't open files at the path
TEST (ledger, store_error)
{
	nano::logger_mt logger;
	nano::mdb_store store (logger, boost::filesystem::path ("///"));
	ASSERT_TRUE (store.init_error ());
	nano::stat stats;
	nano::ledger ledger (store, stats);
}

// Ledger can be initialized and returns a basic query for an empty account
TEST (ledger, empty)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::account account;
	auto transaction (store->tx_begin_read ());
	auto balance (ledger.account_balance (transaction, account));
	ASSERT_TRUE (balance.is_zero ());
}

// Genesis account should have the max balance on empty initialization
TEST (ledger, genesis_balance)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	auto balance (ledger.account_balance (transaction, nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount, balance);
	auto amount (ledger.amount (transaction, nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount, amount);
	nano::account_info info;
	ASSERT_FALSE (store->account_get (transaction, nano::genesis_account, info));
	// Frontier time should have been updated when genesis balance was added
	ASSERT_GE (nano::seconds_since_epoch (), info.modified);
	ASSERT_LT (nano::seconds_since_epoch () - info.modified, 10);
	// Genesis block should be confirmed by default
	uint64_t confirmation_height;
	ASSERT_FALSE (store->confirmation_height_get (transaction, nano::genesis_account, confirmation_height));
	ASSERT_EQ (confirmation_height, 1);
}

// All nodes in the system should agree on the genesis balance
TEST (system, system_genesis)
{
	nano::system system (2);
	for (auto & i : system.nodes)
	{
		auto transaction (i->store.tx_begin_read ());
		ASSERT_EQ (nano::genesis_amount, i->ledger.account_balance (transaction, nano::genesis_account));
	}
}

// Create a send block and publish it.
TEST (ledger, process_send)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	auto transaction (store->tx_begin_write ());
	nano::genesis genesis;
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
	nano::keypair key2;
	nano::send_block send (info1.head, key2.pub, 50, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	nano::block_hash hash1 (send.hash ());
	ASSERT_EQ (nano::test_genesis_key.pub, store->frontier_get (transaction, info1.head));
	ASSERT_EQ (1, info1.block_count);
	// This was a valid block, it should progress.
	auto return1 (ledger.process (transaction, send));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.amount (transaction, hash1));
	ASSERT_TRUE (store->frontier_get (transaction, info1.head).is_zero ());
	ASSERT_EQ (nano::test_genesis_key.pub, store->frontier_get (transaction, hash1));
	ASSERT_EQ (nano::process_result::progress, return1.code);
	ASSERT_EQ (nano::test_genesis_key.pub, return1.account);
	ASSERT_EQ (nano::genesis_amount - 50, return1.amount.number ());
	ASSERT_EQ (50, ledger.account_balance (transaction, nano::test_genesis_key.pub));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.account_pending (transaction, key2.pub));
	nano::account_info info2;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info2));
	ASSERT_EQ (2, info2.block_count);
	auto latest6 (store->block_get (transaction, info2.head));
	ASSERT_NE (nullptr, latest6);
	auto latest7 (dynamic_cast<nano::send_block *> (latest6.get ()));
	ASSERT_NE (nullptr, latest7);
	ASSERT_EQ (send, *latest7);
	// Create an open block opening an account accepting the send we just created
	nano::open_block open (hash1, key2.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	nano::block_hash hash2 (open.hash ());
	// This was a valid block, it should progress.
	auto return2 (ledger.process (transaction, open));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.amount (transaction, hash2));
	ASSERT_EQ (nano::process_result::progress, return2.code);
	ASSERT_EQ (key2.pub, return2.account);
	ASSERT_EQ (nano::genesis_amount - 50, return2.amount.number ());
	ASSERT_EQ (key2.pub, store->frontier_get (transaction, hash2));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.account_balance (transaction, key2.pub));
	ASSERT_EQ (0, ledger.account_pending (transaction, key2.pub));
	ASSERT_EQ (50, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.weight (key2.pub));
	nano::account_info info3;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info3));
	auto latest2 (store->block_get (transaction, info3.head));
	ASSERT_NE (nullptr, latest2);
	auto latest3 (dynamic_cast<nano::send_block *> (latest2.get ()));
	ASSERT_NE (nullptr, latest3);
	ASSERT_EQ (send, *latest3);
	nano::account_info info4;
	ASSERT_FALSE (store->account_get (transaction, key2.pub, info4));
	auto latest4 (store->block_get (transaction, info4.head));
	ASSERT_NE (nullptr, latest4);
	auto latest5 (dynamic_cast<nano::open_block *> (latest4.get ()));
	ASSERT_NE (nullptr, latest5);
	ASSERT_EQ (open, *latest5);
	ASSERT_FALSE (ledger.rollback (transaction, hash2));
	ASSERT_TRUE (store->frontier_get (transaction, hash2).is_zero ());
	nano::account_info info5;
	ASSERT_TRUE (ledger.store.account_get (transaction, key2.pub, info5));
	nano::pending_info pending1;
	ASSERT_FALSE (ledger.store.pending_get (transaction, nano::pending_key (key2.pub, hash1), pending1));
	ASSERT_EQ (nano::test_genesis_key.pub, pending1.source);
	ASSERT_EQ (nano::genesis_amount - 50, pending1.amount.number ());
	ASSERT_EQ (0, ledger.account_balance (transaction, key2.pub));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.account_pending (transaction, key2.pub));
	ASSERT_EQ (50, ledger.account_balance (transaction, nano::test_genesis_key.pub));
	ASSERT_EQ (50, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (0, ledger.weight (key2.pub));
	nano::account_info info6;
	ASSERT_FALSE (ledger.store.account_get (transaction, nano::test_genesis_key.pub, info6));
	ASSERT_EQ (hash1, info6.head);
	ASSERT_FALSE (ledger.rollback (transaction, info6.head));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (nano::test_genesis_key.pub, store->frontier_get (transaction, info1.head));
	ASSERT_TRUE (store->frontier_get (transaction, hash1).is_zero ());
	nano::account_info info7;
	ASSERT_FALSE (ledger.store.account_get (transaction, nano::test_genesis_key.pub, info7));
	ASSERT_EQ (1, info7.block_count);
	ASSERT_EQ (info1.head, info7.head);
	nano::pending_info pending2;
	ASSERT_TRUE (ledger.store.pending_get (transaction, nano::pending_key (key2.pub, hash1), pending2));
	ASSERT_EQ (nano::genesis_amount, ledger.account_balance (transaction, nano::test_genesis_key.pub));
	ASSERT_EQ (0, ledger.account_pending (transaction, key2.pub));
}

TEST (ledger, process_receive)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
	nano::keypair key2;
	nano::send_block send (info1.head, key2.pub, 50, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	nano::block_hash hash1 (send.hash ());
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	nano::keypair key3;
	nano::open_block open (hash1, key3.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	nano::block_hash hash2 (open.hash ());
	auto return1 (ledger.process (transaction, open));
	ASSERT_EQ (nano::process_result::progress, return1.code);
	ASSERT_EQ (key2.pub, return1.account);
	ASSERT_EQ (nano::genesis_amount - 50, return1.amount.number ());
	ASSERT_EQ (nano::genesis_amount - 50, ledger.weight (key3.pub));
	nano::send_block send2 (hash1, key2.pub, 25, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (hash1));
	nano::block_hash hash3 (send2.hash ());
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send2).code);
	nano::receive_block receive (hash2, hash3, key2.prv, key2.pub, *pool.generate (hash2));
	auto hash4 (receive.hash ());
	ASSERT_EQ (key2.pub, store->frontier_get (transaction, hash2));
	auto return2 (ledger.process (transaction, receive));
	ASSERT_EQ (25, ledger.amount (transaction, hash4));
	ASSERT_TRUE (store->frontier_get (transaction, hash2).is_zero ());
	ASSERT_EQ (key2.pub, store->frontier_get (transaction, hash4));
	ASSERT_EQ (nano::process_result::progress, return2.code);
	ASSERT_EQ (key2.pub, return2.account);
	ASSERT_EQ (25, return2.amount.number ());
	ASSERT_EQ (hash4, ledger.latest (transaction, key2.pub));
	ASSERT_EQ (25, ledger.account_balance (transaction, nano::test_genesis_key.pub));
	ASSERT_EQ (0, ledger.account_pending (transaction, key2.pub));
	ASSERT_EQ (nano::genesis_amount - 25, ledger.account_balance (transaction, key2.pub));
	ASSERT_EQ (nano::genesis_amount - 25, ledger.weight (key3.pub));
	ASSERT_FALSE (ledger.rollback (transaction, hash4));
	ASSERT_TRUE (store->block_successor (transaction, hash2).is_zero ());
	ASSERT_EQ (key2.pub, store->frontier_get (transaction, hash2));
	ASSERT_TRUE (store->frontier_get (transaction, hash4).is_zero ());
	ASSERT_EQ (25, ledger.account_balance (transaction, nano::test_genesis_key.pub));
	ASSERT_EQ (25, ledger.account_pending (transaction, key2.pub));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.account_balance (transaction, key2.pub));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.weight (key3.pub));
	ASSERT_EQ (hash2, ledger.latest (transaction, key2.pub));
	nano::pending_info pending1;
	ASSERT_FALSE (ledger.store.pending_get (transaction, nano::pending_key (key2.pub, hash3), pending1));
	ASSERT_EQ (nano::test_genesis_key.pub, pending1.source);
	ASSERT_EQ (25, pending1.amount.number ());
}

TEST (ledger, rollback_receiver)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
	nano::keypair key2;
	nano::send_block send (info1.head, key2.pub, 50, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	nano::block_hash hash1 (send.hash ());
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	nano::keypair key3;
	nano::open_block open (hash1, key3.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	nano::block_hash hash2 (open.hash ());
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open).code);
	ASSERT_EQ (hash2, ledger.latest (transaction, key2.pub));
	ASSERT_EQ (50, ledger.account_balance (transaction, nano::test_genesis_key.pub));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.account_balance (transaction, key2.pub));
	ASSERT_EQ (50, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (0, ledger.weight (key2.pub));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.weight (key3.pub));
	ASSERT_FALSE (ledger.rollback (transaction, hash1));
	ASSERT_EQ (nano::genesis_amount, ledger.account_balance (transaction, nano::test_genesis_key.pub));
	ASSERT_EQ (0, ledger.account_balance (transaction, key2.pub));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (0, ledger.weight (key2.pub));
	ASSERT_EQ (0, ledger.weight (key3.pub));
	nano::account_info info2;
	ASSERT_TRUE (ledger.store.account_get (transaction, key2.pub, info2));
	nano::pending_info pending1;
	ASSERT_TRUE (ledger.store.pending_get (transaction, nano::pending_key (key2.pub, info2.head), pending1));
}

TEST (ledger, rollback_representation)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key5;
	nano::change_block change1 (genesis.hash (), key5.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, change1).code);
	nano::keypair key3;
	nano::change_block change2 (change1.hash (), key3.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (change1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, change2).code);
	nano::keypair key2;
	nano::send_block send1 (change2.hash (), key2.pub, 50, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (change2.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::keypair key4;
	nano::open_block open (send1.hash (), key4.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open).code);
	nano::send_block send2 (send1.hash (), key2.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send2).code);
	nano::receive_block receive1 (open.hash (), send2.hash (), key2.prv, key2.pub, *pool.generate (open.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive1).code);
	ASSERT_EQ (1, ledger.weight (key3.pub));
	ASSERT_EQ (nano::genesis_amount - 1, ledger.weight (key4.pub));
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, key2.pub, info1));
	ASSERT_EQ (key4.pub, info1.representative);
	ASSERT_FALSE (ledger.rollback (transaction, receive1.hash ()));
	nano::account_info info2;
	ASSERT_FALSE (store->account_get (transaction, key2.pub, info2));
	ASSERT_EQ (key4.pub, info2.representative);
	ASSERT_EQ (0, ledger.weight (key2.pub));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.weight (key4.pub));
	ASSERT_FALSE (ledger.rollback (transaction, open.hash ()));
	ASSERT_EQ (1, ledger.weight (key3.pub));
	ASSERT_EQ (0, ledger.weight (key4.pub));
	ledger.rollback (transaction, send1.hash ());
	ASSERT_EQ (nano::genesis_amount, ledger.weight (key3.pub));
	nano::account_info info3;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info3));
	ASSERT_EQ (key3.pub, info3.representative);
	ASSERT_FALSE (ledger.rollback (transaction, change2.hash ()));
	nano::account_info info4;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info4));
	ASSERT_EQ (key5.pub, info4.representative);
	ASSERT_EQ (nano::genesis_amount, ledger.weight (key5.pub));
	ASSERT_EQ (0, ledger.weight (key3.pub));
}

TEST (ledger, receive_rollback)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::send_block send (genesis.hash (), nano::test_genesis_key.pub, nano::genesis_amount - nano::Gxrb_ratio, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	nano::receive_block receive (send.hash (), send.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive).code);
	ASSERT_FALSE (ledger.rollback (transaction, receive.hash ()));
}

TEST (ledger, process_duplicate)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
	nano::keypair key2;
	nano::send_block send (info1.head, key2.pub, 50, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	nano::block_hash hash1 (send.hash ());
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	ASSERT_EQ (nano::process_result::old, ledger.process (transaction, send).code);
	nano::open_block open (hash1, 1, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open).code);
	ASSERT_EQ (nano::process_result::old, ledger.process (transaction, open).code);
}

TEST (ledger, representative_genesis)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	auto latest (ledger.latest (transaction, nano::test_genesis_key.pub));
	ASSERT_FALSE (latest.is_zero ());
	ASSERT_EQ (genesis.open->hash (), ledger.representative (transaction, latest));
}

TEST (ledger, weight)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
}

TEST (ledger, representative_change)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::keypair key2;
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (0, ledger.weight (key2.pub));
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
	nano::change_block block (info1.head, key2.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	ASSERT_EQ (nano::test_genesis_key.pub, store->frontier_get (transaction, info1.head));
	auto return1 (ledger.process (transaction, block));
	ASSERT_EQ (0, ledger.amount (transaction, block.hash ()));
	ASSERT_TRUE (store->frontier_get (transaction, info1.head).is_zero ());
	ASSERT_EQ (nano::test_genesis_key.pub, store->frontier_get (transaction, block.hash ()));
	ASSERT_EQ (nano::process_result::progress, return1.code);
	ASSERT_EQ (nano::test_genesis_key.pub, return1.account);
	ASSERT_EQ (0, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (key2.pub));
	nano::account_info info2;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info2));
	ASSERT_EQ (block.hash (), info2.head);
	ASSERT_FALSE (ledger.rollback (transaction, info2.head));
	ASSERT_EQ (nano::test_genesis_key.pub, store->frontier_get (transaction, info1.head));
	ASSERT_TRUE (store->frontier_get (transaction, block.hash ()).is_zero ());
	nano::account_info info3;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info3));
	ASSERT_EQ (info1.head, info3.head);
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (0, ledger.weight (key2.pub));
}

TEST (ledger, send_fork)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::keypair key2;
	nano::keypair key3;
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
	nano::send_block block (info1.head, key2.pub, 100, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block).code);
	nano::send_block block2 (info1.head, key3.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, block2).code);
}

TEST (ledger, receive_fork)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::keypair key2;
	nano::keypair key3;
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
	nano::send_block block (info1.head, key2.pub, 100, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block).code);
	nano::open_block block2 (block.hash (), key2.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block2).code);
	nano::change_block block3 (block2.hash (), key3.pub, key2.prv, key2.pub, *pool.generate (block2.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block3).code);
	nano::send_block block4 (block.hash (), key2.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block4).code);
	nano::receive_block block5 (block2.hash (), block4.hash (), key2.prv, key2.pub, *pool.generate (block2.hash ()));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, block5).code);
}

TEST (ledger, open_fork)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::keypair key2;
	nano::keypair key3;
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
	nano::send_block block (info1.head, key2.pub, 100, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block).code);
	nano::open_block block2 (block.hash (), key2.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block2).code);
	nano::open_block block3 (block.hash (), key3.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, block3).code);
}

TEST (system, DISABLED_generate_send_existing)
{
	nano::system system (1);
	nano::thread_runner runner (system.io_ctx, system.nodes[0]->config.io_threads);
	system.wallet (0)->insert_adhoc (nano::test_genesis_key.prv);
	nano::keypair stake_preserver;
	auto send_block (system.wallet (0)->send_action (nano::genesis_account, stake_preserver.pub, nano::genesis_amount / 3 * 2, true));
	nano::account_info info1;
	{
		auto transaction (system.nodes[0]->store.tx_begin_read ());
		ASSERT_FALSE (system.nodes[0]->store.account_get (transaction, nano::test_genesis_key.pub, info1));
	}
	std::vector<nano::account> accounts;
	accounts.push_back (nano::test_genesis_key.pub);
	system.generate_send_existing (*system.nodes[0], accounts);
	// Have stake_preserver receive funds after generate_send_existing so it isn't chosen as the destination
	{
		auto transaction (system.nodes[0]->store.tx_begin_write ());
		auto open_block (std::make_shared<nano::open_block> (send_block->hash (), nano::genesis_account, stake_preserver.pub, stake_preserver.prv, stake_preserver.pub, 0));
		system.nodes[0]->work_generate_blocking (*open_block);
		ASSERT_EQ (nano::process_result::progress, system.nodes[0]->ledger.process (transaction, *open_block).code);
	}
	ASSERT_GT (system.nodes[0]->balance (stake_preserver.pub), system.nodes[0]->balance (nano::genesis_account));
	nano::account_info info2;
	{
		auto transaction (system.nodes[0]->store.tx_begin_read ());
		ASSERT_FALSE (system.nodes[0]->store.account_get (transaction, nano::test_genesis_key.pub, info2));
	}
	ASSERT_NE (info1.head, info2.head);
	system.deadline_set (15s);
	while (info2.block_count < info1.block_count + 2)
	{
		ASSERT_NO_ERROR (system.poll ());
		auto transaction (system.nodes[0]->store.tx_begin_read ());
		ASSERT_FALSE (system.nodes[0]->store.account_get (transaction, nano::test_genesis_key.pub, info2));
	}
	ASSERT_EQ (info1.block_count + 2, info2.block_count);
	ASSERT_EQ (info2.balance, nano::genesis_amount / 3);
	{
		auto transaction (system.nodes[0]->store.tx_begin_read ());
		ASSERT_NE (system.nodes[0]->ledger.amount (transaction, info2.head), 0);
	}
	system.stop ();
	runner.join ();
}

TEST (system, generate_send_new)
{
	nano::system system (1);
	nano::thread_runner runner (system.io_ctx, system.nodes[0]->config.io_threads);
	system.wallet (0)->insert_adhoc (nano::test_genesis_key.prv);
	{
		auto transaction (system.nodes[0]->store.tx_begin_read ());
		auto iterator1 (system.nodes[0]->store.latest_begin (transaction));
		ASSERT_NE (system.nodes[0]->store.latest_end (), iterator1);
		++iterator1;
		ASSERT_EQ (system.nodes[0]->store.latest_end (), iterator1);
	}
	nano::keypair stake_preserver;
	auto send_block (system.wallet (0)->send_action (nano::genesis_account, stake_preserver.pub, nano::genesis_amount / 3 * 2, true));
	{
		auto transaction (system.nodes[0]->store.tx_begin_write ());
		auto open_block (std::make_shared<nano::open_block> (send_block->hash (), nano::genesis_account, stake_preserver.pub, stake_preserver.prv, stake_preserver.pub, 0));
		system.nodes[0]->work_generate_blocking (*open_block);
		ASSERT_EQ (nano::process_result::progress, system.nodes[0]->ledger.process (transaction, *open_block).code);
	}
	ASSERT_GT (system.nodes[0]->balance (stake_preserver.pub), system.nodes[0]->balance (nano::genesis_account));
	std::vector<nano::account> accounts;
	accounts.push_back (nano::test_genesis_key.pub);
	system.generate_send_new (*system.nodes[0], accounts);
	nano::account new_account (0);
	{
		auto transaction (system.nodes[0]->wallets.tx_begin_read ());
		auto iterator2 (system.wallet (0)->store.begin (transaction));
		if (iterator2->first != nano::test_genesis_key.pub)
		{
			new_account = iterator2->first;
		}
		++iterator2;
		ASSERT_NE (system.wallet (0)->store.end (), iterator2);
		if (iterator2->first != nano::test_genesis_key.pub)
		{
			new_account = iterator2->first;
		}
		++iterator2;
		ASSERT_EQ (system.wallet (0)->store.end (), iterator2);
		ASSERT_FALSE (new_account.is_zero ());
	}
	system.deadline_set (10s);
	while (system.nodes[0]->balance (new_account) == 0)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
	system.stop ();
	runner.join ();
}

TEST (ledger, representation_changes)
{
	nano::keypair key1;
	nano::rep_weights rep_weights;
	ASSERT_EQ (0, rep_weights.representation_get (key1.pub));
	rep_weights.representation_put (key1.pub, 1);
	ASSERT_EQ (1, rep_weights.representation_get (key1.pub));
	rep_weights.representation_put (key1.pub, 2);
	ASSERT_EQ (2, rep_weights.representation_get (key1.pub));
}

TEST (ledger, representation)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	auto & rep_weights = ledger.rep_weights;
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	ASSERT_EQ (nano::genesis_amount, rep_weights.representation_get (nano::test_genesis_key.pub));
	nano::keypair key2;
	nano::send_block block1 (genesis.hash (), key2.pub, nano::genesis_amount - 100, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block1).code);
	ASSERT_EQ (nano::genesis_amount - 100, rep_weights.representation_get (nano::test_genesis_key.pub));
	nano::keypair key3;
	nano::open_block block2 (block1.hash (), key3.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block2).code);
	ASSERT_EQ (nano::genesis_amount - 100, rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key2.pub));
	ASSERT_EQ (100, rep_weights.representation_get (key3.pub));
	nano::send_block block3 (block1.hash (), key2.pub, nano::genesis_amount - 200, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block3).code);
	ASSERT_EQ (nano::genesis_amount - 200, rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key2.pub));
	ASSERT_EQ (100, rep_weights.representation_get (key3.pub));
	nano::receive_block block4 (block2.hash (), block3.hash (), key2.prv, key2.pub, *pool.generate (block2.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block4).code);
	ASSERT_EQ (nano::genesis_amount - 200, rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key2.pub));
	ASSERT_EQ (200, rep_weights.representation_get (key3.pub));
	nano::keypair key4;
	nano::change_block block5 (block4.hash (), key4.pub, key2.prv, key2.pub, *pool.generate (block4.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block5).code);
	ASSERT_EQ (nano::genesis_amount - 200, rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key2.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key3.pub));
	ASSERT_EQ (200, rep_weights.representation_get (key4.pub));
	nano::keypair key5;
	nano::send_block block6 (block5.hash (), key5.pub, 100, key2.prv, key2.pub, *pool.generate (block5.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block6).code);
	ASSERT_EQ (nano::genesis_amount - 200, rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key2.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key3.pub));
	ASSERT_EQ (100, rep_weights.representation_get (key4.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key5.pub));
	nano::keypair key6;
	nano::open_block block7 (block6.hash (), key6.pub, key5.pub, key5.prv, key5.pub, *pool.generate (key5.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block7).code);
	ASSERT_EQ (nano::genesis_amount - 200, rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key2.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key3.pub));
	ASSERT_EQ (100, rep_weights.representation_get (key4.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key5.pub));
	ASSERT_EQ (100, rep_weights.representation_get (key6.pub));
	nano::send_block block8 (block6.hash (), key5.pub, 0, key2.prv, key2.pub, *pool.generate (block6.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block8).code);
	ASSERT_EQ (nano::genesis_amount - 200, rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key2.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key3.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key4.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key5.pub));
	ASSERT_EQ (100, rep_weights.representation_get (key6.pub));
	nano::receive_block block9 (block7.hash (), block8.hash (), key5.prv, key5.pub, *pool.generate (block7.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block9).code);
	ASSERT_EQ (nano::genesis_amount - 200, rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key2.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key3.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key4.pub));
	ASSERT_EQ (0, rep_weights.representation_get (key5.pub));
	ASSERT_EQ (200, rep_weights.representation_get (key6.pub));
}

TEST (ledger, double_open)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key2;
	nano::send_block send1 (genesis.hash (), key2.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::open_block open1 (send1.hash (), key2.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	nano::open_block open2 (send1.hash (), nano::test_genesis_key.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, open2).code);
}

TEST (ledger, double_receive)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key2;
	nano::send_block send1 (genesis.hash (), key2.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::open_block open1 (send1.hash (), key2.pub, key2.pub, key2.prv, key2.pub, *pool.generate (key2.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	nano::receive_block receive1 (open1.hash (), send1.hash (), key2.prv, key2.pub, *pool.generate (open1.hash ()));
	ASSERT_EQ (nano::process_result::unreceivable, ledger.process (transaction, receive1).code);
}

TEST (votes, check_signature)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.online_weight_minimum = std::numeric_limits<nano::uint128_t>::max ();
	auto & node1 = *system.add_node (node_config);
	nano::genesis genesis;
	nano::keypair key1;
	auto send1 (std::make_shared<nano::send_block> (genesis.hash (), key1.pub, nano::genesis_amount - 100, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	{
		auto transaction (node1.store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send1).code);
	}
	node1.active.start (send1);
	nano::lock_guard<std::mutex> lock (node1.active.mutex);
	auto votes1 (node1.active.roots.find (send1->qualified_root ())->election);
	ASSERT_EQ (1, votes1->last_votes.size ());
	auto vote1 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 1, send1));
	vote1->signature.bytes[0] ^= 1;
	auto transaction (node1.store.tx_begin_read ());
	ASSERT_EQ (nano::vote_code::invalid, node1.vote_processor.vote_blocking (transaction, vote1, std::make_shared<nano::transport::channel_udp> (node1.network.udp_channels, nano::endpoint (boost::asio::ip::address_v6 (), 0), node1.network_params.protocol.protocol_version)));
	vote1->signature.bytes[0] ^= 1;
	ASSERT_EQ (nano::vote_code::vote, node1.vote_processor.vote_blocking (transaction, vote1, std::make_shared<nano::transport::channel_udp> (node1.network.udp_channels, nano::endpoint (boost::asio::ip::address_v6 (), 0), node1.network_params.protocol.protocol_version)));
	ASSERT_EQ (nano::vote_code::replay, node1.vote_processor.vote_blocking (transaction, vote1, std::make_shared<nano::transport::channel_udp> (node1.network.udp_channels, nano::endpoint (boost::asio::ip::address_v6 (), 0), node1.network_params.protocol.protocol_version)));
}

TEST (votes, add_one)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair key1;
	auto send1 (std::make_shared<nano::send_block> (genesis.hash (), key1.pub, nano::genesis_amount - 100, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	auto transaction (node1.store.tx_begin_write ());
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send1).code);
	node1.active.start (send1);
	nano::unique_lock<std::mutex> lock (node1.active.mutex);
	auto votes1 (node1.active.roots.find (send1->qualified_root ())->election);
	ASSERT_EQ (1, votes1->last_votes.size ());
	lock.unlock ();
	auto vote1 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 1, send1));
	ASSERT_FALSE (node1.active.vote (vote1));
	auto vote2 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 2, send1));
	ASSERT_FALSE (node1.active.vote (vote2));
	lock.lock ();
	ASSERT_EQ (2, votes1->last_votes.size ());
	auto existing1 (votes1->last_votes.find (nano::test_genesis_key.pub));
	ASSERT_NE (votes1->last_votes.end (), existing1);
	ASSERT_EQ (send1->hash (), existing1->second.hash);
	auto winner (*votes1->tally ().begin ());
	ASSERT_EQ (*send1, *winner.second);
	ASSERT_EQ (nano::genesis_amount - 100, winner.first);
}

TEST (votes, add_two)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair key1;
	auto send1 (std::make_shared<nano::send_block> (genesis.hash (), key1.pub, nano::genesis_amount - 100, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	auto transaction (node1.store.tx_begin_write ());
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send1).code);
	node1.active.start (send1);
	nano::unique_lock<std::mutex> lock (node1.active.mutex);
	auto votes1 (node1.active.roots.find (send1->qualified_root ())->election);
	lock.unlock ();
	nano::keypair key2;
	auto send2 (std::make_shared<nano::send_block> (genesis.hash (), key2.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	auto vote2 (std::make_shared<nano::vote> (key2.pub, key2.prv, 1, send2));
	ASSERT_FALSE (node1.active.vote (vote2));
	auto vote1 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 1, send1));
	ASSERT_FALSE (node1.active.vote (vote1));
	lock.lock ();
	ASSERT_EQ (3, votes1->last_votes.size ());
	ASSERT_NE (votes1->last_votes.end (), votes1->last_votes.find (nano::test_genesis_key.pub));
	ASSERT_EQ (send1->hash (), votes1->last_votes[nano::test_genesis_key.pub].hash);
	ASSERT_NE (votes1->last_votes.end (), votes1->last_votes.find (key2.pub));
	ASSERT_EQ (send2->hash (), votes1->last_votes[key2.pub].hash);
	auto winner (*votes1->tally ().begin ());
	ASSERT_EQ (*send1, *winner.second);
}

// Higher sequence numbers change the vote
TEST (votes, add_existing)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.online_weight_minimum = std::numeric_limits<nano::uint128_t>::max ();
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto & node1 = *system.add_node (node_config);
	nano::genesis genesis;
	nano::keypair key1;
	auto send1 (std::make_shared<nano::send_block> (genesis.hash (), key1.pub, nano::genesis_amount - nano::Gxrb_ratio, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	{
		auto transaction (node1.store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send1).code);
	}
	node1.active.start (send1);
	auto vote1 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 1, send1));
	ASSERT_FALSE (node1.active.vote (vote1));
	// Block is already processed from vote
	ASSERT_TRUE (node1.active.publish (send1));
	nano::unique_lock<std::mutex> lock (node1.active.mutex);
	auto votes1 (node1.active.roots.find (send1->qualified_root ())->election);
	ASSERT_EQ (1, votes1->last_votes[nano::test_genesis_key.pub].sequence);
	nano::keypair key2;
	auto send2 (std::make_shared<nano::send_block> (genesis.hash (), key2.pub, nano::genesis_amount - nano::Gxrb_ratio, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send2);
	auto vote2 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 2, send2));
	// Pretend we've waited the timeout
	votes1->last_votes[nano::test_genesis_key.pub].time = std::chrono::steady_clock::now () - std::chrono::seconds (20);
	lock.unlock ();
	ASSERT_FALSE (node1.active.vote (vote2));
	ASSERT_FALSE (node1.active.publish (send2));
	lock.lock ();
	ASSERT_EQ (2, votes1->last_votes[nano::test_genesis_key.pub].sequence);
	// Also resend the old vote, and see if we respect the sequence number
	votes1->last_votes[nano::test_genesis_key.pub].time = std::chrono::steady_clock::now () - std::chrono::seconds (20);
	lock.unlock ();
	ASSERT_TRUE (node1.active.vote (vote1));
	lock.lock ();
	ASSERT_EQ (2, votes1->last_votes[nano::test_genesis_key.pub].sequence);
	ASSERT_EQ (2, votes1->last_votes.size ());
	ASSERT_NE (votes1->last_votes.end (), votes1->last_votes.find (nano::test_genesis_key.pub));
	ASSERT_EQ (send2->hash (), votes1->last_votes[nano::test_genesis_key.pub].hash);
	{
		auto transaction (node1.store.tx_begin_read ());
		auto winner (*votes1->tally ().begin ());
		ASSERT_EQ (*send2, *winner.second);
	}
}

// Lower sequence numbers are ignored
TEST (votes, add_old)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair key1;
	auto send1 (std::make_shared<nano::send_block> (genesis.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	auto transaction (node1.store.tx_begin_write ());
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send1).code);
	node1.active.start (send1);
	auto vote1 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 2, send1));
	nano::lock_guard<std::mutex> lock (node1.active.mutex);
	auto votes1 (node1.active.roots.find (send1->qualified_root ())->election);
	auto channel (std::make_shared<nano::transport::channel_udp> (node1.network.udp_channels, node1.network.endpoint (), node1.network_params.protocol.protocol_version));
	node1.vote_processor.vote_blocking (transaction, vote1, channel);
	nano::keypair key2;
	auto send2 (std::make_shared<nano::send_block> (genesis.hash (), key2.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send2);
	auto vote2 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 1, send2));
	votes1->last_votes[nano::test_genesis_key.pub].time = std::chrono::steady_clock::now () - std::chrono::seconds (20);
	node1.vote_processor.vote_blocking (transaction, vote2, channel);
	ASSERT_EQ (2, votes1->last_votes.size ());
	ASSERT_NE (votes1->last_votes.end (), votes1->last_votes.find (nano::test_genesis_key.pub));
	ASSERT_EQ (send1->hash (), votes1->last_votes[nano::test_genesis_key.pub].hash);
	auto winner (*votes1->tally ().begin ());
	ASSERT_EQ (*send1, *winner.second);
}

// Lower sequence numbers are accepted for different accounts
TEST (votes, add_old_different_account)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair key1;
	auto send1 (std::make_shared<nano::send_block> (genesis.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	auto send2 (std::make_shared<nano::send_block> (send1->hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send2);
	auto transaction (node1.store.tx_begin_write ());
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send1).code);
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send2).code);
	node1.active.start (send1);
	node1.active.start (send2);
	nano::unique_lock<std::mutex> lock (node1.active.mutex);
	auto votes1 (node1.active.roots.find (send1->qualified_root ())->election);
	auto votes2 (node1.active.roots.find (send2->qualified_root ())->election);
	ASSERT_EQ (1, votes1->last_votes.size ());
	ASSERT_EQ (1, votes2->last_votes.size ());
	auto vote1 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 2, send1));
	auto channel (std::make_shared<nano::transport::channel_udp> (node1.network.udp_channels, node1.network.endpoint (), node1.network_params.protocol.protocol_version));
	auto vote_result1 (node1.vote_processor.vote_blocking (transaction, vote1, channel));
	ASSERT_EQ (nano::vote_code::vote, vote_result1);
	ASSERT_EQ (2, votes1->last_votes.size ());
	ASSERT_EQ (1, votes2->last_votes.size ());
	auto vote2 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 1, send2));
	auto vote_result2 (node1.vote_processor.vote_blocking (transaction, vote2, channel));
	ASSERT_EQ (nano::vote_code::vote, vote_result2);
	ASSERT_EQ (2, votes1->last_votes.size ());
	ASSERT_EQ (2, votes2->last_votes.size ());
	ASSERT_NE (votes1->last_votes.end (), votes1->last_votes.find (nano::test_genesis_key.pub));
	ASSERT_NE (votes2->last_votes.end (), votes2->last_votes.find (nano::test_genesis_key.pub));
	ASSERT_EQ (send1->hash (), votes1->last_votes[nano::test_genesis_key.pub].hash);
	ASSERT_EQ (send2->hash (), votes2->last_votes[nano::test_genesis_key.pub].hash);
	auto winner1 (*votes1->tally ().begin ());
	ASSERT_EQ (*send1, *winner1.second);
	auto winner2 (*votes2->tally ().begin ());
	ASSERT_EQ (*send2, *winner2.second);
}

// The voting cooldown is respected
TEST (votes, add_cooldown)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair key1;
	auto send1 (std::make_shared<nano::send_block> (genesis.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	auto transaction (node1.store.tx_begin_write ());
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send1).code);
	node1.active.start (send1);
	nano::unique_lock<std::mutex> lock (node1.active.mutex);
	auto votes1 (node1.active.roots.find (send1->qualified_root ())->election);
	auto vote1 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 1, send1));
	auto channel (std::make_shared<nano::transport::channel_udp> (node1.network.udp_channels, node1.network.endpoint (), node1.network_params.protocol.protocol_version));
	node1.vote_processor.vote_blocking (transaction, vote1, channel);
	nano::keypair key2;
	auto send2 (std::make_shared<nano::send_block> (genesis.hash (), key2.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send2);
	auto vote2 (std::make_shared<nano::vote> (nano::test_genesis_key.pub, nano::test_genesis_key.prv, 2, send2));
	node1.vote_processor.vote_blocking (transaction, vote2, channel);
	ASSERT_EQ (2, votes1->last_votes.size ());
	ASSERT_NE (votes1->last_votes.end (), votes1->last_votes.find (nano::test_genesis_key.pub));
	ASSERT_EQ (send1->hash (), votes1->last_votes[nano::test_genesis_key.pub].hash);
	auto winner (*votes1->tally ().begin ());
	ASSERT_EQ (*send1, *winner.second);
}

// Query for block successor
TEST (ledger, successor)
{
	nano::system system (1);
	nano::keypair key1;
	nano::genesis genesis;
	nano::send_block send1 (genesis.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0);
	system.nodes[0]->work_generate_blocking (send1);
	auto transaction (system.nodes[0]->store.tx_begin_write ());
	ASSERT_EQ (nano::process_result::progress, system.nodes[0]->ledger.process (transaction, send1).code);
	ASSERT_EQ (send1, *system.nodes[0]->ledger.successor (transaction, nano::qualified_root (genesis.hash (), nano::root (0))));
	ASSERT_EQ (*genesis.open, *system.nodes[0]->ledger.successor (transaction, genesis.open->qualified_root ()));
	ASSERT_EQ (nullptr, system.nodes[0]->ledger.successor (transaction, nano::qualified_root (0)));
}

TEST (ledger, fail_change_old)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::change_block block (genesis.hash (), key1.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	auto result2 (ledger.process (transaction, block));
	ASSERT_EQ (nano::process_result::old, result2.code);
}

TEST (ledger, fail_change_gap_previous)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::change_block block (1, key1.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (nano::root (1)));
	auto result1 (ledger.process (transaction, block));
	ASSERT_EQ (nano::process_result::gap_previous, result1.code);
}

TEST (ledger, fail_change_bad_signature)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::change_block block (genesis.hash (), key1.pub, nano::keypair ().prv, 0, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block));
	ASSERT_EQ (nano::process_result::bad_signature, result1.code);
}

TEST (ledger, fail_change_fork)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::change_block block1 (genesis.hash (), key1.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block1));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	nano::keypair key2;
	nano::change_block block2 (genesis.hash (), key2.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result2 (ledger.process (transaction, block2));
	ASSERT_EQ (nano::process_result::fork, result2.code);
}

TEST (ledger, fail_send_old)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	auto result2 (ledger.process (transaction, block));
	ASSERT_EQ (nano::process_result::old, result2.code);
}

TEST (ledger, fail_send_gap_previous)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block (1, key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (nano::root (1)));
	auto result1 (ledger.process (transaction, block));
	ASSERT_EQ (nano::process_result::gap_previous, result1.code);
}

TEST (ledger, fail_send_bad_signature)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block (genesis.hash (), key1.pub, 1, nano::keypair ().prv, 0, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block));
	ASSERT_EQ (nano::process_result::bad_signature, result1.code);
}

TEST (ledger, fail_send_negative_spend)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block1).code);
	nano::keypair key2;
	nano::send_block block2 (block1.hash (), key2.pub, 2, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	ASSERT_EQ (nano::process_result::negative_spend, ledger.process (transaction, block2).code);
}

TEST (ledger, fail_send_fork)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block1).code);
	nano::keypair key2;
	nano::send_block block2 (genesis.hash (), key2.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, block2).code);
}

TEST (ledger, fail_open_old)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block1).code);
	nano::open_block block2 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block2).code);
	ASSERT_EQ (nano::process_result::old, ledger.process (transaction, block2).code);
}

TEST (ledger, fail_open_gap_source)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::open_block block2 (1, 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	auto result2 (ledger.process (transaction, block2));
	ASSERT_EQ (nano::process_result::gap_source, result2.code);
}

TEST (ledger, fail_open_bad_signature)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block1).code);
	nano::open_block block2 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	block2.signature.clear ();
	ASSERT_EQ (nano::process_result::bad_signature, ledger.process (transaction, block2).code);
}

TEST (ledger, fail_open_fork_previous)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block1).code);
	nano::send_block block2 (block1.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block2).code);
	nano::open_block block3 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block3).code);
	nano::open_block block4 (block2.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, block4).code);
}

TEST (ledger, fail_open_account_mismatch)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block1).code);
	nano::keypair badkey;
	nano::open_block block2 (block1.hash (), 1, badkey.pub, badkey.prv, badkey.pub, *pool.generate (badkey.pub));
	ASSERT_NE (nano::process_result::progress, ledger.process (transaction, block2).code);
}

TEST (ledger, fail_receive_old)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block1).code);
	nano::send_block block2 (block1.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block2).code);
	nano::open_block block3 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block3).code);
	nano::receive_block block4 (block3.hash (), block2.hash (), key1.prv, key1.pub, *pool.generate (block3.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block4).code);
	ASSERT_EQ (nano::process_result::old, ledger.process (transaction, block4).code);
}

TEST (ledger, fail_receive_gap_source)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block1));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	nano::send_block block2 (block1.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	auto result2 (ledger.process (transaction, block2));
	ASSERT_EQ (nano::process_result::progress, result2.code);
	nano::open_block block3 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	auto result3 (ledger.process (transaction, block3));
	ASSERT_EQ (nano::process_result::progress, result3.code);
	nano::receive_block block4 (block3.hash (), 1, key1.prv, key1.pub, *pool.generate (block3.hash ()));
	auto result4 (ledger.process (transaction, block4));
	ASSERT_EQ (nano::process_result::gap_source, result4.code);
}

TEST (ledger, fail_receive_overreceive)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block1));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	nano::open_block block2 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	auto result3 (ledger.process (transaction, block2));
	ASSERT_EQ (nano::process_result::progress, result3.code);
	nano::receive_block block3 (block2.hash (), block1.hash (), key1.prv, key1.pub, *pool.generate (block2.hash ()));
	auto result4 (ledger.process (transaction, block3));
	ASSERT_EQ (nano::process_result::unreceivable, result4.code);
}

TEST (ledger, fail_receive_bad_signature)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block1));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	nano::send_block block2 (block1.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	auto result2 (ledger.process (transaction, block2));
	ASSERT_EQ (nano::process_result::progress, result2.code);
	nano::open_block block3 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	auto result3 (ledger.process (transaction, block3));
	ASSERT_EQ (nano::process_result::progress, result3.code);
	nano::receive_block block4 (block3.hash (), block2.hash (), nano::keypair ().prv, 0, *pool.generate (block3.hash ()));
	auto result4 (ledger.process (transaction, block4));
	ASSERT_EQ (nano::process_result::bad_signature, result4.code);
}

TEST (ledger, fail_receive_gap_previous_opened)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block1));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	nano::send_block block2 (block1.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	auto result2 (ledger.process (transaction, block2));
	ASSERT_EQ (nano::process_result::progress, result2.code);
	nano::open_block block3 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	auto result3 (ledger.process (transaction, block3));
	ASSERT_EQ (nano::process_result::progress, result3.code);
	nano::receive_block block4 (1, block2.hash (), key1.prv, key1.pub, *pool.generate (nano::root (1)));
	auto result4 (ledger.process (transaction, block4));
	ASSERT_EQ (nano::process_result::gap_previous, result4.code);
}

TEST (ledger, fail_receive_gap_previous_unopened)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block1));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	nano::send_block block2 (block1.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	auto result2 (ledger.process (transaction, block2));
	ASSERT_EQ (nano::process_result::progress, result2.code);
	nano::receive_block block3 (1, block2.hash (), key1.prv, key1.pub, *pool.generate (nano::root (1)));
	auto result3 (ledger.process (transaction, block3));
	ASSERT_EQ (nano::process_result::gap_previous, result3.code);
}

TEST (ledger, fail_receive_fork_previous)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block1));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	nano::send_block block2 (block1.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	auto result2 (ledger.process (transaction, block2));
	ASSERT_EQ (nano::process_result::progress, result2.code);
	nano::open_block block3 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	auto result3 (ledger.process (transaction, block3));
	ASSERT_EQ (nano::process_result::progress, result3.code);
	nano::keypair key2;
	nano::send_block block4 (block3.hash (), key1.pub, 1, key1.prv, key1.pub, *pool.generate (block3.hash ()));
	auto result4 (ledger.process (transaction, block4));
	ASSERT_EQ (nano::process_result::progress, result4.code);
	nano::receive_block block5 (block3.hash (), block2.hash (), key1.prv, key1.pub, *pool.generate (block3.hash ()));
	auto result5 (ledger.process (transaction, block5));
	ASSERT_EQ (nano::process_result::fork, result5.code);
}

TEST (ledger, fail_receive_received_source)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1;
	nano::send_block block1 (genesis.hash (), key1.pub, 2, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	auto result1 (ledger.process (transaction, block1));
	ASSERT_EQ (nano::process_result::progress, result1.code);
	nano::send_block block2 (block1.hash (), key1.pub, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	auto result2 (ledger.process (transaction, block2));
	ASSERT_EQ (nano::process_result::progress, result2.code);
	nano::send_block block6 (block2.hash (), key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block2.hash ()));
	auto result6 (ledger.process (transaction, block6));
	ASSERT_EQ (nano::process_result::progress, result6.code);
	nano::open_block block3 (block1.hash (), 1, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	auto result3 (ledger.process (transaction, block3));
	ASSERT_EQ (nano::process_result::progress, result3.code);
	nano::keypair key2;
	nano::send_block block4 (block3.hash (), key1.pub, 1, key1.prv, key1.pub, *pool.generate (block3.hash ()));
	auto result4 (ledger.process (transaction, block4));
	ASSERT_EQ (nano::process_result::progress, result4.code);
	nano::receive_block block5 (block4.hash (), block2.hash (), key1.prv, key1.pub, *pool.generate (block4.hash ()));
	auto result5 (ledger.process (transaction, block5));
	ASSERT_EQ (nano::process_result::progress, result5.code);
	nano::receive_block block7 (block3.hash (), block2.hash (), key1.prv, key1.pub, *pool.generate (block3.hash ()));
	auto result7 (ledger.process (transaction, block7));
	ASSERT_EQ (nano::process_result::fork, result7.code);
}

TEST (ledger, latest_empty)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::keypair key;
	auto transaction (store->tx_begin_read ());
	auto latest (ledger.latest (transaction, key.pub));
	ASSERT_TRUE (latest.is_zero ());
}

TEST (ledger, latest_root)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key;
	ASSERT_EQ (key.pub, ledger.latest_root (transaction, key.pub));
	auto hash1 (ledger.latest (transaction, nano::test_genesis_key.pub));
	nano::send_block send (hash1, 0, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (hash1));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	ASSERT_EQ (send.hash (), ledger.latest_root (transaction, nano::test_genesis_key.pub));
}

TEST (ledger, change_representative_move_representation)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::keypair key1;
	auto transaction (store->tx_begin_write ());
	nano::genesis genesis;
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	auto hash1 (genesis.hash ());
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::test_genesis_key.pub));
	nano::send_block send (hash1, key1.pub, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (hash1));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	ASSERT_EQ (0, ledger.weight (nano::test_genesis_key.pub));
	nano::keypair key2;
	nano::change_block change (send.hash (), key2.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, change).code);
	nano::keypair key3;
	nano::open_block open (send.hash (), key3.pub, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open).code);
	ASSERT_EQ (nano::genesis_amount, ledger.weight (key3.pub));
}

TEST (ledger, send_open_receive_rollback)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	auto transaction (store->tx_begin_write ());
	nano::genesis genesis;
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
	nano::keypair key1;
	nano::send_block send1 (info1.head, key1.pub, nano::genesis_amount - 50, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
	auto return1 (ledger.process (transaction, send1));
	ASSERT_EQ (nano::process_result::progress, return1.code);
	nano::send_block send2 (send1.hash (), key1.pub, nano::genesis_amount - 100, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	auto return2 (ledger.process (transaction, send2));
	ASSERT_EQ (nano::process_result::progress, return2.code);
	nano::keypair key2;
	nano::open_block open (send2.hash (), key2.pub, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	auto return4 (ledger.process (transaction, open));
	ASSERT_EQ (nano::process_result::progress, return4.code);
	nano::receive_block receive (open.hash (), send1.hash (), key1.prv, key1.pub, *pool.generate (open.hash ()));
	auto return5 (ledger.process (transaction, receive));
	ASSERT_EQ (nano::process_result::progress, return5.code);
	nano::keypair key3;
	ASSERT_EQ (100, ledger.weight (key2.pub));
	ASSERT_EQ (nano::genesis_amount - 100, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (0, ledger.weight (key3.pub));
	nano::change_block change1 (send2.hash (), key3.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send2.hash ()));
	auto return6 (ledger.process (transaction, change1));
	ASSERT_EQ (nano::process_result::progress, return6.code);
	ASSERT_EQ (100, ledger.weight (key2.pub));
	ASSERT_EQ (0, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (nano::genesis_amount - 100, ledger.weight (key3.pub));
	ASSERT_FALSE (ledger.rollback (transaction, receive.hash ()));
	ASSERT_EQ (50, ledger.weight (key2.pub));
	ASSERT_EQ (0, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (nano::genesis_amount - 100, ledger.weight (key3.pub));
	ASSERT_FALSE (ledger.rollback (transaction, open.hash ()));
	ASSERT_EQ (0, ledger.weight (key2.pub));
	ASSERT_EQ (0, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_EQ (nano::genesis_amount - 100, ledger.weight (key3.pub));
	ASSERT_FALSE (ledger.rollback (transaction, change1.hash ()));
	ASSERT_EQ (0, ledger.weight (key2.pub));
	ASSERT_EQ (0, ledger.weight (key3.pub));
	ASSERT_EQ (nano::genesis_amount - 100, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_FALSE (ledger.rollback (transaction, send2.hash ()));
	ASSERT_EQ (0, ledger.weight (key2.pub));
	ASSERT_EQ (0, ledger.weight (key3.pub));
	ASSERT_EQ (nano::genesis_amount - 50, ledger.weight (nano::test_genesis_key.pub));
	ASSERT_FALSE (ledger.rollback (transaction, send1.hash ()));
	ASSERT_EQ (0, ledger.weight (key2.pub));
	ASSERT_EQ (0, ledger.weight (key3.pub));
	ASSERT_EQ (nano::genesis_amount - 0, ledger.weight (nano::test_genesis_key.pub));
}

TEST (ledger, bootstrap_rep_weight)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::account_info info1;
	nano::keypair key2;
	nano::genesis genesis;
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	{
		auto transaction (store->tx_begin_write ());
		store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
		ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
		nano::send_block send (info1.head, key2.pub, std::numeric_limits<nano::uint128_t>::max () - 50, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	}
	ASSERT_EQ (2, ledger.block_count_cache);
	{
		ledger.bootstrap_weight_max_blocks = 3;
		ledger.bootstrap_weights[key2.pub] = 1000;
		ASSERT_EQ (1000, ledger.weight (key2.pub));
	}
	{
		auto transaction (store->tx_begin_write ());
		ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, info1));
		nano::send_block send (info1.head, key2.pub, std::numeric_limits<nano::uint128_t>::max () - 100, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (info1.head));
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	}
	ASSERT_EQ (3, ledger.block_count_cache);
	{
		auto transaction (store->tx_begin_read ());
		ASSERT_EQ (0, ledger.weight (key2.pub));
	}
}

TEST (ledger, block_destination_source)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair dest;
	nano::uint128_t balance (nano::genesis_amount);
	balance -= nano::Gxrb_ratio;
	nano::send_block block1 (genesis.hash (), dest.pub, balance, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	balance -= nano::Gxrb_ratio;
	nano::send_block block2 (block1.hash (), nano::genesis_account, balance, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block1.hash ()));
	balance += nano::Gxrb_ratio;
	nano::receive_block block3 (block2.hash (), block2.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block2.hash ()));
	balance -= nano::Gxrb_ratio;
	nano::state_block block4 (nano::genesis_account, block3.hash (), nano::genesis_account, balance, dest.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block3.hash ()));
	balance -= nano::Gxrb_ratio;
	nano::state_block block5 (nano::genesis_account, block4.hash (), nano::genesis_account, balance, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block4.hash ()));
	balance += nano::Gxrb_ratio;
	nano::state_block block6 (nano::genesis_account, block5.hash (), nano::genesis_account, balance, block5.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (block5.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block1).code);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block2).code);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block3).code);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block4).code);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block5).code);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, block6).code);
	ASSERT_EQ (balance, ledger.balance (transaction, block6.hash ()));
	ASSERT_EQ (dest.pub, ledger.block_destination (transaction, block1));
	ASSERT_TRUE (ledger.block_source (transaction, block1).is_zero ());
	ASSERT_EQ (nano::genesis_account, ledger.block_destination (transaction, block2));
	ASSERT_TRUE (ledger.block_source (transaction, block2).is_zero ());
	ASSERT_TRUE (ledger.block_destination (transaction, block3).is_zero ());
	ASSERT_EQ (block2.hash (), ledger.block_source (transaction, block3));
	ASSERT_EQ (dest.pub, ledger.block_destination (transaction, block4));
	ASSERT_TRUE (ledger.block_source (transaction, block4).is_zero ());
	ASSERT_EQ (nano::genesis_account, ledger.block_destination (transaction, block5));
	ASSERT_TRUE (ledger.block_source (transaction, block5).is_zero ());
	ASSERT_TRUE (ledger.block_destination (transaction, block6).is_zero ());
	ASSERT_EQ (block5.hash (), ledger.block_source (transaction, block6));
}

TEST (ledger, state_account)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_EQ (nano::genesis_account, ledger.account (transaction, send1.hash ()));
}

TEST (ledger, state_send_receive)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_TRUE (store->block_exists (transaction, send1.hash ()));
	auto send2 (store->block_get (transaction, send1.hash ()));
	ASSERT_NE (nullptr, send2);
	ASSERT_EQ (send1, *send2);
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.balance (transaction, send1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, send1.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	ASSERT_TRUE (store->pending_exists (transaction, nano::pending_key (nano::genesis_account, send1.hash ())));
	nano::state_block receive1 (nano::genesis_account, send1.hash (), nano::genesis_account, nano::genesis_amount, send1.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive1).code);
	ASSERT_TRUE (store->block_exists (transaction, receive1.hash ()));
	auto receive2 (store->block_get (transaction, receive1.hash ()));
	ASSERT_NE (nullptr, receive2);
	ASSERT_EQ (receive1, *receive2);
	ASSERT_EQ (nano::genesis_amount, ledger.balance (transaction, receive1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, receive1.hash ()));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
	ASSERT_FALSE (store->pending_exists (transaction, nano::pending_key (nano::genesis_account, send1.hash ())));
}

TEST (ledger, state_receive)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::send_block send1 (genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_TRUE (store->block_exists (transaction, send1.hash ()));
	auto send2 (store->block_get (transaction, send1.hash ()));
	ASSERT_NE (nullptr, send2);
	ASSERT_EQ (send1, *send2);
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.balance (transaction, send1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, send1.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	nano::state_block receive1 (nano::genesis_account, send1.hash (), nano::genesis_account, nano::genesis_amount, send1.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive1).code);
	ASSERT_TRUE (store->block_exists (transaction, receive1.hash ()));
	auto receive2 (store->block_get (transaction, receive1.hash ()));
	ASSERT_NE (nullptr, receive2);
	ASSERT_EQ (receive1, *receive2);
	ASSERT_EQ (nano::genesis_amount, ledger.balance (transaction, receive1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, receive1.hash ()));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
}

TEST (ledger, state_rep_change)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair rep;
	nano::state_block change1 (nano::genesis_account, genesis.hash (), rep.pub, nano::genesis_amount, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, change1).code);
	ASSERT_TRUE (store->block_exists (transaction, change1.hash ()));
	auto change2 (store->block_get (transaction, change1.hash ()));
	ASSERT_NE (nullptr, change2);
	ASSERT_EQ (change1, *change2);
	ASSERT_EQ (nano::genesis_amount, ledger.balance (transaction, change1.hash ()));
	ASSERT_EQ (0, ledger.amount (transaction, change1.hash ()));
	ASSERT_EQ (0, ledger.weight (nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (rep.pub));
}

TEST (ledger, state_open)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_TRUE (store->block_exists (transaction, send1.hash ()));
	auto send2 (store->block_get (transaction, send1.hash ()));
	ASSERT_NE (nullptr, send2);
	ASSERT_EQ (send1, *send2);
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.balance (transaction, send1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, send1.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	ASSERT_TRUE (store->pending_exists (transaction, nano::pending_key (destination.pub, send1.hash ())));
	nano::state_block open1 (destination.pub, 0, nano::genesis_account, nano::Gxrb_ratio, send1.hash (), destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	ASSERT_FALSE (store->pending_exists (transaction, nano::pending_key (destination.pub, send1.hash ())));
	ASSERT_TRUE (store->block_exists (transaction, open1.hash ()));
	auto open2 (store->block_get (transaction, open1.hash ()));
	ASSERT_NE (nullptr, open2);
	ASSERT_EQ (open1, *open2);
	ASSERT_EQ (nano::Gxrb_ratio, ledger.balance (transaction, open1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, open1.hash ()));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
}

// Make sure old block types can't be inserted after a state block.
TEST (ledger, send_after_state_fail)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::send_block send2 (send1.hash (), nano::genesis_account, nano::genesis_amount - (2 * nano::Gxrb_ratio), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, send2).code);
}

// Make sure old block types can't be inserted after a state block.
TEST (ledger, receive_after_state_fail)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::receive_block receive1 (send1.hash (), send1.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, receive1).code);
}

// Make sure old block types can't be inserted after a state block.
TEST (ledger, change_after_state_fail)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::keypair rep;
	nano::change_block change1 (send1.hash (), rep.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, change1).code);
}

TEST (ledger, state_unreceivable_fail)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::send_block send1 (genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_TRUE (store->block_exists (transaction, send1.hash ()));
	auto send2 (store->block_get (transaction, send1.hash ()));
	ASSERT_NE (nullptr, send2);
	ASSERT_EQ (send1, *send2);
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.balance (transaction, send1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, send1.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	nano::state_block receive1 (nano::genesis_account, send1.hash (), nano::genesis_account, nano::genesis_amount, 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::gap_source, ledger.process (transaction, receive1).code);
}

TEST (ledger, state_receive_bad_amount_fail)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::send_block send1 (genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_TRUE (store->block_exists (transaction, send1.hash ()));
	auto send2 (store->block_get (transaction, send1.hash ()));
	ASSERT_NE (nullptr, send2);
	ASSERT_EQ (send1, *send2);
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.balance (transaction, send1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, send1.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	nano::state_block receive1 (nano::genesis_account, send1.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, send1.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::balance_mismatch, ledger.process (transaction, receive1).code);
}

TEST (ledger, state_no_link_amount_fail)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::keypair rep;
	nano::state_block change1 (nano::genesis_account, send1.hash (), rep.pub, nano::genesis_amount, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::balance_mismatch, ledger.process (transaction, change1).code);
}

TEST (ledger, state_receive_wrong_account_fail)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_TRUE (store->block_exists (transaction, send1.hash ()));
	auto send2 (store->block_get (transaction, send1.hash ()));
	ASSERT_NE (nullptr, send2);
	ASSERT_EQ (send1, *send2);
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.balance (transaction, send1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, send1.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	nano::keypair key;
	nano::state_block receive1 (key.pub, 0, nano::genesis_account, nano::Gxrb_ratio, send1.hash (), key.prv, key.pub, *pool.generate (key.pub));
	ASSERT_EQ (nano::process_result::unreceivable, ledger.process (transaction, receive1).code);
}

TEST (ledger, state_open_state_fork)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::state_block open1 (destination.pub, 0, nano::genesis_account, nano::Gxrb_ratio, send1.hash (), destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	nano::open_block open2 (send1.hash (), nano::genesis_account, destination.pub, destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, open2).code);
	ASSERT_EQ (open1.root (), open2.root ());
}

TEST (ledger, state_state_open_fork)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::open_block open1 (send1.hash (), nano::genesis_account, destination.pub, destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	nano::state_block open2 (destination.pub, 0, nano::genesis_account, nano::Gxrb_ratio, send1.hash (), destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, open2).code);
	ASSERT_EQ (open1.root (), open2.root ());
}

TEST (ledger, state_open_previous_fail)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::state_block open1 (destination.pub, 1, nano::genesis_account, nano::Gxrb_ratio, send1.hash (), destination.prv, destination.pub, *pool.generate (1));
	ASSERT_EQ (nano::process_result::gap_previous, ledger.process (transaction, open1).code);
}

TEST (ledger, state_open_source_fail)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::state_block open1 (destination.pub, 0, nano::genesis_account, 0, 0, destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::gap_source, ledger.process (transaction, open1).code);
}

TEST (ledger, state_send_change)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair rep;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), rep.pub, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_TRUE (store->block_exists (transaction, send1.hash ()));
	auto send2 (store->block_get (transaction, send1.hash ()));
	ASSERT_NE (nullptr, send2);
	ASSERT_EQ (send1, *send2);
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.balance (transaction, send1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, send1.hash ()));
	ASSERT_EQ (0, ledger.weight (nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (rep.pub));
}

TEST (ledger, state_receive_change)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_TRUE (store->block_exists (transaction, send1.hash ()));
	auto send2 (store->block_get (transaction, send1.hash ()));
	ASSERT_NE (nullptr, send2);
	ASSERT_EQ (send1, *send2);
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.balance (transaction, send1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, send1.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	nano::keypair rep;
	nano::state_block receive1 (nano::genesis_account, send1.hash (), rep.pub, nano::genesis_amount, send1.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive1).code);
	ASSERT_TRUE (store->block_exists (transaction, receive1.hash ()));
	auto receive2 (store->block_get (transaction, receive1.hash ()));
	ASSERT_NE (nullptr, receive2);
	ASSERT_EQ (receive1, *receive2);
	ASSERT_EQ (nano::genesis_amount, ledger.balance (transaction, receive1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, receive1.hash ()));
	ASSERT_EQ (0, ledger.weight (nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (rep.pub));
}

TEST (ledger, state_open_old)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::open_block open1 (send1.hash (), nano::genesis_account, destination.pub, destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	ASSERT_EQ (nano::Gxrb_ratio, ledger.balance (transaction, open1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, open1.hash ()));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
}

TEST (ledger, state_receive_old)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::state_block send2 (nano::genesis_account, send1.hash (), nano::genesis_account, nano::genesis_amount - (2 * nano::Gxrb_ratio), destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send2).code);
	nano::open_block open1 (send1.hash (), nano::genesis_account, destination.pub, destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	nano::receive_block receive1 (open1.hash (), send2.hash (), destination.prv, destination.pub, *pool.generate (open1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive1).code);
	ASSERT_EQ (2 * nano::Gxrb_ratio, ledger.balance (transaction, receive1.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, receive1.hash ()));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
}

TEST (ledger, state_rollback_send)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_TRUE (store->block_exists (transaction, send1.hash ()));
	auto send2 (store->block_get (transaction, send1.hash ()));
	ASSERT_NE (nullptr, send2);
	ASSERT_EQ (send1, *send2);
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.account_balance (transaction, nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	nano::pending_info info;
	ASSERT_FALSE (store->pending_get (transaction, nano::pending_key (nano::genesis_account, send1.hash ()), info));
	ASSERT_EQ (nano::genesis_account, info.source);
	ASSERT_EQ (nano::Gxrb_ratio, info.amount.number ());
	ASSERT_FALSE (ledger.rollback (transaction, send1.hash ()));
	ASSERT_FALSE (store->block_exists (transaction, send1.hash ()));
	ASSERT_EQ (nano::genesis_amount, ledger.account_balance (transaction, nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
	ASSERT_FALSE (store->pending_exists (transaction, nano::pending_key (nano::genesis_account, send1.hash ())));
	ASSERT_TRUE (store->block_successor (transaction, genesis.hash ()).is_zero ());
}

TEST (ledger, state_rollback_receive)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::state_block receive1 (nano::genesis_account, send1.hash (), nano::genesis_account, nano::genesis_amount, send1.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive1).code);
	ASSERT_FALSE (store->pending_exists (transaction, nano::pending_key (nano::genesis_account, receive1.hash ())));
	ASSERT_FALSE (ledger.rollback (transaction, receive1.hash ()));
	nano::pending_info info;
	ASSERT_FALSE (store->pending_get (transaction, nano::pending_key (nano::genesis_account, send1.hash ()), info));
	ASSERT_EQ (nano::genesis_account, info.source);
	ASSERT_EQ (nano::Gxrb_ratio, info.amount.number ());
	ASSERT_FALSE (store->block_exists (transaction, receive1.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.account_balance (transaction, nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
}

TEST (ledger, state_rollback_received_send)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, key.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::state_block receive1 (key.pub, 0, key.pub, nano::Gxrb_ratio, send1.hash (), key.prv, key.pub, *pool.generate (key.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive1).code);
	ASSERT_FALSE (store->pending_exists (transaction, nano::pending_key (nano::genesis_account, receive1.hash ())));
	ASSERT_FALSE (ledger.rollback (transaction, send1.hash ()));
	ASSERT_FALSE (store->pending_exists (transaction, nano::pending_key (nano::genesis_account, send1.hash ())));
	ASSERT_FALSE (store->block_exists (transaction, send1.hash ()));
	ASSERT_FALSE (store->block_exists (transaction, receive1.hash ()));
	ASSERT_EQ (nano::genesis_amount, ledger.account_balance (transaction, nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
	ASSERT_EQ (0, ledger.account_balance (transaction, key.pub));
	ASSERT_EQ (0, ledger.weight (key.pub));
}

TEST (ledger, state_rep_change_rollback)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair rep;
	nano::state_block change1 (nano::genesis_account, genesis.hash (), rep.pub, nano::genesis_amount, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, change1).code);
	ASSERT_FALSE (ledger.rollback (transaction, change1.hash ()));
	ASSERT_FALSE (store->block_exists (transaction, change1.hash ()));
	ASSERT_EQ (nano::genesis_amount, ledger.account_balance (transaction, nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
	ASSERT_EQ (0, ledger.weight (rep.pub));
}

TEST (ledger, state_open_rollback)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::state_block open1 (destination.pub, 0, nano::genesis_account, nano::Gxrb_ratio, send1.hash (), destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	ASSERT_FALSE (ledger.rollback (transaction, open1.hash ()));
	ASSERT_FALSE (store->block_exists (transaction, open1.hash ()));
	ASSERT_EQ (0, ledger.account_balance (transaction, destination.pub));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	nano::pending_info info;
	ASSERT_FALSE (store->pending_get (transaction, nano::pending_key (destination.pub, send1.hash ()), info));
	ASSERT_EQ (nano::genesis_account, info.source);
	ASSERT_EQ (nano::Gxrb_ratio, info.amount.number ());
}

TEST (ledger, state_send_change_rollback)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair rep;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), rep.pub, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_FALSE (ledger.rollback (transaction, send1.hash ()));
	ASSERT_FALSE (store->block_exists (transaction, send1.hash ()));
	ASSERT_EQ (nano::genesis_amount, ledger.account_balance (transaction, nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount, ledger.weight (nano::genesis_account));
	ASSERT_EQ (0, ledger.weight (rep.pub));
}

TEST (ledger, state_receive_change_rollback)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::keypair rep;
	nano::state_block receive1 (nano::genesis_account, send1.hash (), rep.pub, nano::genesis_amount, send1.hash (), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive1).code);
	ASSERT_FALSE (ledger.rollback (transaction, receive1.hash ()));
	ASSERT_FALSE (store->block_exists (transaction, receive1.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.account_balance (transaction, nano::genesis_account));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	ASSERT_EQ (0, ledger.weight (rep.pub));
}

TEST (ledger, epoch_blocks_v1_general)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block epoch1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch1).code);
	nano::state_block epoch2 (nano::genesis_account, epoch1.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch1.hash ()));
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, epoch2).code);
	nano::account_info genesis_info;
	ASSERT_FALSE (ledger.store.account_get (transaction, nano::genesis_account, genesis_info));
	ASSERT_EQ (genesis_info.epoch (), nano::epoch::epoch_1);
	ASSERT_FALSE (ledger.rollback (transaction, epoch1.hash ()));
	ASSERT_FALSE (ledger.store.account_get (transaction, nano::genesis_account, genesis_info));
	ASSERT_EQ (genesis_info.epoch (), nano::epoch::epoch_0);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch1).code);
	ASSERT_FALSE (ledger.store.account_get (transaction, nano::genesis_account, genesis_info));
	ASSERT_EQ (genesis_info.epoch (), nano::epoch::epoch_1);
	nano::change_block change1 (epoch1.hash (), nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch1.hash ()));
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, change1).code);
	nano::state_block send1 (nano::genesis_account, epoch1.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::open_block open1 (send1.hash (), nano::genesis_account, destination.pub, destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::unreceivable, ledger.process (transaction, open1).code);
	nano::state_block epoch3 (destination.pub, 0, nano::genesis_account, 0, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::representative_mismatch, ledger.process (transaction, epoch3).code);
	nano::state_block epoch4 (destination.pub, 0, 0, 0, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch4).code);
	nano::receive_block receive1 (epoch4.hash (), send1.hash (), destination.prv, destination.pub, *pool.generate (epoch4.hash ()));
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, receive1).code);
	nano::state_block receive2 (destination.pub, epoch4.hash (), destination.pub, nano::Gxrb_ratio, send1.hash (), destination.prv, destination.pub, *pool.generate (epoch4.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive2).code);
	ASSERT_EQ (0, ledger.balance (transaction, epoch4.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.balance (transaction, receive2.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, receive2.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.weight (destination.pub));
}

TEST (ledger, epoch_blocks_v2_general)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block epoch1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_2), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	// Trying to upgrade from epoch 0 to epoch 2. It is a requirement epoch upgrades are sequential unless the account is unopened
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, epoch1).code);
	// Set it to the first epoch and it should now succeed
	epoch1 = nano::state_block (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, epoch1.work);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch1).code);
	nano::state_block epoch2 (nano::genesis_account, epoch1.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_2), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch2).code);
	nano::state_block epoch3 (nano::genesis_account, epoch2.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_2), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch2.hash ()));
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, epoch3).code);
	nano::account_info genesis_info;
	ASSERT_FALSE (ledger.store.account_get (transaction, nano::genesis_account, genesis_info));
	ASSERT_EQ (genesis_info.epoch (), nano::epoch::epoch_2);
	ASSERT_FALSE (ledger.rollback (transaction, epoch1.hash ()));
	ASSERT_FALSE (ledger.store.account_get (transaction, nano::genesis_account, genesis_info));
	ASSERT_EQ (genesis_info.epoch (), nano::epoch::epoch_0);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch1).code);
	ASSERT_FALSE (ledger.store.account_get (transaction, nano::genesis_account, genesis_info));
	ASSERT_EQ (genesis_info.epoch (), nano::epoch::epoch_1);
	nano::change_block change1 (epoch1.hash (), nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch1.hash ()));
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, change1).code);
	nano::state_block send1 (nano::genesis_account, epoch1.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::open_block open1 (send1.hash (), nano::genesis_account, destination.pub, destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::unreceivable, ledger.process (transaction, open1).code);
	nano::state_block epoch4 (destination.pub, 0, 0, 0, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch4).code);
	nano::state_block epoch5 (destination.pub, epoch4.hash (), nano::genesis_account, 0, ledger.epoch_link (nano::epoch::epoch_2), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch4.hash ()));
	ASSERT_EQ (nano::process_result::representative_mismatch, ledger.process (transaction, epoch5).code);
	nano::state_block epoch6 (destination.pub, epoch4.hash (), 0, 0, ledger.epoch_link (nano::epoch::epoch_2), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch4.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch6).code);
	nano::receive_block receive1 (epoch6.hash (), send1.hash (), destination.prv, destination.pub, *pool.generate (epoch6.hash ()));
	ASSERT_EQ (nano::process_result::block_position, ledger.process (transaction, receive1).code);
	nano::state_block receive2 (destination.pub, epoch6.hash (), destination.pub, nano::Gxrb_ratio, send1.hash (), destination.prv, destination.pub, *pool.generate (epoch6.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive2).code);
	ASSERT_EQ (0, ledger.balance (transaction, epoch6.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.balance (transaction, receive2.hash ()));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.amount (transaction, receive2.hash ()));
	ASSERT_EQ (nano::genesis_amount - nano::Gxrb_ratio, ledger.weight (nano::genesis_account));
	ASSERT_EQ (nano::Gxrb_ratio, ledger.weight (destination.pub));
}

TEST (ledger, epoch_blocks_receive_upgrade)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::state_block send1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::state_block epoch1 (nano::genesis_account, send1.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch1).code);
	nano::state_block send2 (nano::genesis_account, epoch1.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio * 2, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send2).code);
	nano::open_block open1 (send1.hash (), destination.pub, destination.pub, destination.prv, destination.pub, *pool.generate (destination.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	nano::receive_block receive1 (open1.hash (), send2.hash (), destination.prv, destination.pub, *pool.generate (open1.hash ()));
	ASSERT_EQ (nano::process_result::unreceivable, ledger.process (transaction, receive1).code);
	nano::state_block receive2 (destination.pub, open1.hash (), destination.pub, nano::Gxrb_ratio * 2, send2.hash (), destination.prv, destination.pub, *pool.generate (open1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive2).code);
	nano::account_info destination_info;
	ASSERT_FALSE (ledger.store.account_get (transaction, destination.pub, destination_info));
	ASSERT_EQ (destination_info.epoch (), nano::epoch::epoch_1);
	ASSERT_FALSE (ledger.rollback (transaction, receive2.hash ()));
	ASSERT_FALSE (ledger.store.account_get (transaction, destination.pub, destination_info));
	ASSERT_EQ (destination_info.epoch (), nano::epoch::epoch_0);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive2).code);
	ASSERT_FALSE (ledger.store.account_get (transaction, destination.pub, destination_info));
	ASSERT_EQ (destination_info.epoch (), nano::epoch::epoch_1);
	nano::keypair destination2;
	nano::state_block send3 (destination.pub, receive2.hash (), destination.pub, nano::Gxrb_ratio, destination2.pub, destination.prv, destination.pub, *pool.generate (receive2.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send3).code);
	nano::open_block open2 (send3.hash (), destination2.pub, destination2.pub, destination2.prv, destination2.pub, *pool.generate (destination2.pub));
	ASSERT_EQ (nano::process_result::unreceivable, ledger.process (transaction, open2).code);
	// Upgrade to epoch 2 and send to destination. Try to create an open block from an epoch 2 source block.
	nano::keypair destination3;
	nano::state_block epoch2 (nano::genesis_account, send2.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio * 2, ledger.epoch_link (nano::epoch::epoch_2), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send2.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch2).code);
	nano::state_block send4 (nano::genesis_account, epoch2.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio * 3, destination3.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (epoch2.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send4).code);
	nano::open_block open3 (send4.hash (), destination3.pub, destination3.pub, destination3.prv, destination3.pub, *pool.generate (destination3.pub));
	ASSERT_EQ (nano::process_result::unreceivable, ledger.process (transaction, open3).code);
	// Send it to an epoch 1 account
	nano::state_block send5 (nano::genesis_account, send4.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio * 4, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send4.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send5).code);
	ASSERT_FALSE (ledger.store.account_get (transaction, destination.pub, destination_info));
	ASSERT_EQ (destination_info.epoch (), nano::epoch::epoch_1);
	nano::state_block receive3 (destination.pub, send3.hash (), destination.pub, nano::Gxrb_ratio * 2, send5.hash (), destination.prv, destination.pub, *pool.generate (send3.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive3).code);
	ASSERT_FALSE (ledger.store.account_get (transaction, destination.pub, destination_info));
	ASSERT_EQ (destination_info.epoch (), nano::epoch::epoch_2);
	// Upgrade an unopened account straight to epoch 2
	nano::keypair destination4;
	nano::state_block send6 (nano::genesis_account, send5.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio * 5, destination4.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send5.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send6).code);
	nano::state_block epoch4 (destination4.pub, 0, 0, 0, ledger.epoch_link (nano::epoch::epoch_2), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (destination4.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch4).code);
}

TEST (ledger, epoch_blocks_fork)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	nano::send_block send1 (genesis.hash (), nano::account (0), nano::genesis_amount, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	nano::state_block epoch1 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, epoch1).code);
	nano::state_block epoch2 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_2), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, epoch2).code);
	nano::state_block epoch3 (nano::genesis_account, send1.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch3).code);
	nano::state_block epoch4 (nano::genesis_account, send1.hash (), nano::genesis_account, nano::genesis_amount, ledger.epoch_link (nano::epoch::epoch_2), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send1.hash ()));
	ASSERT_EQ (nano::process_result::fork, ledger.process (transaction, epoch2).code);
}

TEST (ledger, successor_epoch)
{
	nano::system system (1);
	nano::keypair key1;
	nano::genesis genesis;
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::send_block send1 (genesis.hash (), key1.pub, nano::genesis_amount - 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	nano::state_block open (key1.pub, 0, key1.pub, 1, send1.hash (), key1.prv, key1.pub, *pool.generate (key1.pub));
	nano::state_block change (key1.pub, open.hash (), key1.pub, 1, 0, key1.prv, key1.pub, *pool.generate (open.hash ()));
	auto open_hash = open.hash ();
	nano::state_block epoch_open (reinterpret_cast<nano::account const &> (open_hash), 0, 0, 0, system.nodes[0]->ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (open.hash ()));
	auto transaction (system.nodes[0]->store.tx_begin_write ());
	ASSERT_EQ (nano::process_result::progress, system.nodes[0]->ledger.process (transaction, send1).code);
	ASSERT_EQ (nano::process_result::progress, system.nodes[0]->ledger.process (transaction, open).code);
	ASSERT_EQ (nano::process_result::progress, system.nodes[0]->ledger.process (transaction, change).code);
	ASSERT_EQ (nano::process_result::progress, system.nodes[0]->ledger.process (transaction, epoch_open).code);
	ASSERT_EQ (change, *system.nodes[0]->ledger.successor (transaction, change.qualified_root ()));
	ASSERT_EQ (epoch_open, *system.nodes[0]->ledger.successor (transaction, epoch_open.qualified_root ()));
}

TEST (ledger, block_hash_account_conflict)
{
	nano::block_builder builder;
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair key1;
	nano::keypair key2;
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());

	/*
	 * Generate a send block whose destination is a block hash already
	 * in the ledger and not an account
	 */
	std::shared_ptr<nano::state_block> send1 = builder.state ()
												   .account (nano::genesis_account)
												   .previous (genesis.hash ())
												   .representative (nano::genesis_account)
												   .balance (nano::genesis_amount - 100)
												   .link (key1.pub)
												   .sign (nano::test_genesis_key.prv, nano::test_genesis_key.pub)
												   .work (*pool.generate (genesis.hash ()))
												   .build ();

	std::shared_ptr<nano::state_block> receive1 = builder.state ()
													  .account (key1.pub)
													  .previous (0)
													  .representative (nano::genesis_account)
													  .balance (100)
													  .link (send1->hash ())
													  .sign (key1.prv, key1.pub)
													  .work (*pool.generate (key1.pub))
													  .build ();

	/*
	 * Note that the below link is a block hash when this is intended
	 * to represent a send state block. This can generally never be
	 * received , except by epoch blocks, which can sign an open block
	 * for arbitrary accounts.
	 */
	std::shared_ptr<nano::state_block> send2 = builder.state ()
												   .account (key1.pub)
												   .previous (receive1->hash ())
												   .representative (nano::genesis_account)
												   .balance (90)
												   .link (receive1->hash ())
												   .sign (key1.prv, key1.pub)
												   .work (*pool.generate (receive1->hash ()))
												   .build ();

	/*
	 * Generate an epoch open for the account with the same value as the block hash
	 */
	auto receive1_hash = receive1->hash ();
	std::shared_ptr<nano::state_block> open_epoch1 = builder.state ()
														 .account (reinterpret_cast<nano::account const &> (receive1_hash))
														 .previous (0)
														 .representative (0)
														 .balance (0)
														 .link (node1.ledger.epoch_link (nano::epoch::epoch_1))
														 .sign (nano::test_genesis_key.prv, nano::test_genesis_key.pub)
														 .work (*pool.generate (receive1->hash ()))
														 .build ();

	node1.work_generate_blocking (*send1);
	node1.work_generate_blocking (*receive1);
	node1.work_generate_blocking (*send2);
	node1.work_generate_blocking (*open_epoch1);
	auto transaction (node1.store.tx_begin_write ());
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send1).code);
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *receive1).code);
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *send2).code);
	ASSERT_EQ (nano::process_result::progress, node1.ledger.process (transaction, *open_epoch1).code);
	node1.active.start (send1);
	node1.active.start (receive1);
	node1.active.start (send2);
	node1.active.start (open_epoch1);
	auto votes1 (node1.active.roots.find (send1->qualified_root ())->election);
	auto votes2 (node1.active.roots.find (receive1->qualified_root ())->election);
	auto votes3 (node1.active.roots.find (send2->qualified_root ())->election);
	auto votes4 (node1.active.roots.find (open_epoch1->qualified_root ())->election);
	auto winner1 (*votes1->tally ().begin ());
	auto winner2 (*votes2->tally ().begin ());
	auto winner3 (*votes3->tally ().begin ());
	auto winner4 (*votes4->tally ().begin ());
	ASSERT_EQ (*send1, *winner1.second);
	ASSERT_EQ (*receive1, *winner2.second);
	ASSERT_EQ (*send2, *winner3.second);
	ASSERT_EQ (*open_epoch1, *winner4.second);
}

TEST (ledger, could_fit)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair destination;
	// Test legacy and state change blocks could_fit
	nano::change_block change1 (genesis.hash (), nano::genesis_account, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	nano::state_block change2 (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount, 0, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_TRUE (ledger.could_fit (transaction, change1));
	ASSERT_TRUE (ledger.could_fit (transaction, change2));
	// Test legacy and state send
	nano::keypair key1;
	nano::send_block send1 (change1.hash (), key1.pub, nano::genesis_amount - 1, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (change1.hash ()));
	nano::state_block send2 (nano::genesis_account, change1.hash (), nano::genesis_account, nano::genesis_amount - 1, key1.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (change1.hash ()));
	ASSERT_FALSE (ledger.could_fit (transaction, send1));
	ASSERT_FALSE (ledger.could_fit (transaction, send2));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, change1).code);
	ASSERT_TRUE (ledger.could_fit (transaction, change1));
	ASSERT_TRUE (ledger.could_fit (transaction, change2));
	ASSERT_TRUE (ledger.could_fit (transaction, send1));
	ASSERT_TRUE (ledger.could_fit (transaction, send2));
	// Test legacy and state open
	nano::open_block open1 (send2.hash (), nano::genesis_account, key1.pub, key1.prv, key1.pub, *pool.generate (key1.pub));
	nano::state_block open2 (key1.pub, 0, nano::genesis_account, 1, send2.hash (), key1.prv, key1.pub, *pool.generate (key1.pub));
	ASSERT_FALSE (ledger.could_fit (transaction, open1));
	ASSERT_FALSE (ledger.could_fit (transaction, open2));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send2).code);
	ASSERT_TRUE (ledger.could_fit (transaction, send1));
	ASSERT_TRUE (ledger.could_fit (transaction, send2));
	ASSERT_TRUE (ledger.could_fit (transaction, open1));
	ASSERT_TRUE (ledger.could_fit (transaction, open2));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	ASSERT_TRUE (ledger.could_fit (transaction, open1));
	ASSERT_TRUE (ledger.could_fit (transaction, open2));
	// Create another send to receive
	nano::state_block send3 (nano::genesis_account, send2.hash (), nano::genesis_account, nano::genesis_amount - 2, key1.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (send2.hash ()));
	// Test legacy and state receive
	nano::receive_block receive1 (open1.hash (), send3.hash (), key1.prv, key1.pub, *pool.generate (open1.hash ()));
	nano::state_block receive2 (key1.pub, open1.hash (), nano::genesis_account, 2, send3.hash (), key1.prv, key1.pub, *pool.generate (open1.hash ()));
	ASSERT_FALSE (ledger.could_fit (transaction, receive1));
	ASSERT_FALSE (ledger.could_fit (transaction, receive2));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send3).code);
	ASSERT_TRUE (ledger.could_fit (transaction, receive1));
	ASSERT_TRUE (ledger.could_fit (transaction, receive2));
	// Test epoch (state)
	nano::state_block epoch1 (key1.pub, receive1.hash (), nano::genesis_account, 2, ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (receive1.hash ()));
	ASSERT_FALSE (ledger.could_fit (transaction, epoch1));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive1).code);
	ASSERT_TRUE (ledger.could_fit (transaction, receive1));
	ASSERT_TRUE (ledger.could_fit (transaction, receive2));
	ASSERT_TRUE (ledger.could_fit (transaction, epoch1));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, epoch1).code);
	ASSERT_TRUE (ledger.could_fit (transaction, epoch1));
}

TEST (ledger, unchecked_epoch)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair destination;
	auto send1 (std::make_shared<nano::state_block> (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	auto open1 (std::make_shared<nano::state_block> (destination.pub, 0, destination.pub, nano::Gxrb_ratio, send1->hash (), destination.prv, destination.pub, 0));
	node1.work_generate_blocking (*open1);
	auto epoch1 (std::make_shared<nano::state_block> (destination.pub, open1->hash (), destination.pub, nano::Gxrb_ratio, node1.ledger.epoch_link (nano::epoch::epoch_1), nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*epoch1);
	node1.block_processor.add (epoch1);
	node1.block_processor.flush ();
	{
		auto transaction (node1.store.tx_begin_read ());
		auto unchecked_count (node1.store.unchecked_count (transaction));
		ASSERT_EQ (unchecked_count, 1);
		auto blocks (node1.store.unchecked_get (transaction, epoch1->previous ()));
		ASSERT_EQ (blocks.size (), 1);
		ASSERT_EQ (blocks[0].verified, nano::signature_verification::valid_epoch);
	}
	node1.block_processor.add (send1);
	node1.block_processor.add (open1);
	node1.block_processor.flush ();
	{
		auto transaction (node1.store.tx_begin_read ());
		ASSERT_TRUE (node1.store.block_exists (transaction, epoch1->hash ()));
		auto unchecked_count (node1.store.unchecked_count (transaction));
		ASSERT_EQ (unchecked_count, 0);
		nano::account_info info;
		ASSERT_FALSE (node1.store.account_get (transaction, destination.pub, info));
		ASSERT_EQ (info.epoch (), nano::epoch::epoch_1);
	}
}

TEST (ledger, unchecked_epoch_invalid)
{
	nano::system system;
	nano::node_config node_config (nano::get_available_port (), system.logging);
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto & node1 (*system.add_node (node_config));
	nano::genesis genesis;
	nano::keypair destination;
	auto send1 (std::make_shared<nano::state_block> (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	auto open1 (std::make_shared<nano::state_block> (destination.pub, 0, destination.pub, nano::Gxrb_ratio, send1->hash (), destination.prv, destination.pub, 0));
	node1.work_generate_blocking (*open1);
	// Epoch block with account own signature
	auto epoch1 (std::make_shared<nano::state_block> (destination.pub, open1->hash (), destination.pub, nano::Gxrb_ratio, node1.ledger.epoch_link (nano::epoch::epoch_1), destination.prv, destination.pub, 0));
	node1.work_generate_blocking (*epoch1);
	// Pseudo epoch block (send subtype, destination - epoch link)
	auto epoch2 (std::make_shared<nano::state_block> (destination.pub, open1->hash (), destination.pub, nano::Gxrb_ratio - 1, node1.ledger.epoch_link (nano::epoch::epoch_1), destination.prv, destination.pub, 0));
	node1.work_generate_blocking (*epoch2);
	node1.block_processor.add (epoch1);
	node1.block_processor.add (epoch2);
	node1.block_processor.flush ();
	{
		auto transaction (node1.store.tx_begin_read ());
		auto unchecked_count (node1.store.unchecked_count (transaction));
		ASSERT_EQ (unchecked_count, 2);
		auto blocks (node1.store.unchecked_get (transaction, epoch1->previous ()));
		ASSERT_EQ (blocks.size (), 2);
		ASSERT_EQ (blocks[0].verified, nano::signature_verification::valid);
		ASSERT_EQ (blocks[1].verified, nano::signature_verification::valid);
	}
	node1.block_processor.add (send1);
	node1.block_processor.add (open1);
	node1.block_processor.flush ();
	{
		auto transaction (node1.store.tx_begin_read ());
		ASSERT_FALSE (node1.store.block_exists (transaction, epoch1->hash ()));
		ASSERT_TRUE (node1.store.block_exists (transaction, epoch2->hash ()));
		ASSERT_TRUE (node1.active.empty ());
		auto unchecked_count (node1.store.unchecked_count (transaction));
		ASSERT_EQ (unchecked_count, 0);
		nano::account_info info;
		ASSERT_FALSE (node1.store.account_get (transaction, destination.pub, info));
		ASSERT_NE (info.epoch (), nano::epoch::epoch_1);
	}
}

TEST (ledger, unchecked_open)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair destination;
	auto send1 (std::make_shared<nano::state_block> (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	auto open1 (std::make_shared<nano::open_block> (send1->hash (), destination.pub, destination.pub, destination.prv, destination.pub, 0));
	node1.work_generate_blocking (*open1);
	// Invalid signature for open block
	auto open2 (std::make_shared<nano::open_block> (send1->hash (), nano::test_genesis_key.pub, destination.pub, destination.prv, destination.pub, 0));
	node1.work_generate_blocking (*open2);
	open2->signature.bytes[0] ^= 1;
	node1.block_processor.add (open1);
	node1.block_processor.add (open2);
	node1.block_processor.flush ();
	{
		auto transaction (node1.store.tx_begin_read ());
		auto unchecked_count (node1.store.unchecked_count (transaction));
		ASSERT_EQ (unchecked_count, 1);
		auto blocks (node1.store.unchecked_get (transaction, open1->source ()));
		ASSERT_EQ (blocks.size (), 1);
		ASSERT_EQ (blocks[0].verified, nano::signature_verification::valid);
	}
	node1.block_processor.add (send1);
	node1.block_processor.flush ();
	{
		auto transaction (node1.store.tx_begin_read ());
		ASSERT_TRUE (node1.store.block_exists (transaction, open1->hash ()));
		auto unchecked_count (node1.store.unchecked_count (transaction));
		ASSERT_EQ (unchecked_count, 0);
	}
}

TEST (ledger, unchecked_receive)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::genesis genesis;
	nano::keypair destination;
	auto send1 (std::make_shared<nano::state_block> (nano::genesis_account, genesis.hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send1);
	auto send2 (std::make_shared<nano::state_block> (nano::genesis_account, send1->hash (), nano::genesis_account, nano::genesis_amount - 2 * nano::Gxrb_ratio, destination.pub, nano::test_genesis_key.prv, nano::test_genesis_key.pub, 0));
	node1.work_generate_blocking (*send2);
	auto open1 (std::make_shared<nano::open_block> (send1->hash (), destination.pub, destination.pub, destination.prv, destination.pub, 0));
	node1.work_generate_blocking (*open1);
	auto receive1 (std::make_shared<nano::receive_block> (open1->hash (), send2->hash (), destination.prv, destination.pub, 0));
	node1.work_generate_blocking (*receive1);
	node1.block_processor.add (send1);
	node1.block_processor.add (receive1);
	node1.block_processor.flush ();
	// Previous block for receive1 is unknown, signature cannot be validated
	{
		auto transaction (node1.store.tx_begin_read ());
		auto unchecked_count (node1.store.unchecked_count (transaction));
		ASSERT_EQ (unchecked_count, 1);
		auto blocks (node1.store.unchecked_get (transaction, receive1->previous ()));
		ASSERT_EQ (blocks.size (), 1);
		ASSERT_EQ (blocks[0].verified, nano::signature_verification::unknown);
	}
	node1.block_processor.add (open1);
	node1.block_processor.flush ();
	// Previous block for receive1 is known, signature was validated
	{
		auto transaction (node1.store.tx_begin_read ());
		auto unchecked_count (node1.store.unchecked_count (transaction));
		ASSERT_EQ (unchecked_count, 1);
		auto blocks (node1.store.unchecked_get (transaction, receive1->source ()));
		ASSERT_EQ (blocks.size (), 1);
		ASSERT_EQ (blocks[0].verified, nano::signature_verification::valid);
	}
	node1.block_processor.add (send2);
	node1.block_processor.flush ();
	{
		auto transaction (node1.store.tx_begin_read ());
		ASSERT_TRUE (node1.store.block_exists (transaction, receive1->hash ()));
		auto unchecked_count (node1.store.unchecked_count (transaction));
		ASSERT_EQ (unchecked_count, 0);
	}
}

TEST (ledger, confirmation_height_not_updated)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	auto transaction (store->tx_begin_write ());
	nano::genesis genesis;
	store->initialize (transaction, genesis, ledger.rep_weights, ledger.cemented_count, ledger.block_count_cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info account_info;
	ASSERT_FALSE (store->account_get (transaction, nano::test_genesis_key.pub, account_info));
	nano::keypair key;
	nano::send_block send1 (account_info.head, key.pub, 50, nano::test_genesis_key.prv, nano::test_genesis_key.pub, *pool.generate (account_info.head));
	uint64_t confirmation_height;
	ASSERT_FALSE (store->confirmation_height_get (transaction, nano::genesis_account, confirmation_height));
	ASSERT_EQ (1, confirmation_height);
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send1).code);
	ASSERT_FALSE (store->confirmation_height_get (transaction, nano::genesis_account, confirmation_height));
	ASSERT_EQ (1, confirmation_height);
	nano::open_block open1 (send1.hash (), nano::genesis_account, key.pub, key.prv, key.pub, *pool.generate (key.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open1).code);
	ASSERT_FALSE (store->confirmation_height_get (transaction, key.pub, confirmation_height));
	ASSERT_EQ (0, confirmation_height);
}

TEST (ledger, zero_rep)
{
	nano::system system (1);
	nano::genesis genesis;
	nano::block_builder builder;
	auto block1 = builder.state ()
					  .account (nano::test_genesis_key.pub)
					  .previous (genesis.hash ())
					  .representative (0)
					  .balance (nano::genesis_amount)
					  .link (0)
					  .sign (nano::test_genesis_key.prv, nano::test_genesis_key.pub)
					  .work (*system.work.generate (genesis.hash ()))
					  .build ();
	auto transaction (system.nodes[0]->store.tx_begin_write ());
	ASSERT_EQ (nano::process_result::progress, system.nodes[0]->ledger.process (transaction, *block1).code);
	ASSERT_EQ (0, system.nodes[0]->ledger.rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (nano::genesis_amount, system.nodes[0]->ledger.rep_weights.representation_get (0));
	auto block2 = builder.state ()
					  .account (nano::test_genesis_key.pub)
					  .previous (block1->hash ())
					  .representative (nano::test_genesis_key.pub)
					  .balance (nano::genesis_amount)
					  .link (0)
					  .sign (nano::test_genesis_key.prv, nano::test_genesis_key.pub)
					  .work (*system.work.generate (block1->hash ()))
					  .build ();
	ASSERT_EQ (nano::process_result::progress, system.nodes[0]->ledger.process (transaction, *block2).code);
	ASSERT_EQ (nano::genesis_amount, system.nodes[0]->ledger.rep_weights.representation_get (nano::test_genesis_key.pub));
	ASSERT_EQ (0, system.nodes[0]->ledger.rep_weights.representation_get (0));
}
