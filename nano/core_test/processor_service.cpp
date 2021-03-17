#include <nano/lib/stats.hpp>
#include <nano/lib/work.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/ledger.hpp>
#include <nano/secure/utility.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

TEST (processor_service, bad_send_signature)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::dev_genesis_key.pub, info1));
	nano::keypair key2;
	nano::send_block send (info1.head, nano::dev_genesis_key.pub, 50, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *pool.generate (info1.head));
	send.signature.bytes[32] ^= 0x1;
	ASSERT_EQ (nano::process_result::bad_signature, ledger.process (transaction, send).code);
}

TEST (processor_service, bad_receive_signature)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::account_info info1;
	ASSERT_FALSE (store->account_get (transaction, nano::dev_genesis_key.pub, info1));
	nano::send_block send (info1.head, nano::dev_genesis_key.pub, 50, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *pool.generate (info1.head));
	nano::block_hash hash1 (send.hash ());
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	nano::account_info info2;
	ASSERT_FALSE (store->account_get (transaction, nano::dev_genesis_key.pub, info2));
	nano::receive_block receive (hash1, hash1, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *pool.generate (hash1));
	receive.signature.bytes[32] ^= 0x1;
	ASSERT_EQ (nano::process_result::bad_signature, ledger.process (transaction, receive).code);
}
