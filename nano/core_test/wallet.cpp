#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/lmdb/wallet_value.hpp>
#include <nano/test_common/system.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

#include <boost/filesystem.hpp>

using namespace std::chrono_literals;
unsigned constexpr nano::wallet_store::version_current;

TEST (wallet, no_special_keys_accounts)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (init);
	nano::keypair key1;
	ASSERT_FALSE (wallet.exists (transaction, key1.pub));
	wallet.insert_adhoc (transaction, key1.prv);
	ASSERT_TRUE (wallet.exists (transaction, key1.pub));

	for (uint64_t account = 0; account < nano::wallet_store::special_count; account++)
	{
		nano::account account_l (account);
		ASSERT_FALSE (wallet.exists (transaction, account_l));
	}
}

TEST (wallet, no_key)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (init);
	nano::keypair key1;
	nano::raw_key prv1;
	ASSERT_TRUE (wallet.fetch (transaction, key1.pub, prv1));
	ASSERT_TRUE (wallet.valid_password (transaction));
}

TEST (wallet, fetch_locked)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_TRUE (wallet.valid_password (transaction));
	nano::keypair key1;
	ASSERT_EQ (key1.pub, wallet.insert_adhoc (transaction, key1.prv));
	auto key2 (wallet.deterministic_insert (transaction));
	ASSERT_FALSE (key2.is_zero ());
	nano::raw_key key3;
	key3 = 1;
	wallet.password.value_set (key3);
	ASSERT_FALSE (wallet.valid_password (transaction));
	nano::raw_key key4;
	ASSERT_TRUE (wallet.fetch (transaction, key1.pub, key4));
	ASSERT_TRUE (wallet.fetch (transaction, key2, key4));
}

TEST (wallet, retrieval)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (init);
	nano::keypair key1;
	ASSERT_TRUE (wallet.valid_password (transaction));
	wallet.insert_adhoc (transaction, key1.prv);
	nano::raw_key prv1;
	ASSERT_FALSE (wallet.fetch (transaction, key1.pub, prv1));
	ASSERT_TRUE (wallet.valid_password (transaction));
	ASSERT_EQ (key1.prv, prv1);
	wallet.password.values[0]->bytes[16] ^= 1;
	nano::raw_key prv2;
	ASSERT_TRUE (wallet.fetch (transaction, key1.pub, prv2));
	ASSERT_FALSE (wallet.valid_password (transaction));
}

TEST (wallet, empty_iteration)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (init);
	auto i (wallet.begin (transaction));
	auto j (wallet.end ());
	ASSERT_EQ (i, j);
}

TEST (wallet, one_item_iteration)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (init);
	nano::keypair key1;
	wallet.insert_adhoc (transaction, key1.prv);
	for (auto i (wallet.begin (transaction)), j (wallet.end ()); i != j; ++i)
	{
		ASSERT_EQ (key1.pub, nano::uint256_union (i->first));
		nano::raw_key password;
		wallet.wallet_key (password, transaction);
		nano::raw_key key;
		key.decrypt (nano::wallet_value (i->second).key, password, (nano::uint256_union (i->first)).owords[0].number ());
		ASSERT_EQ (key1.prv, key);
	}
}

TEST (wallet, two_item_iteration)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	nano::keypair key1;
	nano::keypair key2;
	ASSERT_NE (key1.pub, key2.pub);
	std::unordered_set<nano::public_key> pubs;
	std::unordered_set<nano::raw_key> prvs;
	nano::kdf kdf;
	{
		auto transaction (env.tx_begin_write ());
		nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
		ASSERT_FALSE (init);
		wallet.insert_adhoc (transaction, key1.prv);
		wallet.insert_adhoc (transaction, key2.prv);
		for (auto i (wallet.begin (transaction)), j (wallet.end ()); i != j; ++i)
		{
			pubs.insert (i->first);
			nano::raw_key password;
			wallet.wallet_key (password, transaction);
			nano::raw_key key;
			key.decrypt (nano::wallet_value (i->second).key, password, (i->first).owords[0].number ());
			prvs.insert (key);
		}
	}
	ASSERT_EQ (2, pubs.size ());
	ASSERT_EQ (2, prvs.size ());
	ASSERT_NE (pubs.end (), pubs.find (key1.pub));
	ASSERT_NE (prvs.end (), prvs.find (key1.prv));
	ASSERT_NE (pubs.end (), pubs.find (key2.pub));
	ASSERT_NE (prvs.end (), prvs.find (key2.prv));
}

TEST (wallet, insufficient_spend_one)
{
	nano::system system (1);
	nano::keypair key1;
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	auto block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key1.pub, 500));
	ASSERT_NE (nullptr, block);
	ASSERT_EQ (nullptr, system.wallet (0)->send_action (nano::dev::genesis_key.pub, key1.pub, nano::dev::genesis_amount));
}

TEST (wallet, spend_all_one)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::block_hash latest1 (node1.latest (nano::dev::genesis_key.pub));
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key2;
	ASSERT_NE (nullptr, system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, std::numeric_limits<nano::uint128_t>::max ()));
	nano::account_info info2;
	{
		auto transaction (node1.store.tx_begin_read ());
		node1.store.account.get (transaction, nano::dev::genesis_key.pub, info2);
		ASSERT_NE (latest1, info2.head);
		auto block (node1.store.block.get (transaction, info2.head));
		ASSERT_NE (nullptr, block);
		ASSERT_EQ (latest1, block->previous ());
	}
	ASSERT_TRUE (info2.balance.is_zero ());
	ASSERT_EQ (0, node1.balance (nano::dev::genesis_key.pub));
}

TEST (wallet, send_async)
{
	nano::system system (1);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key2;
	std::thread thread ([&system] () {
		ASSERT_TIMELY (10s, system.nodes[0]->balance (nano::dev::genesis_key.pub).is_zero ());
	});
	std::atomic<bool> success (false);
	system.wallet (0)->send_async (nano::dev::genesis_key.pub, key2.pub, std::numeric_limits<nano::uint128_t>::max (), [&success] (std::shared_ptr<nano::block> const & block_a) { ASSERT_NE (nullptr, block_a); success = true; });
	thread.join ();
	ASSERT_TIMELY (2s, success);
}

TEST (wallet, spend)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	nano::block_hash latest1 (node1.latest (nano::dev::genesis_key.pub));
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key2;
	// Sending from empty accounts should always be an error.  Accounts need to be opened with an open block, not a send block.
	ASSERT_EQ (nullptr, system.wallet (0)->send_action (0, key2.pub, 0));
	ASSERT_NE (nullptr, system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, std::numeric_limits<nano::uint128_t>::max ()));
	nano::account_info info2;
	{
		auto transaction (node1.store.tx_begin_read ());
		node1.store.account.get (transaction, nano::dev::genesis_key.pub, info2);
		ASSERT_NE (latest1, info2.head);
		auto block (node1.store.block.get (transaction, info2.head));
		ASSERT_NE (nullptr, block);
		ASSERT_EQ (latest1, block->previous ());
	}
	ASSERT_TRUE (info2.balance.is_zero ());
	ASSERT_EQ (0, node1.balance (nano::dev::genesis_key.pub));
}

TEST (wallet, change)
{
	nano::system system (1);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key2;
	auto block1 (system.nodes[0]->rep_block (nano::dev::genesis_key.pub));
	ASSERT_FALSE (block1.is_zero ());
	ASSERT_NE (nullptr, system.wallet (0)->change_action (nano::dev::genesis_key.pub, key2.pub));
	auto block2 (system.nodes[0]->rep_block (nano::dev::genesis_key.pub));
	ASSERT_FALSE (block2.is_zero ());
	ASSERT_NE (block1, block2);
}

TEST (wallet, partial_spend)
{
	nano::system system (1);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key2;
	ASSERT_NE (nullptr, system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, 500));
	ASSERT_EQ (std::numeric_limits<nano::uint128_t>::max () - 500, system.nodes[0]->balance (nano::dev::genesis_key.pub));
}

TEST (wallet, spend_no_previous)
{
	nano::system system (1);
	{
		system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
		auto transaction (system.nodes[0]->store.tx_begin_read ());
		nano::account_info info1;
		ASSERT_FALSE (system.nodes[0]->store.account.get (transaction, nano::dev::genesis_key.pub, info1));
		for (auto i (0); i < 50; ++i)
		{
			nano::keypair key;
			system.wallet (0)->insert_adhoc (key.prv);
		}
	}
	nano::keypair key2;
	ASSERT_NE (nullptr, system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, 500));
	ASSERT_EQ (std::numeric_limits<nano::uint128_t>::max () - 500, system.nodes[0]->balance (nano::dev::genesis_key.pub));
}

TEST (wallet, find_none)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (init);
	nano::account account (1000);
	ASSERT_EQ (wallet.end (), wallet.find (transaction, account));
}

TEST (wallet, find_existing)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (init);
	nano::keypair key1;
	ASSERT_FALSE (wallet.exists (transaction, key1.pub));
	wallet.insert_adhoc (transaction, key1.prv);
	ASSERT_TRUE (wallet.exists (transaction, key1.pub));
	auto existing (wallet.find (transaction, key1.pub));
	ASSERT_NE (wallet.end (), existing);
	++existing;
	ASSERT_EQ (wallet.end (), existing);
}

TEST (wallet, rekey)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (init);
	nano::raw_key password;
	wallet.password.value (password);
	ASSERT_TRUE (password.is_zero ());
	ASSERT_FALSE (init);
	nano::keypair key1;
	wallet.insert_adhoc (transaction, key1.prv);
	nano::raw_key prv1;
	wallet.fetch (transaction, key1.pub, prv1);
	ASSERT_EQ (key1.prv, prv1);
	ASSERT_FALSE (wallet.rekey (transaction, "1"));
	wallet.password.value (password);
	nano::raw_key password1;
	wallet.derive_key (password1, transaction, "1");
	ASSERT_EQ (password1, password);
	nano::raw_key prv2;
	wallet.fetch (transaction, key1.pub, prv2);
	ASSERT_EQ (key1.prv, prv2);
	*wallet.password.values[0] = 2;
	ASSERT_TRUE (wallet.rekey (transaction, "2"));
}

TEST (account, encode_zero)
{
	nano::account number0 (0);
	std::string str0;
	number0.encode_account (str0);

	/*
	 * Handle different lengths for "xrb_" prefixed and "nano_" prefixed accounts
	 */
	ASSERT_EQ ((str0.front () == 'x') ? 64 : 65, str0.size ());
	ASSERT_EQ (65, str0.size ());
	nano::account number1;
	ASSERT_FALSE (number1.decode_account (str0));
	ASSERT_EQ (number0, number1);
}

TEST (account, encode_all)
{
	nano::account number0;
	number0.decode_hex ("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
	std::string str0;
	number0.encode_account (str0);

	/*
	 * Handle different lengths for "xrb_" prefixed and "nano_" prefixed accounts
	 */
	ASSERT_EQ ((str0.front () == 'x') ? 64 : 65, str0.size ());
	nano::account number1;
	ASSERT_FALSE (number1.decode_account (str0));
	ASSERT_EQ (number0, number1);
}

TEST (account, encode_fail)
{
	nano::account number0 (0);
	std::string str0;
	number0.encode_account (str0);
	str0[16] ^= 1;
	nano::account number1;
	ASSERT_TRUE (number1.decode_account (str0));
}

TEST (wallet, hash_password)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (init);
	nano::raw_key hash1;
	wallet.derive_key (hash1, transaction, "");
	nano::raw_key hash2;
	wallet.derive_key (hash2, transaction, "");
	ASSERT_EQ (hash1, hash2);
	nano::raw_key hash3;
	wallet.derive_key (hash3, transaction, "a");
	ASSERT_NE (hash1, hash3);
}

TEST (fan, reconstitute)
{
	nano::raw_key value0 (0);
	nano::fan fan (value0, 1024);
	for (auto & i : fan.values)
	{
		ASSERT_NE (value0, *i);
	}
	nano::raw_key value1;
	fan.value (value1);
	ASSERT_EQ (value0, value1);
}

TEST (fan, change)
{
	nano::raw_key value0;
	value0 = 0;
	nano::raw_key value1;
	value1 = 1;
	ASSERT_NE (value0, value1);
	nano::fan fan (value0, 1024);
	ASSERT_EQ (1024, fan.values.size ());
	nano::raw_key value2;
	fan.value (value2);
	ASSERT_EQ (value0, value2);
	fan.value_set (value1);
	fan.value (value2);
	ASSERT_EQ (value1, value2);
}

TEST (wallet, reopen_default_password)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	auto transaction (env.tx_begin_write ());
	ASSERT_FALSE (init);
	nano::kdf kdf;
	{
		nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
		ASSERT_FALSE (init);
		ASSERT_TRUE (wallet.valid_password (transaction));
	}
	{
		bool init;
		nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
		ASSERT_FALSE (init);
		ASSERT_TRUE (wallet.valid_password (transaction));
	}
	{
		nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
		ASSERT_FALSE (init);
		wallet.rekey (transaction, "");
		ASSERT_TRUE (wallet.valid_password (transaction));
	}
	{
		bool init;
		nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
		ASSERT_FALSE (init);
		ASSERT_FALSE (wallet.valid_password (transaction));
		wallet.attempt_password (transaction, " ");
		ASSERT_FALSE (wallet.valid_password (transaction));
		wallet.attempt_password (transaction, "");
		ASSERT_TRUE (wallet.valid_password (transaction));
	}
}

TEST (wallet, representative)
{
	auto error (false);
	nano::mdb_env env (error, nano::unique_path ());
	ASSERT_FALSE (error);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (error, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (error);
	ASSERT_FALSE (wallet.is_representative (transaction));
	ASSERT_EQ (nano::dev::genesis->account (), wallet.representative (transaction));
	ASSERT_FALSE (wallet.is_representative (transaction));
	nano::keypair key;
	wallet.representative_set (transaction, key.pub);
	ASSERT_FALSE (wallet.is_representative (transaction));
	ASSERT_EQ (key.pub, wallet.representative (transaction));
	ASSERT_FALSE (wallet.is_representative (transaction));
	wallet.insert_adhoc (transaction, key.prv);
	ASSERT_TRUE (wallet.is_representative (transaction));
}

TEST (wallet, serialize_json_empty)
{
	auto error (false);
	nano::mdb_env env (error, nano::unique_path ());
	ASSERT_FALSE (error);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet1 (error, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (error);
	std::string serialized;
	wallet1.serialize_json (transaction, serialized);
	nano::wallet_store wallet2 (error, kdf, transaction, nano::dev::genesis->account (), 1, "1", serialized);
	ASSERT_FALSE (error);
	nano::raw_key password1;
	nano::raw_key password2;
	wallet1.wallet_key (password1, transaction);
	wallet2.wallet_key (password2, transaction);
	ASSERT_EQ (password1, password2);
	ASSERT_EQ (wallet1.salt (transaction), wallet2.salt (transaction));
	ASSERT_EQ (wallet1.check (transaction), wallet2.check (transaction));
	ASSERT_EQ (wallet1.representative (transaction), wallet2.representative (transaction));
	ASSERT_EQ (wallet1.end (), wallet1.begin (transaction));
	ASSERT_EQ (wallet2.end (), wallet2.begin (transaction));
}

TEST (wallet, serialize_json_one)
{
	auto error (false);
	nano::mdb_env env (error, nano::unique_path ());
	ASSERT_FALSE (error);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet1 (error, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (error);
	nano::keypair key;
	wallet1.insert_adhoc (transaction, key.prv);
	std::string serialized;
	wallet1.serialize_json (transaction, serialized);
	nano::wallet_store wallet2 (error, kdf, transaction, nano::dev::genesis->account (), 1, "1", serialized);
	ASSERT_FALSE (error);
	nano::raw_key password1;
	nano::raw_key password2;
	wallet1.wallet_key (password1, transaction);
	wallet2.wallet_key (password2, transaction);
	ASSERT_EQ (password1, password2);
	ASSERT_EQ (wallet1.salt (transaction), wallet2.salt (transaction));
	ASSERT_EQ (wallet1.check (transaction), wallet2.check (transaction));
	ASSERT_EQ (wallet1.representative (transaction), wallet2.representative (transaction));
	ASSERT_TRUE (wallet2.exists (transaction, key.pub));
	nano::raw_key prv;
	wallet2.fetch (transaction, key.pub, prv);
	ASSERT_EQ (key.prv, prv);
}

TEST (wallet, serialize_json_password)
{
	auto error (false);
	nano::mdb_env env (error, nano::unique_path ());
	ASSERT_FALSE (error);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet1 (error, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (error);
	nano::keypair key;
	wallet1.rekey (transaction, "password");
	wallet1.insert_adhoc (transaction, key.prv);
	std::string serialized;
	wallet1.serialize_json (transaction, serialized);
	nano::wallet_store wallet2 (error, kdf, transaction, nano::dev::genesis->account (), 1, "1", serialized);
	ASSERT_FALSE (error);
	ASSERT_FALSE (wallet2.valid_password (transaction));
	ASSERT_FALSE (wallet2.attempt_password (transaction, "password"));
	ASSERT_TRUE (wallet2.valid_password (transaction));
	nano::raw_key password1;
	nano::raw_key password2;
	wallet1.wallet_key (password1, transaction);
	wallet2.wallet_key (password2, transaction);
	ASSERT_EQ (password1, password2);
	ASSERT_EQ (wallet1.salt (transaction), wallet2.salt (transaction));
	ASSERT_EQ (wallet1.check (transaction), wallet2.check (transaction));
	ASSERT_EQ (wallet1.representative (transaction), wallet2.representative (transaction));
	ASSERT_TRUE (wallet2.exists (transaction, key.pub));
	nano::raw_key prv;
	wallet2.fetch (transaction, key.pub, prv);
	ASSERT_EQ (key.prv, prv);
}

TEST (wallet_store, move)
{
	auto error (false);
	nano::mdb_env env (error, nano::unique_path ());
	ASSERT_FALSE (error);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet1 (error, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	ASSERT_FALSE (error);
	nano::keypair key1;
	wallet1.insert_adhoc (transaction, key1.prv);
	nano::wallet_store wallet2 (error, kdf, transaction, nano::dev::genesis->account (), 1, "1");
	ASSERT_FALSE (error);
	nano::keypair key2;
	wallet2.insert_adhoc (transaction, key2.prv);
	ASSERT_FALSE (wallet1.exists (transaction, key2.pub));
	ASSERT_TRUE (wallet2.exists (transaction, key2.pub));
	std::vector<nano::public_key> keys;
	keys.push_back (key2.pub);
	ASSERT_FALSE (wallet1.move (transaction, wallet2, keys));
	ASSERT_TRUE (wallet1.exists (transaction, key2.pub));
	ASSERT_FALSE (wallet2.exists (transaction, key2.pub));
}

TEST (wallet_store, import)
{
	nano::system system (2);
	auto wallet1 (system.wallet (0));
	auto wallet2 (system.wallet (1));
	nano::keypair key1;
	wallet1->insert_adhoc (key1.prv);
	std::string json;
	wallet1->serialize (json);
	ASSERT_FALSE (wallet2->exists (key1.pub));
	auto error (wallet2->import (json, ""));
	ASSERT_FALSE (error);
	ASSERT_TRUE (wallet2->exists (key1.pub));
}

TEST (wallet_store, fail_import_bad_password)
{
	nano::system system (2);
	auto wallet1 (system.wallet (0));
	auto wallet2 (system.wallet (1));
	nano::keypair key1;
	wallet1->insert_adhoc (key1.prv);
	std::string json;
	wallet1->serialize (json);
	ASSERT_FALSE (wallet2->exists (key1.pub));
	auto error (wallet2->import (json, "1"));
	ASSERT_TRUE (error);
}

TEST (wallet_store, fail_import_corrupt)
{
	nano::system system (2);
	auto wallet1 (system.wallet (1));
	std::string json;
	auto error (wallet1->import (json, "1"));
	ASSERT_TRUE (error);
}

// Test work is precached when a key is inserted
TEST (wallet, work)
{
	nano::system system (1);
	auto wallet (system.wallet (0));
	wallet->insert_adhoc (nano::dev::genesis_key.prv);
	wallet->insert_adhoc (nano::dev::genesis_key.prv);
	auto done (false);
	system.deadline_set (20s);
	while (!done)
	{
		auto transaction (system.wallet (0)->wallets.tx_begin_read ());
		uint64_t work (0);
		if (!wallet->store.work_get (transaction, nano::dev::genesis_key.pub, work))
		{
			done = nano::work_difficulty (nano::dev::genesis->work_version (), nano::dev::genesis->hash (), work) >= system.nodes[0]->default_difficulty (nano::dev::genesis->work_version ());
		}
		ASSERT_NO_ERROR (system.poll ());
	}
}

TEST (wallet, work_generate)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	auto wallet (system.wallet (0));
	nano::uint128_t amount1 (node1.balance (nano::dev::genesis_key.pub));
	uint64_t work1;
	wallet->insert_adhoc (nano::dev::genesis_key.prv);
	nano::account account1;
	{
		auto transaction (node1.wallets.tx_begin_read ());
		account1 = system.account (transaction, 0);
	}
	nano::keypair key;
	auto block (wallet->send_action (nano::dev::genesis_key.pub, key.pub, 100));
	auto transaction (node1.store.tx_begin_read ());
	ASSERT_TIMELY (10s, node1.ledger.account_balance (transaction, nano::dev::genesis_key.pub) != amount1);
	system.deadline_set (10s);
	auto again (true);
	while (again)
	{
		ASSERT_NO_ERROR (system.poll ());
		auto block_transaction (node1.store.tx_begin_read ());
		auto transaction (system.wallet (0)->wallets.tx_begin_read ());
		again = wallet->store.work_get (transaction, account1, work1) || nano::work_difficulty (block->work_version (), node1.ledger.latest_root (block_transaction, account1), work1) < node1.default_difficulty (block->work_version ());
	}
}

TEST (wallet, work_cache_delayed)
{
	nano::system system (1);
	auto & node1 (*system.nodes[0]);
	auto wallet (system.wallet (0));
	uint64_t work1;
	wallet->insert_adhoc (nano::dev::genesis_key.prv);
	nano::account account1;
	{
		auto transaction (node1.wallets.tx_begin_read ());
		account1 = system.account (transaction, 0);
	}
	nano::keypair key;
	auto block1 (wallet->send_action (nano::dev::genesis_key.pub, key.pub, 100));
	ASSERT_EQ (block1->hash (), node1.latest (nano::dev::genesis_key.pub));
	auto block2 (wallet->send_action (nano::dev::genesis_key.pub, key.pub, 100));
	ASSERT_EQ (block2->hash (), node1.latest (nano::dev::genesis_key.pub));
	ASSERT_EQ (block2->hash (), node1.wallets.delayed_work->operator[] (nano::dev::genesis_key.pub));
	auto threshold (node1.default_difficulty (nano::work_version::work_1));
	auto again (true);
	system.deadline_set (10s);
	while (again)
	{
		ASSERT_NO_ERROR (system.poll ());
		if (!wallet->store.work_get (node1.wallets.tx_begin_read (), account1, work1))
		{
			again = nano::work_difficulty (nano::work_version::work_1, block2->hash (), work1) < threshold;
		}
	}
	ASSERT_GE (nano::work_difficulty (nano::work_version::work_1, block2->hash (), work1), threshold);
}

TEST (wallet, insert_locked)
{
	nano::system system (1);
	auto wallet (system.wallet (0));
	{
		auto transaction (wallet->wallets.tx_begin_write ());
		wallet->store.rekey (transaction, "1");
		ASSERT_TRUE (wallet->store.valid_password (transaction));
		wallet->enter_password (transaction, "");
	}
	auto transaction (wallet->wallets.tx_begin_read ());
	ASSERT_FALSE (wallet->store.valid_password (transaction));
	ASSERT_TRUE (wallet->insert_adhoc (nano::keypair ().prv).is_zero ());
}

TEST (wallet, deterministic_keys)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	auto key1 = wallet.deterministic_key (transaction, 0);
	auto key2 = wallet.deterministic_key (transaction, 0);
	ASSERT_EQ (key1, key2);
	auto key3 = wallet.deterministic_key (transaction, 1);
	ASSERT_NE (key1, key3);
	ASSERT_EQ (0, wallet.deterministic_index_get (transaction));
	wallet.deterministic_index_set (transaction, 1);
	ASSERT_EQ (1, wallet.deterministic_index_get (transaction));
	auto key4 (wallet.deterministic_insert (transaction));
	nano::raw_key key5;
	ASSERT_FALSE (wallet.fetch (transaction, key4, key5));
	ASSERT_EQ (key3, key5);
	ASSERT_EQ (2, wallet.deterministic_index_get (transaction));
	wallet.deterministic_index_set (transaction, 1);
	ASSERT_EQ (1, wallet.deterministic_index_get (transaction));
	wallet.erase (transaction, key4);
	ASSERT_FALSE (wallet.exists (transaction, key4));
	auto key8 (wallet.deterministic_insert (transaction));
	ASSERT_EQ (key4, key8);
	auto key6 (wallet.deterministic_insert (transaction));
	nano::raw_key key7;
	ASSERT_FALSE (wallet.fetch (transaction, key6, key7));
	ASSERT_NE (key5, key7);
	ASSERT_EQ (3, wallet.deterministic_index_get (transaction));
	nano::keypair key9;
	ASSERT_EQ (key9.pub, wallet.insert_adhoc (transaction, key9.prv));
	ASSERT_TRUE (wallet.exists (transaction, key9.pub));
	wallet.deterministic_clear (transaction);
	ASSERT_EQ (0, wallet.deterministic_index_get (transaction));
	ASSERT_FALSE (wallet.exists (transaction, key4));
	ASSERT_FALSE (wallet.exists (transaction, key6));
	ASSERT_FALSE (wallet.exists (transaction, key8));
	ASSERT_TRUE (wallet.exists (transaction, key9.pub));
}

TEST (wallet, reseed)
{
	bool init;
	nano::mdb_env env (init, nano::unique_path ());
	ASSERT_FALSE (init);
	auto transaction (env.tx_begin_write ());
	nano::kdf kdf;
	nano::wallet_store wallet (init, kdf, transaction, nano::dev::genesis->account (), 1, "0");
	nano::raw_key seed1;
	seed1 = 1;
	nano::raw_key seed2;
	seed2 = 2;
	wallet.seed_set (transaction, seed1);
	nano::raw_key seed3;
	wallet.seed (seed3, transaction);
	ASSERT_EQ (seed1, seed3);
	auto key1 (wallet.deterministic_insert (transaction));
	ASSERT_EQ (1, wallet.deterministic_index_get (transaction));
	wallet.seed_set (transaction, seed2);
	ASSERT_EQ (0, wallet.deterministic_index_get (transaction));
	nano::raw_key seed4;
	wallet.seed (seed4, transaction);
	ASSERT_EQ (seed2, seed4);
	auto key2 (wallet.deterministic_insert (transaction));
	ASSERT_NE (key1, key2);
	wallet.seed_set (transaction, seed1);
	nano::raw_key seed5;
	wallet.seed (seed5, transaction);
	ASSERT_EQ (seed1, seed5);
	auto key3 (wallet.deterministic_insert (transaction));
	ASSERT_EQ (key1, key3);
}

TEST (wallet, insert_deterministic_locked)
{
	nano::system system (1);
	auto wallet (system.wallet (0));
	auto transaction (wallet->wallets.tx_begin_write ());
	wallet->store.rekey (transaction, "1");
	ASSERT_TRUE (wallet->store.valid_password (transaction));
	wallet->enter_password (transaction, "");
	ASSERT_FALSE (wallet->store.valid_password (transaction));
	ASSERT_TRUE (wallet->deterministic_insert (transaction).is_zero ());
}

TEST (wallet, no_work)
{
	nano::system system (1);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv, false);
	nano::keypair key2;
	auto block (system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, std::numeric_limits<nano::uint128_t>::max (), false));
	ASSERT_NE (nullptr, block);
	ASSERT_NE (0, block->block_work ());
	ASSERT_GE (block->difficulty (), nano::work_threshold (block->work_version (), block->sideband ().details));
	auto transaction (system.wallet (0)->wallets.tx_begin_read ());
	uint64_t cached_work (0);
	system.wallet (0)->store.work_get (transaction, nano::dev::genesis_key.pub, cached_work);
	ASSERT_EQ (0, cached_work);
}

TEST (wallet, send_race)
{
	nano::system system (1);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	nano::keypair key2;
	for (auto i (1); i < 60; ++i)
	{
		ASSERT_NE (nullptr, system.wallet (0)->send_action (nano::dev::genesis_key.pub, key2.pub, nano::Gxrb_ratio));
		ASSERT_EQ (nano::dev::genesis_amount - nano::Gxrb_ratio * i, system.nodes[0]->balance (nano::dev::genesis_key.pub));
	}
}

TEST (wallet, password_race)
{
	nano::system system (1);
	nano::thread_runner runner (system.io_ctx, system.nodes[0]->config.io_threads);
	auto wallet = system.wallet (0);
	std::thread thread ([&wallet] () {
		for (int i = 0; i < 100; i++)
		{
			auto transaction (wallet->wallets.tx_begin_write ());
			wallet->store.rekey (transaction, std::to_string (i));
		}
	});
	for (int i = 0; i < 100; i++)
	{
		auto transaction (wallet->wallets.tx_begin_read ());
		// Password should always be valid, the rekey operation should be atomic.
		bool ok = wallet->store.valid_password (transaction);
		EXPECT_TRUE (ok);
		if (!ok)
		{
			break;
		}
	}
	thread.join ();
	system.stop ();
	runner.join ();
}

TEST (wallet, password_race_corrupt_seed)
{
	nano::system system (1);
	nano::thread_runner runner (system.io_ctx, system.nodes[0]->config.io_threads);
	auto wallet = system.wallet (0);
	nano::raw_key seed;
	{
		auto transaction (wallet->wallets.tx_begin_write ());
		ASSERT_FALSE (wallet->store.rekey (transaction, "4567"));
		wallet->store.seed (seed, transaction);
		ASSERT_FALSE (wallet->store.attempt_password (transaction, "4567"));
	}
	std::vector<std::thread> threads;
	for (int i = 0; i < 100; i++)
	{
		threads.emplace_back ([&wallet] () {
			for (int i = 0; i < 10; i++)
			{
				auto transaction (wallet->wallets.tx_begin_write ());
				wallet->store.rekey (transaction, "0000");
			}
		});
		threads.emplace_back ([&wallet] () {
			for (int i = 0; i < 10; i++)
			{
				auto transaction (wallet->wallets.tx_begin_write ());
				wallet->store.rekey (transaction, "1234");
			}
		});
		threads.emplace_back ([&wallet] () {
			for (int i = 0; i < 10; i++)
			{
				auto transaction (wallet->wallets.tx_begin_read ());
				wallet->store.attempt_password (transaction, "1234");
			}
		});
	}
	for (auto & thread : threads)
	{
		thread.join ();
	}
	system.stop ();
	runner.join ();
	{
		auto transaction (wallet->wallets.tx_begin_write ());
		if (!wallet->store.attempt_password (transaction, "1234"))
		{
			nano::raw_key seed_now;
			wallet->store.seed (seed_now, transaction);
			ASSERT_TRUE (seed_now == seed);
		}
		else if (!wallet->store.attempt_password (transaction, "0000"))
		{
			nano::raw_key seed_now;
			wallet->store.seed (seed_now, transaction);
			ASSERT_TRUE (seed_now == seed);
		}
		else if (!wallet->store.attempt_password (transaction, "4567"))
		{
			nano::raw_key seed_now;
			wallet->store.seed (seed_now, transaction);
			ASSERT_TRUE (seed_now == seed);
		}
		else
		{
			ASSERT_FALSE (true);
		}
	}
}

TEST (wallet, change_seed)
{
	nano::system system (1);
	auto wallet (system.wallet (0));
	wallet->enter_initial_password ();
	nano::raw_key seed1;
	seed1 = 1;
	nano::public_key pub;
	uint32_t index (4);
	auto prv = nano::deterministic_key (seed1, index);
	pub = nano::pub_key (prv);
	wallet->insert_adhoc (nano::dev::genesis_key.prv, false);
	auto block (wallet->send_action (nano::dev::genesis_key.pub, pub, 100));
	ASSERT_NE (nullptr, block);
	system.nodes[0]->block_processor.flush ();
	{
		auto transaction (wallet->wallets.tx_begin_write ());
		wallet->change_seed (transaction, seed1);
		nano::raw_key seed2;
		wallet->store.seed (seed2, transaction);
		ASSERT_EQ (seed1, seed2);
		ASSERT_EQ (index + 1, wallet->store.deterministic_index_get (transaction));
	}
	ASSERT_TRUE (wallet->exists (pub));
}

TEST (wallet, deterministic_restore)
{
	nano::system system (1);
	auto wallet (system.wallet (0));
	wallet->enter_initial_password ();
	nano::raw_key seed1;
	seed1 = 1;
	nano::public_key pub;
	uint32_t index (4);
	{
		auto transaction (wallet->wallets.tx_begin_write ());
		wallet->change_seed (transaction, seed1);
		nano::raw_key seed2;
		wallet->store.seed (seed2, transaction);
		ASSERT_EQ (seed1, seed2);
		ASSERT_EQ (1, wallet->store.deterministic_index_get (transaction));
		auto prv = nano::deterministic_key (seed1, index);
		pub = nano::pub_key (prv);
	}
	wallet->insert_adhoc (nano::dev::genesis_key.prv, false);
	auto block (wallet->send_action (nano::dev::genesis_key.pub, pub, 100));
	ASSERT_NE (nullptr, block);
	system.nodes[0]->block_processor.flush ();
	{
		auto transaction (wallet->wallets.tx_begin_write ());
		wallet->deterministic_restore (transaction);
		ASSERT_EQ (index + 1, wallet->store.deterministic_index_get (transaction));
	}
	ASSERT_TRUE (wallet->exists (pub));
}

TEST (wallet, epoch_2_validation)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	auto & wallet (*system.wallet (0));

	// Upgrade the genesis account to epoch 2
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (node, nano::epoch::epoch_1));
	ASSERT_NE (nullptr, system.upgrade_genesis_epoch (node, nano::epoch::epoch_2));

	wallet.insert_adhoc (nano::dev::genesis_key.prv, false);

	// Test send and receive blocks
	// An epoch 2 receive block should be generated with lower difficulty with high probability
	auto tries = 0;
	auto max_tries = 20;
	auto amount = node.config.receive_minimum.number ();
	while (++tries < max_tries)
	{
		auto send = wallet.send_action (nano::dev::genesis_key.pub, nano::dev::genesis_key.pub, amount, 1);
		ASSERT_NE (nullptr, send);
		ASSERT_EQ (nano::epoch::epoch_2, send->sideband ().details.epoch);
		ASSERT_EQ (nano::epoch::epoch_0, send->sideband ().source_epoch); // Not used for send state blocks

		auto receive = wallet.receive_action (send->hash (), nano::dev::genesis_key.pub, amount, send->link ().as_account (), 1);
		ASSERT_NE (nullptr, receive);
		if (receive->difficulty () < node.network_params.network.publish_thresholds.base)
		{
			ASSERT_GE (receive->difficulty (), node.network_params.network.publish_thresholds.epoch_2_receive);
			ASSERT_EQ (nano::epoch::epoch_2, receive->sideband ().details.epoch);
			ASSERT_EQ (nano::epoch::epoch_2, receive->sideband ().source_epoch);
			break;
		}
	}
	ASSERT_LT (tries, max_tries);

	// Test a change block
	ASSERT_NE (nullptr, wallet.change_action (nano::dev::genesis_key.pub, nano::keypair ().pub, 1));
}

// Receiving from an upgraded account uses the lower threshold and upgrades the receiving account
TEST (wallet, epoch_2_receive_propagation)
{
	auto tries = 0;
	auto const max_tries = 20;
	while (++tries < max_tries)
	{
		nano::system system;
		nano::node_flags node_flags;
		node_flags.disable_request_loop = true;
		auto & node (*system.add_node (node_flags));
		auto & wallet (*system.wallet (0));

		// Upgrade the genesis account to epoch 1
		auto epoch1 = system.upgrade_genesis_epoch (node, nano::epoch::epoch_1);
		ASSERT_NE (nullptr, epoch1);

		nano::keypair key;
		nano::state_block_builder builder;

		// Send and open the account
		wallet.insert_adhoc (nano::dev::genesis_key.prv, false);
		wallet.insert_adhoc (key.prv, false);
		auto amount = node.config.receive_minimum.number ();
		auto send1 = wallet.send_action (nano::dev::genesis_key.pub, key.pub, amount, 1);
		ASSERT_NE (nullptr, send1);
		ASSERT_NE (nullptr, wallet.receive_action (send1->hash (), nano::dev::genesis_key.pub, amount, send1->link ().as_account (), 1));

		// Upgrade the genesis account to epoch 2
		auto epoch2 = system.upgrade_genesis_epoch (node, nano::epoch::epoch_2);
		ASSERT_NE (nullptr, epoch2);

		// Send a block
		auto send2 = wallet.send_action (nano::dev::genesis_key.pub, key.pub, amount, 1);
		ASSERT_NE (nullptr, send2);

		auto receive2 = wallet.receive_action (send2->hash (), key.pub, amount, send2->link ().as_account (), 1);
		ASSERT_NE (nullptr, receive2);
		if (receive2->difficulty () < node.network_params.network.publish_thresholds.base)
		{
			ASSERT_GE (receive2->difficulty (), node.network_params.network.publish_thresholds.epoch_2_receive);
			ASSERT_EQ (nano::epoch::epoch_2, node.store.block.version (node.store.tx_begin_read (), receive2->hash ()));
			ASSERT_EQ (nano::epoch::epoch_2, receive2->sideband ().source_epoch);
			break;
		}
	}
	ASSERT_LT (tries, max_tries);
}

// Opening an upgraded account uses the lower threshold
TEST (wallet, epoch_2_receive_unopened)
{
	// Ensure the lower receive work is used when receiving
	auto tries = 0;
	auto const max_tries = 20;
	while (++tries < max_tries)
	{
		nano::system system;
		nano::node_flags node_flags;
		node_flags.disable_request_loop = true;
		auto & node (*system.add_node (node_flags));
		auto & wallet (*system.wallet (0));

		// Upgrade the genesis account to epoch 1
		auto epoch1 = system.upgrade_genesis_epoch (node, nano::epoch::epoch_1);
		ASSERT_NE (nullptr, epoch1);

		nano::keypair key;
		nano::state_block_builder builder;

		// Send
		wallet.insert_adhoc (nano::dev::genesis_key.prv, false);
		auto amount = node.config.receive_minimum.number ();
		auto send1 = wallet.send_action (nano::dev::genesis_key.pub, key.pub, amount, 1);

		// Upgrade unopened account to epoch_2
		auto epoch2_unopened = nano::state_block (key.pub, 0, 0, 0, node.network_params.ledger.epochs.link (nano::epoch::epoch_2), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (key.pub, node.network_params.network.publish_thresholds.epoch_2));
		ASSERT_EQ (nano::process_result::progress, node.process (epoch2_unopened).code);

		wallet.insert_adhoc (key.prv, false);

		auto receive1 = wallet.receive_action (send1->hash (), key.pub, amount, send1->link ().as_account (), 1);
		ASSERT_NE (nullptr, receive1);
		if (receive1->difficulty () < node.network_params.network.publish_thresholds.base)
		{
			ASSERT_GE (receive1->difficulty (), node.network_params.network.publish_thresholds.epoch_2_receive);
			ASSERT_EQ (nano::epoch::epoch_2, node.store.block.version (node.store.tx_begin_read (), receive1->hash ()));
			ASSERT_EQ (nano::epoch::epoch_1, receive1->sideband ().source_epoch);
			break;
		}
	}
	ASSERT_LT (tries, max_tries);
}

TEST (wallet, foreach_representative_deadlock)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
	node.wallets.compute_reps ();
	ASSERT_EQ (1, node.wallets.reps ().voting);
	node.wallets.foreach_representative ([&node] (nano::public_key const & pub, nano::raw_key const & prv) {
		if (node.wallets.mutex.try_lock ())
		{
			node.wallets.mutex.unlock ();
		}
		else
		{
			ASSERT_FALSE (true);
		}
	});
}

TEST (wallet, search_pending)
{
	nano::system system;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.enable_voting = false;
	config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	nano::node_flags flags;
	flags.disable_search_pending = true;
	auto & node (*system.add_node (config, flags));
	auto & wallet (*system.wallet (0));

	wallet.insert_adhoc (nano::dev::genesis_key.prv);
	nano::block_builder builder;
	auto send = builder.state ()
				.account (nano::dev::genesis->account ())
				.previous (nano::dev::genesis->hash ())
				.representative (nano::dev::genesis->account ())
				.balance (nano::dev::genesis_amount - node.config.receive_minimum.number ())
				.link (nano::dev::genesis->account ())
				.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
				.work (*system.work.generate (nano::dev::genesis->hash ()))
				.build ();
	ASSERT_EQ (nano::process_result::progress, node.process (*send).code);

	// Pending search should start an election
	ASSERT_TRUE (node.active.empty ());
	ASSERT_FALSE (wallet.search_pending (wallet.wallets.tx_begin_read ()));
	auto election = node.active.election (send->qualified_root ());
	ASSERT_NE (nullptr, election);

	// Erase the key so the confirmation does not trigger an automatic receive
	wallet.store.erase (node.wallets.tx_begin_write (), nano::dev::genesis->account ());

	// Now confirm the election
	election->force_confirm ();

	ASSERT_TIMELY (5s, node.block_confirmed (send->hash ()) && node.active.empty ());

	// Re-insert the key
	wallet.insert_adhoc (nano::dev::genesis_key.prv);

	// Pending search should create the receive block
	ASSERT_EQ (2, node.ledger.cache.block_count);
	ASSERT_FALSE (wallet.search_pending (wallet.wallets.tx_begin_read ()));
	ASSERT_TIMELY (3s, node.balance (nano::dev::genesis->account ()) == nano::dev::genesis_amount);
	auto receive_hash = node.ledger.latest (node.store.tx_begin_read (), nano::dev::genesis->account ());
	auto receive = node.block (receive_hash);
	ASSERT_NE (nullptr, receive);
	ASSERT_EQ (receive->sideband ().height, 3);
	ASSERT_EQ (send->hash (), receive->link ().as_block_hash ());
}

TEST (wallet, receive_pruned)
{
	nano::system system;
	nano::node_flags node_flags;
	node_flags.disable_request_loop = true;
	auto & node1 = *system.add_node (node_flags);
	node_flags.enable_pruning = true;
	nano::node_config config (nano::get_available_port (), system.logging);
	config.enable_voting = false; // Remove after allowing pruned voting
	auto & node2 = *system.add_node (config, node_flags);

	auto & wallet1 = *system.wallet (0);
	auto & wallet2 = *system.wallet (1);

	nano::keypair key;
	nano::state_block_builder builder;

	// Send
	wallet1.insert_adhoc (nano::dev::genesis_key.prv, false);
	auto amount = node2.config.receive_minimum.number ();
	auto send1 = wallet1.send_action (nano::dev::genesis_key.pub, key.pub, amount, 1);
	auto send2 = wallet1.send_action (nano::dev::genesis_key.pub, key.pub, 1, 1);

	// Pruning
	ASSERT_TIMELY (5s, node2.ledger.cache.cemented_count == 3);
	{
		auto transaction = node2.store.tx_begin_write ();
		ASSERT_EQ (1, node2.ledger.pruning_action (transaction, send1->hash (), 2));
	}
	ASSERT_EQ (1, node2.ledger.cache.pruned_count);
	ASSERT_TRUE (node2.ledger.block_or_pruned_exists (send1->hash ()));
	ASSERT_FALSE (node2.store.block.exists (node2.store.tx_begin_read (), send1->hash ()));

	wallet2.insert_adhoc (key.prv, false);

	auto open1 = wallet2.receive_action (send1->hash (), key.pub, amount, send1->link ().as_account (), 1);
	ASSERT_NE (nullptr, open1);
	ASSERT_EQ (amount, node2.ledger.balance (node2.store.tx_begin_read (), open1->hash ()));
	ASSERT_TIMELY (5s, node2.ledger.cache.cemented_count == 4);
}
