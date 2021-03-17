#pragma once

#include <nano/lib/config.hpp>
#include <nano/lib/rep_weights.hpp>
#include <nano/lib/threading.hpp>
#include <nano/lib/timer.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/buffer.hpp>

#include <crypto/cryptopp/words.h>

#include <thread>

#define release_assert_success(status)                 \
	if (!success (status))                             \
	{                                                  \
		release_assert (false, error_string (status)); \
	}
namespace
{
template <typename T>
void parallel_traversal (std::function<void(T const &, T const &, bool const)> const & action);
}

namespace nano
{
template <typename Val, typename Derived_Store>
class block_predecessor_set;

/** This base class implements the block_store interface functions which have DB agnostic functionality */
template <typename Val, typename Derived_Store>
class block_store_partial : public block_store
{
public:
	using block_store::block_exists;
	using block_store::unchecked_put;

	friend class nano::block_predecessor_set<Val, Derived_Store>;

	/**
	 * If using a different store version than the latest then you may need
	 * to modify some of the objects in the store to be appropriate for the version before an upgrade.
	 */
	void initialize (nano::write_transaction const & transaction_a, nano::genesis const & genesis_a, nano::ledger_cache & ledger_cache_a) override
	{
		auto const hash_l (genesis_a.hash ());
		debug_assert (accounts_begin (transaction_a) == accounts_end ());
		genesis_a.open->sideband_set (nano::block_sideband (network_params.ledger.genesis_account, 0, network_params.ledger.genesis_amount, 1, nano::seconds_since_epoch (), nano::epoch::epoch_0, false, false, false, nano::epoch::epoch_0));
		block_put (transaction_a, hash_l, *genesis_a.open);
		++ledger_cache_a.block_count;
		confirmation_height_put (transaction_a, network_params.ledger.genesis_account, nano::confirmation_height_info{ 1, genesis_a.hash () });
		++ledger_cache_a.cemented_count;
		account_put (transaction_a, network_params.ledger.genesis_account, { hash_l, network_params.ledger.genesis_account, genesis_a.open->hash (), std::numeric_limits<nano::uint128_t>::max (), nano::seconds_since_epoch (), 1, nano::epoch::epoch_0 });
		++ledger_cache_a.account_count;
		ledger_cache_a.rep_weights.representation_put (network_params.ledger.genesis_account, std::numeric_limits<nano::uint128_t>::max ());
		frontier_put (transaction_a, hash_l, network_params.ledger.genesis_account);
	}

	void block_put (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a, nano::block const & block_a) override
	{
		debug_assert (block_a.sideband ().successor.is_zero () || block_exists (transaction_a, block_a.sideband ().successor));
		std::vector<uint8_t> vector;
		{
			nano::vectorstream stream (vector);
			nano::serialize_block (stream, block_a);
			block_a.sideband ().serialize (stream, block_a.type ());
		}
		block_raw_put (transaction_a, vector, hash_a);
		nano::block_predecessor_set<Val, Derived_Store> predecessor (transaction_a, *this);
		block_a.visit (predecessor);
		debug_assert (block_a.previous ().is_zero () || block_successor (transaction_a, block_a.previous ()) == hash_a);
	}

	// Converts a block hash to a block height
	uint64_t block_account_height (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto const block = block_get (transaction_a, hash_a);
		return block->sideband ().height;
	}

	nano::uint128_t block_balance (nano::transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto const block (block_get (transaction_a, hash_a));
		release_assert (block);
		nano::uint128_t result (block_balance_calculated (block));
		return result;
	}

	std::shared_ptr<nano::block> block_get (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto const value (block_raw_get (transaction_a, hash_a));
		std::shared_ptr<nano::block> result;
		if (value.size () != 0)
		{
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()), value.size ());
			nano::block_type type;
			auto error (try_read (stream, type));
			release_assert (!error);
			result = nano::deserialize_block (stream, type);
			release_assert (result != nullptr);
			nano::block_sideband sideband;
			error = (sideband.deserialize (stream, type));
			release_assert (!error);
			result->sideband_set (sideband);
		}
		return result;
	}

	bool block_exists (nano::transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto const junk = block_raw_get (transaction_a, hash_a);
		return junk.size () != 0;
	}

	std::shared_ptr<nano::block> block_get_no_sideband (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto const value (block_raw_get (transaction_a, hash_a));
		std::shared_ptr<nano::block> result;
		if (value.size () != 0)
		{
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()), value.size ());
			result = nano::deserialize_block (stream);
			debug_assert (result != nullptr);
		}
		return result;
	}

	bool root_exists (nano::transaction const & transaction_a, nano::root const & root_a) override
	{
		return block_exists (transaction_a, root_a.as_block_hash ()) || account_exists (transaction_a, root_a.as_account ());
	}

	nano::account block_account (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto const block (block_get (transaction_a, hash_a));
		debug_assert (block != nullptr);
		return block_account_calculated (*block);
	}

	nano::account block_account_calculated (nano::block const & block_a) const override
	{
		debug_assert (block_a.has_sideband ());
		nano::account result (block_a.account ());
		if (result.is_zero ())
		{
			result = block_a.sideband ().account;
		}
		debug_assert (!result.is_zero ());
		return result;
	}

	nano::uint128_t block_balance_calculated (std::shared_ptr<nano::block> const & block_a) const override
	{
		nano::uint128_t result;
		switch (block_a->type ())
		{
			case nano::block_type::open:
			case nano::block_type::receive:
			case nano::block_type::change:
				result = block_a->sideband ().balance.number ();
				break;
			case nano::block_type::send:
				result = boost::polymorphic_downcast<nano::send_block *> (block_a.get ())->hashables.balance.number ();
				break;
			case nano::block_type::state:
				result = boost::polymorphic_downcast<nano::state_block *> (block_a.get ())->hashables.balance.number ();
				break;
			case nano::block_type::invalid:
			case nano::block_type::not_a_block:
				release_assert (false);
				break;
		}
		return result;
	}

	nano::block_hash block_successor (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		auto const value (block_raw_get (transaction_a, hash_a));
		nano::block_hash result;
		if (value.size () != 0)
		{
			debug_assert (value.size () >= result.bytes.size ());
			auto const type = block_type_from_raw (value.data ());
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()) + block_successor_offset (transaction_a, value.size (), type), result.bytes.size ());
			auto error (nano::try_read (stream, result.bytes));
			(void)error;
			debug_assert (!error);
		}
		else
		{
			result.clear ();
		}
		return result;
	}

	void block_successor_clear (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto const value (block_raw_get (transaction_a, hash_a));
		debug_assert (value.size () != 0);
		auto const type = block_type_from_raw (value.data ());
		std::vector<uint8_t> data (static_cast<uint8_t *> (value.data ()), static_cast<uint8_t *> (value.data ()) + value.size ());
		std::fill_n (data.begin () + block_successor_offset (transaction_a, value.size (), type), sizeof (nano::block_hash), uint8_t{ 0 });
		block_raw_put (transaction_a, data, hash_a);
	}

	nano::store_iterator<nano::unchecked_key, nano::unchecked_info> unchecked_end () const override
	{
		return nano::store_iterator<nano::unchecked_key, nano::unchecked_info> (nullptr);
	}

	nano::store_iterator<nano::endpoint_key, nano::no_value> peers_end () const override
	{
		return nano::store_iterator<nano::endpoint_key, nano::no_value> (nullptr);
	}

	nano::store_iterator<nano::pending_key, nano::pending_info> pending_end () const override
	{
		return nano::store_iterator<nano::pending_key, nano::pending_info> (nullptr);
	}

	nano::store_iterator<uint64_t, nano::amount> online_weight_end () const override
	{
		return nano::store_iterator<uint64_t, nano::amount> (nullptr);
	}

	nano::store_iterator<nano::account, nano::account_info> accounts_end () const override
	{
		return nano::store_iterator<nano::account, nano::account_info> (nullptr);
	}

	nano::store_iterator<nano::block_hash, nano::block_w_sideband> blocks_end () const override
	{
		return nano::store_iterator<nano::block_hash, nano::block_w_sideband> (nullptr);
	}

	nano::store_iterator<nano::account, nano::confirmation_height_info> confirmation_height_end () const override
	{
		return nano::store_iterator<nano::account, nano::confirmation_height_info> (nullptr);
	}

	nano::store_iterator<nano::block_hash, std::nullptr_t> pruned_end () const override
	{
		return nano::store_iterator<nano::block_hash, std::nullptr_t> (nullptr);
	}

	nano::store_iterator<nano::qualified_root, nano::block_hash> final_vote_end () const override
	{
		return nano::store_iterator<nano::qualified_root, nano::block_hash> (nullptr);
	}

	nano::store_iterator<nano::block_hash, nano::account> frontiers_end () const override
	{
		return nano::store_iterator<nano::block_hash, nano::account> (nullptr);
	}

	int version_get (nano::transaction const & transaction_a) const override
	{
		nano::uint256_union const version_key (1);
		nano::db_val<Val> data;
		auto const status = get (transaction_a, tables::meta, nano::db_val<Val> (version_key), data);
		int result (minimum_version);
		if (success (status))
		{
			nano::uint256_union version_value (data);
			debug_assert (version_value.qwords[2] == 0 && version_value.qwords[1] == 0 && version_value.qwords[0] == 0);
			result = version_value.number ().convert_to<int> ();
		}
		return result;
	}

	void block_del (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto const status = del (transaction_a, tables::blocks, hash_a);
		release_assert_success (status);
	}

	nano::epoch block_version (nano::transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto const block = block_get (transaction_a, hash_a);
		if (block && block->type () == nano::block_type::state)
		{
			return block->sideband ().details.epoch;
		}

		return nano::epoch::epoch_0;
	}

	void block_raw_put (nano::write_transaction const & transaction_a, std::vector<uint8_t> const & data, nano::block_hash const & hash_a) override
	{
		nano::db_val<Val> const value{ data.size (), (void *)data.data () };
		auto const status = put (transaction_a, tables::blocks, hash_a, value);
		release_assert_success (status);
	}

	void pending_put (nano::write_transaction const & transaction_a, nano::pending_key const & key_a, nano::pending_info const & pending_info_a) override
	{
		nano::db_val<Val> const pending (pending_info_a);
		auto const status = put (transaction_a, tables::pending, key_a, pending);
		release_assert_success (status);
	}

	void pending_del (nano::write_transaction const & transaction_a, nano::pending_key const & key_a) override
	{
		auto const status = del (transaction_a, tables::pending, key_a);
		release_assert_success (status);
	}

	bool pending_get (nano::transaction const & transaction_a, nano::pending_key const & key_a, nano::pending_info & pending_a) override
	{
		nano::db_val<Val> value;
		nano::db_val<Val> key (key_a);
		auto const status1 = get (transaction_a, tables::pending, key, value);
		release_assert (success (status1) || not_found (status1));
		bool result (true);
		if (success (status1))
		{
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()), value.size ());
			result = pending_a.deserialize (stream);
		}
		return result;
	}

	bool pending_exists (nano::transaction const & transaction_a, nano::pending_key const & key_a) override
	{
		auto iterator (pending_begin (transaction_a, key_a));
		return iterator != pending_end () && nano::pending_key (iterator->first) == key_a;
	}

	bool pending_any (nano::transaction const & transaction_a, nano::account const & account_a) override
	{
		auto iterator (pending_begin (transaction_a, nano::pending_key (account_a, 0)));
		return iterator != pending_end () && nano::pending_key (iterator->first).account == account_a;
	}

	void frontier_put (nano::write_transaction const & transaction_a, nano::block_hash const & block_a, nano::account const & account_a) override
	{
		nano::db_val<Val> const account (account_a);
		auto const status (put (transaction_a, tables::frontiers, block_a, account));
		release_assert_success (status);
	}

	nano::account frontier_get (nano::transaction const & transaction_a, nano::block_hash const & block_a) const override
	{
		nano::db_val<Val> value;
		auto const status (get (transaction_a, tables::frontiers, nano::db_val<Val> (block_a), value));
		release_assert (success (status) || not_found (status));
		nano::account result (0);
		if (success (status))
		{
			result = static_cast<nano::account> (value);
		}
		return result;
	}

	void frontier_del (nano::write_transaction const & transaction_a, nano::block_hash const & block_a) override
	{
		auto const status (del (transaction_a, tables::frontiers, block_a));
		release_assert_success (status);
	}

	void unchecked_put (nano::write_transaction const & transaction_a, nano::unchecked_key const & key_a, nano::unchecked_info const & info_a) override
	{
		nano::db_val<Val> const info (info_a);
		auto const status (put (transaction_a, tables::unchecked, key_a, info));
		release_assert_success (status);
	}

	void unchecked_del (nano::write_transaction const & transaction_a, nano::unchecked_key const & key_a) override
	{
		auto const status (del (transaction_a, tables::unchecked, key_a));
		release_assert_success (status);
	}

	bool unchecked_exists (nano::transaction const & transaction_a, nano::unchecked_key const & unchecked_key_a) override
	{
		nano::db_val<Val> value;
		auto const status (get (transaction_a, tables::unchecked, nano::db_val<Val> (unchecked_key_a), value));
		release_assert (success (status) || not_found (status));
		return (success (status));
	}

	void unchecked_put (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a, std::shared_ptr<nano::block> const & block_a) override
	{
		nano::unchecked_key const key (hash_a, block_a->hash ());
		nano::unchecked_info const info (block_a, block_a->account (), nano::seconds_since_epoch (), nano::signature_verification::unknown);
		unchecked_put (transaction_a, key, info);
	}

	void unchecked_clear (nano::write_transaction const & transaction_a) override
	{
		auto const status = drop (transaction_a, tables::unchecked);
		release_assert_success (status);
	}

	void account_put (nano::write_transaction const & transaction_a, nano::account const & account_a, nano::account_info const & info_a) override
	{
		// Check we are still in sync with other tables
		nano::db_val<Val> const info (info_a);
		auto const status = put (transaction_a, tables::accounts, account_a, info);
		release_assert_success (status);
	}

	void account_del (nano::write_transaction const & transaction_a, nano::account const & account_a) override
	{
		auto const status = del (transaction_a, tables::accounts, account_a);
		release_assert_success (status);
	}

	bool account_get (nano::transaction const & transaction_a, nano::account const & account_a, nano::account_info & info_a) override
	{
		nano::db_val<Val> value;
		nano::db_val<Val> const account (account_a);
		auto const status1 (get (transaction_a, tables::accounts, account, value));
		release_assert (success (status1) || not_found (status1));
		bool result (true);
		if (success (status1))
		{
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()), value.size ());
			result = info_a.deserialize (stream);
		}
		return result;
	}

	bool account_exists (nano::transaction const & transaction_a, nano::account const & account_a) override
	{
		auto iterator (accounts_begin (transaction_a, account_a));
		return iterator != accounts_end () && nano::account (iterator->first) == account_a;
	}

	void online_weight_put (nano::write_transaction const & transaction_a, uint64_t time_a, nano::amount const & amount_a) override
	{
		nano::db_val<Val> const value (amount_a);
		auto const status (put (transaction_a, tables::online_weight, time_a, value));
		release_assert_success (status);
	}

	void online_weight_del (nano::write_transaction const & transaction_a, uint64_t time_a) override
	{
		auto const status (del (transaction_a, tables::online_weight, time_a));
		release_assert_success (status);
	}

	size_t online_weight_count (nano::transaction const & transaction_a) const override
	{
		return count (transaction_a, tables::online_weight);
	}

	void online_weight_clear (nano::write_transaction const & transaction_a) override
	{
		auto const status (drop (transaction_a, tables::online_weight));
		release_assert_success (status);
	}

	void pruned_put (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto const status = put_key (transaction_a, tables::pruned, hash_a);
		release_assert_success (status);
	}

	void pruned_del (nano::write_transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		auto const status = del (transaction_a, tables::pruned, hash_a);
		release_assert_success (status);
	}

	bool pruned_exists (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		return exists (transaction_a, tables::pruned, nano::db_val<Val> (hash_a));
	}

	bool block_or_pruned_exists (nano::transaction const & transaction_a, nano::block_hash const & hash_a) override
	{
		return block_exists (transaction_a, hash_a) || pruned_exists (transaction_a, hash_a);
	}

	size_t pruned_count (nano::transaction const & transaction_a) const override
	{
		return count (transaction_a, tables::pruned);
	}

	void pruned_clear (nano::write_transaction const & transaction_a) override
	{
		auto const status = drop (transaction_a, tables::pruned);
		release_assert_success (status);
	}

	void peer_put (nano::write_transaction const & transaction_a, nano::endpoint_key const & endpoint_a) override
	{
		auto const status = put_key (transaction_a, tables::peers, endpoint_a);
		release_assert_success (status);
	}

	void peer_del (nano::write_transaction const & transaction_a, nano::endpoint_key const & endpoint_a) override
	{
		auto const status (del (transaction_a, tables::peers, endpoint_a));
		release_assert_success (status);
	}

	bool peer_exists (nano::transaction const & transaction_a, nano::endpoint_key const & endpoint_a) const override
	{
		return exists (transaction_a, tables::peers, nano::db_val<Val> (endpoint_a));
	}

	size_t peer_count (nano::transaction const & transaction_a) const override
	{
		return count (transaction_a, tables::peers);
	}

	void peer_clear (nano::write_transaction const & transaction_a) override
	{
		auto const status = drop (transaction_a, tables::peers);
		release_assert_success (status);
	}

	bool exists (nano::transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a) const
	{
		return static_cast<const Derived_Store &> (*this).exists (transaction_a, table_a, key_a);
	}

	uint64_t block_count (nano::transaction const & transaction_a) override
	{
		return count (transaction_a, tables::blocks);
	}

	size_t account_count (nano::transaction const & transaction_a) override
	{
		return count (transaction_a, tables::accounts);
	}

	std::shared_ptr<nano::block> block_random (nano::transaction const & transaction_a) override
	{
		nano::block_hash hash;
		nano::random_pool::generate_block (hash.bytes.data (), hash.bytes.size ());
		auto existing = make_iterator<nano::block_hash, std::shared_ptr<nano::block>> (transaction_a, tables::blocks, nano::db_val<Val> (hash));
		auto const end (nano::store_iterator<nano::block_hash, std::shared_ptr<nano::block>> (nullptr));
		if (existing == end)
		{
			existing = make_iterator<nano::block_hash, std::shared_ptr<nano::block>> (transaction_a, tables::blocks);
		}
		debug_assert (existing != end);
		return existing->second;
	}

	nano::block_hash pruned_random (nano::transaction const & transaction_a) override
	{
		nano::block_hash random_hash;
		nano::random_pool::generate_block (random_hash.bytes.data (), random_hash.bytes.size ());
		auto existing = make_iterator<nano::block_hash, nano::db_val<Val>> (transaction_a, tables::pruned, nano::db_val<Val> (random_hash));
		auto const end (nano::store_iterator<nano::block_hash, nano::db_val<Val>> (nullptr));
		if (existing == end)
		{
			existing = make_iterator<nano::block_hash, nano::db_val<Val>> (transaction_a, tables::pruned);
		}
		return existing != end ? existing->first : 0;
	}

	uint64_t confirmation_height_count (nano::transaction const & transaction_a) override
	{
		return count (transaction_a, tables::confirmation_height);
	}

	void confirmation_height_put (nano::write_transaction const & transaction_a, nano::account const & account_a, nano::confirmation_height_info const & confirmation_height_info_a) override
	{
		nano::db_val<Val> const confirmation_height_info (confirmation_height_info_a);
		auto const status = put (transaction_a, tables::confirmation_height, account_a, confirmation_height_info);
		release_assert_success (status);
	}

	bool confirmation_height_get (nano::transaction const & transaction_a, nano::account const & account_a, nano::confirmation_height_info & confirmation_height_info_a) override
	{
		nano::db_val<Val> value;
		auto const status = get (transaction_a, tables::confirmation_height, nano::db_val<Val> (account_a), value);
		release_assert (success (status) || not_found (status));
		bool result (true);
		if (success (status))
		{
			nano::bufferstream stream (reinterpret_cast<uint8_t const *> (value.data ()), value.size ());
			result = confirmation_height_info_a.deserialize (stream);
		}
		if (result)
		{
			confirmation_height_info_a.height = 0;
			confirmation_height_info_a.frontier = 0;
		}

		return result;
	}

	void confirmation_height_del (nano::write_transaction const & transaction_a, nano::account const & account_a) override
	{
		auto const status (del (transaction_a, tables::confirmation_height, nano::db_val<Val> (account_a)));
		release_assert_success (status);
	}

	bool confirmation_height_exists (nano::transaction const & transaction_a, nano::account const & account_a) const override
	{
		return exists (transaction_a, tables::confirmation_height, nano::db_val<Val> (account_a));
	}

	bool final_vote_put (nano::write_transaction const & transaction_a, nano::qualified_root const & root_a, nano::block_hash const & hash_a) override
	{
		nano::db_val<Val> value;
		auto status = get (transaction_a, tables::final_votes, nano::db_val<Val> (root_a), value);
		release_assert (success (status) || not_found (status));
		bool result (true);
		if (success (status))
		{
			result = static_cast<nano::block_hash> (value) == hash_a;
		}
		else
		{
			status = put (transaction_a, tables::final_votes, root_a, hash_a);
			release_assert_success (status);
		}
		return result;
	}

	std::vector<nano::block_hash> final_vote_get (nano::transaction const & transaction_a, nano::root const & root_a) override
	{
		std::vector<nano::block_hash> result;
		nano::qualified_root const key_start (root_a.raw, 0);
		for (auto i (final_vote_begin (transaction_a, key_start)), n (final_vote_end ()); i != n && nano::qualified_root (i->first).root () == root_a; ++i)
		{
			result.push_back (i->second);
		}
		return result;
	}

	size_t final_vote_count (nano::transaction const & transaction_a) const override
	{
		return count (transaction_a, tables::final_votes);
	}

	void final_vote_del (nano::write_transaction const & transaction_a, nano::root const & root_a) override
	{
		std::vector<nano::qualified_root> final_vote_qualified_roots;
		for (auto i (final_vote_begin (transaction_a, nano::qualified_root (root_a.raw, 0))), n (final_vote_end ()); i != n && nano::qualified_root (i->first).root () == root_a; ++i)
		{
			final_vote_qualified_roots.push_back (i->first);
		}

		for (auto const & final_vote_qualified_root : final_vote_qualified_roots)
		{
			auto const status (del (transaction_a, tables::final_votes, nano::db_val<Val> (final_vote_qualified_root)));
			release_assert_success (status);
		}
	}

	void final_vote_clear (nano::write_transaction const & transaction_a, nano::root const & root_a) override
	{
		final_vote_del (transaction_a, root_a);
	}

	void final_vote_clear (nano::write_transaction const & transaction_a) override
	{
		drop (transaction_a, nano::tables::final_votes);
	}

	void confirmation_height_clear (nano::write_transaction const & transaction_a, nano::account const & account_a) override
	{
		confirmation_height_del (transaction_a, account_a);
	}

	void confirmation_height_clear (nano::write_transaction const & transaction_a) override
	{
		drop (transaction_a, nano::tables::confirmation_height);
	}

	nano::store_iterator<nano::account, nano::account_info> accounts_begin (nano::transaction const & transaction_a, nano::account const & account_a) const override
	{
		return make_iterator<nano::account, nano::account_info> (transaction_a, tables::accounts, nano::db_val<Val> (account_a));
	}

	nano::store_iterator<nano::account, nano::account_info> accounts_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::account, nano::account_info> (transaction_a, tables::accounts);
	}

	nano::store_iterator<nano::block_hash, nano::block_w_sideband> blocks_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::block_hash, nano::block_w_sideband> (transaction_a, tables::blocks);
	}

	nano::store_iterator<nano::block_hash, nano::block_w_sideband> blocks_begin (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		return make_iterator<nano::block_hash, nano::block_w_sideband> (transaction_a, tables::blocks, nano::db_val<Val> (hash_a));
	}

	nano::store_iterator<nano::block_hash, nano::account> frontiers_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::block_hash, nano::account> (transaction_a, tables::frontiers);
	}

	nano::store_iterator<nano::block_hash, nano::account> frontiers_begin (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		return make_iterator<nano::block_hash, nano::account> (transaction_a, tables::frontiers, nano::db_val<Val> (hash_a));
	}

	nano::store_iterator<nano::pending_key, nano::pending_info> pending_begin (nano::transaction const & transaction_a, nano::pending_key const & key_a) const override
	{
		return make_iterator<nano::pending_key, nano::pending_info> (transaction_a, tables::pending, nano::db_val<Val> (key_a));
	}

	nano::store_iterator<nano::pending_key, nano::pending_info> pending_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::pending_key, nano::pending_info> (transaction_a, tables::pending);
	}

	nano::store_iterator<nano::unchecked_key, nano::unchecked_info> unchecked_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::unchecked_key, nano::unchecked_info> (transaction_a, tables::unchecked);
	}

	nano::store_iterator<nano::unchecked_key, nano::unchecked_info> unchecked_begin (nano::transaction const & transaction_a, nano::unchecked_key const & key_a) const override
	{
		return make_iterator<nano::unchecked_key, nano::unchecked_info> (transaction_a, tables::unchecked, nano::db_val<Val> (key_a));
	}

	nano::store_iterator<uint64_t, nano::amount> online_weight_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<uint64_t, nano::amount> (transaction_a, tables::online_weight);
	}

	nano::store_iterator<nano::endpoint_key, nano::no_value> peers_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::endpoint_key, nano::no_value> (transaction_a, tables::peers);
	}

	nano::store_iterator<nano::account, nano::confirmation_height_info> confirmation_height_begin (nano::transaction const & transaction_a, nano::account const & account_a) const override
	{
		return make_iterator<nano::account, nano::confirmation_height_info> (transaction_a, tables::confirmation_height, nano::db_val<Val> (account_a));
	}

	nano::store_iterator<nano::account, nano::confirmation_height_info> confirmation_height_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::account, nano::confirmation_height_info> (transaction_a, tables::confirmation_height);
	}

	nano::store_iterator<nano::block_hash, std::nullptr_t> pruned_begin (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const override
	{
		return make_iterator<nano::block_hash, std::nullptr_t> (transaction_a, tables::pruned, nano::db_val<Val> (hash_a));
	}

	nano::store_iterator<nano::block_hash, std::nullptr_t> pruned_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::block_hash, std::nullptr_t> (transaction_a, tables::pruned);
	}

	nano::store_iterator<nano::qualified_root, nano::block_hash> final_vote_begin (nano::transaction const & transaction_a, nano::qualified_root const & root_a) const override
	{
		return make_iterator<nano::qualified_root, nano::block_hash> (transaction_a, tables::final_votes, nano::db_val<Val> (root_a));
	}

	nano::store_iterator<nano::qualified_root, nano::block_hash> final_vote_begin (nano::transaction const & transaction_a) const override
	{
		return make_iterator<nano::qualified_root, nano::block_hash> (transaction_a, tables::final_votes);
	}

	size_t unchecked_count (nano::transaction const & transaction_a) override
	{
		return count (transaction_a, tables::unchecked);
	}

	void accounts_for_each_par (std::function<void(nano::read_transaction const &, nano::store_iterator<nano::account, nano::account_info>, nano::store_iterator<nano::account, nano::account_info>)> const & action_a) const override
	{
		parallel_traversal<nano::uint256_t> (
		[&action_a, this](nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
			auto const transaction (this->tx_begin_read ());
			action_a (transaction, this->accounts_begin (transaction, start), !is_last ? this->accounts_begin (transaction, end) : this->accounts_end ());
		});
	}

	void confirmation_height_for_each_par (std::function<void(nano::read_transaction const &, nano::store_iterator<nano::account, nano::confirmation_height_info>, nano::store_iterator<nano::account, nano::confirmation_height_info>)> const & action_a) const override
	{
		parallel_traversal<nano::uint256_t> (
		[&action_a, this](nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
			auto const transaction (this->tx_begin_read ());
			action_a (transaction, this->confirmation_height_begin (transaction, start), !is_last ? this->confirmation_height_begin (transaction, end) : this->confirmation_height_end ());
		});
	}

	void pending_for_each_par (std::function<void(nano::read_transaction const &, nano::store_iterator<nano::pending_key, nano::pending_info>, nano::store_iterator<nano::pending_key, nano::pending_info>)> const & action_a) const override
	{
		parallel_traversal<nano::uint512_t> (
		[&action_a, this](nano::uint512_t const & start, nano::uint512_t const & end, bool const is_last) {
			nano::uint512_union union_start (start);
			nano::uint512_union union_end (end);
			nano::pending_key key_start (union_start.uint256s[0].number (), union_start.uint256s[1].number ());
			nano::pending_key key_end (union_end.uint256s[0].number (), union_end.uint256s[1].number ());
			auto const transaction (this->tx_begin_read ());
			action_a (transaction, this->pending_begin (transaction, key_start), !is_last ? this->pending_begin (transaction, key_end) : this->pending_end ());
		});
	}

	void unchecked_for_each_par (std::function<void(nano::read_transaction const &, nano::store_iterator<nano::unchecked_key, nano::unchecked_info>, nano::store_iterator<nano::unchecked_key, nano::unchecked_info>)> const & action_a) const override
	{
		parallel_traversal<nano::uint512_t> (
		[&action_a, this](nano::uint512_t const & start, nano::uint512_t const & end, bool const is_last) {
			nano::uint512_union union_start (start);
			nano::uint512_union union_end (end);
			nano::unchecked_key key_start (union_start.uint256s[0].number (), union_start.uint256s[1].number ());
			nano::unchecked_key key_end (union_end.uint256s[0].number (), union_end.uint256s[1].number ());
			auto const transaction (this->tx_begin_read ());
			action_a (transaction, this->unchecked_begin (transaction, key_start), !is_last ? this->unchecked_begin (transaction, key_end) : this->unchecked_end ());
		});
	}

	void blocks_for_each_par (std::function<void(nano::read_transaction const &, nano::store_iterator<nano::block_hash, block_w_sideband>, nano::store_iterator<nano::block_hash, block_w_sideband>)> const & action_a) const override
	{
		parallel_traversal<nano::uint256_t> (
		[&action_a, this](nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
			auto const transaction (this->tx_begin_read ());
			action_a (transaction, this->blocks_begin (transaction, start), !is_last ? this->blocks_begin (transaction, end) : this->blocks_end ());
		});
	}

	void pruned_for_each_par (std::function<void(nano::read_transaction const &, nano::store_iterator<nano::block_hash, std::nullptr_t>, nano::store_iterator<nano::block_hash, std::nullptr_t>)> const & action_a) const override
	{
		parallel_traversal<nano::uint256_t> (
		[&action_a, this](nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
			auto const transaction (this->tx_begin_read ());
			action_a (transaction, this->pruned_begin (transaction, start), !is_last ? this->pruned_begin (transaction, end) : this->pruned_end ());
		});
	}

	void frontiers_for_each_par (std::function<void(nano::read_transaction const &, nano::store_iterator<nano::block_hash, nano::account>, nano::store_iterator<nano::block_hash, nano::account>)> const & action_a) const override
	{
		parallel_traversal<nano::uint256_t> (
		[&action_a, this](nano::uint256_t const & start, nano::uint256_t const & end, bool const is_last) {
			auto const transaction (this->tx_begin_read ());
			action_a (transaction, this->frontiers_begin (transaction, start), !is_last ? this->frontiers_begin (transaction, end) : this->frontiers_end ());
		});
	}

	void final_vote_for_each_par (std::function<void(nano::read_transaction const &, nano::store_iterator<nano::qualified_root, nano::block_hash>, nano::store_iterator<nano::qualified_root, nano::block_hash>)> const & action_a) const override
	{
		parallel_traversal<nano::uint512_t> (
		[&action_a, this](nano::uint512_t const & start, nano::uint512_t const & end, bool const is_last) {
			auto const transaction (this->tx_begin_read ());
			action_a (transaction, this->final_vote_begin (transaction, start), !is_last ? this->final_vote_begin (transaction, end) : this->final_vote_end ());
		});
	}

	int const minimum_version{ 14 };

protected:
	nano::network_params network_params;
	int const version{ 21 };

	template <typename Key, typename Value>
	nano::store_iterator<Key, Value> make_iterator (nano::transaction const & transaction_a, tables table_a) const
	{
		return static_cast<Derived_Store const &> (*this).template make_iterator<Key, Value> (transaction_a, table_a);
	}

	template <typename Key, typename Value>
	nano::store_iterator<Key, Value> make_iterator (nano::transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key) const
	{
		return static_cast<Derived_Store const &> (*this).template make_iterator<Key, Value> (transaction_a, table_a, key);
	}

	nano::db_val<Val> block_raw_get (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const
	{
		nano::db_val<Val> result;
		auto const status = get (transaction_a, tables::blocks, hash_a, result);
		release_assert (success (status) || not_found (status));
		return result;
	}

	size_t block_successor_offset (nano::transaction const & transaction_a, size_t entry_size_a, nano::block_type type_a) const
	{
		return entry_size_a - nano::block_sideband::size (type_a);
	}

	static nano::block_type block_type_from_raw (void * data_a)
	{
		// The block type is the first byte
		return static_cast<nano::block_type> ((reinterpret_cast<uint8_t const *> (data_a))[0]);
	}

	uint64_t count (nano::transaction const & transaction_a, std::initializer_list<tables> dbs_a) const
	{
		uint64_t total_count = 0;
		for (auto const & db : dbs_a)
		{
			total_count += count (transaction_a, db);
		}
		return total_count;
	}

	int get (nano::transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a, nano::db_val<Val> & value_a) const
	{
		return static_cast<Derived_Store const &> (*this).get (transaction_a, table_a, key_a, value_a);
	}

	int put (nano::write_transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a, nano::db_val<Val> const & value_a)
	{
		return static_cast<Derived_Store &> (*this).put (transaction_a, table_a, key_a, value_a);
	}

	// Put only key without value
	int put_key (nano::write_transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a)
	{
		return put (transaction_a, table_a, key_a, nano::db_val<Val>{ nullptr });
	}

	int del (nano::write_transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a)
	{
		return static_cast<Derived_Store &> (*this).del (transaction_a, table_a, key_a);
	}

	virtual uint64_t count (nano::transaction const & transaction_a, tables table_a) const = 0;
	virtual int drop (nano::write_transaction const & transaction_a, tables table_a) = 0;
	virtual bool not_found (int status) const = 0;
	virtual bool success (int status) const = 0;
	virtual int status_code_not_found () const = 0;
	virtual std::string error_string (int status) const = 0;
};

/**
 * Fill in our predecessors
 */
template <typename Val, typename Derived_Store>
class block_predecessor_set : public nano::block_visitor
{
public:
	block_predecessor_set (nano::write_transaction const & transaction_a, nano::block_store_partial<Val, Derived_Store> & store_a) :
	transaction (transaction_a),
	store (store_a)
	{
	}
	virtual ~block_predecessor_set () = default;
	void fill_value (nano::block const & block_a)
	{
		auto const hash (block_a.hash ());
		auto const value (store.block_raw_get (transaction, block_a.previous ()));
		debug_assert (value.size () != 0);
		auto const type = store.block_type_from_raw (value.data ());
		std::vector<uint8_t> data (static_cast<uint8_t *> (value.data ()), static_cast<uint8_t *> (value.data ()) + value.size ());
		std::copy (hash.bytes.begin (), hash.bytes.end (), data.begin () + store.block_successor_offset (transaction, value.size (), type));
		store.block_raw_put (transaction, data, block_a.previous ());
	}
	void send_block (nano::send_block const & block_a) override
	{
		fill_value (block_a);
	}
	void receive_block (nano::receive_block const & block_a) override
	{
		fill_value (block_a);
	}
	void open_block (nano::open_block const & block_a) override
	{
		// Open blocks don't have a predecessor
	}
	void change_block (nano::change_block const & block_a) override
	{
		fill_value (block_a);
	}
	void state_block (nano::state_block const & block_a) override
	{
		if (!block_a.previous ().is_zero ())
		{
			fill_value (block_a);
		}
	}
	nano::write_transaction const & transaction;
	nano::block_store_partial<Val, Derived_Store> & store;
};
}

namespace
{
template <typename T>
void parallel_traversal (std::function<void(T const &, T const &, bool const)> const & action)
{
	// Between 10 and 40 threads, scales well even in low power systems as long as actions are I/O bound
	unsigned const thread_count = std::max (10u, std::min (40u, 10 * std::thread::hardware_concurrency ()));
	T const value_max{ std::numeric_limits<T>::max () };
	T const split = value_max / thread_count;
	std::vector<std::thread> threads;
	threads.reserve (thread_count);
	for (unsigned thread (0); thread < thread_count; ++thread)
	{
		T const start = thread * split;
		T const end = (thread + 1) * split;
		bool const is_last = thread == thread_count - 1;

		threads.emplace_back ([&action, start, end, is_last] {
			nano::thread_role::set (nano::thread_role::name::db_parallel_traversal);
			action (start, end, is_last);
		});
	}
	for (auto & thread : threads)
	{
		thread.join ();
	}
}
}
