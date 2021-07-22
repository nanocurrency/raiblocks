#pragma once

#include <nano/lib/config.hpp>
#include <nano/lib/rep_weights.hpp>
#include <nano/lib/threading.hpp>
#include <nano/lib/timer.hpp>
#include <nano/secure/buffer.hpp>
#include <nano/secure/store.hpp>
#include <nano/secure/store/account_store_partial.hpp>
#include <nano/secure/store/block_store_partial.hpp>
#include <nano/secure/store/confirmation_height_store_partial.hpp>
#include <nano/secure/store/final_vote_store_partial.hpp>
#include <nano/secure/store/frontier_store_partial.hpp>
#include <nano/secure/store/online_weight_partial.hpp>
#include <nano/secure/store/peer_store_partial.hpp>
#include <nano/secure/store/pending_store_partial.hpp>
#include <nano/secure/store/pruned_store_partial.hpp>
#include <nano/secure/store/unchecked_store_partial.hpp>
#include <nano/secure/store/version_store_partial.hpp>

#include <crypto/cryptopp/words.h>

#include <thread>

class store_partial;
namespace
{
template <typename T>
void parallel_traversal (std::function<void (T const &, T const &, bool const)> const & action);
}

namespace nano
{
template <typename Val, typename Derived_Store>
class block_predecessor_set;

template <typename Val, typename Derived_Store>
void release_assert_success (store_partial<Val, Derived_Store> const & store, const int status)
{
	if (!store.success (status))
	{
		release_assert (false, store.error_string (status));
	}
}

template <typename Val, typename Derived_Store>
class account_store_partial;

template <typename Val, typename Derived_Store>
class unchecked_store_partial;

template <typename Val, typename Derived_Store>
class block_store_partial;

/** This base class implements the store interface functions which have DB agnostic functionality. It also maps all the store classes. */
template <typename Val, typename Derived_Store>
class store_partial : public store
{
	friend void release_assert_success<Val, Derived_Store> (store_partial<Val, Derived_Store> const &, const int);
	friend class nano::block_store_partial<Val, Derived_Store>;
	friend class nano::frontier_store_partial<Val, Derived_Store>;
	friend class nano::account_store_partial<Val, Derived_Store>;
	friend class nano::pending_store_partial<Val, Derived_Store>;
	friend class nano::unchecked_store_partial<Val, Derived_Store>;
	friend class nano::online_weight_store_partial<Val, Derived_Store>;
	friend class nano::pruned_store_partial<Val, Derived_Store>;
	friend class nano::peer_store_partial<Val, Derived_Store>;
	friend class nano::confirmation_height_store_partial<Val, Derived_Store>;
	friend class nano::final_vote_store_partial<Val, Derived_Store>;
	friend class nano::version_store_partial<Val, Derived_Store>;

public:
	// clang-format off
	store_partial (
		nano::block_store_partial<Val, Derived_Store> & block_store_partial_a,
		nano::frontier_store_partial<Val, Derived_Store> & frontier_store_partial_a,
		nano::account_store_partial<Val, Derived_Store> & account_store_partial_a,
		nano::pending_store_partial<Val, Derived_Store> & pending_store_partial_a,
		nano::unchecked_store_partial<Val, Derived_Store> & unchecked_store_partial_a,
		nano::online_weight_store_partial<Val, Derived_Store> & online_weight_store_partial_a,
		nano::pruned_store_partial<Val, Derived_Store> & pruned_store_partial_a,
		nano::peer_store_partial<Val, Derived_Store> & peer_store_partial_a,
		nano::confirmation_height_store_partial<Val, Derived_Store> & confirmation_height_store_partial_a,
		nano::final_vote_store_partial<Val, Derived_Store> & final_vote_store_partial_a,
		nano::version_store_partial<Val, Derived_Store> & version_store_partial_a) :
		store{
			block_store_partial_a,
			frontier_store_partial_a,
			account_store_partial_a,
			pending_store_partial_a,
			unchecked_store_partial_a,
			online_weight_store_partial_a,
			pruned_store_partial_a,
			peer_store_partial_a,
			confirmation_height_store_partial_a,
			final_vote_store_partial_a,
			version_store_partial_a
		}
	{}
	// clang-format on

	/**
	 * If using a different store version than the latest then you may need
	 * to modify some of the objects in the store to be appropriate for the version before an upgrade.
	 */
	void initialize (nano::write_transaction const & transaction_a, nano::ledger_cache & ledger_cache_a) override
	{
		auto hash_l (network_params.ledger.genesis->hash ());
		debug_assert (account.begin (transaction_a) == account.end ());
		network_params.ledger.genesis->sideband_set (nano::block_sideband (network_params.ledger.genesis_account (), 0, nano::dev::genesis_amount, 1, nano::seconds_since_epoch (), nano::epoch::epoch_0, false, false, false, nano::epoch::epoch_0));
		block.put (transaction_a, hash_l, *nano::dev::genesis);
		++ledger_cache_a.block_count;
		confirmation_height.put (transaction_a, network_params.ledger.genesis_account (), nano::confirmation_height_info{ 1, network_params.ledger.genesis->hash () });
		++ledger_cache_a.cemented_count;
		ledger_cache_a.final_votes_confirmation_canary = (network_params.ledger.final_votes_canary_account == network_params.ledger.genesis_account () && 1 >= network_params.ledger.final_votes_canary_height);
		account.put (transaction_a, network_params.ledger.genesis_account (), { hash_l, network_params.ledger.genesis_account (), network_params.ledger.genesis->hash (), std::numeric_limits<nano::uint128_t>::max (), nano::seconds_since_epoch (), 1, nano::epoch::epoch_0 });
		++ledger_cache_a.account_count;
		ledger_cache_a.rep_weights.representation_put (network_params.ledger.genesis_account (), std::numeric_limits<nano::uint128_t>::max ());
		frontier.put (transaction_a, hash_l, network_params.ledger.genesis_account ());
	}

	bool root_exists (nano::transaction const & transaction_a, nano::root const & root_a) override
	{
		return block.exists (transaction_a, root_a.as_block_hash ()) || account.exists (transaction_a, root_a.as_account ());
	}

	bool exists (nano::transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key_a) const
	{
		return static_cast<const Derived_Store &> (*this).exists (transaction_a, table_a, key_a);
	}

	int const minimum_version{ 14 };

protected:
	nano::network_params network_params;
	int const version_number{ 21 };

	template <typename Key, typename Value>
	nano::store_iterator<Key, Value> make_iterator (nano::transaction const & transaction_a, tables table_a, bool const direction_asc = true) const
	{
		return static_cast<Derived_Store const &> (*this).template make_iterator<Key, Value> (transaction_a, table_a, direction_asc);
	}

	template <typename Key, typename Value>
	nano::store_iterator<Key, Value> make_iterator (nano::transaction const & transaction_a, tables table_a, nano::db_val<Val> const & key) const
	{
		return static_cast<Derived_Store const &> (*this).template make_iterator<Key, Value> (transaction_a, table_a, key);
	}

	uint64_t count (nano::transaction const & transaction_a, std::initializer_list<tables> dbs_a) const
	{
		uint64_t total_count = 0;
		for (auto db : dbs_a)
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
		return this->put (transaction_a, table_a, key_a, nano::db_val<Val>{ nullptr });
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
}

namespace
{
template <typename T>
void parallel_traversal (std::function<void (T const &, T const &, bool const)> const & action)
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
