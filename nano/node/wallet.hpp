#pragma once

#include <nano/lib/lmdbconfig.hpp>
#include <nano/lib/locks.hpp>
#include <nano/lib/work.hpp>
#include <nano/node/lmdb/lmdb.hpp>
#include <nano/node/lmdb/wallet_value.hpp>
#include <nano/node/openclwork.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/common.hpp>

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_set>
namespace nano
{
class node;
class node_config;
class wallets;
// The fan spreads a key out over the heap to decrease the likelihood of it being recovered by memory inspection
class fan final
{
public:
	fan (nano::uint256_union const &, size_t);
	void value (nano::raw_key &);
	void value_set (nano::raw_key const &);
	std::vector<std::unique_ptr<nano::uint256_union>> values;

private:
	std::mutex mutex;
	void value_get (nano::raw_key &);
};
class kdf final
{
public:
	void phs (nano::raw_key &, std::string const &, nano::uint256_union const &);
	std::mutex mutex;
};
enum class key_type
{
	not_a_type,
	unknown,
	adhoc,
	deterministic
};
class wallet_store final
{
public:
	wallet_store (bool &, nano::kdf &, nano::transaction &, nano::account, unsigned, std::string const &);
	wallet_store (bool &, nano::kdf &, nano::transaction &, nano::account, unsigned, std::string const &, std::string const &);
	std::vector<nano::account> accounts (nano::transaction const &);
	void initialize (nano::transaction const &, bool &, std::string const &);
	nano::uint256_union check (nano::transaction const &);
	bool rekey (nano::transaction const &, std::string const &);
	bool valid_password (nano::transaction const &);
	bool valid_public_key (nano::public_key const &);
	bool attempt_password (nano::transaction const &, std::string const &);
	void wallet_key (nano::raw_key &, nano::transaction const &);
	void seed (nano::raw_key &, nano::transaction const &);
	void seed_set (nano::transaction const &, nano::raw_key const &);
	nano::key_type key_type (nano::wallet_value const &);
	nano::public_key deterministic_insert (nano::transaction const &);
	nano::public_key deterministic_insert (nano::transaction const &, uint32_t const);
	nano::private_key deterministic_key (nano::transaction const &, uint32_t);
	uint32_t deterministic_index_get (nano::transaction const &);
	void deterministic_index_set (nano::transaction const &, uint32_t);
	void deterministic_clear (nano::transaction const &);
	nano::uint256_union salt (nano::transaction const &);
	bool is_representative (nano::transaction const &);
	nano::account representative (nano::transaction const &);
	void representative_set (nano::transaction const &, nano::account const &);
	nano::public_key insert_adhoc (nano::transaction const &, nano::raw_key const &);
	bool insert_watch (nano::transaction const &, nano::account const &);
	void erase (nano::transaction const &, nano::account const &);
	nano::wallet_value entry_get_raw (nano::transaction const &, nano::account const &);
	void entry_put_raw (nano::transaction const &, nano::account const &, nano::wallet_value const &);
	bool fetch (nano::transaction const &, nano::account const &, nano::raw_key &);
	bool exists (nano::transaction const &, nano::account const &);
	void destroy (nano::transaction const &);
	nano::store_iterator<nano::account, nano::wallet_value> find (nano::transaction const &, nano::account const &);
	nano::store_iterator<nano::account, nano::wallet_value> begin (nano::transaction const &, nano::account const &);
	nano::store_iterator<nano::account, nano::wallet_value> begin (nano::transaction const &);
	nano::store_iterator<nano::account, nano::wallet_value> end ();
	void derive_key (nano::raw_key &, nano::transaction const &, std::string const &);
	void serialize_json (nano::transaction const &, std::string &);
	void write_backup (nano::transaction const &, boost::filesystem::path const &);
	bool move (nano::transaction const &, nano::wallet_store &, std::vector<nano::public_key> const &);
	bool import (nano::transaction const &, nano::wallet_store &);
	bool work_get (nano::transaction const &, nano::public_key const &, uint64_t &);
	void work_put (nano::transaction const &, nano::public_key const &, uint64_t);
	unsigned version (nano::transaction const &);
	void version_put (nano::transaction const &, unsigned);
	nano::fan password;
	nano::fan wallet_key_mem;
	static unsigned const version_1 = 1;
	static unsigned const version_2 = 2;
	static unsigned const version_3 = 3;
	static unsigned const version_4 = 4;
	static unsigned constexpr version_current = version_4;
	static nano::account const version_special;
	static nano::account const wallet_key_special;
	static nano::account const salt_special;
	static nano::account const check_special;
	static nano::account const representative_special;
	static nano::account const seed_special;
	static nano::account const deterministic_index_special;
	static size_t const check_iv_index;
	static size_t const seed_iv_index;
	static int const special_count;
	nano::kdf & kdf;
	std::atomic<MDB_dbi> handle{ 0 };
	std::recursive_mutex mutex;

private:
	MDB_txn * tx (nano::transaction const &) const;
};
// A wallet is a set of account keys encrypted by a common encryption key
class wallet final : public std::enable_shared_from_this<nano::wallet>
{
public:
	std::shared_ptr<nano::block> change_action (nano::account const &, nano::account const &, uint64_t = 0, bool = true);
	std::shared_ptr<nano::block> receive_action (nano::block const &, nano::account const &, nano::uint128_union const &, uint64_t = 0, bool = true);
	std::shared_ptr<nano::block> send_action (nano::account const &, nano::account const &, nano::uint128_t const &, uint64_t = 0, bool = true, boost::optional<std::string> = {});
	bool action_complete (std::shared_ptr<nano::block> const &, nano::account const &, bool const, nano::block_details const &);
	wallet (bool &, nano::transaction &, nano::wallets &, std::string const &);
	wallet (bool &, nano::transaction &, nano::wallets &, std::string const &, std::string const &);
	void enter_initial_password ();
	bool enter_password (nano::transaction const &, std::string const &);
	nano::public_key insert_adhoc (nano::raw_key const &, bool = true);
	nano::public_key insert_adhoc (nano::transaction const &, nano::raw_key const &, bool = true);
	bool insert_watch (nano::transaction const &, nano::public_key const &);
	nano::public_key deterministic_insert (nano::transaction const &, bool = true);
	nano::public_key deterministic_insert (uint32_t, bool = true);
	nano::public_key deterministic_insert (bool = true);
	bool exists (nano::public_key const &);
	bool import (std::string const &, std::string const &);
	void serialize (std::string &);
	bool change_sync (nano::account const &, nano::account const &);
	void change_async (nano::account const &, nano::account const &, std::function<void(std::shared_ptr<nano::block>)> const &, uint64_t = 0, bool = true);
	bool receive_sync (std::shared_ptr<nano::block>, nano::account const &, nano::uint128_t const &);
	void receive_async (std::shared_ptr<nano::block>, nano::account const &, nano::uint128_t const &, std::function<void(std::shared_ptr<nano::block>)> const &, uint64_t = 0, bool = true);
	nano::block_hash send_sync (nano::account const &, nano::account const &, nano::uint128_t const &);
	void send_async (nano::account const &, nano::account const &, nano::uint128_t const &, std::function<void(std::shared_ptr<nano::block>)> const &, uint64_t = 0, bool = true, boost::optional<std::string> = {});
	void work_cache_blocking (nano::account const &, nano::root const &);
	void work_update (nano::transaction const &, nano::account const &, nano::root const &, uint64_t);
	// Schedule work generation after a few seconds
	void work_ensure (nano::account const &, nano::root const &);
	bool search_pending ();
	void init_free_accounts (nano::transaction const &);
	uint32_t deterministic_check (nano::transaction const & transaction_a, uint32_t index);
	/** Changes the wallet seed and returns the first account */
	nano::public_key change_seed (nano::transaction const & transaction_a, nano::raw_key const & prv_a, uint32_t count = 0);
	void deterministic_restore (nano::transaction const & transaction_a);
	bool live ();
	nano::environment_constants constants;
	std::unordered_set<nano::account> free_accounts;
	std::function<void(bool, bool)> lock_observer;
	nano::wallet_store store;
	nano::wallets & wallets;
	std::mutex representatives_mutex;
	std::unordered_set<nano::account> representatives;
};

class work_watcher final : public std::enable_shared_from_this<nano::work_watcher>
{
public:
	work_watcher (nano::node &);
	~work_watcher ();
	void stop ();
	void add (std::shared_ptr<nano::block>);
	void update (nano::qualified_root const &, std::shared_ptr<nano::state_block>);
	void watching (nano::qualified_root const &, std::shared_ptr<nano::state_block>);
	void remove (nano::block const &);
	bool is_watched (nano::qualified_root const &);
	size_t size ();
	std::mutex mutex;
	nano::node & node;
	std::unordered_map<nano::qualified_root, std::shared_ptr<nano::state_block>> watched;
	std::atomic<bool> stopped;
};

class wallet_representatives
{
public:
	uint64_t voting{ 0 }; // Number of representatives with at least the configured minimum voting weight
	uint64_t half_principal{ 0 }; // Number of representatives with at least 50% of principal representative requirements
	std::unordered_set<nano::account> accounts; // Representatives with at least the configured minimum voting weight
	bool have_half_rep () const
	{
		return half_principal > 0;
	}
	bool exists (nano::account const & rep_a) const
	{
		return accounts.count (rep_a) > 0;
	}
	void clear ()
	{
		voting = 0;
		half_principal = 0;
		accounts.clear ();
	}
};

/**
 * The wallets set is all the wallets a node controls.
 * A node may contain multiple wallets independently encrypted and operated.
 */
class wallets final
{
public:
	wallets (bool, nano::node &);
	~wallets ();
	std::shared_ptr<nano::wallet> open (nano::wallet_id const &);
	std::shared_ptr<nano::wallet> create (nano::wallet_id const &);
	bool search_pending (nano::wallet_id const &);
	void search_pending_all ();
	void destroy (nano::wallet_id const &);
	void reload ();
	void do_wallet_actions ();
	void queue_wallet_action (nano::uint128_t const &, std::shared_ptr<nano::wallet>, std::function<void(nano::wallet &)> const &);
	void foreach_representative (std::function<void(nano::public_key const &, nano::raw_key const &)> const &);
	bool exists (nano::transaction const &, nano::account const &);
	void stop ();
	void clear_send_ids (nano::transaction const &);
	nano::wallet_representatives reps () const;
	bool check_rep (nano::account const &, nano::uint128_t const &, const bool = true);
	void compute_reps ();
	void ongoing_compute_reps ();
	void split_if_needed (nano::transaction &, nano::block_store &);
	void move_table (std::string const &, MDB_txn *, MDB_txn *);
	nano::environment_constants constants;
	std::function<void(bool)> observer;
	std::unordered_map<nano::wallet_id, std::shared_ptr<nano::wallet>> items;
	std::multimap<nano::uint128_t, std::pair<std::shared_ptr<nano::wallet>, std::function<void(nano::wallet &)>>, std::greater<nano::uint128_t>> actions;
	nano::locked<std::unordered_map<nano::account, nano::root>> delayed_work;
	std::mutex mutex;
	std::mutex action_mutex;
	nano::condition_variable condition;
	nano::kdf kdf;
	MDB_dbi handle;
	MDB_dbi send_action_ids;
	nano::node & node;
	nano::mdb_env & env;
	std::atomic<bool> stopped;
	std::shared_ptr<nano::work_watcher> watcher;
	std::thread thread;
	static nano::uint128_t const generate_priority;
	static nano::uint128_t const high_priority;
	/** Start read-write transaction */
	nano::write_transaction tx_begin_write ();

	/** Start read-only transaction */
	nano::read_transaction tx_begin_read ();

private:
	mutable std::mutex reps_cache_mutex;
	nano::wallet_representatives representatives;
};

std::unique_ptr<container_info_component> collect_container_info (wallets & wallets, const std::string & name);

class wallets_store
{
public:
	virtual ~wallets_store () = default;
	virtual bool init_error () const = 0;
};
class mdb_wallets_store final : public wallets_store
{
public:
	mdb_wallets_store (boost::filesystem::path const &, nano::lmdb_config const & lmdb_config_a = nano::lmdb_config{});
	nano::mdb_env environment;
	bool init_error () const override;
	bool error{ false };
};
}
