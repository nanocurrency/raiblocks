#pragma once

#include <nano/lib/rep_weights.hpp>
#include <nano/lib/timer.hpp>
#include <nano/secure/common.hpp>

#include <map>

namespace nano
{
class block_store;
class stat;
class write_transaction;

using tally_t = std::map<nano::uint128_t, std::shared_ptr<nano::block>, std::greater<nano::uint128_t>>;

class uncemented_info
{
public:
	uncemented_info (nano::block_hash const & cemented_frontier, nano::block_hash const & frontier, nano::account const & account);
	nano::block_hash cemented_frontier;
	nano::block_hash frontier;
	nano::account account;
};

class ledger final
{
public:
	ledger (nano::block_store &, nano::stat &, nano::generate_cache const & = nano::generate_cache ());
	nano::account account (nano::transaction const &, nano::block_hash const &) const;
	nano::account account_safe (nano::transaction const &, nano::block_hash const &, bool &) const;
	nano::uint128_t amount (nano::transaction const &, nano::account const &);
	nano::uint128_t amount (nano::transaction const &, nano::block_hash const &);
	/** Safe for previous block, but block hash_a must exist */
	nano::uint128_t amount_safe (nano::transaction const &, nano::block_hash const & hash_a, bool &) const;
	nano::uint128_t balance (nano::transaction const &, nano::block_hash const &) const;
	nano::uint128_t balance_safe (nano::transaction const &, nano::block_hash const &, bool &) const;
	nano::uint128_t account_balance (nano::transaction const &, nano::account const &, bool = false);
	nano::uint128_t account_pending (nano::transaction const &, nano::account const &, bool = false);
	nano::uint128_t weight (nano::account const &);
	std::shared_ptr<nano::block> successor (nano::transaction const &, nano::qualified_root const &);
	std::shared_ptr<nano::block> forked_block (nano::transaction const &, nano::block const &);
	bool block_confirmed (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const;
	nano::block_hash latest (nano::transaction const &, nano::account const &);
	nano::root latest_root (nano::transaction const &, nano::account const &);
	nano::block_hash representative (nano::transaction const &, nano::block_hash const &);
	nano::block_hash representative_calculated (nano::transaction const &, nano::block_hash const &);
	bool block_exists (nano::block_hash const &) const;
	bool block_or_pruned_exists (nano::transaction const &, nano::block_hash const &) const;
	bool block_or_pruned_exists (nano::block_hash const &) const;
	bool block_confirmed_or_pruned_exists (nano::transaction const &, nano::block_hash const &) const;
	std::string block_text (char const *);
	std::string block_text (nano::block_hash const &);
	bool is_send (nano::transaction const &, nano::state_block const &) const;
	nano::account const & block_destination (nano::transaction const &, nano::block const &);
	nano::block_hash block_source (nano::transaction const &, nano::block const &);
	std::pair<nano::block_hash, nano::block_hash> hash_root_random (nano::transaction const &) const;
	nano::process_return process (nano::write_transaction const &, nano::block &, nano::signature_verification = nano::signature_verification::unknown);
	bool rollback (nano::write_transaction const &, nano::block_hash const &, std::vector<std::shared_ptr<nano::block>> &);
	bool rollback (nano::write_transaction const &, nano::block_hash const &);
	void update_account (nano::write_transaction const &, nano::account const &, nano::account_info const &, nano::account_info const &);
	uint64_t pruning_action (nano::write_transaction &, nano::block_hash const &, uint64_t);
	void dump_account_chain (nano::account const &, std::ostream & = std::cout);
	bool could_fit (nano::transaction const &, nano::block const &) const;
	bool dependents_confirmed (nano::transaction const &, nano::block const &) const;
	bool is_epoch_link (nano::link const &) const;
	std::array<nano::block_hash, 2> dependent_blocks (nano::transaction const &, nano::block const &) const;
	nano::account const & epoch_signer (nano::link const &) const;
	nano::link const & epoch_link (nano::epoch) const;
	std::multimap<uint64_t, uncemented_info, std::greater<>> unconfirmed_frontiers () const;
	bool migrate_lmdb_to_rocksdb (boost::filesystem::path const &) const;
	static nano::uint128_t const unit;
	nano::network_params network_params;
	nano::block_store & store;
	nano::ledger_cache cache;
	nano::stat & stats;
	std::unordered_map<nano::account, nano::uint128_t> bootstrap_weights;
	std::atomic<size_t> bootstrap_weights_size{ 0 };
	uint64_t bootstrap_weight_max_blocks{ 1 };
	std::atomic<bool> check_bootstrap_weights;
	bool pruning{ false };

private:
	void initialize (nano::generate_cache const &);
};

std::unique_ptr<container_info_component> collect_container_info (ledger & ledger, std::string const & name);
}
