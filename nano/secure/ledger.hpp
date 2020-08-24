#pragma once

#include <nano/lib/rep_weights.hpp>
#include <nano/secure/common.hpp>

#include <map>

namespace nano
{
class block_store;
class stat;
class write_transaction;

using tally_t = std::map<nano::uint128_t, std::shared_ptr<nano::block>, std::greater<nano::uint128_t>>;
class ledger final
{
public:
	ledger (nano::block_store &, nano::stat &, nano::generate_cache const & = nano::generate_cache (), std::function<void()> = nullptr);
	nano::account account (nano::transaction const &, nano::block_hash const &) const;
	nano::uint128_t amount (nano::transaction const &, nano::account const &);
	nano::uint128_t amount (nano::transaction const &, nano::block_hash const &);
	nano::uint128_t balance (nano::transaction const &, nano::block_hash const &) const;
	nano::uint128_t account_balance (nano::transaction const &, nano::account const &);
	nano::uint128_t account_pending (nano::transaction const &, nano::account const &);
	nano::uint128_t weight (nano::account const &);
	std::shared_ptr<nano::block> successor (nano::transaction const &, nano::qualified_root const &);
	std::shared_ptr<nano::block> forked_block (nano::transaction const &, nano::block const &);
	std::shared_ptr<nano::block> backtrack (nano::transaction const &, std::shared_ptr<nano::block> const &, uint64_t);
	bool block_confirmed (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const;
	bool block_not_confirmed_or_not_exists (nano::block const & block_a) const;
	nano::block_hash latest (nano::transaction const &, nano::account const &);
	nano::root latest_root (nano::transaction const &, nano::account const &);
	nano::block_hash representative (nano::transaction const &, nano::block_hash const &);
	nano::block_hash representative_calculated (nano::transaction const &, nano::block_hash const &);
	bool block_exists (nano::block_hash const &);
	std::string block_text (char const *);
	std::string block_text (nano::block_hash const &);
	bool is_send (nano::transaction const &, nano::state_block const &) const;
	nano::account const & block_destination (nano::transaction const &, nano::block const &);
	nano::block_hash block_source (nano::transaction const &, nano::block const &);
	nano::process_return process (nano::write_transaction const &, nano::block &, nano::signature_verification = nano::signature_verification::unknown);
	bool rollback (nano::write_transaction const &, nano::block_hash const &, std::vector<std::shared_ptr<nano::block>> &);
	bool rollback (nano::write_transaction const &, nano::block_hash const &);
	void change_latest (nano::write_transaction const &, nano::account const &, nano::account_info const &, nano::account_info const &);
	void dump_account_chain (nano::account const &, std::ostream & = std::cout);
	bool could_fit (nano::transaction const &, nano::block const &);
	bool can_vote (nano::transaction const &, nano::block const &);
	bool is_epoch_link (nano::link const &);
	std::array<nano::block_hash, 2> dependent_blocks (nano::transaction const &, nano::block const &);
	nano::account const & epoch_signer (nano::link const &) const;
	nano::link const & epoch_link (nano::epoch) const;
	static nano::uint128_t const unit;
	nano::environment_constants constants;
	nano::block_store & store;
	nano::ledger_cache cache;
	nano::stat & stats;
	std::unordered_map<nano::account, nano::uint128_t> bootstrap_weights;
	std::atomic<size_t> bootstrap_weights_size{ 0 };
	uint64_t bootstrap_weight_max_blocks{ 1 };
	std::atomic<bool> check_bootstrap_weights;
	std::function<void()> epoch_2_started_cb;
};

std::unique_ptr<container_info_component> collect_container_info (ledger & ledger, const std::string & name);
}
