#pragma once

#include <nano/lib/numbers.hpp>
#include <nano/lib/utility.hpp>

#include <memory>
#include <unordered_set>
#include <vector>

namespace nano
{
class environment_constants;
class ledger;
class transaction;

/** Track online representatives and trend online weight */
class online_reps final
{
public:
	online_reps (nano::ledger & ledger_a, nano::environment_constants & constants_a, nano::uint128_t minimum_a);
	/** Add voting account \p rep_account to the set of online representatives */
	void observe (nano::account const & rep_account);
	/** Called periodically to sample online weight */
	void sample ();
	/** Returns the trended online stake, but never less than configured minimum */
	nano::uint128_t online_stake () const;
	/** List of online representatives */
	std::vector<nano::account> list ();

private:
	nano::uint128_t trend (nano::transaction &);
	mutable std::mutex mutex;
	nano::ledger & ledger;
	nano::environment_constants & constants;
	std::unordered_set<nano::account> reps;
	nano::uint128_t online;
	nano::uint128_t minimum;

	friend std::unique_ptr<container_info_component> collect_container_info (online_reps & online_reps, const std::string & name);
};

std::unique_ptr<container_info_component> collect_container_info (online_reps & online_reps, const std::string & name);
}
