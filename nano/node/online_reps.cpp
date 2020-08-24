#include <nano/node/online_reps.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/ledger.hpp>

nano::online_reps::online_reps (nano::ledger & ledger_a, nano::environment_constants & constants_a, nano::uint128_t minimum_a) :
ledger (ledger_a),
constants (constants_a),
minimum (minimum_a)
{
	if (!ledger.store.init_error ())
	{
		auto transaction (ledger.store.tx_begin_read ());
		online = trend (transaction);
	}
}

void nano::online_reps::observe (nano::account const & rep_a)
{
	if (ledger.weight (rep_a) > 0)
	{
		nano::lock_guard<std::mutex> lock (mutex);
		reps.insert (rep_a);
	}
}

void nano::online_reps::sample ()
{
	auto transaction (ledger.store.tx_begin_write ({ tables::online_weight }));
	// Discard oldest entries
	while (ledger.store.online_weight_count (transaction) >= constants.node.max_weight_samples)
	{
		auto oldest (ledger.store.online_weight_begin (transaction));
		debug_assert (oldest != ledger.store.online_weight_end ());
		ledger.store.online_weight_del (transaction, oldest->first);
	}
	// Calculate current active rep weight
	nano::uint128_t current;
	std::unordered_set<nano::account> reps_copy;
	{
		nano::lock_guard<std::mutex> lock (mutex);
		reps_copy.swap (reps);
	}
	for (auto & i : reps_copy)
	{
		current += ledger.weight (i);
	}
	ledger.store.online_weight_put (transaction, std::chrono::system_clock::now ().time_since_epoch ().count (), current);
	auto trend_l (trend (transaction));
	nano::lock_guard<std::mutex> lock (mutex);
	online = trend_l;
}

nano::uint128_t nano::online_reps::trend (nano::transaction & transaction_a)
{
	std::vector<nano::uint128_t> items;
	items.reserve (constants.node.max_weight_samples + 1);
	items.push_back (minimum);
	for (auto i (ledger.store.online_weight_begin (transaction_a)), n (ledger.store.online_weight_end ()); i != n; ++i)
	{
		items.push_back (i->second.number ());
	}

	// Pick median value for our target vote weight
	auto median_idx = items.size () / 2;
	nth_element (items.begin (), items.begin () + median_idx, items.end ());
	return nano::uint128_t{ items[median_idx] };
}

nano::uint128_t nano::online_reps::online_stake () const
{
	nano::lock_guard<std::mutex> lock (mutex);
	return std::max (online, minimum);
}

std::vector<nano::account> nano::online_reps::list ()
{
	std::vector<nano::account> result;
	nano::lock_guard<std::mutex> lock (mutex);
	result.insert (result.end (), reps.begin (), reps.end ());
	return result;
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (online_reps & online_reps, const std::string & name)
{
	size_t count;
	{
		nano::lock_guard<std::mutex> guard (online_reps.mutex);
		count = online_reps.reps.size ();
	}

	auto sizeof_element = sizeof (decltype (online_reps.reps)::value_type);
	auto composite = std::make_unique<container_info_composite> (name);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "reps", count, sizeof_element }));
	return composite;
}
