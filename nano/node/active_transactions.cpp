#include <nano/lib/threading.hpp>
#include <nano/node/active_transactions.hpp>
#include <nano/node/confirmation_height_processor.hpp>
#include <nano/node/confirmation_solicitor.hpp>
#include <nano/node/election.hpp>
#include <nano/node/node.hpp>
#include <nano/node/repcrawler.hpp>
#include <nano/secure/blockstore.hpp>

#include <boost/format.hpp>
#include <boost/variant/get.hpp>

#include <numeric>

using namespace std::chrono;

size_t constexpr nano::active_transactions::max_active_elections_frontier_insertion;

constexpr std::chrono::minutes nano::active_transactions::expired_optimistic_election_info_cutoff;

nano::active_transactions::active_transactions (nano::node & node_a, nano::confirmation_height_processor & confirmation_height_processor_a) :
confirmation_height_processor (confirmation_height_processor_a),
node (node_a),
multipliers_cb (20, 1.),
trended_active_multiplier (1.0),
generator (node_a.config, node_a.ledger, node_a.wallets, node_a.vote_processor, node_a.history, node_a.network, node_a.stats),
check_all_elections_period (node_a.network_params.network.is_dev_network () ? 10ms : 5s),
election_time_to_live (node_a.network_params.network.is_dev_network () ? 0s : 2s),
prioritized_cutoff (std::max<size_t> (1, node_a.config.active_elections_size / 10)),
thread ([this]() {
	nano::thread_role::set (nano::thread_role::name::request_loop);
	request_loop ();
})
{
	// Register a callback which will get called after a block is cemented
	confirmation_height_processor.add_cemented_observer ([this](std::shared_ptr<nano::block> const & callback_block_a) {
		this->block_cemented_callback (callback_block_a);
	});

	// Register a callback which will get called if a block is already cemented
	confirmation_height_processor.add_block_already_cemented_observer ([this](nano::block_hash const & hash_a) {
		this->block_already_cemented_callback (hash_a);
	});

	nano::unique_lock<nano::mutex> lock (mutex);
	condition.wait (lock, [& started = started] { return started; });
}

nano::active_transactions::~active_transactions ()
{
	stop ();
}

bool nano::active_transactions::insert_election_from_frontiers_confirmation (std::shared_ptr<nano::block> const & block_a, nano::account const & account_a, nano::uint128_t previous_balance_a, nano::election_behavior election_behavior_a)
{
	bool inserted{ false };
	nano::unique_lock<nano::mutex> lock (mutex);
	if (roots.get<tag_root> ().find (block_a->qualified_root ()) == roots.get<tag_root> ().end ())
	{
		std::function<void(std::shared_ptr<nano::block> const &)> election_confirmation_cb;
		if (election_behavior_a == nano::election_behavior::optimistic)
		{
			election_confirmation_cb = [this](std::shared_ptr<nano::block> const & block_a) {
				--optimistic_elections_count;
			};
		}

		auto insert_result = insert_impl (lock, block_a, previous_balance_a, election_behavior_a, election_confirmation_cb);
		inserted = insert_result.inserted;
		if (inserted)
		{
			insert_result.election->transition_active ();
			if (insert_result.election->optimistic ())
			{
				++optimistic_elections_count;
			}
		}
	}
	return inserted;
}

nano::frontiers_confirmation_info nano::active_transactions::get_frontiers_confirmation_info ()
{
	// Limit maximum count of elections to start
	auto rep_counts (node.wallets.reps ());
	bool representative (node.config.enable_voting && rep_counts.voting > 0);
	bool half_princpal_representative (representative && rep_counts.half_principal > 0);
	/* Check less frequently for regular nodes in auto mode */
	bool agressive_mode (half_princpal_representative || node.config.frontiers_confirmation == nano::frontiers_confirmation_mode::always);
	auto is_dev_network = node.network_params.network.is_dev_network ();
	auto roots_size = size ();
	auto check_time_exceeded = std::chrono::steady_clock::now () >= next_frontier_check;
	auto max_elections = max_active_elections_frontier_insertion;
	auto low_active_elections = roots_size < max_elections;
	bool wallets_check_required = (!skip_wallets || !priority_wallet_cementable_frontiers.empty ()) && !agressive_mode;
	// Minimise dropping real-time transactions, set the number of frontiers added to a factor of the maximum number of possible active elections
	auto max_active = node.config.active_elections_size / 20;
	if (roots_size <= max_active && (check_time_exceeded || wallets_check_required || (!is_dev_network && low_active_elections && agressive_mode)))
	{
		// When the number of active elections is low increase max number of elections for setting confirmation height.
		if (max_active > roots_size + max_elections)
		{
			max_elections = max_active - roots_size;
		}
	}
	else
	{
		max_elections = 0;
	}

	return nano::frontiers_confirmation_info{ max_elections, agressive_mode };
}

void nano::active_transactions::set_next_frontier_check (bool agressive_mode_a)
{
	auto request_interval (std::chrono::milliseconds (node.network_params.network.request_interval_ms));
	auto rel_time_next_frontier_check = request_interval * (agressive_mode_a ? 20 : 60);
	// Decrease check time for dev network
	int dev_network_factor = node.network_params.network.is_dev_network () ? 1000 : 1;

	next_frontier_check = steady_clock::now () + (rel_time_next_frontier_check / dev_network_factor);
}

void nano::active_transactions::confirm_prioritized_frontiers (nano::transaction const & transaction_a, uint64_t max_elections_a, uint64_t & elections_count_a)
{
	nano::unique_lock<nano::mutex> lk (mutex);
	auto start_elections_for_prioritized_frontiers = [&transaction_a, &elections_count_a, max_elections_a, &lk, this](prioritize_num_uncemented & cementable_frontiers) {
		while (!cementable_frontiers.empty () && !this->stopped && elections_count_a < max_elections_a && optimistic_elections_count < max_optimistic ())
		{
			auto cementable_account_front_it = cementable_frontiers.get<tag_uncemented> ().begin ();
			auto cementable_account = *cementable_account_front_it;
			cementable_frontiers.get<tag_uncemented> ().erase (cementable_account_front_it);
			if (expired_optimistic_election_infos.get<tag_account> ().count (cementable_account.account) == 0)
			{
				lk.unlock ();
				nano::account_info info;
				auto error = this->node.store.account_get (transaction_a, cementable_account.account, info);
				if (!error)
				{
					if (!this->confirmation_height_processor.is_processing_block (info.head))
					{
						nano::confirmation_height_info confirmation_height_info;
						this->node.store.confirmation_height_get (transaction_a, cementable_account.account, confirmation_height_info);

						if (info.block_count > confirmation_height_info.height)
						{
							auto block (this->node.store.block_get (transaction_a, info.head));
							auto previous_balance (this->node.ledger.balance (transaction_a, block->previous ()));
							auto inserted_election = this->insert_election_from_frontiers_confirmation (block, cementable_account.account, previous_balance, nano::election_behavior::optimistic);
							if (inserted_election)
							{
								++elections_count_a;
							}
						}
					}
				}
				lk.lock ();
			}
		}
	};

	start_elections_for_prioritized_frontiers (priority_wallet_cementable_frontiers);
	start_elections_for_prioritized_frontiers (priority_cementable_frontiers);
}

void nano::active_transactions::block_cemented_callback (std::shared_ptr<nano::block> const & block_a)
{
	auto transaction = node.store.tx_begin_read ();

	boost::optional<nano::election_status_type> election_status_type;
	if (!confirmation_height_processor.is_processing_added_block (block_a->hash ()))
	{
		election_status_type = confirm_block (transaction, block_a);
	}
	else
	{
		// This block was explicitly added to the confirmation height_processor
		election_status_type = nano::election_status_type::active_confirmed_quorum;
	}

	if (election_status_type.is_initialized ())
	{
		if (election_status_type == nano::election_status_type::inactive_confirmation_height)
		{
			nano::account account (0);
			nano::uint128_t amount (0);
			bool is_state_send (false);
			nano::account pending_account (0);
			node.process_confirmed_data (transaction, block_a, block_a->hash (), account, amount, is_state_send, pending_account);
			node.observers.blocks.notify (nano::election_status{ block_a, 0, std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now ().time_since_epoch ()), std::chrono::duration_values<std::chrono::milliseconds>::zero (), 0, 1, 0, nano::election_status_type::inactive_confirmation_height }, {}, account, amount, is_state_send);
		}
		else
		{
			auto hash (block_a->hash ());
			nano::unique_lock<nano::mutex> election_winners_lk (election_winner_details_mutex);
			auto existing (election_winner_details.find (hash));
			if (existing != election_winner_details.end ())
			{
				auto election = existing->second;
				election_winner_details.erase (hash);
				election_winners_lk.unlock ();
				if (election->confirmed () && election->winner ()->hash () == hash)
				{
					nano::unique_lock<nano::mutex> election_lk (election->mutex);
					auto status_l = election->status;
					election_lk.unlock ();
					add_recently_cemented (status_l);
					auto destination (block_a->link ().is_zero () ? block_a->destination () : block_a->link ().as_account ());
					node.receive_confirmed (transaction, hash, destination);
					nano::account account (0);
					nano::uint128_t amount (0);
					bool is_state_send (false);
					nano::account pending_account (0);
					node.process_confirmed_data (transaction, block_a, hash, account, amount, is_state_send, pending_account);
					election_lk.lock ();
					election->status.type = *election_status_type;
					election->status.confirmation_request_count = election->confirmation_request_count;
					status_l = election->status;
					election_lk.unlock ();
					auto votes (election->votes_with_weight ());
					node.observers.blocks.notify (status_l, votes, account, amount, is_state_send);
					if (amount > 0)
					{
						node.observers.account_balance.notify (account, false);
						if (!pending_account.is_zero ())
						{
							node.observers.account_balance.notify (pending_account, true);
						}
					}
				}
			}
		}

		auto const & account (!block_a->account ().is_zero () ? block_a->account () : block_a->sideband ().account);
		debug_assert (!account.is_zero ());

		// Next-block activations are done after cementing hardcoded bootstrap count to allow confirming very large chains without interference
		bool const cemented_bootstrap_count_reached{ node.ledger.cache.cemented_count >= node.ledger.bootstrap_weight_max_blocks };

		// Next-block activations are only done for blocks with previously active elections
		bool const was_active{ *election_status_type == nano::election_status_type::active_confirmed_quorum || *election_status_type == nano::election_status_type::active_confirmation_height };

		// Activations are only done if there is not a large amount of active elections, ensuring frontier confirmation takes place
		auto const low_active_elections = [this] { return this->size () < nano::active_transactions::max_active_elections_frontier_insertion / 2; };

		if (cemented_bootstrap_count_reached && was_active && low_active_elections ())
		{
			// Start or vote for the next unconfirmed block
			activate (account);

			// Start or vote for the next unconfirmed block in the destination account
			auto const & destination (node.ledger.block_destination (transaction, *block_a));
			if (!destination.is_zero () && destination != account)
			{
				activate (destination);
			}
		}
	}
}

void nano::active_transactions::add_election_winner_details (nano::block_hash const & hash_a, std::shared_ptr<nano::election> const & election_a)
{
	nano::lock_guard<nano::mutex> guard (election_winner_details_mutex);
	election_winner_details.emplace (hash_a, election_a);
}

void nano::active_transactions::remove_election_winner_details (nano::block_hash const & hash_a)
{
	nano::lock_guard<nano::mutex> guard (election_winner_details_mutex);
	election_winner_details.erase (hash_a);
}

void nano::active_transactions::block_already_cemented_callback (nano::block_hash const & hash_a)
{
	// Depending on timing there is a situation where the election_winner_details is not reset.
	// This can happen when a block wins an election, and the block is confirmed + observer
	// called before the block hash gets added to election_winner_details. If the block is confirmed
	// callbacks have already been done, so we can safely just remove it.
	remove_election_winner_details (hash_a);
}

void nano::active_transactions::request_confirm (nano::unique_lock<nano::mutex> & lock_a)
{
	debug_assert (lock_a.owns_lock ());

	bool const check_all_elections_l (std::chrono::steady_clock::now () - last_check_all_elections > check_all_elections_period);
	size_t const this_loop_target_l (check_all_elections_l ? roots.size () : prioritized_cutoff);
	auto const elections_l{ list_active_impl (this_loop_target_l) };

	lock_a.unlock ();

	nano::confirmation_solicitor solicitor (node.network, node.config);
	solicitor.prepare (node.rep_crawler.principal_representatives (std::numeric_limits<size_t>::max ()));
	nano::vote_generator_session generator_session (generator);

	auto const election_ttl_cutoff_l (std::chrono::steady_clock::now () - election_time_to_live);
	size_t unconfirmed_count_l (0);
	nano::timer<std::chrono::milliseconds> elapsed (nano::timer_state::started);

	auto const is_watched = [watched = node.wallets.watcher->list_watched ()](nano::qualified_root const & root_a) -> bool {
		return watched.find (root_a) != watched.end ();
	};

	/*
	 * Loop through active elections in descending order of proof-of-work difficulty, requesting confirmation
	 *
	 * Only up to a certain amount of elections are queued for confirmation request and block rebroadcasting. The remaining elections can still be confirmed if votes arrive
	 * Elections extending the soft config.active_elections_size limit are flushed after a certain time-to-live cutoff
	 * Flushed elections are later re-activated via frontier confirmation
	 */
	for (auto const & election_l : elections_l)
	{
		bool const confirmed_l (election_l->confirmed ());

		if (!election_l->prioritized () && unconfirmed_count_l < prioritized_cutoff)
		{
			election_l->prioritize (generator_session);
		}

		unconfirmed_count_l += !confirmed_l;
		bool const overflow_l (unconfirmed_count_l > node.config.active_elections_size && election_l->election_start < election_ttl_cutoff_l && !is_watched (election_l->qualified_root));
		if (overflow_l || election_l->transition_time (solicitor))
		{
			if (election_l->optimistic () && election_l->failed ())
			{
				if (election_l->confirmation_request_count != 0)
				{
					// Locks active mutex
					add_expired_optimistic_election (*election_l);
				}
				--optimistic_elections_count;
			}

			// Locks active mutex, cleans up the election and erases it from the main container
			erase (election_l->qualified_root);
		}
	}

	solicitor.flush ();
	generator_session.flush ();
	lock_a.lock ();

	// This is updated after the loop to ensure slow machines don't do the full check often
	if (check_all_elections_l)
	{
		last_check_all_elections = std::chrono::steady_clock::now ();
		if (node.config.logging.timing_logging () && this_loop_target_l > prioritized_cutoff)
		{
			node.logger.try_log (boost::str (boost::format ("Processed %1% elections (%2% were already confirmed) in %3% %4%") % this_loop_target_l % (this_loop_target_l - unconfirmed_count_l) % elapsed.value ().count () % elapsed.unit ()));
		}
	}
}

void nano::active_transactions::cleanup_election (nano::unique_lock<nano::mutex> & lock_a, nano::election_cleanup_info const & info_a)
{
	debug_assert (lock_a.owns_lock ());

	if (!info_a.confirmed)
	{
		node.stats.inc (nano::stat::type::election, nano::stat::detail::election_drop);
	}

	for (auto const & [hash, block] : info_a.blocks)
	{
		auto erased (blocks.erase (hash));
		(void)erased;
		debug_assert (erased == 1);
		erase_inactive_votes_cache (hash);
	}

	lock_a.unlock ();
	for (auto const & [hash, block] : info_a.blocks)
	{
		// Notify observers about dropped elections & blocks lost confirmed elections
		if (!info_a.confirmed || hash != info_a.winner)
		{
			node.observers.active_stopped.notify (hash);
		}

		if (!info_a.confirmed)
		{
			// Clear from publish filter
			node.network.publish_filter.clear (block);
		}
	}
	lock_a.lock ();
}

std::vector<std::shared_ptr<nano::election>> nano::active_transactions::list_active (size_t max_a)
{
	nano::lock_guard<nano::mutex> guard (mutex);
	return list_active_impl (max_a);
}

std::vector<std::shared_ptr<nano::election>> nano::active_transactions::list_active_impl (size_t max_a) const
{
	std::vector<std::shared_ptr<nano::election>> result_l;
	result_l.reserve (std::min (max_a, roots.size ()));
	{
		auto & sorted_roots_l (roots.get<tag_difficulty> ());
		size_t count_l{ 0 };
		for (auto i = sorted_roots_l.begin (), n = sorted_roots_l.end (); i != n && count_l < max_a; ++i, ++count_l)
		{
			result_l.push_back (i->election);
		}
	}
	return result_l;
}

void nano::active_transactions::add_expired_optimistic_election (nano::election const & election_a)
{
	nano::lock_guard<nano::mutex> guard (mutex);
	auto account = election_a.status.winner->account ();
	if (account.is_zero ())
	{
		account = election_a.status.winner->sideband ().account;
	}

	auto it = expired_optimistic_election_infos.get<tag_account> ().find (account);
	if (it != expired_optimistic_election_infos.get<tag_account> ().end ())
	{
		expired_optimistic_election_infos.get<tag_account> ().modify (it, [](auto & expired_optimistic_election) {
			expired_optimistic_election.expired_time = std::chrono::steady_clock::now ();
			expired_optimistic_election.election_started = false;
		});
	}
	else
	{
		expired_optimistic_election_infos.emplace (std::chrono::steady_clock::now (), account);
	}

	// Expire the oldest one if a maximum is reached
	auto const max_expired_optimistic_election_infos = 10000;
	if (expired_optimistic_election_infos.size () > max_expired_optimistic_election_infos)
	{
		expired_optimistic_election_infos.get<tag_expired_time> ().erase (expired_optimistic_election_infos.get<tag_expired_time> ().begin ());
	}
	expired_optimistic_election_infos_size = expired_optimistic_election_infos.size ();
}

unsigned nano::active_transactions::max_optimistic ()
{
	return node.ledger.cache.cemented_count < node.ledger.bootstrap_weight_max_blocks ? std::numeric_limits<unsigned>::max () : 50u;
}

void nano::active_transactions::frontiers_confirmation (nano::unique_lock<nano::mutex> & lock_a)
{
	// Spend some time prioritizing accounts with the most uncemented blocks to reduce voting traffic
	auto request_interval = std::chrono::milliseconds (node.network_params.network.request_interval_ms);
	// Spend longer searching ledger accounts when there is a low amount of elections going on
	auto low_active = roots.size () < 1000;
	auto time_to_spend_prioritizing_ledger_accounts = request_interval / (low_active ? 20 : 100);
	auto time_to_spend_prioritizing_wallet_accounts = request_interval / 250;
	auto time_to_spend_confirming_pessimistic_accounts = time_to_spend_prioritizing_ledger_accounts;
	lock_a.unlock ();
	auto transaction = node.store.tx_begin_read ();
	prioritize_frontiers_for_confirmation (transaction, node.network_params.network.is_dev_network () ? std::chrono::milliseconds (50) : time_to_spend_prioritizing_ledger_accounts, time_to_spend_prioritizing_wallet_accounts);
	auto frontiers_confirmation_info = get_frontiers_confirmation_info ();
	if (frontiers_confirmation_info.can_start_elections ())
	{
		uint64_t elections_count (0);
		confirm_prioritized_frontiers (transaction, frontiers_confirmation_info.max_elections, elections_count);
		confirm_expired_frontiers_pessimistically (transaction, frontiers_confirmation_info.max_elections, elections_count);
		set_next_frontier_check (frontiers_confirmation_info.aggressive_mode);
	}
	lock_a.lock ();
}

/*
 * This function takes the expired_optimistic_election_infos generated from failed elections from frontiers confirmations and starts
 * confirming blocks at cemented height + 1 (cemented frontier successor) for an account only if all dependent blocks already
 * confirmed.
 */
void nano::active_transactions::confirm_expired_frontiers_pessimistically (nano::transaction const & transaction_a, uint64_t max_elections_a, uint64_t & elections_count_a)
{
	auto i{ node.store.accounts_begin (transaction_a, next_frontier_account) };
	auto n{ node.store.accounts_end () };
	nano::timer<std::chrono::milliseconds> timer (nano::timer_state::started);
	nano::confirmation_height_info confirmation_height_info;

	// Loop through any expired optimistic elections which have not been started yet. This tag stores already started ones first
	std::vector<nano::account> elections_started_for_account;
	for (auto i = expired_optimistic_election_infos.get<tag_election_started> ().lower_bound (false); i != expired_optimistic_election_infos.get<tag_election_started> ().end ();)
	{
		if (stopped || elections_count_a >= max_elections_a)
		{
			break;
		}

		auto const & account{ i->account };
		nano::account_info account_info;
		bool should_delete{ true };
		if (!node.store.account_get (transaction_a, account, account_info))
		{
			node.store.confirmation_height_get (transaction_a, account, confirmation_height_info);
			if (account_info.block_count > confirmation_height_info.height)
			{
				should_delete = false;
				std::shared_ptr<nano::block> previous_block;
				std::shared_ptr<nano::block> block;
				if (confirmation_height_info.height == 0)
				{
					block = node.store.block_get (transaction_a, account_info.open_block);
				}
				else
				{
					previous_block = node.store.block_get (transaction_a, confirmation_height_info.frontier);
					block = node.store.block_get (transaction_a, previous_block->sideband ().successor);
				}

				if (block && !node.confirmation_height_processor.is_processing_block (block->hash ()) && node.ledger.dependents_confirmed (transaction_a, *block))
				{
					nano::uint128_t previous_balance{ 0 };
					if (previous_block && previous_block->balance ().is_zero ())
					{
						previous_balance = previous_block->sideband ().balance.number ();
					}

					auto inserted_election = insert_election_from_frontiers_confirmation (block, account, previous_balance, nano::election_behavior::normal);
					if (inserted_election)
					{
						++elections_count_a;
					}
					elections_started_for_account.push_back (i->account);
				}
			}
		}

		if (should_delete)
		{
			// This account is confirmed already or doesn't exist.
			i = expired_optimistic_election_infos.get<tag_election_started> ().erase (i);
			expired_optimistic_election_infos_size = expired_optimistic_election_infos.size ();
		}
		else
		{
			++i;
		}
	}

	for (auto const & account : elections_started_for_account)
	{
		auto it = expired_optimistic_election_infos.get<tag_account> ().find (account);
		debug_assert (it != expired_optimistic_election_infos.get<tag_account> ().end ());
		expired_optimistic_election_infos.get<tag_account> ().modify (it, [](auto & expired_optimistic_election_info_a) {
			expired_optimistic_election_info_a.election_started = true;
		});
	}
}

bool nano::active_transactions::should_do_frontiers_confirmation () const
{
	auto pending_confirmation_height_size (confirmation_height_processor.awaiting_processing_size ());
	auto bootstrap_weight_reached (node.ledger.cache.block_count >= node.ledger.bootstrap_weight_max_blocks);
	auto disabled_confirmation_mode = (node.config.frontiers_confirmation == nano::frontiers_confirmation_mode::disabled);
	auto conf_height_capacity_reached = pending_confirmation_height_size > confirmed_frontiers_max_pending_size;
	auto all_cemented = node.ledger.cache.block_count == node.ledger.cache.cemented_count;
	return (!disabled_confirmation_mode && (bootstrap_weight_reached || node.ledger.pruning) && !conf_height_capacity_reached && !all_cemented);
}

void nano::active_transactions::request_loop ()
{
	nano::unique_lock<nano::mutex> lock (mutex);
	started = true;
	lock.unlock ();
	condition.notify_all ();

	// The wallets and active_transactions objects are mutually dependent, so we need a fully
	// constructed node before proceeding.
	this->node.node_initialized_latch.wait ();

	lock.lock ();

	while (!stopped && !node.flags.disable_request_loop)
	{
		// If many votes are queued, ensure at least the currently active ones finish processing
		lock.unlock ();
		if (node.vote_processor.half_full ())
		{
			node.vote_processor.flush_active ();
		}
		lock.lock ();

		auto const stamp_l = std::chrono::steady_clock::now ();

		// frontiers_confirmation should be above update_active_multiplier to ensure new sorted roots are updated
		if (should_do_frontiers_confirmation ())
		{
			frontiers_confirmation (lock);
		}
		update_active_multiplier (lock);
		request_confirm (lock);

		if (!stopped)
		{
			auto const min_sleep_l = std::chrono::milliseconds (node.network_params.network.request_interval_ms / 2);
			auto const wakeup_l = std::max (stamp_l + std::chrono::milliseconds (node.network_params.network.request_interval_ms), std::chrono::steady_clock::now () + min_sleep_l);
			condition.wait_until (lock, wakeup_l, [&wakeup_l, &stopped = stopped] { return stopped || std::chrono::steady_clock::now () >= wakeup_l; });
		}
	}
}

bool nano::active_transactions::prioritize_account_for_confirmation (nano::active_transactions::prioritize_num_uncemented & cementable_frontiers_a, size_t & cementable_frontiers_size_a, nano::account const & account_a, nano::account_info const & info_a, uint64_t confirmation_height_a)
{
	auto inserted_new{ false };
	if (info_a.block_count > confirmation_height_a && !confirmation_height_processor.is_processing_block (info_a.head))
	{
		auto num_uncemented = info_a.block_count - confirmation_height_a;
		nano::lock_guard<nano::mutex> guard (mutex);
		auto it = cementable_frontiers_a.get<tag_account> ().find (account_a);
		if (it != cementable_frontiers_a.get<tag_account> ().end ())
		{
			if (it->blocks_uncemented != num_uncemented)
			{
				// Account already exists and there is now a different uncemented block count so update it in the container
				cementable_frontiers_a.get<tag_account> ().modify (it, [num_uncemented](nano::cementable_account & info) {
					info.blocks_uncemented = num_uncemented;
				});
			}
		}
		else
		{
			debug_assert (cementable_frontiers_size_a <= max_priority_cementable_frontiers);
			if (cementable_frontiers_size_a == max_priority_cementable_frontiers)
			{
				// The maximum amount of frontiers stored has been reached. Check if the current frontier
				// has more uncemented blocks than the lowest uncemented frontier in the collection if so replace it.
				auto least_uncemented_frontier_it = cementable_frontiers_a.get<tag_uncemented> ().end ();
				--least_uncemented_frontier_it;
				if (num_uncemented > least_uncemented_frontier_it->blocks_uncemented)
				{
					cementable_frontiers_a.get<tag_uncemented> ().erase (least_uncemented_frontier_it);
					cementable_frontiers_a.get<tag_account> ().emplace (account_a, num_uncemented);
				}
			}
			else
			{
				inserted_new = true;
				cementable_frontiers_a.get<tag_account> ().emplace (account_a, num_uncemented);
			}
		}
		cementable_frontiers_size_a = cementable_frontiers_a.size ();
	}
	return inserted_new;
}

void nano::active_transactions::prioritize_frontiers_for_confirmation (nano::transaction const & transaction_a, std::chrono::milliseconds ledger_account_traversal_max_time_a, std::chrono::milliseconds wallet_account_traversal_max_time_a)
{
	// Don't try to prioritize when there are a large number of pending confirmation heights as blocks can be cemented in the meantime, making the prioritization less reliable
	if (confirmation_height_processor.awaiting_processing_size () < confirmed_frontiers_max_pending_size)
	{
		size_t priority_cementable_frontiers_size;
		size_t priority_wallet_cementable_frontiers_size;
		{
			nano::lock_guard<nano::mutex> guard (mutex);
			priority_cementable_frontiers_size = priority_cementable_frontiers.size ();
			priority_wallet_cementable_frontiers_size = priority_wallet_cementable_frontiers.size ();
		}

		nano::timer<std::chrono::milliseconds> wallet_account_timer (nano::timer_state::started);
		// Remove any old expired optimistic elections so they are no longer excluded in subsequent checks
		auto expired_cutoff_it (expired_optimistic_election_infos.get<tag_expired_time> ().lower_bound (std::chrono::steady_clock::now () - expired_optimistic_election_info_cutoff));
		expired_optimistic_election_infos.get<tag_expired_time> ().erase (expired_optimistic_election_infos.get<tag_expired_time> ().begin (), expired_cutoff_it);
		expired_optimistic_election_infos_size = expired_optimistic_election_infos.size ();

		auto num_new_inserted{ 0u };
		auto should_iterate = [this, &num_new_inserted]() {
			auto max_optimistic_l = max_optimistic ();
			return !stopped && (max_optimistic_l > optimistic_elections_count && max_optimistic_l - optimistic_elections_count > num_new_inserted);
		};

		if (!skip_wallets)
		{
			// Prioritize wallet accounts first
			{
				nano::lock_guard<nano::mutex> lock (node.wallets.mutex);
				auto wallet_transaction (node.wallets.tx_begin_read ());
				auto const & items = node.wallets.items;
				if (items.empty ())
				{
					skip_wallets = true;
				}
				for (auto item_it = items.cbegin (); item_it != items.cend () && should_iterate (); ++item_it)
				{
					// Skip this wallet if it has been traversed already while there are others still awaiting
					if (wallet_ids_already_iterated.find (item_it->first) != wallet_ids_already_iterated.end ())
					{
						continue;
					}

					nano::account_info info;
					auto & wallet (item_it->second);
					nano::lock_guard<std::recursive_mutex> wallet_lock (wallet->store.mutex);

					auto & next_wallet_frontier_account = next_wallet_id_accounts.emplace (item_it->first, wallet_store::special_count).first->second;

					auto i (wallet->store.begin (wallet_transaction, next_wallet_frontier_account));
					auto n (wallet->store.end ());
					for (; i != n && should_iterate (); ++i)
					{
						auto const & account (i->first);
						if (expired_optimistic_election_infos.get<tag_account> ().count (account) == 0 && !node.store.account_get (transaction_a, account, info))
						{
							nano::confirmation_height_info confirmation_height_info;
							node.store.confirmation_height_get (transaction_a, account, confirmation_height_info);
							// If it exists in normal priority collection delete from there.
							auto it = priority_cementable_frontiers.find (account);
							if (it != priority_cementable_frontiers.end ())
							{
								nano::lock_guard<nano::mutex> guard (mutex);
								priority_cementable_frontiers.erase (it);
								priority_cementable_frontiers_size = priority_cementable_frontiers.size ();
							}

							auto insert_newed = prioritize_account_for_confirmation (priority_wallet_cementable_frontiers, priority_wallet_cementable_frontiers_size, account, info, confirmation_height_info.height);
							if (insert_newed)
							{
								++num_new_inserted;
							}

							if (wallet_account_timer.since_start () >= wallet_account_traversal_max_time_a)
							{
								break;
							}
						}
						next_wallet_frontier_account = account.number () + 1;
					}
					// Go back to the beginning when we have reached the end of the wallet accounts for this wallet
					if (i == n)
					{
						wallet_ids_already_iterated.emplace (item_it->first);
						next_wallet_id_accounts.at (item_it->first) = wallet_store::special_count;

						// Skip wallet accounts when they have all been traversed
						if (std::next (item_it) == items.cend ())
						{
							wallet_ids_already_iterated.clear ();
							skip_wallets = true;
						}
					}
				}
			}
		}

		nano::timer<std::chrono::milliseconds> timer (nano::timer_state::started);
		auto i (node.store.accounts_begin (transaction_a, next_frontier_account));
		auto n (node.store.accounts_end ());
		for (; i != n && should_iterate (); ++i)
		{
			auto const & account (i->first);
			auto const & info (i->second);
			if (priority_wallet_cementable_frontiers.find (account) == priority_wallet_cementable_frontiers.end ())
			{
				if (expired_optimistic_election_infos.get<tag_account> ().count (account) == 0)
				{
					nano::confirmation_height_info confirmation_height_info;
					node.store.confirmation_height_get (transaction_a, account, confirmation_height_info);
					auto insert_newed = prioritize_account_for_confirmation (priority_cementable_frontiers, priority_cementable_frontiers_size, account, info, confirmation_height_info.height);
					if (insert_newed)
					{
						++num_new_inserted;
					}
				}
			}
			next_frontier_account = account.number () + 1;
			if (timer.since_start () >= ledger_account_traversal_max_time_a)
			{
				break;
			}
		}

		// Go back to the beginning when we have reached the end of the accounts and start with wallet accounts next time
		if (i == n)
		{
			next_frontier_account = 0;
			skip_wallets = false;
		}
	}
}

void nano::active_transactions::stop ()
{
	nano::unique_lock<nano::mutex> lock (mutex);
	if (!started)
	{
		condition.wait (lock, [& started = started] { return started; });
	}
	stopped = true;
	lock.unlock ();
	condition.notify_all ();
	if (thread.joinable ())
	{
		thread.join ();
	}
	generator.stop ();
	lock.lock ();
	roots.clear ();
}

nano::election_insertion_result nano::active_transactions::insert_impl (nano::unique_lock<nano::mutex> & lock_a, std::shared_ptr<nano::block> const & block_a, boost::optional<nano::uint128_t> const & previous_balance_a, nano::election_behavior election_behavior_a, std::function<void(std::shared_ptr<nano::block> const &)> const & confirmation_action_a)
{
	debug_assert (lock_a.owns_lock ());
	debug_assert (block_a->has_sideband ());
	nano::election_insertion_result result;
	if (!stopped)
	{
		auto root (block_a->qualified_root ());
		auto existing (roots.get<tag_root> ().find (root));
		if (existing == roots.get<tag_root> ().end ())
		{
			if (recently_confirmed.get<tag_root> ().find (root) == recently_confirmed.get<tag_root> ().end ())
			{
				result.inserted = true;
				auto hash (block_a->hash ());
				auto epoch (block_a->sideband ().details.epoch);
				nano::uint128_t previous_balance (previous_balance_a.value_or (0));
				debug_assert (!(previous_balance_a.value_or (0) > 0 && block_a->previous ().is_zero ()));
				if (!previous_balance_a.is_initialized () && !block_a->previous ().is_zero ())
				{
					auto transaction (node.store.tx_begin_read ());
					if (node.ledger.block_exists (block_a->previous ()))
					{
						previous_balance = node.ledger.balance (transaction, block_a->previous ());
					}
				}
				double multiplier (normalized_multiplier (*block_a));
				bool prioritized = roots.size () < prioritized_cutoff || multiplier > last_prioritized_multiplier.value_or (0);
				result.election = nano::make_shared<nano::election> (
				node, block_a, confirmation_action_a, [& node = node](auto const & rep_a) {
					// Representative is defined as online if replying to live votes or rep_crawler queries
					node.online_reps.observe (rep_a);
				},
				prioritized, election_behavior_a);
				roots.get<tag_root> ().emplace (nano::active_transactions::conflict_info{ root, multiplier, result.election, epoch, previous_balance });
				blocks.emplace (hash, result.election);
				auto const cache = find_inactive_votes_cache_impl (hash);
				lock_a.unlock ();
				result.election->insert_inactive_votes_cache (cache);
				node.stats.inc (nano::stat::type::election, prioritized ? nano::stat::detail::election_priority : nano::stat::detail::election_non_priority);
			}
		}
		else
		{
			result.election = existing->election;
		}

		if (lock_a.owns_lock ())
		{
			lock_a.unlock ();
		}

		// Votes are generated for inserted or ongoing elections if they're prioritized
		// Non-priority elections generate votes when they gain priority in the future
		if (result.election && result.election->prioritized ())
		{
			result.election->generate_votes ();
		}
	}
	return result;
}

nano::election_insertion_result nano::active_transactions::insert (std::shared_ptr<nano::block> const & block_a, boost::optional<nano::uint128_t> const & previous_balance_a, nano::election_behavior election_behavior_a, std::function<void(std::shared_ptr<nano::block> const &)> const & confirmation_action_a)
{
	nano::unique_lock<nano::mutex> lock (mutex);
	return insert_impl (lock, block_a, previous_balance_a, election_behavior_a, confirmation_action_a);
}

// Validate a vote and apply it to the current election if one exists
nano::vote_code nano::active_transactions::vote (std::shared_ptr<nano::vote> const & vote_a)
{
	nano::vote_code result{ nano::vote_code::indeterminate };
	// If all hashes were recently confirmed then it is a replay
	unsigned recently_confirmed_counter (0);
	std::vector<std::pair<std::shared_ptr<nano::election>, nano::block_hash>> process;
	{
		nano::unique_lock<nano::mutex> lock (mutex);
		for (auto vote_block : vote_a->blocks)
		{
			auto & recently_confirmed_by_hash (recently_confirmed.get<tag_hash> ());
			if (vote_block.which ())
			{
				auto const & block_hash (boost::get<nano::block_hash> (vote_block));
				auto existing (blocks.find (block_hash));
				if (existing != blocks.end ())
				{
					process.emplace_back (existing->second, block_hash);
				}
				else if (recently_confirmed_by_hash.count (block_hash) == 0)
				{
					add_inactive_votes_cache (lock, block_hash, vote_a->account, vote_a->timestamp);
				}
				else
				{
					++recently_confirmed_counter;
				}
			}
			else
			{
				auto block (boost::get<std::shared_ptr<nano::block>> (vote_block));
				auto existing (roots.get<tag_root> ().find (block->qualified_root ()));
				if (existing != roots.get<tag_root> ().end ())
				{
					process.emplace_back (existing->election, block->hash ());
				}
				else if (recently_confirmed_by_hash.count (block->hash ()) == 0)
				{
					add_inactive_votes_cache (lock, block->hash (), vote_a->account, vote_a->timestamp);
				}
				else
				{
					++recently_confirmed_counter;
				}
			}
		}
	}

	if (!process.empty ())
	{
		bool replay (false);
		bool processed (false);
		for (auto const & [election, block_hash] : process)
		{
			auto const result_l = election->vote (vote_a->account, vote_a->timestamp, block_hash);
			processed = processed || result_l.processed;
			replay = replay || result_l.replay;
		}

		// Republish vote if it is new and the node does not host a principal representative (or close to)
		if (processed)
		{
			auto const reps (node.wallets.reps ());
			if (!reps.have_half_rep () && !reps.exists (vote_a->account))
			{
				node.network.flood_vote (vote_a, 0.5f);
			}
		}
		result = replay ? nano::vote_code::replay : nano::vote_code::vote;
	}
	else if (recently_confirmed_counter == vote_a->blocks.size ())
	{
		result = nano::vote_code::replay;
	}
	return result;
}

bool nano::active_transactions::active (nano::qualified_root const & root_a)
{
	nano::lock_guard<nano::mutex> lock (mutex);
	return roots.get<tag_root> ().find (root_a) != roots.get<tag_root> ().end ();
}

bool nano::active_transactions::active (nano::block const & block_a)
{
	nano::lock_guard<nano::mutex> guard (mutex);
	return roots.get<tag_root> ().find (block_a.qualified_root ()) != roots.get<tag_root> ().end () && blocks.find (block_a.hash ()) != blocks.end ();
}

std::shared_ptr<nano::election> nano::active_transactions::election (nano::qualified_root const & root_a) const
{
	std::shared_ptr<nano::election> result;
	nano::lock_guard<nano::mutex> lock (mutex);
	auto existing = roots.get<tag_root> ().find (root_a);
	if (existing != roots.get<tag_root> ().end ())
	{
		result = existing->election;
	}
	return result;
}

std::shared_ptr<nano::block> nano::active_transactions::winner (nano::block_hash const & hash_a) const
{
	std::shared_ptr<nano::block> result;
	nano::unique_lock<nano::mutex> lock (mutex);
	auto existing = blocks.find (hash_a);
	if (existing != blocks.end ())
	{
		auto election = existing->second;
		lock.unlock ();
		result = election->winner ();
	}
	return result;
}

nano::election_insertion_result nano::active_transactions::activate (nano::account const & account_a)
{
	nano::election_insertion_result result;
	auto transaction (node.store.tx_begin_read ());
	nano::account_info account_info;
	if (!node.store.account_get (transaction, account_a, account_info))
	{
		nano::confirmation_height_info conf_info;
		node.store.confirmation_height_get (transaction, account_a, conf_info);
		if (conf_info.height < account_info.block_count)
		{
			debug_assert (conf_info.frontier != account_info.head);
			auto hash = conf_info.height == 0 ? account_info.open_block : node.store.block_successor (transaction, conf_info.frontier);
			auto block = node.store.block_get (transaction, hash);
			release_assert (block != nullptr);
			if (node.ledger.dependents_confirmed (transaction, *block))
			{
				result = insert (block);
				if (result.inserted)
				{
					result.election->transition_active ();
				}
			}
		}
	}
	return result;
}

bool nano::active_transactions::update_difficulty (std::shared_ptr<nano::block> const & block_a, bool flood_update)
{
	nano::unique_lock<nano::mutex> lock (mutex);
	auto existing_election (roots.get<tag_root> ().find (block_a->qualified_root ()));
	bool error = existing_election == roots.get<tag_root> ().end () || update_difficulty_impl (existing_election, *block_a);
	// Update election and flood block
	if (!error && flood_update)
	{
		lock.unlock ();
		existing_election->election->publish (block_a);
	}
	return error;
}

bool nano::active_transactions::update_difficulty_impl (nano::active_transactions::roots_iterator const & root_it_a, nano::block const & block_a)
{
	debug_assert (!mutex.try_lock ());
	double multiplier (normalized_multiplier (block_a, root_it_a));
	bool error = multiplier <= root_it_a->multiplier;
	if (!error)
	{
		if (node.config.logging.active_update_logging ())
		{
			node.logger.try_log (boost::str (boost::format ("Election %1% difficulty updated with block %2% from multiplier %3% to %4%") % root_it_a->root.to_string () % block_a.hash ().to_string () % root_it_a->multiplier % multiplier));
		}
		roots.get<tag_root> ().modify (root_it_a, [multiplier](nano::active_transactions::conflict_info & info_a) {
			info_a.multiplier = multiplier;
		});
		node.stats.inc (nano::stat::type::election, nano::stat::detail::election_difficulty_update);
	}
	return error;
}

bool nano::active_transactions::restart (nano::transaction const & transaction_a, std::shared_ptr<nano::block> const & block_a)
{
	bool error = true;
	auto hash (block_a->hash ());
	auto ledger_block (node.store.block_get (transaction_a, hash));
	if (ledger_block != nullptr && ledger_block->block_work () != block_a->block_work () && !node.block_confirmed_or_being_confirmed (transaction_a, hash))
	{
		if (block_a->difficulty () > ledger_block->difficulty ())
		{
			// Re-writing the block is necessary to avoid the same work being received later to force restarting the election
			// The existing block is re-written, not the arriving block, as that one might not have gone through a full signature check
			ledger_block->block_work_set (block_a->block_work ());

			// Deferred write
			node.block_processor.update (ledger_block);

			// Restart election for the upgraded block, previously dropped from elections
			if (node.ledger.dependents_confirmed (transaction_a, *ledger_block))
			{
				auto previous_balance = node.ledger.balance (transaction_a, ledger_block->previous ());
				auto insert_result = insert (ledger_block, previous_balance);
				if (insert_result.inserted)
				{
					error = false;
					node.stats.inc (nano::stat::type::election, nano::stat::detail::election_restart);
				}
			}
		}
	}
	return error;
}

double nano::active_transactions::normalized_multiplier (nano::block const & block_a, boost::optional<nano::active_transactions::roots_iterator> const & root_it_a) const
{
	debug_assert (!mutex.try_lock ());
	auto difficulty (block_a.difficulty ());
	uint64_t threshold (0);
	bool sideband_not_found (false);
	if (block_a.has_sideband ())
	{
		threshold = nano::work_threshold (block_a.work_version (), block_a.sideband ().details);
	}
	else if (root_it_a.is_initialized ())
	{
		auto election (*root_it_a);
		debug_assert (election != roots.end ());
		// This is one of few places where both the active mutex and election mutexes are held
		if (auto election_block = election->election->find (block_a.hash ()); election_block && election_block->has_sideband ())
		{
			threshold = nano::work_threshold (block_a.work_version (), election_block->sideband ().details);
		}
		else
		{
			// This can have incorrect results during an epoch upgrade, but it only affects prioritization
			bool is_send = election->previous_balance > block_a.balance ().number ();
			bool is_receive = election->previous_balance < block_a.balance ().number ();
			nano::block_details details (election->epoch, is_send, is_receive, false);

			threshold = nano::work_threshold (block_a.work_version (), details);
			sideband_not_found = true;
		}
	}
	double multiplier (nano::difficulty::to_multiplier (difficulty, threshold));
	debug_assert (multiplier >= 1 || sideband_not_found);
	if (multiplier >= 1)
	{
		multiplier = nano::normalized_multiplier (multiplier, threshold);
	}
	else
	{
		// Inferred threshold was incorrect
		multiplier = 1;
	}
	return multiplier;
}

void nano::active_transactions::update_active_multiplier (nano::unique_lock<nano::mutex> & lock_a)
{
	debug_assert (!mutex.try_lock ());
	last_prioritized_multiplier.reset ();
	double multiplier (1.);
	// Heurestic to filter out non-saturated network and frontier confirmation
	if (roots.size () >= prioritized_cutoff || (node.network_params.network.is_dev_network () && !roots.empty ()))
	{
		auto & sorted_roots = roots.get<tag_difficulty> ();
		std::vector<double> prioritized;
		prioritized.reserve (std::min (sorted_roots.size (), prioritized_cutoff));
		for (auto it (sorted_roots.begin ()), end (sorted_roots.end ()); it != end && prioritized.size () < prioritized_cutoff; ++it)
		{
			if (!it->election->confirmed ())
			{
				prioritized.push_back (it->multiplier);
			}
		}
		if (prioritized.size () > 10 || (node.network_params.network.is_dev_network () && !prioritized.empty ()))
		{
			multiplier = prioritized[prioritized.size () / 2];
		}
		if (!prioritized.empty ())
		{
			last_prioritized_multiplier = prioritized.back ();
		}
	}
	debug_assert (multiplier >= nano::difficulty::to_multiplier (node.network_params.network.publish_thresholds.entry, node.network_params.network.publish_thresholds.base));
	multipliers_cb.push_front (multiplier);
	auto sum (std::accumulate (multipliers_cb.begin (), multipliers_cb.end (), double(0)));
	double avg_multiplier (sum / multipliers_cb.size ());
	auto difficulty = nano::difficulty::from_multiplier (avg_multiplier, node.default_difficulty (nano::work_version::work_1));
	debug_assert (difficulty >= node.network_params.network.publish_thresholds.entry);

	trended_active_multiplier = avg_multiplier;
	lock_a.unlock ();
	node.observers.difficulty.notify (difficulty);
	lock_a.lock ();
}

uint64_t nano::active_transactions::active_difficulty ()
{
	return nano::difficulty::from_multiplier (active_multiplier (), node.default_difficulty (nano::work_version::work_1));
}

uint64_t nano::active_transactions::limited_active_difficulty (nano::block const & block_a)
{
	uint64_t threshold (0);
	if (block_a.has_sideband ())
	{
		threshold = nano::work_threshold (block_a.work_version (), block_a.sideband ().details);
	}
	else
	{
		threshold = node.default_difficulty (block_a.work_version ());
	}
	return limited_active_difficulty (block_a.work_version (), threshold);
}

uint64_t nano::active_transactions::limited_active_difficulty (nano::work_version const version_a, uint64_t const threshold_a)
{
	auto difficulty (nano::difficulty::from_multiplier (nano::denormalized_multiplier (active_multiplier (), threshold_a), threshold_a));
	return std::min (difficulty, node.max_work_generate_difficulty (version_a));
}

double nano::active_transactions::active_multiplier ()
{
	return trended_active_multiplier.load ();
}

std::deque<nano::election_status> nano::active_transactions::list_recently_cemented ()
{
	nano::lock_guard<nano::mutex> lock (mutex);
	return recently_cemented;
}

void nano::active_transactions::add_recently_cemented (nano::election_status const & status_a)
{
	nano::lock_guard<nano::mutex> guard (mutex);
	recently_cemented.push_back (status_a);
	if (recently_cemented.size () > node.config.confirmation_history_size)
	{
		recently_cemented.pop_front ();
	}
}

void nano::active_transactions::add_recently_confirmed (nano::qualified_root const & root_a, nano::block_hash const & hash_a)
{
	nano::lock_guard<nano::mutex> guard (mutex);
	recently_confirmed.get<tag_sequence> ().emplace_back (root_a, hash_a);
	if (recently_confirmed.size () > recently_confirmed_size)
	{
		recently_confirmed.get<tag_sequence> ().pop_front ();
	}
}

void nano::active_transactions::erase_recently_confirmed (nano::block_hash const & hash_a)
{
	nano::lock_guard<nano::mutex> guard (mutex);
	recently_confirmed.get<tag_hash> ().erase (hash_a);
}

void nano::active_transactions::erase (nano::block const & block_a)
{
	nano::unique_lock<nano::mutex> lock (mutex);
	auto root_it (roots.get<tag_root> ().find (block_a.qualified_root ()));
	if (root_it != roots.get<tag_root> ().end ())
	{
		// This is one of few places where both the active mutex and election mutexes are held
		cleanup_election (lock, root_it->election->cleanup_info ());
		roots.get<tag_root> ().erase (root_it);
		lock.unlock ();
		node.logger.try_log (boost::str (boost::format ("Election erased for block block %1% root %2%") % block_a.hash ().to_string () % block_a.root ().to_string ()));
	}
}

void nano::active_transactions::erase (nano::qualified_root const & root_a)
{
	nano::unique_lock<nano::mutex> lock (mutex);
	auto root_it (roots.get<tag_root> ().find (root_a));
	if (root_it != roots.get<tag_root> ().end ())
	{
		// This is one of few places where both the active mutex and election mutexes are held
		cleanup_election (lock, root_it->election->cleanup_info ());
		roots.get<tag_root> ().erase (root_it);
	}
}

void nano::active_transactions::erase_hash (nano::block_hash const & hash_a)
{
	nano::unique_lock<nano::mutex> lock (mutex);
	[[maybe_unused]] auto erased (blocks.erase (hash_a));
	debug_assert (erased == 1);
}

bool nano::active_transactions::empty ()
{
	nano::lock_guard<nano::mutex> lock (mutex);
	return roots.empty ();
}

size_t nano::active_transactions::size ()
{
	nano::lock_guard<nano::mutex> lock (mutex);
	return roots.size ();
}

bool nano::active_transactions::publish (std::shared_ptr<nano::block> const & block_a)
{
	nano::unique_lock<nano::mutex> lock (mutex);
	auto existing (roots.get<tag_root> ().find (block_a->qualified_root ()));
	auto result (true);
	if (existing != roots.get<tag_root> ().end ())
	{
		update_difficulty_impl (existing, *block_a);
		auto election (existing->election);
		lock.unlock ();
		result = election->publish (block_a);
		if (!result)
		{
			lock.lock ();
			blocks.emplace (block_a->hash (), election);
			auto const cache = find_inactive_votes_cache_impl (block_a->hash ());
			lock.unlock ();
			election->insert_inactive_votes_cache (cache);
			node.stats.inc (nano::stat::type::election, nano::stat::detail::election_block_conflict);
		}
	}
	return result;
}

// Returns the type of election status requiring callbacks calling later
boost::optional<nano::election_status_type> nano::active_transactions::confirm_block (nano::transaction const & transaction_a, std::shared_ptr<nano::block> const & block_a)
{
	auto hash (block_a->hash ());
	nano::unique_lock<nano::mutex> lock (mutex);
	auto existing (blocks.find (hash));
	boost::optional<nano::election_status_type> status_type;
	if (existing != blocks.end ())
	{
		lock.unlock ();
		nano::unique_lock<nano::mutex> election_lock (existing->second->mutex);
		if (existing->second->status.winner && existing->second->status.winner->hash () == hash)
		{
			if (!existing->second->confirmed ())
			{
				existing->second->confirm_once (election_lock, nano::election_status_type::active_confirmation_height);
				status_type = nano::election_status_type::active_confirmation_height;
			}
			else
			{
#ifndef NDEBUG
				nano::unique_lock<nano::mutex> election_winners_lk (election_winner_details_mutex);
				debug_assert (election_winner_details.find (hash) != election_winner_details.cend ());
#endif
				status_type = nano::election_status_type::active_confirmed_quorum;
			}
		}
		else
		{
			status_type = boost::optional<nano::election_status_type>{};
		}
	}
	else
	{
		status_type = nano::election_status_type::inactive_confirmation_height;
	}

	return status_type;
}

size_t nano::active_transactions::priority_cementable_frontiers_size ()
{
	nano::lock_guard<nano::mutex> guard (mutex);
	return priority_cementable_frontiers.size ();
}

size_t nano::active_transactions::priority_wallet_cementable_frontiers_size ()
{
	nano::lock_guard<nano::mutex> guard (mutex);
	return priority_wallet_cementable_frontiers.size ();
}

boost::circular_buffer<double> nano::active_transactions::difficulty_trend ()
{
	nano::lock_guard<nano::mutex> guard (mutex);
	return multipliers_cb;
}

size_t nano::active_transactions::inactive_votes_cache_size ()
{
	nano::lock_guard<nano::mutex> guard (mutex);
	return inactive_votes_cache.size ();
}

void nano::active_transactions::add_inactive_votes_cache (nano::unique_lock<nano::mutex> & lock_a, nano::block_hash const & hash_a, nano::account const & representative_a, uint64_t const timestamp_a)
{
	// Check principal representative status
	if (node.ledger.weight (representative_a) > node.minimum_principal_weight ())
	{
		/** It is important that the new vote is added to the cache before calling inactive_votes_bootstrap_check
		 * This guarantees consistency when a vote is received while also receiving the corresponding block
		 */
		auto & inactive_by_hash (inactive_votes_cache.get<tag_hash> ());
		auto existing (inactive_by_hash.find (hash_a));
		if (existing != inactive_by_hash.end ())
		{
			if (existing->needs_eval ())
			{
				auto is_new (false);
				inactive_by_hash.modify (existing, [representative_a, timestamp_a, &is_new](nano::inactive_cache_information & info) {
					auto it = std::find_if (info.voters.begin (), info.voters.end (), [&representative_a](auto const & item_a) { return item_a.first == representative_a; });
					is_new = (it == info.voters.end ());
					if (is_new)
					{
						info.arrival = std::chrono::steady_clock::now ();
						info.voters.emplace_back (representative_a, timestamp_a);
					}
				});

				if (is_new)
				{
					auto const old_status = existing->status;
					auto const status = inactive_votes_bootstrap_check (lock_a, existing->voters, hash_a, existing->status);
					if (status != old_status)
					{
						// The lock has since been released
						existing = inactive_by_hash.find (hash_a);
						if (existing != inactive_by_hash.end ())
						{
							inactive_by_hash.modify (existing, [status](nano::inactive_cache_information & info) {
								info.status = status;
							});
						}
					}
				}
			}
		}
		else
		{
			auto & inactive_by_arrival (inactive_votes_cache.get<tag_arrival> ());
			nano::inactive_cache_status default_status{};
			inactive_by_arrival.emplace (nano::inactive_cache_information{ std::chrono::steady_clock::now (), hash_a, representative_a, timestamp_a, default_status });
			auto const status (inactive_votes_bootstrap_check (lock_a, representative_a, hash_a, default_status));
			if (status != default_status)
			{
				// The lock has since been released
				existing = inactive_by_hash.find (hash_a);
				if (existing != inactive_by_hash.end ())
				{
					inactive_by_hash.modify (existing, [status](nano::inactive_cache_information & info) {
						info.status = status;
					});
				}
			}
			if (inactive_votes_cache.size () > node.flags.inactive_votes_cache_size)
			{
				inactive_by_arrival.erase (inactive_by_arrival.begin ());
			}
		}
	}
}

void nano::active_transactions::trigger_inactive_votes_cache_election (std::shared_ptr<nano::block> const & block_a)
{
	nano::unique_lock<nano::mutex> lock (mutex);
	auto const status = find_inactive_votes_cache_impl (block_a->hash ()).status;
	if (status.election_started)
	{
		insert_impl (lock, block_a);
	}
}

nano::inactive_cache_information nano::active_transactions::find_inactive_votes_cache (nano::block_hash const & hash_a)
{
	nano::lock_guard<nano::mutex> guard (mutex);
	return find_inactive_votes_cache_impl (hash_a);
}

nano::inactive_cache_information nano::active_transactions::find_inactive_votes_cache_impl (nano::block_hash const & hash_a)
{
	auto & inactive_by_hash (inactive_votes_cache.get<tag_hash> ());
	auto existing (inactive_by_hash.find (hash_a));
	if (existing != inactive_by_hash.end ())
	{
		return *existing;
	}
	else
	{
		return nano::inactive_cache_information{};
	}
}

void nano::active_transactions::erase_inactive_votes_cache (nano::block_hash const & hash_a)
{
	inactive_votes_cache.get<tag_hash> ().erase (hash_a);
}

nano::inactive_cache_status nano::active_transactions::inactive_votes_bootstrap_check (nano::unique_lock<nano::mutex> & lock_a, nano::account const & voter_a, nano::block_hash const & hash_a, nano::inactive_cache_status const & previously_a)
{
	debug_assert (lock_a.owns_lock ());
	lock_a.unlock ();
	return inactive_votes_bootstrap_check_impl (lock_a, node.ledger.weight (voter_a), 1, hash_a, previously_a);
}

nano::inactive_cache_status nano::active_transactions::inactive_votes_bootstrap_check (nano::unique_lock<nano::mutex> & lock_a, std::vector<std::pair<nano::account, uint64_t>> const & voters_a, nano::block_hash const & hash_a, nano::inactive_cache_status const & previously_a)
{
	/** Perform checks on accumulated tally from inactive votes
	 * These votes are generally either for unconfirmed blocks or old confirmed blocks
	 * That check is made after hitting a tally threshold, and always as late and as few times as possible
	 */
	debug_assert (lock_a.owns_lock ());
	lock_a.unlock ();

	nano::uint128_t tally;
	for (auto const & [voter, timestamp] : voters_a)
	{
		tally += node.ledger.weight (voter);
	}

	return inactive_votes_bootstrap_check_impl (lock_a, tally, voters_a.size (), hash_a, previously_a);
}

nano::inactive_cache_status nano::active_transactions::inactive_votes_bootstrap_check_impl (nano::unique_lock<nano::mutex> & lock_a, nano::uint128_t const & tally_a, size_t voters_size_a, nano::block_hash const & hash_a, nano::inactive_cache_status const & previously_a)
{
	debug_assert (!lock_a.owns_lock ());
	nano::inactive_cache_status status (previously_a);
	constexpr unsigned election_start_voters_min{ 5 };
	status.tally = tally_a;
	if (!previously_a.confirmed && tally_a >= node.online_reps.delta ())
	{
		status.bootstrap_started = true;
		status.confirmed = true;
	}
	else if (!previously_a.bootstrap_started && !node.flags.disable_legacy_bootstrap && node.flags.disable_lazy_bootstrap && tally_a > node.gap_cache.bootstrap_threshold ())
	{
		status.bootstrap_started = true;
	}
	if (!previously_a.election_started && voters_size_a >= election_start_voters_min && tally_a >= (node.online_reps.trended () / 100) * node.config.election_hint_weight_percent)
	{
		status.election_started = true;
	}

	if ((status.election_started && !previously_a.election_started) || (status.bootstrap_started && !previously_a.bootstrap_started))
	{
		auto transaction (node.store.tx_begin_read ());
		auto block = node.store.block_get (transaction, hash_a);
		if (block && status.election_started && !previously_a.election_started && !node.block_confirmed_or_being_confirmed (transaction, hash_a))
		{
			if (node.ledger.cache.cemented_count >= node.ledger.bootstrap_weight_max_blocks)
			{
				lock_a.lock ();
				insert_impl (lock_a, block);
			}
		}
		else if (!block && status.bootstrap_started && !previously_a.bootstrap_started && (!node.ledger.pruning || !node.store.pruned_exists (transaction, hash_a)))
		{
			node.gap_cache.bootstrap_start (hash_a);
		}
	}

	if (!lock_a.owns_lock ())
	{
		lock_a.lock ();
	}

	return status;
}

bool nano::purge_singleton_inactive_votes_cache_pool_memory ()
{
	return boost::singleton_pool<boost::fast_pool_allocator_tag, sizeof (nano::active_transactions::ordered_cache::node_type)>::purge_memory ();
}

size_t nano::active_transactions::election_winner_details_size ()
{
	nano::lock_guard<nano::mutex> guard (election_winner_details_mutex);
	return election_winner_details.size ();
}

nano::cementable_account::cementable_account (nano::account const & account_a, size_t blocks_uncemented_a) :
account (account_a), blocks_uncemented (blocks_uncemented_a)
{
}

nano::expired_optimistic_election_info::expired_optimistic_election_info (std::chrono::steady_clock::time_point expired_time_a, nano::account account_a) :
expired_time (expired_time_a),
account (account_a)
{
}

bool nano::frontiers_confirmation_info::can_start_elections () const
{
	return max_elections > 0;
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (active_transactions & active_transactions, std::string const & name)
{
	size_t roots_count;
	size_t blocks_count;
	size_t recently_confirmed_count;
	size_t recently_cemented_count;

	{
		nano::lock_guard<nano::mutex> guard (active_transactions.mutex);
		roots_count = active_transactions.roots.size ();
		blocks_count = active_transactions.blocks.size ();
		recently_confirmed_count = active_transactions.recently_confirmed.size ();
		recently_cemented_count = active_transactions.recently_cemented.size ();
	}

	auto composite = std::make_unique<container_info_composite> (name);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "roots", roots_count, sizeof (decltype (active_transactions.roots)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "blocks", blocks_count, sizeof (decltype (active_transactions.blocks)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "election_winner_details", active_transactions.election_winner_details_size (), sizeof (decltype (active_transactions.election_winner_details)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "recently_confirmed", recently_confirmed_count, sizeof (decltype (active_transactions.recently_confirmed)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "recently_cemented", recently_cemented_count, sizeof (decltype (active_transactions.recently_cemented)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "priority_wallet_cementable_frontiers", active_transactions.priority_wallet_cementable_frontiers_size (), sizeof (nano::cementable_account) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "priority_cementable_frontiers", active_transactions.priority_cementable_frontiers_size (), sizeof (nano::cementable_account) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "expired_optimistic_election_infos", active_transactions.expired_optimistic_election_infos_size, sizeof (decltype (active_transactions.expired_optimistic_election_infos)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "inactive_votes_cache", active_transactions.inactive_votes_cache_size (), sizeof (nano::gap_information) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "optimistic_elections_count", active_transactions.optimistic_elections_count, 0 })); // This isn't an extra container, is just to expose the count easily
	composite->add_component (collect_container_info (active_transactions.generator, "generator"));
	return composite;
}
