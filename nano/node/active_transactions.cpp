#include <nano/lib/threading.hpp>
#include <nano/node/active_transactions.hpp>
#include <nano/node/election.hpp>
#include <nano/node/node.hpp>

#include <boost/format.hpp>
#include <boost/variant/get.hpp>

#include <numeric>

using namespace std::chrono;

nano::active_transactions::active_transactions (nano::node & node_a)
	: node (node_a)
	, long_election_threshold (node.network_params.network.is_test_network () ? 2s : 24s)
	, election_request_delay (node.network_params.network.is_test_network () ? 0s : 1s)
	, election_time_to_live (node.network_params.network.is_test_network () ? 0s : 10s)
	, multipliers_cb (20, 1.)
	, trended_active_difficulty (node.network_params.network.publish_threshold)
	, next_frontier_check (steady_clock::now ())
	, thread ([this]() {
		nano::thread_role::set (nano::thread_role::name::request_loop);
		request_loop ();
	})
{
	nano::unique_lock<std::mutex> lock (mutex);
	condition.wait (lock, [& started = started] { return started; });
}

nano::active_transactions::~active_transactions ()
{
	stop ();
}

void nano::active_transactions::search_frontiers (nano::transaction const & transaction_a)
{
	// Limit maximum count of elections to start
	bool representative (node.config.enable_voting && node.wallets.reps_count > 0);
	bool half_princpal_representative (representative && node.wallets.half_principal_reps_count > 0);
	/* Check less frequently for regular nodes in auto mode */
	bool agressive_mode (half_princpal_representative || node.config.frontiers_confirmation == nano::frontiers_confirmation_mode::always);
	auto request_interval (std::chrono::milliseconds (node.network_params.network.request_interval_ms));
	auto agressive_factor = request_interval * (agressive_mode ? 20 : 100);
	// Decrease check time for test network
	auto is_test_network = node.network_params.network.is_test_network ();
	int test_network_factor = is_test_network ? 1000 : 1;
	auto roots_size = size ();
	nano::unique_lock<std::mutex> lk (mutex);
	auto check_time_exceeded = std::chrono::steady_clock::now () >= next_frontier_check;
	lk.unlock ();
	auto max_elections = (node.config.active_elections_size / 20);
	auto low_active_elections = roots_size < max_elections;
	bool wallets_check_required = (!skip_wallets || !priority_wallet_cementable_frontiers.empty ()) && !agressive_mode;
	// Minimise dropping real-time transactions, set the number of frontiers added to a factor of the total number of active elections
	auto max_active = node.config.active_elections_size / 5;
	if (roots_size <= max_active && (check_time_exceeded || wallets_check_required || (!is_test_network && low_active_elections && agressive_mode)))
	{
		// When the number of active elections is low increase max number of elections for setting confirmation height.
		if (max_active > roots_size + max_elections)
		{
			max_elections = max_active - roots_size;
		}

		// Spend time prioritizing accounts to reduce voting traffic
		auto time_spent_prioritizing_ledger_accounts = request_interval / 10;
		auto time_spent_prioritizing_wallet_accounts = request_interval / 25;
		prioritize_frontiers_for_confirmation (transaction_a, is_test_network ? std::chrono::milliseconds (50) : time_spent_prioritizing_ledger_accounts, time_spent_prioritizing_wallet_accounts);

		size_t elections_count (0);
		lk.lock ();
		auto start_elections_for_prioritized_frontiers = [&transaction_a, &elections_count, max_elections, &lk, &representative, this](prioritize_num_uncemented & cementable_frontiers) {
			while (!cementable_frontiers.empty () && !this->stopped && elections_count < max_elections)
			{
				auto cementable_account_front_it = cementable_frontiers.get<1> ().begin ();
				auto cementable_account = *cementable_account_front_it;
				cementable_frontiers.get<1> ().erase (cementable_account_front_it);
				lk.unlock ();
				nano::account_info info;
				auto error = node.store.account_get (transaction_a, cementable_account.account, info);
				if (!error)
				{
					uint64_t confirmation_height;
					error = node.store.confirmation_height_get (transaction_a, cementable_account.account, confirmation_height);
					release_assert (!error);

					if (info.block_count > confirmation_height && !this->node.pending_confirmation_height.is_processing_block (info.head))
					{
						auto block (this->node.store.block_get (transaction_a, info.head));
						if (!this->start (block, true))
						{
							++elections_count;
							// Calculate votes for local representatives
							if (representative)
							{
								this->node.block_processor.generator.add (block->hash ());
							}
						}
					}
				}
				lk.lock ();
			}
		};
		start_elections_for_prioritized_frontiers (priority_cementable_frontiers);
		start_elections_for_prioritized_frontiers (priority_wallet_cementable_frontiers);
		next_frontier_check = steady_clock::now () + (agressive_factor / test_network_factor);
	}
}
void nano::active_transactions::post_confirmation_height_set (nano::transaction const & transaction_a, std::shared_ptr<nano::block> block_a, nano::block_sideband const & sideband_a, nano::election_status_type election_status_type_a)
{
	if (election_status_type_a == nano::election_status_type::inactive_confirmation_height)
	{
		nano::account account (0);
		nano::uint128_t amount (0);
		bool is_state_send (false);
		nano::account pending_account (0);
		node.process_confirmed_data (transaction_a, block_a, block_a->hash (), sideband_a, account, amount, is_state_send, pending_account);
		node.observers.blocks.notify (nano::election_status{ block_a, 0, std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now ().time_since_epoch ()), std::chrono::duration_values<std::chrono::milliseconds>::zero (), 0, 1, 0, nano::election_status_type::inactive_confirmation_height }, account, amount, is_state_send);
	}
	else
	{
		auto hash (block_a->hash ());
		nano::lock_guard<std::mutex> lock (mutex);
		auto existing (pending_conf_height.find (hash));
		if (existing != pending_conf_height.end ())
		{
			auto election = existing->second;
			if (election->confirmed && !election->stopped && election->status.winner->hash () == hash)
			{
				add_confirmed (existing->second->status, block_a->qualified_root ());

				node.receive_confirmed (transaction_a, block_a, hash);
				nano::account account (0);
				nano::uint128_t amount (0);
				bool is_state_send (false);
				nano::account pending_account (0);
				node.process_confirmed_data (transaction_a, block_a, hash, sideband_a, account, amount, is_state_send, pending_account);
				election->status.type = election_status_type_a;
				election->status.confirmation_request_count = election->confirmation_request_count;
				node.observers.blocks.notify (election->status, account, amount, is_state_send);
				if (amount > 0)
				{
					node.observers.account_balance.notify (account, false);
					if (!pending_account.is_zero ())
					{
						node.observers.account_balance.notify (pending_account, true);
					}
				}
			}

			pending_conf_height.erase (hash);
		}
	}
}

void nano::active_transactions::election_escalate (std::shared_ptr<nano::election> & election_l, nano::transaction const & transaction_l, size_t const & roots_size_l)
{
	static unsigned constexpr high_confirmation_request_count{ 128 };
	// Log votes for very long unconfirmed elections
	if (election_l->confirmation_request_count % (4 * high_confirmation_request_count) == 1)
	{
		auto tally_l (election_l->tally ());
		election_l->log_votes (tally_l);
	}
	/*
	 * Escalation for long unconfirmed elections
	 * Start new elections for previous block & source if there are less than 100 active elections
	 */
	if (election_l->confirmation_request_count % high_confirmation_request_count == 1 && roots_size_l < 100 && !node.network_params.network.is_test_network ())
	{
		bool escalated_l (false);
		std::shared_ptr<nano::block> previous_l;
		auto previous_hash_l (election_l->status.winner->previous ());
		if (!previous_hash_l.is_zero ())
		{
			previous_l = node.store.block_get (transaction_l, previous_hash_l);
			if (previous_l != nullptr && blocks.find (previous_hash_l) == blocks.end () && !node.block_confirmed_or_being_confirmed (transaction_l, previous_hash_l))
			{
				add (std::move (previous_l), true);
				escalated_l = true;
			}
		}
		/* If previous block not existing/not commited yet, block_source can cause segfault for state blocks
					So source check can be done only if previous != nullptr or previous is 0 (open account) */
		if (previous_hash_l.is_zero () || previous_l != nullptr)
		{
			auto source_hash_l (node.ledger.block_source (transaction_l, *election_l->status.winner));
			if (!source_hash_l.is_zero () && source_hash_l != previous_hash_l && blocks.find (source_hash_l) == blocks.end ())
			{
				auto source_l (node.store.block_get (transaction_l, source_hash_l));
				if (source_l != nullptr && !node.block_confirmed_or_being_confirmed (transaction_l, source_hash_l))
				{
					add (std::move (source_l), true);
					escalated_l = true;
				}
			}
		}
		if (escalated_l)
		{
			election_l->update_dependent ();
		}
	}
}

void nano::active_transactions::election_broadcast (std::shared_ptr<nano::election> & election_l, nano::transaction const & transaction_l, std::deque<std::shared_ptr<nano::block>> & blocks_bundle_l, std::unordered_set<nano::qualified_root> & inactive_l, nano::qualified_root & root_l)
{
	if (node.ledger.could_fit (transaction_l, *election_l->status.winner))
	{
		// Broadcast current winner
		if (blocks_bundle_l.size () < max_block_broadcasts)
		{
			blocks_bundle_l.push_back (election_l->status.winner);
		}
	}
	else if (election_l->confirmation_request_count != 0)
	{
		election_l->stop ();
		inactive_l.insert (root_l);
	}
}

bool nano::active_transactions::election_request_confirm (std::shared_ptr<nano::election> & election_l, std::vector<nano::representative> const & representatives_l, size_t const & roots_size_l,
	std::deque<std::pair<std::shared_ptr<nano::block>, std::shared_ptr<std::vector<std::shared_ptr<nano::transport::channel>>>>> & single_confirm_req_bundle_l,
	std::unordered_map<std::shared_ptr<nano::transport::channel>, std::deque<std::pair<nano::block_hash, nano::root>>> & batched_confirm_req_bundle_l)
{
	bool inserted_into_any_bundle{ false };
	std::vector<std::shared_ptr<nano::transport::channel>> rep_channels_missing_vote_l;
	// Add all rep endpoints that haven't already voted
	for (const auto & rep : representatives_l)
	{
		if (election_l->last_votes.find (rep.account) == election_l->last_votes.end ())
		{
			rep_channels_missing_vote_l.push_back (rep.channel);

			if (node.config.logging.vote_logging () && election_l->confirmation_request_count > 0)
			{
				node.logger.try_log ("Representative did not respond to confirm_req, retrying: ", rep.account.to_account ());
			}
		}
	}
	// Unique channels as there can be multiple reps per channel
	rep_channels_missing_vote_l.erase (std::unique (rep_channels_missing_vote_l.begin (), rep_channels_missing_vote_l.end ()), rep_channels_missing_vote_l.end ());
	bool low_reps_weight (rep_channels_missing_vote_l.empty () || node.rep_crawler.total_weight () < node.config.online_weight_minimum.number ());
	if (low_reps_weight && roots_size_l <= 5 && !node.network_params.network.is_test_network ())
	{
		// Spam mode
		auto deque_l (node.network.udp_channels.random_set (100));
		auto vec (std::make_shared<std::vector<std::shared_ptr<nano::transport::channel>>> ());
		for (auto i : deque_l)
		{
			vec->push_back (i);
		}
		single_confirm_req_bundle_l.push_back (std::make_pair (election_l->status.winner, vec));
		inserted_into_any_bundle = true;
	}
	else
	{
		auto single_confirm_req_channels_l (std::make_shared<std::vector<std::shared_ptr<nano::transport::channel>>> ());
		for (auto & rep : rep_channels_missing_vote_l)
		{
			if (rep->get_network_version () >= node.network_params.protocol.tcp_realtime_protocol_version_min)
			{
				// Send batch request to peers supporting confirm_req by hash + root
				auto rep_request_l (batched_confirm_req_bundle_l.find (rep));
				auto block_l (election_l->status.winner);
				auto root_hash_l (std::make_pair (block_l->hash (), block_l->root ()));
				if (rep_request_l == batched_confirm_req_bundle_l.end ())
				{
					// Maximum number of representatives
					if (batched_confirm_req_bundle_l.size () < max_confirm_representatives)
					{
						std::deque<std::pair<nano::block_hash, nano::root>> insert_root_hash = { root_hash_l };
						batched_confirm_req_bundle_l.insert (std::make_pair (rep, insert_root_hash));
						inserted_into_any_bundle = true;
					}
				}
				// Maximum number of hashes
				else if (rep_request_l->second.size () < max_confirm_req_batches * nano::network::confirm_req_hashes_max)
				{
					rep_request_l->second.push_back (root_hash_l);
					inserted_into_any_bundle = true;
				}
			}
			else
			{
				single_confirm_req_channels_l->push_back (rep);
			}
		}
		// broadcast_confirm_req_base modifies reps, so we clone it once to avoid aliasing
		if (single_confirm_req_bundle_l.size () < max_confirm_req && !single_confirm_req_channels_l->empty ())
		{
			single_confirm_req_bundle_l.push_back (std::make_pair (election_l->status.winner, single_confirm_req_channels_l));
			inserted_into_any_bundle = true;
		}
	}
	return inserted_into_any_bundle;
}

void nano::active_transactions::request_confirm (nano::unique_lock<std::mutex> & lock_a)
{
	assert (!mutex.try_lock ());
	auto transaction_l (node.store.tx_begin_read ());
	std::unordered_set<nano::qualified_root> inactive_l;
	std::deque<std::shared_ptr<nano::block>> blocks_bundle_l;
	std::unordered_map<std::shared_ptr<nano::transport::channel>, std::deque<std::pair<nano::block_hash, nano::root>>> batched_confirm_req_bundle_l;
	std::deque<std::pair<std::shared_ptr<nano::block>, std::shared_ptr<std::vector<std::shared_ptr<nano::transport::channel>>>>> single_confirm_req_bundle_l;

	/*
	 * Confirm frontiers when there aren't many confirmations already pending and node finished initial bootstrap
	 * In auto mode start confirm only if node contains almost principal representative (half of required for principal weight)
	 */

	// Due to the confirmation height processor working asynchronously and compressing several roots into one frontier, probably_unconfirmed_frontiers can be wrong
	{
		auto pending_confirmation_height_size (node.pending_confirmation_height.size ());
		bool probably_unconfirmed_frontiers (node.ledger.block_count_cache > node.ledger.cemented_count + roots.size () + pending_confirmation_height_size);
		bool bootstrap_weight_reached (node.ledger.block_count_cache >= node.ledger.bootstrap_weight_max_blocks);
		if (node.config.frontiers_confirmation != nano::frontiers_confirmation_mode::disabled && bootstrap_weight_reached && probably_unconfirmed_frontiers && pending_confirmation_height_size < confirmed_frontiers_max_pending_cut_off)
		{
			lock_a.unlock ();
			search_frontiers (transaction_l);
			lock_a.lock ();
		}
	}

	// Any new election started from process_live only gets requests after at least 1 second
	auto cutoff_l (std::chrono::steady_clock::now () - election_request_delay);
	// Elections taking too long get escalated
	auto long_election_cutoff_l (std::chrono::steady_clock::now () - long_election_threshold);
	// The lowest PoW difficulty elections have a maximum time to live if they are beyond the soft threshold size for the container
	auto election_ttl_cutoff_l (std::chrono::steady_clock::now () - election_time_to_live);

	auto const representatives_l (node.rep_crawler.representatives (std::numeric_limits<size_t>::max ()));
	auto roots_size_l (roots.size ());
	auto & sorted_roots_l = roots.get<1> ();
	size_t count_l{ 0 };

	/*
	 * Loop through active elections in descending order of proof-of-work difficulty, requesting confirmation
	 *
	 * Only up to a certain amount of elections are queued for confirmation request and block rebroadcasting. The remaining elections can still be confirmed if votes arrive
	 * We avoid selecting the same elections repeatedly in the next loops, through a modulo on confirmation_request_count
	 * An election only gets confirmation_request_count increased after the first confirm_req; after that it is increased every loop unless they don't fit in the queues
	 * Elections extending the soft config.active_elections_size limit are flushed after a certain time-to-live cutoff
	 * Flushed elections are later re-activated via frontier confirmation
	 */
	for (auto i = sorted_roots_l.begin (), n = sorted_roots_l.end (); i != n; ++i, ++count_l)
	{
		auto election_l (i->election);
		auto root_l (i->root);
		// Erase finished elections
		if ((election_l->confirmed || election_l->stopped))
		{
			inactive_l.insert (root_l);
		}
		// Drop elections
		else if (count_l >= node.config.active_elections_size && election_l->election_start < election_ttl_cutoff_l && !node.wallets.watcher->is_watched (root_l))
		{
			election_l->stop ();
			inactive_l.insert (root_l);
			add_dropped_elections_cache (root_l);
		}
		// Broadcast and request confirmation
		else if (election_l->skip_delay || election_l->election_start < cutoff_l)
		{
			bool increment_counter_l{ true };
			// Escalate long election after a certain time and number of requests performed
			if (election_l->confirmation_request_count > 4 && election_l->election_start < long_election_cutoff_l)
			{
				election_escalate (election_l, transaction_l, roots_size_l);
			}
			// Block broadcasting
			if (election_l->confirmation_request_count % 8 == 1)
			{
				election_broadcast (election_l, transaction_l, blocks_bundle_l, inactive_l, root_l);
			}
			// Confirmation requesting
			if (election_l->confirmation_request_count % 4 == 0)
			{
				// If failed to insert into any of the bundles (capped), don't increment the counter so that the same root is sent for confirmation in the next loop
				if (!election_request_confirm (election_l, representatives_l, roots_size_l, single_confirm_req_bundle_l, batched_confirm_req_bundle_l))
				{
					increment_counter_l = false;
				}
			}
			if (increment_counter_l || node.network_params.network.is_test_network ())
			{
				++election_l->confirmation_request_count;
			}
		}
	}
	ongoing_broadcasts = !blocks_bundle_l.empty () + !batched_confirm_req_bundle_l.empty () + !single_confirm_req_bundle_l.empty ();
	lock_a.unlock ();

	// Rebroadcast unconfirmed blocks
	if (!blocks_bundle_l.empty ())
	{
		node.network.flood_block_many (
			std::move (blocks_bundle_l), [this]() {
				{
					nano::lock_guard<std::mutex> guard_l (this->mutex);
					--this->ongoing_broadcasts;
				}
				this->condition.notify_all ();
			},
			10); // 10ms/block * 30blocks = 300ms < 500ms
	}
	// Batch confirmation request
	if (!batched_confirm_req_bundle_l.empty ())
	{
		node.network.broadcast_confirm_req_batched_many (
			batched_confirm_req_bundle_l, [this]() {
				{
					nano::lock_guard<std::mutex> guard_l (this->mutex);
					--this->ongoing_broadcasts;
				}
				this->condition.notify_all ();
			},
			15); // 15ms/batch * 20batches = 300ms < 500ms
	}
	// Single confirmation requests
	if (!single_confirm_req_bundle_l.empty ())
	{
		node.network.broadcast_confirm_req_many (
			single_confirm_req_bundle_l, [this]() {
				{
					nano::lock_guard<std::mutex> guard_l (this->mutex);
					--this->ongoing_broadcasts;
				}
				this->condition.notify_all ();
			},
			30); // 30~60ms/req * 5 reqs = 150~300ms < 500ms
	}
	lock_a.lock ();
	// Erase inactive elections
	for (auto i (inactive_l.begin ()), n (inactive_l.end ()); i != n; ++i)
	{
		auto root_it (roots.find (*i));
		if (root_it != roots.end ())
		{
			root_it->election->clear_blocks ();
			root_it->election->clear_dependent ();
			roots.erase (root_it);
		}
	}
}

void nano::active_transactions::request_loop ()
{
	nano::unique_lock<std::mutex> lock (mutex);
	started = true;
	lock.unlock ();
	condition.notify_all ();

	// The wallets and active_transactions objects are mutually dependent, so we need a fully
	// constructed node before proceeding.
	this->node.node_initialized_latch.wait ();

	lock.lock ();

	while (!stopped)
	{
		// Account for the time spent in request_confirm by defining the wakeup point beforehand
		const auto wakeup_l (std::chrono::steady_clock::now () + std::chrono::milliseconds (node.network_params.network.request_interval_ms));

		update_active_difficulty (lock);
		request_confirm (lock);

		// Sleep until all broadcasts are done, plus the remaining loop time
		while (!stopped && ongoing_broadcasts)
		{
			condition.wait (lock);
		}
		if (!stopped)
		{
			// clang-format off
			condition.wait_until (lock, wakeup_l, [&wakeup_l, &stopped = stopped] { return stopped || std::chrono::steady_clock::now () >= wakeup_l; });
			// clang-format on
		}
	}
}

void nano::active_transactions::prioritize_account_for_confirmation (nano::active_transactions::prioritize_num_uncemented & cementable_frontiers_a, size_t & cementable_frontiers_size_a, nano::account const & account_a, nano::account_info const & info_a, uint64_t confirmation_height)
{
	if (info_a.block_count > confirmation_height && !node.pending_confirmation_height.is_processing_block (info_a.head))
	{
		auto num_uncemented = info_a.block_count - confirmation_height;
		nano::lock_guard<std::mutex> guard (mutex);
		auto it = cementable_frontiers_a.find (account_a);
		if (it != cementable_frontiers_a.end ())
		{
			if (it->blocks_uncemented != num_uncemented)
			{
				// Account already exists and there is now a different uncemented block count so update it in the container
				cementable_frontiers_a.modify (it, [num_uncemented](nano::cementable_account & info) {
					info.blocks_uncemented = num_uncemented;
				});
			}
		}
		else
		{
			assert (cementable_frontiers_size_a <= max_priority_cementable_frontiers);
			if (cementable_frontiers_size_a == max_priority_cementable_frontiers)
			{
				// The maximum amount of frontiers stored has been reached. Check if the current frontier
				// has more uncemented blocks than the lowest uncemented frontier in the collection if so replace it.
				auto least_uncemented_frontier_it = cementable_frontiers_a.get<1> ().end ();
				--least_uncemented_frontier_it;
				if (num_uncemented > least_uncemented_frontier_it->blocks_uncemented)
				{
					cementable_frontiers_a.get<1> ().erase (least_uncemented_frontier_it);
					cementable_frontiers_a.emplace (account_a, num_uncemented);
				}
			}
			else
			{
				cementable_frontiers_a.emplace (account_a, num_uncemented);
			}
		}
		cementable_frontiers_size_a = cementable_frontiers_a.size ();
	}
}

void nano::active_transactions::prioritize_frontiers_for_confirmation (nano::transaction const & transaction_a, std::chrono::milliseconds ledger_accounts_time_a, std::chrono::milliseconds wallet_account_time_a)
{
	// Don't try to prioritize when there are a large number of pending confirmation heights as blocks can be cemented in the meantime, making the prioritization less reliable
	if (node.pending_confirmation_height.size () < confirmed_frontiers_max_pending_cut_off)
	{
		size_t priority_cementable_frontiers_size;
		size_t priority_wallet_cementable_frontiers_size;
		{
			nano::lock_guard<std::mutex> guard (mutex);
			priority_cementable_frontiers_size = priority_cementable_frontiers.size ();
			priority_wallet_cementable_frontiers_size = priority_wallet_cementable_frontiers.size ();
		}
		nano::timer<std::chrono::milliseconds> wallet_account_timer;
		wallet_account_timer.start ();

		if (!skip_wallets)
		{
			// Prioritize wallet accounts first
			{
				nano::lock_guard<std::mutex> lock (node.wallets.mutex);
				auto wallet_transaction (node.wallets.tx_begin_read ());
				auto const & items = node.wallets.items;
				if (items.empty ())
				{
					skip_wallets = true;
				}
				for (auto item_it = items.cbegin (); item_it != items.cend (); ++item_it)
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
					uint64_t confirmation_height = 0;
					for (; i != n; ++i)
					{
						auto const & account (i->first);
						if (!node.store.account_get (transaction_a, account, info) && !node.store.confirmation_height_get (transaction_a, account, confirmation_height))
						{
							// If it exists in normal priority collection delete from there.
							auto it = priority_cementable_frontiers.find (account);
							if (it != priority_cementable_frontiers.end ())
							{
								nano::lock_guard<std::mutex> guard (mutex);
								priority_cementable_frontiers.erase (it);
								priority_cementable_frontiers_size = priority_cementable_frontiers.size ();
							}

							prioritize_account_for_confirmation (priority_wallet_cementable_frontiers, priority_wallet_cementable_frontiers_size, account, info, confirmation_height);

							if (wallet_account_timer.since_start () >= wallet_account_time_a)
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

		nano::timer<std::chrono::milliseconds> timer;
		timer.start ();

		auto i (node.store.latest_begin (transaction_a, next_frontier_account));
		auto n (node.store.latest_end ());
		uint64_t confirmation_height = 0;
		for (; i != n && !stopped; ++i)
		{
			auto const & account (i->first);
			auto const & info (i->second);
			if (priority_wallet_cementable_frontiers.find (account) == priority_wallet_cementable_frontiers.end ())
			{
				if (!node.store.confirmation_height_get (transaction_a, account, confirmation_height))
				{
					prioritize_account_for_confirmation (priority_cementable_frontiers, priority_cementable_frontiers_size, account, info, confirmation_height);
				}
			}
			next_frontier_account = account.number () + 1;
			if (timer.since_start () >= ledger_accounts_time_a)
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
	nano::unique_lock<std::mutex> lock (mutex);
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
	lock.lock ();
	roots.clear ();
}

bool nano::active_transactions::start (std::shared_ptr<nano::block> block_a, bool const skip_delay_a, std::function<void(std::shared_ptr<nano::block>)> const & confirmation_action_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	return add (block_a, skip_delay_a, confirmation_action_a);
}

bool nano::active_transactions::add (std::shared_ptr<nano::block> block_a, bool const skip_delay_a, std::function<void(std::shared_ptr<nano::block>)> const & confirmation_action_a)
{
	auto error (true);
	if (!stopped)
	{
		auto root (block_a->qualified_root ());
		auto existing (roots.find (root));
		if (existing == roots.end () && confirmed_set.get<1> ().find (root) == confirmed_set.get<1> ().end ())
		{
			auto hash (block_a->hash ());
			auto election (nano::make_shared<nano::election> (node, block_a, skip_delay_a, confirmation_action_a));
			uint64_t difficulty (0);
			error = nano::work_validate (*block_a, &difficulty);
			release_assert (!error);
			roots.insert (nano::conflict_info{ root, difficulty, difficulty, election });
			blocks.insert (std::make_pair (hash, election));
			adjust_difficulty (hash);
			election->insert_inactive_votes_cache ();
		}
	}
	return error;
}

// Validate a vote and apply it to the current election if one exists
bool nano::active_transactions::vote (std::shared_ptr<nano::vote> vote_a, bool single_lock)
{
	std::shared_ptr<nano::election> election;
	bool replay (false);
	bool processed (false);
	{
		nano::unique_lock<std::mutex> lock;
		if (!single_lock)
		{
			lock = nano::unique_lock<std::mutex> (mutex);
		}
		for (auto vote_block : vote_a->blocks)
		{
			nano::election_vote_result result;
			if (vote_block.which ())
			{
				auto block_hash (boost::get<nano::block_hash> (vote_block));
				auto existing (blocks.find (block_hash));
				if (existing != blocks.end ())
				{
					result = existing->second->vote (vote_a->account, vote_a->sequence, block_hash);
				}
				else
				{
					add_inactive_votes_cache (block_hash, vote_a->account);
				}
			}
			else
			{
				auto block (boost::get<std::shared_ptr<nano::block>> (vote_block));
				auto existing (roots.find (block->qualified_root ()));
				if (existing != roots.end ())
				{
					result = existing->election->vote (vote_a->account, vote_a->sequence, block->hash ());
				}
				else
				{
					add_inactive_votes_cache (block->hash (), vote_a->account);
				}
			}
			replay = replay || result.replay;
			processed = processed || result.processed;
		}
	}
	if (processed)
	{
		node.network.flood_vote (vote_a);
	}
	return replay;
}

bool nano::active_transactions::active (nano::qualified_root const & root_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	return roots.find (root_a) != roots.end ();
}

bool nano::active_transactions::active (nano::block const & block_a)
{
	return active (block_a.qualified_root ());
}

void nano::active_transactions::update_difficulty (std::shared_ptr<nano::block> block_a, boost::optional<nano::write_transaction const &> opt_transaction_a)
{
	nano::unique_lock<std::mutex> lock (mutex);
	auto existing_election (roots.find (block_a->qualified_root ()));
	if (existing_election != roots.end ())
	{
		uint64_t difficulty;
		auto error (nano::work_validate (*block_a, &difficulty));
		(void)error;
		assert (!error);
		if (difficulty > existing_election->difficulty)
		{
			if (node.config.logging.active_update_logging ())
			{
				node.logger.try_log (boost::str (boost::format ("Block %1% was updated from difficulty %2% to %3%") % block_a->hash ().to_string () % nano::to_string_hex (existing_election->difficulty) % nano::to_string_hex (difficulty)));
			}
			roots.modify (existing_election, [difficulty](nano::conflict_info & info_a) {
				info_a.difficulty = difficulty;
			});
			adjust_difficulty (block_a->hash ());
		}
	}
	else if (opt_transaction_a.is_initialized ())
	{
		// Only guaranteed to immediately restart the election if the new block is received within 60s of dropping it
		static constexpr std::chrono::seconds recently_dropped_cutoff{ 60s };
		if (find_dropped_elections_cache (block_a->qualified_root ()) > std::chrono::steady_clock::now () - recently_dropped_cutoff)
		{
			lock.unlock ();
			nano::block_sideband existing_sideband;
			auto hash (block_a->hash ());
			auto existing_block (node.store.block_get (*opt_transaction_a, hash, &existing_sideband));
			release_assert (existing_block != nullptr);
			uint64_t confirmation_height;
			release_assert (!node.store.confirmation_height_get (*opt_transaction_a, node.store.block_account (*opt_transaction_a, hash), confirmation_height));
			bool confirmed = (confirmation_height >= existing_sideband.height);
			if (!confirmed && existing_block->block_work () != block_a->block_work ())
			{
				uint64_t existing_difficulty;
				uint64_t new_difficulty;
				if (!nano::work_validate (*block_a, &new_difficulty) && !nano::work_validate (*existing_block, &existing_difficulty))
				{
					if (new_difficulty > existing_difficulty)
					{
						// Re-writing the block is necessary to avoid the same work being received later to force restarting the election
						// The existing block is re-written, not the arriving block, as that one might not have gone through a full signature check
						existing_block->block_work_set (block_a->block_work ());
						node.store.block_put (*opt_transaction_a, hash, *existing_block, existing_sideband);

						// Restart election for the upgraded block, previously dropped from elections
						lock.lock ();
						add (existing_block);
					}
				}
			}
		}
	}
}

void nano::active_transactions::adjust_difficulty (nano::block_hash const & hash_a)
{
	assert (!mutex.try_lock ());
	std::deque<std::pair<nano::block_hash, int64_t>> remaining_blocks;
	remaining_blocks.emplace_back (hash_a, 0);
	std::unordered_set<nano::block_hash> processed_blocks;
	std::vector<std::pair<nano::qualified_root, int64_t>> elections_list;
	double sum (0.);
	int64_t highest_level (0);
	int64_t lowest_level (0);
	while (!remaining_blocks.empty ())
	{
		auto const & item (remaining_blocks.front ());
		auto hash (item.first);
		auto level (item.second);
		if (processed_blocks.find (hash) == processed_blocks.end ())
		{
			auto existing (blocks.find (hash));
			if (existing != blocks.end () && !existing->second->confirmed && !existing->second->stopped && existing->second->status.winner->hash () == hash)
			{
				auto previous (existing->second->status.winner->previous ());
				if (!previous.is_zero ())
				{
					remaining_blocks.emplace_back (previous, level + 1);
				}
				auto source (existing->second->status.winner->source ());
				if (!source.is_zero () && source != previous)
				{
					remaining_blocks.emplace_back (source, level + 1);
				}
				auto link (existing->second->status.winner->link ());
				if (!link.is_zero () && !node.ledger.is_epoch_link (link) && link != previous)
				{
					remaining_blocks.emplace_back (link, level + 1);
				}
				for (auto & dependent_block : existing->second->dependent_blocks)
				{
					remaining_blocks.emplace_back (dependent_block, level - 1);
				}
				processed_blocks.insert (hash);
				nano::qualified_root root (previous, existing->second->status.winner->root ());
				auto existing_root (roots.find (root));
				if (existing_root != roots.end ())
				{
					sum += nano::difficulty::to_multiplier (existing_root->difficulty, node.network_params.network.publish_threshold);
					elections_list.emplace_back (root, level);
					if (level > highest_level)
					{
						highest_level = level;
					}
					else if (level < lowest_level)
					{
						lowest_level = level;
					}
				}
			}
		}
		remaining_blocks.pop_front ();
	}
	if (!elections_list.empty ())
	{
		double multiplier = sum / elections_list.size ();
		uint64_t average = nano::difficulty::from_multiplier (multiplier, node.network_params.network.publish_threshold);
		// Prevent overflow
		int64_t limiter (0);
		if (std::numeric_limits<std::uint64_t>::max () - average < static_cast<uint64_t> (highest_level))
		{
			// Highest adjusted difficulty value should be std::numeric_limits<std::uint64_t>::max ()
			limiter = std::numeric_limits<std::uint64_t>::max () - average + highest_level;
			assert (std::numeric_limits<std::uint64_t>::max () == average + highest_level - limiter);
		}
		else if (average < std::numeric_limits<std::uint64_t>::min () - lowest_level)
		{
			// Lowest adjusted difficulty value should be std::numeric_limits<std::uint64_t>::min ()
			limiter = std::numeric_limits<std::uint64_t>::min () - average + lowest_level;
			assert (std::numeric_limits<std::uint64_t>::min () == average + lowest_level - limiter);
		}

		// Set adjusted difficulty
		for (auto & item : elections_list)
		{
			auto existing_root (roots.find (item.first));
			uint64_t difficulty_a = average + item.second - limiter;
			roots.modify (existing_root, [difficulty_a](nano::conflict_info & info_a) {
				info_a.adjusted_difficulty = difficulty_a;
			});
		}
	}
}

void nano::active_transactions::update_active_difficulty (nano::unique_lock<std::mutex> & lock_a)
{
	assert (!mutex.try_lock ());
	double multiplier (1.);
	if (!roots.empty ())
	{
		auto & sorted_roots = roots.get<1> ();
		std::vector<uint64_t> active_root_difficulties;
		active_root_difficulties.reserve (std::min (roots.size (), node.config.active_elections_size));
		size_t count (0);
		auto cutoff (std::chrono::steady_clock::now () - election_request_delay - 1s);
		for (auto it (sorted_roots.begin ()), end (sorted_roots.end ()); it != end && count++ < node.config.active_elections_size; ++it)
		{
			if (!it->election->confirmed && !it->election->stopped && it->election->election_start < cutoff)
			{
				active_root_difficulties.push_back (it->adjusted_difficulty);
			}
		}
		if (active_root_difficulties.size () > 10 || (!active_root_difficulties.empty () && node.network_params.network.is_test_network ()))
		{
			multiplier = nano::difficulty::to_multiplier (active_root_difficulties[active_root_difficulties.size () / 2], node.network_params.network.publish_threshold);
		}
	}
	assert (multiplier >= 1);
	multipliers_cb.push_front (multiplier);
	auto sum (std::accumulate (multipliers_cb.begin (), multipliers_cb.end (), double(0)));
	auto difficulty = nano::difficulty::from_multiplier (sum / multipliers_cb.size (), node.network_params.network.publish_threshold);
	assert (difficulty >= node.network_params.network.publish_threshold);

	trended_active_difficulty = difficulty;
	node.observers.difficulty.notify (trended_active_difficulty);
}

uint64_t nano::active_transactions::active_difficulty ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	return trended_active_difficulty;
}

uint64_t nano::active_transactions::limited_active_difficulty ()
{
	return std::min (active_difficulty (), node.config.max_work_generate_difficulty);
}

// List of active blocks in elections
std::deque<std::shared_ptr<nano::block>> nano::active_transactions::list_blocks (bool single_lock)
{
	std::deque<std::shared_ptr<nano::block>> result;
	nano::unique_lock<std::mutex> lock;
	if (!single_lock)
	{
		lock = nano::unique_lock<std::mutex> (mutex);
	}
	for (auto i (roots.begin ()), n (roots.end ()); i != n; ++i)
	{
		result.push_back (i->election->status.winner);
	}
	return result;
}

std::deque<nano::election_status> nano::active_transactions::list_confirmed ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	return confirmed;
}

void nano::active_transactions::add_confirmed (nano::election_status const & status_a, nano::qualified_root const & root_a)
{
	confirmed.push_back (status_a);
	auto inserted (confirmed_set.insert (nano::election_timepoint{ std::chrono::steady_clock::now (), root_a }));
	if (confirmed.size () > node.config.confirmation_history_size)
	{
		confirmed.pop_front ();
		if (inserted.second)
		{
			confirmed_set.erase (confirmed_set.begin ());
		}
	}
}

void nano::active_transactions::erase (nano::block const & block_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	auto root_it (roots.find (block_a.qualified_root ()));
	if (root_it != roots.end ())
	{
		root_it->election->stop ();
		root_it->election->clear_blocks ();
		root_it->election->clear_dependent ();
		roots.erase (root_it);
		node.logger.try_log (boost::str (boost::format ("Election erased for block block %1% root %2%") % block_a.hash ().to_string () % block_a.root ().to_string ()));
	}
}

bool nano::active_transactions::empty ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	return roots.empty ();
}

size_t nano::active_transactions::size ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	return roots.size ();
}

bool nano::active_transactions::publish (std::shared_ptr<nano::block> block_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	auto existing (roots.find (block_a->qualified_root ()));
	auto result (true);
	if (existing != roots.end ())
	{
		auto election (existing->election);
		result = election->publish (block_a);
		if (!result && !election->confirmed)
		{
			blocks.insert (std::make_pair (block_a->hash (), election));
		}
	}
	return result;
}

void nano::active_transactions::clear_block (nano::block_hash const & hash_a)
{
	nano::lock_guard<std::mutex> guard (mutex);
	pending_conf_height.erase (hash_a);
}

// Returns the type of election status requiring callbacks calling later
boost::optional<nano::election_status_type> nano::active_transactions::confirm_block (nano::transaction const & transaction_a, std::shared_ptr<nano::block> block_a)
{
	auto hash (block_a->hash ());
	nano::unique_lock<std::mutex> lock (mutex);
	auto existing (blocks.find (hash));
	if (existing != blocks.end ())
	{
		if (!existing->second->confirmed && !existing->second->stopped && existing->second->status.winner->hash () == hash)
		{
			existing->second->confirm_once (nano::election_status_type::active_confirmation_height);
			return nano::election_status_type::active_confirmation_height;
		}
		else
		{
			return boost::optional<nano::election_status_type>{};
		}
	}
	else
	{
		return nano::election_status_type::inactive_confirmation_height;
	}
}

size_t nano::active_transactions::priority_cementable_frontiers_size ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	return priority_cementable_frontiers.size ();
}

size_t nano::active_transactions::priority_wallet_cementable_frontiers_size ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	return priority_wallet_cementable_frontiers.size ();
}

boost::circular_buffer<double> nano::active_transactions::difficulty_trend ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	return multipliers_cb;
}

size_t nano::active_transactions::inactive_votes_cache_size ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	return inactive_votes_cache.size ();
}

void nano::active_transactions::add_inactive_votes_cache (nano::block_hash const & hash_a, nano::account const & representative_a)
{
	// Check principal representative status
	if (node.ledger.weight (representative_a) > node.minimum_principal_weight ())
	{
		auto existing (inactive_votes_cache.get<1> ().find (hash_a));
		if (existing != inactive_votes_cache.get<1> ().end () && !existing->confirmed)
		{
			auto is_new (false);
			inactive_votes_cache.get<1> ().modify (existing, [representative_a, &is_new](nano::gap_information & info) {
				auto it = std::find (info.voters.begin (), info.voters.end (), representative_a);
				is_new = (it == info.voters.end ());
				if (is_new)
				{
					info.arrival = std::chrono::steady_clock::now ();
					info.voters.push_back (representative_a);
				}
			});

			if (is_new)
			{
				if (node.gap_cache.bootstrap_check (existing->voters, hash_a))
				{
					inactive_votes_cache.get<1> ().modify (existing, [](nano::gap_information & info) {
						info.confirmed = true;
					});
				}
			}
		}
		else
		{
			inactive_votes_cache.insert ({ std::chrono::steady_clock::now (), hash_a, std::vector<nano::account> (1, representative_a) });
			if (inactive_votes_cache.size () > inactive_votes_cache_max)
			{
				inactive_votes_cache.get<0> ().erase (inactive_votes_cache.get<0> ().begin ());
			}
		}
	}
}

nano::gap_information nano::active_transactions::find_inactive_votes_cache (nano::block_hash const & hash_a)
{
	auto existing (inactive_votes_cache.get<1> ().find (hash_a));
	if (existing != inactive_votes_cache.get<1> ().end ())
	{
		return *existing;
	}
	else
	{
		return nano::gap_information{ std::chrono::steady_clock::time_point{}, 0, std::vector<nano::account>{} };
	}
}

size_t nano::active_transactions::dropped_elections_cache_size ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	return dropped_elections_cache.size ();
}

void nano::active_transactions::add_dropped_elections_cache (nano::qualified_root const & root_a)
{
	assert (!mutex.try_lock ());
	dropped_elections_cache.insert (nano::election_timepoint{ std::chrono::steady_clock::now (), root_a });
	if (dropped_elections_cache.size () > dropped_elections_cache_max)
	{
		dropped_elections_cache.get<0> ().erase (dropped_elections_cache.get<0> ().begin ());
	}
}

std::chrono::steady_clock::time_point nano::active_transactions::find_dropped_elections_cache (nano::qualified_root const & root_a)
{
	assert (!mutex.try_lock ());
	auto existing (dropped_elections_cache.get<1> ().find (root_a));
	if (existing != dropped_elections_cache.get<1> ().end ())
	{
		return existing->time;
	}
	else
	{
		return std::chrono::steady_clock::time_point{};
	}
}

nano::cementable_account::cementable_account (nano::account const & account_a, size_t blocks_uncemented_a)
	: account (account_a)
	, blocks_uncemented (blocks_uncemented_a)
{
}

namespace nano
{
std::unique_ptr<seq_con_info_component> collect_seq_con_info (active_transactions & active_transactions, const std::string & name)
{
	size_t roots_count = 0;
	size_t blocks_count = 0;
	size_t confirmed_count = 0;
	size_t pending_conf_height_count = 0;

	{
		nano::lock_guard<std::mutex> guard (active_transactions.mutex);
		roots_count = active_transactions.roots.size ();
		blocks_count = active_transactions.blocks.size ();
		confirmed_count = active_transactions.confirmed.size ();
		pending_conf_height_count = active_transactions.pending_conf_height.size ();
	}

	auto composite = std::make_unique<seq_con_info_composite> (name);
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "roots", roots_count, sizeof (decltype (active_transactions.roots)::value_type) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "blocks", blocks_count, sizeof (decltype (active_transactions.blocks)::value_type) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "pending_conf_height", pending_conf_height_count, sizeof (decltype (active_transactions.pending_conf_height)::value_type) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "confirmed", confirmed_count, sizeof (decltype (active_transactions.confirmed)::value_type) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "priority_wallet_cementable_frontiers_count", active_transactions.priority_wallet_cementable_frontiers_size (), sizeof (nano::cementable_account) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "priority_cementable_frontiers_count", active_transactions.priority_cementable_frontiers_size (), sizeof (nano::cementable_account) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "inactive_votes_cache_count", active_transactions.inactive_votes_cache_size (), sizeof (nano::gap_information) }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "dropped_elections_count", active_transactions.dropped_elections_cache_size (), sizeof (nano::election_timepoint) }));
	return composite;
}
}
