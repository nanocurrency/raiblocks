#include <nano/node/election.hpp>
#include <nano/node/node.hpp>

#include <boost/format.hpp>

nano::election_vote_result::election_vote_result (bool replay_a, bool processed_a)
{
	replay = replay_a;
	processed = processed_a;
}

nano::election::election (nano::node & node_a, std::shared_ptr<nano::block> block_a, bool const skip_delay_a, std::function<void(std::shared_ptr<nano::block>)> const & confirmation_action_a)
	: confirmation_action (confirmation_action_a)
	, node (node_a)
	, election_start (std::chrono::steady_clock::now ())
	, status ({ block_a, 0, std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now ().time_since_epoch ()), std::chrono::duration_values<std::chrono::milliseconds>::zero (), 0, 1, 0, nano::election_status_type::ongoing })
	, skip_delay (skip_delay_a)
	, confirmed (false)
	, stopped (false)
{
	last_votes.insert (std::make_pair (node.network_params.random.not_an_account, nano::vote_info{ std::chrono::steady_clock::now (), 0, block_a->hash () }));
	blocks.insert (std::make_pair (block_a->hash (), block_a));
	update_dependent ();
}

void nano::election::compute_rep_votes (nano::transaction const & transaction_a)
{
	if (node.config.enable_voting)
	{
		node.wallets.foreach_representative ([this, &transaction_a](nano::public_key const & pub_a, nano::raw_key const & prv_a) {
			auto vote (this->node.store.vote_generate (transaction_a, pub_a, prv_a, status.winner));
			this->node.vote_processor.vote (vote, std::make_shared<nano::transport::channel_udp> (this->node.network.udp_channels, this->node.network.endpoint (), this->node.network_params.protocol.protocol_version));
		});
	}
}

void nano::election::confirm_once (nano::election_status_type type_a)
{
	assert (!node.active.mutex.try_lock ());
	if (!confirmed.exchange (true))
	{
		status.election_end = std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now ().time_since_epoch ());
		status.election_duration = std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::steady_clock::now () - election_start);
		status.confirmation_request_count = confirmation_request_count;
		status.block_count = blocks.size ();
		status.voter_count = last_votes.size ();
		status.type = type_a;
		auto status_l (status);
		auto node_l (node.shared ());
		auto confirmation_action_l (confirmation_action);
		node.background ([node_l, status_l, confirmation_action_l]() {
			node_l->process_confirmed (status_l);
			confirmation_action_l (status_l.winner);
		});
		auto root (status.winner->qualified_root ());
		node.active.pending_conf_height.emplace (status.winner->hash (), shared_from_this ());
		clear_blocks ();
		clear_dependent ();
		node.active.roots.erase (root);
	}
}

void nano::election::stop ()
{
	assert (!node.active.mutex.try_lock ());
	if (!stopped && !confirmed)
	{
		stopped = true;
		status.election_end = std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now ().time_since_epoch ());
		status.election_duration = std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::steady_clock::now () - election_start);
		status.confirmation_request_count = confirmation_request_count;
		status.block_count = blocks.size ();
		status.voter_count = last_votes.size ();
		status.type = nano::election_status_type::stopped;
	}
}

bool nano::election::have_quorum (nano::tally_t const & tally_a, nano::uint128_t tally_sum) const
{
	bool result = false;
	if (tally_sum >= node.config.online_weight_minimum.number ())
	{
		auto i (tally_a.begin ());
		++i;
		auto second (i != tally_a.end () ? i->first : 0);
		auto delta_l (node.delta ());
		result = tally_a.begin ()->first > (second + delta_l);
	}
	return result;
}

nano::tally_t nano::election::tally ()
{
	std::unordered_map<nano::block_hash, nano::uint128_t> block_weights;
	for (auto vote_info : last_votes)
	{
		block_weights[vote_info.second.hash] += node.ledger.weight (vote_info.first);
	}
	last_tally = block_weights;
	nano::tally_t result;
	for (auto item : block_weights)
	{
		auto block (blocks.find (item.first));
		if (block != blocks.end ())
		{
			result.insert (std::make_pair (item.second, block->second));
		}
	}
	return result;
}

void nano::election::confirm_if_quorum ()
{
	auto tally_l (tally ());
	assert (!tally_l.empty ());
	auto winner (tally_l.begin ());
	auto block_l (winner->second);
	status.tally = winner->first;
	nano::uint128_t sum (0);
	for (auto & i : tally_l)
	{
		sum += i.first;
	}
	if (sum >= node.config.online_weight_minimum.number () && block_l->hash () != status.winner->hash ())
	{
		auto node_l (node.shared ());
		node_l->block_processor.force (block_l);
		status.winner = block_l;
		update_dependent ();
		node_l->active.adjust_difficulty (block_l->hash ());
	}
	if (have_quorum (tally_l, sum))
	{
		if (node.config.logging.vote_logging () || blocks.size () > 1)
		{
			log_votes (tally_l);
		}
		confirm_once (nano::election_status_type::active_confirmed_quorum);
	}
}

void nano::election::log_votes (nano::tally_t const & tally_a) const
{
	std::stringstream tally;
	std::string line_end (node.config.logging.single_line_record () ? "\t" : "\n");
	tally << boost::str (boost::format ("%1%Vote tally for root %2%") % line_end % status.winner->root ().to_string ());
	for (auto i (tally_a.begin ()), n (tally_a.end ()); i != n; ++i)
	{
		tally << boost::str (boost::format ("%1%Block %2% weight %3%") % line_end % i->second->hash ().to_string () % i->first.convert_to<std::string> ());
	}
	for (auto i (last_votes.begin ()), n (last_votes.end ()); i != n; ++i)
	{
		tally << boost::str (boost::format ("%1%%2% %3%") % line_end % i->first.to_account () % i->second.hash.to_string ());
	}
	node.logger.try_log (tally.str ());
}

nano::election_vote_result nano::election::vote (nano::account rep, uint64_t sequence, nano::block_hash block_hash)
{
	// see republish_vote documentation for an explanation of these rules
	auto replay (false);
	auto online_stake (node.online_reps.online_stake ());
	auto weight (node.ledger.weight (rep));
	auto should_process (false);
	if (node.network_params.network.is_test_network () || weight > node.minimum_principal_weight (online_stake))
	{
		unsigned int cooldown;
		if (weight < online_stake / 100) // 0.1% to 1%
		{
			cooldown = 15;
		}
		else if (weight < online_stake / 20) // 1% to 5%
		{
			cooldown = 5;
		}
		else // 5% or above
		{
			cooldown = 1;
		}
		auto last_vote_it (last_votes.find (rep));
		if (last_vote_it == last_votes.end ())
		{
			should_process = true;
		}
		else
		{
			auto last_vote (last_vote_it->second);
			if (last_vote.sequence < sequence || (last_vote.sequence == sequence && last_vote.hash < block_hash))
			{
				if (last_vote.time <= std::chrono::steady_clock::now () - std::chrono::seconds (cooldown))
				{
					should_process = true;
				}
			}
			else
			{
				replay = true;
			}
		}
		if (should_process)
		{
			node.stats.inc (nano::stat::type::election, nano::stat::detail::vote_new);
			last_votes[rep] = { std::chrono::steady_clock::now (), sequence, block_hash };
			if (!confirmed)
			{
				confirm_if_quorum ();
			}
		}
	}
	return nano::election_vote_result (replay, should_process);
}

bool nano::election::publish (std::shared_ptr<nano::block> block_a)
{
	auto result (false);
	if (blocks.size () >= 10)
	{
		if (last_tally[block_a->hash ()] < node.online_reps.online_stake () / 10)
		{
			result = true;
		}
	}
	if (!result)
	{
		auto transaction (node.store.tx_begin_read ());
		result = node.validate_block_by_previous (transaction, block_a);
		if (!result)
		{
			if (blocks.find (block_a->hash ()) == blocks.end ())
			{
				blocks.insert (std::make_pair (block_a->hash (), block_a));
				confirm_if_quorum ();
				node.network.flood_block (block_a, false);
			}
			else
			{
				result = true;
			}
		}
	}
	return result;
}

size_t nano::election::last_votes_size ()
{
	nano::lock_guard<std::mutex> lock (node.active.mutex);
	return last_votes.size ();
}

void nano::election::update_dependent ()
{
	assert (!node.active.mutex.try_lock ());
	std::vector<nano::block_hash> blocks_search;
	auto hash (status.winner->hash ());
	auto previous (status.winner->previous ());
	if (!previous.is_zero ())
	{
		blocks_search.push_back (previous);
	}
	auto source (status.winner->source ());
	if (!source.is_zero () && source != previous)
	{
		blocks_search.push_back (source);
	}
	auto link (status.winner->link ());
	if (!link.is_zero () && !node.ledger.is_epoch_link (link) && link != previous)
	{
		blocks_search.push_back (link);
	}
	for (auto & block_search : blocks_search)
	{
		auto existing (node.active.blocks.find (block_search));
		if (existing != node.active.blocks.end () && !existing->second->confirmed && !existing->second->stopped)
		{
			if (existing->second->dependent_blocks.find (hash) == existing->second->dependent_blocks.end ())
			{
				existing->second->dependent_blocks.insert (hash);
			}
		}
	}
}

void nano::election::clear_dependent ()
{
	for (auto & dependent_block : dependent_blocks)
	{
		node.active.adjust_difficulty (dependent_block);
	}
}

void nano::election::clear_blocks ()
{
	auto winner_hash (status.winner->hash ());
	for (auto & block : blocks)
	{
		auto & hash (block.first);
		auto erased (node.active.blocks.erase (hash));
		(void)erased;
		// clear_blocks () can be called in active_transactions::publish () before blocks insertion if election was confirmed
		assert (erased == 1 || confirmed);
		// Notify observers about dropped elections & blocks lost confirmed elections
		if (stopped || hash != winner_hash)
		{
			node.observers.active_stopped.notify (hash);
		}
	}
}

void nano::election::insert_inactive_votes_cache ()
{
	auto winner_hash (status.winner->hash ());
	auto cache (node.active.find_inactive_votes_cache (winner_hash));
	for (auto & rep : cache.voters)
	{
		auto inserted (last_votes.emplace (rep, nano::vote_info{ std::chrono::steady_clock::time_point::min (), 0, winner_hash }));
		if (inserted.second)
		{
			node.stats.inc (nano::stat::type::election, nano::stat::detail::vote_cached);
		}
	}
	if (!confirmed && !cache.voters.empty ())
	{
		auto delay (std::chrono::duration_cast<std::chrono::seconds> (std::chrono::steady_clock::now () - cache.arrival));
		if (delay > late_blocks_delay)
		{
			node.stats.inc (nano::stat::type::election, nano::stat::detail::late_block);
			node.stats.add (nano::stat::type::election, nano::stat::detail::late_block_seconds, nano::stat::dir::in, delay.count (), true);
		}
		confirm_if_quorum ();
	}
}
