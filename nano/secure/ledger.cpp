#include <nano/lib/rep_weights.hpp>
#include <nano/lib/stats.hpp>
#include <nano/lib/utility.hpp>
#include <nano/lib/work.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/ledger.hpp>
#include <nano/secure/store.hpp>

#include <crypto/cryptopp/words.h>

namespace
{
/**
 * Roll back the visited block
 */
class rollback_visitor : public nano::block_visitor
{
public:
	rollback_visitor (nano::write_transaction const & transaction_a, nano::ledger & ledger_a, std::vector<std::shared_ptr<nano::block>> & list_a) :
		transaction (transaction_a),
		ledger (ledger_a),
		list (list_a)
	{
	}
	virtual ~rollback_visitor () = default;
	void send_block (nano::send_block const & block_a) override
	{
		auto hash (block_a.hash ());
		nano::pending_info pending;
		nano::pending_key key (block_a.hashables.destination, hash);
		while (!error && ledger.store.pending.get (transaction, key, pending))
		{
			error = ledger.rollback (transaction, ledger.latest (transaction, block_a.hashables.destination), list);
		}
		if (!error)
		{
			nano::account_info info;
			[[maybe_unused]] auto error (ledger.store.account.get (transaction, pending.source, info));
			debug_assert (!error);
			ledger.store.pending.del (transaction, key);
			ledger.cache.rep_weights.representation_add (info.representative, pending.amount.number ());
			nano::account_info new_info (block_a.hashables.previous, info.representative, info.open_block, ledger.balance (transaction, block_a.hashables.previous), nano::seconds_since_epoch (), info.block_count - 1, nano::epoch::epoch_0);
			ledger.update_account (transaction, pending.source, info, new_info);
			ledger.store.block.del (transaction, hash);
			ledger.store.frontier.del (transaction, hash);
			ledger.store.frontier.put (transaction, block_a.hashables.previous, pending.source);
			ledger.store.block.successor_clear (transaction, block_a.hashables.previous);
			ledger.stats.inc (nano::stat::type::rollback, nano::stat::detail::send);
		}
	}
	void receive_block (nano::receive_block const & block_a) override
	{
		auto hash (block_a.hash ());
		auto amount (ledger.amount (transaction, hash));
		auto destination_account (ledger.account (transaction, hash));
		// Pending account entry can be incorrect if source block was pruned. But it's not affecting correct ledger processing
		[[maybe_unused]] bool is_pruned (false);
		auto source_account (ledger.account_safe (transaction, block_a.hashables.source, is_pruned));
		nano::account_info info;
		[[maybe_unused]] auto error (ledger.store.account.get (transaction, destination_account, info));
		debug_assert (!error);
		ledger.cache.rep_weights.representation_add (info.representative, 0 - amount);
		nano::account_info new_info (block_a.hashables.previous, info.representative, info.open_block, ledger.balance (transaction, block_a.hashables.previous), nano::seconds_since_epoch (), info.block_count - 1, nano::epoch::epoch_0);
		ledger.update_account (transaction, destination_account, info, new_info);
		ledger.store.block.del (transaction, hash);
		ledger.store.pending.put (transaction, nano::pending_key (destination_account, block_a.hashables.source), { source_account, amount, nano::epoch::epoch_0 });
		ledger.store.frontier.del (transaction, hash);
		ledger.store.frontier.put (transaction, block_a.hashables.previous, destination_account);
		ledger.store.block.successor_clear (transaction, block_a.hashables.previous);
		ledger.stats.inc (nano::stat::type::rollback, nano::stat::detail::receive);
	}
	void open_block (nano::open_block const & block_a) override
	{
		auto hash (block_a.hash ());
		auto amount (ledger.amount (transaction, hash));
		auto destination_account (ledger.account (transaction, hash));
		// Pending account entry can be incorrect if source block was pruned. But it's not affecting correct ledger processing
		[[maybe_unused]] bool is_pruned (false);
		auto source_account (ledger.account_safe (transaction, block_a.hashables.source, is_pruned));
		ledger.cache.rep_weights.representation_add (block_a.representative (), 0 - amount);
		nano::account_info new_info;
		ledger.update_account (transaction, destination_account, new_info, new_info);
		ledger.store.block.del (transaction, hash);
		ledger.store.pending.put (transaction, nano::pending_key (destination_account, block_a.hashables.source), { source_account, amount, nano::epoch::epoch_0 });
		ledger.store.frontier.del (transaction, hash);
		ledger.stats.inc (nano::stat::type::rollback, nano::stat::detail::open);
	}
	void change_block (nano::change_block const & block_a) override
	{
		auto hash (block_a.hash ());
		auto rep_block (ledger.representative (transaction, block_a.hashables.previous));
		auto account (ledger.account (transaction, block_a.hashables.previous));
		nano::account_info info;
		[[maybe_unused]] auto error (ledger.store.account.get (transaction, account, info));
		debug_assert (!error);
		auto balance (ledger.balance (transaction, block_a.hashables.previous));
		auto block = ledger.store.block.get (transaction, rep_block);
		release_assert (block != nullptr);
		auto representative = block->representative ();
		ledger.cache.rep_weights.representation_add_dual (block_a.representative (), 0 - balance, representative, balance);
		ledger.store.block.del (transaction, hash);
		nano::account_info new_info (block_a.hashables.previous, representative, info.open_block, info.balance, nano::seconds_since_epoch (), info.block_count - 1, nano::epoch::epoch_0);
		ledger.update_account (transaction, account, info, new_info);
		ledger.store.frontier.del (transaction, hash);
		ledger.store.frontier.put (transaction, block_a.hashables.previous, account);
		ledger.store.block.successor_clear (transaction, block_a.hashables.previous);
		ledger.stats.inc (nano::stat::type::rollback, nano::stat::detail::change);
	}
	void state_block (nano::state_block const & block_a) override
	{
		auto hash (block_a.hash ());
		nano::block_hash rep_block_hash (0);
		if (!block_a.hashables.previous.is_zero ())
		{
			rep_block_hash = ledger.representative (transaction, block_a.hashables.previous);
		}
		auto balance (ledger.balance (transaction, block_a.hashables.previous));
		auto is_send (block_a.hashables.balance < balance);
		nano::account representative{ 0 };
		if (!rep_block_hash.is_zero ())
		{
			// Move existing representation & add in amount delta
			auto block (ledger.store.block.get (transaction, rep_block_hash));
			debug_assert (block != nullptr);
			representative = block->representative ();
			ledger.cache.rep_weights.representation_add_dual (representative, balance, block_a.representative (), 0 - block_a.hashables.balance.number ());
		}
		else
		{
			// Add in amount delta only
			ledger.cache.rep_weights.representation_add (block_a.representative (), 0 - block_a.hashables.balance.number ());
		}

		nano::account_info info;
		auto error (ledger.store.account.get (transaction, block_a.hashables.account, info));

		if (is_send)
		{
			nano::pending_key key (block_a.hashables.link.as_account (), hash);
			while (!error && !ledger.store.pending.exists (transaction, key))
			{
				error = ledger.rollback (transaction, ledger.latest (transaction, block_a.hashables.link.as_account ()), list);
			}
			ledger.store.pending.del (transaction, key);
			ledger.stats.inc (nano::stat::type::rollback, nano::stat::detail::send);
		}
		else if (!block_a.hashables.link.is_zero () && !ledger.is_epoch_link (block_a.hashables.link))
		{
			// Pending account entry can be incorrect if source block was pruned. But it's not affecting correct ledger processing
			[[maybe_unused]] bool is_pruned (false);
			auto source_account (ledger.account_safe (transaction, block_a.hashables.link.as_block_hash (), is_pruned));
			nano::pending_info pending_info (source_account, block_a.hashables.balance.number () - balance, block_a.sideband ().source_epoch);
			ledger.store.pending.put (transaction, nano::pending_key (block_a.hashables.account, block_a.hashables.link.as_block_hash ()), pending_info);
			ledger.stats.inc (nano::stat::type::rollback, nano::stat::detail::receive);
		}

		debug_assert (!error);
		auto previous_version (ledger.store.block.version (transaction, block_a.hashables.previous));
		nano::account_info new_info (block_a.hashables.previous, representative, info.open_block, balance, nano::seconds_since_epoch (), info.block_count - 1, previous_version);
		ledger.update_account (transaction, block_a.hashables.account, info, new_info);

		auto previous (ledger.store.block.get (transaction, block_a.hashables.previous));
		if (previous != nullptr)
		{
			ledger.store.block.successor_clear (transaction, block_a.hashables.previous);
			if (previous->type () < nano::block_type::state)
			{
				ledger.store.frontier.put (transaction, block_a.hashables.previous, block_a.hashables.account);
			}
		}
		else
		{
			ledger.stats.inc (nano::stat::type::rollback, nano::stat::detail::open);
		}
		ledger.store.block.del (transaction, hash);
	}
	nano::write_transaction const & transaction;
	nano::ledger & ledger;
	std::vector<std::shared_ptr<nano::block>> & list;
	bool error{ false };
};

class ledger_processor : public nano::mutable_block_visitor
{
public:
	ledger_processor (nano::ledger &, nano::write_transaction const &, nano::signature_verification = nano::signature_verification::unknown);
	virtual ~ledger_processor () = default;
	void send_block (nano::send_block &) override;
	void receive_block (nano::receive_block &) override;
	void open_block (nano::open_block &) override;
	void change_block (nano::change_block &) override;
	void state_block (nano::state_block &) override;
	void state_block_impl (nano::state_block &);
	void epoch_block_impl (nano::state_block &);
	nano::ledger & ledger;
	nano::write_transaction const & transaction;
	nano::signature_verification verification;
	nano::process_return result;

private:
	bool validate_epoch_block (nano::state_block const & block_a);
};

// Returns true if this block which has an epoch link is correctly formed.
bool ledger_processor::validate_epoch_block (nano::state_block const & block_a)
{
	debug_assert (ledger.is_epoch_link (block_a.hashables.link));
	nano::amount prev_balance (0);
	if (!block_a.hashables.previous.is_zero ())
	{
		result.code = ledger.store.block.exists (transaction, block_a.hashables.previous) ? nano::process_result::progress : nano::process_result::gap_previous;
		if (result.code == nano::process_result::progress)
		{
			prev_balance = ledger.balance (transaction, block_a.hashables.previous);
		}
		else if (result.verified == nano::signature_verification::unknown)
		{
			// Check for possible regular state blocks with epoch link (send subtype)
			if (validate_message (block_a.hashables.account, block_a.hash (), block_a.signature))
			{
				// Is epoch block signed correctly
				if (validate_message (ledger.epoch_signer (block_a.link ()), block_a.hash (), block_a.signature))
				{
					result.verified = nano::signature_verification::invalid;
					result.code = nano::process_result::bad_signature;
				}
				else
				{
					result.verified = nano::signature_verification::valid_epoch;
				}
			}
			else
			{
				result.verified = nano::signature_verification::valid;
			}
		}
	}
	return (block_a.hashables.balance == prev_balance);
}

void ledger_processor::state_block (nano::state_block & block_a)
{
	result.code = nano::process_result::progress;
	auto is_epoch_block = false;
	if (ledger.is_epoch_link (block_a.hashables.link))
	{
		// This function also modifies the result variable if epoch is mal-formed
		is_epoch_block = validate_epoch_block (block_a);
	}

	if (result.code == nano::process_result::progress)
	{
		if (is_epoch_block)
		{
			epoch_block_impl (block_a);
		}
		else
		{
			state_block_impl (block_a);
		}
	}
}

void ledger_processor::state_block_impl (nano::state_block & block_a)
{
	auto hash (block_a.hash ());
	auto existing (ledger.block_or_pruned_exists (transaction, hash));
	result.code = existing ? nano::process_result::old : nano::process_result::progress; // Have we seen this block before? (Unambiguous)
	if (result.code == nano::process_result::progress)
	{
		// Validate block if not verified outside of ledger
		if (result.verified != nano::signature_verification::valid)
		{
			result.code = validate_message (block_a.hashables.account, hash, block_a.signature) ? nano::process_result::bad_signature : nano::process_result::progress; // Is this block signed correctly (Unambiguous)
		}
		if (result.code == nano::process_result::progress)
		{
			debug_assert (!validate_message (block_a.hashables.account, hash, block_a.signature));
			result.verified = nano::signature_verification::valid;
			result.code = block_a.hashables.account.is_zero () ? nano::process_result::opened_burn_account : nano::process_result::progress; // Is this for the burn account? (Unambiguous)
			if (result.code == nano::process_result::progress)
			{
				nano::epoch epoch (nano::epoch::epoch_0);
				nano::epoch source_epoch (nano::epoch::epoch_0);
				nano::account_info info;
				nano::amount amount (block_a.hashables.balance);
				auto is_send (false);
				auto is_receive (false);
				auto account_error (ledger.store.account.get (transaction, block_a.hashables.account, info));
				if (!account_error)
				{
					// Account already exists
					epoch = info.epoch ();
					result.previous_balance = info.balance;
					result.code = block_a.hashables.previous.is_zero () ? nano::process_result::fork : nano::process_result::progress; // Has this account already been opened? (Ambigious)
					if (result.code == nano::process_result::progress)
					{
						result.code = ledger.store.block.exists (transaction, block_a.hashables.previous) ? nano::process_result::progress : nano::process_result::gap_previous; // Does the previous block exist in the ledger? (Unambigious)
						if (result.code == nano::process_result::progress)
						{
							is_send = block_a.hashables.balance < info.balance;
							is_receive = !is_send && !block_a.hashables.link.is_zero ();
							amount = is_send ? (info.balance.number () - amount.number ()) : (amount.number () - info.balance.number ());
							result.code = block_a.hashables.previous == info.head ? nano::process_result::progress : nano::process_result::fork; // Is the previous block the account's head block? (Ambigious)
						}
					}
				}
				else
				{
					// Account does not yet exists
					result.previous_balance = 0;
					result.code = block_a.previous ().is_zero () ? nano::process_result::progress : nano::process_result::gap_previous; // Does the first block in an account yield 0 for previous() ? (Unambigious)
					if (result.code == nano::process_result::progress)
					{
						is_receive = true;
						result.code = !block_a.hashables.link.is_zero () ? nano::process_result::progress : nano::process_result::gap_source; // Is the first block receiving from a send ? (Unambigious)
					}
				}
				if (result.code == nano::process_result::progress)
				{
					if (!is_send)
					{
						if (!block_a.hashables.link.is_zero ())
						{
							result.code = ledger.block_or_pruned_exists (transaction, block_a.hashables.link.as_block_hash ()) ? nano::process_result::progress : nano::process_result::gap_source; // Have we seen the source block already? (Harmless)
							if (result.code == nano::process_result::progress)
							{
								nano::pending_key key (block_a.hashables.account, block_a.hashables.link.as_block_hash ());
								nano::pending_info pending;
								result.code = ledger.store.pending.get (transaction, key, pending) ? nano::process_result::unreceivable : nano::process_result::progress; // Has this source already been received (Malformed)
								if (result.code == nano::process_result::progress)
								{
									result.code = amount == pending.amount ? nano::process_result::progress : nano::process_result::balance_mismatch;
									source_epoch = pending.epoch;
									epoch = std::max (epoch, source_epoch);
								}
							}
						}
						else
						{
							// If there's no link, the balance must remain the same, only the representative can change
							result.code = amount.is_zero () ? nano::process_result::progress : nano::process_result::balance_mismatch;
						}
					}
				}
				if (result.code == nano::process_result::progress)
				{
					nano::block_details block_details (epoch, is_send, is_receive, false);
					result.code = block_a.difficulty () >= nano::work_threshold (block_a.work_version (), block_details) ? nano::process_result::progress : nano::process_result::insufficient_work; // Does this block have sufficient work? (Malformed)
					if (result.code == nano::process_result::progress)
					{
						ledger.stats.inc (nano::stat::type::ledger, nano::stat::detail::state_block);
						block_a.sideband_set (nano::block_sideband (block_a.hashables.account /* unused */, 0, 0 /* unused */, info.block_count + 1, nano::seconds_since_epoch (), block_details, source_epoch));
						ledger.store.block.put (transaction, hash, block_a);

						if (!info.head.is_zero ())
						{
							// Move existing representation & add in amount delta
							ledger.cache.rep_weights.representation_add_dual (info.representative, 0 - info.balance.number (), block_a.representative (), block_a.hashables.balance.number ());
						}
						else
						{
							// Add in amount delta only
							ledger.cache.rep_weights.representation_add (block_a.representative (), block_a.hashables.balance.number ());
						}

						if (is_send)
						{
							nano::pending_key key (block_a.hashables.link.as_account (), hash);
							nano::pending_info info (block_a.hashables.account, amount.number (), epoch);
							ledger.store.pending.put (transaction, key, info);
						}
						else if (!block_a.hashables.link.is_zero ())
						{
							ledger.store.pending.del (transaction, nano::pending_key (block_a.hashables.account, block_a.hashables.link.as_block_hash ()));
						}

						nano::account_info new_info (hash, block_a.representative (), info.open_block.is_zero () ? hash : info.open_block, block_a.hashables.balance, nano::seconds_since_epoch (), info.block_count + 1, epoch);
						ledger.update_account (transaction, block_a.hashables.account, info, new_info);
						if (!ledger.store.frontier.get (transaction, info.head).is_zero ())
						{
							ledger.store.frontier.del (transaction, info.head);
						}
					}
				}
			}
		}
	}
}

void ledger_processor::epoch_block_impl (nano::state_block & block_a)
{
	auto hash (block_a.hash ());
	auto existing (ledger.block_or_pruned_exists (transaction, hash));
	result.code = existing ? nano::process_result::old : nano::process_result::progress; // Have we seen this block before? (Unambiguous)
	if (result.code == nano::process_result::progress)
	{
		// Validate block if not verified outside of ledger
		if (result.verified != nano::signature_verification::valid_epoch)
		{
			result.code = validate_message (ledger.epoch_signer (block_a.hashables.link), hash, block_a.signature) ? nano::process_result::bad_signature : nano::process_result::progress; // Is this block signed correctly (Unambiguous)
		}
		if (result.code == nano::process_result::progress)
		{
			debug_assert (!validate_message (ledger.epoch_signer (block_a.hashables.link), hash, block_a.signature));
			result.verified = nano::signature_verification::valid_epoch;
			result.code = block_a.hashables.account.is_zero () ? nano::process_result::opened_burn_account : nano::process_result::progress; // Is this for the burn account? (Unambiguous)
			if (result.code == nano::process_result::progress)
			{
				nano::account_info info;
				auto account_error (ledger.store.account.get (transaction, block_a.hashables.account, info));
				if (!account_error)
				{
					// Account already exists
					result.previous_balance = info.balance;
					result.code = block_a.hashables.previous.is_zero () ? nano::process_result::fork : nano::process_result::progress; // Has this account already been opened? (Ambigious)
					if (result.code == nano::process_result::progress)
					{
						result.code = block_a.hashables.previous == info.head ? nano::process_result::progress : nano::process_result::fork; // Is the previous block the account's head block? (Ambigious)
						if (result.code == nano::process_result::progress)
						{
							result.code = block_a.hashables.representative == info.representative ? nano::process_result::progress : nano::process_result::representative_mismatch;
						}
					}
				}
				else
				{
					result.previous_balance = 0;
					result.code = block_a.hashables.representative.is_zero () ? nano::process_result::progress : nano::process_result::representative_mismatch;
					// Non-exisitng account should have pending entries
					if (result.code == nano::process_result::progress)
					{
						bool pending_exists = ledger.store.pending.any (transaction, block_a.hashables.account);
						result.code = pending_exists ? nano::process_result::progress : nano::process_result::gap_epoch_open_pending;
					}
				}
				if (result.code == nano::process_result::progress)
				{
					auto epoch = ledger.network_params.ledger.epochs.epoch (block_a.hashables.link);
					// Must be an epoch for an unopened account or the epoch upgrade must be sequential
					auto is_valid_epoch_upgrade = account_error ? static_cast<std::underlying_type_t<nano::epoch>> (epoch) > 0 : nano::epochs::is_sequential (info.epoch (), epoch);
					result.code = is_valid_epoch_upgrade ? nano::process_result::progress : nano::process_result::block_position;
					if (result.code == nano::process_result::progress)
					{
						result.code = block_a.hashables.balance == info.balance ? nano::process_result::progress : nano::process_result::balance_mismatch;
						if (result.code == nano::process_result::progress)
						{
							nano::block_details block_details (epoch, false, false, true);
							result.code = block_a.difficulty () >= nano::work_threshold (block_a.work_version (), block_details) ? nano::process_result::progress : nano::process_result::insufficient_work; // Does this block have sufficient work? (Malformed)
							if (result.code == nano::process_result::progress)
							{
								ledger.stats.inc (nano::stat::type::ledger, nano::stat::detail::epoch_block);
								block_a.sideband_set (nano::block_sideband (block_a.hashables.account /* unused */, 0, 0 /* unused */, info.block_count + 1, nano::seconds_since_epoch (), block_details, nano::epoch::epoch_0 /* unused */));
								ledger.store.block.put (transaction, hash, block_a);
								nano::account_info new_info (hash, block_a.representative (), info.open_block.is_zero () ? hash : info.open_block, info.balance, nano::seconds_since_epoch (), info.block_count + 1, epoch);
								ledger.update_account (transaction, block_a.hashables.account, info, new_info);
								if (!ledger.store.frontier.get (transaction, info.head).is_zero ())
								{
									ledger.store.frontier.del (transaction, info.head);
								}
							}
						}
					}
				}
			}
		}
	}
}

void ledger_processor::change_block (nano::change_block & block_a)
{
	auto hash (block_a.hash ());
	auto existing (ledger.block_or_pruned_exists (transaction, hash));
	result.code = existing ? nano::process_result::old : nano::process_result::progress; // Have we seen this block before? (Harmless)
	if (result.code == nano::process_result::progress)
	{
		auto previous (ledger.store.block.get (transaction, block_a.hashables.previous));
		result.code = previous != nullptr ? nano::process_result::progress : nano::process_result::gap_previous; // Have we seen the previous block already? (Harmless)
		if (result.code == nano::process_result::progress)
		{
			result.code = block_a.valid_predecessor (*previous) ? nano::process_result::progress : nano::process_result::block_position;
			if (result.code == nano::process_result::progress)
			{
				auto account (ledger.store.frontier.get (transaction, block_a.hashables.previous));
				result.code = account.is_zero () ? nano::process_result::fork : nano::process_result::progress;
				if (result.code == nano::process_result::progress)
				{
					nano::account_info info;
					auto latest_error (ledger.store.account.get (transaction, account, info));
					(void)latest_error;
					debug_assert (!latest_error);
					debug_assert (info.head == block_a.hashables.previous);
					// Validate block if not verified outside of ledger
					if (result.verified != nano::signature_verification::valid)
					{
						result.code = validate_message (account, hash, block_a.signature) ? nano::process_result::bad_signature : nano::process_result::progress; // Is this block signed correctly (Malformed)
					}
					if (result.code == nano::process_result::progress)
					{
						nano::block_details block_details (nano::epoch::epoch_0, false /* unused */, false /* unused */, false /* unused */);
						result.code = block_a.difficulty () >= nano::work_threshold (block_a.work_version (), block_details) ? nano::process_result::progress : nano::process_result::insufficient_work; // Does this block have sufficient work? (Malformed)
						if (result.code == nano::process_result::progress)
						{
							debug_assert (!validate_message (account, hash, block_a.signature));
							result.verified = nano::signature_verification::valid;
							block_a.sideband_set (nano::block_sideband (account, 0, info.balance, info.block_count + 1, nano::seconds_since_epoch (), block_details, nano::epoch::epoch_0 /* unused */));
							ledger.store.block.put (transaction, hash, block_a);
							auto balance (ledger.balance (transaction, block_a.hashables.previous));
							ledger.cache.rep_weights.representation_add_dual (block_a.representative (), balance, info.representative, 0 - balance);
							nano::account_info new_info (hash, block_a.representative (), info.open_block, info.balance, nano::seconds_since_epoch (), info.block_count + 1, nano::epoch::epoch_0);
							ledger.update_account (transaction, account, info, new_info);
							ledger.store.frontier.del (transaction, block_a.hashables.previous);
							ledger.store.frontier.put (transaction, hash, account);
							result.previous_balance = info.balance;
							ledger.stats.inc (nano::stat::type::ledger, nano::stat::detail::change);
						}
					}
				}
			}
		}
	}
}

void ledger_processor::send_block (nano::send_block & block_a)
{
	auto hash (block_a.hash ());
	auto existing (ledger.block_or_pruned_exists (transaction, hash));
	result.code = existing ? nano::process_result::old : nano::process_result::progress; // Have we seen this block before? (Harmless)
	if (result.code == nano::process_result::progress)
	{
		auto previous (ledger.store.block.get (transaction, block_a.hashables.previous));
		result.code = previous != nullptr ? nano::process_result::progress : nano::process_result::gap_previous; // Have we seen the previous block already? (Harmless)
		if (result.code == nano::process_result::progress)
		{
			result.code = block_a.valid_predecessor (*previous) ? nano::process_result::progress : nano::process_result::block_position;
			if (result.code == nano::process_result::progress)
			{
				auto account (ledger.store.frontier.get (transaction, block_a.hashables.previous));
				result.code = account.is_zero () ? nano::process_result::fork : nano::process_result::progress;
				if (result.code == nano::process_result::progress)
				{
					// Validate block if not verified outside of ledger
					if (result.verified != nano::signature_verification::valid)
					{
						result.code = validate_message (account, hash, block_a.signature) ? nano::process_result::bad_signature : nano::process_result::progress; // Is this block signed correctly (Malformed)
					}
					if (result.code == nano::process_result::progress)
					{
						nano::block_details block_details (nano::epoch::epoch_0, false /* unused */, false /* unused */, false /* unused */);
						result.code = block_a.difficulty () >= nano::work_threshold (block_a.work_version (), block_details) ? nano::process_result::progress : nano::process_result::insufficient_work; // Does this block have sufficient work? (Malformed)
						if (result.code == nano::process_result::progress)
						{
							debug_assert (!validate_message (account, hash, block_a.signature));
							result.verified = nano::signature_verification::valid;
							nano::account_info info;
							auto latest_error (ledger.store.account.get (transaction, account, info));
							(void)latest_error;
							debug_assert (!latest_error);
							debug_assert (info.head == block_a.hashables.previous);
							result.code = info.balance.number () >= block_a.hashables.balance.number () ? nano::process_result::progress : nano::process_result::negative_spend; // Is this trying to spend a negative amount (Malicious)
							if (result.code == nano::process_result::progress)
							{
								auto amount (info.balance.number () - block_a.hashables.balance.number ());
								ledger.cache.rep_weights.representation_add (info.representative, 0 - amount);
								block_a.sideband_set (nano::block_sideband (account, 0, block_a.hashables.balance /* unused */, info.block_count + 1, nano::seconds_since_epoch (), block_details, nano::epoch::epoch_0 /* unused */));
								ledger.store.block.put (transaction, hash, block_a);
								nano::account_info new_info (hash, info.representative, info.open_block, block_a.hashables.balance, nano::seconds_since_epoch (), info.block_count + 1, nano::epoch::epoch_0);
								ledger.update_account (transaction, account, info, new_info);
								ledger.store.pending.put (transaction, nano::pending_key (block_a.hashables.destination, hash), { account, amount, nano::epoch::epoch_0 });
								ledger.store.frontier.del (transaction, block_a.hashables.previous);
								ledger.store.frontier.put (transaction, hash, account);
								result.previous_balance = info.balance;
								ledger.stats.inc (nano::stat::type::ledger, nano::stat::detail::send);
							}
						}
					}
				}
			}
		}
	}
}

void ledger_processor::receive_block (nano::receive_block & block_a)
{
	auto hash (block_a.hash ());
	auto existing (ledger.block_or_pruned_exists (transaction, hash));
	result.code = existing ? nano::process_result::old : nano::process_result::progress; // Have we seen this block already?  (Harmless)
	if (result.code == nano::process_result::progress)
	{
		auto previous (ledger.store.block.get (transaction, block_a.hashables.previous));
		result.code = previous != nullptr ? nano::process_result::progress : nano::process_result::gap_previous;
		if (result.code == nano::process_result::progress)
		{
			result.code = block_a.valid_predecessor (*previous) ? nano::process_result::progress : nano::process_result::block_position;
			if (result.code == nano::process_result::progress)
			{
				auto account (ledger.store.frontier.get (transaction, block_a.hashables.previous));
				result.code = account.is_zero () ? nano::process_result::gap_previous : nano::process_result::progress; //Have we seen the previous block? No entries for account at all (Harmless)
				if (result.code == nano::process_result::progress)
				{
					// Validate block if not verified outside of ledger
					if (result.verified != nano::signature_verification::valid)
					{
						result.code = validate_message (account, hash, block_a.signature) ? nano::process_result::bad_signature : nano::process_result::progress; // Is the signature valid (Malformed)
					}
					if (result.code == nano::process_result::progress)
					{
						debug_assert (!validate_message (account, hash, block_a.signature));
						result.verified = nano::signature_verification::valid;
						result.code = ledger.block_or_pruned_exists (transaction, block_a.hashables.source) ? nano::process_result::progress : nano::process_result::gap_source; // Have we seen the source block already? (Harmless)
						if (result.code == nano::process_result::progress)
						{
							nano::account_info info;
							ledger.store.account.get (transaction, account, info);
							result.code = info.head == block_a.hashables.previous ? nano::process_result::progress : nano::process_result::gap_previous; // Block doesn't immediately follow latest block (Harmless)
							if (result.code == nano::process_result::progress)
							{
								nano::pending_key key (account, block_a.hashables.source);
								nano::pending_info pending;
								result.code = ledger.store.pending.get (transaction, key, pending) ? nano::process_result::unreceivable : nano::process_result::progress; // Has this source already been received (Malformed)
								if (result.code == nano::process_result::progress)
								{
									result.code = pending.epoch == nano::epoch::epoch_0 ? nano::process_result::progress : nano::process_result::unreceivable; // Are we receiving a state-only send? (Malformed)
									if (result.code == nano::process_result::progress)
									{
										nano::block_details block_details (nano::epoch::epoch_0, false /* unused */, false /* unused */, false /* unused */);
										result.code = block_a.difficulty () >= nano::work_threshold (block_a.work_version (), block_details) ? nano::process_result::progress : nano::process_result::insufficient_work; // Does this block have sufficient work? (Malformed)
										if (result.code == nano::process_result::progress)
										{
											auto new_balance (info.balance.number () + pending.amount.number ());
#ifdef NDEBUG
											if (ledger.store.block.exists (transaction, block_a.hashables.source))
											{
												nano::account_info source_info;
												[[maybe_unused]] auto error (ledger.store.account.get (transaction, pending.source, source_info));
												debug_assert (!error);
											}
#endif
											ledger.store.pending.del (transaction, key);
											block_a.sideband_set (nano::block_sideband (account, 0, new_balance, info.block_count + 1, nano::seconds_since_epoch (), block_details, nano::epoch::epoch_0 /* unused */));
											ledger.store.block.put (transaction, hash, block_a);
											nano::account_info new_info (hash, info.representative, info.open_block, new_balance, nano::seconds_since_epoch (), info.block_count + 1, nano::epoch::epoch_0);
											ledger.update_account (transaction, account, info, new_info);
											ledger.cache.rep_weights.representation_add (info.representative, pending.amount.number ());
											ledger.store.frontier.del (transaction, block_a.hashables.previous);
											ledger.store.frontier.put (transaction, hash, account);
											result.previous_balance = info.balance;
											ledger.stats.inc (nano::stat::type::ledger, nano::stat::detail::receive);
										}
									}
								}
							}
						}
					}
				}
				else
				{
					result.code = ledger.store.block.exists (transaction, block_a.hashables.previous) ? nano::process_result::fork : nano::process_result::gap_previous; // If we have the block but it's not the latest we have a signed fork (Malicious)
				}
			}
		}
	}
}

void ledger_processor::open_block (nano::open_block & block_a)
{
	auto hash (block_a.hash ());
	auto existing (ledger.block_or_pruned_exists (transaction, hash));
	result.code = existing ? nano::process_result::old : nano::process_result::progress; // Have we seen this block already? (Harmless)
	if (result.code == nano::process_result::progress)
	{
		// Validate block if not verified outside of ledger
		if (result.verified != nano::signature_verification::valid)
		{
			result.code = validate_message (block_a.hashables.account, hash, block_a.signature) ? nano::process_result::bad_signature : nano::process_result::progress; // Is the signature valid (Malformed)
		}
		if (result.code == nano::process_result::progress)
		{
			debug_assert (!validate_message (block_a.hashables.account, hash, block_a.signature));
			result.verified = nano::signature_verification::valid;
			result.code = ledger.block_or_pruned_exists (transaction, block_a.hashables.source) ? nano::process_result::progress : nano::process_result::gap_source; // Have we seen the source block? (Harmless)
			if (result.code == nano::process_result::progress)
			{
				nano::account_info info;
				result.code = ledger.store.account.get (transaction, block_a.hashables.account, info) ? nano::process_result::progress : nano::process_result::fork; // Has this account already been opened? (Malicious)
				if (result.code == nano::process_result::progress)
				{
					nano::pending_key key (block_a.hashables.account, block_a.hashables.source);
					nano::pending_info pending;
					result.code = ledger.store.pending.get (transaction, key, pending) ? nano::process_result::unreceivable : nano::process_result::progress; // Has this source already been received (Malformed)
					if (result.code == nano::process_result::progress)
					{
						result.code = block_a.hashables.account == ledger.network_params.ledger.burn_account ? nano::process_result::opened_burn_account : nano::process_result::progress; // Is it burning 0 account? (Malicious)
						if (result.code == nano::process_result::progress)
						{
							result.code = pending.epoch == nano::epoch::epoch_0 ? nano::process_result::progress : nano::process_result::unreceivable; // Are we receiving a state-only send? (Malformed)
							if (result.code == nano::process_result::progress)
							{
								nano::block_details block_details (nano::epoch::epoch_0, false /* unused */, false /* unused */, false /* unused */);
								result.code = block_a.difficulty () >= nano::work_threshold (block_a.work_version (), block_details) ? nano::process_result::progress : nano::process_result::insufficient_work; // Does this block have sufficient work? (Malformed)
								if (result.code == nano::process_result::progress)
								{
#ifdef NDEBUG
									if (ledger.store.block.exists (transaction, block_a.hashables.source))
									{
										nano::account_info source_info;
										[[maybe_unused]] auto error (ledger.store.account.get (transaction, pending.source, source_info));
										debug_assert (!error);
									}
#endif
									ledger.store.pending.del (transaction, key);
									block_a.sideband_set (nano::block_sideband (block_a.hashables.account, 0, pending.amount, 1, nano::seconds_since_epoch (), block_details, nano::epoch::epoch_0 /* unused */));
									ledger.store.block.put (transaction, hash, block_a);
									nano::account_info new_info (hash, block_a.representative (), hash, pending.amount.number (), nano::seconds_since_epoch (), 1, nano::epoch::epoch_0);
									ledger.update_account (transaction, block_a.hashables.account, info, new_info);
									ledger.cache.rep_weights.representation_add (block_a.representative (), pending.amount.number ());
									ledger.store.frontier.put (transaction, hash, block_a.hashables.account);
									result.previous_balance = 0;
									ledger.stats.inc (nano::stat::type::ledger, nano::stat::detail::open);
								}
							}
						}
					}
				}
			}
		}
	}
}

ledger_processor::ledger_processor (nano::ledger & ledger_a, nano::write_transaction const & transaction_a, nano::signature_verification verification_a) :
	ledger (ledger_a),
	transaction (transaction_a),
	verification (verification_a)
{
	result.verified = verification;
}
} // namespace

nano::ledger::ledger (nano::store & store_a, nano::stat & stat_a, nano::generate_cache const & generate_cache_a) :
	store (store_a),
	stats (stat_a),
	check_bootstrap_weights (true)
{
	if (!store.init_error ())
	{
		initialize (generate_cache_a);
	}
}

void nano::ledger::initialize (nano::generate_cache const & generate_cache_a)
{
	if (generate_cache_a.reps || generate_cache_a.account_count || generate_cache_a.block_count)
	{
		store.account.for_each_par (
		[this] (nano::read_transaction const & /*unused*/, nano::store_iterator<nano::account, nano::account_info> i, nano::store_iterator<nano::account, nano::account_info> n) {
			uint64_t block_count_l{ 0 };
			uint64_t account_count_l{ 0 };
			decltype (this->cache.rep_weights) rep_weights_l;
			for (; i != n; ++i)
			{
				nano::account_info const & info (i->second);
				block_count_l += info.block_count;
				++account_count_l;
				rep_weights_l.representation_add (info.representative, info.balance.number ());
			}
			this->cache.block_count += block_count_l;
			this->cache.account_count += account_count_l;
			this->cache.rep_weights.copy_from (rep_weights_l);
		});
	}

	if (generate_cache_a.cemented_count)
	{
		store.confirmation_height.for_each_par (
		[this] (nano::read_transaction const & /*unused*/, nano::store_iterator<nano::account, nano::confirmation_height_info> i, nano::store_iterator<nano::account, nano::confirmation_height_info> n) {
			uint64_t cemented_count_l (0);
			for (; i != n; ++i)
			{
				cemented_count_l += i->second.height;
			}
			this->cache.cemented_count += cemented_count_l;
		});
	}

	auto transaction (store.tx_begin_read ());
	cache.pruned_count = store.pruned.count (transaction);

	// Final votes requirement for confirmation canary block
	nano::confirmation_height_info confirmation_height_info;
	if (!store.confirmation_height.get (transaction, network_params.ledger.final_votes_canary_account, confirmation_height_info))
	{
		cache.final_votes_confirmation_canary = (confirmation_height_info.height >= network_params.ledger.final_votes_canary_height);
	}
}

// Balance for account containing hash
nano::uint128_t nano::ledger::balance (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const
{
	return hash_a.is_zero () ? 0 : store.block.balance (transaction_a, hash_a);
}

nano::uint128_t nano::ledger::balance_safe (nano::transaction const & transaction_a, nano::block_hash const & hash_a, bool & error_a) const
{
	nano::uint128_t result (0);
	if (pruning && !hash_a.is_zero () && !store.block.exists (transaction_a, hash_a))
	{
		std::cerr << "error\n";
		error_a = true;
		result = 0;
	}
	else
	{
		result = balance (transaction_a, hash_a);
	}
	return result;
}

// Balance for an account by account number
nano::uint128_t nano::ledger::account_balance (nano::transaction const & transaction_a, nano::account const & account_a, bool only_confirmed_a)
{
	nano::uint128_t result (0);
	if (only_confirmed_a)
	{
		nano::confirmation_height_info info;
		if (!store.confirmation_height.get (transaction_a, account_a, info))
		{
			result = balance (transaction_a, info.frontier);
		}
	}
	else
	{
		nano::account_info info;
		auto none (store.account.get (transaction_a, account_a, info));
		if (!none)
		{
			result = info.balance.number ();
		}
	}
	return result;
}

nano::uint128_t nano::ledger::account_pending (nano::transaction const & transaction_a, nano::account const & account_a, bool only_confirmed_a)
{
	nano::uint128_t result (0);
	nano::account end (account_a.number () + 1);
	for (auto i (store.pending.begin (transaction_a, nano::pending_key (account_a, 0))), n (store.pending.begin (transaction_a, nano::pending_key (end, 0))); i != n; ++i)
	{
		nano::pending_info const & info (i->second);
		if (only_confirmed_a)
		{
			if (block_confirmed (transaction_a, i->first.hash))
			{
				result += info.amount.number ();
			}
		}
		else
		{
			result += info.amount.number ();
		}
	}
	return result;
}

nano::process_return nano::ledger::process (nano::write_transaction const & transaction_a, nano::block & block_a, nano::signature_verification verification)
{
	debug_assert (!nano::work_validate_entry (block_a) || network_params.network.is_dev_network ());
	ledger_processor processor (*this, transaction_a, verification);
	block_a.visit (processor);
	if (processor.result.code == nano::process_result::progress)
	{
		++cache.block_count;
	}
	return processor.result;
}

nano::block_hash nano::ledger::representative (nano::transaction const & transaction_a, nano::block_hash const & hash_a)
{
	auto result (representative_calculated (transaction_a, hash_a));
	debug_assert (result.is_zero () || store.block.exists (transaction_a, result));
	return result;
}

nano::block_hash nano::ledger::representative_calculated (nano::transaction const & transaction_a, nano::block_hash const & hash_a)
{
	representative_visitor visitor (transaction_a, store);
	visitor.compute (hash_a);
	return visitor.result;
}

bool nano::ledger::block_or_pruned_exists (nano::block_hash const & hash_a) const
{
	return block_or_pruned_exists (store.tx_begin_read (), hash_a);
}

bool nano::ledger::block_or_pruned_exists (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const
{
	if (store.pruned.exists (transaction_a, hash_a))
	{
		return true;
	}
	return store.block.exists (transaction_a, hash_a);
}

std::string nano::ledger::block_text (char const * hash_a)
{
	return block_text (nano::block_hash (hash_a));
}

std::string nano::ledger::block_text (nano::block_hash const & hash_a)
{
	std::string result;
	auto transaction (store.tx_begin_read ());
	auto block (store.block.get (transaction, hash_a));
	if (block != nullptr)
	{
		block->serialize_json (result);
	}
	return result;
}

bool nano::ledger::is_send (nano::transaction const & transaction_a, nano::state_block const & block_a) const
{
	/*
	 * if block_a does not have a sideband, then is_send()
	 * requires that the previous block exists in the database.
	 * This is because it must retrieve the balance of the previous block.
	 */
	debug_assert (block_a.has_sideband () || block_a.hashables.previous.is_zero () || store.block.exists (transaction_a, block_a.hashables.previous));

	bool result (false);
	if (block_a.has_sideband ())
	{
		result = block_a.sideband ().details.is_send;
	}
	else
	{
		nano::block_hash previous (block_a.hashables.previous);
		if (!previous.is_zero ())
		{
			if (block_a.hashables.balance < balance (transaction_a, previous))
			{
				result = true;
			}
		}
	}
	return result;
}

nano::account const & nano::ledger::block_destination (nano::transaction const & transaction_a, nano::block const & block_a)
{
	nano::send_block const * send_block (dynamic_cast<nano::send_block const *> (&block_a));
	nano::state_block const * state_block (dynamic_cast<nano::state_block const *> (&block_a));
	if (send_block != nullptr)
	{
		return send_block->hashables.destination;
	}
	else if (state_block != nullptr && is_send (transaction_a, *state_block))
	{
		return state_block->hashables.link.as_account ();
	}
	static nano::account result (0);
	return result;
}

nano::block_hash nano::ledger::block_source (nano::transaction const & transaction_a, nano::block const & block_a)
{
	/*
	 * block_source() requires that the previous block of the block
	 * passed in exist in the database.  This is because it will try
	 * to check account balances to determine if it is a send block.
	 */
	debug_assert (block_a.previous ().is_zero () || store.block.exists (transaction_a, block_a.previous ()));

	// If block_a.source () is nonzero, then we have our source.
	// However, universal blocks will always return zero.
	nano::block_hash result (block_a.source ());
	nano::state_block const * state_block (dynamic_cast<nano::state_block const *> (&block_a));
	if (state_block != nullptr && !is_send (transaction_a, *state_block))
	{
		result = state_block->hashables.link.as_block_hash ();
	}
	return result;
}

std::pair<nano::block_hash, nano::block_hash> nano::ledger::hash_root_random (nano::transaction const & transaction_a) const
{
	nano::block_hash hash (0);
	nano::root root (0);
	if (!pruning)
	{
		auto block (store.block.random (transaction_a));
		hash = block->hash ();
		root = block->root ();
	}
	else
	{
		uint64_t count (cache.block_count);
		release_assert (std::numeric_limits<CryptoPP::word32>::max () > count);
		auto region = static_cast<size_t> (nano::random_pool::generate_word32 (0, static_cast<CryptoPP::word32> (count - 1)));
		// Pruned cache cannot guarantee that pruned blocks are already commited
		if (region < cache.pruned_count)
		{
			hash = store.pruned.random (transaction_a);
		}
		if (hash.is_zero ())
		{
			auto block (store.block.random (transaction_a));
			hash = block->hash ();
			root = block->root ();
		}
	}
	return std::make_pair (hash, root.as_block_hash ());
}

// Vote weight of an account
nano::uint128_t nano::ledger::weight (nano::account const & account_a)
{
	if (check_bootstrap_weights.load ())
	{
		if (cache.block_count < bootstrap_weight_max_blocks)
		{
			auto weight = bootstrap_weights.find (account_a);
			if (weight != bootstrap_weights.end ())
			{
				return weight->second;
			}
		}
		else
		{
			check_bootstrap_weights = false;
		}
	}
	return cache.rep_weights.representation_get (account_a);
}

// Rollback blocks until `block_a' doesn't exist or it tries to penetrate the confirmation height
bool nano::ledger::rollback (nano::write_transaction const & transaction_a, nano::block_hash const & block_a, std::vector<std::shared_ptr<nano::block>> & list_a)
{
	debug_assert (store.block.exists (transaction_a, block_a));
	auto account_l (account (transaction_a, block_a));
	auto block_account_height (store.block.account_height (transaction_a, block_a));
	rollback_visitor rollback (transaction_a, *this, list_a);
	nano::account_info account_info;
	auto error (false);
	while (!error && store.block.exists (transaction_a, block_a))
	{
		nano::confirmation_height_info confirmation_height_info;
		store.confirmation_height.get (transaction_a, account_l, confirmation_height_info);
		if (block_account_height > confirmation_height_info.height)
		{
			auto latest_error = store.account.get (transaction_a, account_l, account_info);
			debug_assert (!latest_error);
			auto block (store.block.get (transaction_a, account_info.head));
			list_a.push_back (block);
			block->visit (rollback);
			error = rollback.error;
			if (!error)
			{
				--cache.block_count;
			}
		}
		else
		{
			error = true;
		}
	}
	return error;
}

bool nano::ledger::rollback (nano::write_transaction const & transaction_a, nano::block_hash const & block_a)
{
	std::vector<std::shared_ptr<nano::block>> rollback_list;
	return rollback (transaction_a, block_a, rollback_list);
}

// Return account containing hash
nano::account nano::ledger::account (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const
{
	return store.block.account (transaction_a, hash_a);
}

nano::account nano::ledger::account_safe (nano::transaction const & transaction_a, nano::block_hash const & hash_a, bool & error_a) const
{
	if (!pruning)
	{
		return store.block.account (transaction_a, hash_a);
	}
	else
	{
		auto block (store.block.get (transaction_a, hash_a));
		if (block != nullptr)
		{
			return store.block.account_calculated (*block);
		}
		else
		{
			error_a = true;
			return 0;
		}
	}
}

// Return amount decrease or increase for block
nano::uint128_t nano::ledger::amount (nano::transaction const & transaction_a, nano::account const & account_a)
{
	release_assert (account_a == network_params.ledger.genesis_account ());
	return nano::dev::genesis_amount;
}

nano::uint128_t nano::ledger::amount (nano::transaction const & transaction_a, nano::block_hash const & hash_a)
{
	auto block (store.block.get (transaction_a, hash_a));
	auto block_balance (balance (transaction_a, hash_a));
	auto previous_balance (balance (transaction_a, block->previous ()));
	return block_balance > previous_balance ? block_balance - previous_balance : previous_balance - block_balance;
}

nano::uint128_t nano::ledger::amount_safe (nano::transaction const & transaction_a, nano::block_hash const & hash_a, bool & error_a) const
{
	auto block (store.block.get (transaction_a, hash_a));
	debug_assert (block);
	auto block_balance (balance (transaction_a, hash_a));
	auto previous_balance (balance_safe (transaction_a, block->previous (), error_a));
	return error_a ? 0 : block_balance > previous_balance ? block_balance - previous_balance : previous_balance - block_balance;
}

// Return latest block for account
nano::block_hash nano::ledger::latest (nano::transaction const & transaction_a, nano::account const & account_a)
{
	nano::account_info info;
	auto latest_error (store.account.get (transaction_a, account_a, info));
	return latest_error ? 0 : info.head;
}

// Return latest root for account, account number if there are no blocks for this account.
nano::root nano::ledger::latest_root (nano::transaction const & transaction_a, nano::account const & account_a)
{
	nano::account_info info;
	if (store.account.get (transaction_a, account_a, info))
	{
		return account_a;
	}
	else
	{
		return info.head;
	}
}

void nano::ledger::dump_account_chain (nano::account const & account_a, std::ostream & stream)
{
	auto transaction (store.tx_begin_read ());
	auto hash (latest (transaction, account_a));
	while (!hash.is_zero ())
	{
		auto block (store.block.get (transaction, hash));
		debug_assert (block != nullptr);
		stream << hash.to_string () << std::endl;
		hash = block->previous ();
	}
}

bool nano::ledger::could_fit (nano::transaction const & transaction_a, nano::block const & block_a) const
{
	auto dependencies (dependent_blocks (transaction_a, block_a));
	return std::all_of (dependencies.begin (), dependencies.end (), [this, &transaction_a] (nano::block_hash const & hash_a) {
		return hash_a.is_zero () || store.block.exists (transaction_a, hash_a);
	});
}

bool nano::ledger::dependents_confirmed (nano::transaction const & transaction_a, nano::block const & block_a) const
{
	auto dependencies (dependent_blocks (transaction_a, block_a));
	return std::all_of (dependencies.begin (), dependencies.end (), [this, &transaction_a] (nano::block_hash const & hash_a) {
		auto result (hash_a.is_zero ());
		if (!result)
		{
			result = block_confirmed (transaction_a, hash_a);
		}
		return result;
	});
}

bool nano::ledger::is_epoch_link (nano::link const & link_a) const
{
	return network_params.ledger.epochs.is_epoch_link (link_a);
}

class dependent_block_visitor : public nano::block_visitor
{
public:
	dependent_block_visitor (nano::ledger const & ledger_a, nano::transaction const & transaction_a) :
		ledger (ledger_a),
		transaction (transaction_a),
		result ({ 0, 0 })
	{
	}
	void send_block (nano::send_block const & block_a) override
	{
		result[0] = block_a.previous ();
	}
	void receive_block (nano::receive_block const & block_a) override
	{
		result[0] = block_a.previous ();
		result[1] = block_a.source ();
	}
	void open_block (nano::open_block const & block_a) override
	{
		if (block_a.source () != ledger.network_params.ledger.genesis_account ())
		{
			result[0] = block_a.source ();
		}
	}
	void change_block (nano::change_block const & block_a) override
	{
		result[0] = block_a.previous ();
	}
	void state_block (nano::state_block const & block_a) override
	{
		result[0] = block_a.hashables.previous;
		result[1] = block_a.hashables.link.as_block_hash ();
		// ledger.is_send will check the sideband first, if block_a has a loaded sideband the check that previous block exists can be skipped
		if (ledger.is_epoch_link (block_a.hashables.link) || ((block_a.has_sideband () || ledger.store.block.exists (transaction, block_a.hashables.previous)) && ledger.is_send (transaction, block_a)))
		{
			result[1].clear ();
		}
	}
	nano::ledger const & ledger;
	nano::transaction const & transaction;
	std::array<nano::block_hash, 2> result;
};

std::array<nano::block_hash, 2> nano::ledger::dependent_blocks (nano::transaction const & transaction_a, nano::block const & block_a) const
{
	dependent_block_visitor visitor (*this, transaction_a);
	block_a.visit (visitor);
	return visitor.result;
}

nano::account const & nano::ledger::epoch_signer (nano::link const & link_a) const
{
	return network_params.ledger.epochs.signer (network_params.ledger.epochs.epoch (link_a));
}

nano::link const & nano::ledger::epoch_link (nano::epoch epoch_a) const
{
	return network_params.ledger.epochs.link (epoch_a);
}

void nano::ledger::update_account (nano::write_transaction const & transaction_a, nano::account const & account_a, nano::account_info const & old_a, nano::account_info const & new_a)
{
	if (!new_a.head.is_zero ())
	{
		if (old_a.head.is_zero () && new_a.open_block == new_a.head)
		{
			++cache.account_count;
		}
		if (!old_a.head.is_zero () && old_a.epoch () != new_a.epoch ())
		{
			// store.account.put won't erase existing entries if they're in different tables
			store.account.del (transaction_a, account_a);
		}
		store.account.put (transaction_a, account_a, new_a);
	}
	else
	{
		debug_assert (!store.confirmation_height.exists (transaction_a, account_a));
		store.account.del (transaction_a, account_a);
		debug_assert (cache.account_count > 0);
		--cache.account_count;
	}
}

std::shared_ptr<nano::block> nano::ledger::successor (nano::transaction const & transaction_a, nano::qualified_root const & root_a)
{
	nano::block_hash successor (0);
	auto get_from_previous = false;
	if (root_a.previous ().is_zero ())
	{
		nano::account_info info;
		if (!store.account.get (transaction_a, root_a.root ().as_account (), info))
		{
			successor = info.open_block;
		}
		else
		{
			get_from_previous = true;
		}
	}
	else
	{
		get_from_previous = true;
	}

	if (get_from_previous)
	{
		successor = store.block.successor (transaction_a, root_a.previous ());
	}
	std::shared_ptr<nano::block> result;
	if (!successor.is_zero ())
	{
		result = store.block.get (transaction_a, successor);
	}
	debug_assert (successor.is_zero () || result != nullptr);
	return result;
}

std::shared_ptr<nano::block> nano::ledger::forked_block (nano::transaction const & transaction_a, nano::block const & block_a)
{
	debug_assert (!store.block.exists (transaction_a, block_a.hash ()));
	auto root (block_a.root ());
	debug_assert (store.block.exists (transaction_a, root.as_block_hash ()) || store.account.exists (transaction_a, root.as_account ()));
	auto result (store.block.get (transaction_a, store.block.successor (transaction_a, root.as_block_hash ())));
	if (result == nullptr)
	{
		nano::account_info info;
		auto error (store.account.get (transaction_a, root.as_account (), info));
		(void)error;
		debug_assert (!error);
		result = store.block.get (transaction_a, info.open_block);
		debug_assert (result != nullptr);
	}
	return result;
}

bool nano::ledger::block_confirmed (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const
{
	if (store.pruned.exists (transaction_a, hash_a))
	{
		return true;
	}
	auto block = store.block.get (transaction_a, hash_a);
	if (block)
	{
		nano::confirmation_height_info confirmation_height_info;
		store.confirmation_height.get (transaction_a, block->account ().is_zero () ? block->sideband ().account : block->account (), confirmation_height_info);
		auto confirmed (confirmation_height_info.height >= block->sideband ().height);
		return confirmed;
	}
	return false;
}

uint64_t nano::ledger::pruning_action (nano::write_transaction & transaction_a, nano::block_hash const & hash_a, uint64_t const batch_size_a)
{
	uint64_t pruned_count (0);
	nano::block_hash hash (hash_a);
	while (!hash.is_zero () && hash != network_params.ledger.genesis_hash ())
	{
		auto block (store.block.get (transaction_a, hash));
		if (block != nullptr)
		{
			store.block.del (transaction_a, hash);
			store.pruned.put (transaction_a, hash);
			hash = block->previous ();
			++pruned_count;
			++cache.pruned_count;
			if (pruned_count % batch_size_a == 0)
			{
				transaction_a.commit ();
				transaction_a.renew ();
			}
		}
		else if (store.pruned.exists (transaction_a, hash))
		{
			hash = 0;
		}
		else
		{
			hash = 0;
			release_assert (false && "Error finding block for pruning");
		}
	}
	return pruned_count;
}

std::multimap<uint64_t, nano::uncemented_info, std::greater<>> nano::ledger::unconfirmed_frontiers () const
{
	nano::locked<std::multimap<uint64_t, nano::uncemented_info, std::greater<>>> result;
	using result_t = decltype (result)::value_type;

	store.account.for_each_par ([this, &result] (nano::read_transaction const & transaction_a, nano::store_iterator<nano::account, nano::account_info> i, nano::store_iterator<nano::account, nano::account_info> n) {
		result_t unconfirmed_frontiers_l;
		for (; i != n; ++i)
		{
			auto const & account (i->first);
			auto const & account_info (i->second);

			nano::confirmation_height_info conf_height_info;
			this->store.confirmation_height.get (transaction_a, account, conf_height_info);

			if (account_info.block_count != conf_height_info.height)
			{
				// Always output as no confirmation height has been set on the account yet
				auto height_delta = account_info.block_count - conf_height_info.height;
				auto const & frontier = account_info.head;
				auto const & cemented_frontier = conf_height_info.frontier;
				unconfirmed_frontiers_l.emplace (std::piecewise_construct, std::forward_as_tuple (height_delta), std::forward_as_tuple (cemented_frontier, frontier, i->first));
			}
		}
		// Merge results
		auto result_locked = result.lock ();
		result_locked->insert (unconfirmed_frontiers_l.begin (), unconfirmed_frontiers_l.end ());
	});
	return result;
}

// A precondition is that the store is an LMDB store
bool nano::ledger::migrate_lmdb_to_rocksdb (boost::filesystem::path const & data_path_a) const
{
	boost::system::error_code error_chmod;
	nano::set_secure_perm_directory (data_path_a, error_chmod);
	auto rockdb_data_path = data_path_a / "rocksdb";
	boost::filesystem::remove_all (rockdb_data_path);

	nano::logger_mt logger;
	auto error (false);

	// Open rocksdb database
	nano::rocksdb_config rocksdb_config;
	rocksdb_config.enable = true;
	auto rocksdb_store = nano::make_store (logger, data_path_a, false, true, rocksdb_config);

	if (!rocksdb_store->init_error ())
	{
		store.block.for_each_par (
		[&rocksdb_store] (nano::read_transaction const & /*unused*/, auto i, auto n) {
			for (; i != n; ++i)
			{
				auto rocksdb_transaction (rocksdb_store->tx_begin_write ({}, { nano::tables::blocks }));

				std::vector<uint8_t> vector;
				{
					nano::vectorstream stream (vector);
					nano::serialize_block (stream, *i->second.block);
					i->second.sideband.serialize (stream, i->second.block->type ());
				}
				rocksdb_store->block.raw_put (rocksdb_transaction, vector, i->first);
			}
		});

		store.unchecked.for_each_par (
		[&rocksdb_store] (nano::read_transaction const & /*unused*/, auto i, auto n) {
			for (; i != n; ++i)
			{
				auto rocksdb_transaction (rocksdb_store->tx_begin_write ({}, { nano::tables::unchecked }));
				rocksdb_store->unchecked.put (rocksdb_transaction, i->first, i->second);
			}
		});

		store.pending.for_each_par (
		[&rocksdb_store] (nano::read_transaction const & /*unused*/, auto i, auto n) {
			for (; i != n; ++i)
			{
				auto rocksdb_transaction (rocksdb_store->tx_begin_write ({}, { nano::tables::pending }));
				rocksdb_store->pending.put (rocksdb_transaction, i->first, i->second);
			}
		});

		store.confirmation_height.for_each_par (
		[&rocksdb_store] (nano::read_transaction const & /*unused*/, auto i, auto n) {
			for (; i != n; ++i)
			{
				auto rocksdb_transaction (rocksdb_store->tx_begin_write ({}, { nano::tables::confirmation_height }));
				rocksdb_store->confirmation_height.put (rocksdb_transaction, i->first, i->second);
			}
		});

		store.account.for_each_par (
		[&rocksdb_store] (nano::read_transaction const & /*unused*/, auto i, auto n) {
			for (; i != n; ++i)
			{
				auto rocksdb_transaction (rocksdb_store->tx_begin_write ({}, { nano::tables::accounts }));
				rocksdb_store->account.put (rocksdb_transaction, i->first, i->second);
			}
		});

		store.frontier.for_each_par (
		[&rocksdb_store] (nano::read_transaction const & /*unused*/, auto i, auto n) {
			for (; i != n; ++i)
			{
				auto rocksdb_transaction (rocksdb_store->tx_begin_write ({}, { nano::tables::frontiers }));
				rocksdb_store->frontier.put (rocksdb_transaction, i->first, i->second);
			}
		});

		store.pruned.for_each_par (
		[&rocksdb_store] (nano::read_transaction const & /*unused*/, auto i, auto n) {
			for (; i != n; ++i)
			{
				auto rocksdb_transaction (rocksdb_store->tx_begin_write ({}, { nano::tables::pruned }));
				rocksdb_store->pruned.put (rocksdb_transaction, i->first);
			}
		});

		store.final_vote.for_each_par (
		[&rocksdb_store] (nano::read_transaction const & /*unused*/, auto i, auto n) {
			for (; i != n; ++i)
			{
				auto rocksdb_transaction (rocksdb_store->tx_begin_write ({}, { nano::tables::final_votes }));
				rocksdb_store->final_vote.put (rocksdb_transaction, i->first, i->second);
			}
		});

		auto lmdb_transaction (store.tx_begin_read ());
		auto version = store.version.get (lmdb_transaction);
		auto rocksdb_transaction (rocksdb_store->tx_begin_write ());
		rocksdb_store->version.put (rocksdb_transaction, version);

		for (auto i (store.online_weight.begin (lmdb_transaction)), n (store.online_weight.end ()); i != n; ++i)
		{
			rocksdb_store->online_weight.put (rocksdb_transaction, i->first, i->second);
		}

		for (auto i (store.peer.begin (lmdb_transaction)), n (store.peer.end ()); i != n; ++i)
		{
			rocksdb_store->peer.put (rocksdb_transaction, i->first);
		}

		// Compare counts
		error |= store.unchecked.count (lmdb_transaction) != rocksdb_store->unchecked.count (rocksdb_transaction);
		error |= store.peer.count (lmdb_transaction) != rocksdb_store->peer.count (rocksdb_transaction);
		error |= store.pruned.count (lmdb_transaction) != rocksdb_store->pruned.count (rocksdb_transaction);
		error |= store.final_vote.count (lmdb_transaction) != rocksdb_store->final_vote.count (rocksdb_transaction);
		error |= store.online_weight.count (lmdb_transaction) != rocksdb_store->online_weight.count (rocksdb_transaction);
		error |= store.version.get (lmdb_transaction) != rocksdb_store->version.get (rocksdb_transaction);

		// For large tables a random key is used instead and makes sure it exists
		auto random_block (store.block.random (lmdb_transaction));
		error |= rocksdb_store->block.get (rocksdb_transaction, random_block->hash ()) == nullptr;

		auto account = random_block->account ().is_zero () ? random_block->sideband ().account : random_block->account ();
		nano::account_info account_info;
		error |= rocksdb_store->account.get (rocksdb_transaction, account, account_info);

		// If confirmation height exists in the lmdb ledger for this account it should exist in the rocksdb ledger
		nano::confirmation_height_info confirmation_height_info;
		if (!store.confirmation_height.get (lmdb_transaction, account, confirmation_height_info))
		{
			error |= rocksdb_store->confirmation_height.get (rocksdb_transaction, account, confirmation_height_info);
		}
	}
	else
	{
		error = true;
	}
	return error;
}

nano::uncemented_info::uncemented_info (nano::block_hash const & cemented_frontier, nano::block_hash const & frontier, nano::account const & account) :
	cemented_frontier (cemented_frontier), frontier (frontier), account (account)
{
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (ledger & ledger, std::string const & name)
{
	auto count = ledger.bootstrap_weights_size.load ();
	auto sizeof_element = sizeof (decltype (ledger.bootstrap_weights)::value_type);
	auto composite = std::make_unique<container_info_composite> (name);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "bootstrap_weights", count, sizeof_element }));
	composite->add_component (collect_container_info (ledger.cache.rep_weights, "rep_weights"));
	return composite;
}
