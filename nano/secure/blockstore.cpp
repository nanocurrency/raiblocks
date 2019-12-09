#include <nano/lib/threading.hpp>
#include <nano/secure/blockstore.hpp>

#include <boost/endian/conversion.hpp>

nano::block_sideband::block_sideband (nano::block_type type_a, nano::account const & account_a, nano::block_hash const & successor_a, nano::amount const & balance_a, uint64_t height_a, uint64_t timestamp_a, nano::epoch epoch_a)
	: type (type_a)
	, successor (successor_a)
	, account (account_a)
	, balance (balance_a)
	, height (height_a)
	, timestamp (timestamp_a)
	, epoch (epoch_a)
{
}

size_t nano::block_sideband::size (nano::block_type type_a)
{
	size_t result (0);
	result += sizeof (successor);
	if (type_a != nano::block_type::state && type_a != nano::block_type::open)
	{
		result += sizeof (account);
	}
	if (type_a != nano::block_type::open)
	{
		result += sizeof (height);
	}
	if (type_a == nano::block_type::receive || type_a == nano::block_type::change || type_a == nano::block_type::open)
	{
		result += sizeof (balance);
	}
	result += sizeof (timestamp);
	if (type_a == nano::block_type::state)
	{
		result += sizeof (epoch);
	}
	return result;
}

void nano::block_sideband::serialize (nano::stream & stream_a) const
{
	nano::write (stream_a, successor.bytes);
	if (type != nano::block_type::state && type != nano::block_type::open)
	{
		nano::write (stream_a, account.bytes);
	}
	if (type != nano::block_type::open)
	{
		nano::write (stream_a, boost::endian::native_to_big (height));
	}
	if (type == nano::block_type::receive || type == nano::block_type::change || type == nano::block_type::open)
	{
		nano::write (stream_a, balance.bytes);
	}
	nano::write (stream_a, boost::endian::native_to_big (timestamp));
	if (type == nano::block_type::state)
	{
		nano::write (stream_a, epoch);
	}
}

bool nano::block_sideband::deserialize (nano::stream & stream_a)
{
	bool result (false);
	try
	{
		nano::read (stream_a, successor.bytes);
		if (type != nano::block_type::state && type != nano::block_type::open)
		{
			nano::read (stream_a, account.bytes);
		}
		if (type != nano::block_type::open)
		{
			nano::read (stream_a, height);
			boost::endian::big_to_native_inplace (height);
		}
		else
		{
			height = 1;
		}
		if (type == nano::block_type::receive || type == nano::block_type::change || type == nano::block_type::open)
		{
			nano::read (stream_a, balance.bytes);
		}
		nano::read (stream_a, timestamp);
		boost::endian::big_to_native_inplace (timestamp);
		if (type == nano::block_type::state)
		{
			nano::read (stream_a, epoch);
		}
	}
	catch (std::runtime_error &)
	{
		result = true;
	}

	return result;
}

nano::summation_visitor::summation_visitor (nano::transaction const & transaction_a, nano::block_store const & store_a, bool is_v14_upgrade_a)
	: transaction (transaction_a)
	, store (store_a)
	, is_v14_upgrade (is_v14_upgrade_a)
{
}

void nano::summation_visitor::send_block (nano::send_block const & block_a)
{
	assert (current->type != summation_type::invalid && current != nullptr);
	if (current->type == summation_type::amount)
	{
		sum_set (block_a.hashables.balance.number ());
		current->balance_hash = block_a.hashables.previous;
		current->amount_hash = 0;
	}
	else
	{
		sum_add (block_a.hashables.balance.number ());
		current->balance_hash = 0;
	}
}

void nano::summation_visitor::state_block (nano::state_block const & block_a)
{
	assert (current->type != summation_type::invalid && current != nullptr);
	sum_set (block_a.hashables.balance.number ());
	if (current->type == summation_type::amount)
	{
		current->balance_hash = block_a.hashables.previous;
		current->amount_hash = 0;
	}
	else
	{
		current->balance_hash = 0;
	}
}

void nano::summation_visitor::receive_block (nano::receive_block const & block_a)
{
	assert (current->type != summation_type::invalid && current != nullptr);
	if (current->type == summation_type::amount)
	{
		current->amount_hash = block_a.hashables.source;
	}
	else
	{
		nano::block_info block_info;
		if (!store.block_info_get (transaction, block_a.hash (), block_info))
		{
			sum_add (block_info.balance.number ());
			current->balance_hash = 0;
		}
		else
		{
			current->amount_hash = block_a.hashables.source;
			current->balance_hash = block_a.hashables.previous;
		}
	}
}

void nano::summation_visitor::open_block (nano::open_block const & block_a)
{
	assert (current->type != summation_type::invalid && current != nullptr);
	if (current->type == summation_type::amount)
	{
		if (block_a.hashables.source != network_params.ledger.genesis_account)
		{
			current->amount_hash = block_a.hashables.source;
		}
		else
		{
			sum_set (network_params.ledger.genesis_amount);
			current->amount_hash = 0;
		}
	}
	else
	{
		current->amount_hash = block_a.hashables.source;
		current->balance_hash = 0;
	}
}

void nano::summation_visitor::change_block (nano::change_block const & block_a)
{
	assert (current->type != summation_type::invalid && current != nullptr);
	if (current->type == summation_type::amount)
	{
		sum_set (0);
		current->amount_hash = 0;
	}
	else
	{
		nano::block_info block_info;
		if (!store.block_info_get (transaction, block_a.hash (), block_info))
		{
			sum_add (block_info.balance.number ());
			current->balance_hash = 0;
		}
		else
		{
			current->balance_hash = block_a.hashables.previous;
		}
	}
}

nano::summation_visitor::frame nano::summation_visitor::push (nano::summation_visitor::summation_type type_a, nano::block_hash const & hash_a)
{
	frames.emplace (type_a, type_a == summation_type::balance ? hash_a : 0, type_a == summation_type::amount ? hash_a : 0);
	return frames.top ();
}

void nano::summation_visitor::sum_add (nano::uint128_t addend_a)
{
	current->sum += addend_a;
	result = current->sum;
}

void nano::summation_visitor::sum_set (nano::uint128_t value_a)
{
	current->sum = value_a;
	result = current->sum;
}

nano::uint128_t nano::summation_visitor::compute_internal (nano::summation_visitor::summation_type type_a, nano::block_hash const & hash_a)
{
	push (type_a, hash_a);

	/*
	 Invocation loop representing balance and amount computations calling each other.
	 This is usually better done by recursion or something like boost::coroutine2, but
	 segmented stacks are not supported on all platforms so we do it manually to avoid
	 stack overflow (the mutual calls are not tail-recursive so we cannot rely on the
	 compiler optimizing that into a loop, though a future alternative is to do a
	 CPS-style implementation to enforce tail calls.)
	*/
	while (!frames.empty ())
	{
		current = &frames.top ();
		assert (current->type != summation_type::invalid && current != nullptr);

		if (current->type == summation_type::balance)
		{
			if (current->awaiting_result)
			{
				sum_add (current->incoming_result);
				current->awaiting_result = false;
			}

			while (!current->awaiting_result && (!current->balance_hash.is_zero () || !current->amount_hash.is_zero ()))
			{
				if (!current->amount_hash.is_zero ())
				{
					// Compute amount
					current->awaiting_result = true;
					push (summation_type::amount, current->amount_hash);
					current->amount_hash = 0;
				}
				else
				{
					auto block (block_get (transaction, current->balance_hash));
					assert (block != nullptr);
					block->visit (*this);
				}
			}

			epilogue ();
		}
		else if (current->type == summation_type::amount)
		{
			if (current->awaiting_result)
			{
				sum_set (current->sum < current->incoming_result ? current->incoming_result - current->sum : current->sum - current->incoming_result);
				current->awaiting_result = false;
			}

			while (!current->awaiting_result && (!current->amount_hash.is_zero () || !current->balance_hash.is_zero ()))
			{
				if (!current->amount_hash.is_zero ())
				{
					auto block = block_get (transaction, current->amount_hash);
					if (block != nullptr)
					{
						block->visit (*this);
					}
					else
					{
						if (current->amount_hash == network_params.ledger.genesis_account)
						{
							sum_set ((std::numeric_limits<nano::uint128_t>::max) ());
							current->amount_hash = 0;
						}
						else
						{
							assert (false);
							sum_set (0);
							current->amount_hash = 0;
						}
					}
				}
				else
				{
					// Compute balance
					current->awaiting_result = true;
					push (summation_type::balance, current->balance_hash);
					current->balance_hash = 0;
				}
			}

			epilogue ();
		}
	}

	return result;
}

void nano::summation_visitor::epilogue ()
{
	if (!current->awaiting_result)
	{
		frames.pop ();
		if (!frames.empty ())
		{
			frames.top ().incoming_result = current->sum;
		}
	}
}

nano::uint128_t nano::summation_visitor::compute_amount (nano::block_hash const & block_hash)
{
	return compute_internal (summation_type::amount, block_hash);
}

nano::uint128_t nano::summation_visitor::compute_balance (nano::block_hash const & block_hash)
{
	return compute_internal (summation_type::balance, block_hash);
}

std::shared_ptr<nano::block> nano::summation_visitor::block_get (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const
{
	return is_v14_upgrade ? store.block_get_v14 (transaction, hash_a) : store.block_get (transaction, hash_a);
}

nano::representative_visitor::representative_visitor (nano::transaction const & transaction_a, nano::block_store & store_a)
	: transaction (transaction_a)
	, store (store_a)
	, result (0)
{
}

void nano::representative_visitor::compute (nano::block_hash const & hash_a)
{
	current = hash_a;
	while (result.is_zero ())
	{
		auto block (store.block_get (transaction, current));
		assert (block != nullptr);
		block->visit (*this);
	}
}

void nano::representative_visitor::send_block (nano::send_block const & block_a)
{
	current = block_a.previous ();
}

void nano::representative_visitor::receive_block (nano::receive_block const & block_a)
{
	current = block_a.previous ();
}

void nano::representative_visitor::open_block (nano::open_block const & block_a)
{
	result = block_a.hash ();
}

void nano::representative_visitor::change_block (nano::change_block const & block_a)
{
	result = block_a.hash ();
}

void nano::representative_visitor::state_block (nano::state_block const & block_a)
{
	result = block_a.hash ();
}

nano::read_transaction::read_transaction (std::unique_ptr<nano::read_transaction_impl> read_transaction_impl)
	: impl (std::move (read_transaction_impl))
{
}

void * nano::read_transaction::get_handle () const
{
	return impl->get_handle ();
}

void nano::read_transaction::reset () const
{
	impl->reset ();
}

void nano::read_transaction::renew () const
{
	impl->renew ();
}

void nano::read_transaction::refresh () const
{
	reset ();
	renew ();
}

nano::write_transaction::write_transaction (std::unique_ptr<nano::write_transaction_impl> write_transaction_impl)
	: impl (std::move (write_transaction_impl))
{
	/*
	 * For IO threads, we do not want them to block on creating write transactions.
	 */
	assert (nano::thread_role::get () != nano::thread_role::name::io);
}

void * nano::write_transaction::get_handle () const
{
	return impl->get_handle ();
}

void nano::write_transaction::commit () const
{
	impl->commit ();
}

void nano::write_transaction::renew ()
{
	impl->renew ();
}

bool nano::write_transaction::contains (nano::tables table_a) const
{
	return impl->contains (table_a);
}
