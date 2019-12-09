#include <nano/secure/versioning.hpp>

#include <boost/endian/conversion.hpp>

#include <lmdb/libraries/liblmdb/lmdb.h>

nano::account_info_v1::account_info_v1 (MDB_val const & val_a)
{
	assert (val_a.mv_size == sizeof (*this));
	static_assert (sizeof (head) + sizeof (rep_block) + sizeof (balance) + sizeof (modified) == sizeof (*this), "Class not packed");
	std::copy (reinterpret_cast<uint8_t const *> (val_a.mv_data), reinterpret_cast<uint8_t const *> (val_a.mv_data) + sizeof (*this), reinterpret_cast<uint8_t *> (this));
}

nano::account_info_v1::account_info_v1 (nano::block_hash const & head_a, nano::block_hash const & rep_block_a, nano::amount const & balance_a, uint64_t modified_a)
	: head (head_a)
	, rep_block (rep_block_a)
	, balance (balance_a)
	, modified (modified_a)
{
}

nano::pending_info_v3::pending_info_v3 (MDB_val const & val_a)
{
	assert (val_a.mv_size == sizeof (*this));
	static_assert (sizeof (source) + sizeof (amount) + sizeof (destination) == sizeof (*this), "Packed class");
	std::copy (reinterpret_cast<uint8_t const *> (val_a.mv_data), reinterpret_cast<uint8_t const *> (val_a.mv_data) + sizeof (*this), reinterpret_cast<uint8_t *> (this));
}

nano::pending_info_v3::pending_info_v3 (nano::account const & source_a, nano::amount const & amount_a, nano::account const & destination_a)
	: source (source_a)
	, amount (amount_a)
	, destination (destination_a)
{
}

nano::pending_info_v14::pending_info_v14 (nano::account const & source_a, nano::amount const & amount_a, nano::epoch epoch_a)
	: source (source_a)
	, amount (amount_a)
	, epoch (epoch_a)
{
}

bool nano::pending_info_v14::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		nano::read (stream_a, source.bytes);
		nano::read (stream_a, amount.bytes);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

size_t nano::pending_info_v14::db_size () const
{
	return sizeof (source) + sizeof (amount);
}

bool nano::pending_info_v14::operator== (nano::pending_info_v14 const & other_a) const
{
	return source == other_a.source && amount == other_a.amount && epoch == other_a.epoch;
}

nano::account_info_v5::account_info_v5 (MDB_val const & val_a)
{
	assert (val_a.mv_size == sizeof (*this));
	static_assert (sizeof (head) + sizeof (rep_block) + sizeof (open_block) + sizeof (balance) + sizeof (modified) == sizeof (*this), "Class not packed");
	std::copy (reinterpret_cast<uint8_t const *> (val_a.mv_data), reinterpret_cast<uint8_t const *> (val_a.mv_data) + sizeof (*this), reinterpret_cast<uint8_t *> (this));
}

nano::account_info_v5::account_info_v5 (nano::block_hash const & head_a, nano::block_hash const & rep_block_a, nano::block_hash const & open_block_a, nano::amount const & balance_a, uint64_t modified_a)
	: head (head_a)
	, rep_block (rep_block_a)
	, open_block (open_block_a)
	, balance (balance_a)
	, modified (modified_a)
{
}

nano::account_info_v13::account_info_v13 (nano::block_hash const & head_a, nano::block_hash const & rep_block_a, nano::block_hash const & open_block_a, nano::amount const & balance_a, uint64_t modified_a, uint64_t block_count_a, nano::epoch epoch_a)
	: head (head_a)
	, rep_block (rep_block_a)
	, open_block (open_block_a)
	, balance (balance_a)
	, modified (modified_a)
	, block_count (block_count_a)
	, epoch (epoch_a)
{
}

size_t nano::account_info_v13::db_size () const
{
	assert (reinterpret_cast<const uint8_t *> (this) == reinterpret_cast<const uint8_t *> (&head));
	assert (reinterpret_cast<const uint8_t *> (&head) + sizeof (head) == reinterpret_cast<const uint8_t *> (&rep_block));
	assert (reinterpret_cast<const uint8_t *> (&rep_block) + sizeof (rep_block) == reinterpret_cast<const uint8_t *> (&open_block));
	assert (reinterpret_cast<const uint8_t *> (&open_block) + sizeof (open_block) == reinterpret_cast<const uint8_t *> (&balance));
	assert (reinterpret_cast<const uint8_t *> (&balance) + sizeof (balance) == reinterpret_cast<const uint8_t *> (&modified));
	assert (reinterpret_cast<const uint8_t *> (&modified) + sizeof (modified) == reinterpret_cast<const uint8_t *> (&block_count));
	return sizeof (head) + sizeof (rep_block) + sizeof (open_block) + sizeof (balance) + sizeof (modified) + sizeof (block_count);
}

nano::account_info_v14::account_info_v14 (nano::block_hash const & head_a, nano::block_hash const & rep_block_a, nano::block_hash const & open_block_a, nano::amount const & balance_a, uint64_t modified_a, uint64_t block_count_a, uint64_t confirmation_height_a, nano::epoch epoch_a)
	: head (head_a)
	, rep_block (rep_block_a)
	, open_block (open_block_a)
	, balance (balance_a)
	, modified (modified_a)
	, block_count (block_count_a)
	, confirmation_height (confirmation_height_a)
	, epoch (epoch_a)
{
}

size_t nano::account_info_v14::db_size () const
{
	assert (reinterpret_cast<const uint8_t *> (this) == reinterpret_cast<const uint8_t *> (&head));
	assert (reinterpret_cast<const uint8_t *> (&head) + sizeof (head) == reinterpret_cast<const uint8_t *> (&rep_block));
	assert (reinterpret_cast<const uint8_t *> (&rep_block) + sizeof (rep_block) == reinterpret_cast<const uint8_t *> (&open_block));
	assert (reinterpret_cast<const uint8_t *> (&open_block) + sizeof (open_block) == reinterpret_cast<const uint8_t *> (&balance));
	assert (reinterpret_cast<const uint8_t *> (&balance) + sizeof (balance) == reinterpret_cast<const uint8_t *> (&modified));
	assert (reinterpret_cast<const uint8_t *> (&modified) + sizeof (modified) == reinterpret_cast<const uint8_t *> (&block_count));
	assert (reinterpret_cast<const uint8_t *> (&block_count) + sizeof (block_count) == reinterpret_cast<const uint8_t *> (&confirmation_height));
	return sizeof (head) + sizeof (rep_block) + sizeof (open_block) + sizeof (balance) + sizeof (modified) + sizeof (block_count) + sizeof (confirmation_height);
}

nano::block_sideband_v14::block_sideband_v14 (nano::block_type type_a, nano::account const & account_a, nano::block_hash const & successor_a, nano::amount const & balance_a, uint64_t height_a, uint64_t timestamp_a)
	: type (type_a)
	, successor (successor_a)
	, account (account_a)
	, balance (balance_a)
	, height (height_a)
	, timestamp (timestamp_a)
{
}

size_t nano::block_sideband_v14::size (nano::block_type type_a)
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
	return result;
}

void nano::block_sideband_v14::serialize (nano::stream & stream_a) const
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
}

bool nano::block_sideband_v14::deserialize (nano::stream & stream_a)
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
	}
	catch (std::runtime_error &)
	{
		result = true;
	}

	return result;
}
