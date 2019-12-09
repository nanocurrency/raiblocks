#pragma once

#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/diagnosticsconfig.hpp>
#include <nano/lib/logger_mt.hpp>
#include <nano/lib/memory.hpp>
#include <nano/lib/rocksdbconfig.hpp>
#include <nano/secure/buffer.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/versioning.hpp>

#include <boost/endian/conversion.hpp>
#include <boost/polymorphic_cast.hpp>

#include <stack>

namespace nano
{
/**
 * Encapsulates database specific container
 */
template <typename Val>
class db_val
{
public:
	db_val (Val const & value_a)
		: value (value_a)
	{
	}

	db_val ()
		: db_val (0, nullptr)
	{
	}

	db_val (nano::uint128_union const & val_a)
		: db_val (sizeof (val_a), const_cast<nano::uint128_union *> (&val_a))
	{
	}

	db_val (nano::uint256_union const & val_a)
		: db_val (sizeof (val_a), const_cast<nano::uint256_union *> (&val_a))
	{
	}

	db_val (nano::account_info const & val_a)
		: db_val (val_a.db_size (), const_cast<nano::account_info *> (&val_a))
	{
	}

	db_val (nano::account_info_v13 const & val_a)
		: db_val (val_a.db_size (), const_cast<nano::account_info_v13 *> (&val_a))
	{
	}

	db_val (nano::account_info_v14 const & val_a)
		: db_val (val_a.db_size (), const_cast<nano::account_info_v14 *> (&val_a))
	{
	}

	db_val (nano::pending_info const & val_a)
		: db_val (val_a.db_size (), const_cast<nano::pending_info *> (&val_a))
	{
		static_assert (std::is_standard_layout<nano::pending_info>::value, "Standard layout is required");
	}

	db_val (nano::pending_info_v14 const & val_a)
		: db_val (val_a.db_size (), const_cast<nano::pending_info_v14 *> (&val_a))
	{
		static_assert (std::is_standard_layout<nano::pending_info_v14>::value, "Standard layout is required");
	}

	db_val (nano::pending_key const & val_a)
		: db_val (sizeof (val_a), const_cast<nano::pending_key *> (&val_a))
	{
		static_assert (std::is_standard_layout<nano::pending_key>::value, "Standard layout is required");
	}

	db_val (nano::unchecked_info const & val_a)
		: buffer (std::make_shared<std::vector<uint8_t>> ())
	{
		{
			nano::vectorstream stream (*buffer);
			val_a.serialize (stream);
		}
		convert_buffer_to_value ();
	}

	db_val (nano::unchecked_key const & val_a)
		: db_val (sizeof (val_a), const_cast<nano::unchecked_key *> (&val_a))
	{
		static_assert (std::is_standard_layout<nano::unchecked_key>::value, "Standard layout is required");
	}

	db_val (nano::block_info const & val_a)
		: db_val (sizeof (val_a), const_cast<nano::block_info *> (&val_a))
	{
		static_assert (std::is_standard_layout<nano::block_info>::value, "Standard layout is required");
	}

	db_val (nano::endpoint_key const & val_a)
		: db_val (sizeof (val_a), const_cast<nano::endpoint_key *> (&val_a))
	{
		static_assert (std::is_standard_layout<nano::endpoint_key>::value, "Standard layout is required");
	}

	db_val (std::shared_ptr<nano::block> const & val_a)
		: buffer (std::make_shared<std::vector<uint8_t>> ())
	{
		{
			nano::vectorstream stream (*buffer);
			nano::serialize_block (stream, *val_a);
		}
		convert_buffer_to_value ();
	}

	db_val (uint64_t val_a)
		: buffer (std::make_shared<std::vector<uint8_t>> ())
	{
		{
			boost::endian::native_to_big_inplace (val_a);
			nano::vectorstream stream (*buffer);
			nano::write (stream, val_a);
		}
		convert_buffer_to_value ();
	}

	explicit operator nano::account_info () const
	{
		nano::account_info result;
		assert (size () == result.db_size ());
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + result.db_size (), reinterpret_cast<uint8_t *> (&result));
		return result;
	}

	explicit operator nano::account_info_v13 () const
	{
		nano::account_info_v13 result;
		assert (size () == result.db_size ());
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + result.db_size (), reinterpret_cast<uint8_t *> (&result));
		return result;
	}

	explicit operator nano::account_info_v14 () const
	{
		nano::account_info_v14 result;
		assert (size () == result.db_size ());
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + result.db_size (), reinterpret_cast<uint8_t *> (&result));
		return result;
	}

	explicit operator nano::block_info () const
	{
		nano::block_info result;
		assert (size () == sizeof (result));
		static_assert (sizeof (nano::block_info::account) + sizeof (nano::block_info::balance) == sizeof (result), "Packed class");
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + sizeof (result), reinterpret_cast<uint8_t *> (&result));
		return result;
	}

	explicit operator nano::pending_info_v14 () const
	{
		nano::pending_info_v14 result;
		assert (size () == result.db_size ());
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + result.db_size (), reinterpret_cast<uint8_t *> (&result));
		return result;
	}

	explicit operator nano::pending_info () const
	{
		nano::pending_info result;
		assert (size () == result.db_size ());
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + result.db_size (), reinterpret_cast<uint8_t *> (&result));
		return result;
	}

	explicit operator nano::pending_key () const
	{
		nano::pending_key result;
		assert (size () == sizeof (result));
		static_assert (sizeof (nano::pending_key::account) + sizeof (nano::pending_key::hash) == sizeof (result), "Packed class");
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + sizeof (result), reinterpret_cast<uint8_t *> (&result));
		return result;
	}

	explicit operator nano::unchecked_info () const
	{
		nano::bufferstream stream (reinterpret_cast<uint8_t const *> (data ()), size ());
		nano::unchecked_info result;
		bool error (result.deserialize (stream));
		(void)error;
		assert (!error);
		return result;
	}

	explicit operator nano::unchecked_key () const
	{
		nano::unchecked_key result;
		assert (size () == sizeof (result));
		static_assert (sizeof (nano::unchecked_key::previous) + sizeof (nano::pending_key::hash) == sizeof (result), "Packed class");
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + sizeof (result), reinterpret_cast<uint8_t *> (&result));
		return result;
	}

	explicit operator nano::uint128_union () const
	{
		return convert<nano::uint128_union> ();
	}

	explicit operator nano::amount () const
	{
		return convert<nano::amount> ();
	}

	explicit operator nano::block_hash () const
	{
		return convert<nano::block_hash> ();
	}

	explicit operator nano::public_key () const
	{
		return convert<nano::public_key> ();
	}

	explicit operator nano::uint256_union () const
	{
		return convert<nano::uint256_union> ();
	}

	explicit operator std::array<char, 64> () const
	{
		nano::bufferstream stream (reinterpret_cast<uint8_t const *> (data ()), size ());
		std::array<char, 64> result;
		auto error = nano::try_read (stream, result);
		(void)error;
		assert (!error);
		return result;
	}

	explicit operator nano::endpoint_key () const
	{
		nano::endpoint_key result;
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + sizeof (result), reinterpret_cast<uint8_t *> (&result));
		return result;
	}

	explicit operator nano::state_block_w_sideband_v14 () const
	{
		nano::bufferstream stream (reinterpret_cast<uint8_t const *> (data ()), size ());
		auto error (false);
		nano::state_block_w_sideband_v14 state_block_w_sideband_v14;
		state_block_w_sideband_v14.state_block = std::make_shared<nano::state_block> (error, stream);
		assert (!error);

		state_block_w_sideband_v14.sideband.type = nano::block_type::state;
		error = state_block_w_sideband_v14.sideband.deserialize (stream);
		assert (!error);

		return state_block_w_sideband_v14;
	}

	explicit operator nano::no_value () const
	{
		return no_value::dummy;
	}

	explicit operator std::shared_ptr<nano::block> () const
	{
		nano::bufferstream stream (reinterpret_cast<uint8_t const *> (data ()), size ());
		std::shared_ptr<nano::block> result (nano::deserialize_block (stream));
		return result;
	}

	template <typename Block>
	std::shared_ptr<Block> convert_to_block () const
	{
		nano::bufferstream stream (reinterpret_cast<uint8_t const *> (data ()), size ());
		auto error (false);
		auto result (std::make_shared<Block> (error, stream));
		assert (!error);
		return result;
	}

	explicit operator std::shared_ptr<nano::send_block> () const
	{
		return convert_to_block<nano::send_block> ();
	}

	explicit operator std::shared_ptr<nano::receive_block> () const
	{
		return convert_to_block<nano::receive_block> ();
	}

	explicit operator std::shared_ptr<nano::open_block> () const
	{
		return convert_to_block<nano::open_block> ();
	}

	explicit operator std::shared_ptr<nano::change_block> () const
	{
		return convert_to_block<nano::change_block> ();
	}

	explicit operator std::shared_ptr<nano::state_block> () const
	{
		return convert_to_block<nano::state_block> ();
	}

	explicit operator std::shared_ptr<nano::vote> () const
	{
		nano::bufferstream stream (reinterpret_cast<uint8_t const *> (data ()), size ());
		auto error (false);
		auto result (nano::make_shared<nano::vote> (error, stream));
		assert (!error);
		return result;
	}

	explicit operator uint64_t () const
	{
		uint64_t result;
		nano::bufferstream stream (reinterpret_cast<uint8_t const *> (data ()), size ());
		auto error (nano::try_read (stream, result));
		(void)error;
		assert (!error);
		boost::endian::big_to_native_inplace (result);
		return result;
	}

	operator Val * () const
	{
		// Allow passing a temporary to a non-c++ function which doesn't have constness
		return const_cast<Val *> (&value);
	}

	operator Val const & () const
	{
		return value;
	}

	// Must be specialized
	void * data () const;
	size_t size () const;
	db_val (size_t size_a, void * data_a);
	void convert_buffer_to_value ();

	Val value;
	std::shared_ptr<std::vector<uint8_t>> buffer;

private:
	template <typename T>
	T convert () const
	{
		T result;
		assert (size () == sizeof (result));
		std::copy (reinterpret_cast<uint8_t const *> (data ()), reinterpret_cast<uint8_t const *> (data ()) + sizeof (result), result.bytes.data ());
		return result;
	}
};

class block_sideband final
{
public:
	block_sideband () = default;
	block_sideband (nano::block_type, nano::account const &, nano::block_hash const &, nano::amount const &, uint64_t, uint64_t, nano::epoch);
	void serialize (nano::stream &) const;
	bool deserialize (nano::stream &);
	static size_t size (nano::block_type);
	nano::block_type type{ nano::block_type::invalid };
	nano::block_hash successor{ 0 };
	nano::account account{ 0 };
	nano::amount balance{ 0 };
	uint64_t height{ 0 };
	uint64_t timestamp{ 0 };
	nano::epoch epoch{ nano::epoch::epoch_0 };
};
class transaction;
class block_store;

/**
 * Summation visitor for blocks, supporting amount and balance computations. These
 * computations are mutually dependant. The natural solution is to use mutual recursion
 * between balance and amount visitors, but this leads to very deep stacks. Hence, the
 * summation visitor uses an iterative approach.
 */
class summation_visitor final : public nano::block_visitor
{
	enum summation_type
	{
		invalid = 0,
		balance = 1,
		amount = 2
	};

	/** Represents an invocation frame */
	class frame final
	{
	public:
		frame (summation_type type_a, nano::block_hash balance_hash_a, nano::block_hash amount_hash_a)
			: type (type_a)
			, balance_hash (balance_hash_a)
			, amount_hash (amount_hash_a)
		{
		}

		/** The summation type guides the block visitor handlers */
		summation_type type{ invalid };
		/** Accumulated balance or amount */
		nano::uint128_t sum{ 0 };
		/** The current balance hash */
		nano::block_hash balance_hash{ 0 };
		/** The current amount hash */
		nano::block_hash amount_hash{ 0 };
		/** If true, this frame is awaiting an invocation result */
		bool awaiting_result{ false };
		/** Set by the invoked frame, representing the return value */
		nano::uint128_t incoming_result{ 0 };
	};

public:
	summation_visitor (nano::transaction const &, nano::block_store const &, bool is_v14_upgrade = false);
	virtual ~summation_visitor () = default;
	/** Computes the balance as of \p block_hash */
	nano::uint128_t compute_balance (nano::block_hash const & block_hash);
	/** Computes the amount delta between \p block_hash and its predecessor */
	nano::uint128_t compute_amount (nano::block_hash const & block_hash);

protected:
	nano::transaction const & transaction;
	nano::block_store const & store;
	nano::network_params network_params;

	/** The final result */
	nano::uint128_t result{ 0 };
	/** The current invocation frame */
	frame * current{ nullptr };
	/** Invocation frames */
	std::stack<frame> frames;
	/** Push a copy of \p hash of the given summation \p type */
	nano::summation_visitor::frame push (nano::summation_visitor::summation_type type, nano::block_hash const & hash);
	void sum_add (nano::uint128_t addend_a);
	void sum_set (nano::uint128_t value_a);
	/** The epilogue yields the result to previous frame, if any */
	void epilogue ();

	nano::uint128_t compute_internal (nano::summation_visitor::summation_type type, nano::block_hash const &);
	void send_block (nano::send_block const &) override;
	void receive_block (nano::receive_block const &) override;
	void open_block (nano::open_block const &) override;
	void change_block (nano::change_block const &) override;
	void state_block (nano::state_block const &) override;

private:
	bool is_v14_upgrade;
	std::shared_ptr<nano::block> block_get (nano::transaction const &, nano::block_hash const &) const;
};

/**
 * Determine the representative for this block
 */
class representative_visitor final : public nano::block_visitor
{
public:
	representative_visitor (nano::transaction const & transaction_a, nano::block_store & store_a);
	~representative_visitor () = default;
	void compute (nano::block_hash const & hash_a);
	void send_block (nano::send_block const & block_a) override;
	void receive_block (nano::receive_block const & block_a) override;
	void open_block (nano::open_block const & block_a) override;
	void change_block (nano::change_block const & block_a) override;
	void state_block (nano::state_block const & block_a) override;
	nano::transaction const & transaction;
	nano::block_store & store;
	nano::block_hash current;
	nano::block_hash result;
};
template <typename T, typename U>
class store_iterator_impl
{
public:
	virtual ~store_iterator_impl () = default;
	virtual nano::store_iterator_impl<T, U> & operator++ () = 0;
	virtual bool operator== (nano::store_iterator_impl<T, U> const & other_a) const = 0;
	virtual bool is_end_sentinal () const = 0;
	virtual void fill (std::pair<T, U> &) const = 0;
	nano::store_iterator_impl<T, U> & operator= (nano::store_iterator_impl<T, U> const &) = delete;
	bool operator== (nano::store_iterator_impl<T, U> const * other_a) const
	{
		return (other_a != nullptr && *this == *other_a) || (other_a == nullptr && is_end_sentinal ());
	}
	bool operator!= (nano::store_iterator_impl<T, U> const & other_a) const
	{
		return !(*this == other_a);
	}
};
/**
 * Iterates the key/value pairs of a transaction
 */
template <typename T, typename U>
class store_iterator final
{
public:
	store_iterator (std::nullptr_t)
	{
	}
	store_iterator (std::unique_ptr<nano::store_iterator_impl<T, U>> impl_a)
		: impl (std::move (impl_a))
	{
		impl->fill (current);
	}
	store_iterator (nano::store_iterator<T, U> && other_a)
		: current (std::move (other_a.current))
		, impl (std::move (other_a.impl))
	{
	}
	nano::store_iterator<T, U> & operator++ ()
	{
		++*impl;
		impl->fill (current);
		return *this;
	}
	nano::store_iterator<T, U> & operator= (nano::store_iterator<T, U> && other_a) noexcept
	{
		impl = std::move (other_a.impl);
		current = std::move (other_a.current);
		return *this;
	}
	nano::store_iterator<T, U> & operator= (nano::store_iterator<T, U> const &) = delete;
	std::pair<T, U> * operator-> ()
	{
		return &current;
	}
	bool operator== (nano::store_iterator<T, U> const & other_a) const
	{
		return (impl == nullptr && other_a.impl == nullptr) || (impl != nullptr && *impl == other_a.impl.get ()) || (other_a.impl != nullptr && *other_a.impl == impl.get ());
	}
	bool operator!= (nano::store_iterator<T, U> const & other_a) const
	{
		return !(*this == other_a);
	}

private:
	std::pair<T, U> current;
	std::unique_ptr<nano::store_iterator_impl<T, U>> impl;
};

// Keep this in alphabetical order
enum class tables
{
	accounts,
	blocks_info, // LMDB only
	cached_counts, // RocksDB only
	change_blocks,
	confirmation_height,
	frontiers,
	meta,
	online_weight,
	open_blocks,
	peers,
	pending,
	receive_blocks,
	representation,
	send_blocks,
	state_blocks,
	unchecked,
	vote
};

class transaction_impl
{
public:
	virtual ~transaction_impl () = default;
	virtual void * get_handle () const = 0;
};

class read_transaction_impl : public transaction_impl
{
public:
	virtual void reset () = 0;
	virtual void renew () = 0;
};

class write_transaction_impl : public transaction_impl
{
public:
	virtual void commit () const = 0;
	virtual void renew () = 0;
	virtual bool contains (nano::tables table_a) const = 0;
};

class transaction
{
public:
	virtual ~transaction () = default;
	virtual void * get_handle () const = 0;
};

/**
 * RAII wrapper of a read MDB_txn where the constructor starts the transaction
 * and the destructor aborts it.
 */
class read_transaction final : public transaction
{
public:
	explicit read_transaction (std::unique_ptr<nano::read_transaction_impl> read_transaction_impl);
	void * get_handle () const override;
	void reset () const;
	void renew () const;
	void refresh () const;

private:
	std::unique_ptr<nano::read_transaction_impl> impl;
};

/**
 * RAII wrapper of a read-write MDB_txn where the constructor starts the transaction
 * and the destructor commits it.
 */
class write_transaction final : public transaction
{
public:
	explicit write_transaction (std::unique_ptr<nano::write_transaction_impl> write_transaction_impl);
	void * get_handle () const override;
	void commit () const;
	void renew ();
	bool contains (nano::tables table_a) const;

private:
	std::unique_ptr<nano::write_transaction_impl> impl;
};

class rep_weights;

/**
 * Manages block storage and iteration
 */
class block_store
{
public:
	virtual ~block_store () = default;
	virtual void initialize (nano::write_transaction const &, nano::genesis const &, nano::rep_weights &, std::atomic<uint64_t> &, std::atomic<uint64_t> &) = 0;
	virtual void block_put (nano::write_transaction const &, nano::block_hash const &, nano::block const &, nano::block_sideband const &) = 0;
	virtual nano::block_hash block_successor (nano::transaction const &, nano::block_hash const &) const = 0;
	virtual void block_successor_clear (nano::write_transaction const &, nano::block_hash const &) = 0;
	virtual std::shared_ptr<nano::block> block_get (nano::transaction const &, nano::block_hash const &, nano::block_sideband * = nullptr) const = 0;
	virtual std::shared_ptr<nano::block> block_get_v14 (nano::transaction const &, nano::block_hash const &, nano::block_sideband_v14 * = nullptr, bool * = nullptr) const = 0;
	virtual std::shared_ptr<nano::block> block_random (nano::transaction const &) = 0;
	virtual void block_del (nano::write_transaction const &, nano::block_hash const &, nano::block_type) = 0;
	virtual bool block_exists (nano::transaction const &, nano::block_hash const &) = 0;
	virtual bool block_exists (nano::transaction const &, nano::block_type, nano::block_hash const &) = 0;
	virtual nano::block_counts block_count (nano::transaction const &) = 0;
	virtual bool root_exists (nano::transaction const &, nano::root const &) = 0;
	virtual bool source_exists (nano::transaction const &, nano::block_hash const &) = 0;
	virtual nano::account block_account (nano::transaction const &, nano::block_hash const &) const = 0;

	virtual void frontier_put (nano::write_transaction const &, nano::block_hash const &, nano::account const &) = 0;
	virtual nano::account frontier_get (nano::transaction const &, nano::block_hash const &) const = 0;
	virtual void frontier_del (nano::write_transaction const &, nano::block_hash const &) = 0;

	virtual void account_put (nano::write_transaction const &, nano::account const &, nano::account_info const &) = 0;
	virtual bool account_get (nano::transaction const &, nano::account const &, nano::account_info &) = 0;
	virtual void account_del (nano::write_transaction const &, nano::account const &) = 0;
	virtual bool account_exists (nano::transaction const &, nano::account const &) = 0;
	virtual size_t account_count (nano::transaction const &) = 0;
	virtual void confirmation_height_clear (nano::write_transaction const &, nano::account const & account, uint64_t existing_confirmation_height) = 0;
	virtual void confirmation_height_clear (nano::write_transaction const &) = 0;
	virtual nano::store_iterator<nano::account, nano::account_info> latest_begin (nano::transaction const &, nano::account const &) = 0;
	virtual nano::store_iterator<nano::account, nano::account_info> latest_begin (nano::transaction const &) = 0;
	virtual nano::store_iterator<nano::account, nano::account_info> latest_end () = 0;

	virtual void pending_put (nano::write_transaction const &, nano::pending_key const &, nano::pending_info const &) = 0;
	virtual void pending_del (nano::write_transaction const &, nano::pending_key const &) = 0;
	virtual bool pending_get (nano::transaction const &, nano::pending_key const &, nano::pending_info &) = 0;
	virtual bool pending_exists (nano::transaction const &, nano::pending_key const &) = 0;
	virtual nano::store_iterator<nano::pending_key, nano::pending_info> pending_begin (nano::transaction const &, nano::pending_key const &) = 0;
	virtual nano::store_iterator<nano::pending_key, nano::pending_info> pending_begin (nano::transaction const &) = 0;
	virtual nano::store_iterator<nano::pending_key, nano::pending_info> pending_end () = 0;

	virtual bool block_info_get (nano::transaction const &, nano::block_hash const &, nano::block_info &) const = 0;
	virtual nano::uint128_t block_balance (nano::transaction const &, nano::block_hash const &) = 0;
	virtual nano::uint128_t block_balance_calculated (std::shared_ptr<nano::block>, nano::block_sideband const &) const = 0;
	virtual nano::epoch block_version (nano::transaction const &, nano::block_hash const &) = 0;

	virtual void unchecked_clear (nano::write_transaction const &) = 0;
	virtual void unchecked_put (nano::write_transaction const &, nano::unchecked_key const &, nano::unchecked_info const &) = 0;
	virtual void unchecked_put (nano::write_transaction const &, nano::block_hash const &, std::shared_ptr<nano::block> const &) = 0;
	virtual std::vector<nano::unchecked_info> unchecked_get (nano::transaction const &, nano::block_hash const &) = 0;
	virtual void unchecked_del (nano::write_transaction const &, nano::unchecked_key const &) = 0;
	virtual nano::store_iterator<nano::unchecked_key, nano::unchecked_info> unchecked_begin (nano::transaction const &) = 0;
	virtual nano::store_iterator<nano::unchecked_key, nano::unchecked_info> unchecked_begin (nano::transaction const &, nano::unchecked_key const &) = 0;
	virtual nano::store_iterator<nano::unchecked_key, nano::unchecked_info> unchecked_end () = 0;
	virtual size_t unchecked_count (nano::transaction const &) = 0;

	// Return latest vote for an account from store
	virtual std::shared_ptr<nano::vote> vote_get (nano::transaction const &, nano::account const &) = 0;
	// Populate vote with the next sequence number
	virtual std::shared_ptr<nano::vote> vote_generate (nano::transaction const &, nano::account const &, nano::raw_key const &, std::shared_ptr<nano::block>) = 0;
	virtual std::shared_ptr<nano::vote> vote_generate (nano::transaction const &, nano::account const &, nano::raw_key const &, std::vector<nano::block_hash>) = 0;
	// Return either vote or the stored vote with a higher sequence number
	virtual std::shared_ptr<nano::vote> vote_max (nano::transaction const &, std::shared_ptr<nano::vote>) = 0;
	// Return latest vote for an account considering the vote cache
	virtual std::shared_ptr<nano::vote> vote_current (nano::transaction const &, nano::account const &) = 0;
	virtual void flush (nano::write_transaction const &) = 0;
	virtual nano::store_iterator<nano::account, std::shared_ptr<nano::vote>> vote_begin (nano::transaction const &) = 0;
	virtual nano::store_iterator<nano::account, std::shared_ptr<nano::vote>> vote_end () = 0;

	virtual void online_weight_put (nano::write_transaction const &, uint64_t, nano::amount const &) = 0;
	virtual void online_weight_del (nano::write_transaction const &, uint64_t) = 0;
	virtual nano::store_iterator<uint64_t, nano::amount> online_weight_begin (nano::transaction const &) const = 0;
	virtual nano::store_iterator<uint64_t, nano::amount> online_weight_end () const = 0;
	virtual size_t online_weight_count (nano::transaction const &) const = 0;
	virtual void online_weight_clear (nano::write_transaction const &) = 0;

	virtual void version_put (nano::write_transaction const &, int) = 0;
	virtual int version_get (nano::transaction const &) const = 0;

	virtual void peer_put (nano::write_transaction const & transaction_a, nano::endpoint_key const & endpoint_a) = 0;
	virtual void peer_del (nano::write_transaction const & transaction_a, nano::endpoint_key const & endpoint_a) = 0;
	virtual bool peer_exists (nano::transaction const & transaction_a, nano::endpoint_key const & endpoint_a) const = 0;
	virtual size_t peer_count (nano::transaction const & transaction_a) const = 0;
	virtual void peer_clear (nano::write_transaction const & transaction_a) = 0;
	virtual nano::store_iterator<nano::endpoint_key, nano::no_value> peers_begin (nano::transaction const & transaction_a) const = 0;
	virtual nano::store_iterator<nano::endpoint_key, nano::no_value> peers_end () const = 0;

	virtual void confirmation_height_put (nano::write_transaction const & transaction_a, nano::account const & account_a, uint64_t confirmation_height_a) = 0;
	virtual bool confirmation_height_get (nano::transaction const & transaction_a, nano::account const & account_a, uint64_t & confirmation_height_a) = 0;
	virtual bool confirmation_height_exists (nano::transaction const & transaction_a, nano::account const & account_a) const = 0;
	virtual void confirmation_height_del (nano::write_transaction const & transaction_a, nano::account const & account_a) = 0;
	virtual uint64_t confirmation_height_count (nano::transaction const & transaction_a) = 0;
	virtual nano::store_iterator<nano::account, uint64_t> confirmation_height_begin (nano::transaction const & transaction_a, nano::account const & account_a) = 0;
	virtual nano::store_iterator<nano::account, uint64_t> confirmation_height_begin (nano::transaction const & transaction_a) = 0;
	virtual nano::store_iterator<nano::account, uint64_t> confirmation_height_end () = 0;

	virtual uint64_t block_account_height (nano::transaction const & transaction_a, nano::block_hash const & hash_a) const = 0;
	virtual std::mutex & get_cache_mutex () = 0;

	virtual bool copy_db (boost::filesystem::path const & destination) = 0;

	/** Not applicable to all sub-classes */
	virtual void serialize_mdb_tracker (boost::property_tree::ptree &, std::chrono::milliseconds, std::chrono::milliseconds) = 0;

	virtual bool init_error () const = 0;

	/** Start read-write transaction */
	virtual nano::write_transaction tx_begin_write (std::vector<nano::tables> const & tables_to_lock = {}, std::vector<nano::tables> const & tables_no_lock = {}) = 0;

	/** Start read-only transaction */
	virtual nano::read_transaction tx_begin_read () = 0;

	virtual std::string vendor_get () const = 0;
};

std::unique_ptr<nano::block_store> make_store (nano::logger_mt & logger, boost::filesystem::path const & path, bool open_read_only = false, bool add_db_postfix = false, nano::rocksdb_config const & rocksdb_config = nano::rocksdb_config{}, nano::txn_tracking_config const & txn_tracking_config_a = nano::txn_tracking_config{}, std::chrono::milliseconds block_processor_batch_max_time_a = std::chrono::milliseconds (5000), int lmdb_max_dbs = 128, size_t batch_size = 512, bool backup_before_upgrade = false, bool rocksdb_backend = false);
}

namespace std
{
template <>
struct hash<::nano::tables>
{
	size_t operator() (::nano::tables const & table_a) const
	{
		return static_cast<size_t> (table_a);
	}
};
}
