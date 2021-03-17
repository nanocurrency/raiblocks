#pragma once

#include <nano/crypto/blake2/blake2.h>
#include <nano/lib/epoch.hpp>
#include <nano/lib/errors.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/lib/optional_ptr.hpp>
#include <nano/lib/stream.hpp>
#include <nano/lib/utility.hpp>
#include <nano/lib/work.hpp>

#include <boost/property_tree/ptree_fwd.hpp>

#include <unordered_map>

namespace nano
{
class block_visitor;
class mutable_block_visitor;
enum class block_type : uint8_t
{
	invalid = 0,
	not_a_block = 1,
	send = 2,
	receive = 3,
	open = 4,
	change = 5,
	state = 6
};
class block_details
{
	static_assert (std::is_same<std::underlying_type<nano::epoch>::type, uint8_t> (), "Epoch enum is not the proper type");
	static_assert (static_cast<uint8_t> (nano::epoch::max) < (1 << 5), "Epoch max is too large for the sideband");

public:
	block_details () = default;
	block_details (nano::epoch epoch_a, bool is_send_a, bool is_receive_a, bool is_epoch_a);
	static constexpr size_t size ()
	{
		return 1;
	}
	bool operator== (block_details const & other_a) const;
	void serialize (nano::stream &) const;
	bool deserialize (nano::stream &);
	nano::epoch epoch{ nano::epoch::epoch_0 };
	bool is_send{ false };
	bool is_receive{ false };
	bool is_epoch{ false };

private:
	uint8_t packed () const;
	void unpack (uint8_t);
};

std::string state_subtype (nano::block_details);

class block_sideband final
{
public:
	block_sideband () = default;
	block_sideband (nano::account const &, nano::block_hash const &, nano::amount const &, uint64_t, uint64_t, nano::block_details const &, nano::epoch source_epoch_a);
	block_sideband (nano::account const &, nano::block_hash const &, nano::amount const &, uint64_t, uint64_t, nano::epoch epoch_a, bool is_send, bool is_receive, bool is_epoch, nano::epoch source_epoch_a);
	void serialize (nano::stream &, nano::block_type) const;
	bool deserialize (nano::stream &, nano::block_type);
	static size_t size (nano::block_type);
	nano::block_hash successor{ 0 };
	nano::account account{ 0 };
	nano::amount balance{ 0 };
	uint64_t height{ 0 };
	uint64_t timestamp{ 0 };
	nano::block_details details;
	nano::epoch source_epoch{ nano::epoch::epoch_0 };
};
class block
{
public:
	// Return a digest of the hashables in this block.
	nano::block_hash const & hash () const;
	// Return a digest of hashables and non-hashables in this block.
	nano::block_hash full_hash () const;
	nano::block_sideband const & sideband () const;
	void sideband_set (nano::block_sideband const &);
	bool has_sideband () const;
	std::string to_json () const;
	virtual void hash (blake2b_state &) const = 0;
	virtual uint64_t block_work () const = 0;
	virtual void block_work_set (uint64_t) = 0;
	virtual nano::account const & account () const;
	// Previous block in account's chain, zero for open block
	virtual nano::block_hash const & previous () const = 0;
	// Source block for open/receive blocks, zero otherwise.
	virtual nano::block_hash const & source () const;
	// Destination account for send blocks, zero otherwise.
	virtual nano::account const & destination () const;
	// Previous block or account number for open blocks
	virtual nano::root const & root () const = 0;
	// Qualified root value based on previous() and root()
	virtual nano::qualified_root qualified_root () const;
	// Link field for state blocks, zero otherwise.
	virtual nano::link const & link () const;
	virtual nano::account const & representative () const;
	virtual nano::amount const & balance () const;
	virtual void serialize (nano::stream &) const = 0;
	virtual void serialize_json (std::string &, bool = false) const = 0;
	virtual void serialize_json (boost::property_tree::ptree &) const = 0;
	virtual void visit (nano::block_visitor &) const = 0;
	virtual void visit (nano::mutable_block_visitor &) = 0;
	virtual bool operator== (nano::block const &) const = 0;
	virtual nano::block_type type () const = 0;
	virtual nano::signature const & block_signature () const = 0;
	virtual void signature_set (nano::signature const &) = 0;
	virtual ~block () = default;
	virtual bool valid_predecessor (nano::block const &) const = 0;
	static size_t size (nano::block_type);
	virtual nano::work_version work_version () const;
	uint64_t difficulty () const;
	// If there are any changes to the hashables, call this to update the cached hash
	void refresh ();

protected:
	mutable nano::block_hash cached_hash{ 0 };
	/**
	 * Contextual details about a block, some fields may or may not be set depending on block type.
	 * This field is set via sideband_set in ledger processing or deserializing blocks from the database.
	 * Otherwise it may be null (for example, an old block or fork).
	 */
	nano::optional_ptr<nano::block_sideband> sideband_m;

private:
	nano::block_hash generate_hash () const;
};
class send_hashables
{
public:
	send_hashables () = default;
	send_hashables (nano::block_hash const &, nano::account const &, nano::amount const &);
	send_hashables (bool &, nano::stream &);
	send_hashables (bool &, boost::property_tree::ptree const &);
	void hash (blake2b_state &) const;
	nano::block_hash previous;
	nano::account destination;
	nano::amount balance;
	static size_t constexpr size = sizeof (previous) + sizeof (destination) + sizeof (balance);
};
class send_block : public nano::block
{
public:
	send_block () = default;
	send_block (nano::block_hash const &, nano::account const &, nano::amount const &, nano::raw_key const &, nano::public_key const &, uint64_t);
	send_block (bool &, nano::stream &);
	send_block (bool &, boost::property_tree::ptree const &);
	virtual ~send_block () = default;
	using nano::block::hash;
	void hash (blake2b_state &) const override;
	uint64_t block_work () const override;
	void block_work_set (uint64_t) override;
	nano::block_hash const & previous () const override;
	nano::account const & destination () const override;
	nano::root const & root () const override;
	nano::amount const & balance () const override;
	void serialize (nano::stream &) const override;
	bool deserialize (nano::stream &);
	void serialize_json (std::string &, bool = false) const override;
	void serialize_json (boost::property_tree::ptree &) const override;
	bool deserialize_json (boost::property_tree::ptree const &);
	void visit (nano::block_visitor &) const override;
	void visit (nano::mutable_block_visitor &) override;
	nano::block_type type () const override;
	nano::signature const & block_signature () const override;
	void signature_set (nano::signature const &) override;
	bool operator== (nano::block const &) const override;
	bool operator== (nano::send_block const &) const;
	bool valid_predecessor (nano::block const &) const override;
	send_hashables hashables;
	nano::signature signature;
	uint64_t work;
	static size_t constexpr size = nano::send_hashables::size + sizeof (signature) + sizeof (work);
};
class receive_hashables
{
public:
	receive_hashables () = default;
	receive_hashables (nano::block_hash const &, nano::block_hash const &);
	receive_hashables (bool &, nano::stream &);
	receive_hashables (bool &, boost::property_tree::ptree const &);
	void hash (blake2b_state &) const;
	nano::block_hash previous;
	nano::block_hash source;
	static size_t constexpr size = sizeof (previous) + sizeof (source);
};
class receive_block : public nano::block
{
public:
	receive_block () = default;
	receive_block (nano::block_hash const &, nano::block_hash const &, nano::raw_key const &, nano::public_key const &, uint64_t);
	receive_block (bool &, nano::stream &);
	receive_block (bool &, boost::property_tree::ptree const &);
	virtual ~receive_block () = default;
	using nano::block::hash;
	void hash (blake2b_state &) const override;
	uint64_t block_work () const override;
	void block_work_set (uint64_t) override;
	nano::block_hash const & previous () const override;
	nano::block_hash const & source () const override;
	nano::root const & root () const override;
	void serialize (nano::stream &) const override;
	bool deserialize (nano::stream &);
	void serialize_json (std::string &, bool = false) const override;
	void serialize_json (boost::property_tree::ptree &) const override;
	bool deserialize_json (boost::property_tree::ptree const &);
	void visit (nano::block_visitor &) const override;
	void visit (nano::mutable_block_visitor &) override;
	nano::block_type type () const override;
	nano::signature const & block_signature () const override;
	void signature_set (nano::signature const &) override;
	bool operator== (nano::block const &) const override;
	bool operator== (nano::receive_block const &) const;
	bool valid_predecessor (nano::block const &) const override;
	receive_hashables hashables;
	nano::signature signature;
	uint64_t work;
	static size_t constexpr size = nano::receive_hashables::size + sizeof (signature) + sizeof (work);
};
class open_hashables
{
public:
	open_hashables () = default;
	open_hashables (nano::block_hash const &, nano::account const &, nano::account const &);
	open_hashables (bool &, nano::stream &);
	open_hashables (bool &, boost::property_tree::ptree const &);
	void hash (blake2b_state &) const;
	nano::block_hash source;
	nano::account representative;
	nano::account account;
	static size_t constexpr size = sizeof (source) + sizeof (representative) + sizeof (account);
};
class open_block : public nano::block
{
public:
	open_block () = default;
	open_block (nano::block_hash const &, nano::account const &, nano::account const &, nano::raw_key const &, nano::public_key const &, uint64_t);
	open_block (nano::block_hash const &, nano::account const &, nano::account const &, std::nullptr_t);
	open_block (bool &, nano::stream &);
	open_block (bool &, boost::property_tree::ptree const &);
	virtual ~open_block () = default;
	using nano::block::hash;
	void hash (blake2b_state &) const override;
	uint64_t block_work () const override;
	void block_work_set (uint64_t) override;
	nano::block_hash const & previous () const override;
	nano::account const & account () const override;
	nano::block_hash const & source () const override;
	nano::root const & root () const override;
	nano::account const & representative () const override;
	void serialize (nano::stream &) const override;
	bool deserialize (nano::stream &);
	void serialize_json (std::string &, bool = false) const override;
	void serialize_json (boost::property_tree::ptree &) const override;
	bool deserialize_json (boost::property_tree::ptree const &);
	void visit (nano::block_visitor &) const override;
	void visit (nano::mutable_block_visitor &) override;
	nano::block_type type () const override;
	nano::signature const & block_signature () const override;
	void signature_set (nano::signature const &) override;
	bool operator== (nano::block const &) const override;
	bool operator== (nano::open_block const &) const;
	bool valid_predecessor (nano::block const &) const override;
	nano::open_hashables hashables;
	nano::signature signature;
	uint64_t work;
	static size_t constexpr size = nano::open_hashables::size + sizeof (signature) + sizeof (work);
};
class change_hashables
{
public:
	change_hashables () = default;
	change_hashables (nano::block_hash const &, nano::account const &);
	change_hashables (bool &, nano::stream &);
	change_hashables (bool &, boost::property_tree::ptree const &);
	void hash (blake2b_state &) const;
	nano::block_hash previous;
	nano::account representative;
	static size_t constexpr size = sizeof (previous) + sizeof (representative);
};
class change_block : public nano::block
{
public:
	change_block () = default;
	change_block (nano::block_hash const &, nano::account const &, nano::raw_key const &, nano::public_key const &, uint64_t);
	change_block (bool &, nano::stream &);
	change_block (bool &, boost::property_tree::ptree const &);
	virtual ~change_block () = default;
	using nano::block::hash;
	void hash (blake2b_state &) const override;
	uint64_t block_work () const override;
	void block_work_set (uint64_t) override;
	nano::block_hash const & previous () const override;
	nano::root const & root () const override;
	nano::account const & representative () const override;
	void serialize (nano::stream &) const override;
	bool deserialize (nano::stream &);
	void serialize_json (std::string &, bool = false) const override;
	void serialize_json (boost::property_tree::ptree &) const override;
	bool deserialize_json (boost::property_tree::ptree const &);
	void visit (nano::block_visitor &) const override;
	void visit (nano::mutable_block_visitor &) override;
	nano::block_type type () const override;
	nano::signature const & block_signature () const override;
	void signature_set (nano::signature const &) override;
	bool operator== (nano::block const &) const override;
	bool operator== (nano::change_block const &) const;
	bool valid_predecessor (nano::block const &) const override;
	nano::change_hashables hashables;
	nano::signature signature;
	uint64_t work;
	static size_t constexpr size = nano::change_hashables::size + sizeof (signature) + sizeof (work);
};
class state_hashables
{
public:
	state_hashables () = default;
	state_hashables (nano::account const &, nano::block_hash const &, nano::account const &, nano::amount const &, nano::link const &);
	state_hashables (bool &, nano::stream &);
	state_hashables (bool &, boost::property_tree::ptree const &);
	void hash (blake2b_state &) const;
	// Account# / public key that operates this account
	// Uses:
	// Bulk signature validation in advance of further ledger processing
	// Arranging uncomitted transactions by account
	nano::account account;
	// Previous transaction in this chain
	nano::block_hash previous;
	// Representative of this account
	nano::account representative;
	// Current balance of this account
	// Allows lookup of account balance simply by looking at the head block
	nano::amount balance;
	// Link field contains source block_hash if receiving, destination account if sending
	nano::link link;
	// Serialized size
	static size_t constexpr size = sizeof (account) + sizeof (previous) + sizeof (representative) + sizeof (balance) + sizeof (link);
};
class state_block : public nano::block
{
public:
	state_block () = default;
	state_block (nano::account const &, nano::block_hash const &, nano::account const &, nano::amount const &, nano::link const &, nano::raw_key const &, nano::public_key const &, uint64_t);
	state_block (bool &, nano::stream &);
	state_block (bool &, boost::property_tree::ptree const &);
	virtual ~state_block () = default;
	using nano::block::hash;
	void hash (blake2b_state &) const override;
	uint64_t block_work () const override;
	void block_work_set (uint64_t) override;
	nano::block_hash const & previous () const override;
	nano::account const & account () const override;
	nano::root const & root () const override;
	nano::link const & link () const override;
	nano::account const & representative () const override;
	nano::amount const & balance () const override;
	void serialize (nano::stream &) const override;
	bool deserialize (nano::stream &);
	void serialize_json (std::string &, bool = false) const override;
	void serialize_json (boost::property_tree::ptree &) const override;
	bool deserialize_json (boost::property_tree::ptree const &);
	void visit (nano::block_visitor &) const override;
	void visit (nano::mutable_block_visitor &) override;
	nano::block_type type () const override;
	nano::signature const & block_signature () const override;
	void signature_set (nano::signature const &) override;
	bool operator== (nano::block const &) const override;
	bool operator== (nano::state_block const &) const;
	bool valid_predecessor (nano::block const &) const override;
	nano::state_hashables hashables;
	nano::signature signature;
	uint64_t work;
	static size_t constexpr size = nano::state_hashables::size + sizeof (signature) + sizeof (work);
};
class block_visitor
{
public:
	virtual void send_block (nano::send_block const &) = 0;
	virtual void receive_block (nano::receive_block const &) = 0;
	virtual void open_block (nano::open_block const &) = 0;
	virtual void change_block (nano::change_block const &) = 0;
	virtual void state_block (nano::state_block const &) = 0;
	virtual ~block_visitor () = default;
};
class mutable_block_visitor
{
public:
	virtual void send_block (nano::send_block &) = 0;
	virtual void receive_block (nano::receive_block &) = 0;
	virtual void open_block (nano::open_block &) = 0;
	virtual void change_block (nano::change_block &) = 0;
	virtual void state_block (nano::state_block &) = 0;
	virtual ~mutable_block_visitor () = default;
};
/**
 * This class serves to find and return unique variants of a block in order to minimize memory usage
 */
class block_uniquer
{
public:
	using value_type = std::pair<const nano::uint256_union, std::weak_ptr<nano::block>>;

	std::shared_ptr<nano::block> unique (std::shared_ptr<nano::block> const &);
	size_t size ();

private:
	nano::mutex mutex{ mutex_identifier (mutexes::block_uniquer) };
	std::unordered_map<std::remove_const_t<value_type::first_type>, value_type::second_type> blocks;
	static unsigned constexpr cleanup_count = 2;
};

std::unique_ptr<container_info_component> collect_container_info (block_uniquer & block_uniquer, std::string const & name);

std::shared_ptr<nano::block> deserialize_block (nano::stream &);
std::shared_ptr<nano::block> deserialize_block (nano::stream &, nano::block_type, nano::block_uniquer * = nullptr);
std::shared_ptr<nano::block> deserialize_block_json (boost::property_tree::ptree const &, nano::block_uniquer * = nullptr);
void serialize_block (nano::stream &, nano::block const &);
void block_memory_pool_purge ();
}
