#include <nano/core_test/testutil.hpp>
#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/config.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/common.hpp>

#include <crypto/cryptopp/words.h>

#include <boost/endian/conversion.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/variant/get.hpp>

#include <limits>
#include <queue>

#include <crypto/ed25519-donna/ed25519.h>

size_t constexpr nano::send_block::size;
size_t constexpr nano::receive_block::size;
size_t constexpr nano::open_block::size;
size_t constexpr nano::change_block::size;
size_t constexpr nano::state_block::size;

nano::nano_networks nano::network_constants::active_network = nano::nano_networks::ACTIVE_NETWORK;

namespace
{
char const * test_private_key_data = "34F0A37AAD20F4A260F0A5B3CB3D7FB50673212263E58A380BC10474BB039CE4";
char const * test_public_key_data = "B0311EA55708D6A53C75CDBF88300259C6D018522FE3D4D0A242E431F9E8B6D0"; // xrb_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo
char const * beta_public_key_data = "A59A439B34662385D48F7FF9CA50030F889BAA9AC320EA5A85AAD777CF82B088"; // nano_3betagfmasj5iqcayzzssba185wamgobois1xbfadcpqgz9r7e6a1zwztn5o
char const * live_public_key_data = "E89208DD038FBB269987689621D52292AE9C35941A7484756ECCED92A65093BA"; // xrb_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3
char const * test_genesis_data = R"%%%({
	"type": "open",
	"source": "B0311EA55708D6A53C75CDBF88300259C6D018522FE3D4D0A242E431F9E8B6D0",
	"representative": "xrb_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo",
	"account": "xrb_3e3j5tkog48pnny9dmfzj1r16pg8t1e76dz5tmac6iq689wyjfpiij4txtdo",
	"work": "7b42a00ee91d5810",
	"signature": "ECDA914373A2F0CA1296475BAEE40500A7F0A7AD72A5A80C81D7FAB7F6C802B2CC7DB50F5DD0FB25B2EF11761FA7344A158DD5A700B21BD47DE5BD0F63153A02"
	})%%%";

char const * beta_genesis_data = R"%%%({
	"type": "open",
	"source": "A59A439B34662385D48F7FF9CA50030F889BAA9AC320EA5A85AAD777CF82B088",
	"representative": "nano_3betagfmasj5iqcayzzssba185wamgobois1xbfadcpqgz9r7e6a1zwztn5o",
	"account": "nano_3betagfmasj5iqcayzzssba185wamgobois1xbfadcpqgz9r7e6a1zwztn5o",
	"work": "cb4efb49972c7106",
	"signature": "C14ACC986C0561871B3382A93F5E64AFE55A3FA2EEE6892A7358DC817E2034AA56160CAA807A21EDFCFA528D034CD266F9ABFA4E2FF221D856255265BFDC0608"
	})%%%";

char const * live_genesis_data = R"%%%({
	"type": "open",
	"source": "E89208DD038FBB269987689621D52292AE9C35941A7484756ECCED92A65093BA",
	"representative": "xrb_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
	"account": "xrb_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
	"work": "62f05417dd3fb691",
	"signature": "9F0C933C8ADE004D808EA1985FA746A7E95BA2A38F867640F53EC8F180BDFE9E2C1268DEAD7C2664F356E37ABA362BC58E46DBA03E523A7B5A19E4B6EB12BB02"
	})%%%";
}

nano::network_params::network_params ()
	: network_params (network_constants::active_network)
{
}

nano::network_params::network_params (nano::nano_networks network_a)
	: network (network_a)
	, protocol (network_a)
	, ledger (network)
	, voting (network)
	, node (network)
	, portmapping (network)
	, bootstrap (network)
{
	unsigned constexpr kdf_full_work = 64 * 1024;
	unsigned constexpr kdf_test_work = 8;
	kdf_work = network.is_test_network () ? kdf_test_work : kdf_full_work;
	header_magic_number = network.is_test_network () ? std::array<uint8_t, 2>{ { 'R', 'A' } } : network.is_beta_network () ? std::array<uint8_t, 2>{ { 'N', 'B' } } : std::array<uint8_t, 2>{ { 'R', 'C' } };
}

nano::protocol_constants::protocol_constants (nano::nano_networks network_a)
{
}

nano::ledger_constants::ledger_constants (nano::network_constants & network_constants)
	: ledger_constants (network_constants.network ())
{
}

nano::ledger_constants::ledger_constants (nano::nano_networks network_a)
	: zero_key ("0")
	, test_genesis_key (test_private_key_data)
	, nano_test_account (test_public_key_data)
	, nano_beta_account (beta_public_key_data)
	, nano_live_account (live_public_key_data)
	, nano_test_genesis (test_genesis_data)
	, nano_beta_genesis (beta_genesis_data)
	, nano_live_genesis (live_genesis_data)
	, genesis_account (network_a == nano::nano_networks::nano_test_network ? nano_test_account : network_a == nano::nano_networks::nano_beta_network ? nano_beta_account : nano_live_account)
	, genesis_block (network_a == nano::nano_networks::nano_test_network ? nano_test_genesis : network_a == nano::nano_networks::nano_beta_network ? nano_beta_genesis : nano_live_genesis)
	, genesis_amount (std::numeric_limits<nano::uint128_t>::max ())
	, burn_account (0)
{
	nano::link epoch_link_v1;
	const char * epoch_message_v1 ("epoch v1 block");
	strncpy ((char *)epoch_link_v1.bytes.data (), epoch_message_v1, epoch_link_v1.bytes.size ());
	epochs.add (nano::epoch::epoch_1, genesis_account, epoch_link_v1);

	nano::link epoch_link_v2;
	auto nano_live_epoch_v2_signer = genesis_account;
	auto epoch_v2_signer (network_a == nano::nano_networks::nano_test_network ? nano_test_account : network_a == nano::nano_networks::nano_beta_network ? nano_beta_account : nano_live_epoch_v2_signer);
	const char * epoch_message_v2 ("epoch v2 block");
	strncpy ((char *)epoch_link_v2.bytes.data (), epoch_message_v2, epoch_link_v2.bytes.size ());
	epochs.add (nano::epoch::epoch_2, epoch_v2_signer, epoch_link_v2);
}

nano::random_constants::random_constants ()
{
	nano::random_pool::generate_block (not_an_account.bytes.data (), not_an_account.bytes.size ());
	nano::random_pool::generate_block (random_128.bytes.data (), random_128.bytes.size ());
}

nano::node_constants::node_constants (nano::network_constants & network_constants)
{
	period = network_constants.is_test_network () ? std::chrono::seconds (1) : std::chrono::seconds (60);
	half_period = network_constants.is_test_network () ? std::chrono::milliseconds (500) : std::chrono::milliseconds (30 * 1000);
	idle_timeout = network_constants.is_test_network () ? period * 15 : period * 2;
	cutoff = period * 5;
	syn_cookie_cutoff = std::chrono::seconds (5);
	backup_interval = std::chrono::minutes (5);
	search_pending_interval = network_constants.is_test_network () ? std::chrono::seconds (1) : std::chrono::seconds (5 * 60);
	peer_interval = search_pending_interval;
	unchecked_cleaning_interval = std::chrono::minutes (30);
	process_confirmed_interval = network_constants.is_test_network () ? std::chrono::milliseconds (50) : std::chrono::milliseconds (500);
	max_weight_samples = network_constants.is_live_network () ? 4032 : 864;
	weight_period = 5 * 60; // 5 minutes
}

nano::voting_constants::voting_constants (nano::network_constants & network_constants)
{
	max_cache = network_constants.is_test_network () ? 2 : 4 * 1024;
}

nano::portmapping_constants::portmapping_constants (nano::network_constants & network_constants)
{
	mapping_timeout = network_constants.is_test_network () ? 53 : 3593;
	check_timeout = network_constants.is_test_network () ? 17 : 53;
}

nano::bootstrap_constants::bootstrap_constants (nano::network_constants & network_constants)
{
	lazy_max_pull_blocks = network_constants.is_test_network () ? 2 : 512;
	lazy_min_pull_blocks = network_constants.is_test_network () ? 1 : 32;
	frontier_retry_limit = network_constants.is_test_network () ? 2 : 16;
	lazy_retry_limit = network_constants.is_test_network () ? 2 : frontier_retry_limit * 10;
	lazy_destinations_retry_limit = network_constants.is_test_network () ? 1 : frontier_retry_limit / 4;
}

/* Convenience constants for core_test which is always on the test network */
namespace
{
nano::ledger_constants test_constants (nano::nano_networks::nano_test_network);
}

nano::keypair const & nano::zero_key (test_constants.zero_key);
nano::keypair const & nano::test_genesis_key (test_constants.test_genesis_key);
nano::account const & nano::nano_test_account (test_constants.nano_test_account);
std::string const & nano::nano_test_genesis (test_constants.nano_test_genesis);
nano::account const & nano::genesis_account (test_constants.genesis_account);
std::string const & nano::genesis_block (test_constants.genesis_block);
nano::uint128_t const & nano::genesis_amount (test_constants.genesis_amount);
nano::account const & nano::burn_account (test_constants.burn_account);

// Create a new random keypair
nano::keypair::keypair ()
{
	random_pool::generate_block (prv.data.bytes.data (), prv.data.bytes.size ());
	ed25519_publickey (prv.data.bytes.data (), pub.bytes.data ());
}

// Create a keypair given a private key
nano::keypair::keypair (nano::raw_key && prv_a)
	: prv (std::move (prv_a))
{
	ed25519_publickey (prv.data.bytes.data (), pub.bytes.data ());
}

// Create a keypair given a hex string of the private key
nano::keypair::keypair (std::string const & prv_a)
{
	auto error (prv.data.decode_hex (prv_a));
	(void)error;
	assert (!error);
	ed25519_publickey (prv.data.bytes.data (), pub.bytes.data ());
}

// Serialize a block prefixed with an 8-bit typecode
void nano::serialize_block (nano::stream & stream_a, nano::block const & block_a)
{
	write (stream_a, block_a.type ());
	block_a.serialize (stream_a);
}

nano::account_info::account_info (nano::block_hash const & head_a, nano::account const & representative_a, nano::block_hash const & open_block_a, nano::amount const & balance_a, uint64_t modified_a, uint64_t block_count_a, nano::epoch epoch_a)
	: head (head_a)
	, representative (representative_a)
	, open_block (open_block_a)
	, balance (balance_a)
	, modified (modified_a)
	, block_count (block_count_a)
	, epoch_m (epoch_a)
{
}

bool nano::account_info::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		nano::read (stream_a, head.bytes);
		nano::read (stream_a, representative.bytes);
		nano::read (stream_a, open_block.bytes);
		nano::read (stream_a, balance.bytes);
		nano::read (stream_a, modified);
		nano::read (stream_a, block_count);
		nano::read (stream_a, epoch_m);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

bool nano::account_info::operator== (nano::account_info const & other_a) const
{
	return head == other_a.head && representative == other_a.representative && open_block == other_a.open_block && balance == other_a.balance && modified == other_a.modified && block_count == other_a.block_count && epoch () == other_a.epoch ();
}

bool nano::account_info::operator!= (nano::account_info const & other_a) const
{
	return !(*this == other_a);
}

size_t nano::account_info::db_size () const
{
	assert (reinterpret_cast<const uint8_t *> (this) == reinterpret_cast<const uint8_t *> (&head));
	assert (reinterpret_cast<const uint8_t *> (&head) + sizeof (head) == reinterpret_cast<const uint8_t *> (&representative));
	assert (reinterpret_cast<const uint8_t *> (&representative) + sizeof (representative) == reinterpret_cast<const uint8_t *> (&open_block));
	assert (reinterpret_cast<const uint8_t *> (&open_block) + sizeof (open_block) == reinterpret_cast<const uint8_t *> (&balance));
	assert (reinterpret_cast<const uint8_t *> (&balance) + sizeof (balance) == reinterpret_cast<const uint8_t *> (&modified));
	assert (reinterpret_cast<const uint8_t *> (&modified) + sizeof (modified) == reinterpret_cast<const uint8_t *> (&block_count));
	assert (reinterpret_cast<const uint8_t *> (&block_count) + sizeof (block_count) == reinterpret_cast<const uint8_t *> (&epoch_m));
	return sizeof (head) + sizeof (representative) + sizeof (open_block) + sizeof (balance) + sizeof (modified) + sizeof (block_count) + sizeof (epoch_m);
}

nano::epoch nano::account_info::epoch () const
{
	return epoch_m;
}

size_t nano::block_counts::sum () const
{
	return send + receive + open + change + state;
}

nano::pending_info::pending_info (nano::account const & source_a, nano::amount const & amount_a, nano::epoch epoch_a)
	: source (source_a)
	, amount (amount_a)
	, epoch (epoch_a)
{
}

bool nano::pending_info::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		nano::read (stream_a, source.bytes);
		nano::read (stream_a, amount.bytes);
		nano::read (stream_a, epoch);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

size_t nano::pending_info::db_size () const
{
	return sizeof (source) + sizeof (amount) + sizeof (epoch);
}

bool nano::pending_info::operator== (nano::pending_info const & other_a) const
{
	return source == other_a.source && amount == other_a.amount && epoch == other_a.epoch;
}

nano::pending_key::pending_key (nano::account const & account_a, nano::block_hash const & hash_a)
	: account (account_a)
	, hash (hash_a)
{
}

bool nano::pending_key::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		nano::read (stream_a, account.bytes);
		nano::read (stream_a, hash.bytes);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

bool nano::pending_key::operator== (nano::pending_key const & other_a) const
{
	return account == other_a.account && hash == other_a.hash;
}

nano::account const & nano::pending_key::key () const
{
	return account;
}

nano::unchecked_info::unchecked_info (std::shared_ptr<nano::block> block_a, nano::account const & account_a, uint64_t modified_a, nano::signature_verification verified_a, bool confirmed_a)
	: block (block_a)
	, account (account_a)
	, modified (modified_a)
	, verified (verified_a)
	, confirmed (confirmed_a)
{
}

void nano::unchecked_info::serialize (nano::stream & stream_a) const
{
	assert (block != nullptr);
	nano::serialize_block (stream_a, *block);
	nano::write (stream_a, account.bytes);
	nano::write (stream_a, modified);
	nano::write (stream_a, verified);
}

bool nano::unchecked_info::deserialize (nano::stream & stream_a)
{
	block = nano::deserialize_block (stream_a);
	bool error (block == nullptr);
	if (!error)
	{
		try
		{
			nano::read (stream_a, account.bytes);
			nano::read (stream_a, modified);
			nano::read (stream_a, verified);
		}
		catch (std::runtime_error const &)
		{
			error = true;
		}
	}
	return error;
}

nano::endpoint_key::endpoint_key (const std::array<uint8_t, 16> & address_a, uint16_t port_a)
	: address (address_a)
	, network_port (boost::endian::native_to_big (port_a))
{
}

const std::array<uint8_t, 16> & nano::endpoint_key::address_bytes () const
{
	return address;
}

uint16_t nano::endpoint_key::port () const
{
	return boost::endian::big_to_native (network_port);
}

nano::block_info::block_info (nano::account const & account_a, nano::amount const & balance_a)
	: account (account_a)
	, balance (balance_a)
{
}

bool nano::vote::operator== (nano::vote const & other_a) const
{
	auto blocks_equal (true);
	if (blocks.size () != other_a.blocks.size ())
	{
		blocks_equal = false;
	}
	else
	{
		for (auto i (0); blocks_equal && i < blocks.size (); ++i)
		{
			auto block (blocks[i]);
			auto other_block (other_a.blocks[i]);
			if (block.which () != other_block.which ())
			{
				blocks_equal = false;
			}
			else if (block.which ())
			{
				if (boost::get<nano::block_hash> (block) != boost::get<nano::block_hash> (other_block))
				{
					blocks_equal = false;
				}
			}
			else
			{
				if (!(*boost::get<std::shared_ptr<nano::block>> (block) == *boost::get<std::shared_ptr<nano::block>> (other_block)))
				{
					blocks_equal = false;
				}
			}
		}
	}
	return sequence == other_a.sequence && blocks_equal && account == other_a.account && signature == other_a.signature;
}

bool nano::vote::operator!= (nano::vote const & other_a) const
{
	return !(*this == other_a);
}

void nano::vote::serialize_json (boost::property_tree::ptree & tree) const
{
	tree.put ("account", account.to_account ());
	tree.put ("signature", signature.number ());
	tree.put ("sequence", std::to_string (sequence));
	boost::property_tree::ptree blocks_tree;
	for (auto block : blocks)
	{
		boost::property_tree::ptree entry;
		if (block.which ())
		{
			entry.put ("", boost::get<nano::block_hash> (block).to_string ());
		}
		else
		{
			entry.put ("", boost::get<std::shared_ptr<nano::block>> (block)->hash ().to_string ());
		}
		blocks_tree.push_back (std::make_pair ("", entry));
	}
	tree.add_child ("blocks", blocks_tree);
}

std::string nano::vote::to_json () const
{
	std::stringstream stream;
	boost::property_tree::ptree tree;
	serialize_json (tree);
	boost::property_tree::write_json (stream, tree);
	return stream.str ();
}

nano::vote::vote (nano::vote const & other_a)
	: sequence (other_a.sequence)
	, blocks (other_a.blocks)
	, account (other_a.account)
	, signature (other_a.signature)
{
}

nano::vote::vote (bool & error_a, nano::stream & stream_a, nano::block_uniquer * uniquer_a)
{
	error_a = deserialize (stream_a, uniquer_a);
}

nano::vote::vote (bool & error_a, nano::stream & stream_a, nano::block_type type_a, nano::block_uniquer * uniquer_a)
{
	try
	{
		nano::read (stream_a, account.bytes);
		nano::read (stream_a, signature.bytes);
		nano::read (stream_a, sequence);

		while (stream_a.in_avail () > 0)
		{
			if (type_a == nano::block_type::not_a_block)
			{
				nano::block_hash block_hash;
				nano::read (stream_a, block_hash);
				blocks.push_back (block_hash);
			}
			else
			{
				std::shared_ptr<nano::block> block (nano::deserialize_block (stream_a, type_a, uniquer_a));
				if (block == nullptr)
				{
					throw std::runtime_error ("Block is null");
				}
				blocks.push_back (block);
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}

	if (blocks.empty ())
	{
		error_a = true;
	}
}

nano::vote::vote (nano::account const & account_a, nano::raw_key const & prv_a, uint64_t sequence_a, std::shared_ptr<nano::block> block_a)
	: sequence (sequence_a)
	, blocks (1, block_a)
	, account (account_a)
	, signature (nano::sign_message (prv_a, account_a, hash ()))
{
}

nano::vote::vote (nano::account const & account_a, nano::raw_key const & prv_a, uint64_t sequence_a, std::vector<nano::block_hash> const & blocks_a)
	: sequence (sequence_a)
	, account (account_a)
{
	assert (!blocks_a.empty ());
	assert (blocks_a.size () <= 12);
	blocks.reserve (blocks_a.size ());
	std::copy (blocks_a.cbegin (), blocks_a.cend (), std::back_inserter (blocks));
	signature = nano::sign_message (prv_a, account_a, hash ());
}

std::string nano::vote::hashes_string () const
{
	std::string result;
	for (auto hash : *this)
	{
		result += hash.to_string ();
		result += ", ";
	}
	return result;
}

const std::string nano::vote::hash_prefix = "vote ";

nano::block_hash nano::vote::hash () const
{
	nano::block_hash result;
	blake2b_state hash;
	blake2b_init (&hash, sizeof (result.bytes));
	if (blocks.size () > 1 || (!blocks.empty () && blocks.front ().which ()))
	{
		blake2b_update (&hash, hash_prefix.data (), hash_prefix.size ());
	}
	for (auto block_hash : *this)
	{
		blake2b_update (&hash, block_hash.bytes.data (), sizeof (block_hash.bytes));
	}
	union
	{
		uint64_t qword;
		std::array<uint8_t, 8> bytes;
	};
	qword = sequence;
	blake2b_update (&hash, bytes.data (), sizeof (bytes));
	blake2b_final (&hash, result.bytes.data (), sizeof (result.bytes));
	return result;
}

nano::block_hash nano::vote::full_hash () const
{
	nano::block_hash result;
	blake2b_state state;
	blake2b_init (&state, sizeof (result.bytes));
	blake2b_update (&state, hash ().bytes.data (), sizeof (hash ().bytes));
	blake2b_update (&state, account.bytes.data (), sizeof (account.bytes.data ()));
	blake2b_update (&state, signature.bytes.data (), sizeof (signature.bytes.data ()));
	blake2b_final (&state, result.bytes.data (), sizeof (result.bytes));
	return result;
}

void nano::vote::serialize (nano::stream & stream_a, nano::block_type type) const
{
	write (stream_a, account);
	write (stream_a, signature);
	write (stream_a, sequence);
	for (auto const & block : blocks)
	{
		if (block.which ())
		{
			assert (type == nano::block_type::not_a_block);
			write (stream_a, boost::get<nano::block_hash> (block));
		}
		else
		{
			if (type == nano::block_type::not_a_block)
			{
				write (stream_a, boost::get<std::shared_ptr<nano::block>> (block)->hash ());
			}
			else
			{
				boost::get<std::shared_ptr<nano::block>> (block)->serialize (stream_a);
			}
		}
	}
}

void nano::vote::serialize (nano::stream & stream_a) const
{
	write (stream_a, account);
	write (stream_a, signature);
	write (stream_a, sequence);
	for (auto const & block : blocks)
	{
		if (block.which ())
		{
			write (stream_a, nano::block_type::not_a_block);
			write (stream_a, boost::get<nano::block_hash> (block));
		}
		else
		{
			nano::serialize_block (stream_a, *boost::get<std::shared_ptr<nano::block>> (block));
		}
	}
}

bool nano::vote::deserialize (nano::stream & stream_a, nano::block_uniquer * uniquer_a)
{
	auto error (false);
	try
	{
		nano::read (stream_a, account);
		nano::read (stream_a, signature);
		nano::read (stream_a, sequence);

		nano::block_type type;

		while (true)
		{
			if (nano::try_read (stream_a, type))
			{
				// Reached the end of the stream
				break;
			}

			if (type == nano::block_type::not_a_block)
			{
				nano::block_hash block_hash;
				nano::read (stream_a, block_hash);
				blocks.push_back (block_hash);
			}
			else
			{
				std::shared_ptr<nano::block> block (nano::deserialize_block (stream_a, type, uniquer_a));
				if (block == nullptr)
				{
					throw std::runtime_error ("Block is empty");
				}

				blocks.push_back (block);
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	if (blocks.empty ())
	{
		error = true;
	}

	return error;
}

bool nano::vote::validate () const
{
	return nano::validate_message (account, hash (), signature);
}

nano::block_hash nano::iterate_vote_blocks_as_hash::operator() (boost::variant<std::shared_ptr<nano::block>, nano::block_hash> const & item) const
{
	nano::block_hash result;
	if (item.which ())
	{
		result = boost::get<nano::block_hash> (item);
	}
	else
	{
		result = boost::get<std::shared_ptr<nano::block>> (item)->hash ();
	}
	return result;
}

boost::transform_iterator<nano::iterate_vote_blocks_as_hash, nano::vote_blocks_vec_iter> nano::vote::begin () const
{
	return boost::transform_iterator<nano::iterate_vote_blocks_as_hash, nano::vote_blocks_vec_iter> (blocks.begin (), nano::iterate_vote_blocks_as_hash ());
}

boost::transform_iterator<nano::iterate_vote_blocks_as_hash, nano::vote_blocks_vec_iter> nano::vote::end () const
{
	return boost::transform_iterator<nano::iterate_vote_blocks_as_hash, nano::vote_blocks_vec_iter> (blocks.end (), nano::iterate_vote_blocks_as_hash ());
}

nano::vote_uniquer::vote_uniquer (nano::block_uniquer & uniquer_a)
	: uniquer (uniquer_a)
{
}

std::shared_ptr<nano::vote> nano::vote_uniquer::unique (std::shared_ptr<nano::vote> vote_a)
{
	auto result (vote_a);
	if (result != nullptr && !result->blocks.empty ())
	{
		if (!result->blocks.front ().which ())
		{
			result->blocks.front () = uniquer.unique (boost::get<std::shared_ptr<nano::block>> (result->blocks.front ()));
		}
		nano::block_hash key (vote_a->full_hash ());
		nano::lock_guard<std::mutex> lock (mutex);
		auto & existing (votes[key]);
		if (auto block_l = existing.lock ())
		{
			result = block_l;
		}
		else
		{
			existing = vote_a;
		}

		release_assert (std::numeric_limits<CryptoPP::word32>::max () > votes.size ());
		for (auto i (0); i < cleanup_count && !votes.empty (); ++i)
		{
			auto random_offset = nano::random_pool::generate_word32 (0, static_cast<CryptoPP::word32> (votes.size () - 1));

			auto existing (std::next (votes.begin (), random_offset));
			if (existing == votes.end ())
			{
				existing = votes.begin ();
			}
			if (existing != votes.end ())
			{
				if (auto block_l = existing->second.lock ())
				{
					// Still live
				}
				else
				{
					votes.erase (existing);
				}
			}
		}
	}
	return result;
}

size_t nano::vote_uniquer::size ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	return votes.size ();
}

namespace nano
{
std::unique_ptr<seq_con_info_component> collect_seq_con_info (vote_uniquer & vote_uniquer, const std::string & name)
{
	auto count = vote_uniquer.size ();
	auto sizeof_element = sizeof (vote_uniquer::value_type);
	auto composite = std::make_unique<seq_con_info_composite> (name);
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "votes", count, sizeof_element }));
	return composite;
}
}

nano::genesis::genesis ()
{
	static nano::network_params network_params;
	boost::property_tree::ptree tree;
	std::stringstream istream (network_params.ledger.genesis_block);
	boost::property_tree::read_json (istream, tree);
	open = nano::deserialize_block_json (tree);
	assert (open != nullptr);
}

nano::block_hash nano::genesis::hash () const
{
	return open->hash ();
}

nano::wallet_id nano::random_wallet_id ()
{
	nano::wallet_id wallet_id;
	nano::uint256_union dummy_secret;
	random_pool::generate_block (dummy_secret.bytes.data (), dummy_secret.bytes.size ());
	ed25519_publickey (dummy_secret.bytes.data (), wallet_id.bytes.data ());
	return wallet_id;
}

nano::unchecked_key::unchecked_key (nano::block_hash const & previous_a, nano::block_hash const & hash_a)
	: previous (previous_a)
	, hash (hash_a)
{
}

bool nano::unchecked_key::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		nano::read (stream_a, previous.bytes);
		nano::read (stream_a, hash.bytes);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

bool nano::unchecked_key::operator== (nano::unchecked_key const & other_a) const
{
	return previous == other_a.previous && hash == other_a.hash;
}

nano::block_hash const & nano::unchecked_key::key () const
{
	return previous;
}
