#include <nano/lib/blocks.hpp>
#include <nano/lib/memory.hpp>
#include <nano/lib/work.hpp>
#include <nano/node/common.hpp>
#include <nano/node/election.hpp>
#include <nano/node/wallet.hpp>
#include <nano/secure/buffer.hpp>

#include <boost/endian/conversion.hpp>
#include <boost/pool/pool_alloc.hpp>
#include <boost/variant/get.hpp>

#include <numeric>

std::bitset<16> constexpr nano::message_header::block_type_mask;
std::bitset<16> constexpr nano::message_header::count_mask;
std::bitset<16> constexpr nano::message_header::telemetry_size_mask;

std::chrono::seconds constexpr nano::telemetry_cache_cutoffs::dev;
std::chrono::seconds constexpr nano::telemetry_cache_cutoffs::beta;
std::chrono::seconds constexpr nano::telemetry_cache_cutoffs::live;

namespace
{
nano::protocol_constants const & get_protocol_constants ()
{
	static nano::environment_constants constants;
	return constants.protocol;
}
}

uint64_t nano::ip_address_hash_raw (boost::asio::ip::address const & ip_a, uint16_t port)
{
	static nano::random_constants constants;
	debug_assert (ip_a.is_v6 ());
	uint64_t result;
	nano::uint128_union address;
	address.bytes = ip_a.to_v6 ().to_bytes ();
	blake2b_state state;
	blake2b_init (&state, sizeof (result));
	blake2b_update (&state, constants.random_128.bytes.data (), constants.random_128.bytes.size ());
	if (port != 0)
	{
		blake2b_update (&state, &port, sizeof (port));
	}
	blake2b_update (&state, address.bytes.data (), address.bytes.size ());
	blake2b_final (&state, &result, sizeof (result));
	return result;
}

nano::message_header::message_header (nano::message_type type_a) :
version_max (get_protocol_constants ().protocol_version),
version_using (get_protocol_constants ().protocol_version),
type (type_a)
{
}

nano::message_header::message_header (bool & error_a, nano::stream & stream_a)
{
	if (!error_a)
	{
		error_a = deserialize (stream_a);
	}
}

void nano::message_header::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	static nano::environment_constants constants;
	nano::write (stream_a, constants.header_magic_number);
	nano::write (stream_a, version_max);
	nano::write (stream_a, version_using);
	nano::write (stream_a, get_protocol_constants ().protocol_version_min (use_epoch_2_min_version_a));
	nano::write (stream_a, type);
	nano::write (stream_a, static_cast<uint16_t> (extensions.to_ullong ()));
}

bool nano::message_header::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		static nano::environment_constants constants;
		uint16_t extensions_l;
		std::array<uint8_t, 2> magic_number_l;
		read (stream_a, magic_number_l);
		if (magic_number_l != constants.header_magic_number)
		{
			throw std::runtime_error ("Magic numbers do not match");
		}

		nano::read (stream_a, version_max);
		nano::read (stream_a, version_using);
		nano::read (stream_a, version_min_m);
		nano::read (stream_a, type);
		nano::read (stream_a, extensions_l);
		extensions = extensions_l;
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

uint8_t nano::message_header::version_min () const
{
	debug_assert (version_min_m != std::numeric_limits<uint8_t>::max ());
	return version_min_m;
}

nano::message::message (nano::message_type type_a) :
header (type_a)
{
}

nano::message::message (nano::message_header const & header_a) :
header (header_a)
{
}

std::shared_ptr<std::vector<uint8_t>> nano::message::to_bytes (bool use_epoch_2_min_version_a) const
{
	auto bytes = std::make_shared<std::vector<uint8_t>> ();
	nano::vectorstream stream (*bytes);
	serialize (stream, use_epoch_2_min_version_a);
	return bytes;
}

nano::shared_const_buffer nano::message::to_shared_const_buffer (bool use_epoch_2_min_version_a) const
{
	return shared_const_buffer (to_bytes (use_epoch_2_min_version_a));
}

nano::block_type nano::message_header::block_type () const
{
	return static_cast<nano::block_type> (((extensions & block_type_mask) >> 8).to_ullong ());
}

void nano::message_header::block_type_set (nano::block_type type_a)
{
	extensions &= ~block_type_mask;
	extensions |= std::bitset<16> (static_cast<unsigned long long> (type_a) << 8);
}

uint8_t nano::message_header::count_get () const
{
	return static_cast<uint8_t> (((extensions & count_mask) >> 12).to_ullong ());
}

void nano::message_header::count_set (uint8_t count_a)
{
	debug_assert (count_a < 16);
	extensions &= ~count_mask;
	extensions |= std::bitset<16> (static_cast<unsigned long long> (count_a) << 12);
}

void nano::message_header::flag_set (uint8_t flag_a)
{
	// Flags from 8 are block_type & count
	debug_assert (flag_a < 8);
	extensions.set (flag_a, true);
}

bool nano::message_header::bulk_pull_is_count_present () const
{
	auto result (false);
	if (type == nano::message_type::bulk_pull)
	{
		if (extensions.test (bulk_pull_count_present_flag))
		{
			result = true;
		}
	}
	return result;
}

bool nano::message_header::node_id_handshake_is_query () const
{
	auto result (false);
	if (type == nano::message_type::node_id_handshake)
	{
		if (extensions.test (node_id_handshake_query_flag))
		{
			result = true;
		}
	}
	return result;
}

bool nano::message_header::node_id_handshake_is_response () const
{
	auto result (false);
	if (type == nano::message_type::node_id_handshake)
	{
		if (extensions.test (node_id_handshake_response_flag))
		{
			result = true;
		}
	}
	return result;
}

size_t nano::message_header::payload_length_bytes () const
{
	switch (type)
	{
		case nano::message_type::bulk_pull:
		{
			return nano::bulk_pull::size + (bulk_pull_is_count_present () ? nano::bulk_pull::extended_parameters_size : 0);
		}
		case nano::message_type::bulk_push:
		case nano::message_type::telemetry_req:
		{
			// These don't have a payload
			return 0;
		}
		case nano::message_type::frontier_req:
		{
			return nano::frontier_req::size;
		}
		case nano::message_type::bulk_pull_account:
		{
			return nano::bulk_pull_account::size;
		}
		case nano::message_type::keepalive:
		{
			return nano::keepalive::size;
		}
		case nano::message_type::publish:
		{
			return nano::block::size (block_type ());
		}
		case nano::message_type::confirm_ack:
		{
			return nano::confirm_ack::size (block_type (), count_get ());
		}
		case nano::message_type::confirm_req:
		{
			return nano::confirm_req::size (block_type (), count_get ());
		}
		case nano::message_type::node_id_handshake:
		{
			return nano::node_id_handshake::size (*this);
		}
		case nano::message_type::telemetry_ack:
		{
			return nano::telemetry_ack::size (*this);
		}
		default:
		{
			debug_assert (false);
			return 0;
		}
	}
}

// MTU - IP header - UDP header
const size_t nano::message_parser::max_safe_udp_message_size = 508;

std::string nano::message_parser::status_string ()
{
	switch (status)
	{
		case nano::message_parser::parse_status::success:
		{
			return "success";
		}
		case nano::message_parser::parse_status::insufficient_work:
		{
			return "insufficient_work";
		}
		case nano::message_parser::parse_status::invalid_header:
		{
			return "invalid_header";
		}
		case nano::message_parser::parse_status::invalid_message_type:
		{
			return "invalid_message_type";
		}
		case nano::message_parser::parse_status::invalid_keepalive_message:
		{
			return "invalid_keepalive_message";
		}
		case nano::message_parser::parse_status::invalid_publish_message:
		{
			return "invalid_publish_message";
		}
		case nano::message_parser::parse_status::invalid_confirm_req_message:
		{
			return "invalid_confirm_req_message";
		}
		case nano::message_parser::parse_status::invalid_confirm_ack_message:
		{
			return "invalid_confirm_ack_message";
		}
		case nano::message_parser::parse_status::invalid_node_id_handshake_message:
		{
			return "invalid_node_id_handshake_message";
		}
		case nano::message_parser::parse_status::invalid_telemetry_req_message:
		{
			return "invalid_telemetry_req_message";
		}
		case nano::message_parser::parse_status::invalid_telemetry_ack_message:
		{
			return "invalid_telemetry_ack_message";
		}
		case nano::message_parser::parse_status::outdated_version:
		{
			return "outdated_version";
		}
		case nano::message_parser::parse_status::duplicate_publish_message:
		{
			return "duplicate_publish_message";
		}
	}

	debug_assert (false);

	return "[unknown parse_status]";
}

nano::message_parser::message_parser (nano::network_filter & publish_filter_a, nano::block_uniquer & block_uniquer_a, nano::vote_uniquer & vote_uniquer_a, nano::message_visitor & visitor_a, nano::work_pool & pool_a, bool use_epoch_2_min_version_a) :
publish_filter (publish_filter_a),
block_uniquer (block_uniquer_a),
vote_uniquer (vote_uniquer_a),
visitor (visitor_a),
pool (pool_a),
status (parse_status::success),
use_epoch_2_min_version (use_epoch_2_min_version_a)
{
}

void nano::message_parser::deserialize_buffer (uint8_t const * buffer_a, size_t size_a)
{
	static nano::network_constants network_constants;
	status = parse_status::success;
	auto error (false);
	if (size_a <= max_safe_udp_message_size)
	{
		// Guaranteed to be deliverable
		nano::bufferstream stream (buffer_a, size_a);
		nano::message_header header (error, stream);
		if (!error)
		{
			if (header.version_using < get_protocol_constants ().protocol_version_min (use_epoch_2_min_version))
			{
				status = parse_status::outdated_version;
			}
			else
			{
				switch (header.type)
				{
					case nano::message_type::keepalive:
					{
						deserialize_keepalive (stream, header);
						break;
					}
					case nano::message_type::publish:
					{
						nano::uint128_t digest;
						if (!publish_filter.apply (buffer_a + header.size, size_a - header.size, &digest))
						{
							deserialize_publish (stream, header, digest);
						}
						else
						{
							status = parse_status::duplicate_publish_message;
						}
						break;
					}
					case nano::message_type::confirm_req:
					{
						deserialize_confirm_req (stream, header);
						break;
					}
					case nano::message_type::confirm_ack:
					{
						deserialize_confirm_ack (stream, header);
						break;
					}
					case nano::message_type::node_id_handshake:
					{
						deserialize_node_id_handshake (stream, header);
						break;
					}
					case nano::message_type::telemetry_req:
					{
						deserialize_telemetry_req (stream, header);
						break;
					}
					case nano::message_type::telemetry_ack:
					{
						deserialize_telemetry_ack (stream, header);
						break;
					}
					default:
					{
						status = parse_status::invalid_message_type;
						break;
					}
				}
			}
		}
		else
		{
			status = parse_status::invalid_header;
		}
	}
}

void nano::message_parser::deserialize_keepalive (nano::stream & stream_a, nano::message_header const & header_a)
{
	auto error (false);
	nano::keepalive incoming (error, stream_a, header_a);
	if (!error && at_end (stream_a))
	{
		visitor.keepalive (incoming);
	}
	else
	{
		status = parse_status::invalid_keepalive_message;
	}
}

void nano::message_parser::deserialize_publish (nano::stream & stream_a, nano::message_header const & header_a, nano::uint128_t const & digest_a)
{
	auto error (false);
	nano::publish incoming (error, stream_a, header_a, digest_a, &block_uniquer);
	if (!error && at_end (stream_a))
	{
		if (!nano::work_validate_entry (*incoming.block))
		{
			visitor.publish (incoming);
		}
		else
		{
			status = parse_status::insufficient_work;
		}
	}
	else
	{
		status = parse_status::invalid_publish_message;
	}
}

void nano::message_parser::deserialize_confirm_req (nano::stream & stream_a, nano::message_header const & header_a)
{
	auto error (false);
	nano::confirm_req incoming (error, stream_a, header_a, &block_uniquer);
	if (!error && at_end (stream_a))
	{
		if (incoming.block == nullptr || !nano::work_validate_entry (*incoming.block))
		{
			visitor.confirm_req (incoming);
		}
		else
		{
			status = parse_status::insufficient_work;
		}
	}
	else
	{
		status = parse_status::invalid_confirm_req_message;
	}
}

void nano::message_parser::deserialize_confirm_ack (nano::stream & stream_a, nano::message_header const & header_a)
{
	auto error (false);
	nano::confirm_ack incoming (error, stream_a, header_a, &vote_uniquer);
	if (!error && at_end (stream_a))
	{
		for (auto & vote_block : incoming.vote->blocks)
		{
			if (!vote_block.which ())
			{
				auto block (boost::get<std::shared_ptr<nano::block>> (vote_block));
				if (nano::work_validate_entry (*block))
				{
					status = parse_status::insufficient_work;
					break;
				}
			}
		}
		if (status == parse_status::success)
		{
			visitor.confirm_ack (incoming);
		}
	}
	else
	{
		status = parse_status::invalid_confirm_ack_message;
	}
}

void nano::message_parser::deserialize_node_id_handshake (nano::stream & stream_a, nano::message_header const & header_a)
{
	bool error_l (false);
	nano::node_id_handshake incoming (error_l, stream_a, header_a);
	if (!error_l && at_end (stream_a))
	{
		visitor.node_id_handshake (incoming);
	}
	else
	{
		status = parse_status::invalid_node_id_handshake_message;
	}
}

void nano::message_parser::deserialize_telemetry_req (nano::stream & stream_a, nano::message_header const & header_a)
{
	nano::telemetry_req incoming (header_a);
	if (at_end (stream_a))
	{
		visitor.telemetry_req (incoming);
	}
	else
	{
		status = parse_status::invalid_telemetry_req_message;
	}
}

void nano::message_parser::deserialize_telemetry_ack (nano::stream & stream_a, nano::message_header const & header_a)
{
	bool error_l (false);
	nano::telemetry_ack incoming (error_l, stream_a, header_a);
	// Intentionally not checking if at the end of stream, because these messages support backwards/forwards compatibility
	if (!error_l)
	{
		visitor.telemetry_ack (incoming);
	}
	else
	{
		status = parse_status::invalid_telemetry_ack_message;
	}
}

bool nano::message_parser::at_end (nano::stream & stream_a)
{
	uint8_t junk;
	auto end (nano::try_read (stream_a, junk));
	return end;
}

nano::keepalive::keepalive () :
message (nano::message_type::keepalive)
{
	nano::endpoint endpoint (boost::asio::ip::address_v6{}, 0);
	for (auto i (peers.begin ()), n (peers.end ()); i != n; ++i)
	{
		*i = endpoint;
	}
}

nano::keepalive::keepalive (bool & error_a, nano::stream & stream_a, nano::message_header const & header_a) :
message (header_a)
{
	if (!error_a)
	{
		error_a = deserialize (stream_a);
	}
}

void nano::keepalive::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.keepalive (*this);
}

void nano::keepalive::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	header.serialize (stream_a, use_epoch_2_min_version_a);
	for (auto i (peers.begin ()), j (peers.end ()); i != j; ++i)
	{
		debug_assert (i->address ().is_v6 ());
		auto bytes (i->address ().to_v6 ().to_bytes ());
		write (stream_a, bytes);
		write (stream_a, i->port ());
	}
}

bool nano::keepalive::deserialize (nano::stream & stream_a)
{
	debug_assert (header.type == nano::message_type::keepalive);
	auto error (false);
	for (auto i (peers.begin ()), j (peers.end ()); i != j && !error; ++i)
	{
		std::array<uint8_t, 16> address;
		uint16_t port;
		if (!try_read (stream_a, address) && !try_read (stream_a, port))
		{
			*i = nano::endpoint (boost::asio::ip::address_v6 (address), port);
		}
		else
		{
			error = true;
		}
	}
	return error;
}

bool nano::keepalive::operator== (nano::keepalive const & other_a) const
{
	return peers == other_a.peers;
}

nano::publish::publish (bool & error_a, nano::stream & stream_a, nano::message_header const & header_a, nano::uint128_t const & digest_a, nano::block_uniquer * uniquer_a) :
message (header_a),
digest (digest_a)
{
	if (!error_a)
	{
		error_a = deserialize (stream_a, uniquer_a);
	}
}

nano::publish::publish (std::shared_ptr<nano::block> block_a) :
message (nano::message_type::publish),
block (block_a)
{
	header.block_type_set (block->type ());
}

void nano::publish::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	debug_assert (block != nullptr);
	header.serialize (stream_a, use_epoch_2_min_version_a);
	block->serialize (stream_a);
}

bool nano::publish::deserialize (nano::stream & stream_a, nano::block_uniquer * uniquer_a)
{
	debug_assert (header.type == nano::message_type::publish);
	block = nano::deserialize_block (stream_a, header.block_type (), uniquer_a);
	auto result (block == nullptr);
	return result;
}

void nano::publish::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.publish (*this);
}

bool nano::publish::operator== (nano::publish const & other_a) const
{
	return *block == *other_a.block;
}

nano::confirm_req::confirm_req (bool & error_a, nano::stream & stream_a, nano::message_header const & header_a, nano::block_uniquer * uniquer_a) :
message (header_a)
{
	if (!error_a)
	{
		error_a = deserialize (stream_a, uniquer_a);
	}
}

nano::confirm_req::confirm_req (std::shared_ptr<nano::block> block_a) :
message (nano::message_type::confirm_req),
block (block_a)
{
	header.block_type_set (block->type ());
}

nano::confirm_req::confirm_req (std::vector<std::pair<nano::block_hash, nano::root>> const & roots_hashes_a) :
message (nano::message_type::confirm_req),
roots_hashes (roots_hashes_a)
{
	// not_a_block (1) block type for hashes + roots request
	header.block_type_set (nano::block_type::not_a_block);
	debug_assert (roots_hashes.size () < 16);
	header.count_set (static_cast<uint8_t> (roots_hashes.size ()));
}

nano::confirm_req::confirm_req (nano::block_hash const & hash_a, nano::root const & root_a) :
message (nano::message_type::confirm_req),
roots_hashes (std::vector<std::pair<nano::block_hash, nano::root>> (1, std::make_pair (hash_a, root_a)))
{
	debug_assert (!roots_hashes.empty ());
	// not_a_block (1) block type for hashes + roots request
	header.block_type_set (nano::block_type::not_a_block);
	debug_assert (roots_hashes.size () < 16);
	header.count_set (static_cast<uint8_t> (roots_hashes.size ()));
}

void nano::confirm_req::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.confirm_req (*this);
}

void nano::confirm_req::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	header.serialize (stream_a, use_epoch_2_min_version_a);
	if (header.block_type () == nano::block_type::not_a_block)
	{
		debug_assert (!roots_hashes.empty ());
		// Write hashes & roots
		for (auto & root_hash : roots_hashes)
		{
			write (stream_a, root_hash.first);
			write (stream_a, root_hash.second);
		}
	}
	else
	{
		debug_assert (block != nullptr);
		block->serialize (stream_a);
	}
}

bool nano::confirm_req::deserialize (nano::stream & stream_a, nano::block_uniquer * uniquer_a)
{
	bool result (false);
	debug_assert (header.type == nano::message_type::confirm_req);
	try
	{
		if (header.block_type () == nano::block_type::not_a_block)
		{
			uint8_t count (header.count_get ());
			for (auto i (0); i != count && !result; ++i)
			{
				nano::block_hash block_hash (0);
				nano::block_hash root (0);
				read (stream_a, block_hash);
				read (stream_a, root);
				if (!block_hash.is_zero () || !root.is_zero ())
				{
					roots_hashes.emplace_back (block_hash, root);
				}
			}

			result = roots_hashes.empty () || (roots_hashes.size () != count);
		}
		else
		{
			block = nano::deserialize_block (stream_a, header.block_type (), uniquer_a);
			result = block == nullptr;
		}
	}
	catch (const std::runtime_error &)
	{
		result = true;
	}

	return result;
}

bool nano::confirm_req::operator== (nano::confirm_req const & other_a) const
{
	bool equal (false);
	if (block != nullptr && other_a.block != nullptr)
	{
		equal = *block == *other_a.block;
	}
	else if (!roots_hashes.empty () && !other_a.roots_hashes.empty ())
	{
		equal = roots_hashes == other_a.roots_hashes;
	}
	return equal;
}

std::string nano::confirm_req::roots_string () const
{
	std::string result;
	for (auto & root_hash : roots_hashes)
	{
		result += root_hash.first.to_string ();
		result += ":";
		result += root_hash.second.to_string ();
		result += ", ";
	}
	return result;
}

size_t nano::confirm_req::size (nano::block_type type_a, size_t count)
{
	size_t result (0);
	if (type_a != nano::block_type::invalid && type_a != nano::block_type::not_a_block)
	{
		result = nano::block::size (type_a);
	}
	else if (type_a == nano::block_type::not_a_block)
	{
		result = count * (sizeof (nano::uint256_union) + sizeof (nano::block_hash));
	}
	return result;
}

nano::confirm_ack::confirm_ack (bool & error_a, nano::stream & stream_a, nano::message_header const & header_a, nano::vote_uniquer * uniquer_a) :
message (header_a),
vote (nano::make_shared<nano::vote> (error_a, stream_a, header.block_type ()))
{
	if (!error_a && uniquer_a)
	{
		vote = uniquer_a->unique (vote);
	}
}

nano::confirm_ack::confirm_ack (std::shared_ptr<nano::vote> vote_a) :
message (nano::message_type::confirm_ack),
vote (vote_a)
{
	debug_assert (!vote_a->blocks.empty ());
	auto & first_vote_block (vote_a->blocks[0]);
	if (first_vote_block.which ())
	{
		header.block_type_set (nano::block_type::not_a_block);
		debug_assert (vote_a->blocks.size () < 16);
		header.count_set (static_cast<uint8_t> (vote_a->blocks.size ()));
	}
	else
	{
		header.block_type_set (boost::get<std::shared_ptr<nano::block>> (first_vote_block)->type ());
	}
}

void nano::confirm_ack::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	debug_assert (header.block_type () == nano::block_type::not_a_block || header.block_type () == nano::block_type::send || header.block_type () == nano::block_type::receive || header.block_type () == nano::block_type::open || header.block_type () == nano::block_type::change || header.block_type () == nano::block_type::state);
	header.serialize (stream_a, use_epoch_2_min_version_a);
	vote->serialize (stream_a, header.block_type ());
}

bool nano::confirm_ack::operator== (nano::confirm_ack const & other_a) const
{
	auto result (*vote == *other_a.vote);
	return result;
}

void nano::confirm_ack::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.confirm_ack (*this);
}

size_t nano::confirm_ack::size (nano::block_type type_a, size_t count)
{
	size_t result (sizeof (nano::account) + sizeof (nano::signature) + sizeof (uint64_t));
	if (type_a != nano::block_type::invalid && type_a != nano::block_type::not_a_block)
	{
		result += nano::block::size (type_a);
	}
	else if (type_a == nano::block_type::not_a_block)
	{
		result += count * sizeof (nano::block_hash);
	}
	return result;
}

nano::frontier_req::frontier_req () :
message (nano::message_type::frontier_req)
{
}

nano::frontier_req::frontier_req (bool & error_a, nano::stream & stream_a, nano::message_header const & header_a) :
message (header_a)
{
	if (!error_a)
	{
		error_a = deserialize (stream_a);
	}
}

void nano::frontier_req::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	header.serialize (stream_a, use_epoch_2_min_version_a);
	write (stream_a, start.bytes);
	write (stream_a, age);
	write (stream_a, count);
}

bool nano::frontier_req::deserialize (nano::stream & stream_a)
{
	debug_assert (header.type == nano::message_type::frontier_req);
	auto error (false);
	try
	{
		nano::read (stream_a, start.bytes);
		nano::read (stream_a, age);
		nano::read (stream_a, count);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

void nano::frontier_req::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.frontier_req (*this);
}

bool nano::frontier_req::operator== (nano::frontier_req const & other_a) const
{
	return start == other_a.start && age == other_a.age && count == other_a.count;
}

nano::bulk_pull::bulk_pull () :
message (nano::message_type::bulk_pull)
{
}

nano::bulk_pull::bulk_pull (bool & error_a, nano::stream & stream_a, nano::message_header const & header_a) :
message (header_a)
{
	if (!error_a)
	{
		error_a = deserialize (stream_a);
	}
}

void nano::bulk_pull::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.bulk_pull (*this);
}

void nano::bulk_pull::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	/*
	 * Ensure the "count_present" flag is set if there
	 * is a limit specifed.  Additionally, do not allow
	 * the "count_present" flag with a value of 0, since
	 * that is a sentinel which we use to mean "all blocks"
	 * and that is the behavior of not having the flag set
	 * so it is wasteful to do this.
	 */
	debug_assert ((count == 0 && !is_count_present ()) || (count != 0 && is_count_present ()));

	header.serialize (stream_a, use_epoch_2_min_version_a);
	write (stream_a, start);
	write (stream_a, end);

	if (is_count_present ())
	{
		std::array<uint8_t, extended_parameters_size> count_buffer{ { 0 } };
		decltype (count) count_little_endian;
		static_assert (sizeof (count_little_endian) < (count_buffer.size () - 1), "count must fit within buffer");

		count_little_endian = boost::endian::native_to_little (count);
		memcpy (count_buffer.data () + 1, &count_little_endian, sizeof (count_little_endian));

		write (stream_a, count_buffer);
	}
}

bool nano::bulk_pull::deserialize (nano::stream & stream_a)
{
	debug_assert (header.type == nano::message_type::bulk_pull);
	auto error (false);
	try
	{
		nano::read (stream_a, start);
		nano::read (stream_a, end);

		if (is_count_present ())
		{
			std::array<uint8_t, extended_parameters_size> extended_parameters_buffers;
			static_assert (sizeof (count) < (extended_parameters_buffers.size () - 1), "count must fit within buffer");

			nano::read (stream_a, extended_parameters_buffers);
			if (extended_parameters_buffers.front () != 0)
			{
				error = true;
			}
			else
			{
				memcpy (&count, extended_parameters_buffers.data () + 1, sizeof (count));
				boost::endian::little_to_native_inplace (count);
			}
		}
		else
		{
			count = 0;
		}
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

bool nano::bulk_pull::is_count_present () const
{
	return header.extensions.test (count_present_flag);
}

void nano::bulk_pull::set_count_present (bool value_a)
{
	header.extensions.set (count_present_flag, value_a);
}

nano::bulk_pull_account::bulk_pull_account () :
message (nano::message_type::bulk_pull_account)
{
}

nano::bulk_pull_account::bulk_pull_account (bool & error_a, nano::stream & stream_a, nano::message_header const & header_a) :
message (header_a)
{
	if (!error_a)
	{
		error_a = deserialize (stream_a);
	}
}

void nano::bulk_pull_account::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.bulk_pull_account (*this);
}

void nano::bulk_pull_account::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	header.serialize (stream_a, use_epoch_2_min_version_a);
	write (stream_a, account);
	write (stream_a, minimum_amount);
	write (stream_a, flags);
}

bool nano::bulk_pull_account::deserialize (nano::stream & stream_a)
{
	debug_assert (header.type == nano::message_type::bulk_pull_account);
	auto error (false);
	try
	{
		nano::read (stream_a, account);
		nano::read (stream_a, minimum_amount);
		nano::read (stream_a, flags);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

nano::bulk_push::bulk_push () :
message (nano::message_type::bulk_push)
{
}

nano::bulk_push::bulk_push (nano::message_header const & header_a) :
message (header_a)
{
}

bool nano::bulk_push::deserialize (nano::stream & stream_a)
{
	debug_assert (header.type == nano::message_type::bulk_push);
	return false;
}

void nano::bulk_push::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	header.serialize (stream_a, use_epoch_2_min_version_a);
}

void nano::bulk_push::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.bulk_push (*this);
}

nano::telemetry_req::telemetry_req () :
message (nano::message_type::telemetry_req)
{
}

nano::telemetry_req::telemetry_req (nano::message_header const & header_a) :
message (header_a)
{
}

bool nano::telemetry_req::deserialize (nano::stream & stream_a)
{
	debug_assert (header.type == nano::message_type::telemetry_req);
	return false;
}

void nano::telemetry_req::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	header.serialize (stream_a, use_epoch_2_min_version_a);
}

void nano::telemetry_req::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.telemetry_req (*this);
}

nano::telemetry_ack::telemetry_ack () :
message (nano::message_type::telemetry_ack)
{
}

nano::telemetry_ack::telemetry_ack (bool & error_a, nano::stream & stream_a, nano::message_header const & message_header) :
message (message_header)
{
	if (!error_a)
	{
		error_a = deserialize (stream_a);
	}
}

nano::telemetry_ack::telemetry_ack (nano::telemetry_data const & telemetry_data_a) :
message (nano::message_type::telemetry_ack),
data (telemetry_data_a)
{
	debug_assert (telemetry_data::size < 2048); // Maximum size the mask allows
	header.extensions &= ~message_header::telemetry_size_mask;
	header.extensions |= std::bitset<16> (static_cast<unsigned long long> (telemetry_data::size));
}

void nano::telemetry_ack::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	header.serialize (stream_a, use_epoch_2_min_version_a);
	if (!is_empty_payload ())
	{
		data.serialize (stream_a);
	}
}

bool nano::telemetry_ack::deserialize (nano::stream & stream_a)
{
	auto error (false);
	debug_assert (header.type == nano::message_type::telemetry_ack);
	try
	{
		if (!is_empty_payload ())
		{
			data.deserialize (stream_a, header.extensions.to_ulong ());
		}
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

void nano::telemetry_ack::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.telemetry_ack (*this);
}

uint16_t nano::telemetry_ack::size () const
{
	return size (header);
}

uint16_t nano::telemetry_ack::size (nano::message_header const & message_header_a)
{
	return static_cast<uint16_t> ((message_header_a.extensions & message_header::telemetry_size_mask).to_ullong ());
}

bool nano::telemetry_ack::is_empty_payload () const
{
	return size () == 0;
}

void nano::telemetry_data::deserialize (nano::stream & stream_a, uint16_t payload_length_a)
{
	read (stream_a, signature);
	read (stream_a, node_id);
	read (stream_a, block_count);
	boost::endian::big_to_native_inplace (block_count);
	read (stream_a, cemented_count);
	boost::endian::big_to_native_inplace (cemented_count);
	read (stream_a, unchecked_count);
	boost::endian::big_to_native_inplace (unchecked_count);
	read (stream_a, account_count);
	boost::endian::big_to_native_inplace (account_count);
	read (stream_a, bandwidth_cap);
	boost::endian::big_to_native_inplace (bandwidth_cap);
	read (stream_a, peer_count);
	boost::endian::big_to_native_inplace (peer_count);
	read (stream_a, protocol_version);
	read (stream_a, uptime);
	boost::endian::big_to_native_inplace (uptime);
	read (stream_a, genesis_block.bytes);
	read (stream_a, major_version);
	read (stream_a, minor_version);
	read (stream_a, patch_version);
	read (stream_a, pre_release_version);
	read (stream_a, maker);

	uint64_t timestamp_l;
	read (stream_a, timestamp_l);
	boost::endian::big_to_native_inplace (timestamp_l);
	timestamp = std::chrono::system_clock::time_point (std::chrono::milliseconds (timestamp_l));
	read (stream_a, active_difficulty);
	boost::endian::big_to_native_inplace (active_difficulty);
}

void nano::telemetry_data::serialize_without_signature (nano::stream & stream_a, uint16_t /* size_a */) const
{
	// All values should be serialized in big endian
	write (stream_a, node_id);
	write (stream_a, boost::endian::native_to_big (block_count));
	write (stream_a, boost::endian::native_to_big (cemented_count));
	write (stream_a, boost::endian::native_to_big (unchecked_count));
	write (stream_a, boost::endian::native_to_big (account_count));
	write (stream_a, boost::endian::native_to_big (bandwidth_cap));
	write (stream_a, boost::endian::native_to_big (peer_count));
	write (stream_a, protocol_version);
	write (stream_a, boost::endian::native_to_big (uptime));
	write (stream_a, genesis_block.bytes);
	write (stream_a, major_version);
	write (stream_a, minor_version);
	write (stream_a, patch_version);
	write (stream_a, pre_release_version);
	write (stream_a, maker);
	write (stream_a, boost::endian::native_to_big (std::chrono::duration_cast<std::chrono::milliseconds> (timestamp.time_since_epoch ()).count ()));
	write (stream_a, boost::endian::native_to_big (active_difficulty));
}

void nano::telemetry_data::serialize (nano::stream & stream_a) const
{
	write (stream_a, signature);
	serialize_without_signature (stream_a, size);
}

nano::error nano::telemetry_data::serialize_json (nano::jsonconfig & json, bool ignore_identification_metrics_a) const
{
	json.put ("block_count", block_count);
	json.put ("cemented_count", cemented_count);
	json.put ("unchecked_count", unchecked_count);
	json.put ("account_count", account_count);
	json.put ("bandwidth_cap", bandwidth_cap);
	json.put ("peer_count", peer_count);
	json.put ("protocol_version", protocol_version);
	json.put ("uptime", uptime);
	json.put ("genesis_block", genesis_block.to_string ());
	json.put ("major_version", major_version);
	json.put ("minor_version", minor_version);
	json.put ("patch_version", patch_version);
	json.put ("pre_release_version", pre_release_version);
	json.put ("maker", maker);
	json.put ("timestamp", std::chrono::duration_cast<std::chrono::milliseconds> (timestamp.time_since_epoch ()).count ());
	json.put ("active_difficulty", nano::to_string_hex (active_difficulty));
	// Keep these last for UI purposes
	if (!ignore_identification_metrics_a)
	{
		json.put ("node_id", node_id.to_node_id ());
		json.put ("signature", signature.to_string ());
	}
	return json.get_error ();
}

nano::error nano::telemetry_data::deserialize_json (nano::jsonconfig & json, bool ignore_identification_metrics_a)
{
	if (!ignore_identification_metrics_a)
	{
		std::string signature_l;
		json.get ("signature", signature_l);
		if (!json.get_error ())
		{
			if (signature.decode_hex (signature_l))
			{
				json.get_error ().set ("Could not deserialize signature");
			}
		}

		std::string node_id_l;
		json.get ("node_id", node_id_l);
		if (!json.get_error ())
		{
			if (node_id.decode_node_id (node_id_l))
			{
				json.get_error ().set ("Could not deserialize node id");
			}
		}
	}

	json.get ("block_count", block_count);
	json.get ("cemented_count", cemented_count);
	json.get ("unchecked_count", unchecked_count);
	json.get ("account_count", account_count);
	json.get ("bandwidth_cap", bandwidth_cap);
	json.get ("peer_count", peer_count);
	json.get ("protocol_version", protocol_version);
	json.get ("uptime", uptime);
	std::string genesis_block_l;
	json.get ("genesis_block", genesis_block_l);
	if (!json.get_error ())
	{
		if (genesis_block.decode_hex (genesis_block_l))
		{
			json.get_error ().set ("Could not deserialize genesis block");
		}
	}
	json.get ("major_version", major_version);
	json.get ("minor_version", minor_version);
	json.get ("patch_version", patch_version);
	json.get ("pre_release_version", pre_release_version);
	json.get ("maker", maker);
	auto timestamp_l = json.get<uint64_t> ("timestamp");
	timestamp = std::chrono::system_clock::time_point (std::chrono::milliseconds (timestamp_l));
	auto current_active_difficulty_text = json.get<std::string> ("active_difficulty");
	auto ec = nano::from_string_hex (current_active_difficulty_text, active_difficulty);
	debug_assert (!ec);
	return json.get_error ();
}

bool nano::telemetry_data::operator== (nano::telemetry_data const & data_a) const
{
	return (signature == data_a.signature && node_id == data_a.node_id && block_count == data_a.block_count && cemented_count == data_a.cemented_count && unchecked_count == data_a.unchecked_count && account_count == data_a.account_count && bandwidth_cap == data_a.bandwidth_cap && uptime == data_a.uptime && peer_count == data_a.peer_count && protocol_version == data_a.protocol_version && genesis_block == data_a.genesis_block && major_version == data_a.major_version && minor_version == data_a.minor_version && patch_version == data_a.patch_version && pre_release_version == data_a.pre_release_version && maker == data_a.maker && timestamp == data_a.timestamp && active_difficulty == data_a.active_difficulty);
}

bool nano::telemetry_data::operator!= (nano::telemetry_data const & data_a) const
{
	return !(*this == data_a);
}

void nano::telemetry_data::sign (nano::keypair const & node_id_a)
{
	debug_assert (node_id == node_id_a.pub);
	std::vector<uint8_t> bytes;
	{
		nano::vectorstream stream (bytes);
		serialize_without_signature (stream, size);
	}

	signature = nano::sign_message (node_id_a.prv, node_id_a.pub, bytes.data (), bytes.size ());
}

bool nano::telemetry_data::validate_signature (uint16_t size_a) const
{
	std::vector<uint8_t> bytes;
	{
		nano::vectorstream stream (bytes);
		serialize_without_signature (stream, size_a);
	}

	return nano::validate_message (node_id, bytes.data (), bytes.size (), signature);
}

nano::node_id_handshake::node_id_handshake (bool & error_a, nano::stream & stream_a, nano::message_header const & header_a) :
message (header_a),
query (boost::none),
response (boost::none)
{
	error_a = deserialize (stream_a);
}

nano::node_id_handshake::node_id_handshake (boost::optional<nano::uint256_union> query, boost::optional<std::pair<nano::account, nano::signature>> response) :
message (nano::message_type::node_id_handshake),
query (query),
response (response)
{
	if (query)
	{
		header.flag_set (nano::message_header::node_id_handshake_query_flag);
	}
	if (response)
	{
		header.flag_set (nano::message_header::node_id_handshake_response_flag);
	}
}

void nano::node_id_handshake::serialize (nano::stream & stream_a, bool use_epoch_2_min_version_a) const
{
	header.serialize (stream_a, use_epoch_2_min_version_a);
	if (query)
	{
		write (stream_a, *query);
	}
	if (response)
	{
		write (stream_a, response->first);
		write (stream_a, response->second);
	}
}

bool nano::node_id_handshake::deserialize (nano::stream & stream_a)
{
	debug_assert (header.type == nano::message_type::node_id_handshake);
	auto error (false);
	try
	{
		if (header.node_id_handshake_is_query ())
		{
			nano::uint256_union query_hash;
			read (stream_a, query_hash);
			query = query_hash;
		}

		if (header.node_id_handshake_is_response ())
		{
			nano::account response_account;
			read (stream_a, response_account);
			nano::signature response_signature;
			read (stream_a, response_signature);
			response = std::make_pair (response_account, response_signature);
		}
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

bool nano::node_id_handshake::operator== (nano::node_id_handshake const & other_a) const
{
	auto result (*query == *other_a.query && *response == *other_a.response);
	return result;
}

void nano::node_id_handshake::visit (nano::message_visitor & visitor_a) const
{
	visitor_a.node_id_handshake (*this);
}

size_t nano::node_id_handshake::size () const
{
	return size (header);
}

size_t nano::node_id_handshake::size (nano::message_header const & header_a)
{
	size_t result (0);
	if (header_a.node_id_handshake_is_query ())
	{
		result = sizeof (nano::uint256_union);
	}
	if (header_a.node_id_handshake_is_response ())
	{
		result += sizeof (nano::account) + sizeof (nano::signature);
	}
	return result;
}

nano::message_visitor::~message_visitor ()
{
}

bool nano::parse_port (std::string const & string_a, uint16_t & port_a)
{
	bool result = false;
	try
	{
		port_a = boost::lexical_cast<uint16_t> (string_a);
	}
	catch (...)
	{
		result = true;
	}
	return result;
}

// Can handle both ipv4 & ipv6 addresses (with and without square brackets)
bool nano::parse_address (std::string const & address_text_a, boost::asio::ip::address & address_a)
{
	auto address_text = address_text_a;
	if (!address_text.empty () && address_text.front () == '[' && address_text.back () == ']')
	{
		// Chop the square brackets off as make_address doesn't always like them
		address_text = address_text.substr (1, address_text.size () - 2);
	}

	boost::system::error_code address_ec;
	address_a = boost::asio::ip::make_address (address_text, address_ec);
	return !!address_ec;
}

bool nano::parse_address_port (std::string const & string, boost::asio::ip::address & address_a, uint16_t & port_a)
{
	auto result (false);
	auto port_position (string.rfind (':'));
	if (port_position != std::string::npos && port_position > 0)
	{
		std::string port_string (string.substr (port_position + 1));
		try
		{
			uint16_t port;
			result = parse_port (port_string, port);
			if (!result)
			{
				boost::system::error_code ec;
				auto address (boost::asio::ip::make_address_v6 (string.substr (0, port_position), ec));
				if (!ec)
				{
					address_a = address;
					port_a = port;
				}
				else
				{
					result = true;
				}
			}
			else
			{
				result = true;
			}
		}
		catch (...)
		{
			result = true;
		}
	}
	else
	{
		result = true;
	}
	return result;
}

bool nano::parse_endpoint (std::string const & string, nano::endpoint & endpoint_a)
{
	boost::asio::ip::address address;
	uint16_t port;
	auto result (parse_address_port (string, address, port));
	if (!result)
	{
		endpoint_a = nano::endpoint (address, port);
	}
	return result;
}

bool nano::parse_tcp_endpoint (std::string const & string, nano::tcp_endpoint & endpoint_a)
{
	boost::asio::ip::address address;
	uint16_t port;
	auto result (parse_address_port (string, address, port));
	if (!result)
	{
		endpoint_a = nano::tcp_endpoint (address, port);
	}
	return result;
}

std::chrono::seconds nano::telemetry_cache_cutoffs::network_to_time (network_constants const & network_constants)
{
	return std::chrono::seconds{ (network_constants.is_live_network () || network_constants.is_test_network ()) ? live : network_constants.is_beta_network () ? beta : dev };
}

nano::node_singleton_memory_pool_purge_guard::node_singleton_memory_pool_purge_guard () :
cleanup_guard ({ nano::block_memory_pool_purge, nano::purge_singleton_pool_memory<nano::vote>, nano::purge_singleton_pool_memory<nano::election> })
{
}
