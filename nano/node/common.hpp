#pragma once

#include <nano/boost/asio/ip/tcp.hpp>
#include <nano/boost/asio/ip/udp.hpp>
#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/asio.hpp>
#include <nano/lib/jsonconfig.hpp>
#include <nano/lib/memory.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/network_filter.hpp>

#include <bitset>

namespace nano
{
using endpoint = boost::asio::ip::udp::endpoint;
bool parse_port (std::string const &, uint16_t &);
bool parse_address (std::string const &, boost::asio::ip::address &);
bool parse_address_port (std::string const &, boost::asio::ip::address &, uint16_t &);
using tcp_endpoint = boost::asio::ip::tcp::endpoint;
bool parse_endpoint (std::string const &, nano::endpoint &);
bool parse_tcp_endpoint (std::string const &, nano::tcp_endpoint &);
uint64_t ip_address_hash_raw (boost::asio::ip::address const & ip_a, uint16_t port = 0);
}

namespace
{
uint64_t endpoint_hash_raw (nano::endpoint const & endpoint_a)
{
	uint64_t result (nano::ip_address_hash_raw (endpoint_a.address (), endpoint_a.port ()));
	return result;
}
uint64_t endpoint_hash_raw (nano::tcp_endpoint const & endpoint_a)
{
	uint64_t result (nano::ip_address_hash_raw (endpoint_a.address (), endpoint_a.port ()));
	return result;
}

template <size_t size>
struct endpoint_hash
{
};
template <>
struct endpoint_hash<8>
{
	size_t operator() (nano::endpoint const & endpoint_a) const
	{
		return endpoint_hash_raw (endpoint_a);
	}
	size_t operator() (nano::tcp_endpoint const & endpoint_a) const
	{
		return endpoint_hash_raw (endpoint_a);
	}
};
template <>
struct endpoint_hash<4>
{
	size_t operator() (nano::endpoint const & endpoint_a) const
	{
		uint64_t big (endpoint_hash_raw (endpoint_a));
		uint32_t result (static_cast<uint32_t> (big) ^ static_cast<uint32_t> (big >> 32));
		return result;
	}
	size_t operator() (nano::tcp_endpoint const & endpoint_a) const
	{
		uint64_t big (endpoint_hash_raw (endpoint_a));
		uint32_t result (static_cast<uint32_t> (big) ^ static_cast<uint32_t> (big >> 32));
		return result;
	}
};
template <size_t size>
struct ip_address_hash
{
};
template <>
struct ip_address_hash<8>
{
	size_t operator() (boost::asio::ip::address const & ip_address_a) const
	{
		return nano::ip_address_hash_raw (ip_address_a);
	}
};
template <>
struct ip_address_hash<4>
{
	size_t operator() (boost::asio::ip::address const & ip_address_a) const
	{
		uint64_t big (nano::ip_address_hash_raw (ip_address_a));
		uint32_t result (static_cast<uint32_t> (big) ^ static_cast<uint32_t> (big >> 32));
		return result;
	}
};
}

namespace std
{
template <>
struct hash<::nano::endpoint>
{
	size_t operator() (::nano::endpoint const & endpoint_a) const
	{
		endpoint_hash<sizeof (size_t)> ehash;
		return ehash (endpoint_a);
	}
};
template <>
struct hash<::nano::tcp_endpoint>
{
	size_t operator() (::nano::tcp_endpoint const & endpoint_a) const
	{
		endpoint_hash<sizeof (size_t)> ehash;
		return ehash (endpoint_a);
	}
};
template <>
struct hash<boost::asio::ip::address>
{
	size_t operator() (boost::asio::ip::address const & ip_a) const
	{
		ip_address_hash<sizeof (size_t)> ihash;
		return ihash (ip_a);
	}
};
}
namespace boost
{
template <>
struct hash<::nano::endpoint>
{
	size_t operator() (::nano::endpoint const & endpoint_a) const
	{
		std::hash<::nano::endpoint> hash;
		return hash (endpoint_a);
	}
};
template <>
struct hash<::nano::tcp_endpoint>
{
	size_t operator() (::nano::tcp_endpoint const & endpoint_a) const
	{
		std::hash<::nano::tcp_endpoint> hash;
		return hash (endpoint_a);
	}
};
template <>
struct hash<boost::asio::ip::address>
{
	size_t operator() (boost::asio::ip::address const & ip_a) const
	{
		std::hash<boost::asio::ip::address> hash;
		return hash (ip_a);
	}
};
}

namespace nano
{
/**
 * Message types are serialized to the network and existing values must thus never change as
 * types are added, removed and reordered in the enum.
 */
enum class message_type : uint8_t
{
	invalid = 0x0,
	not_a_type = 0x1,
	keepalive = 0x2,
	publish = 0x3,
	confirm_req = 0x4,
	confirm_ack = 0x5,
	bulk_pull = 0x6,
	bulk_push = 0x7,
	frontier_req = 0x8,
	/* deleted 0x9 */
	node_id_handshake = 0x0a,
	bulk_pull_account = 0x0b,
	telemetry_req = 0x0c,
	telemetry_ack = 0x0d
};

enum class bulk_pull_account_flags : uint8_t
{
	pending_hash_and_amount = 0x0,
	pending_address_only = 0x1,
	pending_hash_amount_and_address = 0x2
};
class message_visitor;
class message_header final
{
public:
	explicit message_header (nano::message_type);
	message_header (bool &, nano::stream &);
	void serialize (nano::stream &, bool) const;
	bool deserialize (nano::stream &);
	nano::block_type block_type () const;
	void block_type_set (nano::block_type);
	uint8_t count_get () const;
	void count_set (uint8_t);
	uint8_t version_max;
	uint8_t version_using;

private:
	uint8_t version_min_m{ std::numeric_limits<uint8_t>::max () };

public:
	nano::message_type type;
	std::bitset<16> extensions;
	static size_t constexpr size = sizeof (environment_constants::header_magic_number) + sizeof (version_max) + sizeof (version_using) + sizeof (version_min_m) + sizeof (type) + sizeof (/* extensions */ uint16_t);

	void flag_set (uint8_t);
	static uint8_t constexpr bulk_pull_count_present_flag = 0;
	bool bulk_pull_is_count_present () const;
	static uint8_t constexpr node_id_handshake_query_flag = 0;
	static uint8_t constexpr node_id_handshake_response_flag = 1;
	bool node_id_handshake_is_query () const;
	bool node_id_handshake_is_response () const;
	uint8_t version_min () const;

	/** Size of the payload in bytes. For some messages, the payload size is based on header flags. */
	size_t payload_length_bytes () const;

	static std::bitset<16> constexpr block_type_mask{ 0x0f00 };
	static std::bitset<16> constexpr count_mask{ 0xf000 };
	static std::bitset<16> constexpr telemetry_size_mask{ 0x07ff };
};
class message
{
public:
	explicit message (nano::message_type);
	explicit message (nano::message_header const &);
	virtual ~message () = default;
	virtual void serialize (nano::stream &, bool) const = 0;
	virtual void visit (nano::message_visitor &) const = 0;
	std::shared_ptr<std::vector<uint8_t>> to_bytes (bool) const;
	nano::shared_const_buffer to_shared_const_buffer (bool) const;

	nano::message_header header;
};
class work_pool;
class message_parser final
{
public:
	enum class parse_status
	{
		success,
		insufficient_work,
		invalid_header,
		invalid_message_type,
		invalid_keepalive_message,
		invalid_publish_message,
		invalid_confirm_req_message,
		invalid_confirm_ack_message,
		invalid_node_id_handshake_message,
		invalid_telemetry_req_message,
		invalid_telemetry_ack_message,
		outdated_version,
		duplicate_publish_message
	};
	message_parser (nano::network_filter &, nano::block_uniquer &, nano::vote_uniquer &, nano::message_visitor &, nano::work_pool &, bool);
	void deserialize_buffer (uint8_t const *, size_t);
	void deserialize_keepalive (nano::stream &, nano::message_header const &);
	void deserialize_publish (nano::stream &, nano::message_header const &, nano::uint128_t const & = 0);
	void deserialize_confirm_req (nano::stream &, nano::message_header const &);
	void deserialize_confirm_ack (nano::stream &, nano::message_header const &);
	void deserialize_node_id_handshake (nano::stream &, nano::message_header const &);
	void deserialize_telemetry_req (nano::stream &, nano::message_header const &);
	void deserialize_telemetry_ack (nano::stream &, nano::message_header const &);
	bool at_end (nano::stream &);
	nano::network_filter & publish_filter;
	nano::block_uniquer & block_uniquer;
	nano::vote_uniquer & vote_uniquer;
	nano::message_visitor & visitor;
	nano::work_pool & pool;
	parse_status status;
	bool use_epoch_2_min_version;
	std::string status_string ();
	static const size_t max_safe_udp_message_size;
};
class keepalive final : public message
{
public:
	keepalive ();
	keepalive (bool &, nano::stream &, nano::message_header const &);
	void visit (nano::message_visitor &) const override;
	void serialize (nano::stream &, bool) const override;
	bool deserialize (nano::stream &);
	bool operator== (nano::keepalive const &) const;
	std::array<nano::endpoint, 8> peers;
	static size_t constexpr size = 8 * (16 + 2);
};
class publish final : public message
{
public:
	publish (bool &, nano::stream &, nano::message_header const &, nano::uint128_t const & = 0, nano::block_uniquer * = nullptr);
	explicit publish (std::shared_ptr<nano::block>);
	void visit (nano::message_visitor &) const override;
	void serialize (nano::stream &, bool) const override;
	bool deserialize (nano::stream &, nano::block_uniquer * = nullptr);
	bool operator== (nano::publish const &) const;
	std::shared_ptr<nano::block> block;
	nano::uint128_t digest{ 0 };
};
class confirm_req final : public message
{
public:
	confirm_req (bool &, nano::stream &, nano::message_header const &, nano::block_uniquer * = nullptr);
	explicit confirm_req (std::shared_ptr<nano::block>);
	confirm_req (std::vector<std::pair<nano::block_hash, nano::root>> const &);
	confirm_req (nano::block_hash const &, nano::root const &);
	void serialize (nano::stream &, bool) const override;
	bool deserialize (nano::stream &, nano::block_uniquer * = nullptr);
	void visit (nano::message_visitor &) const override;
	bool operator== (nano::confirm_req const &) const;
	std::shared_ptr<nano::block> block;
	std::vector<std::pair<nano::block_hash, nano::root>> roots_hashes;
	std::string roots_string () const;
	static size_t size (nano::block_type, size_t = 0);
};
class confirm_ack final : public message
{
public:
	confirm_ack (bool &, nano::stream &, nano::message_header const &, nano::vote_uniquer * = nullptr);
	explicit confirm_ack (std::shared_ptr<nano::vote>);
	void serialize (nano::stream &, bool) const override;
	void visit (nano::message_visitor &) const override;
	bool operator== (nano::confirm_ack const &) const;
	std::shared_ptr<nano::vote> vote;
	static size_t size (nano::block_type, size_t = 0);
};
class frontier_req final : public message
{
public:
	frontier_req ();
	frontier_req (bool &, nano::stream &, nano::message_header const &);
	void serialize (nano::stream &, bool) const override;
	bool deserialize (nano::stream &);
	void visit (nano::message_visitor &) const override;
	bool operator== (nano::frontier_req const &) const;
	nano::account start;
	uint32_t age;
	uint32_t count;
	static size_t constexpr size = sizeof (start) + sizeof (age) + sizeof (count);
};

class telemetry_data
{
public:
	nano::signature signature{ 0 };
	nano::account node_id{ 0 };
	uint64_t block_count{ 0 };
	uint64_t cemented_count{ 0 };
	uint64_t unchecked_count{ 0 };
	uint64_t account_count{ 0 };
	uint64_t bandwidth_cap{ 0 };
	uint64_t uptime{ 0 };
	uint32_t peer_count{ 0 };
	uint8_t protocol_version{ 0 };
	nano::block_hash genesis_block{ 0 };
	uint8_t major_version{ 0 };
	uint8_t minor_version{ 0 };
	uint8_t patch_version{ 0 };
	uint8_t pre_release_version{ 0 };
	uint8_t maker{ 0 }; // 0 for NF node
	std::chrono::system_clock::time_point timestamp;
	uint64_t active_difficulty{ 0 };

	void serialize (nano::stream &) const;
	void deserialize (nano::stream &, uint16_t);
	nano::error serialize_json (nano::jsonconfig &, bool) const;
	nano::error deserialize_json (nano::jsonconfig &, bool);
	void sign (nano::keypair const &);
	bool validate_signature (uint16_t) const;
	bool operator== (nano::telemetry_data const &) const;
	bool operator!= (nano::telemetry_data const &) const;

	static auto constexpr size = sizeof (signature) + sizeof (node_id) + sizeof (block_count) + sizeof (cemented_count) + sizeof (unchecked_count) + sizeof (account_count) + sizeof (bandwidth_cap) + sizeof (peer_count) + sizeof (protocol_version) + sizeof (uptime) + sizeof (genesis_block) + sizeof (major_version) + sizeof (minor_version) + sizeof (patch_version) + sizeof (pre_release_version) + sizeof (maker) + sizeof (uint64_t) + sizeof (active_difficulty);

private:
	void serialize_without_signature (nano::stream &, uint16_t) const;
};
class telemetry_req final : public message
{
public:
	telemetry_req ();
	explicit telemetry_req (nano::message_header const &);
	void serialize (nano::stream &, bool) const override;
	bool deserialize (nano::stream &);
	void visit (nano::message_visitor &) const override;
};
class telemetry_ack final : public message
{
public:
	telemetry_ack ();
	telemetry_ack (bool &, nano::stream &, nano::message_header const &);
	explicit telemetry_ack (telemetry_data const &);
	void serialize (nano::stream &, bool) const override;
	void visit (nano::message_visitor &) const override;
	bool deserialize (nano::stream &);
	uint16_t size () const;
	bool is_empty_payload () const;
	static uint16_t size (nano::message_header const &);
	nano::telemetry_data data;
};

class bulk_pull final : public message
{
public:
	using count_t = uint32_t;
	bulk_pull ();
	bulk_pull (bool &, nano::stream &, nano::message_header const &);
	void serialize (nano::stream &, bool) const override;
	bool deserialize (nano::stream &);
	void visit (nano::message_visitor &) const override;
	nano::hash_or_account start{ 0 };
	nano::block_hash end{ 0 };
	count_t count{ 0 };
	bool is_count_present () const;
	void set_count_present (bool);
	static size_t constexpr count_present_flag = nano::message_header::bulk_pull_count_present_flag;
	static size_t constexpr extended_parameters_size = 8;
	static size_t constexpr size = sizeof (start) + sizeof (end);
};
class bulk_pull_account final : public message
{
public:
	bulk_pull_account ();
	bulk_pull_account (bool &, nano::stream &, nano::message_header const &);
	void serialize (nano::stream &, bool) const override;
	bool deserialize (nano::stream &);
	void visit (nano::message_visitor &) const override;
	nano::account account;
	nano::amount minimum_amount;
	bulk_pull_account_flags flags;
	static size_t constexpr size = sizeof (account) + sizeof (minimum_amount) + sizeof (bulk_pull_account_flags);
};
class bulk_push final : public message
{
public:
	bulk_push ();
	explicit bulk_push (nano::message_header const &);
	void serialize (nano::stream &, bool) const override;
	bool deserialize (nano::stream &);
	void visit (nano::message_visitor &) const override;
};
class node_id_handshake final : public message
{
public:
	node_id_handshake (bool &, nano::stream &, nano::message_header const &);
	node_id_handshake (boost::optional<nano::uint256_union>, boost::optional<std::pair<nano::account, nano::signature>>);
	void serialize (nano::stream &, bool) const override;
	bool deserialize (nano::stream &);
	void visit (nano::message_visitor &) const override;
	bool operator== (nano::node_id_handshake const &) const;
	boost::optional<nano::uint256_union> query;
	boost::optional<std::pair<nano::account, nano::signature>> response;
	size_t size () const;
	static size_t size (nano::message_header const &);
};
class message_visitor
{
public:
	virtual void keepalive (nano::keepalive const &) = 0;
	virtual void publish (nano::publish const &) = 0;
	virtual void confirm_req (nano::confirm_req const &) = 0;
	virtual void confirm_ack (nano::confirm_ack const &) = 0;
	virtual void bulk_pull (nano::bulk_pull const &) = 0;
	virtual void bulk_pull_account (nano::bulk_pull_account const &) = 0;
	virtual void bulk_push (nano::bulk_push const &) = 0;
	virtual void frontier_req (nano::frontier_req const &) = 0;
	virtual void node_id_handshake (nano::node_id_handshake const &) = 0;
	virtual void telemetry_req (nano::telemetry_req const &) = 0;
	virtual void telemetry_ack (nano::telemetry_ack const &) = 0;
	virtual ~message_visitor ();
};

class telemetry_cache_cutoffs
{
public:
	static std::chrono::seconds constexpr dev{ 3 };
	static std::chrono::seconds constexpr beta{ 15 };
	static std::chrono::seconds constexpr live{ 60 };

	static std::chrono::seconds network_to_time (network_constants const & network_constants);
};

/** Helper guard which contains all the necessary purge (remove all memory even if used) functions */
class node_singleton_memory_pool_purge_guard
{
public:
	node_singleton_memory_pool_purge_guard ();

private:
	nano::cleanup_guard cleanup_guard;
};
}
