#include <nano/node/common.hpp>
#include <nano/node/node.hpp>
#include <nano/node/transport/transport.hpp>

#include <boost/format.hpp>

#include <numeric>

namespace
{
class callback_visitor : public nano::message_visitor
{
public:
	void keepalive (nano::keepalive const & message_a) override
	{
		result = nano::stat::detail::keepalive;
	}
	void publish (nano::publish const & message_a) override
	{
		result = nano::stat::detail::publish;
	}
	void confirm_req (nano::confirm_req const & message_a) override
	{
		result = nano::stat::detail::confirm_req;
	}
	void confirm_ack (nano::confirm_ack const & message_a) override
	{
		result = nano::stat::detail::confirm_ack;
	}
	void bulk_pull (nano::bulk_pull const & message_a) override
	{
		result = nano::stat::detail::bulk_pull;
	}
	void bulk_pull_account (nano::bulk_pull_account const & message_a) override
	{
		result = nano::stat::detail::bulk_pull_account;
	}
	void bulk_push (nano::bulk_push const & message_a) override
	{
		result = nano::stat::detail::bulk_push;
	}
	void frontier_req (nano::frontier_req const & message_a) override
	{
		result = nano::stat::detail::frontier_req;
	}
	void node_id_handshake (nano::node_id_handshake const & message_a) override
	{
		result = nano::stat::detail::node_id_handshake;
	}
	void telemetry_req (nano::telemetry_req const & message_a) override
	{
		result = nano::stat::detail::telemetry_req;
	}
	void telemetry_ack (nano::telemetry_ack const & message_a) override
	{
		result = nano::stat::detail::telemetry_ack;
	}
	nano::stat::detail result;
};
}

nano::endpoint nano::transport::map_endpoint_to_v6 (nano::endpoint const & endpoint_a)
{
	auto endpoint_l (endpoint_a);
	if (endpoint_l.address ().is_v4 ())
	{
		endpoint_l = nano::endpoint (boost::asio::ip::address_v6::v4_mapped (endpoint_l.address ().to_v4 ()), endpoint_l.port ());
	}
	return endpoint_l;
}

nano::endpoint nano::transport::map_tcp_to_endpoint (nano::tcp_endpoint const & endpoint_a)
{
	return nano::endpoint (endpoint_a.address (), endpoint_a.port ());
}

nano::tcp_endpoint nano::transport::map_endpoint_to_tcp (nano::endpoint const & endpoint_a)
{
	return nano::tcp_endpoint (endpoint_a.address (), endpoint_a.port ());
}

nano::transport::channel::channel (nano::node & node_a) :
node (node_a)
{
	set_network_version (node_a.env.constants.protocol.protocol_version);
}

void nano::transport::channel::send (nano::message const & message_a, std::function<void(boost::system::error_code const &, size_t)> const & callback_a, nano::buffer_drop_policy drop_policy_a)
{
	callback_visitor visitor;
	message_a.visit (visitor);
	auto buffer (message_a.to_shared_const_buffer (node.ledger.cache.epoch_2_started));
	auto detail (visitor.result);
	auto is_droppable_by_limiter = drop_policy_a == nano::buffer_drop_policy::limiter;
	auto should_drop (node.network.limiter.should_drop (buffer.size ()));
	if (!is_droppable_by_limiter || !should_drop)
	{
		send_buffer (buffer, detail, callback_a, drop_policy_a);
		node.stats.inc (nano::stat::type::message, detail, nano::stat::dir::out);
	}
	else
	{
		if (callback_a)
		{
			node.background ([callback_a]() {
				callback_a (boost::system::errc::make_error_code (boost::system::errc::not_supported), 0);
			});
		}

		node.stats.inc (nano::stat::type::drop, detail, nano::stat::dir::out);
		if (node.config.logging.network_packet_logging ())
		{
			auto key = static_cast<uint8_t> (detail) << 8;
			node.logger.always_log (boost::str (boost::format ("%1% of size %2% dropped") % node.stats.detail_to_string (key) % buffer.size ()));
		}
	}
}

namespace
{
boost::asio::ip::address_v6 mapped_from_v4_bytes (unsigned long address_a)
{
	return boost::asio::ip::address_v6::v4_mapped (boost::asio::ip::address_v4 (address_a));
}
}

bool nano::transport::reserved_address (nano::endpoint const & endpoint_a, bool allow_local_peers)
{
	debug_assert (endpoint_a.address ().is_v6 ());
	auto bytes (endpoint_a.address ().to_v6 ());
	auto result (false);
	static auto const rfc1700_min (mapped_from_v4_bytes (0x00000000ul));
	static auto const rfc1700_max (mapped_from_v4_bytes (0x00fffffful));
	static auto const rfc1918_1_min (mapped_from_v4_bytes (0x0a000000ul));
	static auto const rfc1918_1_max (mapped_from_v4_bytes (0x0afffffful));
	static auto const rfc1918_2_min (mapped_from_v4_bytes (0xac100000ul));
	static auto const rfc1918_2_max (mapped_from_v4_bytes (0xac1ffffful));
	static auto const rfc1918_3_min (mapped_from_v4_bytes (0xc0a80000ul));
	static auto const rfc1918_3_max (mapped_from_v4_bytes (0xc0a8fffful));
	static auto const rfc6598_min (mapped_from_v4_bytes (0x64400000ul));
	static auto const rfc6598_max (mapped_from_v4_bytes (0x647ffffful));
	static auto const rfc5737_1_min (mapped_from_v4_bytes (0xc0000200ul));
	static auto const rfc5737_1_max (mapped_from_v4_bytes (0xc00002fful));
	static auto const rfc5737_2_min (mapped_from_v4_bytes (0xc6336400ul));
	static auto const rfc5737_2_max (mapped_from_v4_bytes (0xc63364fful));
	static auto const rfc5737_3_min (mapped_from_v4_bytes (0xcb007100ul));
	static auto const rfc5737_3_max (mapped_from_v4_bytes (0xcb0071fful));
	static auto const ipv4_multicast_min (mapped_from_v4_bytes (0xe0000000ul));
	static auto const ipv4_multicast_max (mapped_from_v4_bytes (0xeffffffful));
	static auto const rfc6890_min (mapped_from_v4_bytes (0xf0000000ul));
	static auto const rfc6890_max (mapped_from_v4_bytes (0xfffffffful));
	static auto const rfc6666_min (boost::asio::ip::make_address_v6 ("100::"));
	static auto const rfc6666_max (boost::asio::ip::make_address_v6 ("100::ffff:ffff:ffff:ffff"));
	static auto const rfc3849_min (boost::asio::ip::make_address_v6 ("2001:db8::"));
	static auto const rfc3849_max (boost::asio::ip::make_address_v6 ("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"));
	static auto const rfc4193_min (boost::asio::ip::make_address_v6 ("fc00::"));
	static auto const rfc4193_max (boost::asio::ip::make_address_v6 ("fd00:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
	static auto const ipv6_multicast_min (boost::asio::ip::make_address_v6 ("ff00::"));
	static auto const ipv6_multicast_max (boost::asio::ip::make_address_v6 ("ff00:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
	if (endpoint_a.port () == 0)
	{
		result = true;
	}
	else if (bytes >= rfc1700_min && bytes <= rfc1700_max)
	{
		result = true;
	}
	else if (bytes >= rfc5737_1_min && bytes <= rfc5737_1_max)
	{
		result = true;
	}
	else if (bytes >= rfc5737_2_min && bytes <= rfc5737_2_max)
	{
		result = true;
	}
	else if (bytes >= rfc5737_3_min && bytes <= rfc5737_3_max)
	{
		result = true;
	}
	else if (bytes >= ipv4_multicast_min && bytes <= ipv4_multicast_max)
	{
		result = true;
	}
	else if (bytes >= rfc6890_min && bytes <= rfc6890_max)
	{
		result = true;
	}
	else if (bytes >= rfc6666_min && bytes <= rfc6666_max)
	{
		result = true;
	}
	else if (bytes >= rfc3849_min && bytes <= rfc3849_max)
	{
		result = true;
	}
	else if (bytes >= ipv6_multicast_min && bytes <= ipv6_multicast_max)
	{
		result = true;
	}
	else if (!allow_local_peers)
	{
		if (bytes >= rfc1918_1_min && bytes <= rfc1918_1_max)
		{
			result = true;
		}
		else if (bytes >= rfc1918_2_min && bytes <= rfc1918_2_max)
		{
			result = true;
		}
		else if (bytes >= rfc1918_3_min && bytes <= rfc1918_3_max)
		{
			result = true;
		}
		else if (bytes >= rfc6598_min && bytes <= rfc6598_max)
		{
			result = true;
		}
		else if (bytes >= rfc4193_min && bytes <= rfc4193_max)
		{
			result = true;
		}
	}
	return result;
}

using namespace std::chrono_literals;

nano::bandwidth_limiter::bandwidth_limiter (const double limit_burst_ratio_a, const size_t limit_a) :
bucket (limit_a * limit_burst_ratio_a, limit_a)
{
}

bool nano::bandwidth_limiter::should_drop (const size_t & message_size_a)
{
	return !bucket.try_consume (message_size_a);
}
