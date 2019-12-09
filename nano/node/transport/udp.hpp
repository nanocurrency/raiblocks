#pragma once

#include <nano/node/common.hpp>
#include <nano/node/transport/transport.hpp>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/random_access_index.hpp>
#include <boost/multi_index_container.hpp>

#include <mutex>
#include <unordered_set>

namespace nano
{
class message_buffer;
namespace transport
{
	class udp_channels;
	class channel_udp final : public nano::transport::channel
	{
		friend class nano::transport::udp_channels;

	public:
		channel_udp (nano::transport::udp_channels &, nano::endpoint const &, uint8_t protocol_version);
		size_t hash_code () const override;
		bool operator== (nano::transport::channel const &) const override;
		void send_buffer (nano::shared_const_buffer const &, nano::stat::detail, std::function<void(boost::system::error_code const &, size_t)> const & = nullptr) override;
		std::function<void(boost::system::error_code const &, size_t)> callback (nano::stat::detail, std::function<void(boost::system::error_code const &, size_t)> const & = nullptr) const override;
		std::string to_string () const override;
		bool operator== (nano::transport::channel_udp const & other_a) const
		{
			return &channels == &other_a.channels && endpoint == other_a.endpoint;
		}

		nano::endpoint get_endpoint () const override
		{
			nano::lock_guard<std::mutex> lk (channel_mutex);
			return endpoint;
		}

		nano::tcp_endpoint get_tcp_endpoint () const override
		{
			nano::lock_guard<std::mutex> lk (channel_mutex);
			return nano::transport::map_endpoint_to_tcp (endpoint);
		}

		nano::transport::transport_type get_type () const override
		{
			return nano::transport::transport_type::udp;
		}

	private:
		nano::endpoint endpoint;
		nano::transport::udp_channels & channels;
	};
	class udp_channels final
	{
		friend class nano::transport::channel_udp;

	public:
		udp_channels (nano::node &, uint16_t);
		std::shared_ptr<nano::transport::channel_udp> insert (nano::endpoint const &, unsigned);
		void erase (nano::endpoint const &);
		size_t size () const;
		std::shared_ptr<nano::transport::channel_udp> channel (nano::endpoint const &) const;
		void random_fill (std::array<nano::endpoint, 8> &) const;
		std::unordered_set<std::shared_ptr<nano::transport::channel>> random_set (size_t) const;
		bool store_all (bool = true);
		std::shared_ptr<nano::transport::channel_udp> find_node_id (nano::account const &);
		void clean_node_id (nano::account const &);
		void clean_node_id (nano::endpoint const &, nano::account const &);
		// Get the next peer for attempting a tcp bootstrap connection
		nano::tcp_endpoint bootstrap_peer (uint8_t connection_protocol_version_min);
		void receive ();
		void start ();
		void stop ();
		void send (nano::shared_const_buffer const & buffer_a, nano::endpoint endpoint_a, std::function<void(boost::system::error_code const &, size_t)> const & callback_a);
		nano::endpoint get_local_endpoint () const;
		void receive_action (nano::message_buffer *);
		void process_packets ();
		std::shared_ptr<nano::transport::channel> create (nano::endpoint const &);
		bool max_ip_connections (nano::endpoint const &);
		// Should we reach out to this endpoint with a keepalive message
		bool reachout (nano::endpoint const &);
		std::unique_ptr<seq_con_info_component> collect_seq_con_info (std::string const &);
		void purge (std::chrono::steady_clock::time_point const &);
		void ongoing_keepalive ();
		void list (std::deque<std::shared_ptr<nano::transport::channel>> &);
		void modify (std::shared_ptr<nano::transport::channel_udp>, std::function<void(std::shared_ptr<nano::transport::channel_udp>)>);
		nano::node & node;

	private:
		void close_socket ();
		class endpoint_tag
		{
		};
		class ip_address_tag
		{
		};
		class random_access_tag
		{
		};
		class last_packet_received_tag
		{
		};
		class last_bootstrap_attempt_tag
		{
		};
		class node_id_tag
		{
		};
		class channel_udp_wrapper final
		{
		public:
			std::shared_ptr<nano::transport::channel_udp> channel;
			nano::endpoint endpoint () const
			{
				return channel->get_endpoint ();
			}
			std::chrono::steady_clock::time_point last_packet_received () const
			{
				return channel->get_last_packet_received ();
			}
			std::chrono::steady_clock::time_point last_bootstrap_attempt () const
			{
				return channel->get_last_bootstrap_attempt ();
			}
			boost::asio::ip::address ip_address () const
			{
				return endpoint ().address ();
			}
			nano::account node_id () const
			{
				return channel->get_node_id ();
			}
		};
		class endpoint_attempt final
		{
		public:
			nano::endpoint endpoint;
			std::chrono::steady_clock::time_point last_attempt;
		};
		mutable std::mutex mutex;
		boost::multi_index_container<
			channel_udp_wrapper,
			boost::multi_index::indexed_by<
				boost::multi_index::random_access<boost::multi_index::tag<random_access_tag>>,
				boost::multi_index::ordered_non_unique<boost::multi_index::tag<last_bootstrap_attempt_tag>, boost::multi_index::const_mem_fun<channel_udp_wrapper, std::chrono::steady_clock::time_point, &channel_udp_wrapper::last_bootstrap_attempt>>,
				boost::multi_index::hashed_unique<boost::multi_index::tag<endpoint_tag>, boost::multi_index::const_mem_fun<channel_udp_wrapper, nano::endpoint, &channel_udp_wrapper::endpoint>>,
				boost::multi_index::hashed_non_unique<boost::multi_index::tag<node_id_tag>, boost::multi_index::const_mem_fun<channel_udp_wrapper, nano::account, &channel_udp_wrapper::node_id>>,
				boost::multi_index::ordered_non_unique<boost::multi_index::tag<last_packet_received_tag>, boost::multi_index::const_mem_fun<channel_udp_wrapper, std::chrono::steady_clock::time_point, &channel_udp_wrapper::last_packet_received>>,
				boost::multi_index::ordered_non_unique<boost::multi_index::tag<ip_address_tag>, boost::multi_index::const_mem_fun<channel_udp_wrapper, boost::asio::ip::address, &channel_udp_wrapper::ip_address>>>>
			channels;
		boost::multi_index_container<
			endpoint_attempt,
			boost::multi_index::indexed_by<
				boost::multi_index::hashed_unique<boost::multi_index::member<endpoint_attempt, nano::endpoint, &endpoint_attempt::endpoint>>,
				boost::multi_index::ordered_non_unique<boost::multi_index::member<endpoint_attempt, std::chrono::steady_clock::time_point, &endpoint_attempt::last_attempt>>>>
			attempts;
		boost::asio::strand<boost::asio::io_context::executor_type> strand;
		boost::asio::ip::udp::socket socket;
		nano::endpoint local_endpoint;
		std::atomic<bool> stopped{ false };
	};
} // namespace transport
} // namespace nano
