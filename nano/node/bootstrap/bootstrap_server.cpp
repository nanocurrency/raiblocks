#include <nano/node/bootstrap/bootstrap_bulk_push.hpp>
#include <nano/node/bootstrap/bootstrap_frontier.hpp>
#include <nano/node/bootstrap/bootstrap_server.hpp>
#include <nano/node/node.hpp>
#include <nano/node/transport/tcp.hpp>

#include <boost/format.hpp>
#include <boost/variant/get.hpp>

nano::bootstrap_listener::bootstrap_listener (uint16_t port_a, nano::node & node_a) :
node (node_a),
port (port_a)
{
}

void nano::bootstrap_listener::start ()
{
	nano::lock_guard<nano::mutex> lock (mutex);
	on = true;
	listening_socket = std::make_shared<nano::server_socket> (node, boost::asio::ip::tcp::endpoint (boost::asio::ip::address_v6::any (), port), node.config.tcp_incoming_connections_max);
	boost::system::error_code ec;
	listening_socket->start (ec);
	if (ec)
	{
		node.logger.try_log (boost::str (boost::format ("Error while binding for incoming TCP/bootstrap on port %1%: %2%") % listening_socket->listening_port () % ec.message ()));
		throw std::runtime_error (ec.message ());
	}
	debug_assert (node.network.endpoint ().port () == listening_socket->listening_port ());
	listening_socket->on_connection ([this](std::shared_ptr<nano::socket> const & new_connection, boost::system::error_code const & ec_a) {
		bool keep_accepting = true;
		if (ec_a)
		{
			keep_accepting = false;
			this->node.logger.try_log (boost::str (boost::format ("Error while accepting incoming TCP/bootstrap connections: %1%") % ec_a.message ()));
		}
		else
		{
			accept_action (ec_a, new_connection);
		}
		return keep_accepting;
	});
}

void nano::bootstrap_listener::stop ()
{
	decltype (connections) connections_l;
	{
		nano::lock_guard<nano::mutex> lock (mutex);
		on = false;
		connections_l.swap (connections);
	}
	if (listening_socket)
	{
		nano::lock_guard<nano::mutex> lock (mutex);
		listening_socket->close ();
		listening_socket = nullptr;
	}
}

size_t nano::bootstrap_listener::connection_count ()
{
	nano::lock_guard<nano::mutex> lock (mutex);
	return connections.size ();
}

void nano::bootstrap_listener::accept_action (boost::system::error_code const & ec, std::shared_ptr<nano::socket> const & socket_a)
{
	if (!node.network.excluded_peers.check (socket_a->remote_endpoint ()))
	{
		auto connection (std::make_shared<nano::bootstrap_server> (socket_a, node.shared ()));
		nano::lock_guard<nano::mutex> lock (mutex);
		connections[connection.get ()] = connection;
		connection->receive ();
	}
	else
	{
		node.stats.inc (nano::stat::type::tcp, nano::stat::detail::tcp_excluded);
		if (node.config.logging.network_rejected_logging ())
		{
			node.logger.try_log ("Rejected connection from excluded peer ", socket_a->remote_endpoint ());
		}
	}
}

boost::asio::ip::tcp::endpoint nano::bootstrap_listener::endpoint ()
{
	nano::lock_guard<nano::mutex> lock (mutex);
	if (on && listening_socket)
	{
		return boost::asio::ip::tcp::endpoint (boost::asio::ip::address_v6::loopback (), listening_socket->listening_port ());
	}
	else
	{
		return boost::asio::ip::tcp::endpoint (boost::asio::ip::address_v6::loopback (), 0);
	}
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (bootstrap_listener & bootstrap_listener, std::string const & name)
{
	auto sizeof_element = sizeof (decltype (bootstrap_listener.connections)::value_type);
	auto composite = std::make_unique<container_info_composite> (name);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "connections", bootstrap_listener.connection_count (), sizeof_element }));
	return composite;
}

nano::bootstrap_server::bootstrap_server (std::shared_ptr<nano::socket> const & socket_a, std::shared_ptr<nano::node> const & node_a) :
receive_buffer (std::make_shared<std::vector<uint8_t>> ()),
socket (socket_a),
node (node_a)
{
	receive_buffer->resize (1024);
}

nano::bootstrap_server::~bootstrap_server ()
{
	if (node->config.logging.bulk_pull_logging ())
	{
		node->logger.try_log ("Exiting incoming TCP/bootstrap server");
	}
	if (type == nano::bootstrap_server_type::bootstrap)
	{
		--node->bootstrap.bootstrap_count;
	}
	else if (type == nano::bootstrap_server_type::realtime)
	{
		--node->bootstrap.realtime_count;
		// Clear temporary channel
		auto exisiting_response_channel (node->network.tcp_channels.find_channel (remote_endpoint));
		if (exisiting_response_channel != nullptr)
		{
			exisiting_response_channel->temporary = false;
			node->network.tcp_channels.erase (remote_endpoint);
		}
	}
	stop ();
	nano::lock_guard<nano::mutex> lock (node->bootstrap.mutex);
	node->bootstrap.connections.erase (this);
}

void nano::bootstrap_server::stop ()
{
	if (!stopped.exchange (true))
	{
		if (socket != nullptr)
		{
			socket->close ();
		}
	}
}

void nano::bootstrap_server::receive ()
{
	auto this_l (shared_from_this ());
	socket->async_read (receive_buffer, 8, [this_l](boost::system::error_code const & ec, size_t size_a) {
		// Set remote_endpoint
		if (this_l->remote_endpoint.port () == 0)
		{
			this_l->remote_endpoint = this_l->socket->remote_endpoint ();
		}
		// Receive header
		this_l->receive_header_action (ec, size_a);
	});
}

void nano::bootstrap_server::receive_header_action (boost::system::error_code const & ec, size_t size_a)
{
	if (!ec)
	{
		debug_assert (size_a == 8);
		nano::bufferstream type_stream (receive_buffer->data (), size_a);
		auto error (false);
		nano::message_header header (error, type_stream);
		if (!error)
		{
			auto this_l (shared_from_this ());
			switch (header.type)
			{
				case nano::message_type::bulk_pull:
				{
					node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::bulk_pull, nano::stat::dir::in);
					socket->async_read (receive_buffer, header.payload_length_bytes (), [this_l, header](boost::system::error_code const & ec, size_t size_a) {
						this_l->receive_bulk_pull_action (ec, size_a, header);
					});
					break;
				}
				case nano::message_type::bulk_pull_account:
				{
					node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::bulk_pull_account, nano::stat::dir::in);
					socket->async_read (receive_buffer, header.payload_length_bytes (), [this_l, header](boost::system::error_code const & ec, size_t size_a) {
						this_l->receive_bulk_pull_account_action (ec, size_a, header);
					});
					break;
				}
				case nano::message_type::frontier_req:
				{
					node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::frontier_req, nano::stat::dir::in);
					socket->async_read (receive_buffer, header.payload_length_bytes (), [this_l, header](boost::system::error_code const & ec, size_t size_a) {
						this_l->receive_frontier_req_action (ec, size_a, header);
					});
					break;
				}
				case nano::message_type::bulk_push:
				{
					node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::bulk_push, nano::stat::dir::in);
					if (is_bootstrap_connection ())
					{
						add_request (std::make_unique<nano::bulk_push> (header));
					}
					break;
				}
				case nano::message_type::keepalive:
				{
					socket->async_read (receive_buffer, header.payload_length_bytes (), [this_l, header](boost::system::error_code const & ec, size_t size_a) {
						this_l->receive_keepalive_action (ec, size_a, header);
					});
					break;
				}
				case nano::message_type::publish:
				{
					socket->async_read (receive_buffer, header.payload_length_bytes (), [this_l, header](boost::system::error_code const & ec, size_t size_a) {
						this_l->receive_publish_action (ec, size_a, header);
					});
					break;
				}
				case nano::message_type::confirm_ack:
				{
					socket->async_read (receive_buffer, header.payload_length_bytes (), [this_l, header](boost::system::error_code const & ec, size_t size_a) {
						this_l->receive_confirm_ack_action (ec, size_a, header);
					});
					break;
				}
				case nano::message_type::confirm_req:
				{
					socket->async_read (receive_buffer, header.payload_length_bytes (), [this_l, header](boost::system::error_code const & ec, size_t size_a) {
						this_l->receive_confirm_req_action (ec, size_a, header);
					});
					break;
				}
				case nano::message_type::node_id_handshake:
				{
					socket->async_read (receive_buffer, header.payload_length_bytes (), [this_l, header](boost::system::error_code const & ec, size_t size_a) {
						this_l->receive_node_id_handshake_action (ec, size_a, header);
					});
					break;
				}
				case nano::message_type::telemetry_req:
				{
					if (is_realtime_connection ())
					{
						// Only handle telemetry requests if they are outside of the cutoff time
						auto is_very_first_message = last_telemetry_req == std::chrono::steady_clock::time_point{};
						auto cache_exceeded = std::chrono::steady_clock::now () >= last_telemetry_req + nano::telemetry_cache_cutoffs::network_to_time (node->network_params.network);
						if (is_very_first_message || cache_exceeded)
						{
							last_telemetry_req = std::chrono::steady_clock::now ();
							add_request (std::make_unique<nano::telemetry_req> (header));
						}
						else
						{
							node->stats.inc (nano::stat::type::telemetry, nano::stat::detail::request_within_protection_cache_zone);
						}
					}
					receive ();
					break;
				}
				case nano::message_type::telemetry_ack:
				{
					socket->async_read (receive_buffer, header.payload_length_bytes (), [this_l, header](boost::system::error_code const & ec, size_t size_a) {
						this_l->receive_telemetry_ack_action (ec, size_a, header);
					});
					break;
				}
				default:
				{
					if (node->config.logging.network_logging ())
					{
						node->logger.try_log (boost::str (boost::format ("Received invalid type from bootstrap connection %1%") % static_cast<uint8_t> (header.type)));
					}
					break;
				}
			}
		}
	}
	else
	{
		if (node->config.logging.bulk_pull_logging ())
		{
			node->logger.try_log (boost::str (boost::format ("Error while receiving type: %1%") % ec.message ()));
		}
	}
}

void nano::bootstrap_server::receive_bulk_pull_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		auto error (false);
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto request (std::make_unique<nano::bulk_pull> (error, stream, header_a));
		if (!error)
		{
			if (node->config.logging.bulk_pull_logging ())
			{
				node->logger.try_log (boost::str (boost::format ("Received bulk pull for %1% down to %2%, maximum of %3% from %4%") % request->start.to_string () % request->end.to_string () % (request->count ? request->count : std::numeric_limits<double>::infinity ()) % remote_endpoint));
			}
			if (is_bootstrap_connection () && !node->flags.disable_bootstrap_bulk_pull_server)
			{
				add_request (std::unique_ptr<nano::message> (request.release ()));
			}
			receive ();
		}
	}
}

void nano::bootstrap_server::receive_bulk_pull_account_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		auto error (false);
		debug_assert (size_a == header_a.payload_length_bytes ());
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto request (std::make_unique<nano::bulk_pull_account> (error, stream, header_a));
		if (!error)
		{
			if (node->config.logging.bulk_pull_logging ())
			{
				node->logger.try_log (boost::str (boost::format ("Received bulk pull account for %1% with a minimum amount of %2%") % request->account.to_account () % nano::amount (request->minimum_amount).format_balance (nano::Mxrb_ratio, 10, true)));
			}
			if (is_bootstrap_connection () && !node->flags.disable_bootstrap_bulk_pull_server)
			{
				add_request (std::unique_ptr<nano::message> (request.release ()));
			}
			receive ();
		}
	}
}

void nano::bootstrap_server::receive_frontier_req_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		auto error (false);
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto request (std::make_unique<nano::frontier_req> (error, stream, header_a));
		if (!error)
		{
			if (node->config.logging.bulk_pull_logging ())
			{
				node->logger.try_log (boost::str (boost::format ("Received frontier request for %1% with age %2%") % request->start.to_string () % request->age));
			}
			if (is_bootstrap_connection ())
			{
				add_request (std::unique_ptr<nano::message> (request.release ()));
			}
			receive ();
		}
	}
	else
	{
		if (node->config.logging.network_logging ())
		{
			node->logger.try_log (boost::str (boost::format ("Error sending receiving frontier request: %1%") % ec.message ()));
		}
	}
}

void nano::bootstrap_server::receive_keepalive_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		auto error (false);
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto request (std::make_unique<nano::keepalive> (error, stream, header_a));
		if (!error)
		{
			if (is_realtime_connection ())
			{
				add_request (std::unique_ptr<nano::message> (request.release ()));
			}
			receive ();
		}
	}
	else
	{
		if (node->config.logging.network_keepalive_logging ())
		{
			node->logger.try_log (boost::str (boost::format ("Error receiving keepalive: %1%") % ec.message ()));
		}
	}
}

void nano::bootstrap_server::receive_telemetry_ack_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		auto error (false);
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto request (std::make_unique<nano::telemetry_ack> (error, stream, header_a));
		if (!error)
		{
			if (is_realtime_connection ())
			{
				add_request (std::unique_ptr<nano::message> (request.release ()));
			}
			receive ();
		}
	}
	else
	{
		if (node->config.logging.network_telemetry_logging ())
		{
			node->logger.try_log (boost::str (boost::format ("Error receiving telemetry ack: %1%") % ec.message ()));
		}
	}
}

void nano::bootstrap_server::receive_publish_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		nano::uint128_t digest;
		if (!node->network.publish_filter.apply (receive_buffer->data (), size_a, &digest))
		{
			auto error (false);
			nano::bufferstream stream (receive_buffer->data (), size_a);
			auto request (std::make_unique<nano::publish> (error, stream, header_a, digest));
			if (!error)
			{
				if (is_realtime_connection ())
				{
					if (!nano::work_validate_entry (*request->block))
					{
						add_request (std::unique_ptr<nano::message> (request.release ()));
					}
					else
					{
						node->stats.inc_detail_only (nano::stat::type::error, nano::stat::detail::insufficient_work);
					}
				}
				receive ();
			}
		}
		else
		{
			node->stats.inc (nano::stat::type::filter, nano::stat::detail::duplicate_publish);
			receive ();
		}
	}
	else
	{
		if (node->config.logging.network_message_logging ())
		{
			node->logger.try_log (boost::str (boost::format ("Error receiving publish: %1%") % ec.message ()));
		}
	}
}

void nano::bootstrap_server::receive_confirm_req_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		auto error (false);
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto request (std::make_unique<nano::confirm_req> (error, stream, header_a));
		if (!error)
		{
			if (is_realtime_connection ())
			{
				add_request (std::unique_ptr<nano::message> (request.release ()));
			}
			receive ();
		}
	}
	else if (node->config.logging.network_message_logging ())
	{
		node->logger.try_log (boost::str (boost::format ("Error receiving confirm_req: %1%") % ec.message ()));
	}
}

void nano::bootstrap_server::receive_confirm_ack_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		auto error (false);
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto request (std::make_unique<nano::confirm_ack> (error, stream, header_a));
		if (!error)
		{
			if (is_realtime_connection ())
			{
				bool process_vote (true);
				if (header_a.block_type () != nano::block_type::not_a_block)
				{
					for (auto & vote_block : request->vote->blocks)
					{
						if (!vote_block.which ())
						{
							auto const & block (boost::get<std::shared_ptr<nano::block>> (vote_block));
							if (nano::work_validate_entry (*block))
							{
								process_vote = false;
								node->stats.inc_detail_only (nano::stat::type::error, nano::stat::detail::insufficient_work);
							}
						}
					}
				}
				if (process_vote)
				{
					add_request (std::unique_ptr<nano::message> (request.release ()));
				}
			}
			receive ();
		}
	}
	else if (node->config.logging.network_message_logging ())
	{
		node->logger.try_log (boost::str (boost::format ("Error receiving confirm_ack: %1%") % ec.message ()));
	}
}

void nano::bootstrap_server::receive_node_id_handshake_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		auto error (false);
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto request (std::make_unique<nano::node_id_handshake> (error, stream, header_a));
		if (!error)
		{
			if (type == nano::bootstrap_server_type::undefined && !node->flags.disable_tcp_realtime)
			{
				add_request (std::unique_ptr<nano::message> (request.release ()));
			}
			receive ();
		}
	}
	else if (node->config.logging.network_node_id_handshake_logging ())
	{
		node->logger.try_log (boost::str (boost::format ("Error receiving node_id_handshake: %1%") % ec.message ()));
	}
}

void nano::bootstrap_server::add_request (std::unique_ptr<nano::message> message_a)
{
	debug_assert (message_a != nullptr);
	nano::unique_lock<nano::mutex> lock (mutex);
	auto start (requests.empty ());
	requests.push (std::move (message_a));
	if (start)
	{
		run_next (lock);
	}
}

void nano::bootstrap_server::finish_request ()
{
	nano::unique_lock<nano::mutex> lock (mutex);
	requests.pop ();
	if (!requests.empty ())
	{
		run_next (lock);
	}
	else
	{
		std::weak_ptr<nano::bootstrap_server> this_w (shared_from_this ());
		node->workers.add_timed_task (std::chrono::steady_clock::now () + (node->config.tcp_io_timeout * 2) + std::chrono::seconds (1), [this_w]() {
			if (auto this_l = this_w.lock ())
			{
				this_l->timeout ();
			}
		});
	}
}

void nano::bootstrap_server::finish_request_async ()
{
	std::weak_ptr<nano::bootstrap_server> this_w (shared_from_this ());
	node->background ([this_w]() {
		if (auto this_l = this_w.lock ())
		{
			this_l->finish_request ();
		}
	});
}

void nano::bootstrap_server::timeout ()
{
	if (socket != nullptr)
	{
		if (socket->has_timed_out ())
		{
			if (node->config.logging.bulk_pull_logging ())
			{
				node->logger.try_log ("Closing incoming tcp / bootstrap server by timeout");
			}
			{
				nano::lock_guard<nano::mutex> lock (node->bootstrap.mutex);
				node->bootstrap.connections.erase (this);
			}
			socket->close ();
		}
	}
	else
	{
		nano::lock_guard<nano::mutex> lock (node->bootstrap.mutex);
		node->bootstrap.connections.erase (this);
	}
}

namespace
{
class request_response_visitor : public nano::message_visitor
{
public:
	explicit request_response_visitor (std::shared_ptr<nano::bootstrap_server> const & connection_a) :
	connection (connection_a)
	{
	}
	void keepalive (nano::keepalive const & message_a) override
	{
		connection->node->network.tcp_message_manager.put_message (nano::tcp_message_item{ std::make_shared<nano::keepalive> (message_a), connection->remote_endpoint, connection->remote_node_id, connection->socket, connection->type });
	}
	void publish (nano::publish const & message_a) override
	{
		connection->node->network.tcp_message_manager.put_message (nano::tcp_message_item{ std::make_shared<nano::publish> (message_a), connection->remote_endpoint, connection->remote_node_id, connection->socket, connection->type });
	}
	void confirm_req (nano::confirm_req const & message_a) override
	{
		connection->node->network.tcp_message_manager.put_message (nano::tcp_message_item{ std::make_shared<nano::confirm_req> (message_a), connection->remote_endpoint, connection->remote_node_id, connection->socket, connection->type });
	}
	void confirm_ack (nano::confirm_ack const & message_a) override
	{
		connection->node->network.tcp_message_manager.put_message (nano::tcp_message_item{ std::make_shared<nano::confirm_ack> (message_a), connection->remote_endpoint, connection->remote_node_id, connection->socket, connection->type });
	}
	void bulk_pull (nano::bulk_pull const &) override
	{
		auto response (std::make_shared<nano::bulk_pull_server> (connection, std::unique_ptr<nano::bulk_pull> (static_cast<nano::bulk_pull *> (connection->requests.front ().release ()))));
		response->send_next ();
	}
	void bulk_pull_account (nano::bulk_pull_account const &) override
	{
		auto response (std::make_shared<nano::bulk_pull_account_server> (connection, std::unique_ptr<nano::bulk_pull_account> (static_cast<nano::bulk_pull_account *> (connection->requests.front ().release ()))));
		response->send_frontier ();
	}
	void bulk_push (nano::bulk_push const &) override
	{
		auto response (std::make_shared<nano::bulk_push_server> (connection));
		response->throttled_receive ();
	}
	void frontier_req (nano::frontier_req const &) override
	{
		auto response (std::make_shared<nano::frontier_req_server> (connection, std::unique_ptr<nano::frontier_req> (static_cast<nano::frontier_req *> (connection->requests.front ().release ()))));
		response->send_next ();
	}
	void telemetry_req (nano::telemetry_req const & message_a) override
	{
		connection->node->network.tcp_message_manager.put_message (nano::tcp_message_item{ std::make_shared<nano::telemetry_req> (message_a), connection->remote_endpoint, connection->remote_node_id, connection->socket, connection->type });
	}
	void telemetry_ack (nano::telemetry_ack const & message_a) override
	{
		connection->node->network.tcp_message_manager.put_message (nano::tcp_message_item{ std::make_shared<nano::telemetry_ack> (message_a), connection->remote_endpoint, connection->remote_node_id, connection->socket, connection->type });
	}
	void node_id_handshake (nano::node_id_handshake const & message_a) override
	{
		if (connection->node->config.logging.network_node_id_handshake_logging ())
		{
			connection->node->logger.try_log (boost::str (boost::format ("Received node_id_handshake message from %1%") % connection->remote_endpoint));
		}
		if (message_a.query)
		{
			boost::optional<std::pair<nano::account, nano::signature>> response (std::make_pair (connection->node->node_id.pub, nano::sign_message (connection->node->node_id.prv, connection->node->node_id.pub, *message_a.query)));
			debug_assert (!nano::validate_message (response->first, *message_a.query, response->second));
			auto cookie (connection->node->network.syn_cookies.assign (nano::transport::map_tcp_to_endpoint (connection->remote_endpoint)));
			nano::node_id_handshake response_message (cookie, response);
			auto shared_const_buffer = response_message.to_shared_const_buffer ();
			connection->socket->async_write (shared_const_buffer, [connection = std::weak_ptr<nano::bootstrap_server> (connection)](boost::system::error_code const & ec, size_t size_a) {
				if (auto connection_l = connection.lock ())
				{
					if (ec)
					{
						if (connection_l->node->config.logging.network_node_id_handshake_logging ())
						{
							connection_l->node->logger.try_log (boost::str (boost::format ("Error sending node_id_handshake to %1%: %2%") % connection_l->remote_endpoint % ec.message ()));
						}
						// Stop invalid handshake
						connection_l->stop ();
					}
					else
					{
						connection_l->node->stats.inc (nano::stat::type::message, nano::stat::detail::node_id_handshake, nano::stat::dir::out);
						connection_l->finish_request ();
					}
				}
			});
		}
		else if (message_a.response)
		{
			nano::account const & node_id (message_a.response->first);
			if (!connection->node->network.syn_cookies.validate (nano::transport::map_tcp_to_endpoint (connection->remote_endpoint), node_id, message_a.response->second) && node_id != connection->node->node_id.pub)
			{
				connection->remote_node_id = node_id;
				connection->type = nano::bootstrap_server_type::realtime;
				++connection->node->bootstrap.realtime_count;
				connection->finish_request_async ();
			}
			else
			{
				// Stop invalid handshake
				connection->stop ();
			}
		}
		else
		{
			connection->finish_request_async ();
		}
		nano::account node_id (connection->remote_node_id);
		nano::bootstrap_server_type type (connection->type);
		debug_assert (node_id.is_zero () || type == nano::bootstrap_server_type::realtime);
		connection->node->network.tcp_message_manager.put_message (nano::tcp_message_item{ std::make_shared<nano::node_id_handshake> (message_a), connection->remote_endpoint, connection->remote_node_id, connection->socket, connection->type });
	}
	std::shared_ptr<nano::bootstrap_server> connection;
};
}

void nano::bootstrap_server::run_next (nano::unique_lock<nano::mutex> & lock_a)
{
	debug_assert (!requests.empty ());
	request_response_visitor visitor (shared_from_this ());
	auto type (requests.front ()->header.type);
	if (type == nano::message_type::bulk_pull || type == nano::message_type::bulk_pull_account || type == nano::message_type::bulk_push || type == nano::message_type::frontier_req || type == nano::message_type::node_id_handshake)
	{
		// Bootstrap & node ID (realtime start)
		// Request removed from queue in request_response_visitor. For bootstrap with requests.front ().release (), for node ID with finish_request ()
		requests.front ()->visit (visitor);
	}
	else
	{
		// Realtime
		auto request (std::move (requests.front ()));
		requests.pop ();
		auto timeout_check (requests.empty ());
		lock_a.unlock ();
		request->visit (visitor);
		if (timeout_check)
		{
			std::weak_ptr<nano::bootstrap_server> this_w (shared_from_this ());
			node->workers.add_timed_task (std::chrono::steady_clock::now () + (node->config.tcp_io_timeout * 2) + std::chrono::seconds (1), [this_w]() {
				if (auto this_l = this_w.lock ())
				{
					this_l->timeout ();
				}
			});
		}
	}
}

bool nano::bootstrap_server::is_bootstrap_connection ()
{
	if (type == nano::bootstrap_server_type::undefined && !node->flags.disable_bootstrap_listener && node->bootstrap.bootstrap_count < node->config.bootstrap_connections_max)
	{
		++node->bootstrap.bootstrap_count;
		type = nano::bootstrap_server_type::bootstrap;
	}
	return type == nano::bootstrap_server_type::bootstrap;
}

bool nano::bootstrap_server::is_realtime_connection ()
{
	return type == nano::bootstrap_server_type::realtime || type == nano::bootstrap_server_type::realtime_response_server;
}
