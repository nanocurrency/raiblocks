#include <nano/node/bootstrap/bootstrap_bulk_push.hpp>
#include <nano/node/bootstrap/bootstrap_frontier.hpp>
#include <nano/node/bootstrap/bootstrap_server.hpp>
#include <nano/node/node.hpp>
#include <nano/node/transport/tcp.hpp>

#include <boost/format.hpp>

nano::bootstrap_listener::bootstrap_listener (uint16_t port_a, nano::node & node_a)
	: node (node_a)
	, port (port_a)
{
}

void nano::bootstrap_listener::start ()
{
	listening_socket = std::make_shared<nano::server_socket> (node.shared (), boost::asio::ip::tcp::endpoint (boost::asio::ip::address_v6::any (), port), node.config.tcp_incoming_connections_max);
	boost::system::error_code ec;
	listening_socket->start (ec);
	if (ec)
	{
		node.logger.try_log (boost::str (boost::format ("Error while binding for incoming TCP/bootstrap on port %1%: %2%") % listening_socket->listening_port () % ec.message ()));
		throw std::runtime_error (ec.message ());
	}
	listening_socket->on_connection ([this](std::shared_ptr<nano::socket> new_connection, boost::system::error_code const & ec_a) {
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
		nano::lock_guard<std::mutex> lock (mutex);
		on = false;
		connections_l.swap (connections);
	}
	if (listening_socket)
	{
		listening_socket->close ();
		listening_socket = nullptr;
	}
}

size_t nano::bootstrap_listener::connection_count ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	return connections.size ();
}

void nano::bootstrap_listener::accept_action (boost::system::error_code const & ec, std::shared_ptr<nano::socket> socket_a)
{
	auto connection (std::make_shared<nano::bootstrap_server> (socket_a, node.shared ()));
	{
		nano::lock_guard<std::mutex> lock (mutex);
		connections[connection.get ()] = connection;
		connection->receive ();
	}
}

boost::asio::ip::tcp::endpoint nano::bootstrap_listener::endpoint ()
{
	return boost::asio::ip::tcp::endpoint (boost::asio::ip::address_v6::loopback (), listening_socket->listening_port ());
}

namespace nano
{
std::unique_ptr<seq_con_info_component> collect_seq_con_info (bootstrap_listener & bootstrap_listener, const std::string & name)
{
	auto sizeof_element = sizeof (decltype (bootstrap_listener.connections)::value_type);
	auto composite = std::make_unique<seq_con_info_composite> (name);
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "connections", bootstrap_listener.connection_count (), sizeof_element }));
	return composite;
}
}

nano::bootstrap_server::bootstrap_server (std::shared_ptr<nano::socket> socket_a, std::shared_ptr<nano::node> node_a)
	: receive_buffer (std::make_shared<std::vector<uint8_t>> ())
	, socket (socket_a)
	, node (node_a)
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
			exisiting_response_channel->server = false;
			node->network.tcp_channels.erase (remote_endpoint);
		}
	}
	stop ();
	nano::lock_guard<std::mutex> lock (node->bootstrap.mutex);
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
	// Increase timeout to receive TCP header (idle server socket)
	socket->set_timeout (node->network_params.node.idle_timeout);
	auto this_l (shared_from_this ());
	socket->async_read (receive_buffer, 8, [this_l](boost::system::error_code const & ec, size_t size_a) {
		// Set remote_endpoint
		if (this_l->remote_endpoint.port () == 0)
		{
			this_l->remote_endpoint = this_l->socket->remote_endpoint ();
		}
		// Decrease timeout to default
		this_l->socket->set_timeout (this_l->node->config.tcp_io_timeout);
		// Receive header
		this_l->receive_header_action (ec, size_a);
	});
}

void nano::bootstrap_server::receive_header_action (boost::system::error_code const & ec, size_t size_a)
{
	if (!ec)
	{
		assert (size_a == 8);
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
				node->logger.try_log (boost::str (boost::format ("Received bulk pull for %1% down to %2%, maximum of %3%") % request->start.to_string () % request->end.to_string () % (request->count ? request->count : std::numeric_limits<double>::infinity ())));
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
		assert (size_a == header_a.payload_length_bytes ());
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

void nano::bootstrap_server::receive_publish_action (boost::system::error_code const & ec, size_t size_a, nano::message_header const & header_a)
{
	if (!ec)
	{
		auto error (false);
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto request (std::make_unique<nano::publish> (error, stream, header_a));
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
				add_request (std::unique_ptr<nano::message> (request.release ()));
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
	assert (message_a != nullptr);
	nano::lock_guard<std::mutex> lock (mutex);
	auto start (requests.empty ());
	requests.push (std::move (message_a));
	if (start)
	{
		run_next ();
	}
}

void nano::bootstrap_server::finish_request ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	requests.pop ();
	if (!requests.empty ())
	{
		run_next ();
	}
	else
	{
		std::weak_ptr<nano::bootstrap_server> this_w (shared_from_this ());
		node->alarm.add (std::chrono::steady_clock::now () + (node->config.tcp_io_timeout * 2) + std::chrono::seconds (1), [this_w]() {
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
				nano::lock_guard<std::mutex> lock (node->bootstrap.mutex);
				node->bootstrap.connections.erase (this);
			}
			socket->close ();
		}
	}
	else
	{
		nano::lock_guard<std::mutex> lock (node->bootstrap.mutex);
		node->bootstrap.connections.erase (this);
	}
}

namespace
{
class request_response_visitor : public nano::message_visitor
{
public:
	explicit request_response_visitor (std::shared_ptr<nano::bootstrap_server> const & connection_a)
		: connection (connection_a)
	{
	}
	virtual ~request_response_visitor () = default;
	void keepalive (nano::keepalive const & message_a) override
	{
		connection->finish_request_async ();
		auto connection_l (connection->shared_from_this ());
		connection->node->background ([connection_l, message_a]() {
			connection_l->node->network.tcp_channels.process_keepalive (message_a, connection_l->remote_endpoint);
		});
	}
	void publish (nano::publish const & message_a) override
	{
		connection->finish_request_async ();
		auto connection_l (connection->shared_from_this ());
		connection->node->background ([connection_l, message_a]() {
			connection_l->node->network.tcp_channels.process_message (message_a, connection_l->remote_endpoint, connection_l->remote_node_id, connection_l->socket, connection_l->type);
		});
	}
	void confirm_req (nano::confirm_req const & message_a) override
	{
		connection->finish_request_async ();
		auto connection_l (connection->shared_from_this ());
		connection->node->background ([connection_l, message_a]() {
			connection_l->node->network.tcp_channels.process_message (message_a, connection_l->remote_endpoint, connection_l->remote_node_id, connection_l->socket, connection_l->type);
		});
	}
	void confirm_ack (nano::confirm_ack const & message_a) override
	{
		connection->finish_request_async ();
		auto connection_l (connection->shared_from_this ());
		connection->node->background ([connection_l, message_a]() {
			connection_l->node->network.tcp_channels.process_message (message_a, connection_l->remote_endpoint, connection_l->remote_node_id, connection_l->socket, connection_l->type);
		});
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
	void node_id_handshake (nano::node_id_handshake const & message_a) override
	{
		if (connection->node->config.logging.network_node_id_handshake_logging ())
		{
			connection->node->logger.try_log (boost::str (boost::format ("Received node_id_handshake message from %1%") % connection->remote_endpoint));
		}
		if (message_a.query)
		{
			boost::optional<std::pair<nano::account, nano::signature>> response (std::make_pair (connection->node->node_id.pub, nano::sign_message (connection->node->node_id.prv, connection->node->node_id.pub, *message_a.query)));
			assert (!nano::validate_message (response->first, *message_a.query, response->second));
			auto cookie (connection->node->network.syn_cookies.assign (nano::transport::map_tcp_to_endpoint (connection->remote_endpoint)));
			nano::node_id_handshake response_message (cookie, response);
			auto shared_const_buffer = response_message.to_shared_const_buffer ();
			// clang-format off
			connection->socket->async_write (shared_const_buffer, [connection = std::weak_ptr<nano::bootstrap_server> (connection) ](boost::system::error_code const & ec, size_t size_a) {
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
			// clang-format on
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
		assert (node_id.is_zero () || type == nano::bootstrap_server_type::realtime);
		auto connection_l (connection->shared_from_this ());
		connection->node->background ([connection_l, message_a, node_id, type]() {
			connection_l->node->network.tcp_channels.process_message (message_a, connection_l->remote_endpoint, node_id, connection_l->socket, type);
		});
	}
	std::shared_ptr<nano::bootstrap_server> connection;
};
}

void nano::bootstrap_server::run_next ()
{
	assert (!requests.empty ());
	request_response_visitor visitor (shared_from_this ());
	requests.front ()->visit (visitor);
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
