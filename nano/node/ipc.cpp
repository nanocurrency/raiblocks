#include <nano/boost/asio/local/stream_protocol.hpp>
#include <nano/boost/asio/read.hpp>
#include <nano/lib/config.hpp>
#include <nano/lib/ipc.hpp>
#include <nano/lib/threading.hpp>
#include <nano/lib/timer.hpp>
#include <nano/node/common.hpp>
#include <nano/node/ipc.hpp>
#include <nano/node/json_handler.hpp>
#include <nano/node/node.hpp>

#include <boost/endian/conversion.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <chrono>

using namespace boost::log;

namespace
{
/**
 * A session represents an inbound connection over which multiple requests/reponses are transmitted.
 */
template <typename SOCKET_TYPE>
class session : public nano::ipc::socket_base, public std::enable_shared_from_this<session<SOCKET_TYPE>>
{
public:
	session (nano::ipc::ipc_server & server_a, boost::asio::io_context & io_ctx_a, nano::ipc::ipc_config_transport & config_transport_a)
		: socket_base (io_ctx_a)
		, server (server_a)
		, node (server_a.node)
		, session_id (server_a.id_dispenser.fetch_add (1))
		, io_ctx (io_ctx_a)
		, socket (io_ctx_a)
		, config_transport (config_transport_a)
	{
		if (node.config.logging.log_ipc ())
		{
			node.logger.always_log ("IPC: created session with id: ", session_id);
		}
	}

	SOCKET_TYPE & get_socket ()
	{
		return socket;
	}

	/**
	 * Async read of exactly \p size_a bytes. The callback is invoked only when all the data is available and
	 * no error has occurred. On error, the error is logged, the read cycle stops and the session ends. Clients
	 * are expected to implement reconnect logic.
	 */
	void async_read_exactly (void * buff_a, size_t size_a, std::function<void()> const & callback_a)
	{
		async_read_exactly (buff_a, size_a, std::chrono::seconds (config_transport.io_timeout), callback_a);
	}

	/**
	 * Async read of exactly \p size_a bytes and a specific \p timeout_a.
	 * @see async_read_exactly (void *, size_t, std::function<void()>)
	 */
	void async_read_exactly (void * buff_a, size_t size_a, std::chrono::seconds timeout_a, std::function<void()> const & callback_a)
	{
		timer_start (timeout_a);
		auto this_l (this->shared_from_this ());
		boost::asio::async_read (socket,
			boost::asio::buffer (buff_a, size_a),
			boost::asio::transfer_exactly (size_a),
			[this_l, callback_a](boost::system::error_code const & ec, size_t bytes_transferred_a) {
				this_l->timer_cancel ();
				if (ec == boost::asio::error::connection_aborted || ec == boost::asio::error::connection_reset)
				{
					if (this_l->node.config.logging.log_ipc ())
					{
						this_l->node.logger.always_log (boost::str (boost::format ("IPC: error reading %1% ") % ec.message ()));
					}
				}
				else if (bytes_transferred_a > 0)
				{
					callback_a ();
				}
			});
	}

	/** Handler for payload_encoding::json_legacy */
	void handle_json_query (bool allow_unsafe)
	{
		session_timer.restart ();
		auto request_id_l (std::to_string (server.id_dispenser.fetch_add (1)));

		// This is called when nano::rpc_handler#process_request is done. We convert to
		// json and write the response to the ipc socket with a length prefix.
		auto this_l (this->shared_from_this ());
		auto response_handler_l ([this_l, request_id_l](std::string const & body) {
			auto big = boost::endian::native_to_big (static_cast<uint32_t> (body.size ()));
			std::vector<uint8_t> buffer;
			buffer.insert (buffer.end (), reinterpret_cast<std::uint8_t *> (&big), reinterpret_cast<std::uint8_t *> (&big) + sizeof (std::uint32_t));
			buffer.insert (buffer.end (), body.begin (), body.end ());
			if (this_l->node.config.logging.log_ipc ())
			{
				this_l->node.logger.always_log (boost::str (boost::format ("IPC/RPC request %1% completed in: %2% %3%") % request_id_l % this_l->session_timer.stop ().count () % this_l->session_timer.unit ()));
			}

			this_l->timer_start (std::chrono::seconds (this_l->config_transport.io_timeout));
			nano::async_write (this_l->socket, nano::shared_const_buffer (buffer), [this_l](boost::system::error_code const & error_a, size_t size_a) {
				this_l->timer_cancel ();
				if (!error_a)
				{
					this_l->read_next_request ();
				}
				else if (this_l->node.config.logging.log_ipc ())
				{
					this_l->node.logger.always_log ("IPC: Write failed: ", error_a.message ());
				}
			});

			// Do not call any member variables here (like session_timer) as it's possible that the next request may already be underway.
		});

		node.stats.inc (nano::stat::type::ipc, nano::stat::detail::invocations);
		auto body (std::string (reinterpret_cast<char *> (buffer.data ()), buffer.size ()));

		// Note that if the rpc action is async, the shared_ptr<json_handler> lifetime will be extended by the action handler
		auto handler (std::make_shared<nano::json_handler> (node, server.node_rpc_config, body, response_handler_l, [& server = server]() {
			server.stop ();
			server.node.alarm.add (std::chrono::steady_clock::now () + std::chrono::seconds (3), [& io_ctx = server.node.alarm.io_ctx]() {
				io_ctx.stop ();
			});
		}));
		// For unsafe actions to be allowed, the unsafe encoding must be used AND the transport config must allow it
		handler->process_request (allow_unsafe && config_transport.allow_unsafe);
	}

	/** Async request reader */
	void read_next_request ()
	{
		auto this_l = this->shared_from_this ();

		// Await next request indefinitely
		buffer.resize (sizeof (buffer_size));
		async_read_exactly (buffer.data (), buffer.size (), std::chrono::seconds::max (), [this_l]() {
			if (this_l->buffer[nano::ipc::preamble_offset::lead] != 'N' || this_l->buffer[nano::ipc::preamble_offset::reserved_1] != 0 || this_l->buffer[nano::ipc::preamble_offset::reserved_2] != 0)
			{
				if (this_l->node.config.logging.log_ipc ())
				{
					this_l->node.logger.always_log ("IPC: Invalid preamble");
				}
			}
			else if (this_l->buffer[nano::ipc::preamble_offset::encoding] == static_cast<uint8_t> (nano::ipc::payload_encoding::json_legacy) || this_l->buffer[nano::ipc::preamble_offset::encoding] == static_cast<uint8_t> (nano::ipc::payload_encoding::json_unsafe))
			{
				auto allow_unsafe (this_l->buffer[nano::ipc::preamble_offset::encoding] == static_cast<uint8_t> (nano::ipc::payload_encoding::json_unsafe));
				// Length of payload
				this_l->async_read_exactly (&this_l->buffer_size, sizeof (this_l->buffer_size), [this_l, allow_unsafe]() {
					boost::endian::big_to_native_inplace (this_l->buffer_size);
					this_l->buffer.resize (this_l->buffer_size);
					// Payload (ptree compliant JSON string)
					this_l->async_read_exactly (this_l->buffer.data (), this_l->buffer_size, [this_l, allow_unsafe]() {
						this_l->handle_json_query (allow_unsafe);
					});
				});
			}
			else if (this_l->node.config.logging.log_ipc ())
			{
				this_l->node.logger.always_log ("IPC: Unsupported payload encoding");
			}
		});
	}

	/** Shut down and close socket */
	void close ()
	{
		socket.shutdown (boost::asio::ip::tcp::socket::shutdown_both);
		socket.close ();
	}

private:
	nano::ipc::ipc_server & server;
	nano::node & node;

	/** Unique session id used for logging */
	uint64_t session_id;

	/** Timer for measuring the duration of ipc calls */
	nano::timer<std::chrono::microseconds> session_timer;

	/**
	 * IO context from node, or per-transport, depending on configuration.
	 * Certain transports may scale better if they use a separate context.
	 */
	boost::asio::io_context & io_ctx;

	/** A socket of the given asio type */
	SOCKET_TYPE socket;

	/** Buffer sizes are read into this */
	uint32_t buffer_size{ 0 };

	/** Buffer used to store data received from the client */
	std::vector<uint8_t> buffer;

	/** Transport configuration */
	nano::ipc::ipc_config_transport & config_transport;
};

/** Domain and TCP socket transport */
template <typename ACCEPTOR_TYPE, typename SOCKET_TYPE, typename ENDPOINT_TYPE>
class socket_transport : public nano::ipc::transport
{
public:
	socket_transport (nano::ipc::ipc_server & server_a, ENDPOINT_TYPE endpoint_a, nano::ipc::ipc_config_transport & config_transport_a, int concurrency_a)
		: server (server_a)
		, config_transport (config_transport_a)
	{
		// Using a per-transport event dispatcher?
		if (concurrency_a > 0)
		{
			io_ctx = std::make_unique<boost::asio::io_context> ();
		}

		boost::asio::socket_base::reuse_address option (true);
		boost::asio::socket_base::keep_alive option_keepalive (true);
		acceptor = std::make_unique<ACCEPTOR_TYPE> (context (), endpoint_a);
		acceptor->set_option (option);
		acceptor->set_option (option_keepalive);
		accept ();

		// Start serving IO requests. If concurrency_a is < 1, the node's thread pool/io_context is used instead.
		// A separate io_context for domain sockets may facilitate better performance on some systems.
		if (concurrency_a > 0)
		{
			runner = std::make_unique<nano::thread_runner> (*io_ctx, static_cast<unsigned> (concurrency_a));
		}
	}

	boost::asio::io_context & context () const
	{
		return io_ctx ? *io_ctx : server.node.io_ctx;
	}

	void accept ()
	{
		// Prepare the next session
		auto new_session (std::make_shared<session<SOCKET_TYPE>> (server, context (), config_transport));

		acceptor->async_accept (new_session->get_socket (), [this, new_session](boost::system::error_code const & ec) {
			if (!ec)
			{
				new_session->read_next_request ();
			}
			else
			{
				server.node.logger.always_log ("IPC: acceptor error: ", ec.message ());
			}

			if (ec != boost::asio::error::operation_aborted && acceptor->is_open ())
			{
				this->accept ();
			}
			else
			{
				server.node.logger.always_log ("IPC: shutting down");
			}
		});
	}

	void stop ()
	{
		acceptor->close ();
		if (io_ctx)
		{
			io_ctx->stop ();
		}

		if (runner)
		{
			runner->join ();
		}
	}

private:
	nano::ipc::ipc_server & server;
	nano::ipc::ipc_config_transport & config_transport;
	std::unique_ptr<nano::thread_runner> runner;
	std::unique_ptr<boost::asio::io_context> io_ctx;
	std::unique_ptr<ACCEPTOR_TYPE> acceptor;
};
}

nano::ipc::ipc_server::ipc_server (nano::node & node_a, nano::node_rpc_config const & node_rpc_config_a)
	: node (node_a)
	, node_rpc_config (node_rpc_config_a)
{
	try
	{
		if (node_a.config.ipc_config.transport_domain.enabled)
		{
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS)
			auto threads = node_a.config.ipc_config.transport_domain.io_threads;
			file_remover = std::make_unique<dsock_file_remover> (node_a.config.ipc_config.transport_domain.path);
			boost::asio::local::stream_protocol::endpoint ep{ node_a.config.ipc_config.transport_domain.path };
			transports.push_back (std::make_shared<socket_transport<boost::asio::local::stream_protocol::acceptor, boost::asio::local::stream_protocol::socket, boost::asio::local::stream_protocol::endpoint>> (*this, ep, node_a.config.ipc_config.transport_domain, threads));
#else
			node.logger.always_log ("IPC: Domain sockets are not supported on this platform");
#endif
		}

		if (node_a.config.ipc_config.transport_tcp.enabled)
		{
			auto threads = node_a.config.ipc_config.transport_tcp.io_threads;
			transports.push_back (std::make_shared<socket_transport<boost::asio::ip::tcp::acceptor, boost::asio::ip::tcp::socket, boost::asio::ip::tcp::endpoint>> (*this, boost::asio::ip::tcp::endpoint (boost::asio::ip::tcp::v6 (), node_a.config.ipc_config.transport_tcp.port), node_a.config.ipc_config.transport_tcp, threads));
		}

		node.logger.always_log ("IPC: server started");
	}
	catch (std::runtime_error const & ex)
	{
		node.logger.always_log ("IPC: ", ex.what ());
	}
}

nano::ipc::ipc_server::~ipc_server ()
{
	node.logger.always_log ("IPC: server stopped");
}

void nano::ipc::ipc_server::stop ()
{
	for (auto & transport : transports)
	{
		transport->stop ();
	}
}
