#include <nano/boost/asio/bind_executor.hpp>
#include <nano/boost/asio/local/stream_protocol.hpp>
#include <nano/boost/asio/read.hpp>
#include <nano/boost/asio/strand.hpp>
#include <nano/lib/config.hpp>
#include <nano/lib/ipc.hpp>
#include <nano/lib/locks.hpp>
#include <nano/lib/threading.hpp>
#include <nano/lib/timer.hpp>
#include <nano/node/common.hpp>
#include <nano/node/ipc/action_handler.hpp>
#include <nano/node/ipc/flatbuffers_handler.hpp>
#include <nano/node/ipc/flatbuffers_util.hpp>
#include <nano/node/ipc/ipc_server.hpp>
#include <nano/node/json_handler.hpp>
#include <nano/node/node.hpp>

#include <boost/asio/signal_set.hpp>
#include <boost/endian/conversion.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <atomic>
#include <chrono>
#include <list>

#include <flatbuffers/flatbuffers.h>

using namespace boost::log;

namespace
{
/**
 * A session manages an inbound connection over which messages are exchanged.
 */
template <typename SOCKET_TYPE>
class session final : public nano::ipc::socket_base, public std::enable_shared_from_this<session<SOCKET_TYPE>>
{
public:
	session (nano::ipc::ipc_server & server_a, boost::asio::io_context & io_ctx_a, nano::ipc::ipc_config_transport & config_transport_a) :
	socket_base (io_ctx_a),
	server (server_a), node (server_a.node), session_id (server_a.id_dispenser.fetch_add (1)),
	io_ctx (io_ctx_a), strand (io_ctx_a.get_executor ()), socket (io_ctx_a), config_transport (config_transport_a)
	{
		if (node.config.logging.log_ipc ())
		{
			node.logger.always_log ("IPC: created session with id: ", session_id.load ());
		}
	}

	~session ()
	{
		close ();
	}

	SOCKET_TYPE & get_socket ()
	{
		return socket;
	}

	std::shared_ptr<nano::ipc::subscriber> get_subscriber ()
	{
		class subscriber_impl final : public nano::ipc::subscriber, public std::enable_shared_from_this<subscriber_impl>
		{
		public:
			subscriber_impl (std::shared_ptr<session> const & session_a) :
			session_m (session_a)
			{
			}
			virtual void async_send_message (uint8_t const * data_a, size_t length_a, std::function<void(nano::error const &)> broadcast_completion_handler_a) override
			{
				if (auto session_l = session_m.lock ())
				{
					auto big_endian_length = std::make_shared<uint32_t> (boost::endian::native_to_big (static_cast<uint32_t> (length_a)));
					boost::array<boost::asio::const_buffer, 2> buffers = {
						boost::asio::buffer (big_endian_length.get (), sizeof (std::uint32_t)),
						boost::asio::buffer (data_a, length_a)
					};

					session_l->queued_write (buffers, [broadcast_completion_handler_a, big_endian_length](boost::system::error_code const & ec_a, size_t size_a) {
						if (broadcast_completion_handler_a)
						{
							nano::error error_l (ec_a);
							broadcast_completion_handler_a (error_l);
						}
					});
				}
			}

			uint64_t get_id () const override
			{
				uint64_t id{ 0 };
				if (auto session_l = session_m.lock ())
				{
					id = session_l->session_id;
				}
				return id;
			}

			std::string get_service_name () const override
			{
				std::string name{ 0 };
				if (auto session_l = session_m.lock ())
				{
					name = session_l->service_name;
				}
				return name;
			}

			void set_service_name (std::string const & service_name_a) override
			{
				if (auto session_l = session_m.lock ())
				{
					session_l->service_name = service_name_a;
				}
			}

			nano::ipc::payload_encoding get_active_encoding () const override
			{
				nano::ipc::payload_encoding encoding{ nano::ipc::payload_encoding::flatbuffers };
				if (auto session_l = session_m.lock ())
				{
					encoding = session_l->active_encoding;
				}
				return encoding;
			}

		private:
			std::weak_ptr<session> session_m;
		};

		static std::mutex subscriber_mutex;
		nano::unique_lock<std::mutex> lock (subscriber_mutex);

		if (!subscriber)
		{
			subscriber = std::make_shared<subscriber_impl> (this->shared_from_this ());
		}
		return subscriber;
	}

	/** Write a fixed array of buffers through the queue. Once the last item is completed, the callback is invoked */
	template <std::size_t N>
	void queued_write (boost::array<boost::asio::const_buffer, N> & buffers, std::function<void(boost::system::error_code const &, size_t)> callback_a)
	{
		auto this_l (this->shared_from_this ());
		boost::asio::post (strand, boost::asio::bind_executor (strand, [buffers, callback_a, this_l]() {
			bool write_in_progress = !this_l->send_queue.empty ();
			auto queue_size = this_l->send_queue.size ();
			if (queue_size < this_l->queue_size_max)
			{
				for (size_t i = 0; i < N - 1; i++)
				{
					this_l->send_queue.emplace_back (queue_item{ buffers[i], nullptr });
				}
				this_l->send_queue.emplace_back (queue_item{ buffers[N - 1], callback_a });
			}
			if (!write_in_progress)
			{
				this_l->write_queued_messages ();
			}
		}));
	}

	/**
	 * Write to underlying socket. Writes goes through a queue protected by the strand. Thus, this function
	 * can be called concurrently with other writes.
	 * @note This function explicitely doesn't use nano::shared_const_buffer, as buffers usually originate from Flatbuffers
	 * and copying into the shared_const_buffer vector would impose a significant overhead for large requests and responses.
	 */
	void queued_write (boost::asio::const_buffer const & buffer_a, std::function<void(boost::system::error_code const &, size_t)> callback_a)
	{
		auto this_l (this->shared_from_this ());
		boost::asio::post (strand, boost::asio::bind_executor (strand, [buffer_a, callback_a, this_l]() {
			bool write_in_progress = !this_l->send_queue.empty ();
			auto queue_size = this_l->send_queue.size ();
			if (queue_size < this_l->queue_size_max)
			{
				this_l->send_queue.emplace_back (queue_item{ buffer_a, callback_a });
			}
			if (!write_in_progress)
			{
				this_l->write_queued_messages ();
			}
		}));
	}

	void write_queued_messages ()
	{
		std::weak_ptr<session> this_w (this->shared_from_this ());
		auto msg (send_queue.front ());
		timer_start (std::chrono::seconds (config_transport.io_timeout));
		nano::unsafe_async_write (socket, msg.buffer,
		boost::asio::bind_executor (strand,
		[msg, this_w](boost::system::error_code ec, std::size_t size_a) {
			if (auto this_l = this_w.lock ())
			{
				this_l->timer_cancel ();

				if (msg.callback)
				{
					msg.callback (ec, size_a);
				}

				this_l->send_queue.pop_front ();
				if (!ec && !this_l->send_queue.empty ())
				{
					this_l->write_queued_messages ();
				}
			}
		}));
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
		boost::asio::bind_executor (strand,
		[this_l, callback_a](boost::system::error_code const & ec, size_t bytes_transferred_a) {
			this_l->timer_cancel ();
			if (ec == boost::asio::error::broken_pipe || ec == boost::asio::error::connection_aborted || ec == boost::asio::error::connection_reset || ec == boost::asio::error::connection_refused)
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
		}));
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
			auto buffer (std::make_shared<std::vector<uint8_t>> ());
			buffer->insert (buffer->end (), reinterpret_cast<std::uint8_t *> (&big), reinterpret_cast<std::uint8_t *> (&big) + sizeof (std::uint32_t));
			buffer->insert (buffer->end (), body.begin (), body.end ());
			if (this_l->node.config.logging.log_ipc ())
			{
				this_l->node.logger.always_log (boost::str (boost::format ("IPC/RPC request %1% completed in: %2% %3%") % request_id_l % this_l->session_timer.stop ().count () % this_l->session_timer.unit ()));
			}

			this_l->timer_start (std::chrono::seconds (this_l->config_transport.io_timeout));
			this_l->queued_write (boost::asio::buffer (buffer->data (), buffer->size ()), [this_l, buffer](boost::system::error_code const & error_a, size_t size_a) {
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
			server.node.env.alarm.add (std::chrono::steady_clock::now () + std::chrono::seconds (3), [& io_ctx = server.node.env.ctx]() {
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
			auto encoding (this_l->buffer[nano::ipc::preamble_offset::encoding]);
			this_l->active_encoding = static_cast<nano::ipc::payload_encoding> (encoding);
			if (this_l->buffer[nano::ipc::preamble_offset::lead] != 'N' || this_l->buffer[nano::ipc::preamble_offset::reserved_1] != 0 || this_l->buffer[nano::ipc::preamble_offset::reserved_2] != 0)
			{
				if (this_l->node.config.logging.log_ipc ())
				{
					this_l->node.logger.always_log ("IPC: Invalid preamble");
				}
			}
			else if (encoding == static_cast<uint8_t> (nano::ipc::payload_encoding::json_v1) || encoding == static_cast<uint8_t> (nano::ipc::payload_encoding::json_v1_unsafe))
			{
				auto allow_unsafe (encoding == static_cast<uint8_t> (nano::ipc::payload_encoding::json_v1_unsafe));
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
			else if (encoding == static_cast<uint8_t> (nano::ipc::payload_encoding::flatbuffers) || encoding == static_cast<uint8_t> (nano::ipc::payload_encoding::flatbuffers_json))
			{
				// Length of payload
				this_l->async_read_exactly (&this_l->buffer_size, sizeof (this_l->buffer_size), [this_l, encoding]() {
					boost::endian::big_to_native_inplace (this_l->buffer_size);
					this_l->buffer.resize (this_l->buffer_size);
					// Payload (flatbuffers or flatbuffers mappable json)
					this_l->async_read_exactly (this_l->buffer.data (), this_l->buffer_size, [this_l, encoding]() {
						this_l->session_timer.restart ();

						// Lazily create one Flatbuffers handler instance per session
						if (!this_l->flatbuffers_handler)
						{
							this_l->flatbuffers_handler = std::make_shared<nano::ipc::flatbuffers_handler> (this_l->node, this_l->server, this_l->get_subscriber (), this_l->node.config.ipc_config);
						}

						if (encoding == static_cast<uint8_t> (nano::ipc::payload_encoding::flatbuffers_json))
						{
							this_l->flatbuffers_handler->process_json (this_l->buffer.data (), this_l->buffer_size, [this_l](std::shared_ptr<std::string> body) {
								if (this_l->node.config.logging.log_ipc ())
								{
									this_l->node.logger.always_log (boost::str (boost::format ("IPC/Flatbuffer request completed in: %1% %2%") % this_l->session_timer.stop ().count () % this_l->session_timer.unit ()));
								}

								auto big_endian_length = std::make_shared<uint32_t> (boost::endian::native_to_big (static_cast<uint32_t> (body->size ())));
								boost::array<boost::asio::const_buffer, 2> buffers = {
									boost::asio::buffer (big_endian_length.get (), sizeof (std::uint32_t)),
									boost::asio::buffer (body->data (), body->size ())
								};

								this_l->queued_write (buffers, [this_l, body, big_endian_length](boost::system::error_code const & error_a, size_t size_a) {
									if (!error_a)
									{
										this_l->read_next_request ();
									}
									else if (this_l->node.config.logging.log_ipc ())
									{
										this_l->node.logger.always_log ("IPC: Write failed: ", error_a.message ());
									}
								});
							});
						}
						else
						{
							this_l->flatbuffers_handler->process (this_l->buffer.data (), this_l->buffer_size, [this_l](std::shared_ptr<flatbuffers::FlatBufferBuilder> fbb) {
								if (this_l->node.config.logging.log_ipc ())
								{
									this_l->node.logger.always_log (boost::str (boost::format ("IPC/Flatbuffer request completed in: %1% %2%") % this_l->session_timer.stop ().count () % this_l->session_timer.unit ()));
								}

								auto big_endian_length = std::make_shared<uint32_t> (boost::endian::native_to_big (static_cast<uint32_t> (fbb->GetSize ())));
								boost::array<boost::asio::const_buffer, 2> buffers = {
									boost::asio::buffer (big_endian_length.get (), sizeof (std::uint32_t)),
									boost::asio::buffer (fbb->GetBufferPointer (), fbb->GetSize ())
								};

								this_l->queued_write (buffers, [this_l, fbb, big_endian_length](boost::system::error_code const & error_a, size_t size_a) {
									if (!error_a)
									{
										this_l->read_next_request ();
									}
									else if (this_l->node.config.logging.log_ipc ())
									{
										this_l->node.logger.always_log ("IPC: Write failed: ", error_a.message ());
									}
								});
							});
						}
					});
				});
			}
			else if (this_l->node.config.logging.log_ipc ())
			{
				this_l->node.logger.always_log ("IPC: Unsupported payload encoding");
			}
		});
	}

	/** Shut down and close socket. This is also called if the timer expires. */
	void close ()
	{
		boost::system::error_code ec_ignored;
		socket.shutdown (boost::asio::ip::tcp::socket::shutdown_both, ec_ignored);
		socket.close (ec_ignored);
	}

private:
	/** Holds the buffer and callback for queued writes */
	class queue_item
	{
	public:
		boost::asio::const_buffer buffer;
		std::function<void(boost::system::error_code const &, size_t)> callback;
	};
	size_t const queue_size_max = 64 * 1024;

	nano::ipc::ipc_server & server;
	nano::node & node;

	/** Unique session id */
	std::atomic<uint64_t> session_id;

	/** Service name associated with this session. This is set through the ServiceRegister API */
	nano::locked<std::string> service_name;

	/**
	 * The payload encoding currently in use by this session. This is set as requests are
	 * received and usually never changes (although a client technically can)
	 */
	std::atomic<nano::ipc::payload_encoding> active_encoding;

	/** Timer for measuring the duration of ipc calls */
	nano::timer<std::chrono::microseconds> session_timer;

	/**
	 * IO context from node, or per-transport, depending on configuration.
	 * Certain transports may scale better if they use a separate context.
	 */
	boost::asio::io_context & io_ctx;

	/** IO strand for synchronizing */
	boost::asio::strand<boost::asio::io_context::executor_type> strand;

	/** The send queue is protected by always being accessed through the strand */
	std::deque<queue_item> send_queue;

	/** A socket of the given asio type */
	SOCKET_TYPE socket;

	/** Buffer sizes are read into this */
	uint32_t buffer_size{ 0 };

	/** Buffer used to store data received from the client */
	std::vector<uint8_t> buffer;

	/** Transport configuration */
	nano::ipc::ipc_config_transport & config_transport;

	/** Handler for Flatbuffers requests. This is created lazily on the first request. */
	std::shared_ptr<nano::ipc::flatbuffers_handler> flatbuffers_handler;

	/** Session subscriber */
	std::shared_ptr<nano::ipc::subscriber> subscriber;
};

/** Domain and TCP socket transport */
template <typename ACCEPTOR_TYPE, typename SOCKET_TYPE, typename ENDPOINT_TYPE>
class socket_transport : public nano::ipc::transport
{
public:
	socket_transport (nano::ipc::ipc_server & server_a, ENDPOINT_TYPE endpoint_a, nano::ipc::ipc_config_transport & config_transport_a, int concurrency_a) :
	server (server_a), config_transport (config_transport_a)
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
		return io_ctx ? *io_ctx : server.node.env.ctx;
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

/**
 * Awaits SIGHUP via signal_set instead of std::signal, as this allows the handler to escape the
 * Posix signal handler restrictions
 */
void await_hup_signal (std::shared_ptr<boost::asio::signal_set> const & signals, nano::ipc::ipc_server & server_a)
{
	signals->async_wait ([signals, &server_a](const boost::system::error_code & ec, int signal_number) {
		if (ec != boost::asio::error::operation_aborted)
		{
			std::cout << "Reloading access configuration..." << std::endl;
			auto error (server_a.reload_access_config ());
			if (!error)
			{
				std::cout << "Reloaded access configuration successfully" << std::endl;
			}
			await_hup_signal (signals, server_a);
		}
	});
}

nano::ipc::ipc_server::ipc_server (nano::node & node_a, nano::node_rpc_config const & node_rpc_config_a) :
node (node_a),
node_rpc_config (node_rpc_config_a),
broker (node_a)
{
	try
	{
		nano::error access_config_error (reload_access_config ());
		if (access_config_error)
		{
			std::exit (1);
		}
#ifndef _WIN32
		// Hook up config reloading through the HUP signal
		auto signals (std::make_shared<boost::asio::signal_set> (node.env.ctx, SIGHUP));
		await_hup_signal (signals, *this);
#endif
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

		if (!transports.empty ())
		{
			broker.start ();
		}
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

nano::ipc::broker & nano::ipc::ipc_server::get_broker ()
{
	return broker;
}

nano::ipc::access & nano::ipc::ipc_server::get_access ()
{
	return access;
}

nano::error nano::ipc::ipc_server::reload_access_config ()
{
	nano::error access_config_error (nano::ipc::read_access_config_toml (node.path, access));
	if (access_config_error)
	{
		auto error (boost::str (boost::format ("IPC: invalid access configuration file: %1%") % access_config_error.get_message ()));
		std::cerr << error << std::endl;
		node.logger.always_log (error);
	}
	return access_config_error;
}
