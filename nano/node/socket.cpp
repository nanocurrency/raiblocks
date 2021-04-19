#include <nano/boost/asio/bind_executor.hpp>
#include <nano/boost/asio/dispatch.hpp>
#include <nano/boost/asio/read.hpp>
#include <nano/node/node.hpp>
#include <nano/node/socket.hpp>

#include <boost/format.hpp>

#include <limits>

nano::socket::socket (nano::node & node_a) :
strand{ node_a.io_ctx.get_executor () },
tcp_socket{ node_a.io_ctx },
node{ node_a },
next_deadline{ std::numeric_limits<uint64_t>::max () },
last_completion_time{ 0 },
io_timeout{ node_a.config.tcp_io_timeout }
{
}

nano::socket::~socket ()
{
	close_internal ();
}

void nano::socket::async_connect (nano::tcp_endpoint const & endpoint_a, std::function<void(boost::system::error_code const &)> callback_a)
{
	checkup ();
	auto this_l (shared_from_this ());
	start_timer ();
	this_l->tcp_socket.async_connect (endpoint_a,
	boost::asio::bind_executor (this_l->strand,
	[this_l, callback_a, endpoint_a](boost::system::error_code const & ec) {
		this_l->stop_timer ();
		this_l->remote = endpoint_a;
		callback_a (ec);
	}));
}

void nano::socket::async_read (std::shared_ptr<std::vector<uint8_t>> const & buffer_a, size_t size_a, std::function<void(boost::system::error_code const &, size_t)> callback_a)
{
	if (size_a <= buffer_a->size ())
	{
		auto this_l (shared_from_this ());
		if (!closed)
		{
			start_timer ();
			boost::asio::post (strand, boost::asio::bind_executor (strand, [buffer_a, callback_a, size_a, this_l]() {
				boost::asio::async_read (this_l->tcp_socket, boost::asio::buffer (buffer_a->data (), size_a),
				boost::asio::bind_executor (this_l->strand,
				[this_l, buffer_a, callback_a](boost::system::error_code const & ec, size_t size_a) {
					this_l->node.stats.add (nano::stat::type::traffic_tcp, nano::stat::dir::in, size_a);
					this_l->stop_timer ();
					callback_a (ec, size_a);
				}));
			}));
		}
	}
	else
	{
		debug_assert (false && "nano::socket::async_read called with incorrect buffer size");
		boost::system::error_code ec_buffer = boost::system::errc::make_error_code (boost::system::errc::no_buffer_space);
		callback_a (ec_buffer, 0);
	}
}

void nano::socket::async_write (nano::shared_const_buffer const & buffer_a, std::function<void(boost::system::error_code const &, size_t)> const & callback_a)
{
	if (!closed)
	{
		++queue_size;
		boost::asio::post (strand, boost::asio::bind_executor (strand, [buffer_a, callback_a, this_l = shared_from_this ()]() {
			if (!this_l->closed)
			{
				this_l->start_timer ();
				nano::async_write (this_l->tcp_socket, buffer_a,
				boost::asio::bind_executor (this_l->strand,
				[buffer_a, callback_a, this_l](boost::system::error_code ec, std::size_t size_a) {
					--this_l->queue_size;
					this_l->node.stats.add (nano::stat::type::traffic_tcp, nano::stat::dir::out, size_a);
					this_l->stop_timer ();
					if (callback_a)
					{
						callback_a (ec, size_a);
					}
				}));
			}
			else
			{
				if (callback_a)
				{
					callback_a (boost::system::errc::make_error_code (boost::system::errc::not_supported), 0);
				}
			}
		}));
	}
	else if (callback_a)
	{
		node.background ([callback_a]() {
			callback_a (boost::system::errc::make_error_code (boost::system::errc::not_supported), 0);
		});
	}
}

void nano::socket::start_timer ()
{
	start_timer (io_timeout);
}

void nano::socket::start_timer (std::chrono::seconds deadline_a)
{
	next_deadline = deadline_a.count ();
}

void nano::socket::stop_timer ()
{
	last_completion_time = nano::seconds_since_epoch ();
}

void nano::socket::checkup ()
{
	std::weak_ptr<nano::socket> this_w (shared_from_this ());
	node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::seconds (node.network_params.network.is_dev_network () ? 1 : 2), [this_w]() {
		if (auto this_l = this_w.lock ())
		{
			uint64_t now (nano::seconds_since_epoch ());
			if (this_l->next_deadline != std::numeric_limits<uint64_t>::max () && now - this_l->last_completion_time > this_l->next_deadline)
			{
				if (this_l->node.config.logging.network_timeout_logging ())
				{
					// The remote end may have closed the connection before this side timing out, in which case the remote address is no longer available.
					boost::system::error_code ec_remote_l;
					boost::asio::ip::tcp::endpoint remote_endpoint_l = this_l->tcp_socket.remote_endpoint (ec_remote_l);
					if (!ec_remote_l)
					{
						this_l->node.logger.try_log (boost::str (boost::format ("Disconnecting from %1% due to timeout") % remote_endpoint_l));
					}
				}
				this_l->timed_out = true;
				this_l->close ();
			}
			else if (!this_l->closed)
			{
				this_l->checkup ();
			}
		}
	});
}

bool nano::socket::has_timed_out () const
{
	return timed_out;
}

void nano::socket::timeout_set (std::chrono::seconds io_timeout_a)
{
	io_timeout = io_timeout_a;
}

void nano::socket::close ()
{
	auto this_l (shared_from_this ());
	boost::asio::dispatch (strand, boost::asio::bind_executor (strand, [this_l] {
		this_l->close_internal ();
	}));
}

// This must be called from a strand or the destructor
void nano::socket::close_internal ()
{
	if (!closed.exchange (true))
	{
		io_timeout = std::chrono::seconds (0);
		boost::system::error_code ec;

		// Ignore error code for shutdown as it is best-effort
		tcp_socket.shutdown (boost::asio::ip::tcp::socket::shutdown_both, ec);
		tcp_socket.close (ec);
		if (ec)
		{
			node.logger.try_log ("Failed to close socket gracefully: ", ec.message ());
			node.stats.inc (nano::stat::type::bootstrap, nano::stat::detail::error_socket_close);
		}
	}
}

nano::tcp_endpoint nano::socket::remote_endpoint () const
{
	return remote;
}

nano::server_socket::server_socket (nano::node & node_a, boost::asio::ip::tcp::endpoint local_a, size_t max_connections_a) :
socket{ node_a },
acceptor{ node_a.io_ctx },
local{ local_a },
max_inbound_connections{ max_connections_a }
{
	io_timeout = std::chrono::seconds::max ();
}

void nano::server_socket::start (boost::system::error_code & ec_a)
{
	acceptor.open (local.protocol ());
	acceptor.set_option (boost::asio::ip::tcp::acceptor::reuse_address (true));
	acceptor.bind (local, ec_a);
	if (!ec_a)
	{
		acceptor.listen (boost::asio::socket_base::max_listen_connections, ec_a);
	}
}

void nano::server_socket::close ()
{
	auto this_l (std::static_pointer_cast<nano::server_socket> (shared_from_this ()));

	boost::asio::dispatch (strand, boost::asio::bind_executor (strand, [this_l]() {
		this_l->close_internal ();
		this_l->acceptor.close ();
		for (auto & connection_w : this_l->connections)
		{
			if (auto connection_l = connection_w.lock ())
			{
				connection_l->close ();
			}
		}
		this_l->connections.clear ();
	}));
}

void nano::server_socket::on_connection (std::function<bool(std::shared_ptr<nano::socket> const &, boost::system::error_code const &)> callback_a)
{
	auto this_l (std::static_pointer_cast<nano::server_socket> (shared_from_this ()));

	boost::asio::post (strand, boost::asio::bind_executor (strand, [this_l, callback_a]() {
		if (this_l->acceptor.is_open ())
		{
			if (this_l->connections.size () < this_l->max_inbound_connections)
			{
				// Prepare new connection
				auto new_connection = std::make_shared<nano::socket> (this_l->node);
				this_l->acceptor.async_accept (new_connection->tcp_socket, new_connection->remote,
				boost::asio::bind_executor (this_l->strand,
				[this_l, new_connection, callback_a](boost::system::error_code const & ec_a) {
					this_l->evict_dead_connections ();
					if (this_l->connections.size () < this_l->max_inbound_connections)
					{
						if (!ec_a)
						{
							// Make sure the new connection doesn't idle. Note that in most cases, the callback is going to start
							// an IO operation immediately, which will start a timer.
							new_connection->checkup ();
							new_connection->start_timer (this_l->node.network_params.network.is_dev_network () ? std::chrono::seconds (2) : this_l->node.network_params.node.idle_timeout);
							this_l->node.stats.inc (nano::stat::type::tcp, nano::stat::detail::tcp_accept_success, nano::stat::dir::in);
							this_l->connections.push_back (new_connection);
						}
						else
						{
							this_l->node.logger.try_log ("Unable to accept connection: ", ec_a.message ());
						}

						// If the callback returns true, keep accepting new connections
						if (callback_a (new_connection, ec_a))
						{
							this_l->on_connection (callback_a);
						}
						else
						{
							this_l->node.logger.try_log ("Stopping to accept connections");
						}
					}
					else
					{
						this_l->node.stats.inc (nano::stat::type::tcp, nano::stat::detail::tcp_accept_failure, nano::stat::dir::in);
						boost::asio::post (this_l->strand, boost::asio::bind_executor (this_l->strand, [this_l, callback_a]() {
							this_l->on_connection (callback_a);
						}));
					}
				}));
			}
		}
	}));
}

// This must be called from a strand
void nano::server_socket::evict_dead_connections ()
{
	debug_assert (strand.running_in_this_thread ());
	connections.erase (std::remove_if (connections.begin (), connections.end (), [](auto & connection) { return connection.expired (); }), connections.end ());
}
