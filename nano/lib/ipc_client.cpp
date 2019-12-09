#include <nano/boost/asio/bind_executor.hpp>
#include <nano/boost/asio/ip/tcp.hpp>
#include <nano/boost/asio/local/stream_protocol.hpp>
#include <nano/boost/asio/read.hpp>
#include <nano/boost/asio/strand.hpp>
#include <nano/lib/asio.hpp>
#include <nano/lib/ipc.hpp>
#include <nano/lib/ipc_client.hpp>

#include <boost/endian/conversion.hpp>
#include <boost/polymorphic_cast.hpp>

#include <future>

namespace
{
/** Socket agnostic IO interface */
class channel
{
public:
	virtual void async_read (std::shared_ptr<std::vector<uint8_t>> buffer_a, size_t size_a, std::function<void(boost::system::error_code const &, size_t)> callback_a) = 0;
	virtual void async_write (nano::shared_const_buffer const & buffer_a, std::function<void(boost::system::error_code const &, size_t)> callback_a) = 0;
};

/* Boost v1.70 introduced breaking changes; the conditional compilation allows 1.6x to be supported as well. */
#if BOOST_VERSION < 107000
using socket_type = boost::asio::ip::tcp::socket;
#else
using socket_type = boost::asio::basic_stream_socket<boost::asio::ip::tcp, boost::asio::io_context::executor_type>;
#endif

/** Domain and TCP client socket */
template <typename SOCKET_TYPE, typename ENDPOINT_TYPE>
class socket_client : public nano::ipc::socket_base, public channel
{
public:
	socket_client (boost::asio::io_context & io_ctx_a, ENDPOINT_TYPE endpoint_a)
		: socket_base (io_ctx_a)
		, endpoint (endpoint_a)
		, socket (io_ctx_a)
		, resolver (io_ctx_a)
		, strand (io_ctx_a.get_executor ())
	{
	}

	void async_resolve (std::string const & host_a, uint16_t port_a, std::function<void(boost::system::error_code const &, boost::asio::ip::tcp::endpoint)> callback_a)
	{
		this->timer_start (io_timeout);
		resolver.async_resolve (boost::asio::ip::tcp::resolver::query (host_a, std::to_string (port_a)), [this, callback_a](boost::system::error_code const & ec, boost::asio::ip::tcp::resolver::iterator endpoint_iterator_a) {
			this->timer_cancel ();
			boost::asio::ip::tcp::resolver::iterator end;
			if (!ec && endpoint_iterator_a != end)
			{
				endpoint = *endpoint_iterator_a;
				callback_a (ec, *endpoint_iterator_a);
			}
			else
			{
				callback_a (ec, *end);
			}
		});
	}

	void async_connect (std::function<void(boost::system::error_code const &)> callback_a)
	{
		this->timer_start (io_timeout);
		socket.async_connect (endpoint, boost::asio::bind_executor (strand, [this, callback_a](boost::system::error_code const & ec) {
			this->timer_cancel ();
			callback_a (ec);
		}));
	}

	void async_read (std::shared_ptr<std::vector<uint8_t>> buffer_a, size_t size_a, std::function<void(boost::system::error_code const &, size_t)> callback_a) override
	{
		this->timer_start (io_timeout);
		buffer_a->resize (size_a);
		boost::asio::async_read (socket, boost::asio::buffer (buffer_a->data (), size_a), boost::asio::bind_executor (this->strand, [this, callback_a](boost::system::error_code const & ec, size_t size_a) {
			this->timer_cancel ();
			callback_a (ec, size_a);
		}));
	}

	void async_write (nano::shared_const_buffer const & buffer_a, std::function<void(boost::system::error_code const &, size_t)> callback_a) override
	{
		this->timer_start (io_timeout);
		nano::async_write (socket, buffer_a, boost::asio::bind_executor (this->strand, [this, callback_a](boost::system::error_code const & ec, size_t size_a) {
			this->timer_cancel ();
			callback_a (ec, size_a);
		}));
	}

	/** Shut down and close socket */
	void close () override
	{
		socket.shutdown (boost::asio::ip::tcp::socket::shutdown_both);
		socket.close ();
	}

private:
	ENDPOINT_TYPE endpoint;
	SOCKET_TYPE socket;
	boost::asio::ip::tcp::resolver resolver;
	std::chrono::seconds io_timeout{ 60 };
	boost::asio::strand<boost::asio::io_context::executor_type> strand;
};

/**
 * PIMPL class for ipc_client. This ensures that socket_client and boost details can
 * stay out of the header file.
 */
class client_impl : public nano::ipc::ipc_client_impl
{
public:
	explicit client_impl (boost::asio::io_context & io_ctx_a)
		: io_ctx (io_ctx_a)
	{
	}

	void connect (std::string const & host_a, uint16_t port_a, std::function<void(nano::error)> callback_a)
	{
		tcp_client = std::make_shared<socket_client<socket_type, boost::asio::ip::tcp::endpoint>> (io_ctx, boost::asio::ip::tcp::endpoint (boost::asio::ip::tcp::v6 (), port_a));

		tcp_client->async_resolve (host_a, port_a, [this, callback_a](boost::system::error_code const & ec_resolve_a, boost::asio::ip::tcp::endpoint endpoint_a) {
			if (!ec_resolve_a)
			{
				this->tcp_client->async_connect ([callback_a](const boost::system::error_code & ec_connect_a) {
					callback_a (nano::error (ec_connect_a));
				});
			}
			else
			{
				callback_a (nano::error (ec_resolve_a));
			}
		});
	}

	nano::error connect (std::string const & path_a)
	{
		nano::error err;
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS)
		domain_client = std::make_shared<socket_client<boost::asio::local::stream_protocol::socket, boost::asio::local::stream_protocol::endpoint>> (io_ctx, boost::asio::local::stream_protocol::endpoint (path_a));
#else
		err = nano::error ("Domain sockets are not supported by this platform");
#endif
		return err;
	}

	channel & get_channel ()
	{
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS)
		return tcp_client ? static_cast<channel &> (*tcp_client) : static_cast<channel &> (*domain_client);
#else
		return static_cast<channel &> (*tcp_client);
#endif
	}

private:
	boost::asio::io_context & io_ctx;
	std::shared_ptr<socket_client<socket_type, boost::asio::ip::tcp::endpoint>> tcp_client;
#if defined(BOOST_ASIO_HAS_LOCAL_SOCKETS)
	std::shared_ptr<socket_client<boost::asio::local::stream_protocol::socket, boost::asio::local::stream_protocol::endpoint>> domain_client;
#endif
};
}

nano::ipc::ipc_client::ipc_client (boost::asio::io_context & io_ctx_a)
	: io_ctx (io_ctx_a)
{
}

nano::error nano::ipc::ipc_client::connect (std::string const & path_a)
{
	impl = std::make_unique<client_impl> (io_ctx);
	return boost::polymorphic_downcast<client_impl *> (impl.get ())->connect (path_a);
}

void nano::ipc::ipc_client::async_connect (std::string const & host_a, uint16_t port_a, std::function<void(nano::error)> callback_a)
{
	impl = std::make_unique<client_impl> (io_ctx);
	auto client (boost::polymorphic_downcast<client_impl *> (impl.get ()));
	client->connect (host_a, port_a, callback_a);
}

nano::error nano::ipc::ipc_client::connect (std::string const & host, uint16_t port)
{
	std::promise<nano::error> result_l;
	async_connect (host, port, [&result_l](nano::error err_a) {
		result_l.set_value (err_a);
	});
	return result_l.get_future ().get ();
}

void nano::ipc::ipc_client::async_write (nano::shared_const_buffer const & buffer_a, std::function<void(nano::error, size_t)> callback_a)
{
	auto client (boost::polymorphic_downcast<client_impl *> (impl.get ()));
	client->get_channel ().async_write (buffer_a, [callback_a](const boost::system::error_code & ec_a, size_t bytes_written_a) {
		callback_a (nano::error (ec_a), bytes_written_a);
	});
}

void nano::ipc::ipc_client::async_read (std::shared_ptr<std::vector<uint8_t>> buffer_a, size_t size_a, std::function<void(nano::error, size_t)> callback_a)
{
	auto client (boost::polymorphic_downcast<client_impl *> (impl.get ()));
	client->get_channel ().async_read (buffer_a, size_a, [callback_a](const boost::system::error_code & ec_a, size_t bytes_read_a) {
		callback_a (nano::error (ec_a), bytes_read_a);
	});
}

nano::shared_const_buffer nano::ipc::prepare_request (nano::ipc::payload_encoding encoding_a, std::string const & payload_a)
{
	std::vector<uint8_t> buffer_l;
	if (encoding_a == nano::ipc::payload_encoding::json_legacy)
	{
		buffer_l.push_back ('N');
		buffer_l.push_back (static_cast<uint8_t> (encoding_a));
		buffer_l.push_back (0);
		buffer_l.push_back (0);

		auto payload_length = static_cast<uint32_t> (payload_a.size ());
		uint32_t be = boost::endian::native_to_big (payload_length);
		char * chars = reinterpret_cast<char *> (&be);
		buffer_l.insert (buffer_l.end (), chars, chars + sizeof (uint32_t));
		buffer_l.insert (buffer_l.end (), payload_a.begin (), payload_a.end ());
	}
	return nano::shared_const_buffer{ std::move (buffer_l) };
}

std::string nano::ipc::request (nano::ipc::ipc_client & ipc_client, std::string const & rpc_action_a)
{
	auto req (prepare_request (nano::ipc::payload_encoding::json_legacy, rpc_action_a));
	auto res (std::make_shared<std::vector<uint8_t>> ());

	std::promise<std::string> result_l;
	// clang-format off
	ipc_client.async_write (req, [&ipc_client, &res, &result_l](nano::error err_a, size_t size_a) {
		// Read length
		ipc_client.async_read (res, sizeof (uint32_t), [&ipc_client, &res, &result_l](nano::error err_read_a, size_t size_read_a) {
			uint32_t payload_size_l = boost::endian::big_to_native (*reinterpret_cast<uint32_t *> (res->data ()));
			// Read json payload
			ipc_client.async_read (res, payload_size_l, [&res, &result_l](nano::error err_read_a, size_t size_read_a) {
				result_l.set_value (std::string (res->begin (), res->end ()));
			});
		});
	});
	// clang-format on

	return result_l.get_future ().get ();
}
