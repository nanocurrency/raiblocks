#pragma once

#include <nano/boost/asio/strand.hpp>
#include <nano/boost/beast/core.hpp>
#include <nano/boost/beast/websocket.hpp>
#include <nano/lib/asio.hpp>

#include <memory>

/* Boost v1.70 introduced breaking changes; the conditional compilation allows 1.6x to be supported as well. */
#if BOOST_VERSION < 107000
using socket_type = boost::asio::ip::tcp::socket;
#define beast_buffers boost::beast::buffers
#else
using socket_type = boost::asio::basic_stream_socket<boost::asio::ip::tcp, boost::asio::io_context::executor_type>;
#define beast_buffers boost::beast::make_printable
#endif
using ws_type = boost::beast::websocket::stream<socket_type>;

#ifdef NANO_SECURE_RPC
#include <boost/asio/ssl/context.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket/ssl.hpp>
using wss_type = boost::beast::websocket::stream<boost::beast::ssl_stream<socket_type>>;
#endif

namespace nano::websocket
{
/** The minimal stream interface needed by the Nano websocket implementation */
class websocket_stream_concept
{
public:
	virtual ~websocket_stream_concept () = default;
	virtual boost::asio::strand<boost::asio::io_context::executor_type> & get_strand () = 0;
	virtual void handshake (std::function<void(boost::system::error_code const & ec)> callback_a) = 0;
	virtual void close (boost::beast::websocket::close_reason const & reason_a, boost::system::error_code & ec_a) = 0;
	virtual void async_write (nano::shared_const_buffer const & buffer_a, std::function<void(boost::system::error_code, std::size_t)> callback_a) = 0;
	virtual void async_read (boost::beast::multi_buffer & buffer_a, std::function<void(boost::system::error_code, std::size_t)> callback_a) = 0;
};

/**
 * Beast websockets doesn't provide a common base type for tls and non-tls streams, so we use
 * the type erasure idiom to be able to use both kinds of streams through a common type.
 */
class stream final : public websocket_stream_concept
{
public:
#ifdef NANO_SECURE_RPC
	stream (socket_type socket_a, boost::asio::ssl::context & ctx_a);
#endif
	stream (socket_type socket_a);

	[[nodiscard]] boost::asio::strand<boost::asio::io_context::executor_type> & get_strand () override;
	void handshake (std::function<void(boost::system::error_code const & ec)> callback_a) override;
	void close (boost::beast::websocket::close_reason const & reason_a, boost::system::error_code & ec_a) override;
	void async_write (nano::shared_const_buffer const & buffer_a, std::function<void(boost::system::error_code, std::size_t)> callback_a) override;
	void async_read (boost::beast::multi_buffer & buffer_a, std::function<void(boost::system::error_code, std::size_t)> callback_a) override;

private:
	std::unique_ptr<websocket_stream_concept> impl;
};
}
