#include <nano/node/websocket_stream.hpp>

#include <boost/asio/bind_executor.hpp>

namespace
{
/** Type-erasing wrapper for tls and non-tls websocket streams */
template <typename stream_type>
class stream_wrapper : public nano::websocket::websocket_stream_concept
{
public:
#ifdef NANO_SECURE_RPC
	stream_wrapper (socket_type socket_a, boost::asio::ssl::context & ctx_a) :
	ws (std::move (socket_a), ctx_a), strand (ws.get_executor ())
	{
		is_tls = true;
		ws.text (true);
	}
#endif

	stream_wrapper (socket_type socket_a) :
	ws (std::move (socket_a)), strand (ws.get_executor ())
	{
		ws.text (true);
	}

	void handshake (std::function<void(boost::system::error_code const & ec)> callback_a) override
	{
		if (is_tls)
		{
			ssl_handshake (callback_a);
		}
		else
		{
			// Websocket handshake
			ws.async_accept ([callback_a](boost::system::error_code const & ec) {
				callback_a (ec);
			});
		}
	}

	void ssl_handshake (std::function<void(boost::system::error_code const & ec)> callback_a)
	{
#ifdef NANO_SECURE_RPC
		// Only perform TLS handshakes for TLS streams
		if constexpr (std::is_same<wss_type, stream_type>::value)
		{
			ws.next_layer ().async_handshake (boost::asio::ssl::stream_base::server, [this, callback_a](boost::system::error_code const & ec) {
				if (!ec)
				{
					// Websocket handshake
					this->ws.async_accept ([callback_a](boost::system::error_code const & ec) {
						callback_a (ec);
					});
				}
				else
				{
					callback_a (ec);
				}
			});
		}
#endif
	}

	boost::asio::strand<boost::asio::io_context::executor_type> & get_strand () override
	{
		return strand;
	}

	void close (boost::beast::websocket::close_reason const & reason_a, boost::system::error_code & ec_a) override
	{
		ws.close (reason_a, ec_a);
	}

	void async_write (nano::shared_const_buffer const & buffer_a, std::function<void(boost::system::error_code, std::size_t)> callback_a) override
	{
		ws.async_write (buffer_a, boost::asio::bind_executor (strand, callback_a));
	}

	void async_read (boost::beast::multi_buffer & buffer_a, std::function<void(boost::system::error_code, std::size_t)> callback_a) override
	{
		ws.async_read (buffer_a, boost::asio::bind_executor (strand, callback_a));
	}

private:
	bool is_tls{ false };
	stream_type ws;
	boost::asio::strand<boost::asio::io_context::executor_type> strand;
};
}

#ifdef NANO_SECURE_RPC
nano::websocket::stream::stream (socket_type socket_a, boost::asio::ssl::context & ctx_a)
{
	impl = std::make_unique<stream_wrapper<wss_type>> (std::move (socket_a), ctx_a);
}
#endif
nano::websocket::stream::stream (socket_type socket_a)
{
	impl = std::make_unique<stream_wrapper<ws_type>> (std::move (socket_a));
}

[[nodiscard]] boost::asio::strand<boost::asio::io_context::executor_type> & nano::websocket::stream::get_strand ()
{
	return impl->get_strand ();
}

void nano::websocket::stream::handshake (std::function<void(boost::system::error_code const & ec)> callback_a)
{
	impl->handshake (callback_a);
}

void nano::websocket::stream::close (boost::beast::websocket::close_reason const & reason_a, boost::system::error_code & ec_a)
{
	impl->close (reason_a, ec_a);
}

void nano::websocket::stream::async_write (nano::shared_const_buffer const & buffer_a, std::function<void(boost::system::error_code, std::size_t)> callback_a)
{
	impl->async_write (buffer_a, callback_a);
}

void nano::websocket::stream::async_read (boost::beast::multi_buffer & buffer_a, std::function<void(boost::system::error_code, std::size_t)> callback_a)
{
	impl->async_read (buffer_a, callback_a);
}
