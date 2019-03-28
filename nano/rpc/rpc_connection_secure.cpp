#include <nano/rpc/rpc_connection_secure.hpp>
#include <nano/rpc/rpc_secure.hpp>

#include <boost/polymorphic_pointer_cast.hpp>


nano::rpc_connection_secure::rpc_connection_secure (nano::rpc_config const & rpc_config, nano::network_constants const & network_constants, boost::asio::io_context & io_ctx, nano::logger_mt & logger, nano::rpc_request_processor & rpc_request_processor, boost::asio::ssl::context & ssl_context) :
nano::rpc_connection (rpc_config, network_constants, io_ctx, logger, rpc_request_processor),
stream (socket, ssl_context)
{
}

void nano::rpc_connection_secure::parse_connection ()
{
	// Perform the SSL handshake
	auto this_l = std::static_pointer_cast<nano::rpc_connection_secure> (shared_from_this ());
	stream.async_handshake (boost::asio::ssl::stream_base::server,
	[this_l](auto & ec) {
		this_l->handle_handshake (ec);
	});
}

void nano::rpc_connection_secure::on_shutdown (const boost::system::error_code & error)
{
	// No-op. We initiate the shutdown (since the RPC server kills the connection after each request)
	// and we'll thus get an expected EOF error. If the client disconnects, a short-read error will be expected.
}

void nano::rpc_connection_secure::handle_handshake (const boost::system::error_code & error)
{
	if (!error)
	{
		read ();
	}
	else
	{
		logger.always_log ("TLS: Handshake error: ", error.message ());
	}
}

void nano::rpc_connection_secure::write_completion_handler (std::shared_ptr<nano::rpc_connection> rpc)
{
	auto rpc_connection_secure = boost::polymorphic_pointer_downcast<nano::rpc_connection_secure> (rpc);
	rpc_connection_secure->stream.async_shutdown ([rpc_connection_secure](auto const & ec_shutdown) {
		rpc_connection_secure->on_shutdown (ec_shutdown);
	});
}
