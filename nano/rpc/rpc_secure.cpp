#include <nano/rpc/rpc_connection_secure.hpp>
#include <nano/rpc/rpc_secure.hpp>

#include <boost/format.hpp>
#include <boost/polymorphic_pointer_cast.hpp>

#include <iostream>

bool nano::rpc_secure::on_verify_certificate (bool preverified, boost::asio::ssl::verify_context & ctx)
{
	X509_STORE_CTX * cts = ctx.native_handle ();
	auto error (X509_STORE_CTX_get_error (cts));
	switch (error)
	{
		case X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT:
			logger.always_log ("TLS: Unable to get issuer");
			break;
		case X509_V_ERR_CERT_NOT_YET_VALID:
		case X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD:
			logger.always_log ("TLS: Certificate not yet valid");
			break;
		case X509_V_ERR_CERT_HAS_EXPIRED:
		case X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD:
			logger.always_log ("TLS: Certificate expired");
			break;
		case X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN:
			if (config.secure.verbose_logging)
			{
				logger.always_log ("TLS: self signed certificate in chain");
			}

			// Allow self-signed certificates
			preverified = true;
			break;
		case X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT:
			logger.always_log ("TLS: Self signed certificate not in the list of trusted certs (forgot to subject-hash certificate filename?)");
			break;
		default:
			break;
	}

	if (config.secure.verbose_logging)
	{
		if (error != 0)
		{
			logger.always_log ("TLS: Error: ", X509_verify_cert_error_string (error));
			logger.always_log ("TLS: Error chain depth : ", X509_STORE_CTX_get_error_depth (cts));
		}

		X509 * cert = X509_STORE_CTX_get_current_cert (cts);
		char subject_name[512];
		X509_NAME_oneline (X509_get_subject_name (cert), subject_name, sizeof (subject_name) - 1);
		logger.always_log ("TLS: Verifying: ", subject_name);
		logger.always_log ("TLS: Verification: ", preverified);
	}
	else if (!preverified)
	{
		logger.always_log ("TLS: Pre-verification failed. Turn on verbose logging for more information.");
	}

	return preverified;
}

void nano::rpc_secure::load_certs (boost::asio::ssl::context & context_a)
{
	try
	{
		// This is called if the key is password protected
		context_a.set_password_callback (
			[this](std::size_t,
				boost::asio::ssl::context_base::password_purpose) {
				return config.secure.server_key_passphrase;
			});

		// The following two options disables the session cache and enables stateless session resumption.
		// This is necessary because of the way the RPC server abruptly terminate connections.
		SSL_CTX_set_session_cache_mode (context_a.native_handle (), SSL_SESS_CACHE_OFF);
		SSL_CTX_set_options (context_a.native_handle (), SSL_OP_NO_TICKET);

		context_a.set_options (
			boost::asio::ssl::context::default_workarounds | boost::asio::ssl::context::no_sslv2 | boost::asio::ssl::context::no_sslv3 | boost::asio::ssl::context::single_dh_use);

		context_a.use_certificate_chain_file (config.secure.server_cert_path);
		context_a.use_private_key_file (config.secure.server_key_path, boost::asio::ssl::context::pem);
		context_a.use_tmp_dh_file (config.secure.server_dh_path);

		// Verify client certificates?
		if (!config.secure.client_certs_path.empty ())
		{
			context_a.set_verify_mode (boost::asio::ssl::verify_fail_if_no_peer_cert | boost::asio::ssl::verify_peer);
			context_a.add_verify_path (config.secure.client_certs_path);
			context_a.set_verify_callback ([this](auto preverified, auto & ctx) {
				return this->on_verify_certificate (preverified, ctx);
			});
		}
	}
	catch (boost::system::system_error const & err)
	{
		auto error (boost::str (boost::format ("Could not load certificate information: %1%. Make sure the paths in the secure rpc configuration are correct.") % err.what ()));
		std::cerr << error << std::endl;
		logger.always_log (error);
	}
}

nano::rpc_secure::rpc_secure (boost::asio::io_context & context_a, nano::rpc_config const & config_a, nano::rpc_handler_interface & rpc_handler_interface_a)
	: rpc (context_a, config_a, rpc_handler_interface_a)
	, ssl_context (boost::asio::ssl::context::tlsv12_server)
{
	load_certs (ssl_context);
}

void nano::rpc_secure::accept ()
{
	auto connection (std::make_shared<nano::rpc_connection_secure> (config, io_ctx, logger, rpc_handler_interface, this->ssl_context));
	acceptor.async_accept (connection->socket, boost::asio::bind_executor (connection->strand, [this, connection](boost::system::error_code const & ec) {
		if (acceptor.is_open ())
		{
			accept ();
		}
		if (!ec)
		{
			connection->parse_connection ();
		}
		else
		{
			logger.always_log (boost::str (boost::format ("Error accepting RPC connections: %1%") % ec));
		}
	}));
}
