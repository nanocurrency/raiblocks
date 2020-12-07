#include <nano/lib/config.hpp>
#include <nano/lib/logger_mt.hpp>
#include <nano/lib/tlsconfig.hpp>
#include <nano/lib/tomlconfig.hpp>

#include <boost/format.hpp>

#include <iostream>

namespace nano
{
nano::error nano::tls_config::serialize_toml (nano::tomlconfig & toml) const
{
	toml.put ("enable_https", enable_https, "Enable or disable https:// support.\ntype:bool");
	toml.put ("enable_wss", enable_wss, "Enable or disable wss:// support.\ntype:bool");
	toml.put ("verbose_logging", verbose_logging, "Enable or disable verbose TLS logging.\ntype:bool");
	toml.put ("server_key_passphrase", server_key_passphrase, "Server key passphrase.\ntype:string");
	toml.put ("server_cert_path", server_cert_path, "Directory containing certificates.\ntype:string,path");
	toml.put ("server_key_path", server_key_path, "Path to server key PEM file.\ntype:string,path");
	toml.put ("server_dh_path", server_dh_path, "Path to Diffie-Hellman params file.\ntype:string,path");
	toml.put ("client_certs_path", client_certs_path, "Directory containing optional client certificates.\ntype:string,path");
	return toml.get_error ();
}

nano::error nano::tls_config::deserialize_toml (nano::tomlconfig & toml)
{
	toml.get<bool> ("enable_https", enable_https);
	toml.get<bool> ("enable_wss", enable_wss);
	toml.get<bool> ("verbose_logging", verbose_logging);
	toml.get<std::string> ("server_key_passphrase", server_key_passphrase);
	toml.get<std::string> ("server_cert_path", server_cert_path);
	toml.get<std::string> ("server_key_path", server_key_path);
	toml.get<std::string> ("server_dh_path", server_dh_path);
	toml.get<std::string> ("client_certs_path", client_certs_path);
	return toml.get_error ();
}

#ifdef NANO_SECURE_RPC
namespace
{
	bool on_verify_certificate (bool preverified, boost::asio::ssl::verify_context & ctx, nano::tls_config & config_a, nano::logger_mt & logger_a)
	{
		X509_STORE_CTX * cts = ctx.native_handle ();
		auto error (X509_STORE_CTX_get_error (cts));
		switch (error)
		{
			case X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT:
				logger_a.always_log ("TLS: Unable to get issuer");
				break;
			case X509_V_ERR_CERT_NOT_YET_VALID:
			case X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD:
				logger_a.always_log ("TLS: Certificate not yet valid");
				break;
			case X509_V_ERR_CERT_HAS_EXPIRED:
			case X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD:
				logger_a.always_log ("TLS: Certificate expired");
				break;
			case X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN:
				if (config_a.verbose_logging)
				{
					logger_a.always_log ("TLS: Self-signed certificate in chain");
				}

				// Allow self-signed certificates
				preverified = true;
				break;
			case X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT:
				logger_a.always_log ("TLS: Self-signed certificate not in the list of trusted certs (forgot to subject-hash certificate filename?)");
				break;
			default:
				break;
		}

		if (config_a.verbose_logging)
		{
			if (error != 0)
			{
				logger_a.always_log ("TLS: Error: ", X509_verify_cert_error_string (error));
				logger_a.always_log ("TLS: Error chain depth : ", X509_STORE_CTX_get_error_depth (cts));
			}

			X509 * cert = X509_STORE_CTX_get_current_cert (cts);
			char subject_name[512];
			X509_NAME_oneline (X509_get_subject_name (cert), subject_name, sizeof (subject_name) - 1);
			logger_a.always_log ("TLS: Verifying: ", subject_name);
			logger_a.always_log ("TLS: Verification: ", preverified);
		}
		else if (!preverified)
		{
			logger_a.always_log ("TLS: Pre-verification failed. Turn on verbose logging for more information.");
		}

		return preverified;
	}

	void load_certs (nano::tls_config & config_a, nano::logger_mt & logger_a)
	{
		try
		{
			// This is called if the key is password protected
			config_a.ssl_context.set_password_callback (
			[&config_a](std::size_t,
			boost::asio::ssl::context_base::password_purpose) {
				return config_a.server_key_passphrase;
			});

			// The following two options disables the session cache and enables stateless session resumption.
			// This is necessary because of the way the RPC server abruptly terminate connections.
			SSL_CTX_set_session_cache_mode (config_a.ssl_context.native_handle (), SSL_SESS_CACHE_OFF);
			SSL_CTX_set_options (config_a.ssl_context.native_handle (), SSL_OP_NO_TICKET);

			config_a.ssl_context.set_options (
			boost::asio::ssl::context::default_workarounds | boost::asio::ssl::context::no_sslv2 | boost::asio::ssl::context::no_sslv3 | boost::asio::ssl::context::single_dh_use);

			config_a.ssl_context.use_certificate_chain_file (config_a.server_cert_path);
			config_a.ssl_context.use_private_key_file (config_a.server_key_path, boost::asio::ssl::context::pem);
			config_a.ssl_context.use_tmp_dh_file (config_a.server_dh_path);

			// Verify client certificates?
			if (!config_a.client_certs_path.empty ())
			{
				config_a.ssl_context.set_verify_mode (boost::asio::ssl::verify_fail_if_no_peer_cert | boost::asio::ssl::verify_peer);
				config_a.ssl_context.add_verify_path (config_a.client_certs_path);
				config_a.ssl_context.set_verify_callback ([&config_a, &logger_a](auto preverified, auto & ctx) {
					return on_verify_certificate (preverified, ctx, config_a, logger_a);
				});
			}

			logger_a.always_log ("TLS: successfully configured");
		}
		catch (boost::system::system_error const & err)
		{
			auto error (boost::str (boost::format ("Could not load certificate information: %1%. Make sure the paths and the passphrase in config-tls.toml are correct.") % err.what ()));
			std::cerr << error << std::endl;
			logger_a.always_log (error);
		}
	}
}
#endif

nano::error read_tls_config_toml (boost::filesystem::path const & data_path_a, nano::tls_config & config_a, nano::logger_mt & logger_a, std::vector<std::string> const & config_overrides)
{
	nano::error error;
	auto toml_config_path = nano::get_tls_toml_config_path (data_path_a);

	// Parse and deserialize
	nano::tomlconfig toml;

	std::stringstream config_overrides_stream;
	for (auto const & entry : config_overrides)
	{
		config_overrides_stream << entry << std::endl;
	}
	config_overrides_stream << std::endl;

	// Make sure we don't create an empty toml file if it doesn't exist. Running without a tls toml file is the default.
	if (!error)
	{
		if (boost::filesystem::exists (toml_config_path))
		{
			error = toml.read (config_overrides_stream, toml_config_path);
		}
		else
		{
			error = toml.read (config_overrides_stream);
		}
	}

	if (!error)
	{
		error = config_a.deserialize_toml (toml);
	}

	if (!error && (config_a.enable_https || config_a.enable_wss))
	{
#ifdef NANO_SECURE_RPC
		load_certs (config_a, logger_a);
#else
		auto msg ("https or wss is enabled in the TLS configuration, but the node is not built with NANO_SECURE_RPC");
		std::cerr << msg << std::endl;
		logger_a.always_log (msg);
		std::exit (1);
#endif
	}

	return error;
}
}
