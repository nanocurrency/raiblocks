#pragma once

#include <nano/lib/config.hpp>
#include <nano/lib/errors.hpp>

#include <memory>

namespace nano
{
class jsonconfig;
class tomlconfig;
class tls_config;
namespace websocket
{
	/** websocket configuration */
	class config final
	{
	public:
		config ();
		nano::error deserialize_json (nano::jsonconfig & json_a);
		nano::error serialize_json (nano::jsonconfig & json) const;
		nano::error deserialize_toml (nano::tomlconfig & toml_a);
		nano::error serialize_toml (nano::tomlconfig & toml) const;
		nano::network_constants network_constants;
		bool enabled{ false };
		uint16_t port;
		std::string address;
		/** Optional TLS config */
		std::shared_ptr<nano::tls_config> tls_config;
	};
}
}
