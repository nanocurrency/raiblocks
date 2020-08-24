#pragma once

#include <nano/lib/errors.hpp>
#include <nano/node/node_pow_server_config.hpp>
#include <nano/node/node_rpc_config.hpp>
#include <nano/node/nodeconfig.hpp>
#include <nano/node/openclconfig.hpp>

#include <vector>

namespace nano
{
class jsonconfig;
class tomlconfig;
class daemon_config
{
public:
	daemon_config () = default;
	nano::error deserialize_json (bool &, nano::jsonconfig &);
	nano::error serialize_json (nano::jsonconfig &);
	nano::error deserialize_toml (nano::tomlconfig &);
	nano::error serialize_toml (nano::tomlconfig &);
	bool rpc_enable{ false };
	nano::node_rpc_config rpc;
	nano::node_config node;
	bool opencl_enable{ false };
	nano::opencl_config opencl;
	nano::node_pow_server_config pow_server;
	unsigned json_version () const
	{
		return 2;
	}
};

nano::error read_node_config_toml (boost::filesystem::path const &, nano::daemon_config & config_a, std::vector<std::string> const & config_overrides = std::vector<std::string> ());
nano::error read_and_update_daemon_config (boost::filesystem::path const &, nano::daemon_config & config_a, nano::jsonconfig & json_a);
}
