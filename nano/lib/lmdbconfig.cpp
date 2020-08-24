#include <nano/lib/lmdbconfig.hpp>
#include <nano/lib/tomlconfig.hpp>
#include <nano/secure/common.hpp>

#include <iostream>

nano::error nano::lmdb_config::serialize_toml (nano::tomlconfig & toml) const
{
	std::string sync_string;
	switch (sync)
	{
		case nano::lmdb_config::sync_strategy::always:
			sync_string = "always";
			break;
		case nano::lmdb_config::sync_strategy::nosync_safe:
			sync_string = "nosync_safe";
			break;
		case nano::lmdb_config::sync_strategy::nosync_unsafe:
			sync_string = "nosync_unsafe";
			break;
		case nano::lmdb_config::sync_strategy::nosync_unsafe_large_memory:
			sync_string = "nosync_unsafe_large_memory";
			break;
	}

	toml.put ("sync", sync_string, "Sync strategy for flushing commits to the ledger database. This does not affect the wallet database.\ntype:string,{always, nosync_safe, nosync_unsafe, nosync_unsafe_large_memory}");
	toml.put ("max_databases", max_databases, "Maximum open lmdb databases. Increase default if more than 100 wallets is required.\nNote: external management is recommended when a large amounts of wallets are required (see https://docs.nano.org/integration-guides/key-management/).\ntype:uin32");
	toml.put ("map_size", map_size, "Maximum ledger database map size in bytes.\ntype:uint64");
	return toml.get_error ();
}

nano::error nano::lmdb_config::deserialize_toml (nano::tomlconfig & toml, bool is_deprecated_lmdb_dbs_used)
{
	static nano::environment_constants constants;
	auto default_max_databases = max_databases;
	toml.get_optional<uint32_t> ("max_databases", max_databases);
	toml.get_optional<size_t> ("map_size", map_size);

	// For now we accept either setting, but not both
	if (!constants.network.is_dev_network () && is_deprecated_lmdb_dbs_used && default_max_databases != max_databases)
	{
		toml.get_error ().set ("Both the deprecated node.lmdb_max_dbs and the new node.lmdb.max_databases setting are used. Please use max_databases only.");
	}

	if (!toml.get_error ())
	{
		std::string sync_string = "always";
		toml.get_optional<std::string> ("sync", sync_string);
		if (sync_string == "always")
		{
			sync = nano::lmdb_config::sync_strategy::always;
		}
		else if (sync_string == "nosync_safe")
		{
			sync = nano::lmdb_config::sync_strategy::nosync_safe;
		}
		else if (sync_string == "nosync_unsafe")
		{
			sync = nano::lmdb_config::sync_strategy::nosync_unsafe;
		}
		else if (sync_string == "nosync_unsafe_large_memory")
		{
			sync = nano::lmdb_config::sync_strategy::nosync_unsafe_large_memory;
		}
		else
		{
			toml.get_error ().set (sync_string + " is not a valid sync option");
		}
	}

	return toml.get_error ();
}
