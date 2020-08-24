#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/config.hpp>
#include <nano/lib/jsonconfig.hpp>
#include <nano/lib/rpcconfig.hpp>
#include <nano/lib/tomlconfig.hpp>
#include <nano/node/nodeconfig.hpp>
#include <nano/node/testing.hpp>
#include <nano/node/transport/transport.hpp>

#include <crypto/cryptopp/words.h>

#include <boost/format.hpp>

namespace
{
const char * preconfigured_peers_key = "preconfigured_peers";
const char * signature_checker_threads_key = "signature_checker_threads";
const char * pow_sleep_interval_key = "pow_sleep_interval";
const char * default_beta_peer_network = "peering-beta.nano.org";
const char * default_live_peer_network = "peering.nano.org";
const char * default_test_peer_network = "peering-test.nano.org";
}

nano::node_config::node_config () :
external_address (boost::asio::ip::address_v6{}.to_string ())
{
	switch (constants.network.network ())
	{
		case nano::nano_networks::nano_dev_network:
			enable_voting = true;
			preconfigured_representatives.push_back (constants.ledger.genesis_account);
			break;
		case nano::nano_networks::nano_beta_network:
		{
			preconfigured_peers.push_back (default_beta_peer_network);
			nano::account offline_representative;
			release_assert (!offline_representative.decode_account ("nano_1defau1t9off1ine9rep99999999999999999999999999999999wgmuzxxy"));
			preconfigured_representatives.emplace_back (offline_representative);
			break;
		}
		case nano::nano_networks::nano_live_network:
			preconfigured_peers.push_back (default_live_peer_network);
			preconfigured_representatives.emplace_back ("A30E0A32ED41C8607AA9212843392E853FCBCB4E7CB194E35C94F07F91DE59EF");
			preconfigured_representatives.emplace_back ("67556D31DDFC2A440BF6147501449B4CB9572278D034EE686A6BEE29851681DF");
			preconfigured_representatives.emplace_back ("5C2FBB148E006A8E8BA7A75DD86C9FE00C83F5FFDBFD76EAA09531071436B6AF");
			preconfigured_representatives.emplace_back ("AE7AC63990DAAAF2A69BF11C913B928844BF5012355456F2F164166464024B29");
			preconfigured_representatives.emplace_back ("BD6267D6ECD8038327D2BCC0850BDF8F56EC0414912207E81BCF90DFAC8A4AAA");
			preconfigured_representatives.emplace_back ("2399A083C600AA0572F5E36247D978FCFC840405F8D4B6D33161C0066A55F431");
			preconfigured_representatives.emplace_back ("2298FAB7C61058E77EA554CB93EDEEDA0692CBFCC540AB213B2836B29029E23A");
			preconfigured_representatives.emplace_back ("3FE80B4BC842E82C1C18ABFEEC47EA989E63953BC82AC411F304D13833D52A56");
			break;
		case nano::nano_networks::nano_test_network:
			preconfigured_peers.push_back (default_test_peer_network);
			preconfigured_representatives.push_back (constants.ledger.genesis_account);
			break;
		default:
			debug_assert (false);
			break;
	}
}

nano::error nano::node_config::serialize_toml (nano::tomlconfig & toml) const
{
	toml.put ("peering_port", peering_port, "Node peering port.\ntype:uint16");
	toml.put ("bootstrap_fraction_numerator", bootstrap_fraction_numerator, "Change bootstrap threshold (online stake / 256 * bootstrap_fraction_numerator).\ntype:uint32");
	toml.put ("receive_minimum", receive_minimum.to_string_dec (), "Minimum receive amount. Only affects node wallets. A large amount is recommended to avoid automatic work generation for tiny transactions.\ntype:string,amount,raw");
	toml.put ("online_weight_minimum", online_weight_minimum.to_string_dec (), "Online weight minimum required to confirm a block.\ntype:string,amount,raw");
	toml.put ("online_weight_quorum", online_weight_quorum, "Percentage of votes required to confirm blocks. A value below 50 is not recommended.\ntype:uint64");
	toml.put ("password_fanout", password_fanout, "Password fanout factor.\ntype:uint64");
	toml.put ("io_threads", io_threads, "Number of threads dedicated to I/O operations. Defaults to the number of CPU threads, and at least 4.\ntype:uint64");
	toml.put ("network_threads", network_threads, "Number of threads dedicated to processing network messages. Defaults to the number of CPU threads, and at least 4.\ntype:uint64");
	toml.put ("work_threads", work_threads, "Number of threads dedicated to CPU generated work. Defaults to all available CPU threads.\ntype:uint64");
	toml.put ("signature_checker_threads", signature_checker_threads, "Number of additional threads dedicated to signature verification. Defaults to number of CPU threads / 2.\ntype:uint64");
	toml.put ("enable_voting", enable_voting, "Enable or disable voting. Enabling this option requires additional system resources, namely increased CPU, bandwidth and disk usage.\ntype:bool");
	toml.put ("bootstrap_connections", bootstrap_connections, "Number of outbound bootstrap connections. Must be a power of 2. Defaults to 4.\nWarning: a larger amount of connections may use substantially more system memory.\ntype:uint64");
	toml.put ("bootstrap_connections_max", bootstrap_connections_max, "Maximum number of inbound bootstrap connections. Defaults to 64.\nWarning: a larger amount of connections may use additional system memory.\ntype:uint64");
	toml.put ("bootstrap_initiator_threads", bootstrap_initiator_threads, "Number of threads dedicated to concurrent bootstrap attempts. Defaults to 1.\nWarning: a larger amount of attempts may use additional system memory and disk IO.\ntype:uint64");
	toml.put ("lmdb_max_dbs", deprecated_lmdb_max_dbs, "DEPRECATED: use node.lmdb.max_databases instead.\nMaximum open lmdb databases. Increase default if more than 100 wallets is required.\nNote: external management is recommended when a large number of wallets is required (see https://docs.nano.org/integration-guides/key-management/).\ntype:uint64");
	toml.put ("block_processor_batch_max_time", block_processor_batch_max_time.count (), "The maximum time the block processor can continuously process blocks for.\ntype:milliseconds");
	toml.put ("allow_local_peers", allow_local_peers, "Enable or disable local host peering.\ntype:bool");
	toml.put ("vote_minimum", vote_minimum.to_string_dec (), "Local representatives do not vote if the delegated weight is under this threshold. Saves on system resources.\ntype:string,amount,raw");
	toml.put ("vote_generator_delay", vote_generator_delay.count (), "Delay before votes are sent to allow for efficient bundling of hashes in votes.\ntype:milliseconds");
	toml.put ("vote_generator_threshold", vote_generator_threshold, "Number of bundled hashes required for an additional generator delay.\ntype:uint64,[1..11]");
	toml.put ("unchecked_cutoff_time", unchecked_cutoff_time.count (), "Number of seconds before deleting an unchecked entry.\nWarning: lower values (e.g., 3600 seconds, or 1 hour) may result in unsuccessful bootstraps, especially a bootstrap from scratch.\ntype:seconds");
	toml.put ("tcp_io_timeout", tcp_io_timeout.count (), "Timeout for TCP connect-, read- and write operations.\nWarning: a low value (e.g., below 5 seconds) may result in TCP connections failing.\ntype:seconds");
	toml.put ("pow_sleep_interval", pow_sleep_interval.count (), "Time to sleep between batch work generation attempts. Reduces max CPU usage at the expense of a longer generation time.\ntype:nanoseconds");
	toml.put ("external_address", external_address, "The external address of this node (NAT). If not set, the node will request this information via UPnP.\ntype:string,ip");
	toml.put ("external_port", external_port, "The external port number of this node (NAT). Only used if external_address is set.\ntype:uint16");
	toml.put ("tcp_incoming_connections_max", tcp_incoming_connections_max, "Maximum number of incoming TCP connections.\ntype:uint64");
	toml.put ("use_memory_pools", use_memory_pools, "If true, allocate memory from memory pools. Enabling this may improve performance. Memory is never released to the OS.\ntype:bool");
	toml.put ("confirmation_history_size", confirmation_history_size, "Maximum confirmation history size. If tracking the rate of block confirmations, the websocket feature is recommended instead.\ntype:uint64");
	toml.put ("active_elections_size", active_elections_size, "Number of active elections. Elections beyond this limit have limited survival time.\nWarning: modifying this value may result in a lower confirmation rate.\ntype:uint64,[250..]");
	toml.put ("bandwidth_limit", bandwidth_limit, "Outbound traffic limit in bytes/sec after which messages will be dropped.\nNote: changing to unlimited bandwidth (0) is not recommended for limited connections.\ntype:uint64");
	toml.put ("bandwidth_limit_burst_ratio", bandwidth_limit_burst_ratio, "Burst ratio for outbound traffic shaping.\ntype:double");
	toml.put ("conf_height_processor_batch_min_time", conf_height_processor_batch_min_time.count (), "Minimum write batching time when there are blocks pending confirmation height.\ntype:milliseconds");
	toml.put ("backup_before_upgrade", backup_before_upgrade, "Backup the ledger database before performing upgrades.\nWarning: uses more disk storage and increases startup time when upgrading.\ntype:bool");
	toml.put ("work_watcher_period", work_watcher_period.count (), "Time between checks for confirmation and re-generating higher difficulty work if unconfirmed, for blocks in the work watcher.\ntype:seconds");
	toml.put ("max_work_generate_multiplier", max_work_generate_multiplier, "Maximum allowed difficulty multiplier for work generation.\ntype:double,[1..]");
	toml.put ("frontiers_confirmation", serialize_frontiers_confirmation (frontiers_confirmation), "Mode controlling frontier confirmation rate.\ntype:string,{auto,always,disabled}");
	toml.put ("max_queued_requests", max_queued_requests, "Limit for number of queued confirmation requests for one channel, after which new requests are dropped until the queue drops below this value.\ntype:uint32");

	auto work_peers_l (toml.create_array ("work_peers", "A list of \"address:port\" entries to identify work peers."));
	for (auto i (work_peers.begin ()), n (work_peers.end ()); i != n; ++i)
	{
		work_peers_l->push_back (boost::str (boost::format ("%1%:%2%") % i->first % i->second));
	}

	auto preconfigured_peers_l (toml.create_array ("preconfigured_peers", "A list of \"address\" (hostname or ip address) entries to identify preconfigured peers."));
	for (auto i (preconfigured_peers.begin ()), n (preconfigured_peers.end ()); i != n; ++i)
	{
		preconfigured_peers_l->push_back (*i);
	}

	auto preconfigured_representatives_l (toml.create_array ("preconfigured_representatives", "A list of representative account addresses used when creating new accounts in internal wallets."));
	for (auto i (preconfigured_representatives.begin ()), n (preconfigured_representatives.end ()); i != n; ++i)
	{
		preconfigured_representatives_l->push_back (i->to_account ());
	}

	/** Experimental node entries */
	nano::tomlconfig experimental_l;
	auto secondary_work_peers_l (experimental_l.create_array ("secondary_work_peers", "A list of \"address:port\" entries to identify work peers for secondary work generation."));
	for (auto i (secondary_work_peers.begin ()), n (secondary_work_peers.end ()); i != n; ++i)
	{
		secondary_work_peers_l->push_back (boost::str (boost::format ("%1%:%2%") % i->first % i->second));
	}
	toml.put_child ("experimental", experimental_l);

	nano::tomlconfig callback_l;
	callback_l.put ("address", callback_address, "Callback address.\ntype:string,ip");
	callback_l.put ("port", callback_port, "Callback port number.\ntype:uint16");
	callback_l.put ("target", callback_target, "Callback target path.\ntype:string,uri");
	toml.put_child ("httpcallback", callback_l);

	nano::tomlconfig logging_l;
	logging.serialize_toml (logging_l);
	toml.put_child ("logging", logging_l);

	nano::tomlconfig websocket_l;
	websocket_config.serialize_toml (websocket_l);
	toml.put_child ("websocket", websocket_l);

	nano::tomlconfig ipc_l;
	ipc_config.serialize_toml (ipc_l);
	toml.put_child ("ipc", ipc_l);

	nano::tomlconfig diagnostics_l;
	diagnostics_config.serialize_toml (diagnostics_l);
	toml.put_child ("diagnostics", diagnostics_l);

	nano::tomlconfig stat_l;
	stat_config.serialize_toml (stat_l);
	toml.put_child ("statistics", stat_l);

	nano::tomlconfig rocksdb_l;
	rocksdb_config.serialize_toml (rocksdb_l);
	toml.put_child ("rocksdb", rocksdb_l);

	nano::tomlconfig lmdb_l;
	lmdb_config.serialize_toml (lmdb_l);
	toml.put_child ("lmdb", lmdb_l);

	return toml.get_error ();
}

nano::error nano::node_config::deserialize_toml (nano::tomlconfig & toml)
{
	try
	{
		if (toml.has_key ("httpcallback"))
		{
			auto callback_l (toml.get_required_child ("httpcallback"));
			callback_l.get<std::string> ("address", callback_address);
			callback_l.get<uint16_t> ("port", callback_port);
			callback_l.get<std::string> ("target", callback_target);
		}

		if (toml.has_key ("logging"))
		{
			auto logging_l (toml.get_required_child ("logging"));
			logging.deserialize_toml (logging_l);
		}

		if (toml.has_key ("websocket"))
		{
			auto websocket_config_l (toml.get_required_child ("websocket"));
			websocket_config.deserialize_toml (websocket_config_l);
		}

		if (toml.has_key ("ipc"))
		{
			auto ipc_config_l (toml.get_required_child ("ipc"));
			ipc_config.deserialize_toml (ipc_config_l);
		}

		if (toml.has_key ("diagnostics"))
		{
			auto diagnostics_config_l (toml.get_required_child ("diagnostics"));
			diagnostics_config.deserialize_toml (diagnostics_config_l);
		}

		if (toml.has_key ("statistics"))
		{
			auto stat_config_l (toml.get_required_child ("statistics"));
			stat_config.deserialize_toml (stat_config_l);
		}

		if (toml.has_key ("rocksdb"))
		{
			auto rocksdb_config_l (toml.get_required_child ("rocksdb"));
			rocksdb_config.deserialize_toml (rocksdb_config_l);
		}

		if (toml.has_key ("work_peers"))
		{
			work_peers.clear ();
			toml.array_entries_required<std::string> ("work_peers", [this](std::string const & entry_a) {
				this->deserialize_address (entry_a, this->work_peers);
			});
		}

		if (toml.has_key (preconfigured_peers_key))
		{
			preconfigured_peers.clear ();
			toml.array_entries_required<std::string> (preconfigured_peers_key, [this](std::string entry) {
				preconfigured_peers.push_back (entry);
			});
		}

		if (toml.has_key ("preconfigured_representatives"))
		{
			preconfigured_representatives.clear ();
			toml.array_entries_required<std::string> ("preconfigured_representatives", [this, &toml](std::string entry) {
				nano::account representative (0);
				if (representative.decode_account (entry))
				{
					toml.get_error ().set ("Invalid representative account: " + entry);
				}
				preconfigured_representatives.push_back (representative);
			});
		}

		if (preconfigured_representatives.empty ())
		{
			toml.get_error ().set ("At least one representative account must be set");
		}

		auto receive_minimum_l (receive_minimum.to_string_dec ());
		if (toml.has_key ("receive_minimum"))
		{
			receive_minimum_l = toml.get<std::string> ("receive_minimum");
		}
		if (receive_minimum.decode_dec (receive_minimum_l))
		{
			toml.get_error ().set ("receive_minimum contains an invalid decimal amount");
		}

		auto online_weight_minimum_l (online_weight_minimum.to_string_dec ());
		if (toml.has_key ("online_weight_minimum"))
		{
			online_weight_minimum_l = toml.get<std::string> ("online_weight_minimum");
		}
		if (online_weight_minimum.decode_dec (online_weight_minimum_l))
		{
			toml.get_error ().set ("online_weight_minimum contains an invalid decimal amount");
		}

		auto vote_minimum_l (vote_minimum.to_string_dec ());
		if (toml.has_key ("vote_minimum"))
		{
			vote_minimum_l = toml.get<std::string> ("vote_minimum");
		}
		if (vote_minimum.decode_dec (vote_minimum_l))
		{
			toml.get_error ().set ("vote_minimum contains an invalid decimal amount");
		}

		auto delay_l = vote_generator_delay.count ();
		toml.get ("vote_generator_delay", delay_l);
		vote_generator_delay = std::chrono::milliseconds (delay_l);

		toml.get<unsigned> ("vote_generator_threshold", vote_generator_threshold);

		auto block_processor_batch_max_time_l = block_processor_batch_max_time.count ();
		toml.get ("block_processor_batch_max_time", block_processor_batch_max_time_l);
		block_processor_batch_max_time = std::chrono::milliseconds (block_processor_batch_max_time_l);

		auto unchecked_cutoff_time_l = static_cast<unsigned long> (unchecked_cutoff_time.count ());
		toml.get ("unchecked_cutoff_time", unchecked_cutoff_time_l);
		unchecked_cutoff_time = std::chrono::seconds (unchecked_cutoff_time_l);

		auto tcp_io_timeout_l = static_cast<unsigned long> (tcp_io_timeout.count ());
		toml.get ("tcp_io_timeout", tcp_io_timeout_l);
		tcp_io_timeout = std::chrono::seconds (tcp_io_timeout_l);

		toml.get<uint16_t> ("peering_port", peering_port);
		toml.get<unsigned> ("bootstrap_fraction_numerator", bootstrap_fraction_numerator);
		toml.get<unsigned> ("online_weight_quorum", online_weight_quorum);
		toml.get<unsigned> ("password_fanout", password_fanout);
		toml.get<unsigned> ("io_threads", io_threads);
		toml.get<unsigned> ("work_threads", work_threads);
		toml.get<unsigned> ("network_threads", network_threads);
		toml.get<unsigned> ("bootstrap_connections", bootstrap_connections);
		toml.get<unsigned> ("bootstrap_connections_max", bootstrap_connections_max);
		toml.get<unsigned> ("bootstrap_initiator_threads", bootstrap_initiator_threads);
		toml.get<bool> ("enable_voting", enable_voting);
		toml.get<bool> ("allow_local_peers", allow_local_peers);
		toml.get<unsigned> (signature_checker_threads_key, signature_checker_threads);

		auto lmdb_max_dbs_default = deprecated_lmdb_max_dbs;
		toml.get<int> ("lmdb_max_dbs", deprecated_lmdb_max_dbs);
		bool is_deprecated_lmdb_dbs_used = lmdb_max_dbs_default != deprecated_lmdb_max_dbs;

		// Note: using the deprecated setting will result in a fail-fast config error in the future
		if (!constants.network.is_dev_network () && is_deprecated_lmdb_dbs_used)
		{
			std::cerr << "WARNING: The node.lmdb_max_dbs setting is deprecated and will be removed in a future version." << std::endl;
			std::cerr << "Please use the node.lmdb.max_databases setting instead." << std::endl;
		}

		if (toml.has_key ("lmdb"))
		{
			auto lmdb_config_l (toml.get_required_child ("lmdb"));
			lmdb_config.deserialize_toml (lmdb_config_l, is_deprecated_lmdb_dbs_used);

			// Note that the lmdb config fails is both the deprecated and new setting are changed.
			if (is_deprecated_lmdb_dbs_used)
			{
				lmdb_config.max_databases = deprecated_lmdb_max_dbs;
			}
		}

		boost::asio::ip::address_v6 external_address_l;
		toml.get<boost::asio::ip::address_v6> ("external_address", external_address_l);
		external_address = external_address_l.to_string ();
		toml.get<uint16_t> ("external_port", external_port);
		toml.get<unsigned> ("tcp_incoming_connections_max", tcp_incoming_connections_max);

		auto pow_sleep_interval_l (pow_sleep_interval.count ());
		toml.get (pow_sleep_interval_key, pow_sleep_interval_l);
		pow_sleep_interval = std::chrono::nanoseconds (pow_sleep_interval_l);
		toml.get<bool> ("use_memory_pools", use_memory_pools);
		toml.get<size_t> ("confirmation_history_size", confirmation_history_size);
		toml.get<size_t> ("active_elections_size", active_elections_size);
		toml.get<size_t> ("bandwidth_limit", bandwidth_limit);
		toml.get<double> ("bandwidth_limit_burst_ratio", bandwidth_limit_burst_ratio);
		toml.get<bool> ("backup_before_upgrade", backup_before_upgrade);

		auto work_watcher_period_l = work_watcher_period.count ();
		toml.get ("work_watcher_period", work_watcher_period_l);
		work_watcher_period = std::chrono::seconds (work_watcher_period_l);

		auto conf_height_processor_batch_min_time_l (conf_height_processor_batch_min_time.count ());
		toml.get ("conf_height_processor_batch_min_time", conf_height_processor_batch_min_time_l);
		conf_height_processor_batch_min_time = std::chrono::milliseconds (conf_height_processor_batch_min_time_l);

		nano::network_constants network;
		toml.get<double> ("max_work_generate_multiplier", max_work_generate_multiplier);

		toml.get<uint32_t> ("max_queued_requests", max_queued_requests);

		if (toml.has_key ("frontiers_confirmation"))
		{
			auto frontiers_confirmation_l (toml.get<std::string> ("frontiers_confirmation"));
			frontiers_confirmation = deserialize_frontiers_confirmation (frontiers_confirmation_l);
		}

		if (toml.has_key ("experimental"))
		{
			auto experimental_config_l (toml.get_required_child ("experimental"));
			if (experimental_config_l.has_key ("secondary_work_peers"))
			{
				secondary_work_peers.clear ();
				experimental_config_l.array_entries_required<std::string> ("secondary_work_peers", [this](std::string const & entry_a) {
					this->deserialize_address (entry_a, this->secondary_work_peers);
				});
			}
		}

		// Validate ranges
		nano::environment_constants constants;
		if (online_weight_quorum > 100)
		{
			toml.get_error ().set ("online_weight_quorum must be less than 100");
		}
		if (password_fanout < 16 || password_fanout > 1024 * 1024)
		{
			toml.get_error ().set ("password_fanout must be a number between 16 and 1048576");
		}
		if (io_threads == 0)
		{
			toml.get_error ().set ("io_threads must be non-zero");
		}
		if (active_elections_size <= 250 && !network.is_dev_network ())
		{
			toml.get_error ().set ("active_elections_size must be greater than 250");
		}
		if (bandwidth_limit > std::numeric_limits<size_t>::max ())
		{
			toml.get_error ().set ("bandwidth_limit unbounded = 0, default = 10485760, max = 18446744073709551615");
		}
		if (vote_generator_threshold < 1 || vote_generator_threshold > 11)
		{
			toml.get_error ().set ("vote_generator_threshold must be a number between 1 and 11");
		}
		if (work_watcher_period < std::chrono::seconds (1))
		{
			toml.get_error ().set ("work_watcher_period must be equal or larger than 1");
		}
		if (max_work_generate_multiplier < 1)
		{
			toml.get_error ().set ("max_work_generate_multiplier must be greater than or equal to 1");
		}
		if (frontiers_confirmation == nano::frontiers_confirmation_mode::invalid)
		{
			toml.get_error ().set ("frontiers_confirmation value is invalid (available: always, auto, disabled)");
		}
		if (block_processor_batch_max_time < constants.node.process_confirmed_interval)
		{
			toml.get_error ().set ((boost::format ("block_processor_batch_max_time value must be equal or larger than %1%ms") % constants.node.process_confirmed_interval.count ()).str ());
		}
	}
	catch (std::runtime_error const & ex)
	{
		toml.get_error ().set (ex.what ());
	}

	return toml.get_error ();
}

nano::error nano::node_config::serialize_json (nano::jsonconfig & json) const
{
	json.put ("version", json_version ());
	json.put ("peering_port", peering_port);
	json.put ("bootstrap_fraction_numerator", bootstrap_fraction_numerator);
	json.put ("receive_minimum", receive_minimum.to_string_dec ());

	nano::jsonconfig logging_l;
	logging.serialize_json (logging_l);
	json.put_child ("logging", logging_l);

	nano::jsonconfig work_peers_l;
	for (auto i (work_peers.begin ()), n (work_peers.end ()); i != n; ++i)
	{
		work_peers_l.push (boost::str (boost::format ("%1%:%2%") % i->first % i->second));
	}
	json.put_child ("work_peers", work_peers_l);
	nano::jsonconfig preconfigured_peers_l;
	for (auto i (preconfigured_peers.begin ()), n (preconfigured_peers.end ()); i != n; ++i)
	{
		preconfigured_peers_l.push (*i);
	}
	json.put_child (preconfigured_peers_key, preconfigured_peers_l);

	nano::jsonconfig preconfigured_representatives_l;
	for (auto i (preconfigured_representatives.begin ()), n (preconfigured_representatives.end ()); i != n; ++i)
	{
		preconfigured_representatives_l.push (i->to_account ());
	}
	json.put_child ("preconfigured_representatives", preconfigured_representatives_l);

	json.put ("online_weight_minimum", online_weight_minimum.to_string_dec ());
	json.put ("online_weight_quorum", online_weight_quorum);
	json.put ("password_fanout", password_fanout);
	json.put ("io_threads", io_threads);
	json.put ("network_threads", network_threads);
	json.put ("work_threads", work_threads);
	json.put (signature_checker_threads_key, signature_checker_threads);
	json.put ("enable_voting", enable_voting);
	json.put ("bootstrap_connections", bootstrap_connections);
	json.put ("bootstrap_connections_max", bootstrap_connections_max);
	json.put ("callback_address", callback_address);
	json.put ("callback_port", callback_port);
	json.put ("callback_target", callback_target);
	json.put ("lmdb_max_dbs", deprecated_lmdb_max_dbs);
	json.put ("block_processor_batch_max_time", block_processor_batch_max_time.count ());
	json.put ("allow_local_peers", allow_local_peers);
	json.put ("vote_minimum", vote_minimum.to_string_dec ());
	json.put ("vote_generator_delay", vote_generator_delay.count ());
	json.put ("vote_generator_threshold", vote_generator_threshold);
	json.put ("unchecked_cutoff_time", unchecked_cutoff_time.count ());
	json.put ("tcp_io_timeout", tcp_io_timeout.count ());
	json.put ("pow_sleep_interval", pow_sleep_interval.count ());
	json.put ("external_address", external_address);
	json.put ("external_port", external_port);
	json.put ("tcp_incoming_connections_max", tcp_incoming_connections_max);
	json.put ("use_memory_pools", use_memory_pools);
	nano::jsonconfig websocket_l;
	websocket_config.serialize_json (websocket_l);
	json.put_child ("websocket", websocket_l);
	nano::jsonconfig ipc_l;
	ipc_config.serialize_json (ipc_l);
	json.put_child ("ipc", ipc_l);
	nano::jsonconfig diagnostics_l;
	diagnostics_config.serialize_json (diagnostics_l);
	json.put_child ("diagnostics", diagnostics_l);
	json.put ("confirmation_history_size", confirmation_history_size);
	json.put ("active_elections_size", active_elections_size);
	json.put ("bandwidth_limit", bandwidth_limit);
	json.put ("backup_before_upgrade", backup_before_upgrade);
	json.put ("work_watcher_period", work_watcher_period.count ());

	return json.get_error ();
}

bool nano::node_config::upgrade_json (unsigned version_a, nano::jsonconfig & json)
{
	json.put ("version", json_version ());
	switch (version_a)
	{
		case 1:
		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		case 7:
		case 8:
		case 9:
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 15:
		case 16:
			throw std::runtime_error ("node_config version unsupported for upgrade. Upgrade to a v19, v20 or v21 node first, or delete the config and ledger files");
		case 17:
		{
			json.put ("active_elections_size", 10000); // Update value
			json.put ("vote_generator_delay", 100); // Update value
			json.put ("backup_before_upgrade", backup_before_upgrade);
			json.put ("work_watcher_period", work_watcher_period.count ());
		}
		case 18:
			break;
		default:
			throw std::runtime_error ("Unknown node_config version");
	}
	return version_a < json_version ();
}

nano::error nano::node_config::deserialize_json (bool & upgraded_a, nano::jsonconfig & json)
{
	try
	{
		auto version_l (json.get<unsigned> ("version"));
		upgraded_a |= upgrade_json (version_l, json);

		auto logging_l (json.get_required_child ("logging"));
		logging.deserialize_json (upgraded_a, logging_l);

		work_peers.clear ();
		auto work_peers_l (json.get_required_child ("work_peers"));
		work_peers_l.array_entries<std::string> ([this](std::string entry) {
			auto port_position (entry.rfind (':'));
			bool result = port_position == -1;
			if (!result)
			{
				auto port_str (entry.substr (port_position + 1));
				uint16_t port;
				result |= parse_port (port_str, port);
				if (!result)
				{
					auto address (entry.substr (0, port_position));
					this->work_peers.emplace_back (address, port);
				}
			}
		});

		auto preconfigured_peers_l (json.get_required_child (preconfigured_peers_key));
		preconfigured_peers.clear ();
		preconfigured_peers_l.array_entries<std::string> ([this](std::string entry) {
			preconfigured_peers.push_back (entry);
		});

		auto preconfigured_representatives_l (json.get_required_child ("preconfigured_representatives"));
		preconfigured_representatives.clear ();
		preconfigured_representatives_l.array_entries<std::string> ([this, &json](std::string entry) {
			nano::account representative (0);
			if (representative.decode_account (entry))
			{
				json.get_error ().set ("Invalid representative account: " + entry);
			}
			preconfigured_representatives.push_back (representative);
		});

		if (preconfigured_representatives.empty ())
		{
			json.get_error ().set ("At least one representative account must be set");
		}
		auto stat_config_l (json.get_optional_child ("statistics"));
		if (stat_config_l)
		{
			stat_config.deserialize_json (stat_config_l.get ());
		}

		auto receive_minimum_l (json.get<std::string> ("receive_minimum"));
		if (receive_minimum.decode_dec (receive_minimum_l))
		{
			json.get_error ().set ("receive_minimum contains an invalid decimal amount");
		}

		auto online_weight_minimum_l (json.get<std::string> ("online_weight_minimum"));
		if (online_weight_minimum.decode_dec (online_weight_minimum_l))
		{
			json.get_error ().set ("online_weight_minimum contains an invalid decimal amount");
		}

		auto vote_minimum_l (json.get<std::string> ("vote_minimum"));
		if (vote_minimum.decode_dec (vote_minimum_l))
		{
			json.get_error ().set ("vote_minimum contains an invalid decimal amount");
		}

		auto delay_l = vote_generator_delay.count ();
		json.get ("vote_generator_delay", delay_l);
		vote_generator_delay = std::chrono::milliseconds (delay_l);

		json.get<unsigned> ("vote_generator_threshold", vote_generator_threshold);

		auto block_processor_batch_max_time_l (json.get<unsigned long> ("block_processor_batch_max_time"));
		block_processor_batch_max_time = std::chrono::milliseconds (block_processor_batch_max_time_l);
		auto unchecked_cutoff_time_l = static_cast<unsigned long> (unchecked_cutoff_time.count ());
		json.get ("unchecked_cutoff_time", unchecked_cutoff_time_l);
		unchecked_cutoff_time = std::chrono::seconds (unchecked_cutoff_time_l);

		auto tcp_io_timeout_l = static_cast<unsigned long> (tcp_io_timeout.count ());
		json.get ("tcp_io_timeout", tcp_io_timeout_l);
		tcp_io_timeout = std::chrono::seconds (tcp_io_timeout_l);

		auto ipc_config_l (json.get_optional_child ("ipc"));
		if (ipc_config_l)
		{
			ipc_config.deserialize_json (upgraded_a, ipc_config_l.get ());
		}
		auto websocket_config_l (json.get_optional_child ("websocket"));
		if (websocket_config_l)
		{
			websocket_config.deserialize_json (websocket_config_l.get ());
		}
		auto diagnostics_config_l (json.get_optional_child ("diagnostics"));
		if (diagnostics_config_l)
		{
			diagnostics_config.deserialize_json (diagnostics_config_l.get ());
		}
		json.get<uint16_t> ("peering_port", peering_port);
		json.get<unsigned> ("bootstrap_fraction_numerator", bootstrap_fraction_numerator);
		json.get<unsigned> ("online_weight_quorum", online_weight_quorum);
		json.get<unsigned> ("password_fanout", password_fanout);
		json.get<unsigned> ("io_threads", io_threads);
		json.get<unsigned> ("work_threads", work_threads);
		json.get<unsigned> ("network_threads", network_threads);
		json.get<unsigned> ("bootstrap_connections", bootstrap_connections);
		json.get<unsigned> ("bootstrap_connections_max", bootstrap_connections_max);
		json.get<std::string> ("callback_address", callback_address);
		json.get<uint16_t> ("callback_port", callback_port);
		json.get<std::string> ("callback_target", callback_target);
		json.get<int> ("lmdb_max_dbs", deprecated_lmdb_max_dbs);
		json.get<bool> ("enable_voting", enable_voting);
		json.get<bool> ("allow_local_peers", allow_local_peers);
		json.get<unsigned> (signature_checker_threads_key, signature_checker_threads);
		boost::asio::ip::address_v6 external_address_l;
		json.get<boost::asio::ip::address_v6> ("external_address", external_address_l);
		external_address = external_address_l.to_string ();
		json.get<uint16_t> ("external_port", external_port);
		json.get<unsigned> ("tcp_incoming_connections_max", tcp_incoming_connections_max);

		auto pow_sleep_interval_l (pow_sleep_interval.count ());
		json.get (pow_sleep_interval_key, pow_sleep_interval_l);
		pow_sleep_interval = std::chrono::nanoseconds (pow_sleep_interval_l);
		json.get<bool> ("use_memory_pools", use_memory_pools);
		json.get<size_t> ("confirmation_history_size", confirmation_history_size);
		json.get<size_t> ("active_elections_size", active_elections_size);
		json.get<size_t> ("bandwidth_limit", bandwidth_limit);
		json.get<bool> ("backup_before_upgrade", backup_before_upgrade);

		auto work_watcher_period_l = work_watcher_period.count ();
		json.get ("work_watcher_period", work_watcher_period_l);
		work_watcher_period = std::chrono::seconds (work_watcher_period_l);

		auto conf_height_processor_batch_min_time_l (conf_height_processor_batch_min_time.count ());
		json.get ("conf_height_processor_batch_min_time", conf_height_processor_batch_min_time_l);
		conf_height_processor_batch_min_time = std::chrono::milliseconds (conf_height_processor_batch_min_time_l);

		nano::network_constants network;
		// Validate ranges
		if (online_weight_quorum > 100)
		{
			json.get_error ().set ("online_weight_quorum must be less than 100");
		}
		if (password_fanout < 16 || password_fanout > 1024 * 1024)
		{
			json.get_error ().set ("password_fanout must be a number between 16 and 1048576");
		}
		if (io_threads == 0)
		{
			json.get_error ().set ("io_threads must be non-zero");
		}
		if (active_elections_size <= 250 && !network.is_dev_network ())
		{
			json.get_error ().set ("active_elections_size must be greater than 250");
		}
		if (bandwidth_limit > std::numeric_limits<size_t>::max ())
		{
			json.get_error ().set ("bandwidth_limit unbounded = 0, default = 10485760, max = 18446744073709551615");
		}
		if (vote_generator_threshold < 1 || vote_generator_threshold > 11)
		{
			json.get_error ().set ("vote_generator_threshold must be a number between 1 and 11");
		}
		if (work_watcher_period < std::chrono::seconds (1))
		{
			json.get_error ().set ("work_watcher_period must be equal or larger than 1");
		}
	}
	catch (std::runtime_error const & ex)
	{
		json.get_error ().set (ex.what ());
	}
	return json.get_error ();
}

std::string nano::node_config::serialize_frontiers_confirmation (nano::frontiers_confirmation_mode mode_a) const
{
	switch (mode_a)
	{
		case nano::frontiers_confirmation_mode::always:
			return "always";
		case nano::frontiers_confirmation_mode::automatic:
			return "auto";
		case nano::frontiers_confirmation_mode::disabled:
			return "disabled";
		default:
			return "auto";
	}
}

nano::frontiers_confirmation_mode nano::node_config::deserialize_frontiers_confirmation (std::string const & string_a)
{
	if (string_a == "always")
	{
		return nano::frontiers_confirmation_mode::always;
	}
	else if (string_a == "auto")
	{
		return nano::frontiers_confirmation_mode::automatic;
	}
	else if (string_a == "disabled")
	{
		return nano::frontiers_confirmation_mode::disabled;
	}
	else
	{
		return nano::frontiers_confirmation_mode::invalid;
	}
}

void nano::node_config::deserialize_address (std::string const & entry_a, std::vector<std::pair<std::string, uint16_t>> & container_a) const
{
	auto port_position (entry_a.rfind (':'));
	bool result = (port_position == -1);
	if (!result)
	{
		auto port_str (entry_a.substr (port_position + 1));
		uint16_t port;
		result |= parse_port (port_str, port);
		if (!result)
		{
			auto address (entry_a.substr (0, port_position));
			container_a.emplace_back (address, port);
		}
	}
}

nano::account nano::node_config::random_representative () const
{
	debug_assert (!preconfigured_representatives.empty ());
	size_t index (nano::random_pool::generate_word32 (0, static_cast<CryptoPP::word32> (preconfigured_representatives.size () - 1)));
	auto result (preconfigured_representatives[index]);
	return result;
}
