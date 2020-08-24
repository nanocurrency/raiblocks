#include <nano/lib/cli.hpp>
#include <nano/lib/tomlconfig.hpp>
#include <nano/node/cli.hpp>
#include <nano/node/common.hpp>
#include <nano/node/daemonconfig.hpp>
#include <nano/node/node.hpp>

#include <boost/format.hpp>

namespace
{
void reset_confirmation_heights (nano::block_store & store);
bool is_using_rocksdb (boost::filesystem::path const & data_path, std::error_code & ec);
}

std::string nano::error_cli_messages::message (int ev) const
{
	switch (static_cast<nano::error_cli> (ev))
	{
		case nano::error_cli::generic:
			return "Unknown error";
		case nano::error_cli::parse_error:
			return "Coud not parse command line";
		case nano::error_cli::invalid_arguments:
			return "Invalid arguments";
		case nano::error_cli::unknown_command:
			return "Unknown command";
		case nano::error_cli::database_write_error:
			return "Database write error";
		case nano::error_cli::reading_config:
			return "Config file read error";
		case nano::error_cli::disable_all_network:
			return "Flags --disable_tcp_realtime and --disable_udp cannot be used together";
		case nano::error_cli::ambiguous_udp_options:
			return "Flags --disable_udp and --enable_udp cannot be used together";
	}

	return "Invalid error code";
}

void nano::add_node_options (boost::program_options::options_description & description_a)
{
	// clang-format off
	description_a.add_options ()
	("account_create", "Insert next deterministic key in to <wallet>")
	("account_get", "Get account number for the <key>")
	("account_key", "Get the public key for <account>")
	("vacuum", "Compact database. If data_path is missing, the database in data directory is compacted.")
	("snapshot", "Compact database and create snapshot, functions similar to vacuum but does not replace the existing database")
	("data_path", boost::program_options::value<std::string> (), "Use the supplied path as the data directory")
	("network", boost::program_options::value<std::string> (), "Use the supplied network (live, test, beta or dev)")
	("clear_send_ids", "Remove all send IDs from the database (dangerous: not intended for production use)")
	("online_weight_clear", "Clear online weight history records")
	("peer_clear", "Clear online peers database dump")
	("unchecked_clear", "Clear unchecked blocks")
	("confirmation_height_clear", "Clear confirmation height")
	("rebuild_database", "Rebuild LMDB database with vacuum for best compaction")
	("diagnostics", "Run internal diagnostics")
	("generate_config", boost::program_options::value<std::string> (), "Write configuration to stdout, populated with defaults suitable for this system. Pass the configuration type node or rpc. See also use_defaults.")
	("key_create", "Generates a adhoc random keypair and prints it to stdout")
	("key_expand", "Derive public key and account number from <key>")
	("wallet_add_adhoc", "Insert <key> in to <wallet>")
	("wallet_create", "Creates a new wallet and prints the ID")
	("wallet_change_seed", "Changes seed for <wallet> to <key>")
	("wallet_decrypt_unsafe", "Decrypts <wallet> using <password>, !!THIS WILL PRINT YOUR PRIVATE KEY TO STDOUT!!")
	("wallet_destroy", "Destroys <wallet> and all keys it contains")
	("wallet_import", "Imports keys in <file> using <password> in to <wallet>")
	("wallet_list", "Dumps wallet IDs and public keys")
	("wallet_remove", "Remove <account> from <wallet>")
	("wallet_representative_get", "Prints default representative for <wallet>")
	("wallet_representative_set", "Set <account> as default representative for <wallet>")
	("vote_dump", "Dump most recent votes from representatives")
	("account", boost::program_options::value<std::string> (), "Defines <account> for other commands")
	("file", boost::program_options::value<std::string> (), "Defines <file> for other commands")
	("key", boost::program_options::value<std::string> (), "Defines the <key> for other commands, hex")
	("seed", boost::program_options::value<std::string> (), "Defines the <seed> for other commands, hex")
	("password", boost::program_options::value<std::string> (), "Defines <password> for other commands")
	("wallet", boost::program_options::value<std::string> (), "Defines <wallet> for other commands")
	("force", boost::program_options::value<bool>(), "Bool to force command if allowed")
	("use_defaults", "If present, the generate_config command will generate uncommented entries");
	// clang-format on
}

void nano::add_node_flag_options (boost::program_options::options_description & description_a)
{
	// clang-format off
	description_a.add_options()
		("disable_backup", "Disable wallet automatic backups")
		("disable_lazy_bootstrap", "Disables lazy bootstrap")
		("disable_legacy_bootstrap", "Disables legacy bootstrap")
		("disable_wallet_bootstrap", "Disables wallet lazy bootstrap")
		("disable_bootstrap_listener", "Disables bootstrap processing for TCP listener (not including realtime network TCP connections)")
		("disable_tcp_realtime", "Disables TCP realtime network")
		("disable_udp", "(Deprecated) UDP is disabled by default")
		("enable_udp", "Enables UDP realtime network")
		("disable_unchecked_cleanup", "Disables periodic cleanup of old records from unchecked table")
		("disable_unchecked_drop", "Disables drop of unchecked table at startup")
		("disable_providing_telemetry_metrics", "Disable using any node information in the telemetry_ack messages.")
		("disable_block_processor_unchecked_deletion", "Disable deletion of unchecked blocks after processing")
		("allow_bootstrap_peers_duplicates", "Allow multiple connections to same peer in bootstrap attempts")
		("fast_bootstrap", "Increase bootstrap speed for high end nodes with higher limits")
		("block_processor_batch_size", boost::program_options::value<std::size_t>(), "Increase block processor transaction batch write size, default 0 (limited by config block_processor_batch_max_time), 256k for fast_bootstrap")
		("block_processor_full_size", boost::program_options::value<std::size_t>(), "Increase block processor allowed blocks queue size before dropping live network packets and holding bootstrap download, default 65536, 1 million for fast_bootstrap")
		("block_processor_verification_size", boost::program_options::value<std::size_t>(), "Increase batch signature verification size in block processor, default 0 (limited by config signature_checker_threads), unlimited for fast_bootstrap")
		("inactive_votes_cache_size", boost::program_options::value<std::size_t>(), "Increase cached votes without active elections size, default 16384")
		("vote_processor_capacity", boost::program_options::value<std::size_t>(), "Vote processor queue size before dropping votes, default 144k")
		;
	// clang-format on
}

namespace
{
void database_write_lock_error (std::error_code & ec)
{
	std::cerr << "Write database error, this cannot be run while the node is already running\n";
	ec = nano::error_cli::database_write_error;
}

bool copy_database (nano::environment & env_a, boost::program_options::variables_map const & vm, boost::filesystem::path const & output_path, std::error_code & ec)
{
	bool success = false;
	bool needs_to_write = vm.count ("unchecked_clear") || vm.count ("clear_send_ids") || vm.count ("online_weight_clear") || vm.count ("peer_clear") || vm.count ("confirmation_height_clear") || vm.count ("rebuild_database");

	nano::node_flags flags;
	auto error{ env_a.apply_overrides (flags, nano::environment::purpose::inactive, vm) };
	debug_assert (!error);
	flags.read_only = !needs_to_write;
	nano::inactive_node node (env_a, flags);
	if (!node.node->init_error ())
	{
		if (vm.count ("unchecked_clear"))
		{
			auto transaction (node.node->store.tx_begin_write ());
			node.node->store.unchecked_clear (transaction);
		}
		if (vm.count ("clear_send_ids"))
		{
			auto transaction (node.node->wallets.tx_begin_write ());
			node.node->wallets.clear_send_ids (transaction);
		}
		if (vm.count ("online_weight_clear"))
		{
			auto transaction (node.node->store.tx_begin_write ());
			node.node->store.online_weight_clear (transaction);
		}
		if (vm.count ("peer_clear"))
		{
			auto transaction (node.node->store.tx_begin_write ());
			node.node->store.peer_clear (transaction);
		}
		if (vm.count ("confirmation_height_clear"))
		{
			reset_confirmation_heights (node.node->store);
		}
		if (vm.count ("rebuild_database"))
		{
			auto transaction (node.node->store.tx_begin_write ());
			node.node->store.rebuild_db (transaction);
		}

		success = node.node->copy_with_compaction (output_path);
	}
	else
	{
		database_write_lock_error (ec);
	}
	return success;
}
}

std::error_code nano::handle_node_options (boost::program_options::variables_map const & vm)
{
	std::error_code ec;
	boost::filesystem::path data_path = vm.count ("data_path") ? boost::filesystem::path (vm["data_path"].as<std::string> ()) : nano::working_path ();

	nano::environment env{ data_path };
	if (vm.count ("account_create"))
	{
		if (vm.count ("wallet") == 1)
		{
			nano::wallet_id wallet_id;
			if (!wallet_id.decode_hex (vm["wallet"].as<std::string> ()))
			{
				std::string password;
				if (vm.count ("password") > 0)
				{
					password = vm["password"].as<std::string> ();
				}
				auto inactive_node = nano::default_inactive_node (env, vm);
				auto wallet (inactive_node->node->wallets.open (wallet_id));
				if (wallet != nullptr)
				{
					auto transaction (wallet->wallets.tx_begin_write ());
					if (!wallet->enter_password (transaction, password))
					{
						auto pub (wallet->store.deterministic_insert (transaction));
						std::cout << boost::str (boost::format ("Account: %1%\n") % pub.to_account ());
					}
					else
					{
						std::cerr << "Invalid password\n";
						ec = nano::error_cli::invalid_arguments;
					}
				}
				else
				{
					std::cerr << "Wallet doesn't exist\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				std::cerr << "Invalid wallet id\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else
		{
			std::cerr << "wallet_add command requires one <wallet> option and one <key> option and optionally one <password> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("account_get") > 0)
	{
		if (vm.count ("key") == 1)
		{
			nano::account pub;
			pub.decode_hex (vm["key"].as<std::string> ());
			std::cout << "Account: " << pub.to_account () << std::endl;
		}
		else
		{
			std::cerr << "account comand requires one <key> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("account_key") > 0)
	{
		if (vm.count ("account") == 1)
		{
			nano::account account;
			account.decode_account (vm["account"].as<std::string> ());
			std::cout << "Hex: " << account.to_string () << std::endl;
		}
		else
		{
			std::cerr << "account_key command requires one <account> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("vacuum") > 0)
	{
		try
		{
			auto using_rocksdb = is_using_rocksdb (data_path, ec);
			if (!ec)
			{
				std::cout << "Vacuuming database copy in ";
				boost::filesystem::path source_path;
				boost::filesystem::path backup_path;
				boost::filesystem::path vacuum_path;
				if (using_rocksdb)
				{
					source_path = data_path / "rocksdb";
					backup_path = source_path / "backup";
					vacuum_path = backup_path / "vacuumed";
					if (!boost::filesystem::exists (vacuum_path))
					{
						boost::filesystem::create_directories (vacuum_path);
					}

					std::cout << source_path << "\n";
				}
				else
				{
					source_path = data_path / "data.ldb";
					backup_path = data_path / "backup.vacuum.ldb";
					vacuum_path = data_path / "vacuumed.ldb";
					std::cout << data_path << "\n";
				}
				std::cout << "This may take a while..." << std::endl;

				bool success = copy_database (env, vm, vacuum_path, ec);
				if (success)
				{
					// Note that these throw on failure
					std::cout << "Finalizing" << std::endl;
					if (using_rocksdb)
					{
						nano::remove_all_files_in_dir (backup_path);
						nano::move_all_files_to_dir (source_path, backup_path);
						nano::move_all_files_to_dir (vacuum_path, source_path);
						boost::filesystem::remove_all (vacuum_path);
					}
					else
					{
						boost::filesystem::remove (backup_path);
						boost::filesystem::rename (source_path, backup_path);
						boost::filesystem::rename (vacuum_path, source_path);
					}
					std::cout << "Vacuum completed" << std::endl;
				}
				else
				{
					std::cerr << "Vacuum failed (copying returned false)" << std::endl;
				}
			}
			else
			{
				std::cerr << "Vacuum failed. RocksDB is enabled but the node has not been built with RocksDB support" << std::endl;
			}
		}
		catch (const boost::filesystem::filesystem_error & ex)
		{
			std::cerr << "Vacuum failed during a file operation: " << ex.what () << std::endl;
		}
		catch (...)
		{
			std::cerr << "Vacuum failed (unknown reason)" << std::endl;
		}
	}
	else if (vm.count ("snapshot"))
	{
		try
		{
			auto using_rocksdb = is_using_rocksdb (data_path, ec);
			if (!ec)
			{
				boost::filesystem::path source_path;
				boost::filesystem::path snapshot_path;
				if (using_rocksdb)
				{
					source_path = data_path / "rocksdb";
					snapshot_path = source_path / "backup";
				}
				else
				{
					source_path = data_path / "data.ldb";
					snapshot_path = data_path / "snapshot.ldb";
				}

				std::cout << "Database snapshot of " << source_path << " to " << snapshot_path << " in progress" << std::endl;
				std::cout << "This may take a while..." << std::endl;

				bool success = copy_database (env, vm, snapshot_path, ec);
				if (success)
				{
					std::cout << "Snapshot completed, This can be found at " << snapshot_path << std::endl;
				}
				else
				{
					std::cerr << "Snapshot failed (copying returned false)" << std::endl;
				}
			}
			else
			{
				std::cerr << "Snapshot failed. RocksDB is enabled but the node has not been built with RocksDB support" << std::endl;
			}
		}
		catch (const boost::filesystem::filesystem_error & ex)
		{
			std::cerr << "Snapshot failed during a file operation: " << ex.what () << std::endl;
		}
		catch (...)
		{
			std::cerr << "Snapshot failed (unknown reason)" << std::endl;
		}
	}
	else if (vm.count ("unchecked_clear"))
	{
		nano::node_flags flags;
		auto error{ env.apply_overrides (flags, nano::environment::purpose::inactive, vm) };
		flags.read_only = false;
		nano::inactive_node node (env, flags);
		if (!node.node->init_error ())
		{
			auto transaction (node.node->store.tx_begin_write ());
			node.node->store.unchecked_clear (transaction);
			std::cout << "Unchecked blocks deleted" << std::endl;
		}
		else
		{
			database_write_lock_error (ec);
		}
	}
	else if (vm.count ("clear_send_ids"))
	{
		nano::node_flags flags;
		auto error{ env.apply_overrides (flags, nano::environment::purpose::inactive, vm) };
		flags.read_only = false;
		nano::inactive_node node (env, flags);
		if (!node.node->init_error ())
		{
			auto transaction (node.node->wallets.tx_begin_write ());
			node.node->wallets.clear_send_ids (transaction);
			std::cout << "Send IDs deleted" << std::endl;
		}
		else
		{
			database_write_lock_error (ec);
		}
	}
	else if (vm.count ("online_weight_clear"))
	{
		nano::node_flags flags;
		auto error{ env.apply_overrides (flags, nano::environment::purpose::inactive, vm) };
		flags.read_only = false;
		nano::inactive_node node (env, flags);
		if (!node.node->init_error ())
		{
			auto transaction (node.node->store.tx_begin_write ());
			node.node->store.online_weight_clear (transaction);
			std::cout << "Onine weight records are removed" << std::endl;
		}
		else
		{
			database_write_lock_error (ec);
		}
	}
	else if (vm.count ("peer_clear"))
	{
		nano::node_flags flags;
		auto error{ env.apply_overrides (flags, nano::environment::purpose::inactive, vm) };
		flags.read_only = false;
		nano::inactive_node node (env, flags);
		if (!node.node->init_error ())
		{
			auto transaction (node.node->store.tx_begin_write ());
			node.node->store.peer_clear (transaction);
			std::cout << "Database peers are removed" << std::endl;
		}
		else
		{
			database_write_lock_error (ec);
		}
	}
	else if (vm.count ("confirmation_height_clear"))
	{
		nano::node_flags flags;
		auto error{ env.apply_overrides (flags, nano::environment::purpose::inactive, vm) };
		flags.read_only = false;
		nano::inactive_node node (env, flags);
		if (!node.node->init_error ())
		{
			auto account_it = vm.find ("account");
			if (account_it != vm.cend ())
			{
				auto account_str = account_it->second.as<std::string> ();
				nano::account account;
				if (!account.decode_account (account_str))
				{
					nano::confirmation_height_info confirmation_height_info;
					auto transaction (node.node->store.tx_begin_read ());
					if (!node.node->store.confirmation_height_get (transaction, account, confirmation_height_info))
					{
						auto transaction (node.node->store.tx_begin_write ());
						auto conf_height_reset_num = 0;
						if (account == node.node->env.constants.ledger.genesis_account)
						{
							conf_height_reset_num = 1;
							node.node->store.confirmation_height_put (transaction, account, { confirmation_height_info.height, node.node->env.constants.ledger.genesis_block });
						}
						else
						{
							node.node->store.confirmation_height_clear (transaction, account, confirmation_height_info.height);
						}

						std::cout << "Confirmation height of account " << account_str << " is set to " << conf_height_reset_num << std::endl;
					}
					else
					{
						std::cerr << "Could not find account" << std::endl;
						ec = nano::error_cli::generic;
					}
				}
				else
				{
					std::cerr << "Invalid account id\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				reset_confirmation_heights (node.node->store);
				std::cout << "Confirmation heights of all accounts (except genesis which is set to 1) are set to 0" << std::endl;
			}
		}
		else
		{
			database_write_lock_error (ec);
		}
	}
	else if (vm.count ("generate_config"))
	{
		auto type = vm["generate_config"].as<std::string> ();
		nano::tomlconfig toml;
		bool valid_type = false;
		if (type == "node")
		{
			valid_type = true;
			nano::daemon_config config;
			config.serialize_toml (toml);
		}
		else if (type == "rpc")
		{
			valid_type = true;
			nano::rpc_config config;
			config.serialize_toml (toml);
		}
		else
		{
			std::cerr << "Invalid configuration type " << type << ". Must be node or rpc." << std::endl;
		}

		if (valid_type)
		{
			std::cout << "# This is an example configuration file for Nano. Visit https://docs.nano.org/running-a-node/configuration/ for more information.\n#\n"
			          << "# Fields may need to be defined in the context of a [category] above them.\n"
			          << "# The desired configuration changes should be placed in config-" << type << ".toml in the node data path.\n"
			          << "# To change a value from its default, uncomment (erasing #) the corresponding field.\n"
			          << "# It is not recommended to uncomment every field, as the default value for important fields may change in the future. Only change what you need.\n"
			          << "# Additional information for notable configuration options is available in https://docs.nano.org/running-a-node/configuration/#notable-configuration-options\n";

			if (vm.count ("use_defaults"))
			{
				std::cout << toml.to_string () << std::endl;
			}
			else
			{
				std::cout << toml.to_string_commented_entries () << std::endl;
			}
		}
	}
	else if (vm.count ("diagnostics"))
	{
		auto inactive_node = nano::default_inactive_node (env, vm);
		std::cout << "Testing hash function" << std::endl;
		nano::raw_key key;
		key.data.clear ();
		nano::send_block send (0, 0, 0, key, 0, 0);
		std::cout << "Testing key derivation function" << std::endl;
		nano::raw_key junk1;
		junk1.data.clear ();
		nano::uint256_union junk2 (0);
		nano::kdf kdf;
		kdf.phs (junk1, "", junk2);
		std::cout << "Testing time retrieval latency... " << std::flush;
		nano::timer<std::chrono::nanoseconds> timer (nano::timer_state::started);
		auto const iters = 2'000'000;
		for (auto i (0); i < iters; ++i)
		{
			(void)std::chrono::steady_clock::now ();
		}
		std::cout << timer.stop ().count () / iters << " " << timer.unit () << std::endl;
		std::cout << "Dumping OpenCL information" << std::endl;
		bool error (false);
		nano::opencl_environment environment (error);
		if (!error)
		{
			environment.dump (std::cout);
			std::stringstream stream;
			environment.dump (stream);
			inactive_node->node->logger.always_log (stream.str ());
		}
		else
		{
			std::cerr << "Error initializing OpenCL" << std::endl;
			ec = nano::error_cli::generic;
		}
	}
	else if (vm.count ("key_create"))
	{
		nano::keypair pair;
		std::cout << "Private: " << pair.prv.data.to_string () << std::endl
		          << "Public: " << pair.pub.to_string () << std::endl
		          << "Account: " << pair.pub.to_account () << std::endl;
	}
	else if (vm.count ("key_expand"))
	{
		if (vm.count ("key") == 1)
		{
			nano::private_key prv;
			prv.decode_hex (vm["key"].as<std::string> ());
			nano::public_key pub (nano::pub_key (prv));
			std::cout << "Private: " << prv.to_string () << std::endl
			          << "Public: " << pub.to_string () << std::endl
			          << "Account: " << pub.to_account () << std::endl;
		}
		else
		{
			std::cerr << "key_expand command requires one <key> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("wallet_add_adhoc"))
	{
		if (vm.count ("wallet") == 1 && vm.count ("key") == 1)
		{
			nano::wallet_id wallet_id;
			if (!wallet_id.decode_hex (vm["wallet"].as<std::string> ()))
			{
				std::string password;
				if (vm.count ("password") > 0)
				{
					password = vm["password"].as<std::string> ();
				}
				auto inactive_node = nano::default_inactive_node (env, vm);
				auto wallet (inactive_node->node->wallets.open (wallet_id));
				if (wallet != nullptr)
				{
					auto transaction (wallet->wallets.tx_begin_write ());
					if (!wallet->enter_password (transaction, password))
					{
						nano::raw_key key;
						if (!key.data.decode_hex (vm["key"].as<std::string> ()))
						{
							wallet->store.insert_adhoc (transaction, key);
						}
						else
						{
							std::cerr << "Invalid key\n";
							ec = nano::error_cli::invalid_arguments;
						}
					}
					else
					{
						std::cerr << "Invalid password\n";
						ec = nano::error_cli::invalid_arguments;
					}
				}
				else
				{
					std::cerr << "Wallet doesn't exist\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				std::cerr << "Invalid wallet id\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else
		{
			std::cerr << "wallet_add command requires one <wallet> option and one <key> option and optionally one <password> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("wallet_change_seed"))
	{
		if (vm.count ("wallet") == 1 && (vm.count ("seed") == 1 || vm.count ("key") == 1))
		{
			nano::wallet_id wallet_id;
			if (!wallet_id.decode_hex (vm["wallet"].as<std::string> ()))
			{
				std::string password;
				if (vm.count ("password") > 0)
				{
					password = vm["password"].as<std::string> ();
				}
				auto inactive_node = nano::default_inactive_node (env, vm);
				auto wallet (inactive_node->node->wallets.open (wallet_id));
				if (wallet != nullptr)
				{
					auto transaction (wallet->wallets.tx_begin_write ());
					if (!wallet->enter_password (transaction, password))
					{
						nano::raw_key seed;
						if (vm.count ("seed"))
						{
							if (seed.data.decode_hex (vm["seed"].as<std::string> ()))
							{
								std::cerr << "Invalid seed\n";
								ec = nano::error_cli::invalid_arguments;
							}
						}
						else if (seed.data.decode_hex (vm["key"].as<std::string> ()))
						{
							std::cerr << "Invalid key seed\n";
							ec = nano::error_cli::invalid_arguments;
						}
						if (!ec)
						{
							std::cout << "Changing seed and caching work. Please wait..." << std::endl;
							wallet->change_seed (transaction, seed);
						}
					}
					else
					{
						std::cerr << "Invalid password\n";
						ec = nano::error_cli::invalid_arguments;
					}
				}
				else
				{
					std::cerr << "Wallet doesn't exist\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				std::cerr << "Invalid wallet id\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else
		{
			std::cerr << "wallet_change_seed command requires one <wallet> option and one <seed> option and optionally one <password> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("wallet_create"))
	{
		nano::raw_key seed_key;
		if (vm.count ("seed") == 1)
		{
			if (seed_key.data.decode_hex (vm["seed"].as<std::string> ()))
			{
				std::cerr << "Invalid seed\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else if (vm.count ("seed") > 1)
		{
			std::cerr << "wallet_create command allows one optional <seed> parameter\n";
			ec = nano::error_cli::invalid_arguments;
		}
		else if (vm.count ("key") == 1)
		{
			if (seed_key.data.decode_hex (vm["key"].as<std::string> ()))
			{
				std::cerr << "Invalid seed key\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else if (vm.count ("key") > 1)
		{
			std::cerr << "wallet_create command allows one optional <key> seed parameter\n";
			ec = nano::error_cli::invalid_arguments;
		}
		if (!ec)
		{
			auto inactive_node = nano::default_inactive_node (env, vm);
			auto wallet_key = nano::random_wallet_id ();
			auto wallet (inactive_node->node->wallets.create (wallet_key));
			if (wallet != nullptr)
			{
				if (vm.count ("password") > 0)
				{
					std::string password (vm["password"].as<std::string> ());
					auto transaction (wallet->wallets.tx_begin_write ());
					auto error (wallet->store.rekey (transaction, password));
					if (error)
					{
						std::cerr << "Password change error\n";
						ec = nano::error_cli::invalid_arguments;
					}
				}
				if (vm.count ("seed") || vm.count ("key"))
				{
					auto transaction (wallet->wallets.tx_begin_write ());
					wallet->change_seed (transaction, seed_key);
				}
				std::cout << wallet_key.to_string () << std::endl;
			}
			else
			{
				std::cerr << "Wallet creation error\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
	}
	else if (vm.count ("wallet_decrypt_unsafe"))
	{
		if (vm.count ("wallet") == 1)
		{
			std::string password;
			if (vm.count ("password") == 1)
			{
				password = vm["password"].as<std::string> ();
			}
			nano::wallet_id wallet_id;
			if (!wallet_id.decode_hex (vm["wallet"].as<std::string> ()))
			{
				auto inactive_node = nano::default_inactive_node (env, vm);
				auto node = inactive_node->node;
				auto existing (inactive_node->node->wallets.items.find (wallet_id));
				if (existing != inactive_node->node->wallets.items.end ())
				{
					auto transaction (existing->second->wallets.tx_begin_write ());
					if (!existing->second->enter_password (transaction, password))
					{
						nano::raw_key seed;
						existing->second->store.seed (seed, transaction);
						std::cout << boost::str (boost::format ("Seed: %1%\n") % seed.data.to_string ());
						for (auto i (existing->second->store.begin (transaction)), m (existing->second->store.end ()); i != m; ++i)
						{
							nano::account const & account (i->first);
							nano::raw_key key;
							auto error (existing->second->store.fetch (transaction, account, key));
							(void)error;
							debug_assert (!error);
							std::cout << boost::str (boost::format ("Pub: %1% Prv: %2%\n") % account.to_account () % key.data.to_string ());
							if (nano::pub_key (key.as_private_key ()) != account)
							{
								std::cerr << boost::str (boost::format ("Invalid private key %1%\n") % key.data.to_string ());
							}
						}
					}
					else
					{
						std::cerr << "Invalid password\n";
						ec = nano::error_cli::invalid_arguments;
					}
				}
				else
				{
					std::cerr << "Wallet doesn't exist\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				std::cerr << "Invalid wallet id\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else
		{
			std::cerr << "wallet_decrypt_unsafe requires one <wallet> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("wallet_destroy"))
	{
		if (vm.count ("wallet") == 1)
		{
			nano::wallet_id wallet_id;
			if (!wallet_id.decode_hex (vm["wallet"].as<std::string> ()))
			{
				auto inactive_node = nano::default_inactive_node (env, vm);
				auto node = inactive_node->node;
				if (node->wallets.items.find (wallet_id) != node->wallets.items.end ())
				{
					node->wallets.destroy (wallet_id);
				}
				else
				{
					std::cerr << "Wallet doesn't exist\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				std::cerr << "Invalid wallet id\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else
		{
			std::cerr << "wallet_destroy requires one <wallet> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("wallet_import"))
	{
		if (vm.count ("file") == 1)
		{
			std::string filename (vm["file"].as<std::string> ());
			std::ifstream stream;
			stream.open (filename.c_str ());
			if (!stream.fail ())
			{
				std::stringstream contents;
				contents << stream.rdbuf ();
				std::string password;
				if (vm.count ("password") == 1)
				{
					password = vm["password"].as<std::string> ();
				}
				bool forced (false);
				if (vm.count ("force") == 1)
				{
					forced = vm["force"].as<bool> ();
				}
				if (vm.count ("wallet") == 1)
				{
					nano::wallet_id wallet_id;
					if (!wallet_id.decode_hex (vm["wallet"].as<std::string> ()))
					{
						auto inactive_node = nano::default_inactive_node (env, vm);
						auto node = inactive_node->node;
						auto existing (node->wallets.items.find (wallet_id));
						if (existing != node->wallets.items.end ())
						{
							bool valid (false);
							{
								auto transaction (node->wallets.tx_begin_write ());
								valid = existing->second->store.valid_password (transaction);
								if (!valid)
								{
									valid = !existing->second->enter_password (transaction, password);
								}
							}
							if (valid)
							{
								if (existing->second->import (contents.str (), password))
								{
									std::cerr << "Unable to import wallet\n";
									ec = nano::error_cli::invalid_arguments;
								}
								else
								{
									std::cout << "Import completed\n";
								}
							}
							else
							{
								std::cerr << boost::str (boost::format ("Invalid password for wallet %1%\nNew wallet should have empty (default) password or passwords for new wallet & json file should match\n") % wallet_id.to_string ());
								ec = nano::error_cli::invalid_arguments;
							}
						}
						else
						{
							if (!forced)
							{
								std::cerr << "Wallet doesn't exist\n";
								ec = nano::error_cli::invalid_arguments;
							}
							else
							{
								bool error (true);
								{
									nano::lock_guard<std::mutex> lock (node->wallets.mutex);
									auto transaction (node->wallets.tx_begin_write ());
									nano::wallet wallet (error, transaction, node->wallets, wallet_id.to_string (), contents.str ());
								}
								if (error)
								{
									std::cerr << "Unable to import wallet\n";
									ec = nano::error_cli::invalid_arguments;
								}
								else
								{
									node->wallets.reload ();
									nano::lock_guard<std::mutex> lock (node->wallets.mutex);
									release_assert (node->wallets.items.find (wallet_id) != node->wallets.items.end ());
									std::cout << "Import completed\n";
								}
							}
						}
					}
					else
					{
						std::cerr << "Invalid wallet id\n";
						ec = nano::error_cli::invalid_arguments;
					}
				}
				else
				{
					std::cerr << "wallet_import requires one <wallet> option\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				std::cerr << "Unable to open <file>\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else
		{
			std::cerr << "wallet_import requires one <file> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("wallet_list"))
	{
		auto inactive_node = nano::default_inactive_node (env, vm);
		auto node = inactive_node->node;
		for (auto i (node->wallets.items.begin ()), n (node->wallets.items.end ()); i != n; ++i)
		{
			std::cout << boost::str (boost::format ("Wallet ID: %1%\n") % i->first.to_string ());
			auto transaction (i->second->wallets.tx_begin_read ());
			for (auto j (i->second->store.begin (transaction)), m (i->second->store.end ()); j != m; ++j)
			{
				std::cout << nano::account (j->first).to_account () << '\n';
			}
		}
	}
	else if (vm.count ("wallet_remove"))
	{
		if (vm.count ("wallet") == 1 && vm.count ("account") == 1)
		{
			auto inactive_node = nano::default_inactive_node (env, vm);
			auto node = inactive_node->node;
			nano::wallet_id wallet_id;
			if (!wallet_id.decode_hex (vm["wallet"].as<std::string> ()))
			{
				auto wallet (node->wallets.items.find (wallet_id));
				if (wallet != node->wallets.items.end ())
				{
					nano::account account_id;
					if (!account_id.decode_account (vm["account"].as<std::string> ()))
					{
						auto transaction (wallet->second->wallets.tx_begin_write ());
						auto account (wallet->second->store.find (transaction, account_id));
						if (account != wallet->second->store.end ())
						{
							wallet->second->store.erase (transaction, account_id);
						}
						else
						{
							std::cerr << "Account not found in wallet\n";
							ec = nano::error_cli::invalid_arguments;
						}
					}
					else
					{
						std::cerr << "Invalid account id\n";
						ec = nano::error_cli::invalid_arguments;
					}
				}
				else
				{
					std::cerr << "Wallet not found\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				std::cerr << "Invalid wallet id\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else
		{
			std::cerr << "wallet_remove command requires one <wallet> and one <account> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("wallet_representative_get"))
	{
		if (vm.count ("wallet") == 1)
		{
			nano::wallet_id wallet_id;
			if (!wallet_id.decode_hex (vm["wallet"].as<std::string> ()))
			{
				auto inactive_node = nano::default_inactive_node (env, vm);
				auto node = inactive_node->node;
				auto wallet (node->wallets.items.find (wallet_id));
				if (wallet != node->wallets.items.end ())
				{
					auto transaction (wallet->second->wallets.tx_begin_read ());
					auto representative (wallet->second->store.representative (transaction));
					std::cout << boost::str (boost::format ("Representative: %1%\n") % representative.to_account ());
				}
				else
				{
					std::cerr << "Wallet not found\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				std::cerr << "Invalid wallet id\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else
		{
			std::cerr << "wallet_representative_get requires one <wallet> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("wallet_representative_set"))
	{
		if (vm.count ("wallet") == 1)
		{
			if (vm.count ("account") == 1)
			{
				nano::wallet_id wallet_id;
				if (!wallet_id.decode_hex (vm["wallet"].as<std::string> ()))
				{
					nano::account account;
					if (!account.decode_account (vm["account"].as<std::string> ()))
					{
						auto inactive_node = nano::default_inactive_node (env, vm);
						auto node = inactive_node->node;
						auto wallet (node->wallets.items.find (wallet_id));
						if (wallet != node->wallets.items.end ())
						{
							auto transaction (wallet->second->wallets.tx_begin_write ());
							wallet->second->store.representative_set (transaction, account);
						}
						else
						{
							std::cerr << "Wallet not found\n";
							ec = nano::error_cli::invalid_arguments;
						}
					}
					else
					{
						std::cerr << "Invalid account\n";
						ec = nano::error_cli::invalid_arguments;
					}
				}
				else
				{
					std::cerr << "Invalid wallet id\n";
					ec = nano::error_cli::invalid_arguments;
				}
			}
			else
			{
				std::cerr << "wallet_representative_set requires one <account> option\n";
				ec = nano::error_cli::invalid_arguments;
			}
		}
		else
		{
			std::cerr << "wallet_representative_set requires one <wallet> option\n";
			ec = nano::error_cli::invalid_arguments;
		}
	}
	else if (vm.count ("vote_dump") == 1)
	{
		auto inactive_node = nano::default_inactive_node (env, vm);
		auto node = inactive_node->node;
		auto transaction (node->store.tx_begin_read ());
		for (auto i (node->store.vote_begin (transaction)), n (node->store.vote_end ()); i != n; ++i)
		{
			auto const & vote (i->second);
			std::cerr << boost::str (boost::format ("%1%\n") % vote->to_json ());
		}
	}
	else
	{
		ec = nano::error_cli::unknown_command;
	}

	return ec;
}

std::unique_ptr<nano::inactive_node> nano::default_inactive_node (nano::environment & env_a, boost::program_options::variables_map const & vm_a)
{
	nano::node_flags flags;
	auto error{ env_a.apply_overrides (flags, nano::environment::purpose::inactive, vm_a) };
	return std::make_unique<nano::inactive_node> (env_a, flags);
}

namespace
{
void reset_confirmation_heights (nano::block_store & store)
{
	// First do a clean sweep
	auto transaction (store.tx_begin_write ());
	store.confirmation_height_clear (transaction);

	// Then make sure the confirmation height of the genesis account open block is 1
	nano::environment_constants constants;
	store.confirmation_height_put (transaction, constants.ledger.genesis_account, { 1, constants.ledger.genesis_hash });
}

bool is_using_rocksdb (boost::filesystem::path const & data_path, std::error_code & ec)
{
	nano::daemon_config config;
	auto error = nano::read_node_config_toml (data_path, config);
	if (!error)
	{
		bool use_rocksdb = config.node.rocksdb_config.enable;
		if (use_rocksdb)
		{
#if !NANO_ROCKSDB
			ec = nano::error_cli::database_write_error;
#endif
			return (NANO_ROCKSDB == 1);
		}
	}
	else
	{
		ec = nano::error_cli::reading_config;
	}

	return false;
}
}
