#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/utility.hpp>
#include <nano/nano_node/daemon.hpp>
#include <nano/node/cli.hpp>
#include <nano/node/ipc.hpp>
#include <nano/node/json_handler.hpp>
#include <nano/node/node.hpp>
#include <nano/node/testing.hpp>

#include <boost/filesystem/operations.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>

#include <sstream>

#include <argon2.h>

// Some builds (mac) fail due to "Boost.Stacktrace requires `_Unwind_Backtrace` function".
#ifndef _WIN32
#ifdef NANO_STACKTRACE_BACKTRACE
#define BOOST_STACKTRACE_USE_BACKTRACE
#endif
#ifndef _GNU_SOURCE
#define BEFORE_GNU_SOURCE 0
#define _GNU_SOURCE
#else
#define BEFORE_GNU_SOURCE 1
#endif
#endif
#include <boost/stacktrace.hpp>
#ifndef _WIN32
#if !BEFORE_GNU_SOURCE
#undef _GNU_SOURCE
#endif
#endif

int main (int argc, char * const * argv)
{
	nano::set_umask ();
	nano::node_singleton_memory_pool_purge_guard memory_pool_cleanup_guard;
	boost::program_options::options_description description ("Command line options");
	// clang-format off
	description.add_options ()
		("help", "Print out options")
		("version", "Prints out version")
		("config", boost::program_options::value<std::vector<std::string>>()->multitoken(), "Pass node configuration values. This takes precedence over any values in the configuration file. This option can be repeated multiple times.")
		("daemon", "Start node daemon")
		("debug_block_count", "Display the number of block")
		("debug_bootstrap_generate", "Generate bootstrap sequence of blocks")
		("debug_dump_frontier_unchecked_dependents", "Dump frontiers which have matching unchecked keys")
		("debug_dump_online_weight", "Dump online_weights table")
		("debug_dump_representatives", "List representatives and weights")
		("debug_account_count", "Display the number of accounts")
		("debug_mass_activity", "Generates fake debug activity")
		("debug_profile_generate", "Profile work generation")
		("debug_profile_validate", "Profile work validation")
		("debug_opencl", "OpenCL work generation")
		("debug_profile_kdf", "Profile kdf function")
		("debug_output_last_backtrace_dump", "Displays the contents of the latest backtrace in the event of a nano_node crash")
		("debug_sys_logging", "Test the system logger")
		("debug_verify_profile", "Profile signature verification")
		("debug_verify_profile_batch", "Profile batch signature verification")
		("debug_profile_bootstrap", "Profile bootstrap style blocks processing (at least 10GB of free storage space required)")
		("debug_profile_sign", "Profile signature generation")
		("debug_profile_process", "Profile active blocks processing (only for nano_test_network)")
		("debug_profile_votes", "Profile votes processing (only for nano_test_network)")
		("debug_random_feed", "Generates output to RNG test suites")
		("debug_rpc", "Read an RPC command from stdin and invoke it. Network operations will have no effect.")
		("debug_validate_blocks", "Check all blocks for correct hash, signature, work value")
		("debug_peers", "Display peer IPv6:port connections")
		("debug_cemented_block_count", "Displays the number of cemented (confirmed) blocks")
		("debug_stacktrace", "Display an example stacktrace")
		("debug_account_versions", "Display the total counts of each version for all accounts (including unpocketed)")
		("platform", boost::program_options::value<std::string> (), "Defines the <platform> for OpenCL commands")
		("device", boost::program_options::value<std::string> (), "Defines <device> for OpenCL command")
		("threads", boost::program_options::value<std::string> (), "Defines <threads> count for OpenCL command")
		("difficulty", boost::program_options::value<std::string> (), "Defines <difficulty> for OpenCL command, HEX")
		("pow_sleep_interval", boost::program_options::value<std::string> (), "Defines the amount to sleep inbetween each pow calculation attempt");
	// clang-format on
	nano::add_node_options (description);
	nano::add_node_flag_options (description);
	boost::program_options::variables_map vm;
	try
	{
		boost::program_options::store (boost::program_options::parse_command_line (argc, argv, description), vm);
	}
	catch (boost::program_options::error const & err)
	{
		std::cerr << err.what () << std::endl;
		return 1;
	}
	boost::program_options::notify (vm);
	int result (0);

	auto network (vm.find ("network"));
	if (network != vm.end ())
	{
		auto err (nano::network_constants::set_active_network (network->second.as<std::string> ()));
		if (err)
		{
			std::cerr << nano::network_constants::active_network_err_msg << std::endl;
			std::exit (1);
		}
	}

	auto data_path_it = vm.find ("data_path");
	if (data_path_it == vm.end ())
	{
		std::string error_string;
		if (!nano::migrate_working_path (error_string))
		{
			std::cerr << error_string << std::endl;

			return 1;
		}
	}

	boost::filesystem::path data_path ((data_path_it != vm.end ()) ? data_path_it->second.as<std::string> () : nano::working_path ());
	auto ec = nano::handle_node_options (vm);
	if (ec == nano::error_cli::unknown_command)
	{
		if (vm.count ("daemon") > 0)
		{
			nano_daemon::daemon daemon;
			nano::node_flags flags;
			auto flags_ec = nano::update_flags (flags, vm);
			if (flags_ec)
			{
				std::cerr << flags_ec.message ();
				std::exit (1);
			}
			auto config (vm.find ("config"));
			if (config != vm.end ())
			{
				flags.config_overrides = config->second.as<std::vector<std::string>> ();
			}
			daemon.run (data_path, flags);
		}
		else if (vm.count ("debug_block_count"))
		{
			nano::inactive_node node (data_path);
			auto transaction (node.node->store.tx_begin_read ());
			std::cout << boost::str (boost::format ("Block count: %1%\n") % node.node->store.block_count (transaction).sum ());
		}
		else if (vm.count ("debug_bootstrap_generate"))
		{
			auto key_it = vm.find ("key");
			if (key_it != vm.end ())
			{
				nano::uint256_union key;
				if (!key.decode_hex (key_it->second.as<std::string> ()))
				{
					nano::keypair genesis (key.to_string ());
					nano::work_pool work (std::numeric_limits<unsigned>::max ());
					std::cout << "Genesis: " << genesis.prv.data.to_string () << "\n"
							  << "Public: " << genesis.pub.to_string () << "\n"
							  << "Account: " << genesis.pub.to_account () << "\n";
					nano::keypair landing;
					std::cout << "Landing: " << landing.prv.data.to_string () << "\n"
							  << "Public: " << landing.pub.to_string () << "\n"
							  << "Account: " << landing.pub.to_account () << "\n";
					for (auto i (0); i != 32; ++i)
					{
						nano::keypair rep;
						std::cout << "Rep" << i << ": " << rep.prv.data.to_string () << "\n"
								  << "Public: " << rep.pub.to_string () << "\n"
								  << "Account: " << rep.pub.to_account () << "\n";
					}
					nano::uint128_t balance (std::numeric_limits<nano::uint128_t>::max ());
					nano::open_block genesis_block (reinterpret_cast<const nano::block_hash &> (genesis.pub), genesis.pub, genesis.pub, genesis.prv, genesis.pub, *work.generate (genesis.pub));
					std::cout << genesis_block.to_json ();
					std::cout.flush ();
					nano::block_hash previous (genesis_block.hash ());
					for (auto i (0); i != 8; ++i)
					{
						nano::uint128_t yearly_distribution (nano::uint128_t (1) << (127 - (i == 7 ? 6 : i)));
						auto weekly_distribution (yearly_distribution / 52);
						for (auto j (0); j != 52; ++j)
						{
							assert (balance > weekly_distribution);
							balance = balance < (weekly_distribution * 2) ? 0 : balance - weekly_distribution;
							nano::send_block send (previous, landing.pub, balance, genesis.prv, genesis.pub, *work.generate (previous));
							previous = send.hash ();
							std::cout << send.to_json ();
							std::cout.flush ();
						}
					}
				}
				else
				{
					std::cerr << "Invalid key\n";
					result = -1;
				}
			}
			else
			{
				std::cerr << "Bootstrapping requires one <key> option\n";
				result = -1;
			}
		}
		else if (vm.count ("debug_dump_online_weight"))
		{
			nano::inactive_node node (data_path);
			auto current (node.node->online_reps.online_stake ());
			std::cout << boost::str (boost::format ("Online Weight %1%\n") % current);
			auto transaction (node.node->store.tx_begin_read ());
			for (auto i (node.node->store.online_weight_begin (transaction)), n (node.node->store.online_weight_end ()); i != n; ++i)
			{
				using time_point = std::chrono::system_clock::time_point;
				time_point ts (std::chrono::duration_cast<time_point::duration> (std::chrono::nanoseconds (i->first)));
				std::time_t timestamp = std::chrono::system_clock::to_time_t (ts);
				std::string weight;
				i->second.encode_dec (weight);
				std::cout << boost::str (boost::format ("Timestamp %1% Weight %2%\n") % ctime (&timestamp) % weight);
			}
		}
		else if (vm.count ("debug_dump_representatives"))
		{
			auto node_flags = nano::inactive_node_flag_defaults ();
			node_flags.cache_representative_weights_from_frontiers = true;
			nano::inactive_node node (data_path, 24000, node_flags);
			auto transaction (node.node->store.tx_begin_read ());
			nano::uint128_t total;
			auto rep_amounts = node.node->ledger.rep_weights.get_rep_amounts ();
			std::map<nano::account, nano::uint128_t> ordered_reps (rep_amounts.begin (), rep_amounts.end ());
			for (auto const & rep : ordered_reps)
			{
				total += rep.second;
				std::cout << boost::str (boost::format ("%1% %2% %3%\n") % rep.first.to_account () % rep.second.convert_to<std::string> () % total.convert_to<std::string> ());
			}
		}
		else if (vm.count ("debug_dump_frontier_unchecked_dependents"))
		{
			nano::inactive_node node (data_path);
			std::cout << "Outputting any frontier hashes which have associated key hashes in the unchecked table (may take some time)...\n";

			// Cache the account heads to make searching quicker against unchecked keys.
			auto transaction (node.node->store.tx_begin_read ());
			std::unordered_set<nano::block_hash> frontier_hashes;
			for (auto i (node.node->store.latest_begin (transaction)), n (node.node->store.latest_end ()); i != n; ++i)
			{
				frontier_hashes.insert (i->second.head);
			}

			// Check all unchecked keys for matching frontier hashes. Indicates an issue with process_batch algorithm
			for (auto i (node.node->store.unchecked_begin (transaction)), n (node.node->store.unchecked_end ()); i != n; ++i)
			{
				auto it = frontier_hashes.find (i->first.key ());
				if (it != frontier_hashes.cend ())
				{
					std::cout << it->to_string () << "\n";
				}
			}
		}
		else if (vm.count ("debug_account_count"))
		{
			nano::inactive_node node (data_path);
			auto transaction (node.node->store.tx_begin_read ());
			std::cout << boost::str (boost::format ("Frontier count: %1%\n") % node.node->store.account_count (transaction));
		}
		else if (vm.count ("debug_mass_activity"))
		{
			nano::system system (1);
			uint32_t count (1000000);
			system.generate_mass_activity (count, *system.nodes[0]);
		}
		else if (vm.count ("debug_profile_kdf"))
		{
			nano::network_params network_params;
			nano::uint256_union result;
			nano::uint256_union salt (0);
			std::string password ("");
			while (true)
			{
				auto begin1 (std::chrono::high_resolution_clock::now ());
				auto success (argon2_hash (1, network_params.kdf_work, 1, password.data (), password.size (), salt.bytes.data (), salt.bytes.size (), result.bytes.data (), result.bytes.size (), NULL, 0, Argon2_d, 0x10));
				(void)success;
				auto end1 (std::chrono::high_resolution_clock::now ());
				std::cerr << boost::str (boost::format ("Derivation time: %1%us\n") % std::chrono::duration_cast<std::chrono::microseconds> (end1 - begin1).count ());
			}
		}
		else if (vm.count ("debug_profile_generate"))
		{
			auto pow_rate_limiter = std::chrono::nanoseconds (0);
			auto pow_sleep_interval_it = vm.find ("pow_sleep_interval");
			if (pow_sleep_interval_it != vm.cend ())
			{
				pow_rate_limiter = std::chrono::nanoseconds (boost::lexical_cast<uint64_t> (pow_sleep_interval_it->second.as<std::string> ()));
			}

			nano::work_pool work (std::numeric_limits<unsigned>::max (), pow_rate_limiter);
			nano::change_block block (0, 0, nano::keypair ().prv, 0, 0);
			std::cerr << "Starting generation profiling\n";
			while (true)
			{
				block.hashables.previous.qwords[0] += 1;
				auto begin1 (std::chrono::high_resolution_clock::now ());
				block.block_work_set (*work.generate (block.root ()));
				auto end1 (std::chrono::high_resolution_clock::now ());
				std::cerr << boost::str (boost::format ("%|1$ 12d|\n") % std::chrono::duration_cast<std::chrono::microseconds> (end1 - begin1).count ());
			}
		}
		else if (vm.count ("debug_profile_validate"))
		{
			uint64_t difficulty{ nano::network_constants::publish_full_threshold };
			std::cerr << "Starting validation profile" << std::endl;
			auto start (std::chrono::steady_clock::now ());
			bool valid{ false };
			nano::block_hash hash{ 0 };
			uint64_t count{ 10000000U }; // 10M
			for (uint64_t i (0); i < count; ++i)
			{
				valid = nano::work_value (hash, i) > difficulty;
			}
			std::ostringstream oss (valid ? "true" : "false"); // IO forces compiler to not dismiss the variable
			auto total_time (std::chrono::duration_cast<std::chrono::nanoseconds> (std::chrono::steady_clock::now () - start).count ());
			uint64_t average (total_time / count);
			std::cout << "Average validation time: " << std::to_string (average) << " ns (" << std::to_string (static_cast<unsigned> (count * 1e9 / total_time)) << " validations/s)" << std::endl;
			return average;
		}
		else if (vm.count ("debug_opencl"))
		{
			nano::network_constants network_constants;
			bool error (false);
			nano::opencl_environment environment (error);
			if (!error)
			{
				unsigned short platform (0);
				auto platform_it = vm.find ("platform");
				if (platform_it != vm.end ())
				{
					try
					{
						platform = boost::lexical_cast<unsigned short> (platform_it->second.as<std::string> ());
					}
					catch (boost::bad_lexical_cast &)
					{
						std::cerr << "Invalid platform id\n";
						result = -1;
					}
				}
				unsigned short device (0);
				auto device_it = vm.find ("device");
				if (device_it != vm.end ())
				{
					try
					{
						device = boost::lexical_cast<unsigned short> (device_it->second.as<std::string> ());
					}
					catch (boost::bad_lexical_cast &)
					{
						std::cerr << "Invalid device id\n";
						result = -1;
					}
				}
				unsigned threads (1024 * 1024);
				auto threads_it = vm.find ("threads");
				if (threads_it != vm.end ())
				{
					try
					{
						threads = boost::lexical_cast<unsigned> (threads_it->second.as<std::string> ());
					}
					catch (boost::bad_lexical_cast &)
					{
						std::cerr << "Invalid threads count\n";
						result = -1;
					}
				}
				uint64_t difficulty (network_constants.publish_threshold);
				auto difficulty_it = vm.find ("difficulty");
				if (difficulty_it != vm.end ())
				{
					if (nano::from_string_hex (difficulty_it->second.as<std::string> (), difficulty))
					{
						std::cerr << "Invalid difficulty\n";
						result = -1;
					}
					else if (difficulty < network_constants.publish_threshold)
					{
						std::cerr << "Difficulty below publish threshold\n";
						result = -1;
					}
				}
				if (!result)
				{
					error |= platform >= environment.platforms.size ();
					if (!error)
					{
						error |= device >= environment.platforms[platform].devices.size ();
						if (!error)
						{
							nano::logger_mt logger;
							nano::opencl_config config (platform, device, threads);
							auto opencl (nano::opencl_work::create (true, config, logger));
							nano::work_pool work_pool (std::numeric_limits<unsigned>::max (), std::chrono::nanoseconds (0), opencl ? [&opencl](nano::root const & root_a, uint64_t difficulty_a, std::atomic<int> &) {
								return opencl->generate_work (root_a, difficulty_a);
							}
																																   : std::function<boost::optional<uint64_t> (nano::root const &, uint64_t, std::atomic<int> &)> (nullptr));
							nano::change_block block (0, 0, nano::keypair ().prv, 0, 0);
							std::cerr << boost::str (boost::format ("Starting OpenCL generation profiling. Platform: %1%. Device: %2%. Threads: %3%. Difficulty: %4$#x\n") % platform % device % threads % difficulty);
							for (uint64_t i (0); true; ++i)
							{
								block.hashables.previous.qwords[0] += 1;
								auto begin1 (std::chrono::high_resolution_clock::now ());
								block.block_work_set (*work_pool.generate (block.root (), difficulty));
								auto end1 (std::chrono::high_resolution_clock::now ());
								std::cerr << boost::str (boost::format ("%|1$ 12d|\n") % std::chrono::duration_cast<std::chrono::microseconds> (end1 - begin1).count ());
							}
						}
						else
						{
							std::cout << "Not available device id\n"
									  << std::endl;
							result = -1;
						}
					}
					else
					{
						std::cout << "Not available platform id\n"
								  << std::endl;
						result = -1;
					}
				}
			}
			else
			{
				std::cout << "Error initializing OpenCL" << std::endl;
				result = -1;
			}
		}
		else if (vm.count ("debug_output_last_backtrace_dump"))
		{
			if (boost::filesystem::exists ("nano_node_backtrace.dump"))
			{
				// There is a backtrace, so output the contents
				std::ifstream ifs ("nano_node_backtrace.dump");

				boost::stacktrace::stacktrace st = boost::stacktrace::stacktrace::from_dump (ifs);
				std::cout << "Latest crash backtrace:\n"
						  << st << std::endl;
			}
		}
		else if (vm.count ("debug_verify_profile"))
		{
			nano::keypair key;
			nano::uint256_union message;
			auto signature = nano::sign_message (key.prv, key.pub, message);
			auto begin (std::chrono::high_resolution_clock::now ());
			for (auto i (0u); i < 1000; ++i)
			{
				nano::validate_message (key.pub, message, signature);
			}
			auto end (std::chrono::high_resolution_clock::now ());
			std::cerr << "Signature verifications " << std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count () << std::endl;
		}
		else if (vm.count ("debug_verify_profile_batch"))
		{
			nano::keypair key;
			size_t batch_count (1000);
			nano::uint256_union message;
			nano::uint512_union signature (nano::sign_message (key.prv, key.pub, message));
			std::vector<unsigned char const *> messages (batch_count, message.bytes.data ());
			std::vector<size_t> lengths (batch_count, sizeof (message));
			std::vector<unsigned char const *> pub_keys (batch_count, key.pub.bytes.data ());
			std::vector<unsigned char const *> signatures (batch_count, signature.bytes.data ());
			std::vector<int> verifications;
			verifications.resize (batch_count);
			auto begin (std::chrono::high_resolution_clock::now ());
			nano::validate_message_batch (messages.data (), lengths.data (), pub_keys.data (), signatures.data (), batch_count, verifications.data ());
			auto end (std::chrono::high_resolution_clock::now ());
			std::cerr << "Batch signature verifications " << std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count () << std::endl;
		}
		else if (vm.count ("debug_profile_sign"))
		{
			std::cerr << "Starting blocks signing profiling\n";
			while (true)
			{
				nano::keypair key;
				nano::block_hash latest (0);
				auto begin1 (std::chrono::high_resolution_clock::now ());
				for (uint64_t balance (0); balance < 1000; ++balance)
				{
					nano::send_block send (latest, key.pub, balance, key.prv, key.pub, 0);
					latest = send.hash ();
				}
				auto end1 (std::chrono::high_resolution_clock::now ());
				std::cerr << boost::str (boost::format ("%|1$ 12d|\n") % std::chrono::duration_cast<std::chrono::microseconds> (end1 - begin1).count ());
			}
		}
		else if (vm.count ("debug_profile_process"))
		{
			nano::network_constants::set_active_network (nano::nano_networks::nano_test_network);
			nano::network_params test_params;
			nano::block_builder builder;
			size_t num_accounts (100000);
			size_t num_interations (5); // 100,000 * 5 * 2 = 1,000,000 blocks
			size_t max_blocks (2 * num_accounts * num_interations + num_accounts * 2); //  1,000,000 + 2* 100,000 = 1,200,000 blocks
			std::cerr << boost::str (boost::format ("Starting pregenerating %1% blocks\n") % max_blocks);
			nano::system system (1);
			nano::work_pool work (std::numeric_limits<unsigned>::max ());
			nano::logging logging;
			auto path (nano::unique_path ());
			logging.init (path);
			auto node (std::make_shared<nano::node> (system.io_ctx, 24001, path, system.alarm, logging, work));
			nano::block_hash genesis_latest (node->latest (test_params.ledger.test_genesis_key.pub));
			nano::uint128_t genesis_balance (std::numeric_limits<nano::uint128_t>::max ());
			// Generating keys
			std::vector<nano::keypair> keys (num_accounts);
			std::vector<nano::root> frontiers (num_accounts);
			std::vector<nano::uint128_t> balances (num_accounts, 1000000000);
			// Generating blocks
			std::deque<std::shared_ptr<nano::block>> blocks;
			for (auto i (0); i != num_accounts; ++i)
			{
				genesis_balance = genesis_balance - 1000000000;

				auto send = builder.state ()
								.account (test_params.ledger.test_genesis_key.pub)
								.previous (genesis_latest)
								.representative (test_params.ledger.test_genesis_key.pub)
								.balance (genesis_balance)
								.link (keys[i].pub)
								.sign (keys[i].prv, keys[i].pub)
								.work (*work.generate (genesis_latest))
								.build ();

				genesis_latest = send->hash ();
				blocks.push_back (std::move (send));

				auto open = builder.state ()
								.account (keys[i].pub)
								.previous (0)
								.representative (keys[i].pub)
								.balance (balances[i])
								.link (genesis_latest)
								.sign (test_params.ledger.test_genesis_key.prv, test_params.ledger.test_genesis_key.pub)
								.work (*work.generate (keys[i].pub))
								.build ();

				frontiers[i] = open->hash ();
				blocks.push_back (std::move (open));
			}
			for (auto i (0); i != num_interations; ++i)
			{
				for (auto j (0); j != num_accounts; ++j)
				{
					size_t other (num_accounts - j - 1);
					// Sending to other account
					--balances[j];

					auto send = builder.state ()
									.account (keys[j].pub)
									.previous (frontiers[j])
									.representative (keys[j].pub)
									.balance (balances[j])
									.link (keys[other].pub)
									.sign (keys[j].prv, keys[j].pub)
									.work (*work.generate (frontiers[j]))
									.build ();

					frontiers[j] = send->hash ();
					blocks.push_back (std::move (send));
					// Receiving
					++balances[other];

					auto receive = builder.state ()
									   .account (keys[other].pub)
									   .previous (frontiers[other])
									   .representative (keys[other].pub)
									   .balance (balances[other])
									   .link (static_cast<nano::block_hash const &> (frontiers[j]))
									   .sign (keys[other].prv, keys[other].pub)
									   .work (*work.generate (frontiers[other]))
									   .build ();

					frontiers[other] = receive->hash ();
					blocks.push_back (std::move (receive));
				}
			}
			// Processing blocks
			std::cerr << boost::str (boost::format ("Starting processing %1% active blocks\n") % max_blocks);
			auto begin (std::chrono::high_resolution_clock::now ());
			while (!blocks.empty ())
			{
				auto block (blocks.front ());
				node->process_active (block);
				blocks.pop_front ();
			}
			uint64_t block_count (0);
			while (block_count < max_blocks + 1)
			{
				std::this_thread::sleep_for (std::chrono::milliseconds (100));
				auto transaction (node->store.tx_begin_read ());
				block_count = node->store.block_count (transaction).sum ();
			}
			auto end (std::chrono::high_resolution_clock::now ());
			auto time (std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count ());
			node->stop ();
			std::cerr << boost::str (boost::format ("%|1$ 12d| us \n%2% blocks per second\n") % time % (max_blocks * 1000000 / time));
		}
		else if (vm.count ("debug_profile_votes"))
		{
			nano::network_constants::set_active_network (nano::nano_networks::nano_test_network);
			nano::network_params test_params;
			nano::block_builder builder;
			size_t num_elections (40000);
			size_t num_representatives (25);
			size_t max_votes (num_elections * num_representatives); // 40,000 * 25 = 1,000,000 votes
			std::cerr << boost::str (boost::format ("Starting pregenerating %1% votes\n") % max_votes);
			nano::system system (1);
			nano::work_pool work (std::numeric_limits<unsigned>::max ());
			nano::logging logging;
			auto path (nano::unique_path ());
			logging.init (path);
			auto node (std::make_shared<nano::node> (system.io_ctx, 24001, path, system.alarm, logging, work));
			nano::block_hash genesis_latest (node->latest (test_params.ledger.test_genesis_key.pub));
			nano::uint128_t genesis_balance (std::numeric_limits<nano::uint128_t>::max ());
			// Generating keys
			std::vector<nano::keypair> keys (num_representatives);
			nano::uint128_t balance ((node->config.online_weight_minimum.number () / num_representatives) + 1);
			for (auto i (0); i != num_representatives; ++i)
			{
				auto transaction (node->store.tx_begin_write ());
				genesis_balance = genesis_balance - balance;

				auto send = builder.state ()
								.account (test_params.ledger.test_genesis_key.pub)
								.previous (genesis_latest)
								.representative (test_params.ledger.test_genesis_key.pub)
								.balance (genesis_balance)
								.link (keys[i].pub)
								.sign (test_params.ledger.test_genesis_key.prv, test_params.ledger.test_genesis_key.pub)
								.work (*work.generate (genesis_latest))
								.build ();

				genesis_latest = send->hash ();
				node->ledger.process (transaction, *send);

				auto open = builder.state ()
								.account (keys[i].pub)
								.previous (0)
								.representative (keys[i].pub)
								.balance (balance)
								.link (genesis_latest)
								.sign (keys[i].prv, keys[i].pub)
								.work (*work.generate (keys[i].pub))
								.build ();

				node->ledger.process (transaction, *open);
			}
			// Generating blocks
			std::deque<std::shared_ptr<nano::block>> blocks;
			for (auto i (0); i != num_elections; ++i)
			{
				genesis_balance = genesis_balance - 1;
				nano::keypair destination;

				auto send = builder.state ()
								.account (test_params.ledger.test_genesis_key.pub)
								.previous (genesis_latest)
								.representative (test_params.ledger.test_genesis_key.pub)
								.balance (genesis_balance)
								.link (destination.pub)
								.sign (test_params.ledger.test_genesis_key.prv, test_params.ledger.test_genesis_key.pub)
								.work (*work.generate (genesis_latest))
								.build ();

				genesis_latest = send->hash ();
				blocks.push_back (std::move (send));
			}
			// Generating votes
			std::deque<std::shared_ptr<nano::vote>> votes;
			for (auto j (0); j != num_representatives; ++j)
			{
				uint64_t sequence (1);
				for (auto & i : blocks)
				{
					auto vote (std::make_shared<nano::vote> (keys[j].pub, keys[j].prv, sequence, std::vector<nano::block_hash> (1, i->hash ())));
					votes.push_back (vote);
					sequence++;
				}
			}
			// Processing block & start elections
			while (!blocks.empty ())
			{
				auto block (blocks.front ());
				node->process_active (block);
				blocks.pop_front ();
			}
			node->block_processor.flush ();
			// Processing votes
			std::cerr << boost::str (boost::format ("Starting processing %1% votes\n") % max_votes);
			auto begin (std::chrono::high_resolution_clock::now ());
			while (!votes.empty ())
			{
				auto vote (votes.front ());
				auto channel (std::make_shared<nano::transport::channel_udp> (node->network.udp_channels, node->network.endpoint (), node->network_params.protocol.protocol_version));
				node->vote_processor.vote (vote, channel);
				votes.pop_front ();
			}
			while (!node->active.empty ())
			{
				std::this_thread::sleep_for (std::chrono::milliseconds (100));
			}
			auto end (std::chrono::high_resolution_clock::now ());
			auto time (std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count ());
			node->stop ();
			std::cerr << boost::str (boost::format ("%|1$ 12d| us \n%2% votes per second\n") % time % (max_votes * 1000000 / time));
		}
		else if (vm.count ("debug_random_feed"))
		{
			/*
			 * This command redirects an infinite stream of bytes from the random pool to standard out.
			 * The result can be fed into various tools for testing RNGs and entropy pools.
			 *
			 * Example, running the entire dieharder test suite:
			 *
			 *   ./nano_node --debug_random_feed | dieharder -a -g 200
			 */
			nano::raw_key seed;
			for (;;)
			{
				nano::random_pool::generate_block (seed.data.bytes.data (), seed.data.bytes.size ());
				std::cout.write (reinterpret_cast<const char *> (seed.data.bytes.data ()), seed.data.bytes.size ());
			}
		}
		else if (vm.count ("debug_rpc"))
		{
			std::string rpc_input_l;
			std::ostringstream command_l;
			while (std::cin >> rpc_input_l)
			{
				command_l << rpc_input_l;
			}

			auto response_handler_l ([](std::string const & response_a) {
				std::cout << response_a;
				// Terminate as soon as we have the result, even if background threads (like work generation) are running.
				std::exit (0);
			});

			nano::inactive_node inactive_node_l (data_path);
			nano::node_rpc_config config;
			nano::ipc::ipc_server server (*inactive_node_l.node, config);
			nano::json_handler handler_l (*inactive_node_l.node, config, command_l.str (), response_handler_l);
			handler_l.process_request ();
		}
		else if (vm.count ("debug_validate_blocks"))
		{
			nano::inactive_node node (data_path);
			auto transaction (node.node->store.tx_begin_read ());
			std::cout << boost::str (boost::format ("Performing blocks hash, signature, work validation...\n"));
			size_t count (0);
			uint64_t block_count (0);
			for (auto i (node.node->store.latest_begin (transaction)), n (node.node->store.latest_end ()); i != n; ++i)
			{
				++count;
				if ((count % 20000) == 0)
				{
					std::cout << boost::str (boost::format ("%1% accounts validated\n") % count);
				}
				nano::account_info const & info (i->second);
				nano::account const & account (i->first);
				uint64_t confirmation_height;
				node.node->store.confirmation_height_get (transaction, account, confirmation_height);

				if (confirmation_height > info.block_count)
				{
					std::cerr << "Confirmation height " << confirmation_height << " greater than block count " << info.block_count << " for account: " << account.to_account () << std::endl;
				}

				auto hash (info.open_block);
				nano::block_hash calculated_hash (0);
				nano::block_sideband sideband;
				auto block (node.node->store.block_get (transaction, hash, &sideband)); // Block data
				uint64_t height (0);
				uint64_t previous_timestamp (0);
				nano::account calculated_representative (0);
				while (!hash.is_zero () && block != nullptr)
				{
					++block_count;
					// Check for state & open blocks if account field is correct
					if (block->type () == nano::block_type::open || block->type () == nano::block_type::state)
					{
						if (block->account () != account)
						{
							std::cerr << boost::str (boost::format ("Incorrect account field for block %1%\n") % hash.to_string ());
						}
					}
					// Check if sideband account is correct
					else if (sideband.account != account)
					{
						std::cerr << boost::str (boost::format ("Incorrect sideband account for block %1%\n") % hash.to_string ());
					}
					// Check if previous field is correct
					if (calculated_hash != block->previous ())
					{
						std::cerr << boost::str (boost::format ("Incorrect previous field for block %1%\n") % hash.to_string ());
					}
					// Check if previous & type for open blocks are correct
					if (height == 0 && !block->previous ().is_zero ())
					{
						std::cerr << boost::str (boost::format ("Incorrect previous for open block %1%\n") % hash.to_string ());
					}
					if (height == 0 && block->type () != nano::block_type::open && block->type () != nano::block_type::state)
					{
						std::cerr << boost::str (boost::format ("Incorrect type for open block %1%\n") % hash.to_string ());
					}
					// Check if block data is correct (calculating hash)
					calculated_hash = block->hash ();
					if (calculated_hash != hash)
					{
						std::cerr << boost::str (boost::format ("Invalid data inside block %1% calculated hash: %2%\n") % hash.to_string () % calculated_hash.to_string ());
					}
					// Check if block signature is correct
					if (validate_message (account, hash, block->block_signature ()))
					{
						bool invalid (true);
						// Epoch blocks
						if (block->type () == nano::block_type::state)
						{
							auto & state_block (static_cast<nano::state_block &> (*block.get ()));
							nano::amount prev_balance (0);
							if (!state_block.hashables.previous.is_zero ())
							{
								prev_balance = node.node->ledger.balance (transaction, state_block.hashables.previous);
							}
							if (node.node->ledger.is_epoch_link (state_block.hashables.link) && state_block.hashables.balance == prev_balance)
							{
								invalid = validate_message (node.node->ledger.epoch_signer (block->link ()), hash, block->block_signature ());
							}
						}
						if (invalid)
						{
							std::cerr << boost::str (boost::format ("Invalid signature for block %1%\n") % hash.to_string ());
						}
					}
					// Check if block work value is correct
					if (nano::work_validate (*block.get ()))
					{
						std::cerr << boost::str (boost::format ("Invalid work for block %1% value: %2%\n") % hash.to_string () % nano::to_string_hex (block->block_work ()));
					}
					// Check if sideband height is correct
					++height;
					if (sideband.height != height)
					{
						std::cerr << boost::str (boost::format ("Incorrect sideband height for block %1%. Sideband: %2%. Expected: %3%\n") % hash.to_string () % sideband.height % height);
					}
					// Check if sideband timestamp is after previous timestamp
					if (sideband.timestamp < previous_timestamp)
					{
						std::cerr << boost::str (boost::format ("Incorrect sideband timestamp for block %1%\n") % hash.to_string ());
					}
					previous_timestamp = sideband.timestamp;
					// Calculate representative block
					if (block->type () == nano::block_type::open || block->type () == nano::block_type::change || block->type () == nano::block_type::state)
					{
						calculated_representative = block->representative ();
					}
					// Retrieving successor block hash
					hash = node.node->store.block_successor (transaction, hash);
					// Retrieving block data
					if (!hash.is_zero ())
					{
						block = node.node->store.block_get (transaction, hash, &sideband);
					}
				}
				// Check if required block exists
				if (!hash.is_zero () && block == nullptr)
				{
					std::cerr << boost::str (boost::format ("Required block in account %1% chain was not found in ledger: %2%\n") % account.to_account () % hash.to_string ());
				}
				// Check account block count
				if (info.block_count != height)
				{
					std::cerr << boost::str (boost::format ("Incorrect block count for account %1%. Actual: %2%. Expected: %3%\n") % account.to_account () % height % info.block_count);
				}
				// Check account head block (frontier)
				if (info.head != calculated_hash)
				{
					std::cerr << boost::str (boost::format ("Incorrect frontier for account %1%. Actual: %2%. Expected: %3%\n") % account.to_account () % calculated_hash.to_string () % info.head.to_string ());
				}
				// Check account representative block
				if (info.representative != calculated_representative)
				{
					std::cerr << boost::str (boost::format ("Incorrect representative for account %1%. Actual: %2%. Expected: %3%\n") % account.to_account () % calculated_representative.to_string () % info.representative.to_string ());
				}
			}
			std::cout << boost::str (boost::format ("%1% accounts validated\n") % count);
			// Validate total block count
			auto ledger_block_count (node.node->store.block_count (transaction).sum ());
			if (block_count != ledger_block_count)
			{
				std::cerr << boost::str (boost::format ("Incorrect total block count. Blocks validated %1%. Block count in database: %2%\n") % block_count % ledger_block_count);
			}
			// Validate pending blocks
			count = 0;
			for (auto i (node.node->store.pending_begin (transaction)), n (node.node->store.pending_end ()); i != n; ++i)
			{
				++count;
				if ((count % 200000) == 0)
				{
					std::cout << boost::str (boost::format ("%1% pending blocks validated\n") % count);
				}
				nano::pending_key const & key (i->first);
				nano::pending_info const & info (i->second);
				// Check block existance
				auto block (node.node->store.block_get (transaction, key.hash));
				if (block == nullptr)
				{
					std::cerr << boost::str (boost::format ("Pending block does not exist %1%\n") % key.hash.to_string ());
				}
				else
				{
					// Check if pending destination is correct
					nano::account destination (0);
					if (auto state = dynamic_cast<nano::state_block *> (block.get ()))
					{
						if (node.node->ledger.is_send (transaction, *state))
						{
							destination = state->hashables.link;
						}
					}
					else if (auto send = dynamic_cast<nano::send_block *> (block.get ()))
					{
						destination = send->hashables.destination;
					}
					else
					{
						std::cerr << boost::str (boost::format ("Incorrect type for pending block %1%\n") % key.hash.to_string ());
					}
					if (key.account != destination)
					{
						std::cerr << boost::str (boost::format ("Incorrect destination for pending block %1%\n") % key.hash.to_string ());
					}
					// Check if pending source is correct
					auto account (node.node->ledger.account (transaction, key.hash));
					if (info.source != account)
					{
						std::cerr << boost::str (boost::format ("Incorrect source for pending block %1%\n") % key.hash.to_string ());
					}
					// Check if pending amount is correct
					auto amount (node.node->ledger.amount (transaction, key.hash));
					if (info.amount != amount)
					{
						std::cerr << boost::str (boost::format ("Incorrect amount for pending block %1%\n") % key.hash.to_string ());
					}
				}
			}
			std::cout << boost::str (boost::format ("%1% pending blocks validated\n") % count);
		}
		else if (vm.count ("debug_profile_bootstrap"))
		{
			nano::inactive_node node2 (nano::unique_path (), 24001);
			nano::update_flags (node2.node->flags, vm);
			nano::genesis genesis;
			auto begin (std::chrono::high_resolution_clock::now ());
			uint64_t block_count (0);
			size_t count (0);
			{
				nano::inactive_node node (data_path, 24000);
				auto transaction (node.node->store.tx_begin_read ());
				block_count = node.node->store.block_count (transaction).sum ();
				std::cout << boost::str (boost::format ("Performing bootstrap emulation, %1% blocks in ledger...") % block_count) << std::endl;
				for (auto i (node.node->store.latest_begin (transaction)), n (node.node->store.latest_end ()); i != n; ++i)
				{
					nano::account const & account (i->first);
					nano::account_info const & info (i->second);
					auto hash (info.head);
					while (!hash.is_zero ())
					{
						// Retrieving block data
						auto block (node.node->store.block_get (transaction, hash));
						if (block != nullptr)
						{
							++count;
							if ((count % 100000) == 0)
							{
								std::cout << boost::str (boost::format ("%1% blocks retrieved") % count) << std::endl;
							}
							nano::unchecked_info unchecked_info (block, account, 0, nano::signature_verification::unknown);
							node2.node->block_processor.add (unchecked_info);
							// Retrieving previous block hash
							hash = block->previous ();
						}
					}
				}
			}
			count = 0;
			uint64_t block_count_2 (0);
			while (block_count_2 != block_count)
			{
				std::this_thread::sleep_for (std::chrono::seconds (1));
				auto transaction_2 (node2.node->store.tx_begin_read ());
				block_count_2 = node2.node->store.block_count (transaction_2).sum ();
				if ((count % 60) == 0)
				{
					std::cout << boost::str (boost::format ("%1% (%2%) blocks processed") % block_count_2 % node2.node->store.unchecked_count (transaction_2)) << std::endl;
				}
				count++;
			}
			auto end (std::chrono::high_resolution_clock::now ());
			auto time (std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count ());
			auto seconds (time / 1000000);
			nano::remove_temporary_directories ();
			std::cout << boost::str (boost::format ("%|1$ 12d| seconds \n%2% blocks per second") % seconds % (block_count / seconds)) << std::endl;
		}
		else if (vm.count ("debug_peers"))
		{
			nano::inactive_node node (data_path);
			auto transaction (node.node->store.tx_begin_read ());

			for (auto i (node.node->store.peers_begin (transaction)), n (node.node->store.peers_end ()); i != n; ++i)
			{
				std::cout << boost::str (boost::format ("%1%\n") % nano::endpoint (boost::asio::ip::address_v6 (i->first.address_bytes ()), i->first.port ()));
			}
		}
		else if (vm.count ("debug_cemented_block_count"))
		{
			auto node_flags = nano::inactive_node_flag_defaults ();
			node_flags.cache_cemented_count_from_frontiers = true;
			nano::inactive_node node (data_path, 24000, node_flags);
			std::cout << "Total cemented block count: " << node.node->ledger.cemented_count << std::endl;
		}
		else if (vm.count ("debug_stacktrace"))
		{
			std::cout << boost::stacktrace::stacktrace ();
		}
		else if (vm.count ("debug_sys_logging"))
		{
#ifdef BOOST_WINDOWS
			if (!nano::event_log_reg_entry_exists () && !nano::is_windows_elevated ())
			{
				std::cerr << "The event log requires the HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Services\\EventLog\\Nano\\Nano registry entry, run again as administator to create it.\n";
				return 1;
			}
#endif
			nano::inactive_node node (data_path);
			node.node->logger.always_log (nano::severity_level::error, "Testing system logger");
		}
		else if (vm.count ("debug_account_versions"))
		{
			nano::inactive_node node (data_path);

			auto transaction (node.node->store.tx_begin_read ());
			std::vector<std::unordered_set<nano::account>> opened_account_versions (nano::normalized_epoch (nano::epoch::max));

			// Cache the accounts in a collection to make searching quicker against unchecked keys. Group by epoch
			for (auto i (node.node->store.latest_begin (transaction)), n (node.node->store.latest_end ()); i != n; ++i)
			{
				auto const & account (i->first);
				auto const & account_info (i->second);

				// Epoch 0 will be index 0 for instance
				auto epoch_idx = nano::normalized_epoch (account_info.epoch ());
				opened_account_versions[epoch_idx].emplace (account);
			}

			// Iterate all pending blocks and collect the highest version for each unopened account
			std::unordered_map<nano::account, std::underlying_type_t<nano::epoch>> unopened_highest_pending;
			for (auto i (node.node->store.pending_begin (transaction)), n (node.node->store.pending_end ()); i != n; ++i)
			{
				nano::pending_key const & key (i->first);
				nano::pending_info const & info (i->second);
				// clang-format off
				auto & account = key.account;
				auto exists = std::any_of (opened_account_versions.begin (), opened_account_versions.end (), [&account](auto const & account_version) {
					return account_version.find (account) != account_version.end ();
				});
				// clang-format on
				if (!exists)
				{
					// This is an unopened account, store the highest pending version
					auto it = unopened_highest_pending.find (key.account);
					auto epoch = nano::normalized_epoch (info.epoch);
					if (it != unopened_highest_pending.cend ())
					{
						// Found it, compare against existing value
						if (epoch > it->second)
						{
							it->second = epoch;
						}
					}
					else
					{
						// New unopened account
						unopened_highest_pending.emplace (key.account, epoch);
					}
				}
			}

			auto output_account_version_number = [](auto version, auto num_accounts) {
				std::cout << "Account version " << version << " num accounts: " << num_accounts << "\n";
			};

			// Output total version counts for the opened accounts
			std::cout << "Opened accounts:\n";
			for (auto i = 0u; i < opened_account_versions.size (); ++i)
			{
				output_account_version_number (i, opened_account_versions[i].size ());
			}

			// Accumulate the version numbers for the highest pending epoch for each unopened account.
			std::vector<size_t> unopened_account_version_totals (nano::normalized_epoch (nano::epoch::max));
			for (auto & pair : unopened_highest_pending)
			{
				++unopened_account_version_totals[pair.second];
			}

			// Output total version counts for the unopened accounts
			std::cout << "\nUnopened accounts:\n";
			for (auto i = 0u; i < unopened_account_version_totals.size (); ++i)
			{
				output_account_version_number (i, unopened_account_version_totals[i]);
			}
		}
		else if (vm.count ("version"))
		{
			std::cout << "Version " << NANO_VERSION_STRING << "\n"
					  << "Build Info " << BUILD_INFO << std::endl;
		}
		else
		{
			std::cout << description << std::endl;
			result = -1;
		}
	}
	return result;
}
