#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/cli.hpp>
#include <nano/lib/utility.hpp>
#include <nano/nano_node/daemon.hpp>
#include <nano/node/cli.hpp>
#include <nano/node/daemonconfig.hpp>
#include <nano/node/ipc/ipc_server.hpp>
#include <nano/node/json_handler.hpp>
#include <nano/node/node.hpp>

#include <boost/dll/runtime_symbol_info.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include <numeric>
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

namespace
{
class uint64_from_hex // For use with boost::lexical_cast to read hexadecimal strings
{
public:
	uint64_t value;
};
std::istream & operator>> (std::istream & in, uint64_from_hex & out_val);

class address_library_pair
{
public:
	uint64_t address;
	std::string library;

	address_library_pair (uint64_t address, std::string library);
	bool operator< (const address_library_pair & other) const;
	bool operator== (const address_library_pair & other) const;
};
}

int main (int argc, char * const * argv)
{
	nano::set_umask ();
	nano::node_singleton_memory_pool_purge_guard memory_pool_cleanup_guard;
	boost::program_options::options_description description ("Command line options");
	// clang-format off
	description.add_options ()
		("help", "Print out options")
		("version", "Prints out version")
		("config", boost::program_options::value<std::vector<nano::config_key_value_pair>>()->multitoken(), "Pass node configuration values. This takes precedence over any values in the configuration file. This option can be repeated multiple times.")
		("daemon", "Start node daemon")
		("compare_rep_weights", "Display a summarized comparison between the hardcoded bootstrap weights and representative weights from the ledger. Full comparison is output to logs")
		("debug_block_count", "Display the number of blocks")
		("debug_bootstrap_generate", "Generate bootstrap sequence of blocks")
		("debug_dump_frontier_unchecked_dependents", "Dump frontiers which have matching unchecked keys")
		("debug_dump_trended_weight", "Dump trended weights table")
		("debug_dump_representatives", "List representatives and weights")
		("debug_account_count", "Display the number of accounts")
		("debug_profile_generate", "Profile work generation")
		("debug_profile_validate", "Profile work validation")
		("debug_opencl", "OpenCL work generation")
		("debug_profile_kdf", "Profile kdf function")
		("debug_output_last_backtrace_dump", "Displays the contents of the latest backtrace in the event of a nano_node crash")
		("debug_generate_crash_report", "Consolidates the nano_node_backtrace.dump file. Requires addr2line installed on Linux")
		("debug_sys_logging", "Test the system logger")
		("debug_verify_profile", "Profile signature verification")
		("debug_verify_profile_batch", "Profile batch signature verification")
		("debug_profile_bootstrap", "Profile bootstrap style blocks processing (at least 10GB of free storage space required)")
		("debug_profile_sign", "Profile signature generation")
		("debug_profile_process", "Profile active blocks processing (only for nano_dev_network)")
		("debug_profile_votes", "Profile votes processing (only for nano_dev_network)")
		("debug_profile_frontiers_confirmation", "Profile frontiers confirmation speed (only for nano_dev_network)")
		("debug_random_feed", "Generates output to RNG test suites")
		("debug_rpc", "Read an RPC command from stdin and invoke it. Network operations will have no effect.")
		("debug_peers", "Display peer IPv6:port connections")
		("debug_cemented_block_count", "Displays the number of cemented (confirmed) blocks")
		("debug_stacktrace", "Display an example stacktrace")
		("debug_account_versions", "Display the total counts of each version for all accounts (including unpocketed)")
		("debug_unconfirmed_frontiers", "Displays the account, height (sorted), frontier and cemented frontier for all accounts which are not fully confirmed")
		("validate_blocks,debug_validate_blocks", "Check all blocks for correct hash, signature, work value")
		("debug_prune", "Prune accounts up to last confirmed blocks (EXPERIMENTAL)")
		("platform", boost::program_options::value<std::string> (), "Defines the <platform> for OpenCL commands")
		("device", boost::program_options::value<std::string> (), "Defines <device> for OpenCL command")
		("threads", boost::program_options::value<std::string> (), "Defines <threads> count for various commands")
		("difficulty", boost::program_options::value<std::string> (), "Defines <difficulty> for OpenCL command, HEX")
		("multiplier", boost::program_options::value<std::string> (), "Defines <multiplier> for work generation. Overrides <difficulty>")
		("count", boost::program_options::value<std::string> (), "Defines <count> for various commands")
		("pow_sleep_interval", boost::program_options::value<std::string> (), "Defines the amount to sleep inbetween each pow calculation attempt")
		("address_column", boost::program_options::value<std::string> (), "Defines which column the addresses are located, 0 indexed (check --debug_output_last_backtrace_dump output)")
		("silent", "Silent command execution");
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
				std::cerr << flags_ec.message () << std::endl;
				std::exit (1);
			}
			daemon.run (data_path, flags);
		}
		else if (vm.count ("compare_rep_weights"))
		{
			if (!nano::network_constants ().is_dev_network ())
			{
				auto node_flags = nano::inactive_node_flag_defaults ();
				nano::update_flags (node_flags, vm);
				node_flags.generate_cache.reps = true;
				nano::inactive_node inactive_node (data_path, node_flags);
				auto node = inactive_node.node;

				auto const bootstrap_weights = node->get_bootstrap_weights ();
				auto const & hardcoded = bootstrap_weights.second;
				auto const hardcoded_height = bootstrap_weights.first;
				auto const ledger_unfiltered = node->ledger.cache.rep_weights.get_rep_amounts ();
				auto const ledger_height = node->ledger.cache.block_count.load ();

				auto get_total = [] (decltype (bootstrap_weights.second) const & reps) -> nano::uint128_union {
					return std::accumulate (reps.begin (), reps.end (), nano::uint128_t{ 0 }, [] (auto sum, auto const & rep) { return sum + rep.second; });
				};

				// Hardcoded weights are filtered to a cummulative weight of 99%, need to do the same for ledger weights
				std::remove_const_t<decltype (ledger_unfiltered)> ledger;
				{
					std::vector<std::pair<nano::account, nano::uint128_t>> sorted;
					sorted.reserve (ledger_unfiltered.size ());
					std::copy (ledger_unfiltered.begin (), ledger_unfiltered.end (), std::back_inserter (sorted));
					std::sort (sorted.begin (), sorted.end (), [] (auto const & left, auto const & right) { return left.second > right.second; });
					auto const total_unfiltered = get_total (ledger_unfiltered);
					nano::uint128_t sum{ 0 };
					auto target = (total_unfiltered.number () / 100) * 99;
					for (auto i (sorted.begin ()), n (sorted.end ()); i != n && sum <= target; sum += i->second, ++i)
					{
						ledger.insert (*i);
					}
				}

				auto const total_ledger = get_total (ledger);
				auto const total_hardcoded = get_total (hardcoded);

				struct mismatched_t
				{
					nano::account rep;
					nano::uint128_union hardcoded;
					nano::uint128_union ledger;
					nano::uint128_union diff;
					std::string get_entry () const
					{
						return boost::str (boost::format ("representative %1% hardcoded %2% ledger %3% mismatch %4%")
						% rep.to_account () % hardcoded.format_balance (nano::Mxrb_ratio, 0, true) % ledger.format_balance (nano::Mxrb_ratio, 0, true) % diff.format_balance (nano::Mxrb_ratio, 0, true));
					}
				};

				std::vector<mismatched_t> mismatched;
				mismatched.reserve (hardcoded.size ());
				std::transform (hardcoded.begin (), hardcoded.end (), std::back_inserter (mismatched), [&ledger] (auto const & rep) {
					auto ledger_rep (ledger.find (rep.first));
					nano::uint128_t ledger_weight = (ledger_rep == ledger.end () ? 0 : ledger_rep->second);
					auto absolute = ledger_weight > rep.second ? ledger_weight - rep.second : rep.second - ledger_weight;
					return mismatched_t{ rep.first, rep.second, ledger_weight, absolute };
				});

				// Sort by descending difference
				std::sort (mismatched.begin (), mismatched.end (), [] (mismatched_t const & left, mismatched_t const & right) { return left.diff > right.diff; });

				nano::uint128_union const mismatch_total = std::accumulate (mismatched.begin (), mismatched.end (), nano::uint128_t{ 0 }, [] (auto sum, mismatched_t const & sample) { return sum + sample.diff.number (); });
				nano::uint128_union const mismatch_mean = mismatch_total.number () / mismatched.size ();

				nano::uint512_union mismatch_variance = std::accumulate (mismatched.begin (), mismatched.end (), nano::uint512_t (0), [M = mismatch_mean.number (), N = mismatched.size ()] (nano::uint512_t sum, mismatched_t const & sample) {
					auto x = sample.diff.number ();
					nano::uint512_t const mean_diff = x > M ? x - M : M - x;
					nano::uint512_t const sqr = mean_diff * mean_diff;
					return sum + sqr;
				})
				/ mismatched.size ();

				nano::uint128_union const mismatch_stddev = nano::narrow_cast<nano::uint128_t> (boost::multiprecision::sqrt (mismatch_variance.number ()));

				auto const outlier_threshold = std::max (nano::Gxrb_ratio, mismatch_mean.number () + 1 * mismatch_stddev.number ());
				decltype (mismatched) outliers;
				std::copy_if (mismatched.begin (), mismatched.end (), std::back_inserter (outliers), [outlier_threshold] (mismatched_t const & sample) {
					return sample.diff > outlier_threshold;
				});

				auto const newcomer_threshold = std::max (nano::Gxrb_ratio, mismatch_mean.number ());
				std::vector<std::pair<nano::account, nano::uint128_t>> newcomers;
				std::copy_if (ledger.begin (), ledger.end (), std::back_inserter (newcomers), [&hardcoded] (auto const & rep) {
					return !hardcoded.count (rep.first) && rep.second;
				});

				// Sort by descending weight
				std::sort (newcomers.begin (), newcomers.end (), [] (auto const & left, auto const & right) { return left.second > right.second; });

				auto newcomer_entry = [] (auto const & rep) {
					return boost::str (boost::format ("representative %1% hardcoded --- ledger %2%") % rep.first.to_account () % nano::uint128_union (rep.second).format_balance (nano::Mxrb_ratio, 0, true));
				};

				std::cout << boost::str (boost::format ("hardcoded weight %1% Mnano at %2% blocks\nledger weight %3% Mnano at %4% blocks\nmismatched\n\tsamples %5%\n\ttotal %6% Mnano\n\tmean %7% Mnano\n\tsigma %8% Mnano\n")
				% total_hardcoded.format_balance (nano::Mxrb_ratio, 0, true)
				% hardcoded_height
				% total_ledger.format_balance (nano::Mxrb_ratio, 0, true)
				% ledger_height
				% mismatched.size ()
				% mismatch_total.format_balance (nano::Mxrb_ratio, 0, true)
				% mismatch_mean.format_balance (nano::Mxrb_ratio, 0, true)
				% mismatch_stddev.format_balance (nano::Mxrb_ratio, 0, true));

				if (!outliers.empty ())
				{
					std::cout << "outliers\n";
					for (auto const & outlier : outliers)
					{
						std::cout << '\t' << outlier.get_entry () << '\n';
					}
				}

				if (!newcomers.empty ())
				{
					std::cout << "newcomers\n";
					for (auto const & newcomer : newcomers)
					{
						if (newcomer.second > newcomer_threshold)
						{
							std::cout << '\t' << newcomer_entry (newcomer) << '\n';
						}
					}
				}

				// Log more data
				auto const log_threshold = nano::Gxrb_ratio;
				for (auto const & sample : mismatched)
				{
					if (sample.diff > log_threshold)
					{
						node->logger.always_log (sample.get_entry ());
					}
				}
				for (auto const & newcomer : newcomers)
				{
					if (newcomer.second > log_threshold)
					{
						node->logger.always_log (newcomer_entry (newcomer));
					}
				}
			}
			else
			{
				std::cout << "Not available for the test network" << std::endl;
				result = -1;
			}
		}
		else if (vm.count ("debug_block_count"))
		{
			auto node_flags = nano::inactive_node_flag_defaults ();
			nano::update_flags (node_flags, vm);
			node_flags.generate_cache.block_count = true;
			nano::inactive_node inactive_node (data_path, node_flags);
			auto node = inactive_node.node;
			std::cout << boost::str (boost::format ("Block count: %1%\n") % node->ledger.cache.block_count);
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
					std::cout << "Genesis: " << genesis.prv.to_string () << "\n"
							  << "Public: " << genesis.pub.to_string () << "\n"
							  << "Account: " << genesis.pub.to_account () << "\n";
					nano::keypair landing;
					std::cout << "Landing: " << landing.prv.to_string () << "\n"
							  << "Public: " << landing.pub.to_string () << "\n"
							  << "Account: " << landing.pub.to_account () << "\n";
					for (auto i (0); i != 32; ++i)
					{
						nano::keypair rep;
						std::cout << "Rep" << i << ": " << rep.prv.to_string () << "\n"
								  << "Public: " << rep.pub.to_string () << "\n"
								  << "Account: " << rep.pub.to_account () << "\n";
					}
					nano::network_constants network_constants;
					nano::uint128_t balance (std::numeric_limits<nano::uint128_t>::max ());
					nano::open_block genesis_block (reinterpret_cast<const nano::block_hash &> (genesis.pub), genesis.pub, genesis.pub, genesis.prv, genesis.pub, *work.generate (nano::work_version::work_1, genesis.pub, network_constants.publish_thresholds.epoch_1));
					std::cout << genesis_block.to_json ();
					std::cout.flush ();
					nano::block_hash previous (genesis_block.hash ());
					for (auto i (0); i != 8; ++i)
					{
						nano::uint128_t yearly_distribution (nano::uint128_t (1) << (127 - (i == 7 ? 6 : i)));
						auto weekly_distribution (yearly_distribution / 52);
						for (auto j (0); j != 52; ++j)
						{
							debug_assert (balance > weekly_distribution);
							balance = balance < (weekly_distribution * 2) ? 0 : balance - weekly_distribution;
							nano::send_block send (previous, landing.pub, balance, genesis.prv, genesis.pub, *work.generate (nano::work_version::work_1, previous, network_constants.publish_thresholds.epoch_1));
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
		else if (vm.count ("debug_dump_trended_weight"))
		{
			auto inactive_node = nano::default_inactive_node (data_path, vm);
			auto node = inactive_node->node;
			auto current (node->online_reps.trended ());
			std::cout << boost::str (boost::format ("Trended Weight %1%\n") % current);
			auto transaction (node->store.tx_begin_read ());
			for (auto i (node->store.online_weight.begin (transaction)), n (node->store.online_weight.end ()); i != n; ++i)
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
			nano::update_flags (node_flags, vm);
			node_flags.generate_cache.reps = true;
			nano::inactive_node inactive_node (data_path, node_flags);
			auto node = inactive_node.node;
			auto transaction (node->store.tx_begin_read ());
			nano::uint128_t total;
			auto rep_amounts = node->ledger.cache.rep_weights.get_rep_amounts ();
			std::map<nano::account, nano::uint128_t> ordered_reps (rep_amounts.begin (), rep_amounts.end ());
			for (auto const & rep : ordered_reps)
			{
				total += rep.second;
				std::cout << boost::str (boost::format ("%1% %2% %3%\n") % rep.first.to_account () % rep.second.convert_to<std::string> () % total.convert_to<std::string> ());
			}
		}
		else if (vm.count ("debug_dump_frontier_unchecked_dependents"))
		{
			auto inactive_node = nano::default_inactive_node (data_path, vm);
			auto node = inactive_node->node;
			std::cout << "Outputting any frontier hashes which have associated key hashes in the unchecked table (may take some time)...\n";

			// Cache the account heads to make searching quicker against unchecked keys.
			auto transaction (node->store.tx_begin_read ());
			std::unordered_set<nano::block_hash> frontier_hashes;
			for (auto i (node->store.account.begin (transaction)), n (node->store.account.end ()); i != n; ++i)
			{
				frontier_hashes.insert (i->second.head);
			}

			// Check all unchecked keys for matching frontier hashes. Indicates an issue with process_batch algorithm
			for (auto i (node->store.unchecked.begin (transaction)), n (node->store.unchecked.end ()); i != n; ++i)
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
			auto node_flags = nano::inactive_node_flag_defaults ();
			nano::update_flags (node_flags, vm);
			node_flags.generate_cache.account_count = true;
			nano::inactive_node inactive_node (data_path, node_flags);
			std::cout << boost::str (boost::format ("Frontier count: %1%\n") % inactive_node.node->ledger.cache.account_count);
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
			nano::network_constants network_constants;
			uint64_t difficulty{ network_constants.publish_full.base };
			auto multiplier_it = vm.find ("multiplier");
			if (multiplier_it != vm.end ())
			{
				try
				{
					auto multiplier (boost::lexical_cast<double> (multiplier_it->second.as<std::string> ()));
					difficulty = nano::difficulty::from_multiplier (multiplier, difficulty);
				}
				catch (boost::bad_lexical_cast &)
				{
					std::cerr << "Invalid multiplier\n";
					return -1;
				}
			}
			else
			{
				auto difficulty_it = vm.find ("difficulty");
				if (difficulty_it != vm.end ())
				{
					if (nano::from_string_hex (difficulty_it->second.as<std::string> (), difficulty))
					{
						std::cerr << "Invalid difficulty\n";
						return -1;
					}
				}
			}

			auto pow_rate_limiter = std::chrono::nanoseconds (0);
			auto pow_sleep_interval_it = vm.find ("pow_sleep_interval");
			if (pow_sleep_interval_it != vm.cend ())
			{
				pow_rate_limiter = std::chrono::nanoseconds (boost::lexical_cast<uint64_t> (pow_sleep_interval_it->second.as<std::string> ()));
			}

			nano::work_pool work (std::numeric_limits<unsigned>::max (), pow_rate_limiter);
			nano::change_block block (0, 0, nano::keypair ().prv, 0, 0);
			if (!result)
			{
				std::cerr << boost::str (boost::format ("Starting generation profiling. Difficulty: %1$#x (%2%x from base difficulty %3$#x)\n") % difficulty % nano::to_string (nano::difficulty::to_multiplier (difficulty, network_constants.publish_full.base), 4) % network_constants.publish_full.base);
				while (!result)
				{
					block.hashables.previous.qwords[0] += 1;
					auto begin1 (std::chrono::high_resolution_clock::now ());
					block.block_work_set (*work.generate (nano::work_version::work_1, block.root (), difficulty));
					auto end1 (std::chrono::high_resolution_clock::now ());
					std::cerr << boost::str (boost::format ("%|1$ 12d|\n") % std::chrono::duration_cast<std::chrono::microseconds> (end1 - begin1).count ());
				}
			}
		}
		else if (vm.count ("debug_profile_validate"))
		{
			uint64_t difficulty{ nano::network_constants ().publish_full.base };
			std::cerr << "Starting validation profile" << std::endl;
			auto start (std::chrono::steady_clock::now ());
			bool valid{ false };
			nano::block_hash hash{ 0 };
			uint64_t count{ 10000000U }; // 10M
			for (uint64_t i (0); i < count; ++i)
			{
				valid = nano::work_v1::value (hash, i) > difficulty;
			}
			std::ostringstream oss (valid ? "true" : "false"); // IO forces compiler to not dismiss the variable
			auto total_time (std::chrono::duration_cast<std::chrono::nanoseconds> (std::chrono::steady_clock::now () - start).count ());
			uint64_t average (total_time / count);
			std::cout << "Average validation time: " << std::to_string (average) << " ns (" << std::to_string (static_cast<unsigned> (count * 1e9 / total_time)) << " validations/s)" << std::endl;
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
						return -1;
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
						return -1;
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
						return -1;
					}
				}
				uint64_t difficulty (network_constants.publish_full.base);
				auto multiplier_it = vm.find ("multiplier");
				if (multiplier_it != vm.end ())
				{
					try
					{
						auto multiplier (boost::lexical_cast<double> (multiplier_it->second.as<std::string> ()));
						difficulty = nano::difficulty::from_multiplier (multiplier, difficulty);
					}
					catch (boost::bad_lexical_cast &)
					{
						std::cerr << "Invalid multiplier\n";
						return -1;
					}
				}
				else
				{
					auto difficulty_it = vm.find ("difficulty");
					if (difficulty_it != vm.end ())
					{
						if (nano::from_string_hex (difficulty_it->second.as<std::string> (), difficulty))
						{
							std::cerr << "Invalid difficulty\n";
							return -1;
						}
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
							nano::work_pool work_pool (0, std::chrono::nanoseconds (0), opencl ? [&opencl] (nano::work_version const version_a, nano::root const & root_a, uint64_t difficulty_a, std::atomic<int> &) {
								return opencl->generate_work (version_a, root_a, difficulty_a);
							}
																							   : std::function<boost::optional<uint64_t> (nano::work_version const, nano::root const &, uint64_t, std::atomic<int> &)> (nullptr));
							nano::change_block block (0, 0, nano::keypair ().prv, 0, 0);
							std::cerr << boost::str (boost::format ("Starting OpenCL generation profiling. Platform: %1%. Device: %2%. Threads: %3%. Difficulty: %4$#x (%5%x from base difficulty %6$#x)\n") % platform % device % threads % difficulty % nano::to_string (nano::difficulty::to_multiplier (difficulty, network_constants.publish_full.base), 4) % network_constants.publish_full.base);
							for (uint64_t i (0); true; ++i)
							{
								block.hashables.previous.qwords[0] += 1;
								auto begin1 (std::chrono::high_resolution_clock::now ());
								block.block_work_set (*work_pool.generate (nano::work_version::work_1, block.root (), difficulty));
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
		else if (vm.count ("debug_generate_crash_report"))
		{
			if (boost::filesystem::exists ("nano_node_backtrace.dump"))
			{
				// There is a backtrace, so output the contents
				std::ifstream ifs ("nano_node_backtrace.dump");
				boost::stacktrace::stacktrace st = boost::stacktrace::stacktrace::from_dump (ifs);

				std::string crash_report_filename = "nano_node_crash_report.txt";

#if defined(_WIN32) || defined(__APPLE__)
				// Only linux has load addresses, so just write the dump to a readable file.
				// It's the best we can do to keep consistency.
				std::ofstream ofs (crash_report_filename);
				ofs << st;
#else
				// Read all the nano node files
				boost::system::error_code err;
				auto running_executable_filepath = boost::dll::program_location (err);
				if (!err)
				{
					auto num = 0;
					auto format = boost::format ("nano_node_crash_load_address_dump_%1%.txt");
					std::vector<address_library_pair> base_addresses;

					// The first one only has the load address
					uint64_from_hex base_address;
					std::string line;
					if (boost::filesystem::exists (boost::str (format % num)))
					{
						std::getline (std::ifstream (boost::str (format % num)), line);
						if (boost::conversion::try_lexical_convert (line, base_address))
						{
							base_addresses.emplace_back (base_address.value, running_executable_filepath.string ());
						}
					}
					++num;

					// Now do the rest of the files
					while (boost::filesystem::exists (boost::str (format % num)))
					{
						std::ifstream ifs_dump_filename (boost::str (format % num));

						// 2 lines, the path to the dynamic library followed by the load address
						std::string dynamic_lib_path;
						std::getline (ifs_dump_filename, dynamic_lib_path);
						std::getline (ifs_dump_filename, line);

						if (boost::conversion::try_lexical_convert (line, base_address))
						{
							base_addresses.emplace_back (base_address.value, dynamic_lib_path);
						}

						++num;
					}

					std::sort (base_addresses.begin (), base_addresses.end ());

					auto address_column_it = vm.find ("address_column");
					auto column = -1;
					if (address_column_it != vm.end ())
					{
						if (!boost::conversion::try_lexical_convert (address_column_it->second.as<std::string> (), column))
						{
							std::cerr << "Error: Invalid address column\n";
							result = -1;
						}
					}

					// Extract the addresses from the dump file.
					std::stringstream stacktrace_ss;
					stacktrace_ss << st;
					std::vector<uint64_t> backtrace_addresses;
					while (std::getline (stacktrace_ss, line))
					{
						std::istringstream iss (line);
						std::vector<std::string> results (std::istream_iterator<std::string>{ iss }, std::istream_iterator<std::string> ());

						if (column != -1)
						{
							if (column < results.size ())
							{
								uint64_from_hex address_hex;
								if (boost::conversion::try_lexical_convert (results[column], address_hex))
								{
									backtrace_addresses.push_back (address_hex.value);
								}
								else
								{
									std::cerr << "Error: Address column does not point to valid addresses\n";
									result = -1;
								}
							}
							else
							{
								std::cerr << "Error: Address column too high\n";
								result = -1;
							}
						}
						else
						{
							for (const auto & text : results)
							{
								uint64_from_hex address_hex;
								if (boost::conversion::try_lexical_convert (text, address_hex))
								{
									backtrace_addresses.push_back (address_hex.value);
									break;
								}
							}
						}
					}

					// Recreate the crash report with an empty file
					boost::filesystem::remove (crash_report_filename);
					{
						std::ofstream ofs (crash_report_filename);
						nano::set_secure_perm_file (crash_report_filename);
					}

					// Hold the results from all addr2line calls, if all fail we can assume that addr2line is not installed,
					// and inform the user that it needs installing
					std::vector<int> system_codes;

					auto run_addr2line = [&backtrace_addresses, &base_addresses, &system_codes, &crash_report_filename] (bool use_relative_addresses) {
						for (auto backtrace_address : backtrace_addresses)
						{
							// Find the closest address to it
							for (auto base_address : boost::adaptors::reverse (base_addresses))
							{
								if (backtrace_address > base_address.address)
								{
									// Addresses need to be in hex for addr2line to work
									auto address = use_relative_addresses ? backtrace_address - base_address.address : backtrace_address;
									std::stringstream ss;
									ss << std::uppercase << std::hex << address;

									// Call addr2line to convert the address into something readable.
									auto res = std::system (boost::str (boost::format ("addr2line -fCi %1% -e %2% >> %3%") % ss.str () % base_address.library % crash_report_filename).c_str ());
									system_codes.push_back (res);
									break;
								}
							}
						}
					};

					// First run addr2line using absolute addresses
					run_addr2line (false);
					{
						std::ofstream ofs (crash_report_filename, std::ios_base::out | std::ios_base::app);
						ofs << std::endl
							<< "Using relative addresses:" << std::endl; // Add an empty line to separate the absolute & relative output
					}

					// Now run using relative addresses. This will give actual results for other dlls, the results from the nano_node executable.
					run_addr2line (true);

					if (std::find (system_codes.begin (), system_codes.end (), 0) == system_codes.end ())
					{
						std::cerr << "Error: Check that addr2line is installed and that nano_node_crash_load_address_dump_*.txt files exist." << std::endl;
						result = -1;
					}
				}
				else
				{
					std::cerr << "Error: Could not determine running executable path" << std::endl;
					result = -1;
				}
#endif
				if (result == 0)
				{
					std::cout << (boost::format ("%1% created") % crash_report_filename).str () << std::endl;
				}
			}
			else
			{
				std::cerr << "Error: nano_node_backtrace.dump could not be found";
				result = -1;
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
			nano::network_constants::set_active_network (nano::networks::nano_dev_network);
			nano::network_params dev_params;
			nano::block_builder builder;
			size_t num_accounts (100000);
			size_t num_iterations (5); // 100,000 * 5 * 2 = 1,000,000 blocks
			size_t max_blocks (2 * num_accounts * num_iterations + num_accounts * 2); //  1,000,000 + 2 * 100,000 = 1,200,000 blocks
			std::cout << boost::str (boost::format ("Starting pregenerating %1% blocks\n") % max_blocks);
			nano::node_flags node_flags;
			nano::update_flags (node_flags, vm);
			nano::inactive_node inactive_node (nano::unique_path (), data_path, node_flags);
			auto node = inactive_node.node;

			nano::block_hash genesis_latest (node->latest (nano::dev::genesis_key.pub));
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
							.account (nano::dev::genesis_key.pub)
							.previous (genesis_latest)
							.representative (nano::dev::genesis_key.pub)
							.balance (genesis_balance)
							.link (keys[i].pub)
							.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
							.work (*node->work.generate (nano::work_version::work_1, genesis_latest, node->network_params.network.publish_thresholds.epoch_1))
							.build ();

				genesis_latest = send->hash ();
				blocks.push_back (std::move (send));

				auto open = builder.state ()
							.account (keys[i].pub)
							.previous (0)
							.representative (keys[i].pub)
							.balance (balances[i])
							.link (genesis_latest)
							.sign (keys[i].prv, keys[i].pub)
							.work (*node->work.generate (nano::work_version::work_1, keys[i].pub, node->network_params.network.publish_thresholds.epoch_1))
							.build ();

				frontiers[i] = open->hash ();
				blocks.push_back (std::move (open));
			}
			for (auto i (0); i != num_iterations; ++i)
			{
				for (auto j (0); j != num_accounts; ++j)
				{
					size_t other (num_accounts - j - 1);
					// Sending to other account
					--balances[j];

					auto send = builder.state ()
								.account (keys[j].pub)
								.previous (frontiers[j].as_block_hash ())
								.representative (keys[j].pub)
								.balance (balances[j])
								.link (keys[other].pub)
								.sign (keys[j].prv, keys[j].pub)
								.work (*node->work.generate (nano::work_version::work_1, frontiers[j], node->network_params.network.publish_thresholds.epoch_1))
								.build ();

					frontiers[j] = send->hash ();
					blocks.push_back (std::move (send));
					// Receiving
					++balances[other];

					auto receive = builder.state ()
								   .account (keys[other].pub)
								   .previous (frontiers[other].as_block_hash ())
								   .representative (keys[other].pub)
								   .balance (balances[other])
								   .link (frontiers[j].as_block_hash ())
								   .sign (keys[other].prv, keys[other].pub)
								   .work (*node->work.generate (nano::work_version::work_1, frontiers[other], node->network_params.network.publish_thresholds.epoch_1))
								   .build ();

					frontiers[other] = receive->hash ();
					blocks.push_back (std::move (receive));
				}
			}
			// Processing blocks
			std::cout << boost::str (boost::format ("Starting processing %1% blocks\n") % max_blocks);
			auto begin (std::chrono::high_resolution_clock::now ());
			while (!blocks.empty ())
			{
				auto block (blocks.front ());
				node->process_active (block);
				blocks.pop_front ();
			}
			nano::timer<std::chrono::seconds> timer_l (nano::timer_state::started);
			while (node->ledger.cache.block_count != max_blocks + 1)
			{
				std::this_thread::sleep_for (std::chrono::milliseconds (10));
				// Message each 15 seconds
				if (timer_l.after_deadline (std::chrono::seconds (15)))
				{
					timer_l.restart ();
					std::cout << boost::str (boost::format ("%1% (%2%) blocks processed (unchecked), %3% remaining") % node->ledger.cache.block_count % node->store.unchecked.count (node->store.tx_begin_read ()) % node->block_processor.size ()) << std::endl;
				}
			}

			node->block_processor.flush ();
			auto end (std::chrono::high_resolution_clock::now ());
			auto time (std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count ());
			node->stop ();
			std::cout << boost::str (boost::format ("%|1$ 12d| us \n%2% blocks per second\n") % time % (max_blocks * 1000000 / time));
			release_assert (node->ledger.cache.block_count == max_blocks + 1);
		}
		else if (vm.count ("debug_profile_votes"))
		{
			nano::network_constants::set_active_network (nano::networks::nano_dev_network);
			nano::network_params dev_params;
			nano::block_builder builder;
			size_t num_elections (40000);
			size_t num_representatives (25);
			size_t max_votes (num_elections * num_representatives); // 40,000 * 25 = 1,000,000 votes
			std::cerr << boost::str (boost::format ("Starting pregenerating %1% votes\n") % max_votes);
			nano::node_flags node_flags;
			nano::update_flags (node_flags, vm);
			nano::node_wrapper node_wrapper (nano::unique_path (), data_path, node_flags);
			auto node = node_wrapper.node;

			nano::block_hash genesis_latest (node->latest (nano::dev::genesis_key.pub));
			nano::uint128_t genesis_balance (std::numeric_limits<nano::uint128_t>::max ());
			// Generating keys
			std::vector<nano::keypair> keys (num_representatives);
			nano::uint128_t balance ((node->config.online_weight_minimum.number () / num_representatives) + 1);
			for (auto i (0); i != num_representatives; ++i)
			{
				auto transaction (node->store.tx_begin_write ());
				genesis_balance = genesis_balance - balance;

				auto send = builder.state ()
							.account (nano::dev::genesis_key.pub)
							.previous (genesis_latest)
							.representative (nano::dev::genesis_key.pub)
							.balance (genesis_balance)
							.link (keys[i].pub)
							.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
							.work (*node->work.generate (nano::work_version::work_1, genesis_latest, node->network_params.network.publish_thresholds.epoch_1))
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
							.work (*node->work.generate (nano::work_version::work_1, keys[i].pub, node->network_params.network.publish_thresholds.epoch_1))
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
							.account (nano::dev::genesis_key.pub)
							.previous (genesis_latest)
							.representative (nano::dev::genesis_key.pub)
							.balance (genesis_balance)
							.link (destination.pub)
							.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
							.work (*node->work.generate (nano::work_version::work_1, genesis_latest, node->network_params.network.publish_thresholds.epoch_1))
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
				auto channel (std::make_shared<nano::transport::channel_loopback> (*node));
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
		else if (vm.count ("debug_profile_frontiers_confirmation"))
		{
			nano::force_nano_dev_network ();
			nano::network_params dev_params;
			nano::block_builder builder;
			size_t count (32 * 1024);
			auto count_it = vm.find ("count");
			if (count_it != vm.end ())
			{
				try
				{
					count = boost::lexical_cast<size_t> (count_it->second.as<std::string> ());
				}
				catch (boost::bad_lexical_cast &)
				{
					std::cerr << "Invalid count\n";
					return -1;
				}
			}
			std::cout << boost::str (boost::format ("Starting generating %1% blocks...\n") % (count * 2));
			boost::asio::io_context io_ctx1;
			boost::asio::io_context io_ctx2;
			nano::work_pool work (std::numeric_limits<unsigned>::max ());
			nano::logging logging;
			auto path1 (nano::unique_path ());
			auto path2 (nano::unique_path ());
			logging.init (path1);
			std::vector<std::string> config_overrides;
			auto config (vm.find ("config"));
			if (config != vm.end ())
			{
				config_overrides = nano::config_overrides (config->second.as<std::vector<nano::config_key_value_pair>> ());
			}
			nano::daemon_config daemon_config (data_path);
			auto error = nano::read_node_config_toml (data_path, daemon_config, config_overrides);

			nano::node_config config1 = daemon_config.node;
			config1.peering_port = 24000;

			nano::node_flags flags;
			nano::update_flags (flags, vm);
			flags.disable_lazy_bootstrap = true;
			flags.disable_legacy_bootstrap = true;
			flags.disable_wallet_bootstrap = true;
			flags.disable_bootstrap_listener = true;
			auto node1 (std::make_shared<nano::node> (io_ctx1, path1, config1, work, flags, 0));
			nano::block_hash genesis_latest (node1->latest (nano::dev::genesis_key.pub));
			nano::uint128_t genesis_balance (std::numeric_limits<nano::uint128_t>::max ());
			// Generating blocks
			std::deque<std::shared_ptr<nano::block>> blocks;
			for (auto i (0); i != count; ++i)
			{
				nano::keypair key;
				genesis_balance = genesis_balance - 1;

				auto send = builder.state ()
							.account (nano::dev::genesis_key.pub)
							.previous (genesis_latest)
							.representative (nano::dev::genesis_key.pub)
							.balance (genesis_balance)
							.link (key.pub)
							.sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
							.work (*work.generate (nano::work_version::work_1, genesis_latest, dev_params.network.publish_thresholds.epoch_1))
							.build ();

				genesis_latest = send->hash ();

				auto open = builder.state ()
							.account (key.pub)
							.previous (0)
							.representative (key.pub)
							.balance (1)
							.link (genesis_latest)
							.sign (key.prv, key.pub)
							.work (*work.generate (nano::work_version::work_1, key.pub, dev_params.network.publish_thresholds.epoch_1))
							.build ();

				blocks.push_back (std::move (send));
				blocks.push_back (std::move (open));
				if (i % 20000 == 0 && i != 0)
				{
					std::cout << boost::str (boost::format ("%1% blocks generated\n") % (i * 2));
				}
			}
			node1->start ();
			nano::thread_runner runner1 (io_ctx1, node1->config.io_threads);

			std::cout << boost::str (boost::format ("Processing %1% blocks\n") % (count * 2));
			for (auto & block : blocks)
			{
				node1->block_processor.add (block);
			}
			node1->block_processor.flush ();
			auto iteration (0);
			while (node1->ledger.cache.block_count != count * 2 + 1)
			{
				std::this_thread::sleep_for (std::chrono::milliseconds (500));
				if (++iteration % 60 == 0)
				{
					std::cout << boost::str (boost::format ("%1% blocks processed\n") % node1->ledger.cache.block_count);
				}
			}
			// Confirm blocks for node1
			for (auto & block : blocks)
			{
				node1->confirmation_height_processor.add (block);
			}
			while (node1->ledger.cache.cemented_count != node1->ledger.cache.block_count)
			{
				std::this_thread::sleep_for (std::chrono::milliseconds (500));
				if (++iteration % 60 == 0)
				{
					std::cout << boost::str (boost::format ("%1% blocks cemented\n") % node1->ledger.cache.cemented_count);
				}
			}

			// Start new node
			nano::node_config config2 = daemon_config.node;
			config1.peering_port = 24001;
			if (error)
			{
				std::cerr << "\n"
						  << error.get_message () << std::endl;
				std::exit (1);
			}
			else
			{
				config2.frontiers_confirmation = daemon_config.node.frontiers_confirmation;
				config2.active_elections_size = daemon_config.node.active_elections_size;
			}

			auto node2 (std::make_shared<nano::node> (io_ctx2, path2, config2, work, flags, 1));
			node2->start ();
			nano::thread_runner runner2 (io_ctx2, node2->config.io_threads);
			std::cout << boost::str (boost::format ("Processing %1% blocks (test node)\n") % (count * 2));
			// Processing block
			while (!blocks.empty ())
			{
				auto block (blocks.front ());
				node2->block_processor.add (block);
				blocks.pop_front ();
			}
			node2->block_processor.flush ();
			while (node2->ledger.cache.block_count != count * 2 + 1)
			{
				std::this_thread::sleep_for (std::chrono::milliseconds (500));
				if (++iteration % 60 == 0)
				{
					std::cout << boost::str (boost::format ("%1% blocks processed\n") % node2->ledger.cache.block_count);
				}
			}
			// Insert representative
			std::cout << "Initializing representative\n";
			auto wallet (node1->wallets.create (nano::random_wallet_id ()));
			wallet->insert_adhoc (nano::dev::genesis_key.prv);
			node2->network.merge_peer (node1->network.endpoint ());
			while (node2->rep_crawler.representative_count () == 0)
			{
				std::this_thread::sleep_for (std::chrono::milliseconds (10));
				if (++iteration % 500 == 0)
				{
					std::cout << "Representative initialization iteration...\n";
				}
			}
			auto begin (std::chrono::high_resolution_clock::now ());
			std::cout << boost::str (boost::format ("Starting confirming %1% frontiers (test node)\n") % (count + 1));
			// Wait for full frontiers confirmation
			while (node2->ledger.cache.cemented_count != node2->ledger.cache.block_count)
			{
				std::this_thread::sleep_for (std::chrono::milliseconds (25));
				if (++iteration % 1200 == 0)
				{
					std::cout << boost::str (boost::format ("%1% blocks confirmed\n") % node2->ledger.cache.cemented_count);
				}
			}
			auto end (std::chrono::high_resolution_clock::now ());
			auto time (std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count ());
			std::cout << boost::str (boost::format ("%|1$ 12d| us \n%2% frontiers per second\n") % time % ((count + 1) * 1000000 / time));
			io_ctx1.stop ();
			io_ctx2.stop ();
			runner1.join ();
			runner2.join ();
			node1->stop ();
			node2->stop ();
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
				nano::random_pool::generate_block (seed.bytes.data (), seed.bytes.size ());
				std::cout.write (reinterpret_cast<const char *> (seed.bytes.data ()), seed.bytes.size ());
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

			auto response_handler_l ([] (std::string const & response_a) {
				std::cout << response_a;
				// Terminate as soon as we have the result, even if background threads (like work generation) are running.
				std::exit (0);
			});

			auto node_flags = nano::inactive_node_flag_defaults ();
			nano::update_flags (node_flags, vm);
			node_flags.generate_cache.enable_all ();
			nano::inactive_node inactive_node_l (data_path, node_flags);

			nano::node_rpc_config config;
			nano::ipc::ipc_server server (*inactive_node_l.node, config);
			auto handler_l (std::make_shared<nano::json_handler> (*inactive_node_l.node, config, command_l.str (), response_handler_l));
			handler_l->process_request ();
		}
		else if (vm.count ("validate_blocks") || vm.count ("debug_validate_blocks"))
		{
			nano::timer<std::chrono::seconds> timer;
			timer.start ();
			auto node_flags = nano::inactive_node_flag_defaults ();
			nano::update_flags (node_flags, vm);
			node_flags.generate_cache.block_count = true;
			nano::inactive_node inactive_node (data_path, node_flags);
			auto node = inactive_node.node;
			bool const silent (vm.count ("silent"));
			unsigned threads_count (1);
			auto threads_it = vm.find ("threads");
			if (threads_it != vm.end ())
			{
				if (!boost::conversion::try_lexical_convert (threads_it->second.as<std::string> (), threads_count))
				{
					std::cerr << "Invalid threads count\n";
					return -1;
				}
			}
			threads_count = std::max (1u, threads_count);
			std::vector<std::thread> threads;
			nano::mutex mutex;
			nano::condition_variable condition;
			std::atomic<bool> finished (false);
			std::deque<std::pair<nano::account, nano::account_info>> accounts;
			std::atomic<size_t> count (0);
			std::atomic<uint64_t> block_count (0);
			std::atomic<uint64_t> errors (0);

			auto print_error_message = [&silent, &errors] (std::string const & error_message_a) {
				if (!silent)
				{
					static nano::mutex cerr_mutex;
					nano::lock_guard<nano::mutex> lock (cerr_mutex);
					std::cerr << error_message_a;
				}
				++errors;
			};

			auto start_threads = [node, &threads_count, &threads, &mutex, &condition, &finished] (const auto & function_a, auto & deque_a) {
				for (auto i (0U); i < threads_count; ++i)
				{
					threads.emplace_back ([&function_a, node, &mutex, &condition, &finished, &deque_a] () {
						auto transaction (node->store.tx_begin_read ());
						nano::unique_lock<nano::mutex> lock (mutex);
						while (!deque_a.empty () || !finished)
						{
							while (deque_a.empty () && !finished)
							{
								condition.wait (lock);
							}
							if (!deque_a.empty ())
							{
								auto pair (deque_a.front ());
								deque_a.pop_front ();
								lock.unlock ();
								function_a (node, transaction, pair.first, pair.second);
								lock.lock ();
							}
						}
					});
				}
			};

			auto check_account = [&print_error_message, &silent, &count, &block_count] (std::shared_ptr<nano::node> const & node, nano::read_transaction const & transaction, nano::account const & account, nano::account_info const & info) {
				++count;
				if (!silent && (count % 20000) == 0)
				{
					std::cout << boost::str (boost::format ("%1% accounts validated\n") % count);
				}
				nano::confirmation_height_info confirmation_height_info;
				node->store.confirmation_height.get (transaction, account, confirmation_height_info);

				if (confirmation_height_info.height > info.block_count)
				{
					print_error_message (boost::str (boost::format ("Confirmation height %1% greater than block count %2% for account: %3%\n") % confirmation_height_info.height % info.block_count % account.to_account ()));
				}

				auto hash (info.open_block);
				nano::block_hash calculated_hash (0);
				auto block (node->store.block.get (transaction, hash)); // Block data
				uint64_t height (0);
				if (node->ledger.pruning && confirmation_height_info.height != 0)
				{
					hash = confirmation_height_info.frontier;
					block = node->store.block.get (transaction, hash);
					// Iteration until pruned block
					bool pruned_block (false);
					while (!pruned_block && !block->previous ().is_zero ())
					{
						auto previous_block (node->store.block.get (transaction, block->previous ()));
						if (previous_block != nullptr)
						{
							hash = previous_block->hash ();
							block = previous_block;
						}
						else
						{
							pruned_block = true;
							if (!node->store.pruned.exists (transaction, block->previous ()))
							{
								print_error_message (boost::str (boost::format ("Pruned previous block does not exist %1%\n") % block->previous ().to_string ()));
							}
						}
					}
					calculated_hash = block->previous ();
					height = block->sideband ().height - 1;
					if (!node->ledger.block_or_pruned_exists (transaction, info.open_block))
					{
						print_error_message (boost::str (boost::format ("Open block does not exist %1%\n") % info.open_block.to_string ()));
					}
				}
				uint64_t previous_timestamp (0);
				nano::account calculated_representative (0);
				while (!hash.is_zero () && block != nullptr)
				{
					++block_count;
					auto const & sideband (block->sideband ());
					// Check for state & open blocks if account field is correct
					if (block->type () == nano::block_type::open || block->type () == nano::block_type::state)
					{
						if (block->account () != account)
						{
							print_error_message (boost::str (boost::format ("Incorrect account field for block %1%\n") % hash.to_string ()));
						}
					}
					// Check if sideband account is correct
					else if (sideband.account != account)
					{
						print_error_message (boost::str (boost::format ("Incorrect sideband account for block %1%\n") % hash.to_string ()));
					}
					// Check if previous field is correct
					if (calculated_hash != block->previous ())
					{
						print_error_message (boost::str (boost::format ("Incorrect previous field for block %1%\n") % hash.to_string ()));
					}
					// Check if previous & type for open blocks are correct
					if (height == 0 && !block->previous ().is_zero ())
					{
						print_error_message (boost::str (boost::format ("Incorrect previous for open block %1%\n") % hash.to_string ()));
					}
					if (height == 0 && block->type () != nano::block_type::open && block->type () != nano::block_type::state)
					{
						print_error_message (boost::str (boost::format ("Incorrect type for open block %1%\n") % hash.to_string ()));
					}
					// Check if block data is correct (calculating hash)
					calculated_hash = block->hash ();
					if (calculated_hash != hash)
					{
						print_error_message (boost::str (boost::format ("Invalid data inside block %1% calculated hash: %2%\n") % hash.to_string () % calculated_hash.to_string ()));
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
							bool error_or_pruned (false);
							if (!state_block.hashables.previous.is_zero ())
							{
								prev_balance = node->ledger.balance_safe (transaction, state_block.hashables.previous, error_or_pruned);
							}
							if (node->ledger.is_epoch_link (state_block.hashables.link))
							{
								if ((state_block.hashables.balance == prev_balance && !error_or_pruned) || (node->ledger.pruning && error_or_pruned && block->sideband ().details.is_epoch))
								{
									invalid = validate_message (node->ledger.epoch_signer (block->link ()), hash, block->block_signature ());
								}
							}
						}
						if (invalid)
						{
							print_error_message (boost::str (boost::format ("Invalid signature for block %1%\n") % hash.to_string ()));
						}
					}
					// Validate block details set in the sideband
					bool block_details_error = false;
					if (block->type () != nano::block_type::state)
					{
						// Not state
						block_details_error = sideband.details.is_send || sideband.details.is_receive || sideband.details.is_epoch;
					}
					else
					{
						bool error_or_pruned (false);
						auto prev_balance (node->ledger.balance_safe (transaction, block->previous (), error_or_pruned));
						if (!node->ledger.pruning || !error_or_pruned)
						{
							if (block->balance () < prev_balance)
							{
								// State send
								block_details_error = !sideband.details.is_send || sideband.details.is_receive || sideband.details.is_epoch;
							}
							else
							{
								if (block->link ().is_zero ())
								{
									// State change
									block_details_error = sideband.details.is_send || sideband.details.is_receive || sideband.details.is_epoch;
								}
								else if (block->balance () == prev_balance && node->ledger.is_epoch_link (block->link ()))
								{
									// State epoch
									block_details_error = !sideband.details.is_epoch || sideband.details.is_send || sideband.details.is_receive;
								}
								else
								{
									// State receive
									block_details_error = !sideband.details.is_receive || sideband.details.is_send || sideband.details.is_epoch;
									block_details_error |= !node->ledger.block_or_pruned_exists (transaction, block->link ().as_block_hash ());
								}
							}
						}
						else if (!node->store.pruned.exists (transaction, block->previous ()))
						{
							print_error_message (boost::str (boost::format ("Previous pruned block does not exist %1%\n") % block->previous ().to_string ()));
						}
					}
					if (block_details_error)
					{
						print_error_message (boost::str (boost::format ("Incorrect sideband block details for block %1%\n") % hash.to_string ()));
					}
					// Check link epoch version
					if (sideband.details.is_receive && (!node->ledger.pruning || !node->store.pruned.exists (transaction, block->link ().as_block_hash ())))
					{
						if (sideband.source_epoch != node->store.block.version (transaction, block->link ().as_block_hash ()))
						{
							print_error_message (boost::str (boost::format ("Incorrect source epoch for block %1%\n") % hash.to_string ()));
						}
					}
					// Check if block work value is correct
					if (block->difficulty () < nano::work_threshold (block->work_version (), block->sideband ().details))
					{
						print_error_message (boost::str (boost::format ("Invalid work for block %1% value: %2%\n") % hash.to_string () % nano::to_string_hex (block->block_work ())));
					}
					// Check if sideband height is correct
					++height;
					if (sideband.height != height)
					{
						print_error_message (boost::str (boost::format ("Incorrect sideband height for block %1%. Sideband: %2%. Expected: %3%\n") % hash.to_string () % sideband.height % height));
					}
					// Check if sideband timestamp is after previous timestamp
					if (sideband.timestamp < previous_timestamp)
					{
						print_error_message (boost::str (boost::format ("Incorrect sideband timestamp for block %1%\n") % hash.to_string ()));
					}
					previous_timestamp = sideband.timestamp;
					// Calculate representative block
					if (block->type () == nano::block_type::open || block->type () == nano::block_type::change || block->type () == nano::block_type::state)
					{
						calculated_representative = block->representative ();
					}
					// Retrieving successor block hash
					hash = node->store.block.successor (transaction, hash);
					// Retrieving block data
					if (!hash.is_zero ())
					{
						block = node->store.block.get (transaction, hash);
					}
				}
				// Check if required block exists
				if (!hash.is_zero () && block == nullptr)
				{
					print_error_message (boost::str (boost::format ("Required block in account %1% chain was not found in ledger: %2%\n") % account.to_account () % hash.to_string ()));
				}
				// Check account block count
				if (info.block_count != height)
				{
					print_error_message (boost::str (boost::format ("Incorrect block count for account %1%. Actual: %2%. Expected: %3%\n") % account.to_account () % height % info.block_count));
				}
				// Check account head block (frontier)
				if (info.head != calculated_hash)
				{
					print_error_message (boost::str (boost::format ("Incorrect frontier for account %1%. Actual: %2%. Expected: %3%\n") % account.to_account () % calculated_hash.to_string () % info.head.to_string ()));
				}
				// Check account representative block
				if (info.representative != calculated_representative)
				{
					print_error_message (boost::str (boost::format ("Incorrect representative for account %1%. Actual: %2%. Expected: %3%\n") % account.to_account () % calculated_representative.to_string () % info.representative.to_string ()));
				}
			};

			start_threads (check_account, accounts);

			if (!silent)
			{
				std::cout << boost::str (boost::format ("Performing %1% threads blocks hash, signature, work validation...\n") % threads_count);
			}
			size_t const accounts_deque_overflow (32 * 1024);
			auto transaction (node->store.tx_begin_read ());
			for (auto i (node->store.account.begin (transaction)), n (node->store.account.end ()); i != n; ++i)
			{
				{
					nano::unique_lock<nano::mutex> lock (mutex);
					if (accounts.size () > accounts_deque_overflow)
					{
						auto wait_ms (250 * accounts.size () / accounts_deque_overflow);
						const auto wakeup (std::chrono::steady_clock::now () + std::chrono::milliseconds (wait_ms));
						condition.wait_until (lock, wakeup);
					}
					accounts.emplace_back (i->first, i->second);
				}
				condition.notify_all ();
			}
			{
				nano::lock_guard<nano::mutex> lock (mutex);
				finished = true;
			}
			condition.notify_all ();
			for (auto & thread : threads)
			{
				thread.join ();
			}
			threads.clear ();
			if (!silent)
			{
				std::cout << boost::str (boost::format ("%1% accounts validated\n") % count);
			}

			// Validate total block count
			auto ledger_block_count (node->store.block.count (transaction));
			if (node->flags.enable_pruning)
			{
				block_count += 1; // Add disconnected genesis block
			}
			if (block_count != ledger_block_count)
			{
				print_error_message (boost::str (boost::format ("Incorrect total block count. Blocks validated %1%. Block count in database: %2%\n") % block_count % ledger_block_count));
			}

			// Validate pending blocks
			count = 0;
			finished = false;
			std::deque<std::pair<nano::pending_key, nano::pending_info>> pending;

			auto check_pending = [&print_error_message, &silent, &count] (std::shared_ptr<nano::node> const & node, nano::read_transaction const & transaction, nano::pending_key const & key, nano::pending_info const & info) {
				++count;
				if (!silent && (count % 500000) == 0)
				{
					std::cout << boost::str (boost::format ("%1% pending blocks validated\n") % count);
				}
				// Check block existance
				auto block (node->store.block.get_no_sideband (transaction, key.hash));
				bool pruned (false);
				if (block == nullptr)
				{
					pruned = node->ledger.pruning && node->store.pruned.exists (transaction, key.hash);
					if (!pruned)
					{
						print_error_message (boost::str (boost::format ("Pending block does not exist %1%\n") % key.hash.to_string ()));
					}
				}
				else
				{
					// Check if pending destination is correct
					nano::account destination (0);
					bool previous_pruned = node->ledger.pruning && node->store.pruned.exists (transaction, block->previous ());
					if (previous_pruned)
					{
						block = node->store.block.get (transaction, key.hash);
					}
					if (auto state = dynamic_cast<nano::state_block *> (block.get ()))
					{
						if (node->ledger.is_send (transaction, *state))
						{
							destination = state->hashables.link.as_account ();
						}
					}
					else if (auto send = dynamic_cast<nano::send_block *> (block.get ()))
					{
						destination = send->hashables.destination;
					}
					else
					{
						print_error_message (boost::str (boost::format ("Incorrect type for pending block %1%\n") % key.hash.to_string ()));
					}
					if (key.account != destination)
					{
						print_error_message (boost::str (boost::format ("Incorrect destination for pending block %1%\n") % key.hash.to_string ()));
					}
					// Check if pending source is correct
					auto account (node->ledger.account (transaction, key.hash));
					if (info.source != account && !pruned)
					{
						print_error_message (boost::str (boost::format ("Incorrect source for pending block %1%\n") % key.hash.to_string ()));
					}
					// Check if pending amount is correct
					if (!pruned && !previous_pruned)
					{
						auto amount (node->ledger.amount (transaction, key.hash));
						if (info.amount != amount)
						{
							print_error_message (boost::str (boost::format ("Incorrect amount for pending block %1%\n") % key.hash.to_string ()));
						}
					}
				}
			};

			start_threads (check_pending, pending);

			size_t const pending_deque_overflow (64 * 1024);
			for (auto i (node->store.pending.begin (transaction)), n (node->store.pending.end ()); i != n; ++i)
			{
				{
					nano::unique_lock<nano::mutex> lock (mutex);
					if (pending.size () > pending_deque_overflow)
					{
						auto wait_ms (50 * pending.size () / pending_deque_overflow);
						const auto wakeup (std::chrono::steady_clock::now () + std::chrono::milliseconds (wait_ms));
						condition.wait_until (lock, wakeup);
					}
					pending.emplace_back (i->first, i->second);
				}
				condition.notify_all ();
			}
			{
				nano::lock_guard<nano::mutex> lock (mutex);
				finished = true;
			}
			condition.notify_all ();
			for (auto & thread : threads)
			{
				thread.join ();
			}
			if (!silent)
			{
				std::cout << boost::str (boost::format ("%1% pending blocks validated\n") % count);
				timer.stop ();
				std::cout << boost::str (boost::format ("%1% %2% validation time\n") % timer.value ().count () % timer.unit ());
			}
			if (errors == 0)
			{
				std::cout << "Validation status: Ok\n";
			}
			else
			{
				std::cout << boost::str (boost::format ("Validation status: Failed\n%1% errors found\n") % errors);
			}
		}
		else if (vm.count ("debug_profile_bootstrap"))
		{
			auto node_flags = nano::inactive_node_flag_defaults ();
			node_flags.read_only = false;
			nano::update_flags (node_flags, vm);
			nano::inactive_node node (nano::unique_path (), node_flags);
			auto begin (std::chrono::high_resolution_clock::now ());
			uint64_t block_count (0);
			size_t count (0);
			std::deque<nano::unchecked_info> epoch_open_blocks;
			{
				auto node_flags = nano::inactive_node_flag_defaults ();
				nano::update_flags (node_flags, vm);
				node_flags.generate_cache.block_count = true;
				nano::inactive_node inactive_node (data_path, node_flags);
				auto source_node = inactive_node.node;
				auto transaction (source_node->store.tx_begin_read ());
				block_count = source_node->ledger.cache.block_count;
				std::cout << boost::str (boost::format ("Performing bootstrap emulation, %1% blocks in ledger...") % block_count) << std::endl;
				for (auto i (source_node->store.account.begin (transaction)), n (source_node->store.account.end ()); i != n; ++i)
				{
					nano::account const & account (i->first);
					nano::account_info const & info (i->second);
					auto hash (info.head);
					while (!hash.is_zero ())
					{
						// Retrieving block data
						auto block (source_node->store.block.get_no_sideband (transaction, hash));
						if (block != nullptr)
						{
							++count;
							if ((count % 500000) == 0)
							{
								std::cout << boost::str (boost::format ("%1% blocks retrieved") % count) << std::endl;
							}
							nano::unchecked_info unchecked_info (block, account, 0, nano::signature_verification::unknown);
							node.node->block_processor.add (unchecked_info);
							if (block->type () == nano::block_type::state && block->previous ().is_zero () && source_node->ledger.is_epoch_link (block->link ()))
							{
								// Epoch open blocks can be rejected without processed pending blocks to account, push it later again
								epoch_open_blocks.push_back (unchecked_info);
							}
							// Retrieving previous block hash
							hash = block->previous ();
						}
					}
				}
			}
			nano::timer<std::chrono::seconds> timer_l (nano::timer_state::started);
			while (node.node->ledger.cache.block_count != block_count)
			{
				std::this_thread::sleep_for (std::chrono::milliseconds (500));
				// Add epoch open blocks again if required
				if (node.node->block_processor.size () == 0)
				{
					for (auto & unchecked_info : epoch_open_blocks)
					{
						node.node->block_processor.add (unchecked_info);
					}
				}
				// Message each 60 seconds
				if (timer_l.after_deadline (std::chrono::seconds (60)))
				{
					timer_l.restart ();
					std::cout << boost::str (boost::format ("%1% (%2%) blocks processed (unchecked)") % node.node->ledger.cache.block_count % node.node->store.unchecked.count (node.node->store.tx_begin_read ())) << std::endl;
				}
			}

			node.node->block_processor.flush ();

			auto end (std::chrono::high_resolution_clock::now ());
			auto time (std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count ());
			auto us_in_second (1000000);
			auto seconds (time / us_in_second);
			nano::remove_temporary_directories ();
			std::cout << boost::str (boost::format ("%|1$ 12d| seconds \n%2% blocks per second") % seconds % (block_count * us_in_second / time)) << std::endl;
			release_assert (node.node->ledger.cache.block_count == block_count);
		}
		else if (vm.count ("debug_peers"))
		{
			auto inactive_node = nano::default_inactive_node (data_path, vm);
			auto node = inactive_node->node;
			auto transaction (node->store.tx_begin_read ());

			for (auto i (node->store.peer.begin (transaction)), n (node->store.peer.end ()); i != n; ++i)
			{
				std::cout << boost::str (boost::format ("%1%\n") % nano::endpoint (boost::asio::ip::address_v6 (i->first.address_bytes ()), i->first.port ()));
			}
		}
		else if (vm.count ("debug_cemented_block_count"))
		{
			auto node_flags = nano::inactive_node_flag_defaults ();
			node_flags.generate_cache.cemented_count = true;
			nano::update_flags (node_flags, vm);
			nano::inactive_node node (data_path, node_flags);
			std::cout << "Total cemented block count: " << node.node->ledger.cache.cemented_count << std::endl;
		}
		else if (vm.count ("debug_prune"))
		{
			auto node_flags = nano::inactive_node_flag_defaults ();
			node_flags.read_only = false;
			nano::update_flags (node_flags, vm);
			nano::inactive_node inactive_node (data_path, node_flags);
			auto node = inactive_node.node;
			node->ledger_pruning (node_flags.block_processor_batch_size != 0 ? node_flags.block_processor_batch_size : 16 * 1024, true, true);
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
			auto inactive_node = nano::default_inactive_node (data_path, vm);
			inactive_node->node->logger.always_log (nano::severity_level::error, "Testing system logger");
		}
		else if (vm.count ("debug_account_versions"))
		{
			auto inactive_node = nano::default_inactive_node (data_path, vm);
			auto node = inactive_node->node;
			auto const epoch_count = nano::normalized_epoch (nano::epoch::max) + static_cast<std::underlying_type<nano::epoch>::type> (1);
			// Cache the accounts in a collection to make searching quicker against unchecked keys. Group by epoch
			nano::locked<std::vector<boost::unordered_set<nano::account>>> opened_account_versions_shared (epoch_count);
			using opened_account_versions_t = decltype (opened_account_versions_shared)::value_type;
			node->store.account.for_each_par (
			[&opened_account_versions_shared, epoch_count] (nano::read_transaction const & /*unused*/, nano::store_iterator<nano::account, nano::account_info> i, nano::store_iterator<nano::account, nano::account_info> n) {
				// First cache locally
				opened_account_versions_t opened_account_versions_l (epoch_count);
				for (; i != n; ++i)
				{
					auto const & account (i->first);
					auto const & account_info (i->second);

					// Epoch 0 will be index 0 for instance
					auto epoch_idx = nano::normalized_epoch (account_info.epoch ());
					opened_account_versions_l[epoch_idx].emplace (account);
				}
				// Now merge
				auto opened_account_versions = opened_account_versions_shared.lock ();
				debug_assert (opened_account_versions->size () == opened_account_versions_l.size ());
				for (auto idx (0); idx < opened_account_versions_l.size (); ++idx)
				{
					auto & accounts = opened_account_versions->at (idx);
					auto const & accounts_l = opened_account_versions_l.at (idx);
					accounts.insert (accounts_l.begin (), accounts_l.end ());
				}
			});

			// Caching in a single set speeds up lookup
			boost::unordered_set<nano::account> opened_accounts;
			{
				auto opened_account_versions = opened_account_versions_shared.lock ();
				for (auto const & account_version : *opened_account_versions)
				{
					opened_accounts.insert (account_version.cbegin (), account_version.cend ());
				}
			}

			// Iterate all pending blocks and collect the lowest version for each unopened account
			nano::locked<boost::unordered_map<nano::account, std::underlying_type_t<nano::epoch>>> unopened_highest_pending_shared;
			using unopened_highest_pending_t = decltype (unopened_highest_pending_shared)::value_type;
			node->store.pending.for_each_par (
			[&unopened_highest_pending_shared, &opened_accounts] (nano::read_transaction const & /*unused*/, nano::store_iterator<nano::pending_key, nano::pending_info> i, nano::store_iterator<nano::pending_key, nano::pending_info> n) {
				// First cache locally
				unopened_highest_pending_t unopened_highest_pending_l;
				for (; i != n; ++i)
				{
					nano::pending_key const & key (i->first);
					nano::pending_info const & info (i->second);
					auto & account = key.account;
					auto exists = opened_accounts.find (account) != opened_accounts.end ();
					if (!exists)
					{
						// This is an unopened account, store the lowest pending version
						auto epoch = nano::normalized_epoch (info.epoch);
						auto & existing_or_new = unopened_highest_pending_l[key.account];
						existing_or_new = std::max (epoch, existing_or_new);
					}
				}
				// Now merge
				auto unopened_highest_pending = unopened_highest_pending_shared.lock ();
				for (auto const & [account, epoch] : unopened_highest_pending_l)
				{
					auto & existing_or_new = unopened_highest_pending->operator[] (account);
					existing_or_new = std::max (epoch, existing_or_new);
				}
			});

			auto output_account_version_number = [] (auto version, auto num_accounts) {
				std::cout << "Account version " << version << " num accounts: " << num_accounts << "\n";
			};

			// Only single-threaded access from now on
			auto const & opened_account_versions = *opened_account_versions_shared.lock ();
			auto const & unopened_highest_pending = *unopened_highest_pending_shared.lock ();

			// Output total version counts for the opened accounts
			std::cout << "Opened accounts:\n";
			for (auto i = 0u; i < opened_account_versions.size (); ++i)
			{
				output_account_version_number (i, opened_account_versions[i].size ());
			}

			// Accumulate the version numbers for the highest pending epoch for each unopened account.
			std::vector<size_t> unopened_account_version_totals (epoch_count);
			for (auto const & [account, epoch] : unopened_highest_pending)
			{
				++unopened_account_version_totals[epoch];
			}

			// Output total version counts for the unopened accounts
			std::cout << "\nUnopened accounts:\n";
			for (auto i = 0u; i < unopened_account_version_totals.size (); ++i)
			{
				output_account_version_number (i, unopened_account_version_totals[i]);
			}
		}
		else if (vm.count ("debug_unconfirmed_frontiers"))
		{
			auto inactive_node = nano::default_inactive_node (data_path, vm);
			auto node = inactive_node->node;

			auto unconfirmed_frontiers = node->ledger.unconfirmed_frontiers ();
			std::cout << "Account: Height delta | Frontier | Confirmed frontier\n";
			for (auto const & [height_delta, unconfirmed_info] : unconfirmed_frontiers)
			{
				std::cout << (boost::format ("%1%: %2% %3% %4%\n") % unconfirmed_info.account.to_account () % height_delta % unconfirmed_info.frontier.to_string () % unconfirmed_info.cemented_frontier.to_string ()).str ();
			}

			std::cout << "\nNumber of unconfirmed frontiers: " << unconfirmed_frontiers.size () << std::endl;
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

namespace
{
std::istream & operator>> (std::istream & in, uint64_from_hex & out_val)
{
	in >> std::hex >> out_val.value;
	return in;
}

address_library_pair::address_library_pair (uint64_t address, std::string library) :
	address (address), library (library)
{
}

bool address_library_pair::operator< (const address_library_pair & other) const
{
	return address < other.address;
}

bool address_library_pair::operator== (const address_library_pair & other) const
{
	return address == other.address;
}
}
