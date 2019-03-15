#include <boost/lexical_cast.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/program_options.hpp>
#include <nano/lib/errors.hpp>
#include <nano/lib/jsonconfig.hpp>
#include <nano/lib/utility.hpp>
#include <nano/nano_wallet/icon.hpp>
#include <nano/node/cli.hpp>
#include <nano/node/ipc.hpp>
#include <nano/node/working.hpp>
#include <nano/rpc/rpc.hpp>

namespace
{
void logging_init (boost::filesystem::path const & application_path_a)
{
	static std::atomic_flag logging_already_added = ATOMIC_FLAG_INIT;
	if (!logging_already_added.test_and_set ())
	{
		boost::log::add_common_attributes ();
		auto path = application_path_a / "log";

		uintmax_t max_size{ 128 * 1024 * 1024 };
		uintmax_t rotation_size{ 4 * 1024 * 1024 };
		bool flush{ true };
		boost::log::add_file_log (boost::log::keywords::target = path, boost::log::keywords::file_name = path / "rpc_log_%Y-%m-%d_%H-%M-%S.%N.log", boost::log::keywords::rotation_size = rotation_size, boost::log::keywords::auto_flush = flush, boost::log::keywords::scan_method = boost::log::sinks::file::scan_method::scan_matching, boost::log::keywords::max_size = max_size, boost::log::keywords::format = "[%TimeStamp%]: %Message%");
	}
}

void run (boost::filesystem::path const & data_path)
{
	boost::filesystem::create_directories (data_path);
	boost::system::error_code error_chmod;
	nano::set_secure_perm_directory (data_path, error_chmod);
	std::unique_ptr<nano::thread_runner> runner;

	nano::rpc_config rpc_config;
	auto error = nano::read_and_update_rpc_config (data_path, rpc_config);
	if (!error)
	{
		logging_init (data_path);
		boost::asio::io_context io_ctx;
		try
		{
			nano::ipc::ipc_client client (io_ctx);

			nano::rpc rpc (io_ctx, rpc_config, client);
			std::promise<nano::error> promise;
			client.async_connect (rpc_config.address.to_string (), rpc_config.ipc_port, [&promise, &rpc, &io_ctx, &rpc_config, &client](nano::error err) {
				if (!err)
				{
					rpc.start ();
				}
				promise.set_value (err);
			});

			runner = std::make_unique<nano::thread_runner> (io_ctx, rpc_config.io_threads);

			// Check if connection was successful
			auto err = promise.get_future ().get ();
			runner->join ();
			if (err)
			{
				std::cerr << "Error connecting to node, check it is running and using the expected port (" << err.get_message () << ")\n";
			}
		}
		catch (const std::runtime_error & e)
		{
			std::cerr << "Error while running rpc (" << e.what () << ")\n";
		}
	}
	else
	{
		std::cerr << "Error deserializing config: " << error.get_message () << std::endl;
	}
}
}

int main (int argc, char * const * argv)
{
	nano::set_umask ();

	boost::program_options::options_description description ("Command line options");

	// clang-format off
	description.add_options ()
		("help", "Print out options")
		("version", "Prints out version")
		("daemon", "Start node daemon");
	// clang-format on

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
		auto err (nano::network_params::set_active_network (network->second.as<std::string> ()));
		if (err)
		{
			std::cerr << err.get_message () << std::endl;
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
	if (vm.count ("daemon") > 0)
	{
		run (data_path);
	}
	else if (vm.count ("version"))
	{
		if (NANO_VERSION_PATCH == 0)
		{
			std::cout << "Version " << NANO_MAJOR_MINOR_VERSION << std::endl;
		}
		else
		{
			std::cout << "Version " << NANO_MAJOR_MINOR_RC_VERSION << std::endl;
		}
	}
	else
	{
		std::cout << description << std::endl;
		result = -1;
	}

	return 1;
}
