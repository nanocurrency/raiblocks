#include <nano/lib/threading.hpp>

#include <iostream>

namespace nano
{
namespace thread_role
{
	/*
	 * nano::thread_role namespace
	 *
	 * Manage thread role
	 */
	static thread_local nano::thread_role::name current_thread_role = nano::thread_role::name::unknown;
	nano::thread_role::name get ()
	{
		return current_thread_role;
	}

	std::string get_string (nano::thread_role::name role)
	{
		std::string thread_role_name_string;

		switch (role)
		{
			case nano::thread_role::name::unknown:
				thread_role_name_string = "<unknown>";
				break;
			case nano::thread_role::name::io:
				thread_role_name_string = "I/O";
				break;
			case nano::thread_role::name::work:
				thread_role_name_string = "Work pool";
				break;
			case nano::thread_role::name::packet_processing:
				thread_role_name_string = "Pkt processing";
				break;
			case nano::thread_role::name::alarm:
				thread_role_name_string = "Alarm";
				break;
			case nano::thread_role::name::vote_processing:
				thread_role_name_string = "Vote processing";
				break;
			case nano::thread_role::name::block_processing:
				thread_role_name_string = "Blck processing";
				break;
			case nano::thread_role::name::request_loop:
				thread_role_name_string = "Request loop";
				break;
			case nano::thread_role::name::wallet_actions:
				thread_role_name_string = "Wallet actions";
				break;
			case nano::thread_role::name::work_watcher:
				thread_role_name_string = "Work watcher";
				break;
			case nano::thread_role::name::bootstrap_initiator:
				thread_role_name_string = "Bootstrap init";
				break;
			case nano::thread_role::name::voting:
				thread_role_name_string = "Voting";
				break;
			case nano::thread_role::name::signature_checking:
				thread_role_name_string = "Signature check";
				break;
			case nano::thread_role::name::rpc_request_processor:
				thread_role_name_string = "RPC processor";
				break;
			case nano::thread_role::name::rpc_process_container:
				thread_role_name_string = "RPC process";
				break;
			case nano::thread_role::name::confirmation_height_processing:
				thread_role_name_string = "Conf height";
				break;
			case nano::thread_role::name::worker:
				thread_role_name_string = "Worker";
				break;
		}

		/*
		 * We want to constrain the thread names to 15
		 * characters, since this is the smallest maximum
		 * length supported by the platforms we support
		 * (specifically, Linux)
		 */
		assert (thread_role_name_string.size () < 16);
		return (thread_role_name_string);
	}

	std::string get_string ()
	{
		return get_string (current_thread_role);
	}

	void set (nano::thread_role::name role)
	{
		auto thread_role_name_string (get_string (role));

		nano::thread_role::set_os_name (thread_role_name_string);

		nano::thread_role::current_thread_role = role;
	}
}
}

void nano::thread_attributes::set (boost::thread::attributes & attrs)
{
	auto attrs_l (&attrs);
	attrs_l->set_stack_size (8000000); //8MB
}

nano::thread_runner::thread_runner (boost::asio::io_context & io_ctx_a, unsigned service_threads_a)
	: io_guard (boost::asio::make_work_guard (io_ctx_a))
{
	boost::thread::attributes attrs;
	nano::thread_attributes::set (attrs);
	for (auto i (0u); i < service_threads_a; ++i)
	{
		threads.push_back (boost::thread (attrs, [&io_ctx_a]() {
			nano::thread_role::set (nano::thread_role::name::io);
			try
			{
				io_ctx_a.run ();
			}
			catch (std::exception const & ex)
			{
				std::cerr << ex.what () << std::endl;
#ifndef NDEBUG
				throw;
#endif
			}
			catch (...)
			{
#ifndef NDEBUG
				/*
				 * In a release build, catch and swallow the
				 * io_context exception, in debug mode pass it
				 * on
				 */
				throw;
#endif
			}
		}));
	}
}

nano::thread_runner::~thread_runner ()
{
	join ();
}

void nano::thread_runner::join ()
{
	io_guard.reset ();
	for (auto & i : threads)
	{
		if (i.joinable ())
		{
			i.join ();
		}
	}
}

void nano::thread_runner::stop_event_processing ()
{
	io_guard.get_executor ().context ().stop ();
}
