#include <nano/lib/utility.hpp>

#include <boost/dll/runtime_symbol_info.hpp>
#include <boost/filesystem.hpp>

#include <cstddef>
#include <iostream>
#include <limits>
#include <sstream>
#include <string_view>
#include <thread>

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
// On Windows this include defines min/max macros, so keep below other includes
// to reduce conflicts with other std functions
#include <boost/stacktrace.hpp>
#ifndef _WIN32
#if !BEFORE_GNU_SOURCE
#undef _GNU_SOURCE
#endif
#endif

#ifndef _WIN32
#include <sys/resource.h>
#endif

std::size_t nano::get_filedescriptor_limit ()
{
	std::size_t fd_limit = (std::numeric_limits<std::size_t>::max) ();
#ifndef _WIN32
	struct rlimit limit;
	if (getrlimit (RLIMIT_NOFILE, &limit) == 0)
	{
		fd_limit = static_cast<std::size_t> (limit.rlim_cur);
	}
#endif
	return fd_limit;
}

nano::container_info_composite::container_info_composite (std::string const & name) :
name (name)
{
}

bool nano::container_info_composite::is_composite () const
{
	return true;
}

void nano::container_info_composite::add_component (std::unique_ptr<container_info_component> child)
{
	children.push_back (std::move (child));
}

std::vector<std::unique_ptr<nano::container_info_component>> const & nano::container_info_composite::get_children () const
{
	return children;
}

std::string const & nano::container_info_composite::get_name () const
{
	return name;
}

nano::container_info_leaf::container_info_leaf (const container_info & info) :
info (info)
{
}

bool nano::container_info_leaf::is_composite () const
{
	return false;
}

nano::container_info const & nano::container_info_leaf::get_info () const
{
	return info;
}

void nano::dump_crash_stacktrace ()
{
	boost::stacktrace::safe_dump_to ("nano_node_backtrace.dump");
}

std::string nano::generate_stacktrace ()
{
	auto stacktrace = boost::stacktrace::stacktrace ();
	std::stringstream ss;
	ss << stacktrace;
	return ss.str ();
}

void nano::remove_all_files_in_dir (boost::filesystem::path const & dir)
{
	for (auto & p : boost::filesystem::directory_iterator (dir))
	{
		auto path = p.path ();
		if (boost::filesystem::is_regular_file (path))
		{
			boost::filesystem::remove (path);
		}
	}
}

void nano::move_all_files_to_dir (boost::filesystem::path const & from, boost::filesystem::path const & to)
{
	for (auto & p : boost::filesystem::directory_iterator (from))
	{
		auto path = p.path ();
		if (boost::filesystem::is_regular_file (path))
		{
			boost::filesystem::rename (path, to / path.filename ());
		}
	}
}

/*
 * Backing code for "release_assert" & "debug_assert", which are macros
 */
void assert_internal (const char * check_expr, const char * func, const char * file, unsigned int line, bool is_release_assert, std::string_view error_msg)
{
	std::cerr << "Assertion (" << check_expr << ") failed\n"
	          << func << "\n"
	          << file << ":" << line << "\n";
	if (!error_msg.empty ())
	{
		std::cerr << "Error: " << error_msg << "\n";
	}
	std::cerr << "\n";

	// Output stack trace to cerr
	auto backtrace_str = nano::generate_stacktrace ();
	std::cerr << backtrace_str << std::endl;

	// "abort" at the end of this function will go into any signal handlers (the daemon ones will generate a stack trace and load memory address files on non-Windows systems).
	// As there is no async-signal-safe way to generate stacktraces on Windows it must be done before aborting
#ifdef _WIN32
	{
		// Try construct the stacktrace dump in the same folder as the the running executable, otherwise use the current directory.
		boost::system::error_code err;
		auto running_executable_filepath = boost::dll::program_location (err);
		std::string filename = is_release_assert ? "nano_node_backtrace_release_assert.txt" : "nano_node_backtrace_assert.txt";
		std::string filepath = filename;
		if (!err)
		{
			filepath = (running_executable_filepath.parent_path () / filename).string ();
		}

		std::ofstream file (filepath);
		nano::set_secure_perm_file (filepath);
		file << backtrace_str;
	}
#endif

	abort ();
}
