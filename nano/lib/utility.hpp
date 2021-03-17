#pragma once

#include <nano/lib/locks.hpp>

#include <boost/current_function.hpp>
#include <boost/preprocessor/facilities/empty.hpp>
#include <boost/preprocessor/facilities/overload.hpp>

#include <cassert>
#include <functional>
#include <mutex>
#include <vector>

namespace boost
{
namespace filesystem
{
	class path;
}

namespace system
{
	class error_code;
}
}

void assert_internal (const char * check_expr, const char * func, const char * file, unsigned int line, bool is_release_assert, std::string_view error = "");

#define release_assert_1(check) check ? (void)0 : assert_internal (#check, BOOST_CURRENT_FUNCTION, __FILE__, __LINE__, true)
#define release_assert_2(check, error_msg) check ? (void)0 : assert_internal (#check, BOOST_CURRENT_FUNCTION, __FILE__, __LINE__, true, error_msg)
#if !BOOST_PP_VARIADICS_MSVC
#define release_assert(...)                          \
	BOOST_PP_OVERLOAD (release_assert_, __VA_ARGS__) \
	(__VA_ARGS__)
#else
#define release_assert(...) BOOST_PP_CAT (BOOST_PP_OVERLOAD (release_assert_, __VA_ARGS__) (__VA_ARGS__), BOOST_PP_EMPTY ())
#endif

#ifdef NDEBUG
#define debug_assert(check) (void)0
#else
#define debug_assert_1(check) check ? (void)0 : assert_internal (#check, BOOST_CURRENT_FUNCTION, __FILE__, __LINE__, false)
#define debug_assert_2(check, error_msg) check ? (void)0 : assert_internal (#check, BOOST_CURRENT_FUNCTION, __FILE__, __LINE__, false, error_msg)
#if !BOOST_PP_VARIADICS_MSVC
#define debug_assert(...)                          \
	BOOST_PP_OVERLOAD (debug_assert_, __VA_ARGS__) \
	(__VA_ARGS__)
#else
#define debug_assert(...) BOOST_PP_CAT (BOOST_PP_OVERLOAD (debug_assert_, __VA_ARGS__) (__VA_ARGS__), BOOST_PP_EMPTY ())
#endif
#endif

namespace nano
{
/* These containers are used to collect information about sequence containers.
 * It makes use of the composite design pattern to collect information
 * from sequence containers and sequence containers inside member variables.
 */
struct container_info
{
	std::string name;
	size_t count;
	size_t sizeof_element;
};

class container_info_component
{
public:
	virtual ~container_info_component () = default;
	virtual bool is_composite () const = 0;
};

class container_info_composite : public container_info_component
{
public:
	container_info_composite (std::string const & name);
	bool is_composite () const override;
	void add_component (std::unique_ptr<container_info_component> child);
	std::vector<std::unique_ptr<container_info_component>> const & get_children () const;
	std::string const & get_name () const;

private:
	std::string name;
	std::vector<std::unique_ptr<container_info_component>> children;
};

class container_info_leaf : public container_info_component
{
public:
	container_info_leaf (container_info const & info);
	bool is_composite () const override;
	container_info const & get_info () const;

private:
	container_info info;
};

// Lower priority of calling work generating thread
void work_thread_reprioritize ();

/*
 * Functions for managing filesystem permissions, platform specific
 */
void set_umask ();
void set_secure_perm_directory (boost::filesystem::path const & path);
void set_secure_perm_directory (boost::filesystem::path const & path, boost::system::error_code & ec);
void set_secure_perm_file (boost::filesystem::path const & path);
void set_secure_perm_file (boost::filesystem::path const & path, boost::system::error_code & ec);

/*
 * Function to check if running Windows as an administrator
 */
bool is_windows_elevated ();

/*
 * Function to check if the Windows Event log registry key exists
 */
bool event_log_reg_entry_exists ();

/*
 * Create the load memory addresses for the executable and shared libraries.
 */
void create_load_memory_address_files ();

/*
 * Dumps a stacktrace file which can be read using the --debug_output_last_backtrace_dump CLI command
 */
void dump_crash_stacktrace ();

/*
 * Generates the current stacktrace
 */
std::string generate_stacktrace ();

/**
 * Some systems, especially in virtualized environments, may have very low file descriptor limits,
 * causing the node to fail. This function attempts to query the limit and returns the value. If the
 * limit cannot be queried, or running on a Windows system, this returns max-value of size_t.
 * Increasing the limit programatically is highly system-dependent, and the process may lack the
 * required permissions; the node thus merely logs low limits as a potential problem and leaves
 * the system configuration to the user.
 */
size_t get_filedescriptor_limit ();

template <typename... T>
class observer_set final
{
public:
	void add (std::function<void(T...)> const & observer_a)
	{
		nano::lock_guard<nano::mutex> lock (mutex);
		observers.push_back (observer_a);
	}
	void notify (T... args)
	{
		nano::lock_guard<nano::mutex> lock (mutex);
		for (auto & i : observers)
		{
			i (args...);
		}
	}
	nano::mutex mutex{ mutex_identifier (mutexes::observer_set) };
	std::vector<std::function<void(T...)>> observers;
};

template <typename... T>
std::unique_ptr<container_info_component> collect_container_info (observer_set<T...> & observer_set, std::string const & name)
{
	size_t count = 0;
	{
		nano::lock_guard<nano::mutex> lock (observer_set.mutex);
		count = observer_set.observers.size ();
	}

	auto sizeof_element = sizeof (typename decltype (observer_set.observers)::value_type);
	auto composite = std::make_unique<container_info_composite> (name);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "observers", count, sizeof_element }));
	return composite;
}

void remove_all_files_in_dir (boost::filesystem::path const & dir);
void move_all_files_to_dir (boost::filesystem::path const & from, boost::filesystem::path const & to);

template <class InputIt, class OutputIt, class Pred, class Func>
void transform_if (InputIt first, InputIt last, OutputIt dest, Pred pred, Func transform)
{
	while (first != last)
	{
		if (pred (*first))
		{
			*dest++ = transform (*first);
		}

		++first;
	}
}

/** Safe narrowing cast which silences warnings and asserts on data loss in debug builds. This is optimized away. */
template <typename TARGET_TYPE, typename SOURCE_TYPE>
constexpr TARGET_TYPE narrow_cast (SOURCE_TYPE const & val)
{
	auto res (static_cast<TARGET_TYPE> (val));
	debug_assert (val == static_cast<SOURCE_TYPE> (res));
	return res;
}
}
