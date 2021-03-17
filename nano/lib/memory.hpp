#pragma once

#include <boost/pool/pool_alloc.hpp>

#include <functional>
#include <memory>
#include <vector>

namespace nano
{
#ifdef __APPLE__
#define MEMORY_POOL_DISABLED
#endif

bool get_use_memory_pools ();
void set_use_memory_pools (bool use_memory_pools);

/** This makes some heuristic assumptions about the implementation defined shared_ptr internals.
    Should only be used in the memory pool purge functions at exit, which doesn't matter much if
    it is incorrect (other than reports from heap memory analysers) */
template <typename T>
constexpr size_t determine_shared_ptr_pool_size ()
{
	static_assert (sizeof (T) >= sizeof (size_t), "An assumption is made about the size being allocated");
#ifdef __APPLE__
	constexpr auto size_control_block = 3 * sizeof (size_t);
#else
	constexpr auto size_control_block = 2 * sizeof (size_t);
#endif

	return size_control_block + sizeof (T);
}

/** Deallocates all memory from a singleton_pool (invalidates all existing pointers). Returns true if any memory was deallocated */
template <typename object>
bool purge_shared_ptr_singleton_pool_memory ()
{
	return boost::singleton_pool<boost::fast_pool_allocator_tag, nano::determine_shared_ptr_pool_size<object> ()>::purge_memory ();
}

class cleanup_guard final
{
public:
	explicit cleanup_guard (std::vector<std::function<void()>> const & cleanup_funcs_a);
	~cleanup_guard ();

private:
	std::vector<std::function<void()>> cleanup_funcs;
};

template <typename T, typename... Args>
std::shared_ptr<T> make_shared (Args &&... args)
{
	if (nano::get_use_memory_pools ())
	{
		return std::allocate_shared<T> (boost::fast_pool_allocator<T> (), std::forward<Args> (args)...);
	}
	else
	{
		return std::make_shared<T> (std::forward<Args> (args)...);
	}
}
}
