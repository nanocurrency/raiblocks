#include <nano/lib/memory.hpp>
#include <nano/secure/common.hpp>

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace
{
/** This allocator records the size of all allocations that happen */
template <class T>
class record_allocations_new_delete_allocator
{
public:
	using value_type = T;

	explicit record_allocations_new_delete_allocator (std::vector<size_t> * allocated)
		: allocated (allocated)
	{
	}

	template <typename U>
	record_allocations_new_delete_allocator (const record_allocations_new_delete_allocator<U> & a)
	{
		allocated = a.allocated;
	}

	template <typename U>
	record_allocations_new_delete_allocator & operator= (const record_allocations_new_delete_allocator<U> &) = delete;

	T * allocate (size_t num_to_allocate)
	{
		auto size_allocated = (sizeof (T) * num_to_allocate);
		allocated->push_back (size_allocated);
		return static_cast<T *> (::operator new (size_allocated));
	}

	void deallocate (T * p, size_t num_to_deallocate)
	{
		::operator delete (p);
	}

	std::vector<size_t> * allocated;
};

template <typename T>
size_t get_allocated_size ()
{
	std::vector<size_t> allocated;
	record_allocations_new_delete_allocator<T> alloc (&allocated);
	std::allocate_shared<T, record_allocations_new_delete_allocator<T>> (alloc);
	assert (allocated.size () == 1);
	return allocated.front ();
}
}

TEST (memory_pool, validate_cleanup)
{
	// This might be turned off, e.g on Mac for instance, so don't do this test
	if (!nano::get_use_memory_pools ())
	{
		return;
	}

	nano::make_shared<nano::open_block> ();
	nano::make_shared<nano::receive_block> ();
	nano::make_shared<nano::send_block> ();
	nano::make_shared<nano::change_block> ();
	nano::make_shared<nano::state_block> ();
	nano::make_shared<nano::vote> ();

	ASSERT_TRUE (nano::purge_singleton_pool_memory<nano::open_block> ());
	ASSERT_TRUE (nano::purge_singleton_pool_memory<nano::receive_block> ());
	ASSERT_TRUE (nano::purge_singleton_pool_memory<nano::send_block> ());
	ASSERT_TRUE (nano::purge_singleton_pool_memory<nano::state_block> ());
	ASSERT_TRUE (nano::purge_singleton_pool_memory<nano::vote> ());

	// Change blocks have the same size as open_block so won't deallocate any memory
	ASSERT_FALSE (nano::purge_singleton_pool_memory<nano::change_block> ());

	ASSERT_EQ (nano::determine_shared_ptr_pool_size<nano::open_block> (), get_allocated_size<nano::open_block> () - sizeof (size_t));
	ASSERT_EQ (nano::determine_shared_ptr_pool_size<nano::receive_block> (), get_allocated_size<nano::receive_block> () - sizeof (size_t));
	ASSERT_EQ (nano::determine_shared_ptr_pool_size<nano::send_block> (), get_allocated_size<nano::send_block> () - sizeof (size_t));
	ASSERT_EQ (nano::determine_shared_ptr_pool_size<nano::change_block> (), get_allocated_size<nano::change_block> () - sizeof (size_t));
	ASSERT_EQ (nano::determine_shared_ptr_pool_size<nano::state_block> (), get_allocated_size<nano::state_block> () - sizeof (size_t));
	ASSERT_EQ (nano::determine_shared_ptr_pool_size<nano::vote> (), get_allocated_size<nano::vote> () - sizeof (size_t));
}
