#include <nano/lib/locks.hpp>
#include <nano/lib/utility.hpp>

#include <iostream>

#if NANO_TIMED_LOCKS > 0
namespace
{
template <typename Mutex>
void output (const char * str, std::chrono::milliseconds time, Mutex & mutex)
{
	static std::mutex cout_mutex;
	auto stacktrace = nano::generate_stacktrace ();
	// Guard standard out to keep the output from being interleaved
	std::lock_guard<std::mutex> guard (cout_mutex);
	std::cout << std::addressof (mutex) << " Mutex " << str << " for: " << time.count () << "ms\n"
			  << stacktrace << std::endl;
}

template <typename Mutex>
void output_if_held_long_enough (nano::timer<std::chrono::milliseconds> & timer, Mutex & mutex)
{
	auto time_held = timer.since_start ();
	if (time_held >= std::chrono::milliseconds (NANO_TIMED_LOCKS))
	{
		output ("held", time_held, mutex);
	}
	timer.stop ();
}

template <typename Mutex>
void output_if_blocked_long_enough (nano::timer<std::chrono::milliseconds> & timer, Mutex & mutex)
{
	auto time_blocked = timer.since_start ();
	if (time_blocked >= std::chrono::milliseconds (NANO_TIMED_LOCKS))
	{
		output ("blocked", time_blocked, mutex);
	}
}
}

namespace nano
{
lock_guard<std::mutex>::lock_guard (std::mutex & mutex)
	: mut (mutex)
{
	timer.start ();

	mut.lock ();
	output_if_blocked_long_enough (timer, mut);
}

lock_guard<std::mutex>::~lock_guard () noexcept
{
	mut.unlock ();
	output_if_held_long_enough (timer, mut);
}

// Explicit instantiations for allowed types
template class lock_guard<std::mutex>;

template <typename Mutex, typename U>
unique_lock<Mutex, U>::unique_lock (Mutex & mutex)
	: mut (std::addressof (mutex))
{
	lock_impl ();
}

template <typename Mutex, typename U>
void unique_lock<Mutex, U>::lock_impl ()
{
	timer.start ();

	mut->lock ();
	owns = true;

	output_if_blocked_long_enough (timer, *mut);
}

template <typename Mutex, typename U>
unique_lock<Mutex, U> & unique_lock<Mutex, U>::operator= (unique_lock<Mutex, U> && other) noexcept
{
	if (this != std::addressof (other))
	{
		if (owns)
		{
			mut->unlock ();
			owns = false;

			output_if_held_long_enough (timer, *mut);
		}

		mut = other.mut;
		owns = other.owns;
		timer = other.timer;

		other.mut = nullptr;
		other.owns = false;
	}
	return *this;
}

template <typename Mutex, typename U>
unique_lock<Mutex, U>::~unique_lock () noexcept
{
	if (owns)
	{
		mut->unlock ();
		owns = false;

		output_if_held_long_enough (timer, *mut);
	}
}

template <typename Mutex, typename U>
void unique_lock<Mutex, U>::lock ()
{
	validate ();
	lock_impl ();
}

template <typename Mutex, typename U>
bool unique_lock<Mutex, U>::try_lock ()
{
	validate ();
	owns = mut->try_lock ();

	if (owns)
	{
		timer.start ();
	}

	return owns;
}

template <typename Mutex, typename U>
void unique_lock<Mutex, U>::unlock ()
{
	if (!mut || !owns)
	{
		throw (std::system_error (std::make_error_code (std::errc::operation_not_permitted)));
	}

	mut->unlock ();
	owns = false;

	output_if_held_long_enough (timer, *mut);
}

template <typename Mutex, typename U>
bool unique_lock<Mutex, U>::owns_lock () const noexcept
{
	return owns;
}

template <typename Mutex, typename U>
unique_lock<Mutex, U>::operator bool () const noexcept
{
	return owns;
}

template <typename Mutex, typename U>
Mutex * unique_lock<Mutex, U>::mutex () const noexcept
{
	return mut;
}

template <typename Mutex, typename U>
void unique_lock<Mutex, U>::validate () const
{
	if (!mut)
	{
		throw (std::system_error (std::make_error_code (std::errc::operation_not_permitted)));
	}

	if (owns)
	{
		throw (std::system_error (std::make_error_code (std::errc::resource_deadlock_would_occur)));
	}
}

// Explicit instantiations for allowed types
template class unique_lock<std::mutex>;
}
#endif
