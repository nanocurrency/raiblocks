#pragma once

#define USING_NANO_TIMED_LOCKS (NANO_TIMED_LOCKS > 0)

#if USING_NANO_TIMED_LOCKS
#include <nano/lib/timer.hpp>
#endif

#include <condition_variable>
#include <mutex>

namespace nano
{
class mutex;
extern nano::mutex * mutex_to_filter;
extern nano::mutex mutex_to_filter_mutex;
bool should_be_filtered (const char * name);
bool any_filters_registered ();

enum class mutexes
{
	active,
	block_arrival,
	block_processor,
	block_uniquer,
	blockstore_cache,
	confirmation_height_processor,
	election_winner_details,
	gap_cache,
	network_filter,
	observer_set,
	request_aggregator,
	state_block_signature_verification,
	telemetry,
	vote_generator,
	vote_processor,
	vote_uniquer,
	votes_cache,
	work_pool
};

char const * mutex_identifier (mutexes mutex);

class mutex
{
public:
	mutex () = default;
	explicit mutex (const char * name_a)
#if USING_NANO_TIMED_LOCKS
	:
	name (name_a)
#endif
	{
#if USING_NANO_TIMED_LOCKS
		// This mutex should be filtered
		if (name && should_be_filtered (name))
		{
			std::lock_guard guard (mutex_to_filter_mutex);
			mutex_to_filter = this;
		}
#endif
	}

#if USING_NANO_TIMED_LOCKS
	~mutex ()
	{
		// Unfilter this destroyed mutex
		if (name && should_be_filtered (name))
		{
			// Unregister the mutex
			std::lock_guard guard (mutex_to_filter_mutex);
			mutex_to_filter = nullptr;
		}
	}
#endif

	void lock ()
	{
		mutex_m.lock ();
	}

	void unlock ()
	{
		mutex_m.unlock ();
	}

	bool try_lock ()
	{
		return mutex_m.try_lock ();
	}

#if USING_NANO_TIMED_LOCKS
	char const * get_name () const
	{
		return name ? name : "";
	}
#endif

private:
#if USING_NANO_TIMED_LOCKS
	char const * name{ nullptr };
#endif
	std::mutex mutex_m;
};

#if USING_NANO_TIMED_LOCKS
template <typename Mutex>
void output (const char * str, std::chrono::milliseconds time, Mutex & mutex);

template <typename Mutex>
void output_if_held_long_enough (nano::timer<std::chrono::milliseconds> & timer, Mutex & mutex);

#ifndef NANO_TIMED_LOCKS_IGNORE_BLOCKED
template <typename Mutex>
void output_if_blocked_long_enough (nano::timer<std::chrono::milliseconds> & timer, Mutex & mutex);
#endif

template <typename Mutex>
class lock_guard final
{
public:
	explicit lock_guard (Mutex & mutex_a) :
	guard (mutex_a)
	{
	}

	lock_guard (const lock_guard &) = delete;
	lock_guard & operator= (const lock_guard &) = delete;

private:
	std::lock_guard<Mutex> guard;
};

template <>
class lock_guard<nano::mutex> final
{
public:
	explicit lock_guard (nano::mutex & mutex_a);
	~lock_guard () noexcept;

	lock_guard (const lock_guard &) = delete;
	lock_guard & operator= (const lock_guard &) = delete;

private:
	nano::mutex & mut;
	nano::timer<std::chrono::milliseconds> timer;
};

template <typename Mutex, typename = std::enable_if_t<std::is_same<Mutex, nano::mutex>::value>>
class unique_lock final
{
public:
	unique_lock () = default;
	explicit unique_lock (Mutex & mutex_a);
	unique_lock (Mutex & mutex_a, std::defer_lock_t) noexcept;
	unique_lock (unique_lock && other) = delete;
	unique_lock & operator= (unique_lock && other) noexcept;
	~unique_lock () noexcept;
	unique_lock (const unique_lock &) = delete;
	unique_lock & operator= (const unique_lock &) = delete;

	void lock ();
	bool try_lock ();
	void unlock ();
	bool owns_lock () const noexcept;
	explicit operator bool () const noexcept;
	Mutex * mutex () const noexcept;

private:
	Mutex * mut{ nullptr };
	bool owns{ false };

	nano::timer<std::chrono::milliseconds> timer;

	void validate () const;
	void lock_impl ();

	friend class condition_variable;
};

/** Assumes std implementations of std::condition_variable never actually call nano::unique_lock::lock/unlock,
    but instead use OS intrinsics with the mutex handle directly. Due to this we also do not account for any
	time the condition variable is blocked on another holder of the mutex. */
class condition_variable final
{
public:
	condition_variable () = default;
	condition_variable (condition_variable const &) = delete;
	condition_variable & operator= (condition_variable const &) = delete;

	void notify_one () noexcept;
	void notify_all () noexcept;
	void wait (nano::unique_lock<nano::mutex> & lt);

	template <typename Pred>
	void wait (nano::unique_lock<nano::mutex> & lk, Pred pred)
	{
		while (!pred ())
		{
			wait (lk);
		}
	}

	template <typename Clock, typename Duration>
	std::cv_status wait_until (nano::unique_lock<nano::mutex> & lk, std::chrono::time_point<Clock, Duration> const & timeout_time)
	{
		if (!lk.mut || !lk.owns)
		{
			throw (std::system_error (std::make_error_code (std::errc::operation_not_permitted)));
		}

		output_if_held_long_enough (lk.timer, *lk.mut);
		// Start again in case cnd.wait calls unique_lock::lock/unlock () depending on some implementations
		lk.timer.start ();
		auto cv_status = cnd.wait_until (lk, timeout_time);
		lk.timer.restart ();
		return cv_status;
	}

	template <typename Clock, typename Duration, typename Pred>
	bool wait_until (nano::unique_lock<nano::mutex> & lk, std::chrono::time_point<Clock, Duration> const & timeout_time, Pred pred)
	{
		while (!pred ())
		{
			if (wait_until (lk, timeout_time) == std::cv_status::timeout)
			{
				return pred ();
			}
		}
		return true;
	}

	template <typename Rep, typename Period>
	void wait_for (nano::unique_lock<nano::mutex> & lk, std::chrono::duration<Rep, Period> const & rel_time)
	{
		wait_until (lk, std::chrono::steady_clock::now () + rel_time);
	}

	template <typename Rep, typename Period, typename Pred>
	bool wait_for (nano::unique_lock<nano::mutex> & lk, std::chrono::duration<Rep, Period> const & rel_time, Pred pred)
	{
		return wait_until (lk, std::chrono::steady_clock::now () + rel_time, std::move (pred));
	}

private:
	std::condition_variable_any cnd;
};

#else
template <typename Mutex>
using lock_guard = std::lock_guard<Mutex>;

template <typename Mutex>
using unique_lock = std::unique_lock<Mutex>;

// For consistency wrapping the less well known _any variant which can be used with any lockable type
using condition_variable = std::condition_variable_any;
#endif

/** A general purpose monitor template */
template <class T>
class locked
{
public:
	using value_type = T;

	template <typename... Args>
	explicit locked (Args &&... args) :
	obj (std::forward<Args> (args)...)
	{
	}

	struct scoped_lock final
	{
		explicit scoped_lock (locked * owner_a) :
		owner (owner_a)
		{
			owner->mutex.lock ();
		}

		~scoped_lock ()
		{
			owner->mutex.unlock ();
		}

		T * operator-> ()
		{
			return &owner->obj;
		}

		T & get () const
		{
			return owner->obj;
		}

		T & operator* () const
		{
			return get ();
		}

		locked * owner{ nullptr };
	};

	scoped_lock operator-> ()
	{
		return scoped_lock (this);
	}

	T & operator= (T const & other)
	{
		nano::unique_lock<nano::mutex> lk (mutex);
		obj = other;
		return obj;
	}

	operator T () const
	{
		return obj;
	}

	/** Returns a scoped lock wrapper, allowing multiple calls to the underlying object under the same lock */
	scoped_lock lock ()
	{
		return scoped_lock (this);
	}

private:
	T obj;
	nano::mutex mutex;
};
}
