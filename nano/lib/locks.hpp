#pragma once

#include <nano/lib/timer.hpp>

#include <condition_variable>
#include <mutex>
#include <unordered_map>

namespace nano
{
#if NANO_TIMED_LOCKS > 0
template <typename Mutex>
class lock_guard final
{
public:
	explicit lock_guard (Mutex & mutex_a)
		: guard (mutex_a)
	{
	}

	lock_guard (const lock_guard &) = delete;
	lock_guard & operator= (const lock_guard &) = delete;

private:
	std::lock_guard<Mutex> guard;
};

template <>
class lock_guard<std::mutex> final
{
public:
	explicit lock_guard (std::mutex & mutex_a);
	~lock_guard () noexcept;

	lock_guard (const lock_guard &) = delete;
	lock_guard & operator= (const lock_guard &) = delete;

private:
	std::mutex & mut;
	nano::timer<std::chrono::milliseconds> timer;
};

template <typename Mutex, typename = std::enable_if_t<std::is_same<Mutex, std::mutex>::value>>
class unique_lock final
{
public:
	unique_lock () = default;
	explicit unique_lock (Mutex & mutex_a);
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
};

#else
template <typename Mutex>
using lock_guard = std::lock_guard<Mutex>;

template <typename Mutex>
using unique_lock = std::unique_lock<Mutex>;
#endif

// For consistency wrapping the less well known _any variant which can be used with any lockable type
using condition_variable = std::condition_variable_any;
}
