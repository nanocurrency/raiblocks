#include <nano/lib/utility.hpp>
#include <nano/node/write_database_queue.hpp>

#include <algorithm>

nano::write_guard::write_guard (nano::condition_variable & cv_a, std::function<void()> guard_finish_callback_a)
	: cv (cv_a)
	, guard_finish_callback (guard_finish_callback_a)
{
}

nano::write_guard::~write_guard ()
{
	guard_finish_callback ();
	cv.notify_all ();
}

// clang-format off
nano::write_database_queue::write_database_queue ()
	: guard_finish_callback ([& queue = queue, &mutex = mutex]() {
		nano::lock_guard<std::mutex> guard (mutex);
		queue.pop_front ();
	})
{
}
// clang-format on

nano::write_guard nano::write_database_queue::wait (nano::writer writer)
{
	nano::unique_lock<std::mutex> lk (mutex);
	// Add writer to the end of the queue if it's not already waiting
	auto exists = std::find (queue.cbegin (), queue.cend (), writer) != queue.cend ();
	if (!exists)
	{
		queue.push_back (writer);
	}

	while (!stopped && queue.front () != writer)
	{
		cv.wait (lk);
	}

	return write_guard (cv, guard_finish_callback);
}

bool nano::write_database_queue::contains (nano::writer writer)
{
	nano::lock_guard<std::mutex> guard (mutex);
	return std::find (queue.cbegin (), queue.cend (), writer) != queue.cend ();
}

bool nano::write_database_queue::process (nano::writer writer)
{
	auto result = false;
	{
		nano::lock_guard<std::mutex> guard (mutex);
		// Add writer to the end of the queue if it's not already waiting
		auto exists = std::find (queue.cbegin (), queue.cend (), writer) != queue.cend ();
		if (!exists)
		{
			queue.push_back (writer);
		}

		result = (queue.front () == writer);
	}

	if (!result)
	{
		cv.notify_all ();
	}

	return result;
}

nano::write_guard nano::write_database_queue::pop ()
{
	return write_guard (cv, guard_finish_callback);
}

void nano::write_database_queue::stop ()
{
	stopped = true;
	cv.notify_all ();
}
