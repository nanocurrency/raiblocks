#include <nano/boost/asio/post.hpp>
#include <nano/lib/locks.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/signatures.hpp>

nano::signature_checker::signature_checker (unsigned num_threads)
	: thread_pool (num_threads)
	, single_threaded (num_threads == 0)
	, num_threads (num_threads)
{
	if (!single_threaded)
	{
		set_thread_names (num_threads);
	}
}

nano::signature_checker::~signature_checker ()
{
	stop ();
}

void nano::signature_checker::verify (nano::signature_check_set & check_a)
{
	{
		// Don't process anything else if we have stopped
		nano::lock_guard<std::mutex> guard (mutex);
		if (stopped)
		{
			return;
		}
	}

	if (check_a.size < multithreaded_cutoff || single_threaded)
	{
		// Not dealing with many so just use the calling thread for checking signatures
		auto result = verify_batch (check_a, 0, check_a.size);
		release_assert (result);
		return;
	}

	// Split up the tasks equally over the calling thread and the thread pool.
	// Any overflow on the modulus of the batch_size is given to the calling thread, so the thread pool
	// only ever operates on batch_size sizes.
	size_t overflow_size = check_a.size % batch_size;
	size_t num_full_batches = check_a.size / batch_size;

	auto total_threads_to_split_over = num_threads + 1;
	auto num_base_batches_each = num_full_batches / total_threads_to_split_over;
	auto num_full_overflow_batches = num_full_batches % total_threads_to_split_over;

	auto size_calling_thread = (num_base_batches_each * batch_size) + overflow_size;
	auto num_full_batches_thread = (num_base_batches_each * num_threads);
	if (num_full_overflow_batches > 0)
	{
		size_calling_thread += batch_size;
		auto remaining = num_full_overflow_batches - 1;
		num_full_batches_thread += remaining;
	}

	release_assert (check_a.size == (num_full_batches_thread * batch_size + size_calling_thread));

	std::promise<void> promise;
	std::future<void> future = promise.get_future ();

	// Verify a number of signature batches over the thread pool (does not block)
	verify_async (check_a, num_full_batches_thread, promise);

	// Verify the rest on the calling thread, this operates on the signatures at the end of the check set
	auto result = verify_batch (check_a, check_a.size - size_calling_thread, size_calling_thread);
	release_assert (result);

	// Blocks until all the work is done
	future.wait ();
}

void nano::signature_checker::stop ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	if (!stopped)
	{
		stopped = true;
		thread_pool.join ();
	}
}

void nano::signature_checker::flush ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	while (!stopped && tasks_remaining != 0)
		;
}

bool nano::signature_checker::verify_batch (const nano::signature_check_set & check_a, size_t start_index, size_t size)
{
	/* Returns false if there are at least 1 invalid signature */
	auto code (nano::validate_message_batch (check_a.messages + start_index, check_a.message_lengths + start_index, check_a.pub_keys + start_index, check_a.signatures + start_index, size, check_a.verifications + start_index));
	(void)code;

	return std::all_of (check_a.verifications + start_index, check_a.verifications + start_index + size, [](int verification) { return verification == 0 || verification == 1; });
}

/* This operates on a number of signatures of size (num_batches * batch_size) from the beginning of the check_a pointers.
 * Caller should check the value of the promise which indicateswhen the work has been completed.
 */
void nano::signature_checker::verify_async (nano::signature_check_set & check_a, size_t num_batches, std::promise<void> & promise)
{
	auto task = std::make_shared<Task> (check_a, num_batches);
	++tasks_remaining;

	for (size_t batch = 0; batch < num_batches; ++batch)
	{
		auto size = batch_size;
		auto start_index = batch * batch_size;

		boost::asio::post (thread_pool, [this, task, size, start_index, &promise] {
			auto result = this->verify_batch (task->check, start_index, size);
			release_assert (result);

			if (--task->pending == 0)
			{
				--tasks_remaining;
				promise.set_value ();
			}
		});
	}
}

// Set the names of all the threads in the thread pool for easier identification
void nano::signature_checker::set_thread_names (unsigned num_threads)
{
	auto ready = false;
	auto pending = num_threads;
	nano::condition_variable cv;

	std::vector<std::promise<void>> promises (num_threads);
	std::vector<std::future<void>> futures;
	futures.reserve (num_threads);
	std::transform (promises.begin (), promises.end (), std::back_inserter (futures), [](auto & promise) {
		return promise.get_future ();
	});

	for (auto i = 0u; i < num_threads; ++i)
	{
		// clang-format off
		boost::asio::post (thread_pool, [&cv, &ready, &pending, &mutex = mutex, &promise = promises[i]]() {
			nano::unique_lock<std::mutex> lk (mutex);
			nano::thread_role::set (nano::thread_role::name::signature_checking);
			if (--pending == 0)
			{
				// All threads have been reached
				ready = true;
				lk.unlock ();
				cv.notify_all ();
			}
			else
			{
				// We need to wait until the other threads are finished
				cv.wait (lk, [&ready]() { return ready; });
			}
			promise.set_value ();
		});
		// clang-format on
	}

	// Wait until all threads have finished
	for (auto & future : futures)
	{
		future.wait ();
	}
	assert (pending == 0);
}
