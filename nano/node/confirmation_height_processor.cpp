#include <nano/lib/logger_mt.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/lib/threading.hpp>
#include <nano/lib/utility.hpp>
#include <nano/node/confirmation_height_processor.hpp>
#include <nano/node/write_database_queue.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/ledger.hpp>

#include <boost/thread/latch.hpp>

#include <numeric>

nano::confirmation_height_processor::confirmation_height_processor (nano::ledger & ledger_a, nano::write_database_queue & write_database_queue_a, std::chrono::milliseconds batch_separate_pending_min_time_a, nano::logger_mt & logger_a, boost::latch & latch, confirmation_height_mode mode_a) :
ledger (ledger_a),
write_database_queue (write_database_queue_a),
// clang-format off
unbounded_processor (ledger_a, write_database_queue_a, batch_separate_pending_min_time_a, logger_a, stopped, original_hash, batch_write_size, [this](auto & cemented_blocks) { this->notify_observers (cemented_blocks); }, [this](auto const & block_hash_a) { this->notify_observers (block_hash_a); }, [this]() { return this->awaiting_processing_size (); }),
bounded_processor (ledger_a, write_database_queue_a, batch_separate_pending_min_time_a, logger_a, stopped, original_hash, batch_write_size, [this](auto & cemented_blocks) { this->notify_observers (cemented_blocks); }, [this](auto const & block_hash_a) { this->notify_observers (block_hash_a); }, [this]() { return this->awaiting_processing_size (); }),
// clang-format on
thread ([this, &latch, mode_a]() {
	nano::thread_role::set (nano::thread_role::name::confirmation_height_processing);
	// Do not start running the processing thread until other threads have finished their operations
	latch.wait ();
	this->run (mode_a);
})
{
}

nano::confirmation_height_processor::~confirmation_height_processor ()
{
	stop ();
}

void nano::confirmation_height_processor::stop ()
{
	{
		nano::lock_guard<std::mutex> guard (mutex);
		stopped = true;
	}
	condition.notify_one ();
	if (thread.joinable ())
	{
		thread.join ();
	}
}

void nano::confirmation_height_processor::run (confirmation_height_mode mode_a)
{
	nano::unique_lock<std::mutex> lk (mutex);
	while (!stopped)
	{
		if (!paused && !awaiting_processing.empty ())
		{
			lk.unlock ();
			if (bounded_processor.pending_empty () && unbounded_processor.pending_empty ())
			{
				lk.lock ();
				original_hashes_pending.clear ();
				lk.unlock ();
			}

			set_next_hash ();

			const auto num_blocks_to_use_unbounded = confirmation_height::unbounded_cutoff;
			auto blocks_within_automatic_unbounded_selection = (ledger.cache.block_count < num_blocks_to_use_unbounded || ledger.cache.block_count - num_blocks_to_use_unbounded < ledger.cache.cemented_count);

			// Don't want to mix up pending writes across different processors
			auto valid_unbounded = (mode_a == confirmation_height_mode::automatic && blocks_within_automatic_unbounded_selection && bounded_processor.pending_empty ());
			auto force_unbounded = (!unbounded_processor.pending_empty () || mode_a == confirmation_height_mode::unbounded);
			if (force_unbounded || valid_unbounded)
			{
				debug_assert (bounded_processor.pending_empty ());
				if (unbounded_processor.pending_empty ())
				{
					unbounded_processor.reset ();
				}
				unbounded_processor.process ();
			}
			else
			{
				debug_assert (mode_a == confirmation_height_mode::bounded || mode_a == confirmation_height_mode::automatic);
				debug_assert (confirmation_height_unbounded_processor.pending_empty ());
				if (bounded_processor.pending_empty ())
				{
					bounded_processor.reset ();
				}
				bounded_processor.process ();
			}

			lk.lock ();
		}
		else
		{
			auto lock_and_cleanup = [&lk, this]() {
				lk.lock ();
				original_hash.clear ();
				original_hashes_pending.clear ();
			};

			if (!paused)
			{
				lk.unlock ();

				// If there are blocks pending cementing, then make sure we flush out the remaining writes
				if (!bounded_processor.pending_empty ())
				{
					debug_assert (confirmation_height_unbounded_processor.pending_empty ());
					{
						auto scoped_write_guard = write_database_queue.wait (nano::writer::confirmation_height);
						bounded_processor.cement_blocks (scoped_write_guard);
					}
					lock_and_cleanup ();
					bounded_processor.reset ();
				}
				else if (!unbounded_processor.pending_empty ())
				{
					debug_assert (bounded_processor.pending_empty ());
					{
						auto scoped_write_guard = write_database_queue.wait (nano::writer::confirmation_height);
						unbounded_processor.cement_blocks (scoped_write_guard);
					}
					lock_and_cleanup ();
					unbounded_processor.reset ();
				}
				else
				{
					lock_and_cleanup ();
					condition.wait (lk);
				}
			}
			else
			{
				original_hash.clear ();
				condition.wait (lk);
			}
		}
	}
}

// Pausing only affects processing new blocks, not the current one being processed. Currently only used in tests
void nano::confirmation_height_processor::pause ()
{
	nano::lock_guard<std::mutex> lk (mutex);
	paused = true;
}

void nano::confirmation_height_processor::unpause ()
{
	{
		nano::lock_guard<std::mutex> lk (mutex);
		paused = false;
	}
	condition.notify_one ();
}

void nano::confirmation_height_processor::add (nano::block_hash const & hash_a)
{
	{
		nano::lock_guard<std::mutex> lk (mutex);
		awaiting_processing.get<tag_sequence> ().emplace_back (hash_a);
	}
	condition.notify_one ();
}

void nano::confirmation_height_processor::set_next_hash ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	debug_assert (!awaiting_processing.empty ());
	original_hash = awaiting_processing.get<tag_sequence> ().front ();
	original_hashes_pending.insert (original_hash);
	awaiting_processing.get<tag_sequence> ().pop_front ();
}

// Not thread-safe, only call before this processor has begun cementing
void nano::confirmation_height_processor::add_cemented_observer (std::function<void(std::shared_ptr<nano::block>)> const & callback_a)
{
	cemented_observers.push_back (callback_a);
}

// Not thread-safe, only call before this processor has begun cementing
void nano::confirmation_height_processor::add_block_already_cemented_observer (std::function<void(nano::block_hash const &)> const & callback_a)
{
	block_already_cemented_observers.push_back (callback_a);
}

void nano::confirmation_height_processor::notify_observers (std::vector<std::shared_ptr<nano::block>> const & cemented_blocks)
{
	for (auto const & block_callback_data : cemented_blocks)
	{
		for (auto const & observer : cemented_observers)
		{
			observer (block_callback_data);
		}
	}
}

void nano::confirmation_height_processor::notify_observers (nano::block_hash const & hash_already_cemented_a)
{
	for (auto const & observer : block_already_cemented_observers)
	{
		observer (hash_already_cemented_a);
	}
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (confirmation_height_processor & confirmation_height_processor_a, const std::string & name_a)
{
	auto composite = std::make_unique<container_info_composite> (name_a);

	size_t cemented_observers_count = confirmation_height_processor_a.cemented_observers.size ();
	size_t block_already_cemented_observers_count = confirmation_height_processor_a.block_already_cemented_observers.size ();
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "cemented_observers", cemented_observers_count, sizeof (decltype (confirmation_height_processor_a.cemented_observers)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "block_already_cemented_observers", block_already_cemented_observers_count, sizeof (decltype (confirmation_height_processor_a.block_already_cemented_observers)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "awaiting_processing", confirmation_height_processor_a.awaiting_processing_size (), sizeof (decltype (confirmation_height_processor_a.awaiting_processing)::value_type) }));
	composite->add_component (collect_container_info (confirmation_height_processor_a.bounded_processor, "bounded_processor"));
	composite->add_component (collect_container_info (confirmation_height_processor_a.unbounded_processor, "unbounded_processor"));
	return composite;
}

size_t nano::confirmation_height_processor::awaiting_processing_size ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	return awaiting_processing.size ();
}

bool nano::confirmation_height_processor::is_processing_block (nano::block_hash const & hash_a)
{
	nano::lock_guard<std::mutex> guard (mutex);
	return original_hashes_pending.find (hash_a) != original_hashes_pending.cend () || awaiting_processing.get<tag_hash> ().find (hash_a) != awaiting_processing.get<tag_hash> ().cend ();
}

nano::block_hash nano::confirmation_height_processor::current ()
{
	nano::lock_guard<std::mutex> lk (mutex);
	return original_hash;
}
