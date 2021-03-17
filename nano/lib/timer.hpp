#pragma once

#include <chrono>
#include <iosfwd>
#include <string>
#include <vector>

namespace nano
{
enum class timer_state
{
	started,
	stopped
};

/** Timer utility with nesting support */
template <typename UNIT = std::chrono::milliseconds, typename CLOCK = std::chrono::steady_clock>
class timer
{
public:
	timer () = default;
	explicit timer (nano::timer_state state_a, std::string const & description_a = "timer");
	explicit timer (std::string const & description_a);
	timer (std::string const & description_a, timer * parent_a);

	/** Do not output if measured time is below the time units threshold in \p minimum_a */
	timer & set_minimum (UNIT minimum_a);

	/**
	 * Create a child timer without starting it.
	 * Since the timing API needs to have low overhead, this function
	 * does not check if a timer with the same name already exists.
	 */
	timer & child (std::string const & description_a = "child timer");

	/** Create and start a child timer */
	timer & start_child (std::string const & description_a = "child timer");

	/** Start the timer. This will assert if the timer is already started. */
	void start ();

	/**
	 * Restarts the timer by setting start time to current time and resetting tick count.
	 * This can be called in any timer state.
	 * @return duration
	 */
	UNIT restart ();

	/**
	 * Stops the timer and increases the measurement count. A timer can be started and paused
	 * multiple times (e.g. in a loop).
	 * @return duration
	 */
	UNIT pause ();

	/**
	 * Stop timer. This will assert if the timer is not in a started state.
	 * @return duration
	 */
	UNIT stop ();

	/**
	 * Updates and returns current tick count.
	 */
	UNIT value ();

	/** Returns the duration in UNIT since the timer was last started. */
	UNIT since_start () const;

	/** Returns true if the timer was last started longer than \p duration_a units ago*/
	bool after_deadline (UNIT duration_a);

	/** Returns true if the timer was last started shorter than \p duration_a units ago*/
	bool before_deadline (UNIT duration_a);

	/** Stop timer and write measurements to \p stream_a */
	void stop (std::ostream & stream_a);

	/** Stop timer and write measurements to \p output_a */
	void stop (std::string & output_a);

	/** Print measurements to the \p stream_a */
	void print (std::ostream & stream_a);

	/** Returns the SI unit string */
	std::string unit () const;
	nano::timer_state current_state () const;

private:
	timer * parent{ nullptr };
	std::vector<timer> children;
	nano::timer_state state{ nano::timer_state::stopped };
	std::string desc;
	std::chrono::time_point<CLOCK> begin;
	UNIT ticks{ 0 };
	UNIT minimum{ UNIT::zero () };
	unsigned measurements{ 0 };
	void update_ticks ();
};

inline uint64_t milliseconds_since_epoch ()
{
	return std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now ().time_since_epoch ()).count ();
}

inline uint64_t seconds_since_epoch ()
{
	return std::chrono::duration_cast<std::chrono::seconds> (std::chrono::system_clock::now ().time_since_epoch ()).count ();
}
}
