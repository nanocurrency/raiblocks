#pragma once

#include <nano/lib/locks.hpp>

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/manipulators/to_log.hpp>

#include <array>
#include <chrono>
#include <mutex>

namespace nano
{
enum class severity_level
{
	normal = 0,
	error
};
}

// Attribute value tag type
struct severity_tag;

inline boost::log::formatting_ostream & operator<< (boost::log::formatting_ostream & strm, boost::log::to_log_manip<nano::severity_level, severity_tag> const & manip)
{
	// Needs to match order in the severity_level enum
	static std::array<const char *, 2> strings = {
		"",
		"Error: "
	};

	nano::severity_level level = manip.get ();
	assert (static_cast<int> (level) < strings.size ());
	strm << strings[static_cast<int> (level)];
	return strm;
}

namespace nano
{
// A wrapper around a boost logger object to allow minimum
// time spaced output to prevent logging happening too quickly.
class logger_mt
{
private:
	void add_to_stream (boost::log::record_ostream & stream)
	{
	}

	template <typename LogItem, typename... LogItems>
	void add_to_stream (boost::log::record_ostream & stream, const LogItem & first_log_item, LogItems &&... remainder_log_items)
	{
		stream << first_log_item;
		add_to_stream (stream, remainder_log_items...);
	}

	template <typename... LogItems>
	void output (nano::severity_level severity_level, LogItems &&... log_items)
	{
		boost::log::record rec = boost_logger_mt.open_record (boost::log::keywords::severity = severity_level);
		if (rec)
		{
			boost::log::record_ostream strm (rec);
			add_to_stream (strm, std::forward<LogItems> (log_items)...);
			strm.flush ();
			boost_logger_mt.push_record (std::move (rec));
		}
	}

public:
	logger_mt () = default;

	/**
	 * @param min_log_delta_a The minimum time between successive output
	 */
	explicit logger_mt (std::chrono::milliseconds const & min_log_delta_a)
		: min_log_delta (min_log_delta_a)
	{
	}

	/*
	 * @param log_items A collection of objects with overloaded operator<< to be output to the log file
	 * @params severity_level The severity level that this log message should have.
	 */
	template <typename... LogItems>
	void always_log (nano::severity_level severity_level, LogItems &&... log_items)
	{
		output (severity_level, std::forward<LogItems> (log_items)...);
	}

	/*
	 * @param log_items A collection of objects with overloaded operator<< to be output to the log file.
	 */
	template <typename... LogItems>
	void always_log (LogItems &&... log_items)
	{
		always_log (nano::severity_level::normal, std::forward<LogItems> (log_items)...);
	}

	/*
	 * @param log_items Output to the log file if the last write was over min_log_delta time ago.
	 * @params severity_level The severity level that this log message should have.
	 * @return true if nothing was logged
	 */
	template <typename... LogItems>
	bool try_log (nano::severity_level severity_level, LogItems &&... log_items)
	{
		auto error (true);
		auto time_now = std::chrono::steady_clock::now ();
		nano::unique_lock<std::mutex> lk (last_log_time_mutex);
		if (((time_now - last_log_time) > min_log_delta) || last_log_time == std::chrono::steady_clock::time_point{})
		{
			last_log_time = time_now;
			lk.unlock ();
			output (severity_level, std::forward<LogItems> (log_items)...);
			error = false;
		}
		return error;
	}

	/*
	 * @param log_items Output to the log file if the last write was over min_log_delta time ago.
	 * @return true if nothing was logged
	 */
	template <typename... LogItems>
	bool try_log (LogItems &&... log_items)
	{
		return try_log (nano::severity_level::normal, std::forward<LogItems> (log_items)...);
	}

	std::chrono::milliseconds min_log_delta{ 0 };

private:
	std::mutex last_log_time_mutex;
	std::chrono::steady_clock::time_point last_log_time;
	boost::log::sources::severity_logger_mt<severity_level> boost_logger_mt;
};
}
