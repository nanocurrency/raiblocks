#pragma once

#include <nano/lib/errors.hpp>
#include <nano/lib/utility.hpp>

#include <boost/circular_buffer.hpp>

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace nano
{
class node;
class tomlconfig;
class jsonconfig;
/**
 * Serialize and deserialize the 'statistics' node from config.json
 * All configuration values have defaults. In particular, file logging of statistics
 * is disabled by default.
 */
class stat_config final
{
public:
	/** Reads the JSON statistics node */
	nano::error deserialize_json (nano::jsonconfig & json);
	nano::error deserialize_toml (nano::tomlconfig & toml);
	nano::error serialize_toml (nano::tomlconfig & toml) const;

	/** If true, sampling of counters is enabled */
	bool sampling_enabled{ false };

	/** How many sample intervals to keep in the ring buffer */
	size_t capacity{ 0 };

	/** Sample interval in milliseconds */
	size_t interval{ 0 };

	/** How often to log sample array, in milliseconds. Default is 0 (no logging) */
	size_t log_interval_samples{ 0 };

	/** How often to log counters, in milliseconds. Default is 0 (no logging) */
	size_t log_interval_counters{ 0 };

	/** Maximum number of log outputs before rotating the file */
	size_t log_rotation_count{ 100 };

	/** If true, write headers on each counter or samples writeout. The header contains log type and the current wall time. */
	bool log_headers{ true };

	/** Filename for the counter log  */
	std::string log_counters_filename{ "counters.stat" };

	/** Filename for the sampling log */
	std::string log_samples_filename{ "samples.stat" };
};

/** Value and wall time of measurement */
class stat_datapoint final
{
public:
	stat_datapoint () = default;
	stat_datapoint (stat_datapoint const & other_a);
	stat_datapoint & operator= (stat_datapoint const & other_a);
	uint64_t get_value () const;
	void set_value (uint64_t value_a);
	std::chrono::system_clock::time_point get_timestamp () const;
	void set_timestamp (std::chrono::system_clock::time_point timestamp_a);
	void add (uint64_t addend, bool update_timestamp = true);

private:
	mutable std::mutex datapoint_mutex;
	/** Value of the sample interval */
	uint64_t value{ 0 };
	/** When the sample was added. This is wall time (system_clock), suitable for display purposes. */
	std::chrono::system_clock::time_point timestamp{ std::chrono::system_clock::now () };
};

/** Bookkeeping of statistics for a specific type/detail/direction combination */
class stat_entry final
{
public:
	stat_entry (size_t capacity, size_t interval)
		: samples (capacity)
		, sample_interval (interval)
	{
	}

	/** Optional samples. Note that this doesn't allocate any memory unless sampling is configured, which sets the capacity. */
	boost::circular_buffer<stat_datapoint> samples;

	/** Start time of current sample interval. This is a steady clock for measuring interval; the datapoint contains the wall time. */
	std::chrono::steady_clock::time_point sample_start_time{ std::chrono::steady_clock::now () };

	/** Sample interval in milliseconds. If 0, sampling is disabled. */
	size_t sample_interval;

	/** Value within the current sample interval */
	stat_datapoint sample_current;

	/** Counting value for this entry, including the time of last update. This is never reset and only increases. */
	stat_datapoint counter;

	/** Zero or more observers for samples. Called at the end of the sample interval. */
	nano::observer_set<boost::circular_buffer<stat_datapoint> &> sample_observers;

	/** Observers for count. Called on each update. */
	nano::observer_set<uint64_t, uint64_t> count_observers;
};

/** Log sink interface */
class stat_log_sink
{
public:
	virtual ~stat_log_sink () = default;

	/** Returns a reference to the log output stream */
	virtual std::ostream & out () = 0;

	/** Called before logging starts */
	virtual void begin ()
	{
	}

	/** Called after logging is completed */
	virtual void finalize ()
	{
	}

	/** Write a header enrty to the log */
	virtual void write_header (std::string const & header, std::chrono::system_clock::time_point & walltime)
	{
	}

	/** Write a counter or sampling entry to the log */
	virtual void write_entry (tm & tm, std::string const & type, std::string const & detail, std::string const & dir, uint64_t value)
	{
	}

	/** Rotates the log (e.g. empty file). This is a no-op for sinks where rotation is not supported. */
	virtual void rotate ()
	{
	}

	/** Returns a reference to the log entry counter */
	size_t & entries ()
	{
		return log_entries;
	}

	/** Returns the string representation of the log. If not supported, an empty string is returned. */
	virtual std::string to_string ()
	{
		return "";
	}

	/**
	 * Returns the object representation of the log result. The type depends on the sink used.
	 * @returns Object, or nullptr if no object result is available.
	 */
	virtual void * to_object ()
	{
		return nullptr;
	}

protected:
	std::string tm_to_string (tm & tm);
	size_t log_entries{ 0 };
};

/**
 * Collects counts and samples for inbound and outbound traffic, blocks, errors, and so on.
 * Stats can be queried and observed on a type level (such as message and ledger) as well as a more
 * specific detail level (such as send blocks)
 */
class stat final
{
public:
	/** Primary statistics type */
	enum class type : uint8_t
	{
		traffic_udp,
		traffic_tcp,
		error,
		message,
		block,
		ledger,
		rollback,
		bootstrap,
		vote,
		election,
		http_callback,
		peering,
		ipc,
		tcp,
		udp,
		observer,
		confirmation_height,
		drop
	};

	/** Optional detail type */
	enum class detail : uint8_t
	{
		all = 0,

		// error specific
		bad_sender,
		insufficient_work,
		http_callback,
		unreachable_host,

		// observer specific
		observer_confirmation_active_quorum,
		observer_confirmation_active_conf_height,
		observer_confirmation_inactive,

		// ledger, block, bootstrap
		send,
		receive,
		open,
		change,
		state_block,
		epoch_block,
		fork,

		// message specific
		keepalive,
		publish,
		republish_vote,
		confirm_req,
		confirm_ack,
		node_id_handshake,

		// bootstrap, callback
		initiate,
		initiate_lazy,
		initiate_wallet_lazy,

		// bootstrap specific
		bulk_pull,
		bulk_pull_account,
		bulk_pull_deserialize_receive_block,
		bulk_pull_error_starting_request,
		bulk_pull_failed_account,
		bulk_pull_receive_block_failure,
		bulk_pull_request_failure,
		bulk_push,
		frontier_req,
		frontier_confirmation_failed,
		frontier_confirmation_successful,
		error_socket_close,

		// vote specific
		vote_valid,
		vote_replay,
		vote_invalid,
		vote_overflow,

		// election specific
		vote_new,
		vote_cached,
		late_block,
		late_block_seconds,

		// udp
		blocking,
		overflow,
		invalid_magic,
		invalid_network,
		invalid_header,
		invalid_message_type,
		invalid_keepalive_message,
		invalid_publish_message,
		invalid_confirm_req_message,
		invalid_confirm_ack_message,
		invalid_node_id_handshake_message,
		outdated_version,

		// tcp
		tcp_accept_success,
		tcp_accept_failure,
		tcp_write_drop,

		// ipc
		invocations,

		// peering
		handshake,

		// confirmation height
		blocks_confirmed,
		invalid_block
	};

	/** Direction of the stat. If the direction is irrelevant, use in */
	enum class dir : uint8_t
	{
		in,
		out
	};

	/** Constructor using the default config values */
	stat () = default;

	/**
	 * Initialize stats with a config.
	 * @param config Configuration object; deserialized from config.json
	 */
	stat (nano::stat_config config);

	/**
	 * Call this to override the default sample interval and capacity, for a specific stat entry.
	 * This must be called before any stat entries are added, as part of the node initialiation.
	 */
	void configure (stat::type type, stat::detail detail, stat::dir dir, size_t interval, size_t capacity)
	{
		get_entry (key_of (type, detail, dir), interval, capacity);
	}

	/**
	 * Disables sampling for a given type/detail/dir combination
	 */
	void disable_sampling (stat::type type, stat::detail detail, stat::dir dir)
	{
		auto entry = get_entry (key_of (type, detail, dir));
		entry->sample_interval = 0;
	}

	/** Increments the given counter */
	void inc (stat::type type, stat::dir dir = stat::dir::in)
	{
		add (type, dir, 1);
	}

	/** Increments the counter for \detail, but doesn't update at the type level */
	void inc_detail_only (stat::type type, stat::detail detail, stat::dir dir = stat::dir::in)
	{
		add (type, detail, dir, 1);
	}

	/** Increments the given counter */
	void inc (stat::type type, stat::detail detail, stat::dir dir = stat::dir::in)
	{
		add (type, detail, dir, 1);
	}

	/** Adds \p value to the given counter */
	void add (stat::type type, stat::dir dir, uint64_t value)
	{
		add (type, detail::all, dir, value);
	}

	/**
	 * Add \p value to stat. If sampling is configured, this will update the current sample and
	 * call any sample observers if the interval is over.
	 *
	 * @param type Main statistics type
	 * @param detail Detail type, or detail::none to register on type-level only
	 * @param dir Direction
	 * @param value The amount to add
	 * @param detail_only If true, only update the detail-level counter
	 */
	void add (stat::type type, stat::detail detail, stat::dir dir, uint64_t value, bool detail_only = false)
	{
		constexpr uint32_t no_detail_mask = 0xffff00ff;
		uint32_t key = key_of (type, detail, dir);

		update (key, value);

		// Optionally update at type-level as well
		if (!detail_only && (key & no_detail_mask) != key)
		{
			update (key & no_detail_mask, value);
		}
	}

	/**
	 * Add a sampling observer for a given counter.
	 * The observer receives a snapshot of the current sampling. Accessing the sample buffer is thus thread safe.
	 * To avoid recursion, the observer callback must only use the received data point snapshop, not query the stat object.
	 * @param observer The observer receives a snapshot of the current samples.
	 */
	void observe_sample (stat::type type, stat::detail detail, stat::dir dir, std::function<void(boost::circular_buffer<stat_datapoint> &)> observer)
	{
		get_entry (key_of (type, detail, dir))->sample_observers.add (observer);
	}

	void observe_sample (stat::type type, stat::dir dir, std::function<void(boost::circular_buffer<stat_datapoint> &)> observer)
	{
		observe_sample (type, stat::detail::all, dir, observer);
	}

	/**
	 * Add count observer for a given type, detail and direction combination. The observer receives old and new value.
	 * To avoid recursion, the observer callback must only use the received counts, not query the stat object.
	 * @param observer The observer receives the old and the new count.
	 */
	void observe_count (stat::type type, stat::detail detail, stat::dir dir, std::function<void(uint64_t, uint64_t)> observer)
	{
		get_entry (key_of (type, detail, dir))->count_observers.add (observer);
	}

	/** Returns a potentially empty list of the last N samples, where N is determined by the 'capacity' configuration */
	boost::circular_buffer<stat_datapoint> * samples (stat::type type, stat::detail detail, stat::dir dir)
	{
		return &get_entry (key_of (type, detail, dir))->samples;
	}

	/** Returns current value for the given counter at the type level */
	uint64_t count (stat::type type, stat::dir dir = stat::dir::in)
	{
		return count (type, stat::detail::all, dir);
	}

	/** Returns current value for the given counter at the detail level */
	uint64_t count (stat::type type, stat::detail detail, stat::dir dir = stat::dir::in)
	{
		return get_entry (key_of (type, detail, dir))->counter.get_value ();
	}

	/** Returns the number of seconds since clear() was last called, or node startup if it's never called. */
	std::chrono::seconds last_reset ();

	/** Clear all stats */
	void clear ();

	/** Log counters to the given log link */
	void log_counters (stat_log_sink & sink);

	/** Log samples to the given log sink */
	void log_samples (stat_log_sink & sink);

	/** Returns a new JSON log sink */
	std::unique_ptr<stat_log_sink> log_sink_json () const;

	/** Returns string representation of detail */
	static std::string detail_to_string (uint32_t key);

	/** Stop stats being output */
	void stop ();

private:
	static std::string type_to_string (uint32_t key);
	static std::string dir_to_string (uint32_t key);

	/** Constructs a key given type, detail and direction. This is used as input to update(...) and get_entry(...) */
	uint32_t key_of (stat::type type, stat::detail detail, stat::dir dir) const
	{
		return static_cast<uint8_t> (type) << 16 | static_cast<uint8_t> (detail) << 8 | static_cast<uint8_t> (dir);
	}

	/** Get entry for key, creating a new entry if necessary, using interval and sample count from config */
	std::shared_ptr<nano::stat_entry> get_entry (uint32_t key);

	/** Get entry for key, creating a new entry if necessary */
	std::shared_ptr<nano::stat_entry> get_entry (uint32_t key, size_t sample_interval, size_t max_samples);

	/** Unlocked implementation of get_entry() */
	std::shared_ptr<nano::stat_entry> get_entry_impl (uint32_t key, size_t sample_interval, size_t max_samples);

	/**
	 * Update count and sample and call any observers on the key
	 * @param key a key constructor from stat::type, stat::detail and stat::direction
	 * @value Amount to add to the counter
	 */
	void update (uint32_t key, uint64_t value);

	/** Unlocked implementation of log_counters() to avoid using recursive locking */
	void log_counters_impl (stat_log_sink & sink);

	/** Unlocked implementation of log_samples() to avoid using recursive locking */
	void log_samples_impl (stat_log_sink & sink);

	/** Time of last clear() call */
	std::chrono::steady_clock::time_point timestamp{ std::chrono::steady_clock::now () };

	/** Configuration deserialized from config.json */
	nano::stat_config config;

	/** Stat entries are sorted by key to simplify processing of log output */
	std::map<uint32_t, std::shared_ptr<nano::stat_entry>> entries;
	std::chrono::steady_clock::time_point log_last_count_writeout{ std::chrono::steady_clock::now () };
	std::chrono::steady_clock::time_point log_last_sample_writeout{ std::chrono::steady_clock::now () };

	/** Whether stats should be output */
	bool stopped{ false };

	/** All access to stat is thread safe, including calls from observers on the same thread */
	std::mutex stat_mutex;
};
}
