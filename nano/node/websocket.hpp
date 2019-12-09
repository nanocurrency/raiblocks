#pragma once

#include <nano/boost/beast/core.hpp>
#include <nano/boost/beast/websocket.hpp>
#include <nano/lib/blocks.hpp>
#include <nano/lib/numbers.hpp>

#include <boost/property_tree/json_parser.hpp>

#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/* Boost v1.70 introduced breaking changes; the conditional compilation allows 1.6x to be supported as well. */
#if BOOST_VERSION < 107000
using socket_type = boost::asio::ip::tcp::socket;
#define beast_buffers boost::beast::buffers
#else
using socket_type = boost::asio::basic_stream_socket<boost::asio::ip::tcp, boost::asio::io_context::executor_type>;
#define beast_buffers boost::beast::make_printable
#endif

namespace nano
{
class wallets;
class logger_mt;
class vote;
class election_status;
enum class election_status_type : uint8_t;
namespace websocket
{
	class listener;
	class confirmation_options;

	/** Supported topics */
	enum class topic
	{
		invalid = 0,
		/** Acknowledgement of prior incoming message */
		ack,
		/** A confirmation message */
		confirmation,
		/** Stopped election message (dropped elections due to bounding or block lost the elections) */
		stopped_election,
		/** A vote message **/
		vote,
		/** An active difficulty message */
		active_difficulty,
		/** Work generation message */
		work,
		/** Auxiliary length, not a valid topic, must be the last enum */
		_length
	};
	constexpr size_t number_topics{ static_cast<size_t> (topic::_length) - static_cast<size_t> (topic::invalid) };

	/** A message queued for broadcasting */
	class message final
	{
	public:
		message (nano::websocket::topic topic_a)
			: topic (topic_a)
		{
		}
		message (nano::websocket::topic topic_a, boost::property_tree::ptree & tree_a)
			: topic (topic_a)
			, contents (tree_a)
		{
		}

		std::string to_string () const;
		nano::websocket::topic topic;
		boost::property_tree::ptree contents;
	};

	/** Message builder. This is expanded with new builder functions are necessary. */
	class message_builder final
	{
	public:
		message block_confirmed (std::shared_ptr<nano::block> block_a, nano::account const & account_a, nano::amount const & amount_a, std::string subtype, bool include_block, nano::election_status const & election_status_a, nano::websocket::confirmation_options const & options_a);
		message stopped_election (nano::block_hash const & hash_a);
		message vote_received (std::shared_ptr<nano::vote> vote_a);
		message difficulty_changed (uint64_t publish_threshold_a, uint64_t difficulty_active_a);
		message work_generation (nano::block_hash const & root_a, uint64_t const work_a, uint64_t const difficulty_a, uint64_t const publish_threshold_a, std::chrono::milliseconds const & duration_a, std::string const & peer_a, std::vector<std::string> const & bad_peers_a, bool const completed_a = true, bool const cancelled_a = false);
		message work_cancelled (nano::block_hash const & root_a, uint64_t const difficulty_a, uint64_t const publish_threshold_a, std::chrono::milliseconds const & duration_a, std::vector<std::string> const & bad_peers_a);
		message work_failed (nano::block_hash const & root_a, uint64_t const difficulty_a, uint64_t const publish_threshold_a, std::chrono::milliseconds const & duration_a, std::vector<std::string> const & bad_peers_a);

	private:
		/** Set the common fields for messages: timestamp and topic. */
		void set_common_fields (message & message_a);
	};

	/** Options for subscriptions */
	class options
	{
	public:
		/**
		 * Checks if a message should be filtered for default options (no options given).
		 * @param message_a the message to be checked
		 * @return false - the message should always be broadcasted
		 */
		virtual bool should_filter (message const & message_a) const
		{
			return false;
		}
		virtual ~options () = default;
	};

	/**
	 * Options for block confirmation subscriptions
	 * Non-filtering options:
	 * - "include_block" (bool, default true) - if false, do not include block contents. Only account, amount and hash will be included.
	 * Filtering options:
	 * - "all_local_accounts" (bool) - will only not filter blocks that have local wallet accounts as source/destination
	 * - "accounts" (array of std::strings) - will only not filter blocks that have these accounts as source/destination
	 * @remark Both options can be given, the resulting filter is an intersection of individual filters
	 * @warn Legacy blocks are always filtered (not broadcasted)
	 */
	class confirmation_options final : public options
	{
	public:
		confirmation_options (nano::wallets & wallets_a);
		confirmation_options (boost::property_tree::ptree const & options_a, nano::wallets & wallets_a, nano::logger_mt & logger_a);

		/**
		 * Checks if a message should be filtered for given block confirmation options.
		 * @param message_a the message to be checked
		 * @return false if the message should be broadcasted, true if it should be filtered
		 */
		bool should_filter (message const & message_a) const override;

		/** Returns whether or not block contents should be included */
		bool get_include_block () const
		{
			return include_block;
		}

		/** Returns whether or not to include election info, such as tally and duration */
		bool get_include_election_info () const
		{
			return include_election_info;
		}

		static constexpr const uint8_t type_active_quorum = 1;
		static constexpr const uint8_t type_active_confirmation_height = 2;
		static constexpr const uint8_t type_inactive = 4;
		static constexpr const uint8_t type_all_active = type_active_quorum | type_active_confirmation_height;
		static constexpr const uint8_t type_all = type_all_active | type_inactive;

	private:
		nano::wallets & wallets;
		bool include_election_info{ false };
		bool include_block{ true };
		bool has_account_filtering_options{ false };
		bool all_local_accounts{ false };
		uint8_t confirmation_types{ type_all };
		std::unordered_set<std::string> accounts;
	};

	/**
	 * Filtering options for vote subscriptions
	 * Possible filtering options:
	 * * "representatives" (array of std::strings) - will only broadcast votes from these representatives
	 */
	class vote_options final : public options
	{
	public:
		vote_options ();
		vote_options (boost::property_tree::ptree const & options_a, nano::logger_mt & logger_a);

		/**
		 * Checks if a message should be filtered for given vote received options.
		 * @param message_a the message to be checked
		 * @return false if the message should be broadcasted, true if it should be filtered
		 */
		bool should_filter (message const & message_a) const override;

	private:
		std::unordered_set<std::string> representatives;
	};

	/** A websocket session managing its own lifetime */
	class session final : public std::enable_shared_from_this<session>
	{
		friend class listener;

	public:
		/** Constructor that takes ownership over \p socket_a */
		explicit session (nano::websocket::listener & listener_a, socket_type socket_a);
		~session ();

		/** Perform Websocket handshake and start reading messages */
		void handshake ();

		/** Close the websocket and end the session */
		void close ();

		/** Read the next message. This implicitely handles incoming websocket pings. */
		void read ();

		/** Enqueue \p message_a for writing to the websockets */
		void write (nano::websocket::message message_a);

	private:
		/** The owning listener */
		nano::websocket::listener & ws_listener;
		/** Websocket */
		boost::beast::websocket::stream<socket_type> ws;
		/** Buffer for received messages */
		boost::beast::multi_buffer read_buffer;
		/** All websocket operations that are thread unsafe must go through a strand. */
		boost::asio::strand<boost::asio::io_context::executor_type> strand;
		/** Outgoing messages. The send queue is protected by accessing it only through the strand */
		std::deque<message> send_queue;

		/** Hash functor for topic enums */
		struct topic_hash
		{
			template <typename T>
			std::size_t operator() (T t) const
			{
				return static_cast<std::size_t> (t);
			}
		};
		/** Map of subscriptions -> options registered by this session. */
		std::unordered_map<topic, std::unique_ptr<options>, topic_hash> subscriptions;
		std::mutex subscriptions_mutex;

		/** Handle incoming message */
		void handle_message (boost::property_tree::ptree const & message_a);
		/** Acknowledge incoming message */
		void send_ack (std::string action_a, std::string id_a);
		/** Send all queued messages. This must be called from the write strand. */
		void write_queued_messages ();
	};

	/** Creates a new session for each incoming connection */
	class listener final : public std::enable_shared_from_this<listener>
	{
	public:
		listener (nano::logger_mt & logger_a, nano::wallets & wallets_a, boost::asio::io_context & io_ctx_a, boost::asio::ip::tcp::endpoint endpoint_a);

		/** Start accepting connections */
		void run ();
		void accept ();
		void on_accept (boost::system::error_code ec_a);

		/** Close all websocket sessions and stop listening for new connections */
		void stop ();

		/** Broadcast block confirmation. The content of the message depends on subscription options (such as "include_block") */
		void broadcast_confirmation (std::shared_ptr<nano::block> block_a, nano::account const & account_a, nano::amount const & amount_a, std::string subtype, nano::election_status const & election_status_a);

		/** Broadcast \p message to all session subscribing to the message topic. */
		void broadcast (nano::websocket::message message_a);

		nano::logger_mt & get_logger () const
		{
			return logger;
		}

		nano::wallets & get_wallets () const
		{
			return wallets;
		}

		/**
		 * Per-topic subscribers check. Relies on all sessions correctly increasing and
		 * decreasing the subscriber counts themselves.
		 */
		bool any_subscriber (nano::websocket::topic const & topic_a) const
		{
			return subscriber_count (topic_a) > 0;
		}
		/** Getter for subscriber count of a specific topic*/
		size_t subscriber_count (nano::websocket::topic const & topic_a) const
		{
			return topic_subscriber_count[static_cast<std::size_t> (topic_a)];
		}

	private:
		/** A websocket session can increase and decrease subscription counts. */
		friend nano::websocket::session;

		/** Adds to subscription count of a specific topic*/
		void increase_subscriber_count (nano::websocket::topic const & topic_a);
		/** Removes from subscription count of a specific topic*/
		void decrease_subscriber_count (nano::websocket::topic const & topic_a);

		nano::logger_mt & logger;
		nano::wallets & wallets;
		boost::asio::ip::tcp::acceptor acceptor;
		socket_type socket;
		std::mutex sessions_mutex;
		std::vector<std::weak_ptr<session>> sessions;
		std::array<std::atomic<std::size_t>, number_topics> topic_subscriber_count{};
		std::atomic<bool> stopped{ false };
	};
}
}
