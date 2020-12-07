#include <nano/boost/asio/bind_executor.hpp>
#include <nano/boost/asio/dispatch.hpp>
#include <nano/boost/asio/strand.hpp>
#include <nano/lib/tlsconfig.hpp>
#include <nano/lib/work.hpp>
#include <nano/node/election.hpp>
#include <nano/node/transport/transport.hpp>
#include <nano/node/wallet.hpp>
#include <nano/node/websocket.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <algorithm>
#include <chrono>

nano::websocket::confirmation_options::confirmation_options (nano::wallets & wallets_a) :
wallets (wallets_a)
{
}

nano::websocket::confirmation_options::confirmation_options (boost::property_tree::ptree const & options_a, nano::wallets & wallets_a, nano::logger_mt & logger_a) :
wallets (wallets_a),
logger (logger_a)
{
	// Non-account filtering options
	include_block = options_a.get<bool> ("include_block", true);
	include_election_info = options_a.get<bool> ("include_election_info", false);

	confirmation_types = 0;
	auto type_l (options_a.get<std::string> ("confirmation_type", "all"));

	if (boost::iequals (type_l, "active"))
	{
		confirmation_types = type_all_active;
	}
	else if (boost::iequals (type_l, "active_quorum"))
	{
		confirmation_types = type_active_quorum;
	}
	else if (boost::iequals (type_l, "active_confirmation_height"))
	{
		confirmation_types = type_active_confirmation_height;
	}
	else if (boost::iequals (type_l, "inactive"))
	{
		confirmation_types = type_inactive;
	}
	else
	{
		confirmation_types = type_all;
	}

	// Account filtering options
	auto all_local_accounts_l (options_a.get_optional<bool> ("all_local_accounts"));
	if (all_local_accounts_l.is_initialized ())
	{
		all_local_accounts = all_local_accounts_l.get ();
		has_account_filtering_options = true;

		if (!include_block)
		{
			logger_a.always_log ("Websocket: Filtering option \"all_local_accounts\" requires that \"include_block\" is set to true to be effective");
		}
	}
	auto accounts_l (options_a.get_child_optional ("accounts"));
	if (accounts_l)
	{
		has_account_filtering_options = true;
		for (auto account_l : *accounts_l)
		{
			nano::account result_l (0);
			if (!result_l.decode_account (account_l.second.data ()))
			{
				// Do not insert the given raw data to keep old prefix support
				accounts.insert (result_l.to_account ());
			}
			else
			{
				logger_a.always_log ("Websocket: invalid account provided for filtering blocks: ", account_l.second.data ());
			}
		}

		if (!include_block)
		{
			logger_a.always_log ("Websocket: Filtering option \"accounts\" requires that \"include_block\" is set to true to be effective");
		}
	}
	check_filter_empty ();
}

bool nano::websocket::confirmation_options::should_filter (nano::websocket::message const & message_a) const
{
	bool should_filter_conf_type_l (true);

	auto type_text_l (message_a.contents.get<std::string> ("message.confirmation_type"));
	if (type_text_l == "active_quorum" && confirmation_types & type_active_quorum)
	{
		should_filter_conf_type_l = false;
	}
	else if (type_text_l == "active_confirmation_height" && confirmation_types & type_active_confirmation_height)
	{
		should_filter_conf_type_l = false;
	}
	else if (type_text_l == "inactive" && confirmation_types & type_inactive)
	{
		should_filter_conf_type_l = false;
	}

	bool should_filter_account (has_account_filtering_options);
	auto destination_opt_l (message_a.contents.get_optional<std::string> ("message.block.link_as_account"));
	if (destination_opt_l)
	{
		auto source_text_l (message_a.contents.get<std::string> ("message.account"));
		if (all_local_accounts)
		{
			auto transaction_l (wallets.tx_begin_read ());
			nano::account source_l (0), destination_l (0);
			auto decode_source_ok_l (!source_l.decode_account (source_text_l));
			auto decode_destination_ok_l (!destination_l.decode_account (destination_opt_l.get ()));
			(void)decode_source_ok_l;
			(void)decode_destination_ok_l;
			debug_assert (decode_source_ok_l && decode_destination_ok_l);
			if (wallets.exists (transaction_l, source_l) || wallets.exists (transaction_l, destination_l))
			{
				should_filter_account = false;
			}
		}
		if (accounts.find (source_text_l) != accounts.end () || accounts.find (destination_opt_l.get ()) != accounts.end ())
		{
			should_filter_account = false;
		}
	}

	return should_filter_conf_type_l || should_filter_account;
}

bool nano::websocket::confirmation_options::update (boost::property_tree::ptree const & options_a)
{
	auto update_accounts = [this](boost::property_tree::ptree const & accounts_text_a, bool insert_a) {
		this->has_account_filtering_options = true;
		for (auto const & account_l : accounts_text_a)
		{
			nano::account result_l (0);
			if (!result_l.decode_account (account_l.second.data ()))
			{
				// Re-encode to keep old prefix support
				auto encoded_l (result_l.to_account ());
				if (insert_a)
				{
					this->accounts.insert (encoded_l);
				}
				else
				{
					this->accounts.erase (encoded_l);
				}
			}
			else if (this->logger.is_initialized ())
			{
				this->logger->always_log ("Websocket: invalid account provided for filtering blocks: ", account_l.second.data ());
			}
		}
	};

	// Adding accounts as filter exceptions
	auto accounts_add_l (options_a.get_child_optional ("accounts_add"));
	if (accounts_add_l)
	{
		update_accounts (*accounts_add_l, true);
	}

	// Removing accounts as filter exceptions
	auto accounts_del_l (options_a.get_child_optional ("accounts_del"));
	if (accounts_del_l)
	{
		update_accounts (*accounts_del_l, false);
	}

	check_filter_empty ();
	return false;
}

void nano::websocket::confirmation_options::check_filter_empty () const
{
	// Warn the user if the options resulted in an empty filter
	if (logger.is_initialized () && has_account_filtering_options && !all_local_accounts && accounts.empty ())
	{
		logger->always_log ("Websocket: provided options resulted in an empty block confirmation filter");
	}
}

nano::websocket::vote_options::vote_options (boost::property_tree::ptree const & options_a, nano::logger_mt & logger_a)
{
	include_replays = options_a.get<bool> ("include_replays", false);
	include_indeterminate = options_a.get<bool> ("include_indeterminate", false);
	auto representatives_l (options_a.get_child_optional ("representatives"));
	if (representatives_l)
	{
		for (auto representative_l : *representatives_l)
		{
			nano::account result_l (0);
			if (!result_l.decode_account (representative_l.second.data ()))
			{
				// Do not insert the given raw data to keep old prefix support
				representatives.insert (result_l.to_account ());
			}
			else
			{
				logger_a.always_log ("Websocket: invalid account given to filter votes: ", representative_l.second.data ());
			}
		}
		// Warn the user if the option will be ignored
		if (representatives.empty ())
		{
			logger_a.always_log ("Websocket: account filter for votes is empty, no messages will be filtered");
		}
	}
}

bool nano::websocket::vote_options::should_filter (nano::websocket::message const & message_a) const
{
	auto type (message_a.contents.get<std::string> ("message.type"));
	bool should_filter_l = (!include_replays && type == "replay") || (!include_indeterminate && type == "indeterminate");
	if (!should_filter_l && !representatives.empty ())
	{
		auto representative_text_l (message_a.contents.get<std::string> ("message.account"));
		if (representatives.find (representative_text_l) == representatives.end ())
		{
			should_filter_l = true;
		}
	}
	return should_filter_l;
}

#ifdef NANO_SECURE_RPC

nano::websocket::session::session (nano::websocket::listener & listener_a, socket_type socket_a, boost::asio::ssl::context & ctx_a) :
ws_listener (listener_a), ws (std::move (socket_a), ctx_a)
{
	ws_listener.get_logger ().try_log ("Websocket: secure session started");
}

#endif

nano::websocket::session::session (nano::websocket::listener & listener_a, socket_type socket_a) :
ws_listener (listener_a), ws (std::move (socket_a))
{
	ws_listener.get_logger ().try_log ("Websocket: session started");
}

nano::websocket::session::~session ()
{
	{
		nano::unique_lock<std::mutex> lk (subscriptions_mutex);
		for (auto & subscription : subscriptions)
		{
			ws_listener.decrease_subscriber_count (subscription.first);
		}
	}
}

void nano::websocket::session::handshake ()
{
	auto this_l (shared_from_this ());
	ws.handshake ([this_l](boost::system::error_code const & ec) {
		if (!ec)
		{
			// Start reading incoming messages
			this_l->read ();
		}
		else
		{
			this_l->ws_listener.get_logger ().always_log ("Websocket: handshake failed: ", ec.message ());
		}
	});
}

void nano::websocket::session::close ()
{
	ws_listener.get_logger ().try_log ("Websocket: session closing");

	auto this_l (shared_from_this ());
	boost::asio::dispatch (ws.get_strand (),
	[this_l]() {
		boost::beast::websocket::close_reason reason;
		reason.code = boost::beast::websocket::close_code::normal;
		reason.reason = "Shutting down";
		boost::system::error_code ec_ignore;
		this_l->ws.close (reason, ec_ignore);
	});
}

void nano::websocket::session::write (nano::websocket::message message_a)
{
	nano::unique_lock<std::mutex> lk (subscriptions_mutex);
	auto subscription (subscriptions.find (message_a.topic));
	if (message_a.topic == nano::websocket::topic::ack || (subscription != subscriptions.end () && !subscription->second->should_filter (message_a)))
	{
		lk.unlock ();
		auto this_l (shared_from_this ());
		boost::asio::post (ws.get_strand (),
		[message_a, this_l]() {
			bool write_in_progress = !this_l->send_queue.empty ();
			this_l->send_queue.emplace_back (message_a);
			if (!write_in_progress)
			{
				this_l->write_queued_messages ();
			}
		});
	}
}

void nano::websocket::session::write_queued_messages ()
{
	auto msg (send_queue.front ().to_string ());
	auto this_l (shared_from_this ());

	ws.async_write (nano::shared_const_buffer (msg),
	[this_l](boost::system::error_code ec, std::size_t bytes_transferred) {
		this_l->send_queue.pop_front ();
		if (!ec)
		{
			if (!this_l->send_queue.empty ())
			{
				this_l->write_queued_messages ();
			}
		}
	});
}

void nano::websocket::session::read ()
{
	auto this_l (shared_from_this ());

	boost::asio::post (ws.get_strand (), [this_l]() {
		this_l->ws.async_read (this_l->read_buffer,
		[this_l](boost::system::error_code ec, std::size_t bytes_transferred) {
			if (!ec)
			{
				std::stringstream os;
				os << beast_buffers (this_l->read_buffer.data ());
				std::string incoming_message = os.str ();

				// Prepare next read by clearing the multibuffer
				this_l->read_buffer.consume (this_l->read_buffer.size ());

				boost::property_tree::ptree tree_msg;
				try
				{
					boost::property_tree::read_json (os, tree_msg);
					this_l->handle_message (tree_msg);
					this_l->read ();
				}
				catch (boost::property_tree::json_parser::json_parser_error const & ex)
				{
					this_l->ws_listener.get_logger ().try_log ("Websocket: json parsing failed: ", ex.what ());
				}
			}
			else if (ec != boost::asio::error::eof)
			{
				this_l->ws_listener.get_logger ().try_log ("Websocket: read failed: ", ec.message ());
			}
		});
	});
}

namespace
{
nano::websocket::topic to_topic (std::string const & topic_a)
{
	nano::websocket::topic topic = nano::websocket::topic::invalid;
	if (topic_a == "confirmation")
	{
		topic = nano::websocket::topic::confirmation;
	}
	else if (topic_a == "stopped_election")
	{
		topic = nano::websocket::topic::stopped_election;
	}
	else if (topic_a == "vote")
	{
		topic = nano::websocket::topic::vote;
	}
	else if (topic_a == "ack")
	{
		topic = nano::websocket::topic::ack;
	}
	else if (topic_a == "active_difficulty")
	{
		topic = nano::websocket::topic::active_difficulty;
	}
	else if (topic_a == "work")
	{
		topic = nano::websocket::topic::work;
	}
	else if (topic_a == "bootstrap")
	{
		topic = nano::websocket::topic::bootstrap;
	}
	else if (topic_a == "telemetry")
	{
		topic = nano::websocket::topic::telemetry;
	}
	else if (topic_a == "new_unconfirmed_block")
	{
		topic = nano::websocket::topic::new_unconfirmed_block;
	}

	return topic;
}

std::string from_topic (nano::websocket::topic topic_a)
{
	std::string topic = "invalid";
	if (topic_a == nano::websocket::topic::confirmation)
	{
		topic = "confirmation";
	}
	else if (topic_a == nano::websocket::topic::stopped_election)
	{
		topic = "stopped_election";
	}
	else if (topic_a == nano::websocket::topic::vote)
	{
		topic = "vote";
	}
	else if (topic_a == nano::websocket::topic::ack)
	{
		topic = "ack";
	}
	else if (topic_a == nano::websocket::topic::active_difficulty)
	{
		topic = "active_difficulty";
	}
	else if (topic_a == nano::websocket::topic::work)
	{
		topic = "work";
	}
	else if (topic_a == nano::websocket::topic::bootstrap)
	{
		topic = "bootstrap";
	}
	else if (topic_a == nano::websocket::topic::telemetry)
	{
		topic = "telemetry";
	}
	else if (topic_a == nano::websocket::topic::new_unconfirmed_block)
	{
		topic = "new_unconfirmed_block";
	}

	return topic;
}
}

void nano::websocket::session::send_ack (std::string action_a, std::string id_a)
{
	auto milli_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now ().time_since_epoch ()).count ();
	nano::websocket::message msg (nano::websocket::topic::ack);
	boost::property_tree::ptree & message_l = msg.contents;
	message_l.add ("ack", action_a);
	message_l.add ("time", std::to_string (milli_since_epoch));
	if (!id_a.empty ())
	{
		message_l.add ("id", id_a);
	}
	write (msg);
}

void nano::websocket::session::handle_message (boost::property_tree::ptree const & message_a)
{
	std::string action (message_a.get<std::string> ("action", ""));
	auto topic_l (to_topic (message_a.get<std::string> ("topic", "")));
	auto ack_l (message_a.get<bool> ("ack", false));
	auto id_l (message_a.get<std::string> ("id", ""));
	auto action_succeeded (false);
	if (action == "subscribe" && topic_l != nano::websocket::topic::invalid)
	{
		auto options_text_l (message_a.get_child_optional ("options"));
		nano::lock_guard<std::mutex> lk (subscriptions_mutex);
		std::unique_ptr<nano::websocket::options> options_l{ nullptr };
		if (options_text_l && topic_l == nano::websocket::topic::confirmation)
		{
			options_l = std::make_unique<nano::websocket::confirmation_options> (options_text_l.get (), ws_listener.get_wallets (), ws_listener.get_logger ());
		}
		else if (options_text_l && topic_l == nano::websocket::topic::vote)
		{
			options_l = std::make_unique<nano::websocket::vote_options> (options_text_l.get (), ws_listener.get_logger ());
		}
		else
		{
			options_l = std::make_unique<nano::websocket::options> ();
		}
		auto existing (subscriptions.find (topic_l));
		if (existing != subscriptions.end ())
		{
			existing->second = std::move (options_l);
			ws_listener.get_logger ().always_log ("Websocket: updated subscription to topic: ", from_topic (topic_l));
		}
		else
		{
			subscriptions.emplace (topic_l, std::move (options_l));
			ws_listener.get_logger ().always_log ("Websocket: new subscription to topic: ", from_topic (topic_l));
			ws_listener.increase_subscriber_count (topic_l);
		}
		action_succeeded = true;
	}
	else if (action == "update")
	{
		nano::lock_guard<std::mutex> lk (subscriptions_mutex);
		auto existing (subscriptions.find (topic_l));
		if (existing != subscriptions.end ())
		{
			auto options_text_l (message_a.get_child_optional ("options"));
			if (options_text_l.is_initialized () && !existing->second->update (*options_text_l))
			{
				action_succeeded = true;
			}
		}
	}
	else if (action == "unsubscribe" && topic_l != nano::websocket::topic::invalid)
	{
		nano::lock_guard<std::mutex> lk (subscriptions_mutex);
		if (subscriptions.erase (topic_l))
		{
			ws_listener.get_logger ().always_log ("Websocket: removed subscription to topic: ", from_topic (topic_l));
			ws_listener.decrease_subscriber_count (topic_l);
		}
		action_succeeded = true;
	}
	else if (action == "ping")
	{
		action_succeeded = true;
		ack_l = "true";
		action = "pong";
	}
	if (ack_l && action_succeeded)
	{
		send_ack (action, id_l);
	}
}

void nano::websocket::listener::stop ()
{
	stopped = true;
	acceptor.close ();

	nano::lock_guard<std::mutex> lk (sessions_mutex);
	for (auto & weak_session : sessions)
	{
		auto session_ptr (weak_session.lock ());
		if (session_ptr)
		{
			session_ptr->close ();
		}
	}
	sessions.clear ();
}

nano::websocket::listener::listener (std::shared_ptr<nano::tls_config> const & tls_config_a, nano::logger_mt & logger_a, nano::wallets & wallets_a, boost::asio::io_context & io_ctx_a, boost::asio::ip::tcp::endpoint endpoint_a) :
tls_config (tls_config_a),
logger (logger_a),
wallets (wallets_a),
acceptor (io_ctx_a),
socket (io_ctx_a)
{
	try
	{
		for (std::atomic<std::size_t> & item : topic_subscriber_count)
		{
			item = std::size_t (0);
		}
		acceptor.open (endpoint_a.protocol ());
		acceptor.set_option (boost::asio::socket_base::reuse_address (true));
		acceptor.bind (endpoint_a);
		acceptor.listen (boost::asio::socket_base::max_listen_connections);
	}
	catch (std::exception const & ex)
	{
		logger.always_log ("Websocket: listen failed: ", ex.what ());
	}
}

void nano::websocket::listener::run ()
{
	if (acceptor.is_open ())
	{
		accept ();
	}
}

void nano::websocket::listener::accept ()
{
	auto this_l (shared_from_this ());
	acceptor.async_accept (socket,
	[this_l](boost::system::error_code const & ec) {
		this_l->on_accept (ec);
	});
}

void nano::websocket::listener::on_accept (boost::system::error_code ec)
{
	if (ec)
	{
		logger.always_log ("Websocket: accept failed: ", ec.message ());
	}
	else
	{
		// Create the session and initiate websocket handshake
		std::shared_ptr<nano::websocket::session> session;
		if (tls_config && tls_config->enable_wss)
		{
#ifdef NANO_SECURE_RPC
			session = std::make_shared<nano::websocket::session> (*this, std::move (socket), tls_config->ssl_context);
#endif
		}
		else
		{
			session = std::make_shared<nano::websocket::session> (*this, std::move (socket));
		}

		sessions_mutex.lock ();
		sessions.push_back (session);
		// Clean up expired sessions
		sessions.erase (std::remove_if (sessions.begin (), sessions.end (), [](auto & elem) { return elem.expired (); }), sessions.end ());
		sessions_mutex.unlock ();
		session->handshake ();
	}

	if (!stopped)
	{
		accept ();
	}
}

void nano::websocket::listener::broadcast_confirmation (std::shared_ptr<nano::block> block_a, nano::account const & account_a, nano::amount const & amount_a, std::string subtype, nano::election_status const & election_status_a)
{
	nano::websocket::message_builder builder;

	nano::lock_guard<std::mutex> lk (sessions_mutex);
	boost::optional<nano::websocket::message> msg_with_block;
	boost::optional<nano::websocket::message> msg_without_block;
	for (auto & weak_session : sessions)
	{
		auto session_ptr (weak_session.lock ());
		if (session_ptr)
		{
			auto subscription (session_ptr->subscriptions.find (nano::websocket::topic::confirmation));
			if (subscription != session_ptr->subscriptions.end ())
			{
				nano::websocket::confirmation_options default_options (wallets);
				auto conf_options (dynamic_cast<nano::websocket::confirmation_options *> (subscription->second.get ()));
				if (conf_options == nullptr)
				{
					conf_options = &default_options;
				}
				auto include_block (conf_options == nullptr ? true : conf_options->get_include_block ());

				if (include_block && !msg_with_block)
				{
					msg_with_block = builder.block_confirmed (block_a, account_a, amount_a, subtype, include_block, election_status_a, *conf_options);
				}
				else if (!include_block && !msg_without_block)
				{
					msg_without_block = builder.block_confirmed (block_a, account_a, amount_a, subtype, include_block, election_status_a, *conf_options);
				}
				else
				{
					debug_assert (false);
				}

				session_ptr->write (include_block ? msg_with_block.get () : msg_without_block.get ());
			}
		}
	}
}

void nano::websocket::listener::broadcast (nano::websocket::message message_a)
{
	nano::lock_guard<std::mutex> lk (sessions_mutex);
	for (auto & weak_session : sessions)
	{
		auto session_ptr (weak_session.lock ());
		if (session_ptr)
		{
			session_ptr->write (message_a);
		}
	}
}

void nano::websocket::listener::increase_subscriber_count (nano::websocket::topic const & topic_a)
{
	topic_subscriber_count[static_cast<std::size_t> (topic_a)] += 1;
}

void nano::websocket::listener::decrease_subscriber_count (nano::websocket::topic const & topic_a)
{
	auto & count (topic_subscriber_count[static_cast<std::size_t> (topic_a)]);
	release_assert (count > 0);
	count -= 1;
}

nano::websocket::message nano::websocket::message_builder::stopped_election (nano::block_hash const & hash_a)
{
	nano::websocket::message message_l (nano::websocket::topic::stopped_election);
	set_common_fields (message_l);

	boost::property_tree::ptree message_node_l;
	message_node_l.add ("hash", hash_a.to_string ());
	message_l.contents.add_child ("message", message_node_l);

	return message_l;
}

nano::websocket::message nano::websocket::message_builder::block_confirmed (std::shared_ptr<nano::block> block_a, nano::account const & account_a, nano::amount const & amount_a, std::string subtype, bool include_block_a, nano::election_status const & election_status_a, nano::websocket::confirmation_options const & options_a)
{
	nano::websocket::message message_l (nano::websocket::topic::confirmation);
	set_common_fields (message_l);

	// Block confirmation properties
	boost::property_tree::ptree message_node_l;
	message_node_l.add ("account", account_a.to_account ());
	message_node_l.add ("amount", amount_a.to_string_dec ());
	message_node_l.add ("hash", block_a->hash ().to_string ());

	std::string confirmation_type = "unknown";
	switch (election_status_a.type)
	{
		case nano::election_status_type::active_confirmed_quorum:
			confirmation_type = "active_quorum";
			break;
		case nano::election_status_type::active_confirmation_height:
			confirmation_type = "active_confirmation_height";
			break;
		case nano::election_status_type::inactive_confirmation_height:
			confirmation_type = "inactive";
			break;
		default:
			break;
	};
	message_node_l.add ("confirmation_type", confirmation_type);

	if (options_a.get_include_election_info ())
	{
		boost::property_tree::ptree election_node_l;
		election_node_l.add ("duration", election_status_a.election_duration.count ());
		election_node_l.add ("time", election_status_a.election_end.count ());
		election_node_l.add ("tally", election_status_a.tally.to_string_dec ());
		election_node_l.add ("blocks", std::to_string (election_status_a.block_count));
		election_node_l.add ("voters", std::to_string (election_status_a.voter_count));
		election_node_l.add ("request_count", std::to_string (election_status_a.confirmation_request_count));
		message_node_l.add_child ("election_info", election_node_l);
	}

	if (include_block_a)
	{
		boost::property_tree::ptree block_node_l;
		block_a->serialize_json (block_node_l);
		if (!subtype.empty ())
		{
			block_node_l.add ("subtype", subtype);
		}
		message_node_l.add_child ("block", block_node_l);
	}

	message_l.contents.add_child ("message", message_node_l);

	return message_l;
}

nano::websocket::message nano::websocket::message_builder::vote_received (std::shared_ptr<nano::vote> vote_a, nano::vote_code code_a)
{
	nano::websocket::message message_l (nano::websocket::topic::vote);
	set_common_fields (message_l);

	// Vote information
	boost::property_tree::ptree vote_node_l;
	vote_a->serialize_json (vote_node_l);

	// Vote processing information
	std::string vote_type = "invalid";
	switch (code_a)
	{
		case nano::vote_code::vote:
			vote_type = "vote";
			break;
		case nano::vote_code::replay:
			vote_type = "replay";
			break;
		case nano::vote_code::indeterminate:
			vote_type = "indeterminate";
			break;
		case nano::vote_code::invalid:
			debug_assert (false);
			break;
	}
	vote_node_l.put ("type", vote_type);
	message_l.contents.add_child ("message", vote_node_l);
	return message_l;
}

nano::websocket::message nano::websocket::message_builder::difficulty_changed (uint64_t publish_threshold_a, uint64_t receive_threshold_a, uint64_t difficulty_active_a)
{
	nano::websocket::message message_l (nano::websocket::topic::active_difficulty);
	set_common_fields (message_l);

	// Active difficulty information
	boost::property_tree::ptree difficulty_l;
	difficulty_l.put ("network_minimum", nano::to_string_hex (publish_threshold_a));
	difficulty_l.put ("network_receive_minimum", nano::to_string_hex (receive_threshold_a));
	difficulty_l.put ("network_current", nano::to_string_hex (difficulty_active_a));
	auto multiplier = nano::difficulty::to_multiplier (difficulty_active_a, publish_threshold_a);
	difficulty_l.put ("multiplier", nano::to_string (multiplier));
	auto const receive_current_denormalized (nano::denormalized_multiplier (multiplier, nano::network_params{}.network.publish_thresholds.epoch_2_receive));
	difficulty_l.put ("network_receive_current", nano::to_string_hex (nano::difficulty::from_multiplier (receive_current_denormalized, receive_threshold_a)));

	message_l.contents.add_child ("message", difficulty_l);
	return message_l;
}

nano::websocket::message nano::websocket::message_builder::work_generation (nano::work_version const version_a, nano::block_hash const & root_a, uint64_t work_a, uint64_t difficulty_a, uint64_t publish_threshold_a, std::chrono::milliseconds const & duration_a, std::string const & peer_a, std::vector<std::string> const & bad_peers_a, bool completed_a, bool cancelled_a)
{
	nano::websocket::message message_l (nano::websocket::topic::work);
	set_common_fields (message_l);

	// Active difficulty information
	boost::property_tree::ptree work_l;
	work_l.put ("success", completed_a ? "true" : "false");
	work_l.put ("reason", completed_a ? "" : cancelled_a ? "cancelled" : "failure");
	work_l.put ("duration", duration_a.count ());

	boost::property_tree::ptree request_l;
	request_l.put ("version", nano::to_string (version_a));
	request_l.put ("hash", root_a.to_string ());
	request_l.put ("difficulty", nano::to_string_hex (difficulty_a));
	auto request_multiplier_l (nano::difficulty::to_multiplier (difficulty_a, publish_threshold_a));
	request_l.put ("multiplier", nano::to_string (request_multiplier_l));
	work_l.add_child ("request", request_l);

	if (completed_a)
	{
		boost::property_tree::ptree result_l;
		result_l.put ("source", peer_a);
		result_l.put ("work", nano::to_string_hex (work_a));
		auto result_difficulty_l (nano::work_difficulty (version_a, root_a, work_a));
		result_l.put ("difficulty", nano::to_string_hex (result_difficulty_l));
		auto result_multiplier_l (nano::difficulty::to_multiplier (result_difficulty_l, publish_threshold_a));
		result_l.put ("multiplier", nano::to_string (result_multiplier_l));
		work_l.add_child ("result", result_l);
	}

	boost::property_tree::ptree bad_peers_l;
	for (auto & peer_text : bad_peers_a)
	{
		boost::property_tree::ptree entry;
		entry.put ("", peer_text);
		bad_peers_l.push_back (std::make_pair ("", entry));
	}
	work_l.add_child ("bad_peers", bad_peers_l);

	message_l.contents.add_child ("message", work_l);
	return message_l;
}

nano::websocket::message nano::websocket::message_builder::work_cancelled (nano::work_version const version_a, nano::block_hash const & root_a, uint64_t const difficulty_a, uint64_t const publish_threshold_a, std::chrono::milliseconds const & duration_a, std::vector<std::string> const & bad_peers_a)
{
	return work_generation (version_a, root_a, 0, difficulty_a, publish_threshold_a, duration_a, "", bad_peers_a, false, true);
}

nano::websocket::message nano::websocket::message_builder::work_failed (nano::work_version const version_a, nano::block_hash const & root_a, uint64_t const difficulty_a, uint64_t const publish_threshold_a, std::chrono::milliseconds const & duration_a, std::vector<std::string> const & bad_peers_a)
{
	return work_generation (version_a, root_a, 0, difficulty_a, publish_threshold_a, duration_a, "", bad_peers_a, false, false);
}

nano::websocket::message nano::websocket::message_builder::bootstrap_started (std::string const & id_a, std::string const & mode_a)
{
	nano::websocket::message message_l (nano::websocket::topic::bootstrap);
	set_common_fields (message_l);

	// Bootstrap information
	boost::property_tree::ptree bootstrap_l;
	bootstrap_l.put ("reason", "started");
	bootstrap_l.put ("id", id_a);
	bootstrap_l.put ("mode", mode_a);

	message_l.contents.add_child ("message", bootstrap_l);
	return message_l;
}

nano::websocket::message nano::websocket::message_builder::bootstrap_exited (std::string const & id_a, std::string const & mode_a, std::chrono::steady_clock::time_point const start_time_a, uint64_t const total_blocks_a)
{
	nano::websocket::message message_l (nano::websocket::topic::bootstrap);
	set_common_fields (message_l);

	// Bootstrap information
	boost::property_tree::ptree bootstrap_l;
	bootstrap_l.put ("reason", "exited");
	bootstrap_l.put ("id", id_a);
	bootstrap_l.put ("mode", mode_a);
	bootstrap_l.put ("total_blocks", total_blocks_a);
	bootstrap_l.put ("duration", std::chrono::duration_cast<std::chrono::seconds> (std::chrono::steady_clock::now () - start_time_a).count ());

	message_l.contents.add_child ("message", bootstrap_l);
	return message_l;
}

nano::websocket::message nano::websocket::message_builder::telemetry_received (nano::telemetry_data const & telemetry_data_a, nano::endpoint const & endpoint_a)
{
	nano::websocket::message message_l (nano::websocket::topic::telemetry);
	set_common_fields (message_l);

	// Telemetry information
	nano::jsonconfig telemetry_l;
	telemetry_data_a.serialize_json (telemetry_l, false);
	telemetry_l.put ("address", endpoint_a.address ());
	telemetry_l.put ("port", endpoint_a.port ());

	message_l.contents.add_child ("message", telemetry_l.get_tree ());
	return message_l;
}

nano::websocket::message nano::websocket::message_builder::new_block_arrived (nano::block const & block_a)
{
	nano::websocket::message message_l (nano::websocket::topic::new_unconfirmed_block);
	set_common_fields (message_l);

	boost::property_tree::ptree block_l;
	block_a.serialize_json (block_l);
	auto subtype (nano::state_subtype (block_a.sideband ().details));
	block_l.put ("subtype", subtype);

	message_l.contents.add_child ("message", block_l);
	return message_l;
}

void nano::websocket::message_builder::set_common_fields (nano::websocket::message & message_a)
{
	using namespace std::chrono;
	auto milli_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now ().time_since_epoch ()).count ();

	// Common message information
	message_a.contents.add ("topic", from_topic (message_a.topic));
	message_a.contents.add ("time", std::to_string (milli_since_epoch));
}

std::string nano::websocket::message::to_string () const
{
	std::ostringstream ostream;
	boost::property_tree::write_json (ostream, contents);
	ostream.flush ();
	return ostream.str ();
}
