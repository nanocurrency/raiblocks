#include <nano/node/distributed_work.hpp>
#include <nano/node/node.hpp>
#include <nano/node/websocket.hpp>

std::shared_ptr<request_type> nano::work_peer_request::get_prepared_json_request (std::string const & request_string_a) const
{
	auto request (std::make_shared<boost::beast::http::request<boost::beast::http::string_body>> ());
	request->method (boost::beast::http::verb::post);
	request->set (boost::beast::http::field::content_type, "application/json");
	request->target ("/");
	request->version (11);
	request->body () = request_string_a;
	request->prepare_payload ();
	return request;
}

nano::distributed_work::distributed_work (nano::node & node_a, nano::root const & root_a, std::vector<std::pair<std::string, uint16_t>> const & peers_a, unsigned int backoff_a, std::function<void(boost::optional<uint64_t>)> const & callback_a, uint64_t difficulty_a, boost::optional<nano::account> const & account_a)
	: callback (callback_a)
	, backoff (backoff_a)
	, node (node_a)
	, root (root_a)
	, account (account_a)
	, peers (peers_a)
	, need_resolve (peers_a)
	, difficulty (difficulty_a)
	, elapsed (nano::timer_state::started, "distributed work generation timer")
{
	assert (!finished);
	assert (status == work_generation_status::ongoing);
}

nano::distributed_work::~distributed_work ()
{
	assert (status != work_generation_status::ongoing);
	if (!node.stopped && node.websocket_server && node.websocket_server->any_subscriber (nano::websocket::topic::work))
	{
		nano::websocket::message_builder builder;
		if (status == work_generation_status::success)
		{
			node.websocket_server->broadcast (builder.work_generation (root, work_result, difficulty, node.network_params.network.publish_threshold, elapsed.value (), winner, bad_peers));
		}
		else if (status == work_generation_status::cancelled)
		{
			node.websocket_server->broadcast (builder.work_cancelled (root, difficulty, node.network_params.network.publish_threshold, elapsed.value (), bad_peers));
		}
		else if (status == work_generation_status::failure_local || status == work_generation_status::failure_peers)
		{
			node.websocket_server->broadcast (builder.work_failed (root, difficulty, node.network_params.network.publish_threshold, elapsed.value (), bad_peers));
		}
	}
	stop_once (true);
}

void nano::distributed_work::start ()
{
	if (need_resolve.empty ())
	{
		start_work ();
	}
	else
	{
		auto current (need_resolve.back ());
		need_resolve.pop_back ();
		auto this_l (shared_from_this ());
		boost::system::error_code ec;
		auto parsed_address (boost::asio::ip::make_address_v6 (current.first, ec));
		if (!ec)
		{
			outstanding[parsed_address] = current.second;
			start ();
		}
		else
		{
			node.network.resolver.async_resolve (boost::asio::ip::udp::resolver::query (current.first, std::to_string (current.second)), [current, this_l](boost::system::error_code const & ec, boost::asio::ip::udp::resolver::iterator i_a) {
				if (!ec)
				{
					for (auto i (i_a), n (boost::asio::ip::udp::resolver::iterator{}); i != n; ++i)
					{
						auto endpoint (i->endpoint ());
						this_l->outstanding[endpoint.address ()] = endpoint.port ();
					}
				}
				else
				{
					this_l->node.logger.try_log (boost::str (boost::format ("Error resolving work peer: %1%:%2%: %3%") % current.first % current.second % ec.message ()));
				}
				this_l->start ();
			});
		}
	}
}

void nano::distributed_work::start_work ()
{
	auto this_l (shared_from_this ());

	// Start work generation if peers are not acting correctly, or if there are no peers configured
	if ((outstanding.empty () || node.unresponsive_work_peers) && node.local_work_generation_enabled ())
	{
		local_generation_started = true;
		node.work.generate (
			root, [this_l](boost::optional<uint64_t> const & work_a) {
				if (work_a.is_initialized ())
				{
					this_l->set_once (*work_a);
				}
				else if (!this_l->finished.exchange (true))
				{
					this_l->status = work_generation_status::failure_local;
					if (this_l->callback)
					{
						this_l->callback (boost::none);
					}
				}
				this_l->stop_once (false);
			},
			difficulty);
	}

	if (!outstanding.empty ())
	{
		nano::lock_guard<std::mutex> guard (mutex);
		for (auto const & i : outstanding)
		{
			auto host (i.first);
			auto service (i.second);
			auto connection (std::make_shared<nano::work_peer_request> (this_l->node.io_ctx, host, service));
			connections.emplace_back (connection);
			connection->socket.async_connect (nano::tcp_endpoint (host, service), [this_l, connection](boost::system::error_code const & ec) {
				if (!ec)
				{
					std::string request_string;
					{
						boost::property_tree::ptree request;
						request.put ("action", "work_generate");
						request.put ("hash", this_l->root.to_string ());
						request.put ("difficulty", nano::to_string_hex (this_l->difficulty));
						if (this_l->account.is_initialized ())
						{
							request.put ("account", this_l->account.get ().to_account ());
						}
						std::stringstream ostream;
						boost::property_tree::write_json (ostream, request);
						request_string = ostream.str ();
					}
					auto request (connection->get_prepared_json_request (request_string));
					boost::beast::http::async_write (connection->socket, *request, [this_l, connection, request](boost::system::error_code const & ec, size_t bytes_transferred) {
						if (!ec)
						{
							boost::beast::http::async_read (connection->socket, connection->buffer, connection->response, [this_l, connection](boost::system::error_code const & ec, size_t bytes_transferred) {
								if (!ec)
								{
									if (connection->response.result () == boost::beast::http::status::ok)
									{
										this_l->success (connection->response.body (), connection->address, connection->port);
									}
									else
									{
										this_l->node.logger.try_log (boost::str (boost::format ("Work peer responded with an error %1% %2%: %3%") % connection->address % connection->port % connection->response.result ()));
										this_l->add_bad_peer (connection->address, connection->port);
										this_l->failure (connection->address);
									}
								}
								else if (ec == boost::system::errc::operation_canceled)
								{
									// The only case where we send a cancel is if we preempt stopped waiting for the response
									this_l->cancel_connection (connection);
									this_l->failure (connection->address);
								}
								else
								{
									this_l->node.logger.try_log (boost::str (boost::format ("Unable to read from work_peer %1% %2%: %3% (%4%)") % connection->address % connection->port % ec.message () % ec.value ()));
									this_l->add_bad_peer (connection->address, connection->port);
									this_l->failure (connection->address);
								}
							});
						}
						else
						{
							this_l->node.logger.try_log (boost::str (boost::format ("Unable to write to work_peer %1% %2%: %3% (%4%)") % connection->address % connection->port % ec.message () % ec.value ()));
							this_l->add_bad_peer (connection->address, connection->port);
							this_l->failure (connection->address);
						}
					});
				}
				else
				{
					this_l->node.logger.try_log (boost::str (boost::format ("Unable to connect to work_peer %1% %2%: %3% (%4%)") % connection->address % connection->port % ec.message () % ec.value ()));
					this_l->add_bad_peer (connection->address, connection->port);
					this_l->failure (connection->address);
				}
			});
		}
	}

	if (!local_generation_started && outstanding.empty () && callback)
	{
		callback (boost::none);
	}
}

void nano::distributed_work::cancel_connection (std::shared_ptr<nano::work_peer_request> connection_a)
{
	auto this_l (shared_from_this ());
	auto cancelling_l (std::make_shared<nano::work_peer_request> (node.io_ctx, connection_a->address, connection_a->port));
	cancelling_l->socket.async_connect (nano::tcp_endpoint (cancelling_l->address, cancelling_l->port), [this_l, cancelling_l](boost::system::error_code const & ec) {
		if (!ec)
		{
			std::string request_string;
			{
				boost::property_tree::ptree request;
				request.put ("action", "work_cancel");
				request.put ("hash", this_l->root.to_string ());
				std::stringstream ostream;
				boost::property_tree::write_json (ostream, request);
				request_string = ostream.str ();
			}
			auto request (cancelling_l->get_prepared_json_request (request_string));
			boost::beast::http::async_write (cancelling_l->socket, *request, [this_l, request, cancelling_l](boost::system::error_code const & ec, size_t bytes_transferred) {
				if (ec)
				{
					this_l->node.logger.try_log (boost::str (boost::format ("Unable to send work_cancel to work_peer %1% %2%: %3% (%4%)") % cancelling_l->address % cancelling_l->port % ec.message () % ec.value ()));
				}
			});
		}
	});
}

void nano::distributed_work::success (std::string const & body_a, boost::asio::ip::address const & address_a, uint16_t port_a)
{
	auto last (remove (address_a));
	std::stringstream istream (body_a);
	try
	{
		boost::property_tree::ptree result;
		boost::property_tree::read_json (istream, result);
		auto work_text (result.get<std::string> ("work"));
		uint64_t work;
		if (!nano::from_string_hex (work_text, work))
		{
			uint64_t result_difficulty (0);
			if (!nano::work_validate (root, work, &result_difficulty) && result_difficulty >= difficulty)
			{
				node.unresponsive_work_peers = false;
				set_once (work, boost::str (boost::format ("%1%:%2%") % address_a % port_a));
				stop_once (true);
			}
			else
			{
				node.logger.try_log (boost::str (boost::format ("Incorrect work response from %1%:%2% for root %3% with diffuculty %4%: %5%") % address_a % port_a % root.to_string () % nano::to_string_hex (difficulty) % work_text));
				add_bad_peer (address_a, port_a);
				handle_failure (last);
			}
		}
		else
		{
			node.logger.try_log (boost::str (boost::format ("Work response from %1%:%2% wasn't a number: %3%") % address_a % port_a % work_text));
			add_bad_peer (address_a, port_a);
			handle_failure (last);
		}
	}
	catch (...)
	{
		node.logger.try_log (boost::str (boost::format ("Work response from %1%:%2% wasn't parsable: %3%") % address_a % port_a % body_a));
		add_bad_peer (address_a, port_a);
		handle_failure (last);
	}
}

void nano::distributed_work::stop_once (bool const local_stop_a)
{
	if (!stopped.exchange (true))
	{
		nano::lock_guard<std::mutex> guard (mutex);
		if (local_stop_a && node.local_work_generation_enabled ())
		{
			node.work.cancel (root);
		}
		for (auto & connection_w : connections)
		{
			if (auto connection_l = connection_w.lock ())
			{
				boost::system::error_code ec;
				connection_l->socket.cancel (ec);
				if (ec)
				{
					node.logger.try_log (boost::str (boost::format ("Error cancelling operation with work_peer %1% %2%: %3%") % connection_l->address % connection_l->port % ec.message () % ec.value ()));
				}
				try
				{
					connection_l->socket.close ();
				}
				catch (const boost::system::system_error & ec)
				{
					node.logger.try_log (boost::str (boost::format ("Error closing socket with work_peer %1% %2%: %3%") % connection_l->address % connection_l->port % ec.what () % ec.code ()));
				}
			}
		}
		connections.clear ();
		outstanding.clear ();
	}
}

void nano::distributed_work::set_once (uint64_t work_a, std::string const & source_a)
{
	if (!finished.exchange (true))
	{
		elapsed.stop ();
		status = work_generation_status::success;
		if (callback)
		{
			callback (work_a);
		}
		winner = source_a;
		work_result = work_a;
		if (node.config.logging.work_generation_time ())
		{
			boost::format unformatted_l ("Work generation for %1%, with a threshold difficulty of %2% (multiplier %3%x) complete: %4% ms");
			auto multiplier_text_l (nano::to_string (nano::difficulty::to_multiplier (difficulty, node.network_params.network.publish_threshold), 2));
			node.logger.try_log (boost::str (unformatted_l % root.to_string () % nano::to_string_hex (difficulty) % multiplier_text_l % elapsed.value ().count ()));
		}
	}
}

void nano::distributed_work::cancel_once ()
{
	if (!finished.exchange (true))
	{
		elapsed.stop ();
		status = work_generation_status::cancelled;
		if (callback)
		{
			callback (boost::none);
		}
		stop_once (true);
		if (node.config.logging.work_generation_time ())
		{
			node.logger.try_log (boost::str (boost::format ("Work generation for %1% was cancelled after %2% ms") % root.to_string () % elapsed.value ().count ()));
		}
	}
}

void nano::distributed_work::failure (boost::asio::ip::address const & address_a)
{
	auto last (remove (address_a));
	handle_failure (last);
}

void nano::distributed_work::handle_failure (bool const last_a)
{
	if (last_a && !finished)
	{
		node.unresponsive_work_peers = true;
		if (!local_generation_started && !finished.exchange (true))
		{
			status = work_generation_status::failure_peers;
			if (backoff == 1 && node.config.logging.work_generation_time ())
			{
				node.logger.always_log ("Work peer(s) failed to generate work for root ", root.to_string (), ", retrying...");
			}
			auto now (std::chrono::steady_clock::now ());
			std::weak_ptr<nano::node> node_w (node.shared ());
			auto next_backoff (std::min (backoff * 2, (unsigned int)60 * 5));
			// clang-format off
			node.alarm.add (now + std::chrono::seconds (backoff), [ node_w, root_l = root, peers_l = peers, callback_l = callback, next_backoff, difficulty = difficulty, account_l = account ] {
				bool error_l {true};
				if (auto node_l = node_w.lock ())
				{
					error_l = node_l->distributed_work.make (next_backoff, root_l, peers_l, callback_l, difficulty, account_l);
				}
				if (error_l && callback_l)
				{
					callback_l (boost::none);
				}
			});
			// clang-format on
		}
		else
		{
			// wait for local work generation to complete
		}
	}
}

bool nano::distributed_work::remove (boost::asio::ip::address const & address_a)
{
	nano::lock_guard<std::mutex> guard (mutex);
	outstanding.erase (address_a);
	return outstanding.empty ();
}

void nano::distributed_work::add_bad_peer (boost::asio::ip::address const & address_a, uint16_t port_a)
{
	nano::lock_guard<std::mutex> guard (mutex);
	bad_peers.emplace_back (boost::str (boost::format ("%1%:%2%") % address_a % port_a));
}
