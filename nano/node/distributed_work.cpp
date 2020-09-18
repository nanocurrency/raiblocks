#include <nano/boost/asio/bind_executor.hpp>
#include <nano/boost/asio/post.hpp>
#include <nano/node/distributed_work.hpp>
#include <nano/node/node.hpp>
#include <nano/node/websocket.hpp>

#include <boost/algorithm/string/erase.hpp>

std::shared_ptr<request_type> nano::distributed_work::peer_request::get_prepared_json_request (std::string const & request_string_a) const
{
	auto request (std::make_shared<request_type> ());
	request->method (boost::beast::http::verb::post);
	request->set (boost::beast::http::field::content_type, "application/json");
	auto address_string = boost::algorithm::erase_first_copy (endpoint.address ().to_string (), "::ffff:");
	request->set (boost::beast::http::field::host, address_string);
	request->target ("/");
	request->version (11);
	request->body () = request_string_a;
	request->prepare_payload ();
	return request;
}

nano::distributed_work::distributed_work (nano::node & node_a, nano::work_request const & request_a, std::chrono::seconds const & backoff_a) :
node (node_a),
node_w (node_a.shared ()),
request (request_a),
backoff (backoff_a),
strand (node_a.io_ctx.get_executor ()),
need_resolve (request_a.peers),
elapsed (nano::timer_state::started, "distributed work generation timer")
{
	debug_assert (!finished);
	debug_assert (status == work_generation_status::ongoing);
}

nano::distributed_work::~distributed_work ()
{
	debug_assert (status != work_generation_status::ongoing);
	if (auto node_l = node_w.lock ())
	{
		if (!node_l->stopped && node_l->websocket_server && node_l->websocket_server->any_subscriber (nano::websocket::topic::work))
		{
			nano::websocket::message_builder builder;
			if (status == work_generation_status::success)
			{
				node_l->websocket_server->broadcast (builder.work_generation (request.version, request.root.as_block_hash (), work_result, request.difficulty, node_l->default_difficulty (request.version), elapsed.value (), winner, bad_peers));
			}
			else if (status == work_generation_status::cancelled)
			{
				node_l->websocket_server->broadcast (builder.work_cancelled (request.version, request.root.as_block_hash (), request.difficulty, node_l->default_difficulty (request.version), elapsed.value (), bad_peers));
			}
			else if (status == work_generation_status::failure_local || status == work_generation_status::failure_peers)
			{
				node_l->websocket_server->broadcast (builder.work_failed (request.version, request.root.as_block_hash (), request.difficulty, node_l->default_difficulty (request.version), elapsed.value (), bad_peers));
			}
		}
		stop_once (true);
	}
}

void nano::distributed_work::start ()
{
	// Start work generation if peers are not acting correctly, or if there are no peers configured
	if ((need_resolve.empty () || node.unresponsive_work_peers) && node.local_work_generation_enabled ())
	{
		start_local ();
	}
	// Fallback when local generation is required but it is not enabled is to simply call the callback with an error
	else if (need_resolve.empty () && request.callback)
	{
		status = work_generation_status::failure_local;
		request.callback (boost::none);
	}
	for (auto const & peer : need_resolve)
	{
		boost::system::error_code ec;
		auto parsed_address (boost::asio::ip::make_address_v6 (peer.first, ec));
		if (!ec)
		{
			do_request (nano::tcp_endpoint (parsed_address, peer.second));
		}
		else
		{
			auto this_l (shared_from_this ());
			node.network.resolver.async_resolve (boost::asio::ip::udp::resolver::query (peer.first, std::to_string (peer.second)), [peer, this_l, &extra = resolved_extra](boost::system::error_code const & ec, boost::asio::ip::udp::resolver::iterator i_a) {
				if (!ec)
				{
					this_l->do_request (nano::tcp_endpoint (i_a->endpoint ().address (), i_a->endpoint ().port ()));
					++i_a;
					for (auto & i : boost::make_iterator_range (i_a, {}))
					{
						++extra;
						this_l->do_request (nano::tcp_endpoint (i.endpoint ().address (), i.endpoint ().port ()));
					}
				}
				else
				{
					this_l->node.logger.try_log (boost::str (boost::format ("Error resolving work peer: %1%:%2%: %3%") % peer.first % peer.second % ec.message ()));
					this_l->failure ();
				}
			});
		}
	}
}

void nano::distributed_work::start_local ()
{
	auto this_l (shared_from_this ());
	local_generation_started = true;
	node.work.generate (request.version, request.root, request.difficulty, [this_l](boost::optional<uint64_t> const & work_a) {
		if (work_a.is_initialized ())
		{
			this_l->set_once (*work_a);
		}
		else if (!this_l->finished.exchange (true))
		{
			this_l->status = work_generation_status::failure_local;
			if (this_l->request.callback)
			{
				this_l->request.callback (boost::none);
			}
		}
		this_l->stop_once (false);
	});
}

void nano::distributed_work::do_request (nano::tcp_endpoint const & endpoint_a)
{
	auto connection (std::make_shared<peer_request> (node.io_ctx, endpoint_a));
	{
		nano::lock_guard<std::mutex> lock (mutex);
		connections.emplace_back (connection);
	}
	boost::asio::spawn (
	strand,
	[this_l = shared_from_this (), connection = connection](boost::asio::yield_context yield) {
		boost::system::error_code ec;
		connection->socket.async_connect (connection->endpoint, yield[ec]);
		if (!ec && !this_l->stopped)
		{
			std::string request_string;
			{
				boost::property_tree::ptree request;
				request.put ("action", "work_generate");
				request.put ("hash", this_l->request.root.to_string ());
				request.put ("difficulty", nano::to_string_hex (this_l->request.difficulty));
				if (this_l->request.account.is_initialized ())
				{
					request.put ("account", this_l->request.account.get ().to_account ());
				}
				std::stringstream ostream;
				boost::property_tree::write_json (ostream, request);
				request_string = ostream.str ();
			}
			auto peer_request (connection->get_prepared_json_request (request_string));
			boost::beast::http::async_write (connection->socket, *peer_request,
			boost::asio::bind_executor (this_l->strand,
			[this_l, connection, peer_request](boost::system::error_code const & ec, size_t size_a) {
				if (!ec && !this_l->stopped)
				{
					boost::beast::http::async_read (connection->socket, connection->buffer, connection->response,
					boost::asio::bind_executor (this_l->strand, [this_l, connection](boost::system::error_code const & ec, size_t size_a) {
						if (!ec && !this_l->stopped)
						{
							if (connection->response.result () == boost::beast::http::status::ok)
							{
								this_l->success (connection->response.body (), connection->endpoint);
							}
							else if (ec)
							{
								this_l->node.logger.try_log (boost::str (boost::format ("Work peer responded with an error %1% %2%: %3%") % connection->endpoint.address () % connection->endpoint.port () % connection->response.result ()));
								this_l->add_bad_peer (connection->endpoint);
								this_l->failure ();
							}
						}
						else if (ec)
						{
							this_l->do_cancel (connection->endpoint);
							this_l->failure ();
						}
					}));
				}
				else if (ec && ec != boost::system::errc::operation_canceled)
				{
					this_l->node.logger.try_log (boost::str (boost::format ("Unable to write to work_peer %1% %2%: %3% (%4%)") % connection->endpoint.address () % connection->endpoint.port () % ec.message () % ec.value ()));
					this_l->add_bad_peer (connection->endpoint);
					this_l->failure ();
				}
			}));
		}
		else if (ec && ec != boost::system::errc::operation_canceled)
		{
			this_l->node.logger.try_log (boost::str (boost::format ("Unable to connect to work_peer %1% %2%: %3% (%4%)") % connection->endpoint.address () % connection->endpoint.port () % ec.message () % ec.value ()));
			this_l->add_bad_peer (connection->endpoint);
			this_l->failure ();
		}
	},
	boost::coroutines::attributes (128 * 1024));
}

void nano::distributed_work::do_cancel (nano::tcp_endpoint const & endpoint_a)
{
	auto cancelling_l (std::make_shared<peer_request> (node.io_ctx, endpoint_a));
	boost::asio::spawn (strand,
	[this_l = shared_from_this (), cancelling_l](boost::asio::yield_context yield) {
		boost::system::error_code ec;
		cancelling_l->socket.async_connect (cancelling_l->endpoint, yield[ec]);
		if (!ec)
		{
			std::string request_string;
			{
				boost::property_tree::ptree request;
				request.put ("action", "work_cancel");
				request.put ("hash", this_l->request.root.to_string ());
				std::stringstream ostream;
				boost::property_tree::write_json (ostream, request);
				request_string = ostream.str ();
			}
			auto peer_cancel (cancelling_l->get_prepared_json_request (request_string));
			boost::beast::http::async_write (cancelling_l->socket, *peer_cancel,
			boost::asio::bind_executor (this_l->strand,
			[this_l, peer_cancel, cancelling_l](boost::system::error_code const & ec, size_t bytes_transferred) {
				if (ec && ec != boost::system::errc::operation_canceled)
				{
					this_l->node.logger.try_log (boost::str (boost::format ("Unable to send work_cancel to work_peer %1% %2%: %3% (%4%)") % cancelling_l->endpoint.address () % cancelling_l->endpoint.port () % ec.message () % ec.value ()));
				}
			}));
		}
	});
}

void nano::distributed_work::success (std::string const & body_a, nano::tcp_endpoint const & endpoint_a)
{
	bool error = true;
	try
	{
		std::stringstream istream (body_a);
		boost::property_tree::ptree result;
		boost::property_tree::read_json (istream, result);
		auto work_text (result.get<std::string> ("work"));
		uint64_t work;
		if (!nano::from_string_hex (work_text, work))
		{
			if (nano::work_difficulty (request.version, request.root, work) >= request.difficulty)
			{
				error = false;
				node.unresponsive_work_peers = false;
				set_once (work, boost::str (boost::format ("%1%:%2%") % endpoint_a.address () % endpoint_a.port ()));
				stop_once (true);
			}
			else
			{
				node.logger.try_log (boost::str (boost::format ("Incorrect work response from %1%:%2% for root %3% with diffuculty %4%: %5%") % endpoint_a.address () % endpoint_a.port () % request.root.to_string () % nano::to_string_hex (request.difficulty) % work_text));
			}
		}
		else
		{
			node.logger.try_log (boost::str (boost::format ("Work response from %1%:%2% wasn't a number: %3%") % endpoint_a.address () % endpoint_a.port () % work_text));
		}
	}
	catch (...)
	{
		node.logger.try_log (boost::str (boost::format ("Work response from %1%:%2% wasn't parsable: %3%") % endpoint_a.address () % endpoint_a.port () % body_a));
	}
	if (error)
	{
		add_bad_peer (endpoint_a);
		failure ();
	}
}

void nano::distributed_work::stop_once (bool const local_stop_a)
{
	if (!stopped.exchange (true))
	{
		nano::lock_guard<std::mutex> guard (mutex);
		if (local_stop_a && node.local_work_generation_enabled ())
		{
			node.work.cancel (request.root);
		}
		for (auto & connection_w : connections)
		{
			if (auto connection_l = connection_w.lock ())
			{
				auto this_l (shared_from_this ());
				boost::asio::post (strand, boost::asio::bind_executor (strand, [this_l, connection_l] {
					boost::system::error_code ec;
					if (connection_l->socket.is_open ())
					{
						connection_l->socket.cancel (ec);
						if (!ec)
						{
							connection_l->socket.close (ec);
							if (ec)
							{
								this_l->node.logger.try_log (boost::str (boost::format ("Error closing socket with work_peer %1% %2%: %3%") % connection_l->endpoint.address () % connection_l->endpoint.port () % ec.message () % ec.value ()));
							}
						}
						else
						{
							this_l->node.logger.try_log (boost::str (boost::format ("Error cancelling operation with work_peer %1% %2%: %3%") % connection_l->endpoint.address () % connection_l->endpoint.port () % ec.message () % ec.value ()));
						}
					}
				}));
			}
		}
		connections.clear ();
	}
}

void nano::distributed_work::set_once (uint64_t const work_a, std::string const & source_a)
{
	if (!finished.exchange (true))
	{
		elapsed.stop ();
		status = work_generation_status::success;
		if (request.callback)
		{
			request.callback (work_a);
		}
		winner = source_a;
		work_result = work_a;
		if (node.config.logging.work_generation_time ())
		{
			boost::format unformatted_l ("Work generation for %1%, with a threshold difficulty of %2% (multiplier %3%x) complete: %4% ms");
			auto multiplier_text_l (nano::to_string (nano::difficulty::to_multiplier (request.difficulty, node.default_difficulty (request.version)), 2));
			node.logger.try_log (boost::str (unformatted_l % request.root.to_string () % nano::to_string_hex (request.difficulty) % multiplier_text_l % elapsed.value ().count ()));
		}
	}
}

void nano::distributed_work::cancel ()
{
	if (!finished.exchange (true))
	{
		elapsed.stop ();
		status = work_generation_status::cancelled;
		if (request.callback)
		{
			request.callback (boost::none);
		}
		stop_once (true);
		if (node.config.logging.work_generation_time ())
		{
			node.logger.try_log (boost::str (boost::format ("Work generation for %1% was cancelled after %2% ms") % request.root.to_string () % elapsed.value ().count ()));
		}
	}
}

void nano::distributed_work::failure ()
{
	if (++failures == need_resolve.size () + resolved_extra.load ())
	{
		handle_failure ();
	}
}

void nano::distributed_work::handle_failure ()
{
	if (!finished)
	{
		node.unresponsive_work_peers = true;
		if (!local_generation_started && !finished.exchange (true))
		{
			status = work_generation_status::failure_peers;
			if (backoff == std::chrono::seconds (1) && node.config.logging.work_generation_time ())
			{
				node.logger.always_log ("Work peer(s) failed to generate work for root ", request.root.to_string (), ", retrying...");
			}
			auto now (std::chrono::steady_clock::now ());
			std::weak_ptr<nano::node> node_w (node.shared ());
			auto next_backoff (std::min (backoff * 2, std::chrono::seconds (5 * 60)));
			node.alarm.add (now + std::chrono::seconds (backoff), [node_w, request_l = request, next_backoff] {
				bool error_l{ true };
				if (auto node_l = node_w.lock ())
				{
					error_l = node_l->distributed_work.make (next_backoff, request_l);
				}
				if (error_l && request_l.callback)
				{
					request_l.callback (boost::none);
				}
			});
		}
		else
		{
			// wait for local work generation to complete
		}
	}
}

void nano::distributed_work::add_bad_peer (nano::tcp_endpoint const & endpoint_a)
{
	nano::lock_guard<std::mutex> guard (mutex);
	bad_peers.emplace_back (boost::str (boost::format ("%1%:%2%") % endpoint_a.address () % endpoint_a.port ()));
}
