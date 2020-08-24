#pragma once

#include <nano/lib/errors.hpp>
#include <nano/lib/locks.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/lib/work.hpp>
#include <nano/node/common.hpp>

#include <boost/asio.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <unordered_set>

namespace beast = boost::beast;
namespace http = beast::http;
namespace ptree = boost::property_tree;
namespace asio = boost::asio;
using tcp = boost::asio::ip::tcp;

namespace
{
enum class work_peer_type
{
	good,
	malicious,
	slow
};

class work_peer_connection : public std::enable_shared_from_this<work_peer_connection>
{
	const std::string generic_error = "Unable to parse JSON";
	const std::string empty_response = "Empty response";

public:
	work_peer_connection (asio::io_context & ioc_a, work_peer_type const type_a, nano::work_version const version_a, nano::work_pool & pool_a, std::function<void(bool const)> on_generation_a, std::function<void()> on_cancel_a) :
	socket (ioc_a),
	type (type_a),
	version (version_a),
	work_pool (pool_a),
	on_generation (on_generation_a),
	on_cancel (on_cancel_a),
	timer (ioc_a)
	{
	}
	void start ()
	{
		read_request ();
	}
	tcp::socket socket;

private:
	work_peer_type type;
	nano::work_version version;
	nano::work_pool & work_pool;
	beast::flat_buffer buffer{ 8192 };
	http::request<http::string_body> request;
	http::response<http::dynamic_body> response;
	std::function<void(bool const)> on_generation;
	std::function<void()> on_cancel;
	asio::deadline_timer timer;

	void read_request ()
	{
		auto this_l = shared_from_this ();
		http::async_read (socket, buffer, request, [this_l](beast::error_code ec, std::size_t const /*size_a*/) {
			if (!ec)
			{
				this_l->process_request ();
			}
		});
	}

	void process_request ()
	{
		switch (request.method ())
		{
			case http::verb::post:
				response.result (http::status::ok);
				create_response ();
				break;

			default:
				response.result (http::status::bad_request);
				break;
		}
	}

	void create_response ()
	{
		std::stringstream istream (request.body ());
		try
		{
			ptree::ptree result;
			ptree::read_json (istream, result);
			handle (result);
		}
		catch (...)
		{
			error (generic_error);
			write_response ();
		}
		response.version (request.version ());
		response.keep_alive (false);
	}

	void write_response ()
	{
		auto this_l = shared_from_this ();
		response.set (http::field::content_length, response.body ().size ());
		http::async_write (socket, response, [this_l](beast::error_code ec, std::size_t /*size_a*/) {
			this_l->socket.shutdown (tcp::socket::shutdown_send, ec);
			this_l->socket.close ();
		});
	}

	void error (std::string const & message_a)
	{
		ptree::ptree error_l;
		error_l.put ("error", message_a);
		std::stringstream ostream;
		ptree::write_json (ostream, error_l);
		beast::ostream (response.body ()) << ostream.str ();
	}

	void handle_generate (nano::block_hash const & hash_a)
	{
		if (type == work_peer_type::good)
		{
			auto hash = hash_a;
			auto request_difficulty = nano::work_threshold_base (version);
			auto this_l (shared_from_this ());
			work_pool.generate (version, hash, request_difficulty, [this_l, hash](boost::optional<uint64_t> work_a) {
				auto result = work_a.value_or (0);
				auto result_difficulty (nano::work_difficulty (this_l->version, hash, result));
				ptree::ptree message_l;
				message_l.put ("work", nano::to_string_hex (result));
				message_l.put ("difficulty", nano::to_string_hex (result_difficulty));
				message_l.put ("multiplier", nano::to_string (nano::difficulty::to_multiplier (result_difficulty, nano::work_threshold_base (this_l->version))));
				message_l.put ("hash", hash.to_string ());
				std::stringstream ostream;
				ptree::write_json (ostream, message_l);
				beast::ostream (this_l->response.body ()) << ostream.str ();
				// Delay response by 500ms as a slow peer, immediate async call for a good peer
				this_l->timer.expires_from_now (boost::posix_time::milliseconds (this_l->type == work_peer_type::slow ? 500 : 0));
				this_l->timer.async_wait ([this_l, result](const boost::system::error_code & ec) {
					if (this_l->on_generation)
					{
						this_l->on_generation (result != 0);
					}
					this_l->write_response ();
				});
			});
		}
		else if (type == work_peer_type::malicious)
		{
			// Respond immediately with no work
			on_generation (false);
			write_response ();
		}
	}

	void handle (ptree::ptree const & tree_a)
	{
		auto action_text (tree_a.get<std::string> ("action"));
		auto hash_text (tree_a.get<std::string> ("hash"));
		nano::block_hash hash;
		hash.decode_hex (hash_text);
		if (action_text == "work_generate")
		{
			handle_generate (hash);
		}
		else if (action_text == "work_cancel")
		{
			error (empty_response);
			on_cancel ();
			write_response ();
		}
		else
		{
			throw;
		}
	}
};

class fake_work_peer : public std::enable_shared_from_this<fake_work_peer>
{
public:
	fake_work_peer () = delete;
	fake_work_peer (nano::work_pool & pool_a, asio::io_context & ioc_a, unsigned short port_a, work_peer_type const type_a, nano::work_version const version_a = nano::work_version::work_1) :
	pool (pool_a),
	endpoint (tcp::v4 (), port_a),
	ioc (ioc_a),
	acceptor (ioc_a, endpoint),
	type (type_a),
	version (version_a)
	{
	}
	void start ()
	{
		listen ();
	}
	unsigned short port () const
	{
		return endpoint.port ();
	}
	std::atomic<size_t> generations_good{ 0 };
	std::atomic<size_t> generations_bad{ 0 };
	std::atomic<size_t> cancels{ 0 };

private:
	void listen ()
	{
		std::weak_ptr<fake_work_peer> this_w (shared_from_this ());
		auto connection (std::make_shared<work_peer_connection> (
		ioc, type, version, pool,
		[this_w](bool const good_generation) {
			if (auto this_l = this_w.lock ())
			{
				if (good_generation)
				{
					++this_l->generations_good;
				}
				else
				{
					++this_l->generations_bad;
				}
			};
		},
		[this_w]() {
			if (auto this_l = this_w.lock ())
			{
				++this_l->cancels;
			}
		}));
		acceptor.async_accept (connection->socket, [connection, this_w](beast::error_code ec) {
			if (!ec)
			{
				if (auto this_l = this_w.lock ())
				{
					connection->start ();
					this_l->listen ();
				}
			}
		});
	}
	nano::work_pool & pool;
	tcp::endpoint endpoint;
	asio::io_context & ioc;
	tcp::acceptor acceptor;
	work_peer_type const type;
	nano::work_version version;
};
}
