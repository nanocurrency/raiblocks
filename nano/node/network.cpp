#include <nano/crypto_lib/random_pool_shuffle.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/network.hpp>
#include <nano/node/node.hpp>
#include <nano/node/telemetry.hpp>
#include <nano/secure/buffer.hpp>

#include <boost/asio/steady_timer.hpp>
#include <boost/format.hpp>
#include <boost/variant/get.hpp>

#include <numeric>

nano::network::network (nano::node & node_a, uint16_t port_a) :
syn_cookies (node_a.network_params.node.max_peers_per_ip),
buffer_container (node_a.stats, nano::network::buffer_size, 4096), // 2Mb receive buffer
resolver (node_a.io_ctx),
limiter (node_a.config.bandwidth_limit_burst_ratio, node_a.config.bandwidth_limit),
tcp_message_manager (node_a.config.tcp_incoming_connections_max),
node (node_a),
publish_filter (256 * 1024),
udp_channels (node_a, port_a),
tcp_channels (node_a),
port (port_a),
disconnect_observer ([]() {})
{
	boost::thread::attributes attrs;
	nano::thread_attributes::set (attrs);
	// UDP
	for (size_t i = 0; i < node.config.network_threads && !node.flags.disable_udp; ++i)
	{
		packet_processing_threads.emplace_back (attrs, [this]() {
			nano::thread_role::set (nano::thread_role::name::packet_processing);
			try
			{
				udp_channels.process_packets ();
			}
			catch (boost::system::error_code & ec)
			{
				this->node.logger.always_log (FATAL_LOG_PREFIX, ec.message ());
				release_assert (false);
			}
			catch (std::error_code & ec)
			{
				this->node.logger.always_log (FATAL_LOG_PREFIX, ec.message ());
				release_assert (false);
			}
			catch (std::runtime_error & err)
			{
				this->node.logger.always_log (FATAL_LOG_PREFIX, err.what ());
				release_assert (false);
			}
			catch (...)
			{
				this->node.logger.always_log (FATAL_LOG_PREFIX, "Unknown exception");
				release_assert (false);
			}
			if (this->node.config.logging.network_packet_logging ())
			{
				this->node.logger.try_log ("Exiting UDP packet processing thread");
			}
		});
	}
	// TCP
	for (size_t i = 0; i < node.config.network_threads && !node.flags.disable_tcp_realtime; ++i)
	{
		packet_processing_threads.emplace_back (attrs, [this]() {
			nano::thread_role::set (nano::thread_role::name::packet_processing);
			try
			{
				tcp_channels.process_messages ();
			}
			catch (boost::system::error_code & ec)
			{
				this->node.logger.always_log (FATAL_LOG_PREFIX, ec.message ());
				release_assert (false);
			}
			catch (std::error_code & ec)
			{
				this->node.logger.always_log (FATAL_LOG_PREFIX, ec.message ());
				release_assert (false);
			}
			catch (std::runtime_error & err)
			{
				this->node.logger.always_log (FATAL_LOG_PREFIX, err.what ());
				release_assert (false);
			}
			catch (...)
			{
				this->node.logger.always_log (FATAL_LOG_PREFIX, "Unknown exception");
				release_assert (false);
			}
			if (this->node.config.logging.network_packet_logging ())
			{
				this->node.logger.try_log ("Exiting TCP packet processing thread");
			}
		});
	}
}

nano::network::~network ()
{
	stop ();
}

void nano::network::start ()
{
	ongoing_cleanup ();
	ongoing_syn_cookie_cleanup ();
	if (!node.flags.disable_udp)
	{
		udp_channels.start ();
		debug_assert (udp_channels.get_local_endpoint ().port () == port);
	}
	if (!node.flags.disable_tcp_realtime)
	{
		tcp_channels.start ();
	}
	ongoing_keepalive ();
}

void nano::network::stop ()
{
	if (!stopped.exchange (true))
	{
		udp_channels.stop ();
		tcp_channels.stop ();
		resolver.cancel ();
		buffer_container.stop ();
		tcp_message_manager.stop ();
		port = 0;
		for (auto & thread : packet_processing_threads)
		{
			thread.join ();
		}
	}
}

void nano::network::send_keepalive (std::shared_ptr<nano::transport::channel> const & channel_a)
{
	nano::keepalive message;
	random_fill (message.peers);
	channel_a->send (message);
}

void nano::network::send_keepalive_self (std::shared_ptr<nano::transport::channel> const & channel_a)
{
	nano::keepalive message;
	fill_keepalive_self (message.peers);
	channel_a->send (message);
}

void nano::network::send_node_id_handshake (std::shared_ptr<nano::transport::channel> const & channel_a, boost::optional<nano::uint256_union> const & query, boost::optional<nano::uint256_union> const & respond_to)
{
	boost::optional<std::pair<nano::account, nano::signature>> response (boost::none);
	if (respond_to)
	{
		response = std::make_pair (node.node_id.pub, nano::sign_message (node.node_id.prv, node.node_id.pub, *respond_to));
		debug_assert (!nano::validate_message (response->first, *respond_to, response->second));
	}
	nano::node_id_handshake message (query, response);
	if (node.config.logging.network_node_id_handshake_logging ())
	{
		node.logger.try_log (boost::str (boost::format ("Node ID handshake sent with node ID %1% to %2%: query %3%, respond_to %4% (signature %5%)") % node.node_id.pub.to_node_id () % channel_a->get_endpoint () % (query ? query->to_string () : std::string ("[none]")) % (respond_to ? respond_to->to_string () : std::string ("[none]")) % (response ? response->second.to_string () : std::string ("[none]"))));
	}
	channel_a->send (message);
}

void nano::network::flood_message (nano::message const & message_a, nano::buffer_drop_policy const drop_policy_a, float const scale_a)
{
	for (auto & i : list (fanout (scale_a)))
	{
		i->send (message_a, nullptr, drop_policy_a);
	}
}

void nano::network::flood_block (std::shared_ptr<nano::block> const & block_a, nano::buffer_drop_policy const drop_policy_a)
{
	nano::publish message (block_a);
	flood_message (message, drop_policy_a);
}

void nano::network::flood_block_initial (std::shared_ptr<nano::block> const & block_a)
{
	nano::publish message (block_a);
	for (auto const & i : node.rep_crawler.principal_representatives ())
	{
		i.channel->send (message, nullptr, nano::buffer_drop_policy::no_limiter_drop);
	}
	for (auto & i : list_non_pr (fanout (1.0)))
	{
		i->send (message, nullptr, nano::buffer_drop_policy::no_limiter_drop);
	}
}

void nano::network::flood_vote (std::shared_ptr<nano::vote> const & vote_a, float scale)
{
	nano::confirm_ack message (vote_a);
	for (auto & i : list (fanout (scale)))
	{
		i->send (message, nullptr);
	}
}

void nano::network::flood_vote_pr (std::shared_ptr<nano::vote> const & vote_a)
{
	nano::confirm_ack message (vote_a);
	for (auto const & i : node.rep_crawler.principal_representatives ())
	{
		i.channel->send (message, nullptr, nano::buffer_drop_policy::no_limiter_drop);
	}
}

void nano::network::flood_block_many (std::deque<std::shared_ptr<nano::block>> blocks_a, std::function<void()> callback_a, unsigned delay_a)
{
	if (!blocks_a.empty ())
	{
		auto block_l (blocks_a.front ());
		blocks_a.pop_front ();
		flood_block (block_l);
		if (!blocks_a.empty ())
		{
			std::weak_ptr<nano::node> node_w (node.shared ());
			node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::milliseconds (delay_a + std::rand () % delay_a), [node_w, blocks (std::move (blocks_a)), callback_a, delay_a]() {
				if (auto node_l = node_w.lock ())
				{
					node_l->network.flood_block_many (std::move (blocks), callback_a, delay_a);
				}
			});
		}
		else if (callback_a)
		{
			callback_a ();
		}
	}
}

void nano::network::send_confirm_req (std::shared_ptr<nano::transport::channel> const & channel_a, std::pair<nano::block_hash, nano::block_hash> const & hash_root_a)
{
	// Confirmation request with hash + root
	nano::confirm_req req (hash_root_a.first, hash_root_a.second);
	channel_a->send (req);
}

void nano::network::broadcast_confirm_req (std::shared_ptr<nano::block> const & block_a)
{
	auto list (std::make_shared<std::vector<std::shared_ptr<nano::transport::channel>>> (node.rep_crawler.representative_endpoints (std::numeric_limits<size_t>::max ())));
	if (list->empty () || node.rep_crawler.total_weight () < node.online_reps.delta ())
	{
		// broadcast request to all peers (with max limit 2 * sqrt (peers count))
		auto peers (node.network.list (std::min<size_t> (100, node.network.fanout (2.0))));
		list->clear ();
		list->insert (list->end (), peers.begin (), peers.end ());
	}

	/*
	 * In either case (broadcasting to all representatives, or broadcasting to
	 * all peers because there are not enough connected representatives),
	 * limit each instance to a single random up-to-32 selection.  The invoker
	 * of "broadcast_confirm_req" will be responsible for calling it again
	 * if the votes for a block have not arrived in time.
	 */
	size_t const max_endpoints = 32;
	nano::random_pool_shuffle (list->begin (), list->end ());
	if (list->size () > max_endpoints)
	{
		list->erase (list->begin () + max_endpoints, list->end ());
	}

	broadcast_confirm_req_base (block_a, list, 0);
}

void nano::network::broadcast_confirm_req_base (std::shared_ptr<nano::block> const & block_a, std::shared_ptr<std::vector<std::shared_ptr<nano::transport::channel>>> const & endpoints_a, unsigned delay_a, bool resumption)
{
	size_t const max_reps = 10;
	if (!resumption && node.config.logging.network_logging ())
	{
		node.logger.try_log (boost::str (boost::format ("Broadcasting confirm req for block %1% to %2% representatives") % block_a->hash ().to_string () % endpoints_a->size ()));
	}
	auto count (0);
	while (!endpoints_a->empty () && count < max_reps)
	{
		auto channel (endpoints_a->back ());
		send_confirm_req (channel, std::make_pair (block_a->hash (), block_a->root ().as_block_hash ()));
		endpoints_a->pop_back ();
		count++;
	}
	if (!endpoints_a->empty ())
	{
		delay_a += std::rand () % broadcast_interval_ms;

		std::weak_ptr<nano::node> node_w (node.shared ());
		node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::milliseconds (delay_a), [node_w, block_a, endpoints_a, delay_a]() {
			if (auto node_l = node_w.lock ())
			{
				node_l->network.broadcast_confirm_req_base (block_a, endpoints_a, delay_a, true);
			}
		});
	}
}

void nano::network::broadcast_confirm_req_batched_many (std::unordered_map<std::shared_ptr<nano::transport::channel>, std::deque<std::pair<nano::block_hash, nano::root>>> request_bundle_a, std::function<void()> callback_a, unsigned delay_a, bool resumption_a)
{
	if (!resumption_a && node.config.logging.network_logging ())
	{
		node.logger.try_log (boost::str (boost::format ("Broadcasting batch confirm req to %1% representatives") % request_bundle_a.size ()));
	}

	for (auto i (request_bundle_a.begin ()), n (request_bundle_a.end ()); i != n;)
	{
		std::vector<std::pair<nano::block_hash, nano::root>> roots_hashes_l;
		// Limit max request size hash + root to 7 pairs
		while (roots_hashes_l.size () < confirm_req_hashes_max && !i->second.empty ())
		{
			// expects ordering by priority, descending
			roots_hashes_l.push_back (i->second.front ());
			i->second.pop_front ();
		}
		nano::confirm_req req (roots_hashes_l);
		i->first->send (req);
		if (i->second.empty ())
		{
			i = request_bundle_a.erase (i);
		}
		else
		{
			++i;
		}
	}
	if (!request_bundle_a.empty ())
	{
		std::weak_ptr<nano::node> node_w (node.shared ());
		node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::milliseconds (delay_a), [node_w, request_bundle_a, callback_a, delay_a]() {
			if (auto node_l = node_w.lock ())
			{
				node_l->network.broadcast_confirm_req_batched_many (request_bundle_a, callback_a, delay_a, true);
			}
		});
	}
	else if (callback_a)
	{
		callback_a ();
	}
}

void nano::network::broadcast_confirm_req_many (std::deque<std::pair<std::shared_ptr<nano::block>, std::shared_ptr<std::vector<std::shared_ptr<nano::transport::channel>>>>> requests_a, std::function<void()> callback_a, unsigned delay_a)
{
	auto pair_l (requests_a.front ());
	requests_a.pop_front ();
	auto block_l (pair_l.first);
	// confirm_req to representatives
	auto endpoints (pair_l.second);
	if (!endpoints->empty ())
	{
		broadcast_confirm_req_base (block_l, endpoints, delay_a);
	}
	/* Continue while blocks remain
	Broadcast with random delay between delay_a & 2*delay_a */
	if (!requests_a.empty ())
	{
		std::weak_ptr<nano::node> node_w (node.shared ());
		node.workers.add_timed_task (std::chrono::steady_clock::now () + std::chrono::milliseconds (delay_a + std::rand () % delay_a), [node_w, requests_a, callback_a, delay_a]() {
			if (auto node_l = node_w.lock ())
			{
				node_l->network.broadcast_confirm_req_many (requests_a, callback_a, delay_a);
			}
		});
	}
	else if (callback_a)
	{
		callback_a ();
	}
}

namespace
{
class network_message_visitor : public nano::message_visitor
{
public:
	network_message_visitor (nano::node & node_a, std::shared_ptr<nano::transport::channel> const & channel_a) :
	node (node_a),
	channel (channel_a)
	{
	}
	void keepalive (nano::keepalive const & message_a) override
	{
		if (node.config.logging.network_keepalive_logging ())
		{
			node.logger.try_log (boost::str (boost::format ("Received keepalive message from %1%") % channel->to_string ()));
		}
		node.stats.inc (nano::stat::type::message, nano::stat::detail::keepalive, nano::stat::dir::in);
		node.network.merge_peers (message_a.peers);
		// Check for special node port data
		auto peer0 (message_a.peers[0]);
		if (peer0.address () == boost::asio::ip::address_v6{} && peer0.port () != 0)
		{
			nano::endpoint new_endpoint (channel->get_tcp_endpoint ().address (), peer0.port ());
			node.network.merge_peer (new_endpoint);
		}
	}
	void publish (nano::publish const & message_a) override
	{
		if (node.config.logging.network_message_logging ())
		{
			node.logger.try_log (boost::str (boost::format ("Publish message from %1% for %2%") % channel->to_string () % message_a.block->hash ().to_string ()));
		}
		node.stats.inc (nano::stat::type::message, nano::stat::detail::publish, nano::stat::dir::in);
		if (!node.block_processor.full ())
		{
			node.process_active (message_a.block);
		}
		else
		{
			node.network.publish_filter.clear (message_a.digest);
			node.stats.inc (nano::stat::type::drop, nano::stat::detail::publish, nano::stat::dir::in);
		}
	}
	void confirm_req (nano::confirm_req const & message_a) override
	{
		if (node.config.logging.network_message_logging ())
		{
			if (!message_a.roots_hashes.empty ())
			{
				node.logger.try_log (boost::str (boost::format ("Confirm_req message from %1% for hashes:roots %2%") % channel->to_string () % message_a.roots_string ()));
			}
			else
			{
				node.logger.try_log (boost::str (boost::format ("Confirm_req message from %1% for %2%") % channel->to_string () % message_a.block->hash ().to_string ()));
			}
		}
		node.stats.inc (nano::stat::type::message, nano::stat::detail::confirm_req, nano::stat::dir::in);
		// Don't load nodes with disabled voting
		if (node.config.enable_voting && node.wallets.reps ().voting > 0)
		{
			if (message_a.block != nullptr)
			{
				node.aggregator.add (channel, { { message_a.block->hash (), message_a.block->root () } });
			}
			else if (!message_a.roots_hashes.empty ())
			{
				node.aggregator.add (channel, message_a.roots_hashes);
			}
		}
	}
	void confirm_ack (nano::confirm_ack const & message_a) override
	{
		if (node.config.logging.network_message_logging ())
		{
			node.logger.try_log (boost::str (boost::format ("Received confirm_ack message from %1% for %2% timestamp %3%") % channel->to_string () % message_a.vote->hashes_string () % std::to_string (message_a.vote->timestamp)));
		}
		node.stats.inc (nano::stat::type::message, nano::stat::detail::confirm_ack, nano::stat::dir::in);
		if (!message_a.vote->account.is_zero ())
		{
			if (message_a.header.block_type () != nano::block_type::not_a_block)
			{
				for (auto & vote_block : message_a.vote->blocks)
				{
					if (!vote_block.which ())
					{
						auto const & block (boost::get<std::shared_ptr<nano::block>> (vote_block));
						if (!node.block_processor.full ())
						{
							node.process_active (block);
						}
						else
						{
							node.stats.inc (nano::stat::type::drop, nano::stat::detail::confirm_ack, nano::stat::dir::in);
						}
					}
				}
			}
			node.vote_processor.vote (message_a.vote, channel);
		}
	}
	void bulk_pull (nano::bulk_pull const &) override
	{
		debug_assert (false);
	}
	void bulk_pull_account (nano::bulk_pull_account const &) override
	{
		debug_assert (false);
	}
	void bulk_push (nano::bulk_push const &) override
	{
		debug_assert (false);
	}
	void frontier_req (nano::frontier_req const &) override
	{
		debug_assert (false);
	}
	void node_id_handshake (nano::node_id_handshake const & message_a) override
	{
		node.stats.inc (nano::stat::type::message, nano::stat::detail::node_id_handshake, nano::stat::dir::in);
	}
	void telemetry_req (nano::telemetry_req const & message_a) override
	{
		if (node.config.logging.network_telemetry_logging ())
		{
			node.logger.try_log (boost::str (boost::format ("Telemetry_req message from %1%") % channel->to_string ()));
		}
		node.stats.inc (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in);

		// Send an empty telemetry_ack if we do not want, just to acknowledge that we have received the message to
		// remove any timeouts on the server side waiting for a message.
		nano::telemetry_ack telemetry_ack;
		if (!node.flags.disable_providing_telemetry_metrics)
		{
			auto telemetry_data = nano::local_telemetry_data (node.ledger, node.network, node.config.bandwidth_limit, node.network_params, node.startup_time, node.active.active_difficulty (), node.node_id);
			telemetry_ack = nano::telemetry_ack (telemetry_data);
		}
		channel->send (telemetry_ack, nullptr, nano::buffer_drop_policy::no_socket_drop);
	}
	void telemetry_ack (nano::telemetry_ack const & message_a) override
	{
		if (node.config.logging.network_telemetry_logging ())
		{
			node.logger.try_log (boost::str (boost::format ("Received telemetry_ack message from %1%") % channel->to_string ()));
		}
		node.stats.inc (nano::stat::type::message, nano::stat::detail::telemetry_ack, nano::stat::dir::in);
		if (node.telemetry)
		{
			node.telemetry->set (message_a, *channel);
		}
	}
	nano::node & node;
	std::shared_ptr<nano::transport::channel> channel;
};
}

void nano::network::process_message (nano::message const & message_a, std::shared_ptr<nano::transport::channel> const & channel_a)
{
	network_message_visitor visitor (node, channel_a);
	message_a.visit (visitor);
}

// Send keepalives to all the peers we've been notified of
void nano::network::merge_peers (std::array<nano::endpoint, 8> const & peers_a)
{
	for (auto i (peers_a.begin ()), j (peers_a.end ()); i != j; ++i)
	{
		merge_peer (*i);
	}
}

void nano::network::merge_peer (nano::endpoint const & peer_a)
{
	if (!reachout (peer_a, node.config.allow_local_peers))
	{
		std::weak_ptr<nano::node> node_w (node.shared ());
		node.network.tcp_channels.start_tcp (peer_a, [node_w](std::shared_ptr<nano::transport::channel> const & channel_a) {
			if (auto node_l = node_w.lock ())
			{
				node_l->network.send_keepalive (channel_a);
			}
		});
	}
}

bool nano::network::not_a_peer (nano::endpoint const & endpoint_a, bool allow_local_peers)
{
	bool result (false);
	if (endpoint_a.address ().to_v6 ().is_unspecified ())
	{
		result = true;
	}
	else if (nano::transport::reserved_address (endpoint_a, allow_local_peers))
	{
		result = true;
	}
	else if (endpoint_a == endpoint ())
	{
		result = true;
	}
	return result;
}

bool nano::network::reachout (nano::endpoint const & endpoint_a, bool allow_local_peers)
{
	// Don't contact invalid IPs
	bool error = not_a_peer (endpoint_a, allow_local_peers);
	if (!error)
	{
		error |= udp_channels.reachout (endpoint_a);
		error |= tcp_channels.reachout (endpoint_a);
	}
	return error;
}

std::deque<std::shared_ptr<nano::transport::channel>> nano::network::list (size_t count_a, uint8_t minimum_version_a, bool include_tcp_temporary_channels_a)
{
	std::deque<std::shared_ptr<nano::transport::channel>> result;
	tcp_channels.list (result, minimum_version_a, include_tcp_temporary_channels_a);
	udp_channels.list (result, minimum_version_a);
	nano::random_pool_shuffle (result.begin (), result.end ());
	if (result.size () > count_a)
	{
		result.resize (count_a, nullptr);
	}
	return result;
}

std::deque<std::shared_ptr<nano::transport::channel>> nano::network::list_non_pr (size_t count_a)
{
	std::deque<std::shared_ptr<nano::transport::channel>> result;
	tcp_channels.list (result);
	udp_channels.list (result);
	nano::random_pool_shuffle (result.begin (), result.end ());
	result.erase (std::remove_if (result.begin (), result.end (), [this](std::shared_ptr<nano::transport::channel> const & channel) {
		return this->node.rep_crawler.is_pr (*channel);
	}),
	result.end ());
	if (result.size () > count_a)
	{
		result.resize (count_a, nullptr);
	}
	return result;
}

// Simulating with sqrt_broadcast_simulate shows we only need to broadcast to sqrt(total_peers) random peers in order to successfully publish to everyone with high probability
size_t nano::network::fanout (float scale) const
{
	return static_cast<size_t> (std::ceil (scale * size_sqrt ()));
}

std::unordered_set<std::shared_ptr<nano::transport::channel>> nano::network::random_set (size_t count_a, uint8_t min_version_a, bool include_temporary_channels_a) const
{
	std::unordered_set<std::shared_ptr<nano::transport::channel>> result (tcp_channels.random_set (count_a, min_version_a, include_temporary_channels_a));
	std::unordered_set<std::shared_ptr<nano::transport::channel>> udp_random (udp_channels.random_set (count_a, min_version_a));
	for (auto i (udp_random.begin ()), n (udp_random.end ()); i != n && result.size () < count_a * 1.5; ++i)
	{
		result.insert (*i);
	}
	while (result.size () > count_a)
	{
		result.erase (result.begin ());
	}
	return result;
}

void nano::network::random_fill (std::array<nano::endpoint, 8> & target_a) const
{
	auto peers (random_set (target_a.size (), 0, false)); // Don't include channels with ephemeral remote ports
	debug_assert (peers.size () <= target_a.size ());
	auto endpoint (nano::endpoint (boost::asio::ip::address_v6{}, 0));
	debug_assert (endpoint.address ().is_v6 ());
	std::fill (target_a.begin (), target_a.end (), endpoint);
	auto j (target_a.begin ());
	for (auto i (peers.begin ()), n (peers.end ()); i != n; ++i, ++j)
	{
		debug_assert ((*i)->get_endpoint ().address ().is_v6 ());
		debug_assert (j < target_a.end ());
		*j = (*i)->get_endpoint ();
	}
}

void nano::network::fill_keepalive_self (std::array<nano::endpoint, 8> & target_a) const
{
	random_fill (target_a);
	// Replace part of message with node external address or listening port
	target_a[1] = nano::endpoint (boost::asio::ip::address_v6{}, 0); // For node v19 (response channels)
	if (node.config.external_address != boost::asio::ip::address_v6{}.to_string () && node.config.external_port != 0)
	{
		target_a[0] = nano::endpoint (boost::asio::ip::make_address_v6 (node.config.external_address), node.config.external_port);
	}
	else
	{
		auto external_address (node.port_mapping.external_address ());
		if (external_address.address () != boost::asio::ip::address_v4::any ())
		{
			target_a[0] = nano::endpoint (boost::asio::ip::address_v6{}, port);
			boost::system::error_code ec;
			auto external_v6 = boost::asio::ip::make_address_v6 (external_address.address ().to_string (), ec);
			target_a[1] = nano::endpoint (external_v6, external_address.port ());
		}
		else
		{
			target_a[0] = nano::endpoint (boost::asio::ip::address_v6{}, port);
		}
	}
}

nano::tcp_endpoint nano::network::bootstrap_peer (bool lazy_bootstrap)
{
	nano::tcp_endpoint result (boost::asio::ip::address_v6::any (), 0);
	bool use_udp_peer (nano::random_pool::generate_word32 (0, 1));
	if (use_udp_peer || tcp_channels.size () == 0)
	{
		result = udp_channels.bootstrap_peer (node.network_params.protocol.protocol_version_min ());
	}
	if (result == nano::tcp_endpoint (boost::asio::ip::address_v6::any (), 0))
	{
		result = tcp_channels.bootstrap_peer (node.network_params.protocol.protocol_version_min ());
	}
	return result;
}

std::shared_ptr<nano::transport::channel> nano::network::find_channel (nano::endpoint const & endpoint_a)
{
	std::shared_ptr<nano::transport::channel> result (tcp_channels.find_channel (nano::transport::map_endpoint_to_tcp (endpoint_a)));
	if (!result)
	{
		result = udp_channels.channel (endpoint_a);
	}
	return result;
}

std::shared_ptr<nano::transport::channel> nano::network::find_node_id (nano::account const & node_id_a)
{
	std::shared_ptr<nano::transport::channel> result (tcp_channels.find_node_id (node_id_a));
	if (!result)
	{
		result = udp_channels.find_node_id (node_id_a);
	}
	return result;
}

nano::endpoint nano::network::endpoint ()
{
	return nano::endpoint (boost::asio::ip::address_v6::loopback (), port);
}

void nano::network::cleanup (std::chrono::steady_clock::time_point const & cutoff_a)
{
	tcp_channels.purge (cutoff_a);
	udp_channels.purge (cutoff_a);
	if (node.network.empty ())
	{
		disconnect_observer ();
	}
}

void nano::network::ongoing_cleanup ()
{
	cleanup (std::chrono::steady_clock::now () - node.network_params.node.cutoff);
	std::weak_ptr<nano::node> node_w (node.shared ());
	node.workers.add_timed_task (std::chrono::steady_clock::now () + node.network_params.node.period, [node_w]() {
		if (auto node_l = node_w.lock ())
		{
			node_l->network.ongoing_cleanup ();
		}
	});
}

void nano::network::ongoing_syn_cookie_cleanup ()
{
	syn_cookies.purge (std::chrono::steady_clock::now () - nano::transport::syn_cookie_cutoff);
	std::weak_ptr<nano::node> node_w (node.shared ());
	node.workers.add_timed_task (std::chrono::steady_clock::now () + (nano::transport::syn_cookie_cutoff * 2), [node_w]() {
		if (auto node_l = node_w.lock ())
		{
			node_l->network.ongoing_syn_cookie_cleanup ();
		}
	});
}

void nano::network::ongoing_keepalive ()
{
	flood_keepalive (0.75f);
	flood_keepalive_self (0.25f);
	std::weak_ptr<nano::node> node_w (node.shared ());
	node.workers.add_timed_task (std::chrono::steady_clock::now () + node.network_params.node.half_period, [node_w]() {
		if (auto node_l = node_w.lock ())
		{
			node_l->network.ongoing_keepalive ();
		}
	});
}

size_t nano::network::size () const
{
	return tcp_channels.size () + udp_channels.size ();
}

float nano::network::size_sqrt () const
{
	return static_cast<float> (std::sqrt (size ()));
}

bool nano::network::empty () const
{
	return size () == 0;
}

void nano::network::erase (nano::transport::channel const & channel_a)
{
	auto const channel_type = channel_a.get_type ();
	if (channel_type == nano::transport::transport_type::tcp)
	{
		tcp_channels.erase (channel_a.get_tcp_endpoint ());
	}
	else if (channel_type != nano::transport::transport_type::loopback)
	{
		udp_channels.erase (channel_a.get_endpoint ());
		udp_channels.clean_node_id (channel_a.get_node_id ());
	}
}

nano::message_buffer_manager::message_buffer_manager (nano::stat & stats_a, size_t size, size_t count) :
stats (stats_a),
free (count),
full (count),
slab (size * count),
entries (count),
stopped (false)
{
	debug_assert (count > 0);
	debug_assert (size > 0);
	auto slab_data (slab.data ());
	auto entry_data (entries.data ());
	for (auto i (0); i < count; ++i, ++entry_data)
	{
		*entry_data = { slab_data + i * size, 0, nano::endpoint () };
		free.push_back (entry_data);
	}
}

nano::message_buffer * nano::message_buffer_manager::allocate ()
{
	nano::unique_lock<nano::mutex> lock (mutex);
	if (!stopped && free.empty () && full.empty ())
	{
		stats.inc (nano::stat::type::udp, nano::stat::detail::blocking, nano::stat::dir::in);
		condition.wait (lock, [& stopped = stopped, &free = free, &full = full] { return stopped || !free.empty () || !full.empty (); });
	}
	nano::message_buffer * result (nullptr);
	if (!free.empty ())
	{
		result = free.front ();
		free.pop_front ();
	}
	if (result == nullptr && !full.empty ())
	{
		result = full.front ();
		full.pop_front ();
		stats.inc (nano::stat::type::udp, nano::stat::detail::overflow, nano::stat::dir::in);
	}
	release_assert (result || stopped);
	return result;
}

void nano::message_buffer_manager::enqueue (nano::message_buffer * data_a)
{
	debug_assert (data_a != nullptr);
	{
		nano::lock_guard<nano::mutex> lock (mutex);
		full.push_back (data_a);
	}
	condition.notify_all ();
}

nano::message_buffer * nano::message_buffer_manager::dequeue ()
{
	nano::unique_lock<nano::mutex> lock (mutex);
	while (!stopped && full.empty ())
	{
		condition.wait (lock);
	}
	nano::message_buffer * result (nullptr);
	if (!full.empty ())
	{
		result = full.front ();
		full.pop_front ();
	}
	return result;
}

void nano::message_buffer_manager::release (nano::message_buffer * data_a)
{
	debug_assert (data_a != nullptr);
	{
		nano::lock_guard<nano::mutex> lock (mutex);
		free.push_back (data_a);
	}
	condition.notify_all ();
}

void nano::message_buffer_manager::stop ()
{
	{
		nano::lock_guard<nano::mutex> lock (mutex);
		stopped = true;
	}
	condition.notify_all ();
}

nano::tcp_message_manager::tcp_message_manager (unsigned incoming_connections_max_a) :
max_entries (incoming_connections_max_a * nano::tcp_message_manager::max_entries_per_connection + 1)
{
	debug_assert (max_entries > 0);
}

void nano::tcp_message_manager::put_message (nano::tcp_message_item const & item_a)
{
	{
		nano::unique_lock<nano::mutex> lock (mutex);
		while (entries.size () >= max_entries && !stopped)
		{
			producer_condition.wait (lock);
		}
		entries.push_back (item_a);
	}
	consumer_condition.notify_one ();
}

nano::tcp_message_item nano::tcp_message_manager::get_message ()
{
	nano::tcp_message_item result;
	nano::unique_lock<nano::mutex> lock (mutex);
	while (entries.empty () && !stopped)
	{
		consumer_condition.wait (lock);
	}
	if (!entries.empty ())
	{
		result = std::move (entries.front ());
		entries.pop_front ();
	}
	else
	{
		result = nano::tcp_message_item{ std::make_shared<nano::keepalive> (), nano::tcp_endpoint (boost::asio::ip::address_v6::any (), 0), 0, nullptr, nano::bootstrap_server_type::undefined };
	}
	lock.unlock ();
	producer_condition.notify_one ();
	return result;
}

void nano::tcp_message_manager::stop ()
{
	{
		nano::lock_guard<nano::mutex> lock (mutex);
		stopped = true;
	}
	consumer_condition.notify_all ();
	producer_condition.notify_all ();
}

nano::syn_cookies::syn_cookies (size_t max_cookies_per_ip_a) :
max_cookies_per_ip (max_cookies_per_ip_a)
{
}

boost::optional<nano::uint256_union> nano::syn_cookies::assign (nano::endpoint const & endpoint_a)
{
	auto ip_addr (endpoint_a.address ());
	debug_assert (ip_addr.is_v6 ());
	nano::lock_guard<nano::mutex> lock (syn_cookie_mutex);
	unsigned & ip_cookies = cookies_per_ip[ip_addr];
	boost::optional<nano::uint256_union> result;
	if (ip_cookies < max_cookies_per_ip)
	{
		if (cookies.find (endpoint_a) == cookies.end ())
		{
			nano::uint256_union query;
			random_pool::generate_block (query.bytes.data (), query.bytes.size ());
			syn_cookie_info info{ query, std::chrono::steady_clock::now () };
			cookies[endpoint_a] = info;
			++ip_cookies;
			result = query;
		}
	}
	return result;
}

bool nano::syn_cookies::validate (nano::endpoint const & endpoint_a, nano::account const & node_id, nano::signature const & sig)
{
	auto ip_addr (endpoint_a.address ());
	debug_assert (ip_addr.is_v6 ());
	nano::lock_guard<nano::mutex> lock (syn_cookie_mutex);
	auto result (true);
	auto cookie_it (cookies.find (endpoint_a));
	if (cookie_it != cookies.end () && !nano::validate_message (node_id, cookie_it->second.cookie, sig))
	{
		result = false;
		cookies.erase (cookie_it);
		unsigned & ip_cookies = cookies_per_ip[ip_addr];
		if (ip_cookies > 0)
		{
			--ip_cookies;
		}
		else
		{
			debug_assert (false && "More SYN cookies deleted than created for IP");
		}
	}
	return result;
}

void nano::syn_cookies::purge (std::chrono::steady_clock::time_point const & cutoff_a)
{
	nano::lock_guard<nano::mutex> lock (syn_cookie_mutex);
	auto it (cookies.begin ());
	while (it != cookies.end ())
	{
		auto info (it->second);
		if (info.created_at < cutoff_a)
		{
			unsigned & per_ip = cookies_per_ip[it->first.address ()];
			if (per_ip > 0)
			{
				--per_ip;
			}
			else
			{
				debug_assert (false && "More SYN cookies deleted than created for IP");
			}
			it = cookies.erase (it);
		}
		else
		{
			++it;
		}
	}
}

size_t nano::syn_cookies::cookies_size ()
{
	nano::lock_guard<nano::mutex> lock (syn_cookie_mutex);
	return cookies.size ();
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (network & network, std::string const & name)
{
	auto composite = std::make_unique<container_info_composite> (name);
	composite->add_component (network.tcp_channels.collect_container_info ("tcp_channels"));
	composite->add_component (network.udp_channels.collect_container_info ("udp_channels"));
	composite->add_component (network.syn_cookies.collect_container_info ("syn_cookies"));
	composite->add_component (collect_container_info (network.excluded_peers, "excluded_peers"));
	return composite;
}

std::unique_ptr<nano::container_info_component> nano::syn_cookies::collect_container_info (std::string const & name)
{
	size_t syn_cookies_count;
	size_t syn_cookies_per_ip_count;
	{
		nano::lock_guard<nano::mutex> syn_cookie_guard (syn_cookie_mutex);
		syn_cookies_count = cookies.size ();
		syn_cookies_per_ip_count = cookies_per_ip.size ();
	}
	auto composite = std::make_unique<container_info_composite> (name);
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "syn_cookies", syn_cookies_count, sizeof (decltype (cookies)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "syn_cookies_per_ip", syn_cookies_per_ip_count, sizeof (decltype (cookies_per_ip)::value_type) }));
	return composite;
}
