#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/bootstrap/bootstrap.hpp>
#include <nano/node/bootstrap/bootstrap_bulk_push.hpp>
#include <nano/node/bootstrap/bootstrap_frontier.hpp>
#include <nano/node/common.hpp>
#include <nano/node/node.hpp>
#include <nano/node/transport/tcp.hpp>
#include <nano/node/transport/udp.hpp>

#include <boost/format.hpp>

#include <algorithm>

constexpr double nano::bootstrap_limits::bootstrap_connection_scale_target_blocks;
constexpr double nano::bootstrap_limits::bootstrap_connection_scale_target_blocks_lazy;
constexpr double nano::bootstrap_limits::bootstrap_minimum_blocks_per_sec;
constexpr double nano::bootstrap_limits::bootstrap_minimum_termination_time_sec;
constexpr unsigned nano::bootstrap_limits::bootstrap_max_new_connections;
constexpr size_t nano::bootstrap_limits::bootstrap_max_confirm_frontiers;
constexpr double nano::bootstrap_limits::required_frontier_confirmation_ratio;
constexpr unsigned nano::bootstrap_limits::frontier_confirmation_blocks_limit;
constexpr unsigned nano::bootstrap_limits::requeued_pulls_limit;
constexpr unsigned nano::bootstrap_limits::requeued_pulls_limit_test;
constexpr std::chrono::seconds nano::bootstrap_limits::lazy_flush_delay_sec;
constexpr unsigned nano::bootstrap_limits::lazy_destinations_request_limit;
constexpr uint64_t nano::bootstrap_limits::lazy_batch_pull_count_resize_blocks_limit;
constexpr double nano::bootstrap_limits::lazy_batch_pull_count_resize_ratio;
constexpr size_t nano::bootstrap_limits::lazy_blocks_restart_limit;
constexpr std::chrono::hours nano::bootstrap_excluded_peers::exclude_time_hours;
constexpr std::chrono::hours nano::bootstrap_excluded_peers::exclude_remove_hours;

nano::bootstrap_client::bootstrap_client (std::shared_ptr<nano::node> node_a, std::shared_ptr<nano::bootstrap_attempt> attempt_a, std::shared_ptr<nano::transport::channel_tcp> channel_a, std::shared_ptr<nano::socket> socket_a)
	: node (node_a)
	, attempt (attempt_a)
	, channel (channel_a)
	, socket (socket_a)
	, receive_buffer (std::make_shared<std::vector<uint8_t>> ())
	, start_time (std::chrono::steady_clock::now ())
	, block_count (0)
	, pending_stop (false)
	, hard_stop (false)
{
	++attempt->connections;
	receive_buffer->resize (256);
}

nano::bootstrap_client::~bootstrap_client ()
{
	--attempt->connections;
}

double nano::bootstrap_client::block_rate () const
{
	auto elapsed = std::max (elapsed_seconds (), nano::bootstrap_limits::bootstrap_minimum_elapsed_seconds_blockrate);
	return static_cast<double> (block_count.load () / elapsed);
}

double nano::bootstrap_client::elapsed_seconds () const
{
	return std::chrono::duration_cast<std::chrono::duration<double>> (std::chrono::steady_clock::now () - start_time).count ();
}

void nano::bootstrap_client::stop (bool force)
{
	pending_stop = true;
	if (force)
	{
		hard_stop = true;
	}
}

std::shared_ptr<nano::bootstrap_client> nano::bootstrap_client::shared ()
{
	return shared_from_this ();
}

nano::bootstrap_attempt::bootstrap_attempt (std::shared_ptr<nano::node> node_a, nano::bootstrap_mode mode_a)
	: next_log (std::chrono::steady_clock::now ())
	, node (node_a)
	, mode (mode_a)
{
	node->logger.always_log ("Starting bootstrap attempt");
	node->bootstrap_initiator.notify_listeners (true);
}

nano::bootstrap_attempt::~bootstrap_attempt ()
{
	node->logger.always_log ("Exiting bootstrap attempt");
	node->bootstrap_initiator.notify_listeners (false);
}

bool nano::bootstrap_attempt::should_log ()
{
	nano::lock_guard<std::mutex> guard (next_log_mutex);
	auto result (false);
	auto now (std::chrono::steady_clock::now ());
	if (next_log < now)
	{
		result = true;
		next_log = now + std::chrono::seconds (15);
	}
	return result;
}

bool nano::bootstrap_attempt::request_frontier (nano::unique_lock<std::mutex> & lock_a, bool first_attempt)
{
	auto result (true);
	auto connection_l (connection (lock_a, first_attempt));
	connection_frontier_request = connection_l;
	if (connection_l)
	{
		endpoint_frontier_request = connection_l->channel->get_tcp_endpoint ();
		std::future<bool> future;
		{
			auto client (std::make_shared<nano::frontier_req_client> (connection_l));
			client->run ();
			frontiers = client;
			future = client->promise.get_future ();
		}
		lock_a.unlock ();
		result = consume_future (future); // This is out of scope of `client' so when the last reference via boost::asio::io_context is lost and the client is destroyed, the future throws an exception.
		lock_a.lock ();
		if (result)
		{
			pulls.clear ();
		}
		if (node->config.logging.network_logging ())
		{
			if (!result)
			{
				node->logger.try_log (boost::str (boost::format ("Completed frontier request, %1% out of sync accounts according to %2%") % pulls.size () % connection_l->channel->to_string ()));
			}
			else
			{
				node->stats.inc (nano::stat::type::error, nano::stat::detail::frontier_req, nano::stat::dir::out);
			}
		}
	}
	return result;
}

void nano::bootstrap_attempt::request_pull (nano::unique_lock<std::mutex> & lock_a)
{
	auto connection_l (connection (lock_a));
	if (connection_l)
	{
		auto pull (pulls.front ());
		pulls.pop_front ();
		if (mode != nano::bootstrap_mode::legacy)
		{
			// Check if pull is obsolete (head was processed)
			while (!pulls.empty () && !pull.head.is_zero () && lazy_processed_or_exists (pull.head))
			{
				pull = pulls.front ();
				pulls.pop_front ();
			}
		}
		recent_pulls_head.push_back (pull.head);
		if (recent_pulls_head.size () > nano::bootstrap_limits::bootstrap_max_confirm_frontiers)
		{
			recent_pulls_head.pop_front ();
		}
		++pulling;
		// The bulk_pull_client destructor attempt to requeue_pull which can cause a deadlock if this is the last reference
		// Dispatch request in an external thread in case it needs to be destroyed
		node->background ([connection_l, pull]() {
			auto client (std::make_shared<nano::bulk_pull_client> (connection_l, pull));
			client->request ();
		});
	}
}

void nano::bootstrap_attempt::request_push (nano::unique_lock<std::mutex> & lock_a)
{
	bool error (false);
	if (auto connection_shared = connection_frontier_request.lock ())
	{
		std::future<bool> future;
		{
			auto client (std::make_shared<nano::bulk_push_client> (connection_shared));
			client->start ();
			push = client;
			future = client->promise.get_future ();
		}
		lock_a.unlock ();
		error = consume_future (future); // This is out of scope of `client' so when the last reference via boost::asio::io_context is lost and the client is destroyed, the future throws an exception.
		lock_a.lock ();
	}
	if (node->config.logging.network_logging ())
	{
		node->logger.try_log ("Exiting bulk push client");
		if (error)
		{
			node->logger.try_log ("Bulk push client failed");
		}
	}
}

bool nano::bootstrap_attempt::still_pulling ()
{
	assert (!mutex.try_lock ());
	auto running (!stopped);
	auto more_pulls (!pulls.empty ());
	auto still_pulling (pulling > 0);
	return running && (more_pulls || still_pulling);
}

void nano::bootstrap_attempt::run_start (nano::unique_lock<std::mutex> & lock_a)
{
	frontiers_received = false;
	frontiers_confirmed = false;
	total_blocks = 0;
	requeued_pulls = 0;
	pulls.clear ();
	recent_pulls_head.clear ();
	auto frontier_failure (true);
	uint64_t frontier_attempts (0);
	while (!stopped && frontier_failure)
	{
		++frontier_attempts;
		frontier_failure = request_frontier (lock_a, frontier_attempts == 1);
	}
	frontiers_received = true;
	// Shuffle pulls.
	release_assert (std::numeric_limits<CryptoPP::word32>::max () > pulls.size ());
	if (!pulls.empty ())
	{
		for (auto i = static_cast<CryptoPP::word32> (pulls.size () - 1); i > 0; --i)
		{
			auto k = nano::random_pool::generate_word32 (0, i);
			std::swap (pulls[i], pulls[k]);
		}
	}
}

void nano::bootstrap_attempt::run ()
{
	assert (!node->flags.disable_legacy_bootstrap);
	start_populate_connections ();
	nano::unique_lock<std::mutex> lock (mutex);
	run_start (lock);
	while (still_pulling ())
	{
		while (still_pulling ())
		{
			if (!pulls.empty ())
			{
				request_pull (lock);
			}
			else
			{
				condition.wait (lock);
			}
			attempt_restart_check (lock);
		}
		// Flushing may resolve forks which can add more pulls
		node->logger.try_log ("Flushing unchecked blocks");
		lock.unlock ();
		node->block_processor.flush ();
		lock.lock ();
		node->logger.try_log ("Finished flushing unchecked blocks");
	}
	if (!stopped)
	{
		node->logger.try_log ("Completed pulls");
		if (!node->flags.disable_bootstrap_bulk_push_client)
		{
			request_push (lock);
		}
		++runs_count;
		// Start wallet lazy bootstrap if required
		if (!wallet_accounts.empty () && !node->flags.disable_wallet_bootstrap)
		{
			lock.unlock ();
			mode = nano::bootstrap_mode::wallet_lazy;
			total_blocks = 0;
			wallet_run ();
			lock.lock ();
		}
		// Start lazy bootstrap if some lazy keys were inserted
		else if (runs_count < 3 && !lazy_finished () && !node->flags.disable_lazy_bootstrap)
		{
			lock.unlock ();
			mode = nano::bootstrap_mode::lazy;
			total_blocks = 0;
			lazy_run ();
			lock.lock ();
		}
		if (!stopped)
		{
			node->unchecked_cleanup ();
		}
	}
	stopped = true;
	condition.notify_all ();
	idle.clear ();
}

std::shared_ptr<nano::bootstrap_client> nano::bootstrap_attempt::connection (nano::unique_lock<std::mutex> & lock_a, bool use_front_connection)
{
	// clang-format off
	condition.wait (lock_a, [& stopped = stopped, &idle = idle] { return stopped || !idle.empty (); });
	// clang-format on
	std::shared_ptr<nano::bootstrap_client> result;
	if (!idle.empty ())
	{
		if (!use_front_connection)
		{
			result = idle.back ();
			idle.pop_back ();
		}
		else
		{
			result = idle.front ();
			idle.pop_front ();
		}
	}
	return result;
}

bool nano::bootstrap_attempt::consume_future (std::future<bool> & future_a)
{
	bool result;
	try
	{
		result = future_a.get ();
	}
	catch (std::future_error &)
	{
		result = true;
	}
	return result;
}

struct block_rate_cmp
{
	bool operator() (const std::shared_ptr<nano::bootstrap_client> & lhs, const std::shared_ptr<nano::bootstrap_client> & rhs) const
	{
		return lhs->block_rate () > rhs->block_rate ();
	}
};

unsigned nano::bootstrap_attempt::target_connections (size_t pulls_remaining)
{
	if (node->config.bootstrap_connections >= node->config.bootstrap_connections_max)
	{
		return std::max (1U, node->config.bootstrap_connections_max);
	}

	// Only scale up to bootstrap_connections_max for large pulls.
	double target_blocks = (mode == nano::bootstrap_mode::lazy) ? nano::bootstrap_limits::bootstrap_connection_scale_target_blocks_lazy : nano::bootstrap_limits::bootstrap_connection_scale_target_blocks;
	double step_scale = std::min (1.0, std::max (0.0, (double)pulls_remaining / target_blocks));
	double lazy_term = (mode == nano::bootstrap_mode::lazy) ? (double)node->config.bootstrap_connections : 0.0;
	double target = (double)node->config.bootstrap_connections + (double)(node->config.bootstrap_connections_max - node->config.bootstrap_connections) * step_scale + lazy_term;
	return std::max (1U, (unsigned)(target + 0.5f));
}

void nano::bootstrap_attempt::populate_connections ()
{
	double rate_sum = 0.0;
	size_t num_pulls = 0;
	std::priority_queue<std::shared_ptr<nano::bootstrap_client>, std::vector<std::shared_ptr<nano::bootstrap_client>>, block_rate_cmp> sorted_connections;
	std::unordered_set<nano::tcp_endpoint> endpoints;
	{
		nano::unique_lock<std::mutex> lock (mutex);
		num_pulls = pulls.size ();
		std::deque<std::weak_ptr<nano::bootstrap_client>> new_clients;
		for (auto & c : clients)
		{
			if (auto client = c.lock ())
			{
				if (auto socket_l = client->channel->socket.lock ())
				{
					new_clients.push_back (client);
					endpoints.insert (socket_l->remote_endpoint ());
					double elapsed_sec = client->elapsed_seconds ();
					auto blocks_per_sec = client->block_rate ();
					rate_sum += blocks_per_sec;
					if (client->elapsed_seconds () > nano::bootstrap_limits::bootstrap_connection_warmup_time_sec && client->block_count > 0)
					{
						sorted_connections.push (client);
					}
					// Force-stop the slowest peers, since they can take the whole bootstrap hostage by dribbling out blocks on the last remaining pull.
					// This is ~1.5kilobits/sec.
					if (elapsed_sec > nano::bootstrap_limits::bootstrap_minimum_termination_time_sec && blocks_per_sec < nano::bootstrap_limits::bootstrap_minimum_blocks_per_sec)
					{
						if (node->config.logging.bulk_pull_logging ())
						{
							node->logger.try_log (boost::str (boost::format ("Stopping slow peer %1% (elapsed sec %2%s > %3%s and %4% blocks per second < %5%)") % client->channel->to_string () % elapsed_sec % nano::bootstrap_limits::bootstrap_minimum_termination_time_sec % blocks_per_sec % nano::bootstrap_limits::bootstrap_minimum_blocks_per_sec));
						}

						client->stop (true);
						new_clients.pop_back ();
					}
				}
			}
		}
		// Cleanup expired clients
		clients.swap (new_clients);
	}

	auto target = target_connections (num_pulls);

	// We only want to drop slow peers when more than 2/3 are active. 2/3 because 1/2 is too aggressive, and 100% rarely happens.
	// Probably needs more tuning.
	if (sorted_connections.size () >= (target * 2) / 3 && target >= 4)
	{
		// 4 -> 1, 8 -> 2, 16 -> 4, arbitrary, but seems to work well.
		auto drop = (int)roundf (sqrtf ((float)target - 2.0f));

		if (node->config.logging.bulk_pull_logging ())
		{
			node->logger.try_log (boost::str (boost::format ("Dropping %1% bulk pull peers, target connections %2%") % drop % target));
		}

		for (int i = 0; i < drop; i++)
		{
			auto client = sorted_connections.top ();

			if (node->config.logging.bulk_pull_logging ())
			{
				node->logger.try_log (boost::str (boost::format ("Dropping peer with block rate %1%, block count %2% (%3%) ") % client->block_rate () % client->block_count % client->channel->to_string ()));
			}

			client->stop (false);
			sorted_connections.pop ();
		}
	}

	if (node->config.logging.bulk_pull_logging ())
	{
		nano::unique_lock<std::mutex> lock (mutex);
		node->logger.try_log (boost::str (boost::format ("Bulk pull connections: %1%, rate: %2% blocks/sec, remaining account pulls: %3%, total blocks: %4%") % connections.load () % (int)rate_sum % pulls.size () % (int)total_blocks.load ()));
	}

	if (connections < target)
	{
		auto delta = std::min ((target - connections) * 2, nano::bootstrap_limits::bootstrap_max_new_connections);
		// TODO - tune this better
		// Not many peers respond, need to try to make more connections than we need.
		for (auto i = 0u; i < delta; i++)
		{
			auto endpoint (node->network.bootstrap_peer (mode == nano::bootstrap_mode::lazy));
			if (endpoint != nano::tcp_endpoint (boost::asio::ip::address_v6::any (), 0) && endpoints.find (endpoint) == endpoints.end () && !node->bootstrap_initiator.excluded_peers.check (endpoint))
			{
				connect_client (endpoint);
				nano::lock_guard<std::mutex> lock (mutex);
				endpoints.insert (endpoint);
			}
			else if (connections == 0)
			{
				node->logger.try_log (boost::str (boost::format ("Bootstrap stopped because there are no peers")));
				stopped = true;
				condition.notify_all ();
			}
		}
	}
	if (!stopped)
	{
		std::weak_ptr<nano::bootstrap_attempt> this_w (shared_from_this ());
		node->alarm.add (std::chrono::steady_clock::now () + std::chrono::seconds (1), [this_w]() {
			if (auto this_l = this_w.lock ())
			{
				this_l->populate_connections ();
			}
		});
	}
}

void nano::bootstrap_attempt::start_populate_connections ()
{
	if (!populate_connections_started.exchange (true))
	{
		populate_connections ();
	}
}

void nano::bootstrap_attempt::add_connection (nano::endpoint const & endpoint_a)
{
	connect_client (nano::tcp_endpoint (endpoint_a.address (), endpoint_a.port ()));
}

void nano::bootstrap_attempt::connect_client (nano::tcp_endpoint const & endpoint_a)
{
	++connections;
	auto socket (std::make_shared<nano::socket> (node));
	auto this_l (shared_from_this ());
	socket->async_connect (endpoint_a,
		[this_l, socket, endpoint_a](boost::system::error_code const & ec) {
			if (!ec)
			{
				if (this_l->node->config.logging.bulk_pull_logging ())
				{
					this_l->node->logger.try_log (boost::str (boost::format ("Connection established to %1%") % endpoint_a));
				}
				auto client (std::make_shared<nano::bootstrap_client> (this_l->node, this_l, std::make_shared<nano::transport::channel_tcp> (*this_l->node, socket), socket));
				this_l->pool_connection (client);
			}
			else
			{
				if (this_l->node->config.logging.network_logging ())
				{
					switch (ec.value ())
					{
						default:
							this_l->node->logger.try_log (boost::str (boost::format ("Error initiating bootstrap connection to %1%: %2%") % endpoint_a % ec.message ()));
							break;
						case boost::system::errc::connection_refused:
						case boost::system::errc::operation_canceled:
						case boost::system::errc::timed_out:
						case 995: //Windows The I/O operation has been aborted because of either a thread exit or an application request
						case 10061: //Windows No connection could be made because the target machine actively refused it
							break;
					}
				}
			}
			--this_l->connections;
		});
}

void nano::bootstrap_attempt::pool_connection (std::shared_ptr<nano::bootstrap_client> client_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	if (!stopped && !client_a->pending_stop && !node->bootstrap_initiator.excluded_peers.check (client_a->channel->get_tcp_endpoint ()))
	{
		// Idle bootstrap client socket
		if (auto socket_l = client_a->channel->socket.lock ())
		{
			socket_l->start_timer (node->network_params.node.idle_timeout);
			// Push into idle deque
			idle.push_back (client_a);
		}
	}
	condition.notify_all ();
}

void nano::bootstrap_attempt::stop ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	stopped = true;
	condition.notify_all ();
	for (auto i : clients)
	{
		if (auto client = i.lock ())
		{
			client->socket->close ();
		}
	}
	if (auto i = frontiers.lock ())
	{
		try
		{
			i->promise.set_value (true);
		}
		catch (std::future_error &)
		{
		}
	}
	if (auto i = push.lock ())
	{
		try
		{
			i->promise.set_value (true);
		}
		catch (std::future_error &)
		{
		}
	}
}

void nano::bootstrap_attempt::add_pull (nano::pull_info const & pull_a)
{
	nano::pull_info pull (pull_a);
	node->bootstrap_initiator.cache.update_pull (pull);
	{
		nano::lock_guard<std::mutex> lock (mutex);
		pulls.push_back (pull);
	}
	condition.notify_all ();
}

void nano::bootstrap_attempt::requeue_pull (nano::pull_info const & pull_a, bool network_error)
{
	auto pull (pull_a);
	if (!network_error)
	{
		++pull.attempts;
	}
	++requeued_pulls;
	if (mode != nano::bootstrap_mode::lazy && pull.attempts < pull.retry_limit + (pull.processed / 10000))
	{
		nano::lock_guard<std::mutex> lock (mutex);
		pulls.push_front (pull);
		condition.notify_all ();
	}
	else if (mode == nano::bootstrap_mode::lazy && (pull.retry_limit == std::numeric_limits<unsigned>::max () || pull.attempts <= pull.retry_limit + (pull.processed / node->network_params.bootstrap.lazy_max_pull_blocks)))
	{
		assert (pull.account_or_head == pull.head);
		if (!lazy_processed_or_exists (pull.account_or_head))
		{
			// Retry for lazy pulls
			nano::lock_guard<std::mutex> lock (mutex);
			pulls.push_back (pull);
			condition.notify_all ();
		}
	}
	else
	{
		if (node->config.logging.bulk_pull_logging ())
		{
			node->logger.try_log (boost::str (boost::format ("Failed to pull account %1% down to %2% after %3% attempts and %4% blocks processed") % pull.account_or_head.to_account () % pull.end.to_string () % pull.attempts % pull.processed));
		}
		node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::bulk_pull_failed_account, nano::stat::dir::in);

		node->bootstrap_initiator.cache.add (pull);
		if (mode == nano::bootstrap_mode::lazy && pull.processed > 0)
		{
			assert (pull.account_or_head == pull.head);
			nano::lock_guard<std::mutex> lazy_lock (lazy_mutex);
			lazy_add (pull.account_or_head, pull.retry_limit);
		}
	}
}

void nano::bootstrap_attempt::add_bulk_push_target (nano::block_hash const & head, nano::block_hash const & end)
{
	nano::lock_guard<std::mutex> lock (mutex);
	bulk_push_targets.emplace_back (head, end);
}

void nano::bootstrap_attempt::attempt_restart_check (nano::unique_lock<std::mutex> & lock_a)
{
	/* Conditions to start frontiers confirmation:
	- not completed frontiers confirmation
	- more than 256 pull retries usually indicating issues with requested pulls
	- or 128k processed blocks indicating large bootstrap */
	if (!frontiers_confirmed && (requeued_pulls > (!node->network_params.network.is_test_network () ? nano::bootstrap_limits::requeued_pulls_limit : nano::bootstrap_limits::requeued_pulls_limit_test) || total_blocks > nano::bootstrap_limits::frontier_confirmation_blocks_limit))
	{
		auto confirmed (confirm_frontiers (lock_a));
		assert (lock_a.owns_lock ());
		if (!confirmed)
		{
			node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::frontier_confirmation_failed, nano::stat::dir::in);
			auto score (node->bootstrap_initiator.excluded_peers.add (endpoint_frontier_request, node->network.size ()));
			if (score >= nano::bootstrap_excluded_peers::score_limit)
			{
				node->logger.always_log (boost::str (boost::format ("Adding peer %1% to excluded peers list with score %2% after %3% seconds bootstrap attempt") % endpoint_frontier_request % score % std::chrono::duration_cast<std::chrono::seconds> (std::chrono::steady_clock::now () - attempt_start).count ()));
			}
			lock_a.unlock ();
			stop ();
			lock_a.lock ();
			// Start new bootstrap connection
			auto node_l (node->shared ());
			node->background ([node_l]() {
				node_l->bootstrap_initiator.bootstrap (true);
			});
		}
		else
		{
			node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::frontier_confirmation_successful, nano::stat::dir::in);
		}
		frontiers_confirmed = confirmed;
	}
}

bool nano::bootstrap_attempt::confirm_frontiers (nano::unique_lock<std::mutex> & lock_a)
{
	bool confirmed (false);
	assert (!frontiers_confirmed);
	// clang-format off
	condition.wait (lock_a, [& stopped = stopped] { return !stopped; });
	// clang-format on
	std::vector<nano::block_hash> frontiers;
	for (auto i (pulls.begin ()), end (pulls.end ()); i != end && frontiers.size () != nano::bootstrap_limits::bootstrap_max_confirm_frontiers; ++i)
	{
		if (!i->head.is_zero () && std::find (frontiers.begin (), frontiers.end (), i->head) == frontiers.end ())
		{
			frontiers.push_back (i->head);
		}
	}
	for (auto i (recent_pulls_head.begin ()), end (recent_pulls_head.end ()); i != end && frontiers.size () != nano::bootstrap_limits::bootstrap_max_confirm_frontiers; ++i)
	{
		if (!i->is_zero () && std::find (frontiers.begin (), frontiers.end (), *i) == frontiers.end ())
		{
			frontiers.push_back (*i);
		}
	}
	lock_a.unlock ();
	auto frontiers_count (frontiers.size ());
	if (frontiers_count > 0)
	{
		const size_t reps_limit = 20;
		auto representatives (node->rep_crawler.representatives ());
		auto reps_weight (node->rep_crawler.total_weight ());
		auto representatives_copy (representatives);
		nano::uint128_t total_weight (0);
		// Select random peers from bottom 50% of principal representatives
		if (representatives.size () > 1)
		{
			std::reverse (representatives.begin (), representatives.end ());
			representatives.resize (representatives.size () / 2);
			for (auto i = static_cast<CryptoPP::word32> (representatives.size () - 1); i > 0; --i)
			{
				auto k = nano::random_pool::generate_word32 (0, i);
				std::swap (representatives[i], representatives[k]);
			}
			if (representatives.size () > reps_limit)
			{
				representatives.resize (reps_limit);
			}
		}
		for (auto const & rep : representatives)
		{
			total_weight += rep.weight.number ();
		}
		// Select peers with total 25% of reps stake from top 50% of principal representatives
		representatives_copy.resize (representatives_copy.size () / 2);
		while (total_weight < reps_weight / 4) // 25%
		{
			auto k = nano::random_pool::generate_word32 (0, static_cast<CryptoPP::word32> (representatives_copy.size () - 1));
			auto rep (representatives_copy[k]);
			if (std::find (representatives.begin (), representatives.end (), rep) == representatives.end ())
			{
				representatives.push_back (rep);
				total_weight += rep.weight.number ();
			}
		}
		// Start requests
		for (auto i (0), max_requests (20); i <= max_requests && !confirmed && !stopped; ++i)
		{
			std::unordered_map<std::shared_ptr<nano::transport::channel>, std::deque<std::pair<nano::block_hash, nano::root>>> batched_confirm_req_bundle;
			std::deque<std::pair<nano::block_hash, nano::root>> request;
			// Find confirmed frontiers (tally > 12.5% of reps stake, 60% of requestsed reps responded
			for (auto ii (frontiers.begin ()); ii != frontiers.end ();)
			{
				if (node->ledger.block_exists (*ii))
				{
					ii = frontiers.erase (ii);
				}
				else
				{
					nano::lock_guard<std::mutex> active_lock (node->active.mutex);
					auto existing (node->active.find_inactive_votes_cache (*ii));
					nano::uint128_t tally;
					for (auto & voter : existing.voters)
					{
						tally += node->ledger.weight (voter);
					}
					if (existing.confirmed || (tally > reps_weight / 8 && existing.voters.size () >= representatives.size () * 0.6)) // 12.5% of weight, 60% of reps
					{
						ii = frontiers.erase (ii);
					}
					else
					{
						for (auto const & rep : representatives)
						{
							if (std::find (existing.voters.begin (), existing.voters.end (), rep.account) == existing.voters.end ())
							{
								release_assert (!ii->is_zero ());
								auto rep_request (batched_confirm_req_bundle.find (rep.channel));
								if (rep_request == batched_confirm_req_bundle.end ())
								{
									std::deque<std::pair<nano::block_hash, nano::root>> insert_root_hash = { std::make_pair (*ii, *ii) };
									batched_confirm_req_bundle.emplace (rep.channel, insert_root_hash);
								}
								else
								{
									rep_request->second.emplace_back (*ii, *ii);
								}
							}
						}
						++ii;
					}
				}
			}
			auto confirmed_count (frontiers_count - frontiers.size ());
			if (confirmed_count >= frontiers_count * nano::bootstrap_limits::required_frontier_confirmation_ratio) // 80% of frontiers confirmed
			{
				confirmed = true;
			}
			else if (i < max_requests)
			{
				node->network.broadcast_confirm_req_batched_many (batched_confirm_req_bundle);
				std::this_thread::sleep_for (std::chrono::milliseconds (!node->network_params.network.is_test_network () ? 500 : 5));
			}
		}
		if (!confirmed)
		{
			node->logger.always_log (boost::str (boost::format ("Failed to confirm frontiers for bootstrap attempt. %1% of %2% frontiers were not confirmed") % frontiers.size () % frontiers_count));
		}
	}
	lock_a.lock ();
	return confirmed;
}

void nano::bootstrap_attempt::lazy_start (nano::hash_or_account const & hash_or_account_a, bool confirmed)
{
	nano::lock_guard<std::mutex> lazy_lock (lazy_mutex);
	// Add start blocks, limit 1024 (4k with disabled legacy bootstrap)
	size_t max_keys (node->flags.disable_legacy_bootstrap ? 4 * 1024 : 1024);
	if (lazy_keys.size () < max_keys && lazy_keys.find (hash_or_account_a) == lazy_keys.end () && lazy_blocks.find (hash_or_account_a) == lazy_blocks.end ())
	{
		lazy_keys.insert (hash_or_account_a);
		lazy_pulls.emplace_back (hash_or_account_a, confirmed ? std::numeric_limits<unsigned>::max () : node->network_params.bootstrap.lazy_retry_limit);
	}
}

void nano::bootstrap_attempt::lazy_add (nano::hash_or_account const & hash_or_account_a, unsigned retry_limit)
{
	// Add only unknown blocks
	assert (!lazy_mutex.try_lock ());
	if (lazy_blocks.find (hash_or_account_a) == lazy_blocks.end ())
	{
		lazy_pulls.emplace_back (hash_or_account_a, retry_limit);
	}
}

void nano::bootstrap_attempt::lazy_requeue (nano::block_hash const & hash_a, nano::block_hash const & previous_a, bool confirmed_a)
{
	nano::unique_lock<std::mutex> lazy_lock (lazy_mutex);
	// Add only known blocks
	auto existing (lazy_blocks.find (hash_a));
	if (existing != lazy_blocks.end ())
	{
		lazy_blocks.erase (existing);
		lazy_lock.unlock ();
		requeue_pull (nano::pull_info (hash_a, hash_a, previous_a, static_cast<nano::pull_info::count_t> (1), confirmed_a ? std::numeric_limits<unsigned>::max () : node->network_params.bootstrap.lazy_destinations_retry_limit));
	}
}

void nano::bootstrap_attempt::lazy_pull_flush ()
{
	assert (!mutex.try_lock ());
	static size_t const max_pulls (nano::bootstrap_limits::bootstrap_connection_scale_target_blocks_lazy * 3);
	if (pulls.size () < max_pulls)
	{
		last_lazy_flush = std::chrono::steady_clock::now ();
		nano::lock_guard<std::mutex> lazy_lock (lazy_mutex);
		assert (node->network_params.bootstrap.lazy_max_pull_blocks <= std::numeric_limits<nano::pull_info::count_t>::max ());
		nano::pull_info::count_t batch_count (node->network_params.bootstrap.lazy_max_pull_blocks);
		if (total_blocks > nano::bootstrap_limits::lazy_batch_pull_count_resize_blocks_limit && !lazy_blocks.empty ())
		{
			double lazy_blocks_ratio (total_blocks / lazy_blocks.size ());
			if (lazy_blocks_ratio > nano::bootstrap_limits::lazy_batch_pull_count_resize_ratio)
			{
				// Increasing blocks ratio weight as more important (^3). Small batch count should lower blocks ratio below target
				double lazy_blocks_factor (std::pow (lazy_blocks_ratio / nano::bootstrap_limits::lazy_batch_pull_count_resize_ratio, 3.0));
				// Decreasing total block count weight as less important (sqrt)
				double total_blocks_factor (std::sqrt (total_blocks / nano::bootstrap_limits::lazy_batch_pull_count_resize_blocks_limit));
				uint32_t batch_count_min (node->network_params.bootstrap.lazy_max_pull_blocks / (lazy_blocks_factor * total_blocks_factor));
				batch_count = std::max (node->network_params.bootstrap.lazy_min_pull_blocks, batch_count_min);
			}
		}
		size_t count (0);
		auto transaction (node->store.tx_begin_read ());
		while (!lazy_pulls.empty () && count < max_pulls)
		{
			auto const & pull_start (lazy_pulls.front ());
			// Recheck if block was already processed
			if (lazy_blocks.find (pull_start.first) == lazy_blocks.end () && !node->store.block_exists (transaction, pull_start.first))
			{
				pulls.emplace_back (pull_start.first, pull_start.first, nano::block_hash (0), batch_count, pull_start.second);
				++count;
			}
			lazy_pulls.pop_front ();
		}
	}
}

bool nano::bootstrap_attempt::lazy_finished ()
{
	if (stopped)
	{
		return true;
	}
	bool result (true);
	auto transaction (node->store.tx_begin_read ());
	nano::lock_guard<std::mutex> lazy_lock (lazy_mutex);
	for (auto it (lazy_keys.begin ()), end (lazy_keys.end ()); it != end && !stopped;)
	{
		if (node->store.block_exists (transaction, *it))
		{
			it = lazy_keys.erase (it);
		}
		else
		{
			result = false;
			break;
			// No need to increment `it` as we break above.
		}
	}
	// Finish lazy bootstrap without lazy pulls (in combination with still_pulling ())
	if (!result && lazy_pulls.empty () && lazy_state_backlog.empty ())
	{
		result = true;
	}
	// Don't close lazy bootstrap until all destinations are processed
	if (result && !lazy_destinations.empty ())
	{
		result = false;
	}
	return result;
}

bool nano::bootstrap_attempt::lazy_has_expired () const
{
	bool result (false);
	// Max 30 minutes run with enabled legacy bootstrap
	static std::chrono::minutes const max_lazy_time (node->flags.disable_legacy_bootstrap ? 7 * 24 * 60 : 30);
	if (std::chrono::steady_clock::now () - lazy_start_time >= max_lazy_time)
	{
		result = true;
	}
	else if (!node->flags.disable_legacy_bootstrap && lazy_blocks_count > nano::bootstrap_limits::lazy_blocks_restart_limit)
	{
		result = true;
	}
	return result;
}

void nano::bootstrap_attempt::lazy_clear ()
{
	assert (!lazy_mutex.try_lock ());
	lazy_blocks.clear ();
	lazy_blocks_count = 0;
	lazy_keys.clear ();
	lazy_pulls.clear ();
	lazy_state_backlog.clear ();
	lazy_balances.clear ();
	lazy_destinations.clear ();
}

void nano::bootstrap_attempt::lazy_run ()
{
	assert (!node->flags.disable_lazy_bootstrap);
	start_populate_connections ();
	lazy_start_time = std::chrono::steady_clock::now ();
	nano::unique_lock<std::mutex> lock (mutex);
	while ((still_pulling () || !lazy_finished ()) && !lazy_has_expired ())
	{
		unsigned iterations (0);
		while (still_pulling () && !lazy_has_expired ())
		{
			if (!pulls.empty ())
			{
				request_pull (lock);
			}
			else
			{
				lazy_pull_flush ();
				if (pulls.empty ())
				{
					condition.wait_for (lock, std::chrono::seconds (1));
				}
			}
			++iterations;
			// Flushing lazy pulls
			if (iterations % 100 == 0 || last_lazy_flush + nano::bootstrap_limits::lazy_flush_delay_sec < std::chrono::steady_clock::now ())
			{
				lazy_pull_flush ();
			}
			// Start backlog cleanup
			if (iterations % 200 == 0)
			{
				lazy_backlog_cleanup ();
			}
			// Destinations check
			if (pulls.empty () && lazy_destinations_flushed)
			{
				lazy_destinations_flush ();
			}
		}
		// Flushing lazy pulls
		lazy_pull_flush ();
		// Check if some blocks required for backlog were processed. Start destinations check
		if (pulls.empty ())
		{
			lazy_backlog_cleanup ();
			lazy_destinations_flush ();
		}
	}
	if (!stopped)
	{
		node->logger.try_log ("Completed lazy pulls");
		nano::unique_lock<std::mutex> lazy_lock (lazy_mutex);
		++runs_count;
		// Start wallet lazy bootstrap if required
		if (!wallet_accounts.empty () && !node->flags.disable_wallet_bootstrap)
		{
			pulls.clear ();
			lazy_clear ();
			mode = nano::bootstrap_mode::wallet_lazy;
			lock.unlock ();
			lazy_lock.unlock ();
			wallet_run ();
			lock.lock ();
		}
		// Fallback to legacy bootstrap
		else if (runs_count < 3 && !lazy_keys.empty () && !node->flags.disable_legacy_bootstrap)
		{
			pulls.clear ();
			lazy_clear ();
			mode = nano::bootstrap_mode::legacy;
			lock.unlock ();
			lazy_lock.unlock ();
			run ();
			lock.lock ();
		}
	}
	stopped = true;
	condition.notify_all ();
	idle.clear ();
}

bool nano::bootstrap_attempt::process_block (std::shared_ptr<nano::block> block_a, nano::account const & known_account_a, uint64_t pull_blocks, nano::bulk_pull::count_t max_blocks, bool block_expected, unsigned retry_limit)
{
	bool stop_pull (false);
	if (mode != nano::bootstrap_mode::legacy && block_expected)
	{
		stop_pull = process_block_lazy (block_a, known_account_a, pull_blocks, max_blocks, retry_limit);
	}
	else if (mode != nano::bootstrap_mode::legacy)
	{
		// Drop connection with unexpected block for lazy bootstrap
		stop_pull = true;
	}
	else
	{
		nano::unchecked_info info (block_a, known_account_a, 0, nano::signature_verification::unknown);
		node->block_processor.add (info);
	}
	return stop_pull;
}

bool nano::bootstrap_attempt::process_block_lazy (std::shared_ptr<nano::block> block_a, nano::account const & known_account_a, uint64_t pull_blocks, nano::bulk_pull::count_t max_blocks, unsigned retry_limit)
{
	bool stop_pull (false);
	auto hash (block_a->hash ());
	nano::unique_lock<std::mutex> lazy_lock (lazy_mutex);
	// Processing new blocks
	if (lazy_blocks.find (hash) == lazy_blocks.end ())
	{
		// Search for new dependencies
		if (!block_a->source ().is_zero () && !node->ledger.block_exists (block_a->source ()) && block_a->source () != node->network_params.ledger.genesis_account)
		{
			lazy_add (block_a->source (), retry_limit);
		}
		else if (block_a->type () == nano::block_type::state)
		{
			lazy_block_state (block_a, retry_limit);
		}
		else if (block_a->type () == nano::block_type::send)
		{
			std::shared_ptr<nano::send_block> block_l (std::static_pointer_cast<nano::send_block> (block_a));
			if (block_l != nullptr && !block_l->hashables.destination.is_zero ())
			{
				lazy_destinations_increment (block_l->hashables.destination);
			}
		}
		lazy_blocks.insert (hash);
		++lazy_blocks_count;
		// Adding lazy balances for first processed block in pull
		if (pull_blocks == 0 && (block_a->type () == nano::block_type::state || block_a->type () == nano::block_type::send))
		{
			lazy_balances.emplace (hash, block_a->balance ().number ());
		}
		// Clearing lazy balances for previous block
		if (!block_a->previous ().is_zero () && lazy_balances.find (block_a->previous ()) != lazy_balances.end ())
		{
			lazy_balances.erase (block_a->previous ());
		}
		lazy_block_state_backlog_check (block_a, hash);
		lazy_lock.unlock ();
		nano::unchecked_info info (block_a, known_account_a, 0, nano::signature_verification::unknown, retry_limit == std::numeric_limits<unsigned>::max ());
		node->block_processor.add (info);
	}
	// Force drop lazy bootstrap connection for long bulk_pull
	if (pull_blocks > max_blocks)
	{
		stop_pull = true;
	}
	return stop_pull;
}

void nano::bootstrap_attempt::lazy_block_state (std::shared_ptr<nano::block> block_a, unsigned retry_limit)
{
	std::shared_ptr<nano::state_block> block_l (std::static_pointer_cast<nano::state_block> (block_a));
	if (block_l != nullptr)
	{
		auto transaction (node->store.tx_begin_read ());
		nano::uint128_t balance (block_l->hashables.balance.number ());
		auto const & link (block_l->hashables.link);
		// If link is not epoch link or 0. And if block from link is unknown
		if (!link.is_zero () && !node->ledger.is_epoch_link (link) && lazy_blocks.find (link) == lazy_blocks.end () && !node->store.block_exists (transaction, link))
		{
			auto const & previous (block_l->hashables.previous);
			// If state block previous is 0 then source block required
			if (previous.is_zero ())
			{
				lazy_add (link, retry_limit);
			}
			// In other cases previous block balance required to find out subtype of state block
			else if (node->store.block_exists (transaction, previous))
			{
				if (node->ledger.balance (transaction, previous) <= balance)
				{
					lazy_add (link, retry_limit);
				}
				else
				{
					lazy_destinations_increment (link);
				}
			}
			// Search balance of already processed previous blocks
			else if (lazy_blocks.find (previous) != lazy_blocks.end ())
			{
				auto previous_balance (lazy_balances.find (previous));
				if (previous_balance != lazy_balances.end ())
				{
					if (previous_balance->second <= balance)
					{
						lazy_add (link, retry_limit);
					}
					else
					{
						lazy_destinations_increment (link);
					}
					lazy_balances.erase (previous_balance);
				}
			}
			// Insert in backlog state blocks if previous wasn't already processed
			else
			{
				lazy_state_backlog.emplace (previous, nano::lazy_state_backlog_item{ link, balance, retry_limit });
			}
		}
	}
}

void nano::bootstrap_attempt::lazy_block_state_backlog_check (std::shared_ptr<nano::block> block_a, nano::block_hash const & hash_a)
{
	// Search unknown state blocks balances
	auto find_state (lazy_state_backlog.find (hash_a));
	if (find_state != lazy_state_backlog.end ())
	{
		auto next_block (find_state->second);
		// Retrieve balance for previous state & send blocks
		if (block_a->type () == nano::block_type::state || block_a->type () == nano::block_type::send)
		{
			if (block_a->balance ().number () <= next_block.balance) // balance
			{
				lazy_add (next_block.link, next_block.retry_limit); // link
			}
			else
			{
				lazy_destinations_increment (next_block.link);
			}
		}
		// Assumption for other legacy block types
		else if (lazy_undefined_links.find (next_block.link) == lazy_undefined_links.end ())
		{
			lazy_add (next_block.link, node->network_params.bootstrap.lazy_retry_limit); // Head is not confirmed. It can be account or hash or non-existing
			lazy_undefined_links.insert (next_block.link);
		}
		lazy_state_backlog.erase (find_state);
	}
}

void nano::bootstrap_attempt::lazy_backlog_cleanup ()
{
	auto transaction (node->store.tx_begin_read ());
	nano::lock_guard<std::mutex> lazy_lock (lazy_mutex);
	for (auto it (lazy_state_backlog.begin ()), end (lazy_state_backlog.end ()); it != end && !stopped;)
	{
		if (node->store.block_exists (transaction, it->first))
		{
			auto next_block (it->second);
			if (node->ledger.balance (transaction, it->first) <= next_block.balance) // balance
			{
				lazy_add (next_block.link, next_block.retry_limit); // link
			}
			else
			{
				lazy_destinations_increment (next_block.link);
			}
			it = lazy_state_backlog.erase (it);
		}
		else
		{
			lazy_add (it->first, it->second.retry_limit);
			++it;
		}
	}
}

void nano::bootstrap_attempt::lazy_destinations_increment (nano::account const & destination_a)
{
	// Enabled only if legacy bootstrap is not available. Legacy bootstrap is a more effective way to receive all existing destinations
	if (node->flags.disable_legacy_bootstrap)
	{
		// Update accounts counter for send blocks
		auto existing (lazy_destinations.get<account_tag> ().find (destination_a));
		if (existing != lazy_destinations.get<account_tag> ().end ())
		{
			lazy_destinations.get<account_tag> ().modify (existing, [](nano::lazy_destinations_item & item_a) {
				++item_a.count;
			});
		}
		else
		{
			lazy_destinations.insert (nano::lazy_destinations_item{ destination_a, 1 });
		}
	}
}

void nano::bootstrap_attempt::lazy_destinations_flush ()
{
	lazy_destinations_flushed = true;
	size_t count (0);
	nano::lock_guard<std::mutex> lazy_lock (lazy_mutex);
	for (auto it (lazy_destinations.get<count_tag> ().begin ()), end (lazy_destinations.get<count_tag> ().end ()); it != end && count < nano::bootstrap_limits::lazy_destinations_request_limit && !stopped;)
	{
		lazy_add (it->account, node->network_params.bootstrap.lazy_destinations_retry_limit);
		it = lazy_destinations.get<count_tag> ().erase (it);
		++count;
	}
}

bool nano::bootstrap_attempt::lazy_processed_or_exists (nano::block_hash const & hash_a)
{
	bool result (false);
	nano::unique_lock<std::mutex> lazy_lock (lazy_mutex);
	if (lazy_blocks.find (hash_a) != lazy_blocks.end ())
	{
		result = true;
	}
	else
	{
		lazy_lock.unlock ();
		if (node->ledger.block_exists (hash_a))
		{
			result = true;
		}
	}
	return result;
}

void nano::bootstrap_attempt::request_pending (nano::unique_lock<std::mutex> & lock_a)
{
	auto connection_l (connection (lock_a));
	if (connection_l)
	{
		auto account (wallet_accounts.front ());
		wallet_accounts.pop_front ();
		++pulling;
		// The bulk_pull_account_client destructor attempt to requeue_pull which can cause a deadlock if this is the last reference
		// Dispatch request in an external thread in case it needs to be destroyed
		node->background ([connection_l, account]() {
			auto client (std::make_shared<nano::bulk_pull_account_client> (connection_l, account));
			client->request ();
		});
	}
}

void nano::bootstrap_attempt::requeue_pending (nano::account const & account_a)
{
	auto account (account_a);
	{
		nano::lock_guard<std::mutex> lock (mutex);
		wallet_accounts.push_front (account);
		condition.notify_all ();
	}
}

void nano::bootstrap_attempt::wallet_start (std::deque<nano::account> & accounts_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	wallet_accounts.swap (accounts_a);
}

bool nano::bootstrap_attempt::wallet_finished ()
{
	assert (!mutex.try_lock ());
	auto running (!stopped);
	auto more_accounts (!wallet_accounts.empty ());
	auto still_pulling (pulling > 0);
	return running && (more_accounts || still_pulling);
}

void nano::bootstrap_attempt::wallet_run ()
{
	assert (!node->flags.disable_wallet_bootstrap);
	start_populate_connections ();
	auto start_time (std::chrono::steady_clock::now ());
	auto max_time (std::chrono::minutes (10));
	nano::unique_lock<std::mutex> lock (mutex);
	while (wallet_finished () && std::chrono::steady_clock::now () - start_time < max_time)
	{
		if (!wallet_accounts.empty ())
		{
			request_pending (lock);
		}
		else
		{
			condition.wait (lock);
		}
	}
	if (!stopped)
	{
		node->logger.try_log ("Completed wallet lazy pulls");
		++runs_count;
		// Start lazy bootstrap if some lazy keys were inserted
		if (!lazy_finished ())
		{
			lock.unlock ();
			total_blocks = 0;
			lazy_run ();
			lock.lock ();
		}
	}
	stopped = true;
	condition.notify_all ();
	idle.clear ();
}

nano::bootstrap_initiator::bootstrap_initiator (nano::node & node_a)
	: node (node_a)
	, stopped (false)
	, thread ([this]() {
		nano::thread_role::set (nano::thread_role::name::bootstrap_initiator);
		run_bootstrap ();
	})
{
}

nano::bootstrap_initiator::~bootstrap_initiator ()
{
	stop ();
}

void nano::bootstrap_initiator::bootstrap (bool force)
{
	nano::unique_lock<std::mutex> lock (mutex);
	if (force && attempt != nullptr)
	{
		attempt->stop ();
		// clang-format off
		condition.wait (lock, [&attempt = attempt, &stopped = stopped] { return stopped || attempt == nullptr; });
		// clang-format on
	}
	if (!stopped && attempt == nullptr)
	{
		node.stats.inc (nano::stat::type::bootstrap, nano::stat::detail::initiate, nano::stat::dir::out);
		attempt = std::make_shared<nano::bootstrap_attempt> (node.shared ());
		condition.notify_all ();
	}
}

void nano::bootstrap_initiator::bootstrap (nano::endpoint const & endpoint_a, bool add_to_peers, bool frontiers_confirmed)
{
	if (add_to_peers)
	{
		node.network.udp_channels.insert (nano::transport::map_endpoint_to_v6 (endpoint_a), node.network_params.protocol.protocol_version);
	}
	nano::unique_lock<std::mutex> lock (mutex);
	if (!stopped)
	{
		if (attempt != nullptr)
		{
			attempt->stop ();
			// clang-format off
			condition.wait (lock, [&attempt = attempt, &stopped = stopped] { return stopped || attempt == nullptr; });
			// clang-format on
		}
		node.stats.inc (nano::stat::type::bootstrap, nano::stat::detail::initiate, nano::stat::dir::out);
		attempt = std::make_shared<nano::bootstrap_attempt> (node.shared ());
		if (frontiers_confirmed)
		{
			excluded_peers.remove (nano::transport::map_endpoint_to_tcp (endpoint_a));
		}
		if (!excluded_peers.check (nano::transport::map_endpoint_to_tcp (endpoint_a)))
		{
			attempt->add_connection (endpoint_a);
		}
		attempt->frontiers_confirmed = frontiers_confirmed;
		condition.notify_all ();
	}
}

void nano::bootstrap_initiator::bootstrap_lazy (nano::hash_or_account const & hash_or_account_a, bool force, bool confirmed)
{
	{
		nano::unique_lock<std::mutex> lock (mutex);
		if (force && attempt != nullptr)
		{
			attempt->stop ();
			// clang-format off
			condition.wait (lock, [&attempt = attempt, &stopped = stopped] { return stopped || attempt == nullptr; });
			// clang-format on
		}
		node.stats.inc (nano::stat::type::bootstrap, nano::stat::detail::initiate_lazy, nano::stat::dir::out);
		if (attempt == nullptr)
		{
			attempt = std::make_shared<nano::bootstrap_attempt> (node.shared (), nano::bootstrap_mode::lazy);
		}
		attempt->lazy_start (hash_or_account_a, confirmed);
	}
	condition.notify_all ();
}

void nano::bootstrap_initiator::bootstrap_wallet (std::deque<nano::account> & accounts_a)
{
	{
		nano::unique_lock<std::mutex> lock (mutex);
		node.stats.inc (nano::stat::type::bootstrap, nano::stat::detail::initiate_wallet_lazy, nano::stat::dir::out);
		if (attempt == nullptr)
		{
			attempt = std::make_shared<nano::bootstrap_attempt> (node.shared (), nano::bootstrap_mode::wallet_lazy);
		}
		attempt->wallet_start (accounts_a);
	}
	condition.notify_all ();
}

void nano::bootstrap_initiator::run_bootstrap ()
{
	nano::unique_lock<std::mutex> lock (mutex);
	while (!stopped)
	{
		if (attempt != nullptr)
		{
			lock.unlock ();
			if (attempt->mode == nano::bootstrap_mode::legacy)
			{
				attempt->run ();
			}
			else if (attempt->mode == nano::bootstrap_mode::lazy)
			{
				attempt->lazy_run ();
			}
			else
			{
				attempt->wallet_run ();
			}
			lock.lock ();
			attempt = nullptr;
			condition.notify_all ();
		}
		else
		{
			condition.wait (lock);
		}
	}
}

void nano::bootstrap_initiator::add_observer (std::function<void(bool)> const & observer_a)
{
	nano::lock_guard<std::mutex> lock (observers_mutex);
	observers.push_back (observer_a);
}

bool nano::bootstrap_initiator::in_progress ()
{
	return current_attempt () != nullptr;
}

std::shared_ptr<nano::bootstrap_attempt> nano::bootstrap_initiator::current_attempt ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	return attempt;
}

void nano::bootstrap_initiator::stop ()
{
	if (!stopped.exchange (true))
	{
		{
			nano::lock_guard<std::mutex> guard (mutex);
			if (attempt != nullptr)
			{
				attempt->stop ();
			}
		}
		condition.notify_all ();

		if (thread.joinable ())
		{
			thread.join ();
		}
	}
}

void nano::bootstrap_initiator::notify_listeners (bool in_progress_a)
{
	nano::lock_guard<std::mutex> lock (observers_mutex);
	for (auto & i : observers)
	{
		i (in_progress_a);
	}
}

namespace nano
{
std::unique_ptr<seq_con_info_component> collect_seq_con_info (bootstrap_initiator & bootstrap_initiator, const std::string & name)
{
	size_t count = 0;
	size_t cache_count = 0;
	size_t excluded_peers_count = 0;
	{
		nano::lock_guard<std::mutex> guard (bootstrap_initiator.observers_mutex);
		count = bootstrap_initiator.observers.size ();
	}
	{
		nano::lock_guard<std::mutex> guard (bootstrap_initiator.cache.pulls_cache_mutex);
		cache_count = bootstrap_initiator.cache.cache.size ();
	}
	{
		nano::lock_guard<std::mutex> guard (bootstrap_initiator.excluded_peers.excluded_peers_mutex);
		excluded_peers_count = bootstrap_initiator.excluded_peers.peers.size ();
	}

	auto sizeof_element = sizeof (decltype (bootstrap_initiator.observers)::value_type);
	auto sizeof_cache_element = sizeof (decltype (bootstrap_initiator.cache.cache)::value_type);
	auto sizeof_excluded_peers_element = sizeof (decltype (bootstrap_initiator.excluded_peers.peers)::value_type);
	auto composite = std::make_unique<seq_con_info_composite> (name);
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "observers", count, sizeof_element }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "pulls_cache", cache_count, sizeof_cache_element }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "excluded_peers", excluded_peers_count, sizeof_excluded_peers_element }));
	return composite;
}
}

void nano::pulls_cache::add (nano::pull_info const & pull_a)
{
	if (pull_a.processed > 500)
	{
		nano::lock_guard<std::mutex> guard (pulls_cache_mutex);
		// Clean old pull
		if (cache.size () > cache_size_max)
		{
			cache.erase (cache.begin ());
		}
		assert (cache.size () <= cache_size_max);
		nano::uint512_union head_512 (pull_a.account_or_head, pull_a.head_original);
		auto existing (cache.get<account_head_tag> ().find (head_512));
		if (existing == cache.get<account_head_tag> ().end ())
		{
			// Insert new pull
			auto inserted (cache.insert (nano::cached_pulls{ std::chrono::steady_clock::now (), head_512, pull_a.head }));
			(void)inserted;
			assert (inserted.second);
		}
		else
		{
			// Update existing pull
			cache.get<account_head_tag> ().modify (existing, [pull_a](nano::cached_pulls & cache_a) {
				cache_a.time = std::chrono::steady_clock::now ();
				cache_a.new_head = pull_a.head;
			});
		}
	}
}

void nano::pulls_cache::update_pull (nano::pull_info & pull_a)
{
	nano::lock_guard<std::mutex> guard (pulls_cache_mutex);
	nano::uint512_union head_512 (pull_a.account_or_head, pull_a.head_original);
	auto existing (cache.get<account_head_tag> ().find (head_512));
	if (existing != cache.get<account_head_tag> ().end ())
	{
		pull_a.head = existing->new_head;
	}
}

void nano::pulls_cache::remove (nano::pull_info const & pull_a)
{
	nano::lock_guard<std::mutex> guard (pulls_cache_mutex);
	nano::uint512_union head_512 (pull_a.account_or_head, pull_a.head_original);
	cache.get<account_head_tag> ().erase (head_512);
}

uint64_t nano::bootstrap_excluded_peers::add (nano::tcp_endpoint const & endpoint_a, size_t network_peers_count)
{
	uint64_t result (0);
	nano::lock_guard<std::mutex> guard (excluded_peers_mutex);
	// Clean old excluded peers
	while (peers.size () > 1 && peers.size () > std::min (static_cast<double> (excluded_peers_size_max), network_peers_count * excluded_peers_percentage_limit))
	{
		peers.erase (peers.begin ());
	}
	assert (peers.size () <= excluded_peers_size_max);
	auto existing (peers.get<endpoint_tag> ().find (endpoint_a));
	if (existing == peers.get<endpoint_tag> ().end ())
	{
		// Insert new endpoint
		auto inserted (peers.insert (nano::excluded_peers_item{ std::chrono::steady_clock::steady_clock::now () + exclude_time_hours, endpoint_a, 1 }));
		(void)inserted;
		assert (inserted.second);
		result = 1;
	}
	else
	{
		// Update existing endpoint
		peers.get<endpoint_tag> ().modify (existing, [&result](nano::excluded_peers_item & item_a) {
			++item_a.score;
			result = item_a.score;
			if (item_a.score == nano::bootstrap_excluded_peers::score_limit)
			{
				item_a.exclude_until = std::chrono::steady_clock::now () + nano::bootstrap_excluded_peers::exclude_time_hours;
			}
			else if (item_a.score > nano::bootstrap_excluded_peers::score_limit)
			{
				item_a.exclude_until = std::chrono::steady_clock::now () + nano::bootstrap_excluded_peers::exclude_time_hours * item_a.score * 2;
			}
		});
	}
	return result;
}

bool nano::bootstrap_excluded_peers::check (nano::tcp_endpoint const & endpoint_a)
{
	bool excluded (false);
	nano::lock_guard<std::mutex> guard (excluded_peers_mutex);
	auto existing (peers.get<endpoint_tag> ().find (endpoint_a));
	if (existing != peers.get<endpoint_tag> ().end () && existing->score >= score_limit)
	{
		if (existing->exclude_until > std::chrono::steady_clock::now ())
		{
			excluded = true;
		}
		else if (existing->exclude_until + exclude_remove_hours * existing->score < std::chrono::steady_clock::now ())
		{
			peers.get<endpoint_tag> ().erase (existing);
		}
	}
	return excluded;
}

void nano::bootstrap_excluded_peers::remove (nano::tcp_endpoint const & endpoint_a)
{
	nano::lock_guard<std::mutex> guard (excluded_peers_mutex);
	peers.get<endpoint_tag> ().erase (endpoint_a);
}
