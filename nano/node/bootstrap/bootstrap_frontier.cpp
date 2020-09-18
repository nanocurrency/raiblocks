#include <nano/node/bootstrap/bootstrap_attempt.hpp>
#include <nano/node/bootstrap/bootstrap_frontier.hpp>
#include <nano/node/node.hpp>
#include <nano/node/transport/tcp.hpp>

#include <boost/format.hpp>

constexpr double nano::bootstrap_limits::bootstrap_connection_warmup_time_sec;
constexpr double nano::bootstrap_limits::bootstrap_minimum_elapsed_seconds_blockrate;
constexpr double nano::bootstrap_limits::bootstrap_minimum_frontier_blocks_per_sec;
constexpr unsigned nano::bootstrap_limits::bulk_push_cost_limit;

constexpr size_t nano::frontier_req_client::size_frontier;

void nano::frontier_req_client::run ()
{
	nano::frontier_req request;
	request.start.clear ();
	request.age = std::numeric_limits<decltype (request.age)>::max ();
	request.count = std::numeric_limits<decltype (request.count)>::max ();
	auto this_l (shared_from_this ());
	connection->channel->send (
	request, [this_l](boost::system::error_code const & ec, size_t size_a) {
		if (!ec)
		{
			this_l->connection->node->spawn (
			[this_l](boost::asio::yield_context yield) {
				this_l->receive_frontiers (yield);
			});
		}
		else
		{
			if (this_l->connection->node->config.logging.network_logging ())
			{
				this_l->connection->node->logger.try_log (boost::str (boost::format ("Error while sending bootstrap request %1%") % ec.message ()));
			}
		}
	},
	nano::buffer_drop_policy::no_limiter_drop);
}

nano::frontier_req_client::frontier_req_client (std::shared_ptr<nano::bootstrap_client> connection_a, std::shared_ptr<nano::bootstrap_attempt> attempt_a) :
connection (connection_a),
attempt (attempt_a),
current (0),
count (0),
bulk_push_cost (0)
{
	next ();
}

nano::frontier_req_client::~frontier_req_client ()
{
}

void nano::frontier_req_client::receive_frontiers (boost::asio::yield_context yield)
{
	start_time = std::chrono::steady_clock::now ();
	nano::account account{ 0 };
	do
	{
		boost::system::error_code ec;
		auto read = connection->socket->async_read (connection->receive_buffer, nano::frontier_req_client::size_frontier, yield[ec]);
		// An issue with asio is that sometimes, instead of reporting a bad file descriptor during disconnect,
		// we simply get a size of 0.
		if (!ec && read == nano::frontier_req_client::size_frontier)
		{
			nano::block_hash latest;
			nano::bufferstream stream{ connection->receive_buffer->data (), nano::frontier_req_client::size_frontier };
			auto error = nano::try_read (stream, account) || nano::try_read (stream, latest);
			(void)error;
			debug_assert (!error);
			++count;
			std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>> (std::chrono::steady_clock::now () - start_time);

			double elapsed_sec = std::max (time_span.count (), nano::bootstrap_limits::bootstrap_minimum_elapsed_seconds_blockrate);
			double blocks_per_sec = static_cast<double> (count) / elapsed_sec;
			if (elapsed_sec > nano::bootstrap_limits::bootstrap_connection_warmup_time_sec && blocks_per_sec < nano::bootstrap_limits::bootstrap_minimum_frontier_blocks_per_sec)
			{
				connection->node->logger.try_log (boost::str (boost::format ("Aborting frontier req because it was too slow")));
				promise.set_value (true);
				return;
			}
			if (attempt->should_log ())
			{
				connection->node->logger.always_log (boost::str (boost::format ("Received %1% frontiers from %2%") % std::to_string (count) % connection->channel->to_string ()));
			}
			if (!account.is_zero ())
			{
				while (!current.is_zero () && current < account)
				{
					// We know about an account they don't.
					unsynced (frontier, 0);
					next ();
				}
				if (!current.is_zero ())
				{
					if (account == current)
					{
						if (latest == frontier)
						{
							// In sync
						}
						else
						{
							if (connection->node->ledger.block_exists (latest))
							{
								// We know about a block they don't.
								unsynced (frontier, latest);
							}
							else
							{
								attempt->add_frontier (nano::pull_info (account, latest, frontier, attempt->incremental_id, 0, connection->node->network_params.bootstrap.frontier_retry_limit));
								// Either we're behind or there's a fork we differ on
								// Either way, bulk pushing will probably not be effective
								bulk_push_cost += 5;
							}
						}
						next ();
					}
					else
					{
						debug_assert (account < current);
						attempt->add_frontier (nano::pull_info (account, latest, nano::block_hash (0), attempt->incremental_id, 0, connection->node->network_params.bootstrap.frontier_retry_limit));
					}
				}
				else
				{
					attempt->add_frontier (nano::pull_info (account, latest, nano::block_hash (0), attempt->incremental_id, 0, connection->node->network_params.bootstrap.frontier_retry_limit));
				}
			}
			else
			{
				while (!current.is_zero ())
				{
					// We know about an account they don't.
					unsynced (frontier, 0);
					next ();
				}
				if (connection->node->config.logging.bulk_pull_logging ())
				{
					connection->node->logger.try_log ("Bulk push cost: ", bulk_push_cost);
				}
				{
					try
					{
						promise.set_value (false);
					}
					catch (std::future_error &)
					{
					}
					connection->connections->pool_connection (connection);
				}
			}
		}
		else if (ec)
		{
			if (connection->node->config.logging.network_logging ())
			{
				connection->node->logger.try_log (boost::str (boost::format ("Error while receiving frontier %1%") % ec.message ()));
			}
		}
		else
		{
			if (connection->node->config.logging.network_message_logging ())
			{
				connection->node->logger.try_log (boost::str (boost::format ("Invalid size: expected %1%, got %2%") % nano::frontier_req_client::size_frontier % read));
			}
		}
	} while (!account.is_zero ());
}

void nano::frontier_req_client::unsynced (nano::block_hash const & head, nano::block_hash const & end)
{
	if (bulk_push_cost < nano::bootstrap_limits::bulk_push_cost_limit)
	{
		attempt->add_bulk_push_target (head, end);
		if (end.is_zero ())
		{
			bulk_push_cost += 2;
		}
		else
		{
			bulk_push_cost += 1;
		}
	}
}

void nano::frontier_req_client::next ()
{
	// Filling accounts deque to prevent often read transactions
	if (accounts.empty ())
	{
		size_t max_size (128);
		auto transaction (connection->node->store.tx_begin_read ());
		for (auto i (connection->node->store.latest_begin (transaction, current.number () + 1)), n (connection->node->store.latest_end ()); i != n && accounts.size () != max_size; ++i)
		{
			nano::account_info const & info (i->second);
			nano::account const & account (i->first);
			accounts.emplace_back (account, info.head);
		}
		/* If loop breaks before max_size, then latest_end () is reached
		Add empty record to finish frontier_req_server */
		if (accounts.size () != max_size)
		{
			accounts.emplace_back (nano::account (0), nano::block_hash (0));
		}
	}
	// Retrieving accounts from deque
	auto const & account_pair (accounts.front ());
	current = account_pair.first;
	frontier = account_pair.second;
	accounts.pop_front ();
}

nano::frontier_req_server::frontier_req_server (std::shared_ptr<nano::bootstrap_server> const & connection_a, std::unique_ptr<nano::frontier_req> request_a) :
connection (connection_a),
current (request_a->start.number () - 1),
frontier (0),
request (std::move (request_a)),
count (0)
{
	next ();
}

void nano::frontier_req_server::send_next ()
{
	if (!current.is_zero () && count < request->count)
	{
		std::vector<uint8_t> send_buffer;
		{
			nano::vectorstream stream (send_buffer);
			write (stream, current.bytes);
			write (stream, frontier.bytes);
		}
		auto this_l (shared_from_this ());
		if (connection->node->config.logging.bulk_pull_logging ())
		{
			connection->node->logger.try_log (boost::str (boost::format ("Sending frontier for %1% %2%") % current.to_account () % frontier.to_string ()));
		}
		next ();
		connection->socket->async_write (nano::shared_const_buffer (std::move (send_buffer)), [this_l](boost::system::error_code const & ec, size_t size_a) {
			this_l->sent_action (ec, size_a);
		});
	}
	else
	{
		send_finished ();
	}
}

void nano::frontier_req_server::send_finished ()
{
	std::vector<uint8_t> send_buffer;
	{
		nano::vectorstream stream (send_buffer);
		nano::uint256_union zero (0);
		write (stream, zero.bytes);
		write (stream, zero.bytes);
	}
	auto this_l (shared_from_this ());
	if (connection->node->config.logging.network_logging ())
	{
		connection->node->logger.try_log ("Frontier sending finished");
	}
	connection->socket->async_write (nano::shared_const_buffer (std::move (send_buffer)), [this_l](boost::system::error_code const & ec, size_t size_a) {
		this_l->no_block_sent (ec, size_a);
	});
}

void nano::frontier_req_server::no_block_sent (boost::system::error_code const & ec, size_t size_a)
{
	if (!ec)
	{
		connection->finish_request ();
	}
	else
	{
		if (connection->node->config.logging.network_logging ())
		{
			connection->node->logger.try_log (boost::str (boost::format ("Error sending frontier finish: %1%") % ec.message ()));
		}
	}
}

void nano::frontier_req_server::sent_action (boost::system::error_code const & ec, size_t size_a)
{
	if (!ec)
	{
		count++;
		send_next ();
	}
	else
	{
		if (connection->node->config.logging.network_logging ())
		{
			connection->node->logger.try_log (boost::str (boost::format ("Error sending frontier pair: %1%") % ec.message ()));
		}
	}
}

void nano::frontier_req_server::next ()
{
	// Filling accounts deque to prevent often read transactions
	if (accounts.empty ())
	{
		auto now (nano::seconds_since_epoch ());
		bool skip_old (request->age != std::numeric_limits<decltype (request->age)>::max ());
		size_t max_size (128);
		auto transaction (connection->node->store.tx_begin_read ());
		for (auto i (connection->node->store.latest_begin (transaction, current.number () + 1)), n (connection->node->store.latest_end ()); i != n && accounts.size () != max_size; ++i)
		{
			nano::account_info const & info (i->second);
			if (!skip_old || (now - info.modified) <= request->age)
			{
				nano::account const & account (i->first);
				accounts.emplace_back (account, info.head);
			}
		}
		/* If loop breaks before max_size, then latest_end () is reached
		Add empty record to finish frontier_req_server */
		if (accounts.size () != max_size)
		{
			accounts.emplace_back (nano::account (0), nano::block_hash (0));
		}
	}
	// Retrieving accounts from deque
	auto const & account_pair (accounts.front ());
	current = account_pair.first;
	frontier = account_pair.second;
	accounts.pop_front ();
}
