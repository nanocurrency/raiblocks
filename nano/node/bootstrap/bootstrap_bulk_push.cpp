#include <nano/node/bootstrap/bootstrap.hpp>
#include <nano/node/bootstrap/bootstrap_bulk_push.hpp>
#include <nano/node/node.hpp>
#include <nano/node/transport/tcp.hpp>

#include <boost/format.hpp>

nano::bulk_push_client::bulk_push_client (std::shared_ptr<nano::bootstrap_client> const & connection_a)
	: connection (connection_a)
{
}

nano::bulk_push_client::~bulk_push_client ()
{
}

void nano::bulk_push_client::start ()
{
	nano::bulk_push message;
	auto this_l (shared_from_this ());
	connection->channel->send (
		message, [this_l](boost::system::error_code const & ec, size_t size_a) {
			auto transaction (this_l->connection->node->store.tx_begin_read ());
			if (!ec)
			{
				this_l->push (transaction);
			}
			else
			{
				if (this_l->connection->node->config.logging.bulk_pull_logging ())
				{
					this_l->connection->node->logger.try_log (boost::str (boost::format ("Unable to send bulk_push request: %1%") % ec.message ()));
				}
			}
		},
		false); // is bootstrap traffic is_droppable false
}

void nano::bulk_push_client::push (nano::transaction const & transaction_a)
{
	std::shared_ptr<nano::block> block;
	bool finished (false);
	while (block == nullptr && !finished)
	{
		if (current_target.first.is_zero () || current_target.first == current_target.second)
		{
			nano::lock_guard<std::mutex> guard (connection->attempt->mutex);
			if (!connection->attempt->bulk_push_targets.empty ())
			{
				current_target = connection->attempt->bulk_push_targets.back ();
				connection->attempt->bulk_push_targets.pop_back ();
			}
			else
			{
				finished = true;
			}
		}
		if (!finished)
		{
			block = connection->node->store.block_get (transaction_a, current_target.first);
			if (block == nullptr)
			{
				current_target.first = nano::block_hash (0);
			}
			else
			{
				if (connection->node->config.logging.bulk_pull_logging ())
				{
					connection->node->logger.try_log ("Bulk pushing range ", current_target.first.to_string (), " down to ", current_target.second.to_string ());
				}
			}
		}
	}
	if (finished)
	{
		send_finished ();
	}
	else
	{
		current_target.first = block->previous ();
		push_block (*block);
	}
}

void nano::bulk_push_client::send_finished ()
{
	nano::shared_const_buffer buffer (static_cast<uint8_t> (nano::block_type::not_a_block));
	auto this_l (shared_from_this ());
	connection->channel->send_buffer (buffer, nano::stat::detail::all, [this_l](boost::system::error_code const & ec, size_t size_a) {
		try
		{
			this_l->promise.set_value (false);
		}
		catch (std::future_error &)
		{
		}
	});
}

void nano::bulk_push_client::push_block (nano::block const & block_a)
{
	std::vector<uint8_t> buffer;
	{
		nano::vectorstream stream (buffer);
		nano::serialize_block (stream, block_a);
	}
	auto this_l (shared_from_this ());
	connection->channel->send_buffer (nano::shared_const_buffer (std::move (buffer)), nano::stat::detail::all, [this_l](boost::system::error_code const & ec, size_t size_a) {
		if (!ec)
		{
			auto transaction (this_l->connection->node->store.tx_begin_read ());
			this_l->push (transaction);
		}
		else
		{
			if (this_l->connection->node->config.logging.bulk_pull_logging ())
			{
				this_l->connection->node->logger.try_log (boost::str (boost::format ("Error sending block during bulk push: %1%") % ec.message ()));
			}
		}
	});
}

nano::bulk_push_server::bulk_push_server (std::shared_ptr<nano::bootstrap_server> const & connection_a)
	: receive_buffer (std::make_shared<std::vector<uint8_t>> ())
	, connection (connection_a)
{
	receive_buffer->resize (256);
}

void nano::bulk_push_server::throttled_receive ()
{
	if (!connection->node->block_processor.half_full ())
	{
		receive ();
	}
	else
	{
		auto this_l (shared_from_this ());
		connection->node->alarm.add (std::chrono::steady_clock::now () + std::chrono::seconds (1), [this_l]() {
			if (!this_l->connection->stopped)
			{
				this_l->throttled_receive ();
			}
		});
	}
}

void nano::bulk_push_server::receive ()
{
	if (connection->node->bootstrap_initiator.in_progress ())
	{
		if (connection->node->config.logging.bulk_pull_logging ())
		{
			connection->node->logger.try_log ("Aborting bulk_push because a bootstrap attempt is in progress");
		}
	}
	else
	{
		auto this_l (shared_from_this ());
		connection->socket->async_read (receive_buffer, 1, [this_l](boost::system::error_code const & ec, size_t size_a) {
			if (!ec)
			{
				this_l->received_type ();
			}
			else
			{
				if (this_l->connection->node->config.logging.bulk_pull_logging ())
				{
					this_l->connection->node->logger.try_log (boost::str (boost::format ("Error receiving block type: %1%") % ec.message ()));
				}
			}
		});
	}
}

void nano::bulk_push_server::received_type ()
{
	auto this_l (shared_from_this ());
	nano::block_type type (static_cast<nano::block_type> (receive_buffer->data ()[0]));
	switch (type)
	{
		case nano::block_type::send:
		{
			connection->node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::send, nano::stat::dir::in);
			connection->socket->async_read (receive_buffer, nano::send_block::size, [this_l, type](boost::system::error_code const & ec, size_t size_a) {
				this_l->received_block (ec, size_a, type);
			});
			break;
		}
		case nano::block_type::receive:
		{
			connection->node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::receive, nano::stat::dir::in);
			connection->socket->async_read (receive_buffer, nano::receive_block::size, [this_l, type](boost::system::error_code const & ec, size_t size_a) {
				this_l->received_block (ec, size_a, type);
			});
			break;
		}
		case nano::block_type::open:
		{
			connection->node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::open, nano::stat::dir::in);
			connection->socket->async_read (receive_buffer, nano::open_block::size, [this_l, type](boost::system::error_code const & ec, size_t size_a) {
				this_l->received_block (ec, size_a, type);
			});
			break;
		}
		case nano::block_type::change:
		{
			connection->node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::change, nano::stat::dir::in);
			connection->socket->async_read (receive_buffer, nano::change_block::size, [this_l, type](boost::system::error_code const & ec, size_t size_a) {
				this_l->received_block (ec, size_a, type);
			});
			break;
		}
		case nano::block_type::state:
		{
			connection->node->stats.inc (nano::stat::type::bootstrap, nano::stat::detail::state_block, nano::stat::dir::in);
			connection->socket->async_read (receive_buffer, nano::state_block::size, [this_l, type](boost::system::error_code const & ec, size_t size_a) {
				this_l->received_block (ec, size_a, type);
			});
			break;
		}
		case nano::block_type::not_a_block:
		{
			connection->finish_request ();
			break;
		}
		default:
		{
			if (connection->node->config.logging.network_packet_logging ())
			{
				connection->node->logger.try_log ("Unknown type received as block type");
			}
			break;
		}
	}
}

void nano::bulk_push_server::received_block (boost::system::error_code const & ec, size_t size_a, nano::block_type type_a)
{
	if (!ec)
	{
		nano::bufferstream stream (receive_buffer->data (), size_a);
		auto block (nano::deserialize_block (stream, type_a));
		if (block != nullptr && !nano::work_validate (*block))
		{
			connection->node->process_active (std::move (block));
			throttled_receive ();
		}
		else
		{
			if (connection->node->config.logging.bulk_pull_logging ())
			{
				connection->node->logger.try_log ("Error deserializing block received from pull request");
			}
		}
	}
}
