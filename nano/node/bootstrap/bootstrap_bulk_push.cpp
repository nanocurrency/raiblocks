#include <nano/node/bootstrap/bootstrap_attempt.hpp>
#include <nano/node/bootstrap/bootstrap_bulk_push.hpp>
#include <nano/node/node.hpp>
#include <nano/node/transport/tcp.hpp>

#include <boost/format.hpp>

nano::bulk_push_client::bulk_push_client (std::shared_ptr<nano::bootstrap_client> const & connection_a, std::shared_ptr<nano::bootstrap_attempt> const & attempt_a) :
connection (connection_a),
attempt (attempt_a)
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
		if (!ec)
		{
			this_l->push ();
		}
		else
		{
			if (this_l->connection->node->config.logging.bulk_pull_logging ())
			{
				this_l->connection->node->logger.try_log (boost::str (boost::format ("Unable to send bulk_push request: %1%") % ec.message ()));
			}
		}
	},
	nano::buffer_drop_policy::no_limiter_drop);
}

void nano::bulk_push_client::push ()
{
	std::shared_ptr<nano::block> block;
	bool finished (false);
	while (block == nullptr && !finished)
	{
		if (current_target.first.is_zero () || current_target.first == current_target.second)
		{
			finished = attempt->request_bulk_push_target (current_target);
		}
		if (!finished)
		{
			block = connection->node->block (current_target.first);
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
	connection->channel->send_buffer (buffer, [this_l](boost::system::error_code const & ec, size_t size_a) {
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
	connection->channel->send_buffer (nano::shared_const_buffer (std::move (buffer)), [this_l](boost::system::error_code const & ec, size_t size_a) {
		if (!ec)
		{
			this_l->push ();
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

nano::bulk_push_server::bulk_push_server (std::shared_ptr<nano::bootstrap_server> const & connection_a) :
receive_buffer (std::make_shared<std::vector<uint8_t>> ()),
connection (connection_a)
{
	receive_buffer->resize (256);
}

void nano::bulk_push_server::throttled_receive ()
{
	if (!connection->node->block_processor.half_full ())
	{
		connection->node->spawn (
		[this_l = shared_from_this ()](boost::asio::yield_context yield) {
			this_l->receive (yield);
		});
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

void nano::bulk_push_server::receive (boost::asio::yield_context yield)
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
		boost::system::error_code ec;
		connection->socket->async_read (receive_buffer, 1, yield[ec]);
		if (!ec)
		{
			nano::block_type type (static_cast<nano::block_type> (receive_buffer->data ()[0]));
			received_type (type, yield);
		}
		else
		{
			if (connection->node->config.logging.bulk_pull_logging ())
			{
				connection->node->logger.try_log (boost::str (boost::format ("Error receiving block type: %1%") % ec.message ()));
			}
		}
	}
}

void nano::bulk_push_server::received_type (nano::block_type type_a, boost::asio::yield_context yield)
{
	switch (type_a)
	{
		case nano::block_type::send:
		case nano::block_type::receive:
		case nano::block_type::open:
		case nano::block_type::change:
		case nano::block_type::state:
		{
			size_t to_read = 0;
			nano::stat::detail stat = nano::stat::detail::all;
			switch (type_a)
			{
				case nano::block_type::send:
					to_read = nano::send_block::size;
					stat = nano::stat::detail::send;
					break;
				case nano::block_type::receive:
					to_read = nano::receive_block::size;
					stat = nano::stat::detail::receive;
					break;
				case nano::block_type::open:
					to_read = nano::open_block::size;
					stat = nano::stat::detail::open;
					break;
				case nano::block_type::change:
					to_read = nano::change_block::size;
					stat = nano::stat::detail::change;
					break;
				case nano::block_type::state:
					to_read = nano::state_block::size;
					stat = nano::stat::detail::state_block;
					break;
				default:
					debug_assert (false);
					break;
			}
			connection->node->stats.inc (nano::stat::type::bootstrap, stat, nano::stat::dir::in);
			boost::system::error_code ec;
			auto read = connection->socket->async_read (receive_buffer, to_read, yield[ec]);
			if (!ec)
			{
				received_block (type_a, read);
			}
			break;
		}
		case nano::block_type::not_a_block:
			connection->finish_request ();
			break;
		default:
			if (connection->node->config.logging.network_packet_logging ())
			{
				connection->node->logger.try_log ("Unknown type received as block type");
			}
			break;
	}
}

void nano::bulk_push_server::received_block (nano::block_type type_a, size_t size_a)
{
	nano::bufferstream stream{ receive_buffer->data (), size_a };
	auto block = nano::deserialize_block (stream, type_a);
	if (block != nullptr && !nano::work_validate_entry (*block))
	{
		connection->node->process_active (std::move (block));
		throttled_receive ();
	}
	else if (block == nullptr)
	{
		if (connection->node->config.logging.bulk_pull_logging ())
		{
			connection->node->logger.try_log ("Error deserializing block received from pull request");
		}
	}
	else // Work invalid
	{
		if (connection->node->config.logging.bulk_pull_logging ())
		{
			connection->node->logger.try_log (boost::str (boost::format ("Insufficient work for bulk push block: %1%") % block->hash ().to_string ()));
		}
		connection->node->stats.inc_detail_only (nano::stat::type::error, nano::stat::detail::insufficient_work);
	}
}
