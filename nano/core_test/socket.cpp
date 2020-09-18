#include <nano/lib/threading.hpp>
#include <nano/node/socket.hpp>
#include <nano/node/testing.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

#include <boost/asio/spawn.hpp>

using namespace std::chrono_literals;

TEST (socket, drop_policy)
{
	auto node_flags = nano::inactive_node_flag_defaults ();
	node_flags.read_only = false;
	nano::inactive_node inactivenode (nano::unique_path (), node_flags);
	auto node = inactivenode.node;

	nano::thread_runner runner (node->io_ctx, 1);

	std::vector<std::shared_ptr<nano::socket>> connections;

	// We're going to write twice the queue size + 1, and the server isn't reading
	// The total number of drops should thus be 1 (the socket allows doubling the queue size for no_socket_drop)
	size_t max_write_queue_size = 0;
	{
		auto client_dummy (std::make_shared<nano::socket> (*node));
		max_write_queue_size = client_dummy->queue_size_max;
	}

	auto func = [&](size_t total_message_count, nano::buffer_drop_policy drop_policy) {
		auto server_port (nano::get_available_port ());
		boost::asio::ip::tcp::endpoint endpoint (boost::asio::ip::address_v6::any (), server_port);

		auto server_socket (std::make_shared<nano::server_socket> (*node, endpoint, 1));
		boost::system::error_code ec;
		server_socket->start (ec);
		ASSERT_FALSE (ec);

		// Accept connection, but don't read so the writer will drop.
		server_socket->on_connection ([&connections](std::shared_ptr<nano::socket> new_connection, boost::system::error_code const & ec_a) {
			connections.push_back (new_connection);
			return true;
		});

		auto client (std::make_shared<nano::socket> (*node));
		nano::transport::channel_tcp channel{ *node, client };
		nano::util::counted_completion write_completion (total_message_count);

		node->spawn (
		[client, &channel, total_message_count, node, &write_completion, &drop_policy, server_port](boost::asio::yield_context yield) mutable {
			boost::system::error_code ec;
			client->async_connect (boost::asio::ip::tcp::endpoint (boost::asio::ip::address_v6::loopback (), server_port), yield[ec]);
			for (int i = 0; i < total_message_count; i++)
			{
				std::vector<uint8_t> buff (1);
				channel.send_buffer (
				nano::shared_const_buffer (std::move (buff)), [&write_completion, client](boost::system::error_code const & ec, size_t size_a) mutable {
					client.reset ();
					write_completion.increment ();
				},
				drop_policy);
			}
		});
		ASSERT_FALSE (write_completion.await_count_for (std::chrono::seconds (5)));
		ASSERT_EQ (1, client.use_count ());
	};

	func (max_write_queue_size * 2 + 1, nano::buffer_drop_policy::no_socket_drop);
	ASSERT_EQ (1, node->stats.count (nano::stat::type::tcp, nano::stat::detail::tcp_write_no_socket_drop, nano::stat::dir::out));
	ASSERT_EQ (0, node->stats.count (nano::stat::type::tcp, nano::stat::detail::tcp_write_drop, nano::stat::dir::out));

	func (max_write_queue_size + 1, nano::buffer_drop_policy::limiter);
	// The stats are accumulated from before
	ASSERT_EQ (1, node->stats.count (nano::stat::type::tcp, nano::stat::detail::tcp_write_no_socket_drop, nano::stat::dir::out));
	ASSERT_EQ (1, node->stats.count (nano::stat::type::tcp, nano::stat::detail::tcp_write_drop, nano::stat::dir::out));

	node->stop ();
	runner.stop_event_processing ();
	runner.join ();
}

TEST (socket, concurrent_writes)
{
	auto node_flags = nano::inactive_node_flag_defaults ();
	node_flags.read_only = false;
	nano::inactive_node inactivenode (nano::unique_path (), node_flags);
	auto node = inactivenode.node;

	// This gives more realistic execution than using system#poll, allowing writes to
	// queue up and drain concurrently.
	nano::thread_runner runner (node->io_ctx, 1);

	constexpr size_t max_connections = 4;
	constexpr size_t client_count = max_connections;
	constexpr size_t message_count = 4;
	constexpr size_t total_message_count = client_count * message_count;

	// We're expecting client_count*4 messages
	nano::util::counted_completion read_count_completion (total_message_count);
	std::function<void(std::shared_ptr<nano::socket>)> reader = [&read_count_completion, &total_message_count, &reader](std::shared_ptr<nano::socket> socket_a) {
		auto buff (std::make_shared<std::vector<uint8_t>> ());
		buff->resize (1);
#ifndef _WIN32
#pragma GCC diagnostic push
#if defined(__has_warning)
#if __has_warning("-Wunused-lambda-capture")
/** total_message_count is constexpr and a capture isn't needed. However, removing it fails to compile on VS2017 due to a known compiler bug. */
#pragma GCC diagnostic ignored "-Wunused-lambda-capture"
#endif
#endif
#endif
		socket_a->async_read (buff, 1, [&read_count_completion, &reader, &total_message_count, socket_a, buff](boost::system::error_code const & ec, size_t size_a) {
			if (!ec)
			{
				if (read_count_completion.increment () < total_message_count)
				{
					reader (socket_a);
				}
			}
			else if (ec != boost::asio::error::eof)
			{
				std::cerr << "async_read: " << ec.message () << std::endl;
			}
		});
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
	};

	boost::asio::ip::tcp::endpoint endpoint (boost::asio::ip::address_v4::any (), 25000);

	auto server_socket (std::make_shared<nano::server_socket> (*node, endpoint, max_connections));
	boost::system::error_code ec;
	server_socket->start (ec);
	ASSERT_FALSE (ec);
	std::vector<std::shared_ptr<nano::socket>> connections;

	// On every new connection, start reading data
	server_socket->on_connection ([&connections, &reader](std::shared_ptr<nano::socket> new_connection, boost::system::error_code const & ec_a) {
		if (ec_a)
		{
			std::cerr << "on_connection: " << ec_a.message () << std::endl;
		}
		else
		{
			connections.push_back (new_connection);
			reader (new_connection);
		}
		// Keep accepting connections
		return true;
	});

	nano::util::counted_completion connection_count_completion (client_count);
	std::vector<std::shared_ptr<nano::socket>> clients;
	for (unsigned i = 0; i < client_count; i++)
	{
		auto client (std::make_shared<nano::socket> (*node));
		clients.push_back (client);
		node->spawn (
		[client, &connection_count_completion](boost::asio::yield_context yield) {
			boost::system::error_code ec;
			client->async_connect (boost::asio::ip::tcp::endpoint (boost::asio::ip::address_v4::loopback (), 25000), yield[ec]);
			if (ec)
			{
				std::cerr << "async_connect: " << ec.message () << std::endl;
			}
			else
			{
				connection_count_completion.increment ();
			}
		});
	}
	ASSERT_FALSE (connection_count_completion.await_count_for (10s));

	// Execute overlapping writes from multiple threads
	auto client (clients[0]);
	std::vector<std::thread> client_threads;
	for (int i = 0; i < client_count; i++)
	{
#ifndef _WIN32
#pragma GCC diagnostic push
#if defined(__has_warning)
#if __has_warning("-Wunused-lambda-capture")
/** total_message_count is constexpr and a capture isn't needed. However, removing it fails to compile on VS2017 due to a known compiler bug. */
#pragma GCC diagnostic ignored "-Wunused-lambda-capture"
#endif
#endif
#endif
		client_threads.emplace_back ([&client, &message_count]() {
			for (int i = 0; i < message_count; i++)
			{
				std::vector<uint8_t> buff;
				buff.push_back ('A' + i);
				client->async_write (nano::shared_const_buffer (std::move (buff)));
			}
		});
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
	}

	ASSERT_FALSE (read_count_completion.await_count_for (10s));
	node->stop ();
	runner.stop_event_processing ();
	runner.join ();

	ASSERT_EQ (node->stats.count (nano::stat::type::tcp, nano::stat::detail::tcp_accept_success, nano::stat::dir::in), client_count);
	// We may exhaust max connections and have some tcp accept failures, but no more than the client count
	ASSERT_LT (node->stats.count (nano::stat::type::tcp, nano::stat::detail::tcp_accept_failure, nano::stat::dir::in), client_count);

	for (auto & t : client_threads)
	{
		t.join ();
	}
}

TEST (socket_yield, context)
{
	nano::system system{ 1 };
	auto node = system.nodes[0];
	boost::asio::ip::tcp::endpoint endpoint{ boost::asio::ip::address_v6::loopback (), nano::get_available_port () };
	std::promise<uint8_t> promise;
	auto future = promise.get_future ();
	boost::asio::spawn (system.io_ctx, [&promise, &endpoint, node](boost::asio::yield_context yield) {
		boost::system::error_code ec;
		ASSERT_FALSE (ec);
		auto socket0 = std::make_shared<nano::server_socket> (*node, endpoint, 1);
		socket0->start (ec);
		ASSERT_FALSE (ec);
		auto socket1 = std::make_shared<nano::socket> (*node);
		socket0->async_accept (*socket1, yield[ec]);
		ASSERT_FALSE (ec);
		auto buffer = std::make_shared<std::vector<uint8_t>> (1);
		socket1->async_read (buffer, 1, yield[ec]);
		ASSERT_FALSE (ec);
		promise.set_value (buffer->operator[] (0));
	});
	boost::asio::spawn (system.io_ctx, [&endpoint, node](boost::asio::yield_context yield) {
		boost::system::error_code ec;
		ASSERT_FALSE (ec);
		auto socket = std::make_shared<nano::socket> (*node);
		socket->async_connect (endpoint, yield[ec]);
		ASSERT_FALSE (ec);
		auto buffer = std::make_shared<std::vector<uint8_t>> (1);
		buffer->operator[] (0) = 42;
		auto written = socket->async_write (nano::shared_const_buffer{ buffer }, yield[ec]);
		ASSERT_FALSE (ec);
		ASSERT_EQ (1, written);
	});
	ASSERT_TIMELY (1s, future.wait_for (std::chrono::milliseconds (0)) == std::future_status::ready);
	ASSERT_EQ (42, future.get ());
}

TEST (socket_yied, async_connect_fail)
{
	nano::system system{ 1 };
	auto node = system.nodes[0];
	std::promise<boost::system::error_code> promise;
	auto future = promise.get_future ();
	boost::asio::spawn (system.io_ctx, [&promise, node](boost::asio::yield_context yield) {
		boost::system::error_code ec;
		auto socket = std::make_shared<nano::socket> (*node);
		socket->async_connect ({ boost::asio::ip::address_v6::loopback (), 0 }, yield[ec]);
		promise.set_value (ec);
	});
	ASSERT_TIMELY (1s, future.wait_for (std::chrono::milliseconds (0)) == std::future_status::ready);
	ASSERT_TRUE (future.get ());
}

TEST (socket_yield, async_read_fail)
{
	nano::system system{ 1 };
	auto node = system.nodes[0];
	std::promise<boost::system::error_code> promise;
	auto future = promise.get_future ();
	boost::asio::spawn (system.io_ctx, [&promise, node](boost::asio::yield_context yield) {
		boost::system::error_code ec;
		auto socket = std::make_shared<nano::socket> (*node);
		auto buffer = std::make_shared<std::vector<uint8_t>> (1);
		socket->async_read (buffer, 1, yield[ec]);
		promise.set_value (ec);
	});
	ASSERT_TIMELY (1s, future.wait_for (std::chrono::milliseconds (0)) == std::future_status::ready);
	ASSERT_TRUE (future.get ());
}

TEST (socket_yield, async_write_fail)
{
	nano::system system{ 1 };
	auto node = system.nodes[0];
	std::promise<boost::system::error_code> promise;
	auto future = promise.get_future ();
	boost::asio::spawn (system.io_ctx, [&promise, node](boost::asio::yield_context yield) {
		boost::system::error_code ec;
		auto socket = std::make_shared<nano::socket> (*node);
		auto buffer = std::make_shared<std::vector<uint8_t>> (1);
		socket->async_write (nano::shared_const_buffer{ buffer }, yield[ec]);
		promise.set_value (ec);
	});
	ASSERT_TIMELY (1s, future.wait_for (std::chrono::milliseconds (0)) == std::future_status::ready);
	ASSERT_TRUE (future.get ());
	ASSERT_EQ (0, node->stats.count (nano::stat::type::traffic_tcp, nano::stat::dir::out));
}

TEST (socket_yield, async_write_closed_fail)
{
	nano::system system{ 1 };
	auto node = system.nodes[0];
	std::promise<boost::system::error_code> promise;
	auto future = promise.get_future ();
	boost::asio::spawn (system.io_ctx, [&promise, node](boost::asio::yield_context yield) {
		boost::system::error_code ec;
		auto socket = std::make_shared<nano::socket> (*node);
		socket->close ();
		auto buffer = std::make_shared<std::vector<uint8_t>> (1);
		socket->async_write (nano::shared_const_buffer{ buffer }, yield[ec]);
		promise.set_value (ec);
	});
	ASSERT_TIMELY (1s, future.wait_for (std::chrono::milliseconds (0)) == std::future_status::ready);
	ASSERT_TRUE (future.get ().value ());
}
