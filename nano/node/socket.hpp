#pragma once

#include <nano/boost/asio/ip/tcp.hpp>
#include <nano/boost/asio/strand.hpp>
#include <nano/lib/asio.hpp>

#include <boost/optional.hpp>

#include <chrono>
#include <deque>
#include <memory>
#include <vector>

namespace nano
{
/** Policy to affect at which stage a buffer can be dropped */
enum class buffer_drop_policy
{
	/** Can be dropped by bandwidth limiter (default) */
	limiter,
	/** Should not be dropped by bandwidth limiter */
	no_limiter_drop,
	/** Should not be dropped by bandwidth limiter or socket write queue limiter */
	no_socket_drop
};

class node;
class server_socket;

/** Socket class for tcp clients and newly accepted connections */
class socket : public std::enable_shared_from_this<nano::socket>
{
	friend class server_socket;

public:
	/**
	 * Constructor
	 * @param node Owning node
	 * @param io_timeout If tcp async operation is not completed within the timeout, the socket is closed. If not set, the tcp_io_timeout config option is used.
	 * @param concurrency write concurrency
	 */
	explicit socket (nano::node & node);
	virtual ~socket ();
	void async_connect (boost::asio::ip::tcp::endpoint const &, std::function<void(boost::system::error_code const &)>);
	void async_read (std::shared_ptr<std::vector<uint8_t>> const &, size_t, std::function<void(boost::system::error_code const &, size_t)>);
	void async_write (nano::shared_const_buffer const &, std::function<void(boost::system::error_code const &, size_t)> const & = nullptr);

	void close ();
	boost::asio::ip::tcp::endpoint remote_endpoint () const;
	/** Returns true if the socket has timed out */
	bool has_timed_out () const;
	/** This can be called to change the maximum idle time, e.g. based on the type of traffic detected. */
	void timeout_set (std::chrono::seconds io_timeout_a);
	void start_timer (std::chrono::seconds deadline_a);
	bool max () const
	{
		return queue_size >= queue_size_max;
	}
	bool full () const
	{
		return queue_size >= queue_size_max * 2;
	}

protected:
	/** Holds the buffer and callback for queued writes */
	class queue_item
	{
	public:
		nano::shared_const_buffer buffer;
		std::function<void(boost::system::error_code const &, size_t)> callback;
	};

	boost::asio::strand<boost::asio::io_context::executor_type> strand;
	boost::asio::ip::tcp::socket tcp_socket;
	nano::node & node;

	/** The other end of the connection */
	boost::asio::ip::tcp::endpoint remote;

	std::atomic<uint64_t> next_deadline;
	std::atomic<uint64_t> last_completion_time;
	std::atomic<bool> timed_out{ false };
	std::atomic<std::chrono::seconds> io_timeout;
	std::atomic<size_t> queue_size{ 0 };

	/** Set by close() - completion handlers must check this. This is more reliable than checking
	 error codes as the OS may have already completed the async operation. */
	std::atomic<bool> closed{ false };
	void close_internal ();
	void start_timer ();
	void stop_timer ();
	void checkup ();

public:
	static size_t constexpr queue_size_max = 128;
};

/** Socket class for TCP servers */
class server_socket final : public socket
{
public:
	/**
	 * Constructor
	 * @param node_a Owning node
	 * @param local_a Address and port to listen on
	 * @param max_connections_a Maximum number of concurrent connections
	 * @param concurrency_a Write concurrency for new connections
	 */
	explicit server_socket (nano::node & node_a, boost::asio::ip::tcp::endpoint local_a, size_t max_connections_a);
	/**Start accepting new connections */
	void start (boost::system::error_code &);
	/** Stop accepting new connections */
	void close ();
	/** Register callback for new connections. The callback must return true to keep accepting new connections. */
	void on_connection (std::function<bool(std::shared_ptr<nano::socket> const & new_connection, boost::system::error_code const &)>);
	uint16_t listening_port ()
	{
		return local.port ();
	}

private:
	std::vector<std::weak_ptr<nano::socket>> connections;
	boost::asio::ip::tcp::acceptor acceptor;
	boost::asio::ip::tcp::endpoint local;
	size_t max_inbound_connections;
	void evict_dead_connections ();
};
}
