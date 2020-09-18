#include <boost/asio/spawn.hpp>
#include <boost/asio/steady_timer.hpp>

#include <gtest/gtest.h>

#include <thread>
#include <vector>

TEST (coroutines, multithreaded_insert)
{
	size_t threads = 16;
	size_t inserts = 1000;
	boost::asio::io_context ctx;
	std::vector<std::thread> producers;
	std::atomic<uint64_t> items = 0;
	std::atomic<uint64_t> runs = 0;
	for (auto i = 0; i < threads; ++i)
	{
		producers.emplace_back ([&ctx, &inserts, &items, &runs] () {
			for (auto i = 0; i < inserts; ++i)
			{
				++items;
				boost::asio::spawn (ctx, [&ctx, &runs] (boost::asio::yield_context yield) {
					boost::asio::steady_timer timer{ ctx };
					timer.expires_from_now (std::chrono::milliseconds (1000));
					timer.async_wait (yield);
					++runs;
				});
			}
		});
	}
	std::vector<std::thread> consumers;
	for (auto i = 0; i < threads; ++i)
	{
		consumers.emplace_back ([&ctx] () {
			ctx.run ();
		});
	}
	for (auto & i: producers)
	{
		i.join ();
	}
	for (auto & i : consumers)
	{
		i.join ();
	}
	ASSERT_EQ (threads * inserts, items);
	ASSERT_EQ (threads * inserts, runs);
}
