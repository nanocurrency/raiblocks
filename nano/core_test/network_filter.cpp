#include <nano/node/common.hpp>
#include <nano/secure/buffer.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/network_filter.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

TEST (network_filter, unit)
{
	nano::ledger_constants constants{ nano::networks::nano_dev_network };
	nano::network_filter filter (1);
	auto one_block = [&filter] (std::shared_ptr<nano::block> const & block_a, bool expect_duplicate_a) {
		nano::publish message (block_a);
		auto bytes (message.to_bytes ());
		nano::bufferstream stream (bytes->data (), bytes->size ());

		// First read the header
		bool error{ false };
		nano::message_header header (error, stream);
		ASSERT_FALSE (error);

		// This validates nano::message_header::size
		ASSERT_EQ (bytes->size (), block_a->size (block_a->type ()) + header.size);

		// Now filter the rest of the stream
		bool duplicate (filter.apply (bytes->data (), bytes->size () - header.size));
		ASSERT_EQ (expect_duplicate_a, duplicate);

		// Make sure the stream was rewinded correctly
		auto block (nano::deserialize_block (stream, header.block_type ()));
		ASSERT_NE (nullptr, block);
		ASSERT_EQ (*block, *block_a);
	};
	one_block (constants.genesis, false);
	for (int i = 0; i < 10; ++i)
	{
		one_block (constants.genesis, true);
	}
	nano::state_block_builder builder;
	auto new_block = builder
					 .account (nano::dev::genesis_key.pub)
					 .previous (constants.genesis_hash ())
					 .representative (nano::dev::genesis_key.pub)
					 .balance (nano::dev::genesis_amount - 10 * nano::xrb_ratio)
					 .link (nano::public_key ())
					 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
					 .work (0)
					 .build_shared ();

	one_block (new_block, false);
	for (int i = 0; i < 10; ++i)
	{
		one_block (new_block, true);
	}
	for (int i = 0; i < 100; ++i)
	{
		one_block (constants.genesis, false);
		one_block (new_block, false);
	}
}

TEST (network_filter, many)
{
	nano::ledger_constants constants{ nano::networks::nano_dev_network };
	nano::network_filter filter (4);
	nano::keypair key1;
	for (int i = 0; i < 100; ++i)
	{
		nano::state_block_builder builder;
		auto block = builder
					 .account (nano::dev::genesis_key.pub)
					 .previous (constants.genesis_hash ())
					 .representative (nano::dev::genesis_key.pub)
					 .balance (nano::dev::genesis_amount - i * 10 * nano::xrb_ratio)
					 .link (key1.pub)
					 .sign (nano::dev::genesis_key.prv, nano::dev::genesis_key.pub)
					 .work (0)
					 .build_shared ();

		nano::publish message (block);
		auto bytes (message.to_bytes ());
		nano::bufferstream stream (bytes->data (), bytes->size ());

		// First read the header
		bool error{ false };
		nano::message_header header (error, stream);
		ASSERT_FALSE (error);

		// This validates nano::message_header::size
		ASSERT_EQ (bytes->size (), block->size + header.size);

		// Now filter the rest of the stream
		// All blocks should pass through
		ASSERT_FALSE (filter.apply (bytes->data (), block->size));
		ASSERT_FALSE (error);

		// Make sure the stream was rewinded correctly
		auto deserialized_block (nano::deserialize_block (stream, header.block_type ()));
		ASSERT_NE (nullptr, deserialized_block);
		ASSERT_EQ (*block, *deserialized_block);
	}
}

TEST (network_filter, clear)
{
	nano::network_filter filter (1);
	std::vector<uint8_t> bytes1{ 1, 2, 3 };
	std::vector<uint8_t> bytes2{ 1 };
	ASSERT_FALSE (filter.apply (bytes1.data (), bytes1.size ()));
	ASSERT_TRUE (filter.apply (bytes1.data (), bytes1.size ()));
	filter.clear (bytes1.data (), bytes1.size ());
	ASSERT_FALSE (filter.apply (bytes1.data (), bytes1.size ()));
	ASSERT_TRUE (filter.apply (bytes1.data (), bytes1.size ()));
	filter.clear (bytes2.data (), bytes2.size ());
	ASSERT_TRUE (filter.apply (bytes1.data (), bytes1.size ()));
	ASSERT_FALSE (filter.apply (bytes2.data (), bytes2.size ()));
}

TEST (network_filter, optional_digest)
{
	nano::network_filter filter (1);
	std::vector<uint8_t> bytes1{ 1, 2, 3 };
	nano::uint128_t digest{ 0 };
	ASSERT_FALSE (filter.apply (bytes1.data (), bytes1.size (), &digest));
	ASSERT_NE (0, digest);
	ASSERT_TRUE (filter.apply (bytes1.data (), bytes1.size ()));
	filter.clear (digest);
	ASSERT_FALSE (filter.apply (bytes1.data (), bytes1.size ()));
}
