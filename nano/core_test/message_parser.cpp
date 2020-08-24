#include <nano/node/testing.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

namespace
{
class dev_visitor : public nano::message_visitor
{
public:
	void keepalive (nano::keepalive const &) override
	{
		++keepalive_count;
	}
	void publish (nano::publish const &) override
	{
		++publish_count;
	}
	void confirm_req (nano::confirm_req const &) override
	{
		++confirm_req_count;
	}
	void confirm_ack (nano::confirm_ack const &) override
	{
		++confirm_ack_count;
	}
	void bulk_pull (nano::bulk_pull const &) override
	{
		ASSERT_FALSE (true);
	}
	void bulk_pull_account (nano::bulk_pull_account const &) override
	{
		ASSERT_FALSE (true);
	}
	void bulk_push (nano::bulk_push const &) override
	{
		ASSERT_FALSE (true);
	}
	void frontier_req (nano::frontier_req const &) override
	{
		ASSERT_FALSE (true);
	}
	void node_id_handshake (nano::node_id_handshake const &) override
	{
		ASSERT_FALSE (true);
	}
	void telemetry_req (nano::telemetry_req const &) override
	{
		ASSERT_FALSE (true);
	}
	void telemetry_ack (nano::telemetry_ack const &) override
	{
		ASSERT_FALSE (true);
	}

	uint64_t keepalive_count{ 0 };
	uint64_t publish_count{ 0 };
	uint64_t confirm_req_count{ 0 };
	uint64_t confirm_ack_count{ 0 };
};
}

TEST (message_parser, exact_confirm_ack_size)
{
	nano::system system (1);
	dev_visitor visitor;
	nano::network_filter filter (1);
	nano::block_uniquer block_uniquer;
	nano::vote_uniquer vote_uniquer (block_uniquer);
	nano::message_parser parser (filter, block_uniquer, vote_uniquer, visitor, system.env.work, false);
	auto block (std::make_shared<nano::send_block> (1, 1, 2, nano::keypair ().prv, 4, *system.env.work.generate (nano::root (1))));
	auto vote (std::make_shared<nano::vote> (0, nano::keypair ().prv, 0, std::move (block)));
	nano::confirm_ack message (vote);
	std::vector<uint8_t> bytes;
	{
		nano::vectorstream stream (bytes);
		message.serialize (stream, true);
	}
	ASSERT_EQ (0, visitor.confirm_ack_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	auto error (false);
	nano::bufferstream stream1 (bytes.data (), bytes.size ());
	nano::message_header header1 (error, stream1);
	ASSERT_FALSE (error);
	parser.deserialize_confirm_ack (stream1, header1);
	ASSERT_EQ (1, visitor.confirm_ack_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	bytes.push_back (0);
	nano::bufferstream stream2 (bytes.data (), bytes.size ());
	nano::message_header header2 (error, stream2);
	ASSERT_FALSE (error);
	parser.deserialize_confirm_ack (stream2, header2);
	ASSERT_EQ (1, visitor.confirm_ack_count);
	ASSERT_NE (parser.status, nano::message_parser::parse_status::success);
}

TEST (message_parser, exact_confirm_req_size)
{
	nano::system system (1);
	dev_visitor visitor;
	nano::network_filter filter (1);
	nano::block_uniquer block_uniquer;
	nano::vote_uniquer vote_uniquer (block_uniquer);
	nano::message_parser parser (filter, block_uniquer, vote_uniquer, visitor, system.env.work, false);
	auto block (std::make_shared<nano::send_block> (1, 1, 2, nano::keypair ().prv, 4, *system.env.work.generate (nano::root (1))));
	nano::confirm_req message (std::move (block));
	std::vector<uint8_t> bytes;
	{
		nano::vectorstream stream (bytes);
		message.serialize (stream, false);
	}
	ASSERT_EQ (0, visitor.confirm_req_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	auto error (false);
	nano::bufferstream stream1 (bytes.data (), bytes.size ());
	nano::message_header header1 (error, stream1);
	ASSERT_FALSE (error);
	parser.deserialize_confirm_req (stream1, header1);
	ASSERT_EQ (1, visitor.confirm_req_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	bytes.push_back (0);
	nano::bufferstream stream2 (bytes.data (), bytes.size ());
	nano::message_header header2 (error, stream2);
	ASSERT_FALSE (error);
	parser.deserialize_confirm_req (stream2, header2);
	ASSERT_EQ (1, visitor.confirm_req_count);
	ASSERT_NE (parser.status, nano::message_parser::parse_status::success);
}

TEST (message_parser, exact_confirm_req_hash_size)
{
	nano::system system (1);
	dev_visitor visitor;
	nano::network_filter filter (1);
	nano::block_uniquer block_uniquer;
	nano::vote_uniquer vote_uniquer (block_uniquer);
	nano::message_parser parser (filter, block_uniquer, vote_uniquer, visitor, system.env.work, true);
	nano::send_block block (1, 1, 2, nano::keypair ().prv, 4, *system.env.work.generate (nano::root (1)));
	nano::confirm_req message (block.hash (), block.root ());
	std::vector<uint8_t> bytes;
	{
		nano::vectorstream stream (bytes);
		message.serialize (stream, false);
	}
	ASSERT_EQ (0, visitor.confirm_req_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	auto error (false);
	nano::bufferstream stream1 (bytes.data (), bytes.size ());
	nano::message_header header1 (error, stream1);
	ASSERT_FALSE (error);
	parser.deserialize_confirm_req (stream1, header1);
	ASSERT_EQ (1, visitor.confirm_req_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	bytes.push_back (0);
	nano::bufferstream stream2 (bytes.data (), bytes.size ());
	nano::message_header header2 (error, stream2);
	ASSERT_FALSE (error);
	parser.deserialize_confirm_req (stream2, header2);
	ASSERT_EQ (1, visitor.confirm_req_count);
	ASSERT_NE (parser.status, nano::message_parser::parse_status::success);
}

TEST (message_parser, exact_publish_size)
{
	nano::system system (1);
	dev_visitor visitor;
	nano::network_filter filter (1);
	nano::block_uniquer block_uniquer;
	nano::vote_uniquer vote_uniquer (block_uniquer);
	nano::message_parser parser (filter, block_uniquer, vote_uniquer, visitor, system.env.work, true);
	auto block (std::make_shared<nano::send_block> (1, 1, 2, nano::keypair ().prv, 4, *system.env.work.generate (nano::root (1))));
	nano::publish message (std::move (block));
	std::vector<uint8_t> bytes;
	{
		nano::vectorstream stream (bytes);
		message.serialize (stream, false);
	}
	ASSERT_EQ (0, visitor.publish_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	auto error (false);
	nano::bufferstream stream1 (bytes.data (), bytes.size ());
	nano::message_header header1 (error, stream1);
	ASSERT_FALSE (error);
	parser.deserialize_publish (stream1, header1);
	ASSERT_EQ (1, visitor.publish_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	bytes.push_back (0);
	nano::bufferstream stream2 (bytes.data (), bytes.size ());
	nano::message_header header2 (error, stream2);
	ASSERT_FALSE (error);
	parser.deserialize_publish (stream2, header2);
	ASSERT_EQ (1, visitor.publish_count);
	ASSERT_NE (parser.status, nano::message_parser::parse_status::success);
}

TEST (message_parser, exact_keepalive_size)
{
	nano::system system (1);
	dev_visitor visitor;
	nano::network_filter filter (1);
	nano::block_uniquer block_uniquer;
	nano::vote_uniquer vote_uniquer (block_uniquer);
	nano::message_parser parser (filter, block_uniquer, vote_uniquer, visitor, system.env.work, true);
	nano::keepalive message;
	std::vector<uint8_t> bytes;
	{
		nano::vectorstream stream (bytes);
		message.serialize (stream, true);
	}
	ASSERT_EQ (0, visitor.keepalive_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	auto error (false);
	nano::bufferstream stream1 (bytes.data (), bytes.size ());
	nano::message_header header1 (error, stream1);
	ASSERT_FALSE (error);
	parser.deserialize_keepalive (stream1, header1);
	ASSERT_EQ (1, visitor.keepalive_count);
	ASSERT_EQ (parser.status, nano::message_parser::parse_status::success);
	bytes.push_back (0);
	nano::bufferstream stream2 (bytes.data (), bytes.size ());
	nano::message_header header2 (error, stream2);
	ASSERT_FALSE (error);
	parser.deserialize_keepalive (stream2, header2);
	ASSERT_EQ (1, visitor.keepalive_count);
	ASSERT_NE (parser.status, nano::message_parser::parse_status::success);
}
