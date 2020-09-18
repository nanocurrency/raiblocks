#pragma once

#include <nano/node/common.hpp>
#include <nano/node/socket.hpp>

#include <unordered_set>

namespace nano
{
class bootstrap_attempt;
class pull_info
{
public:
	using count_t = nano::bulk_pull::count_t;
	pull_info () = default;
	pull_info (nano::hash_or_account const &, nano::block_hash const &, nano::block_hash const &, uint64_t, count_t = 0, unsigned = 16);
	nano::hash_or_account account_or_head{ 0 };
	nano::block_hash head{ 0 };
	nano::block_hash head_original{ 0 };
	nano::block_hash end{ 0 };
	count_t count{ 0 };
	unsigned attempts{ 0 };
	uint64_t processed{ 0 };
	unsigned retry_limit{ 0 };
	uint64_t bootstrap_id{ 0 };
};
class bootstrap_client;
class bulk_pull_client final : public std::enable_shared_from_this<nano::bulk_pull_client>
{
public:
	bulk_pull_client (std::shared_ptr<nano::bootstrap_client>, std::shared_ptr<nano::bootstrap_attempt>, nano::pull_info const &);
	~bulk_pull_client ();
	void request ();
	void receive_block ();
	void throttled_receive_block ();
	void received_type (nano::block_type type_a, boost::asio::yield_context yield);
	void received_block (nano::block_type type_a, size_t size_a);
	nano::block_hash first ();
	std::shared_ptr<nano::bootstrap_client> connection;
	std::shared_ptr<nano::bootstrap_attempt> attempt;
	nano::block_hash expected;
	nano::account known_account;
	nano::pull_info pull;
	uint64_t pull_blocks;
	uint64_t unexpected_count;
	bool network_error{ false };
};
class bulk_pull_account_client final : public std::enable_shared_from_this<nano::bulk_pull_account_client>
{
public:
	bulk_pull_account_client (std::shared_ptr<nano::bootstrap_client>, std::shared_ptr<nano::bootstrap_attempt>, nano::account const &);
	~bulk_pull_account_client ();
	void request ();
	void receive_pending (boost::asio::yield_context yield);
	std::shared_ptr<nano::bootstrap_client> connection;
	std::shared_ptr<nano::bootstrap_attempt> attempt;
	nano::account account;
	uint64_t pull_blocks;
};
class bootstrap_server;
class bulk_pull;
class bulk_pull_server final : public std::enable_shared_from_this<nano::bulk_pull_server>
{
public:
	bulk_pull_server (std::shared_ptr<nano::bootstrap_server> const &, std::unique_ptr<nano::bulk_pull>);
	void set_current_end ();
	std::shared_ptr<nano::block> get_next ();
	void send_next ();
	void sent_action (boost::system::error_code const &, size_t);
	void send_finished ();
	void no_block_sent (boost::system::error_code const &, size_t);
	std::shared_ptr<nano::bootstrap_server> connection;
	std::unique_ptr<nano::bulk_pull> request;
	nano::block_hash current;
	bool include_start;
	nano::bulk_pull::count_t max_count;
	nano::bulk_pull::count_t sent_count;
};
class bulk_pull_account;
class bulk_pull_account_server final : public std::enable_shared_from_this<nano::bulk_pull_account_server>
{
public:
	bulk_pull_account_server (std::shared_ptr<nano::bootstrap_server> const &, std::unique_ptr<nano::bulk_pull_account>);
	void set_params ();
	std::pair<std::unique_ptr<nano::pending_key>, std::unique_ptr<nano::pending_info>> get_next ();
	void send_frontier ();
	void send_next_block ();
	void sent_action (boost::system::error_code const &, size_t);
	void send_finished ();
	void complete (boost::system::error_code const &, size_t);
	std::shared_ptr<nano::bootstrap_server> connection;
	std::unique_ptr<nano::bulk_pull_account> request;
	std::unordered_set<nano::uint256_union> deduplication;
	nano::pending_key current_key;
	bool pending_address_only;
	bool pending_include_address;
	bool invalid_request;
};
}
