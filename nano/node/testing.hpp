#pragma once

#include <nano/lib/errors.hpp>
#include <nano/node/node.hpp>

#include <chrono>

namespace nano
{
/** Test-system related error codes */
enum class error_system
{
	generic = 1,
	deadline_expired
};
class system final
{
public:
	system ();
	system (uint16_t, nano::transport::transport_type = nano::transport::transport_type::tcp, nano::node_config = nano::node_config{});
	~system ();
	void generate_activity (nano::node &, std::vector<nano::account> &);
	void generate_mass_activity (uint32_t, nano::node &);
	void generate_usage_traffic (uint32_t, uint32_t, size_t);
	void generate_usage_traffic (uint32_t, uint32_t);
	nano::account get_random_account (std::vector<nano::account> &);
	nano::uint128_t get_random_amount (nano::transaction const &, nano::node &, nano::account const &);
	void generate_rollback (nano::node &, std::vector<nano::account> &);
	void generate_change_known (nano::node &, std::vector<nano::account> &);
	void generate_change_unknown (nano::node &, std::vector<nano::account> &);
	void generate_receive (nano::node &);
	void generate_send_new (nano::node &, std::vector<nano::account> &);
	void generate_send_existing (nano::node &, std::vector<nano::account> &);
	std::unique_ptr<nano::state_block> upgrade_genesis_epoch (nano::node &, nano::epoch const);
	std::shared_ptr<nano::wallet> wallet (size_t);
	nano::account account (nano::transaction const &, size_t);
	/** Generate work with difficulty between \p min_difficulty_a (inclusive) and \p max_difficulty_a (exclusive) */
	uint64_t work_generate_limited (nano::block_hash const & root_a, uint64_t min_difficulty_a, uint64_t max_difficulty_a);
	/**
	 * Polls, sleep if there's no work to be done (default 50ms), then check the deadline
	 * @returns 0 or nano::deadline_expired
	 */
	std::error_code poll (const std::chrono::nanoseconds & sleep_time = std::chrono::milliseconds (50));
	std::error_code poll_until_true (std::chrono::nanoseconds deadline, std::function<bool()>);
	void stop ();
	void deadline_set (const std::chrono::duration<double, std::nano> & delta);
	std::shared_ptr<nano::node> add_node (nano::node_config const & = nano::node_config{}, nano::transport::transport_type = nano::transport::transport_type::tcp);
	std::vector<std::shared_ptr<nano::node>> nodes;
	std::chrono::time_point<std::chrono::steady_clock, std::chrono::duration<double>> deadline{ std::chrono::steady_clock::time_point::max () };
	double deadline_scaling_factor{ 1.0 };
	unsigned node_sequence{ 0 };
	std::shared_ptr<nano::environment> env_impl;
	nano::environment & env;
};
std::unique_ptr<nano::state_block> upgrade_epoch (nano::work_pool &, nano::ledger &, nano::epoch);
void blocks_confirm (nano::node &, std::vector<std::shared_ptr<nano::block>> const &);
uint16_t get_available_port ();
void cleanup_dev_directories_on_exit ();
/** To use RocksDB in tests make sure the node is built with the cmake variable -DNANO_ROCKSDB=ON and the environment variable TEST_USE_ROCKSDB=1 is set */
bool using_rocksdb_in_tests ();
}
REGISTER_ERROR_CODES (nano, error_system);
