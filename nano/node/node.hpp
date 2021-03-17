#pragma once

#include <nano/lib/config.hpp>
#include <nano/lib/stats.hpp>
#include <nano/lib/work.hpp>
#include <nano/node/active_transactions.hpp>
#include <nano/node/blockprocessor.hpp>
#include <nano/node/bootstrap/bootstrap.hpp>
#include <nano/node/bootstrap/bootstrap_attempt.hpp>
#include <nano/node/bootstrap/bootstrap_server.hpp>
#include <nano/node/confirmation_height_processor.hpp>
#include <nano/node/distributed_work_factory.hpp>
#include <nano/node/election.hpp>
#include <nano/node/gap_cache.hpp>
#include <nano/node/network.hpp>
#include <nano/node/node_observers.hpp>
#include <nano/node/nodeconfig.hpp>
#include <nano/node/online_reps.hpp>
#include <nano/node/portmapping.hpp>
#include <nano/node/repcrawler.hpp>
#include <nano/node/request_aggregator.hpp>
#include <nano/node/signatures.hpp>
#include <nano/node/telemetry.hpp>
#include <nano/node/vote_processor.hpp>
#include <nano/node/wallet.hpp>
#include <nano/node/write_database_queue.hpp>
#include <nano/secure/ledger.hpp>
#include <nano/secure/utility.hpp>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/program_options.hpp>
#include <boost/thread/latch.hpp>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace nano
{
namespace websocket
{
	class listener;
}
class node;
class telemetry;
class work_pool;
class block_arrival_info final
{
public:
	std::chrono::steady_clock::time_point arrival;
	nano::block_hash hash;
};
// This class tracks blocks that are probably live because they arrived in a UDP packet
// This gives a fairly reliable way to differentiate between blocks being inserted via bootstrap or new, live blocks.
class block_arrival final
{
public:
	// Return `true' to indicated an error if the block has already been inserted
	bool add (nano::block_hash const &);
	bool recent (nano::block_hash const &);
	// clang-format off
	class tag_sequence {};
	class tag_hash {};
	boost::multi_index_container<nano::block_arrival_info,
		boost::multi_index::indexed_by<
			boost::multi_index::sequenced<boost::multi_index::tag<tag_sequence>>,
			boost::multi_index::hashed_unique<boost::multi_index::tag<tag_hash>,
				boost::multi_index::member<nano::block_arrival_info, nano::block_hash, &nano::block_arrival_info::hash>>>>
	arrival;
	// clang-format on
	nano::mutex mutex{ mutex_identifier (mutexes::block_arrival) };
	static size_t constexpr arrival_size_min = 8 * 1024;
	static std::chrono::seconds constexpr arrival_time_min = std::chrono::seconds (300);
};

std::unique_ptr<container_info_component> collect_container_info (block_arrival & block_arrival, std::string const & name);

std::unique_ptr<container_info_component> collect_container_info (rep_crawler & rep_crawler, std::string const & name);

class node final : public std::enable_shared_from_this<nano::node>
{
public:
	node (boost::asio::io_context &, uint16_t, boost::filesystem::path const &, nano::logging const &, nano::work_pool &, nano::node_flags = nano::node_flags (), unsigned seq = 0);
	node (boost::asio::io_context &, boost::filesystem::path const &, nano::node_config const &, nano::work_pool &, nano::node_flags = nano::node_flags (), unsigned seq = 0);
	~node ();
	template <typename T>
	void background (T action_a)
	{
		io_ctx.post (action_a);
	}
	bool copy_with_compaction (boost::filesystem::path const &);
	void keepalive (std::string const &, uint16_t);
	void start ();
	void stop ();
	std::shared_ptr<nano::node> shared ();
	int store_version ();
	void receive_confirmed (nano::transaction const & block_transaction_a, nano::block_hash const & hash_a, nano::account const & destination_a);
	void process_confirmed_data (nano::transaction const &, std::shared_ptr<nano::block> const &, nano::block_hash const &, nano::account &, nano::uint128_t &, bool &, nano::account &);
	void process_confirmed (nano::election_status const &, uint64_t = 0);
	void process_active (std::shared_ptr<nano::block> const &);
	nano::process_return process (nano::block &);
	nano::process_return process_local (std::shared_ptr<nano::block> const &, bool const = false);
	void process_local_async (std::shared_ptr<nano::block> const &, bool const = false);
	void keepalive_preconfigured (std::vector<std::string> const &);
	nano::block_hash latest (nano::account const &);
	nano::uint128_t balance (nano::account const &);
	std::shared_ptr<nano::block> block (nano::block_hash const &);
	std::pair<nano::uint128_t, nano::uint128_t> balance_pending (nano::account const &, bool only_confirmed);
	nano::uint128_t weight (nano::account const &);
	nano::block_hash rep_block (nano::account const &);
	nano::uint128_t minimum_principal_weight ();
	nano::uint128_t minimum_principal_weight (nano::uint128_t const &);
	void ongoing_rep_calculation ();
	void ongoing_bootstrap ();
	void ongoing_peer_store ();
	void ongoing_unchecked_cleanup ();
	void backup_wallet ();
	void search_pending ();
	void bootstrap_wallet ();
	void unchecked_cleanup ();
	bool collect_ledger_pruning_targets (std::deque<nano::block_hash> &, nano::account &, uint64_t const, uint64_t const, uint64_t const);
	void ledger_pruning (uint64_t const, bool, bool);
	void ongoing_ledger_pruning ();
	int price (nano::uint128_t const &, int);
	// The default difficulty updates to base only when the first epoch_2 block is processed
	uint64_t default_difficulty (nano::work_version const) const;
	uint64_t default_receive_difficulty (nano::work_version const) const;
	uint64_t max_work_generate_difficulty (nano::work_version const) const;
	bool local_work_generation_enabled () const;
	bool work_generation_enabled () const;
	bool work_generation_enabled (std::vector<std::pair<std::string, uint16_t>> const &) const;
	boost::optional<uint64_t> work_generate_blocking (nano::block &, uint64_t);
	boost::optional<uint64_t> work_generate_blocking (nano::work_version const, nano::root const &, uint64_t, boost::optional<nano::account> const & = boost::none);
	void work_generate (nano::work_version const, nano::root const &, uint64_t, std::function<void(boost::optional<uint64_t>)>, boost::optional<nano::account> const & = boost::none, bool const = false);
	void add_initial_peers ();
	void block_confirm (std::shared_ptr<nano::block> const &);
	bool block_confirmed (nano::block_hash const &);
	bool block_confirmed_or_being_confirmed (nano::transaction const &, nano::block_hash const &);
	void process_fork (nano::transaction const &, std::shared_ptr<nano::block> const &, uint64_t);
	void do_rpc_callback (boost::asio::ip::tcp::resolver::iterator i_a, std::string const &, uint16_t, std::shared_ptr<std::string> const &, std::shared_ptr<std::string> const &, std::shared_ptr<boost::asio::ip::tcp::resolver> const &);
	void ongoing_online_weight_calculation ();
	void ongoing_online_weight_calculation_queue ();
	bool online () const;
	bool init_error () const;
	bool epoch_upgrader (nano::raw_key const &, nano::epoch, uint64_t, uint64_t);
	std::pair<uint64_t, decltype (nano::ledger::bootstrap_weights)> get_bootstrap_weights () const;
	nano::write_database_queue write_database_queue;
	boost::asio::io_context & io_ctx;
	boost::latch node_initialized_latch;
	nano::network_params network_params;
	nano::node_config config;
	nano::stat stats;
	nano::thread_pool workers;
	std::shared_ptr<nano::websocket::listener> websocket_server;
	nano::node_flags flags;
	nano::work_pool & work;
	nano::distributed_work_factory distributed_work;
	nano::logger_mt logger;
	std::unique_ptr<nano::block_store> store_impl;
	nano::block_store & store;
	std::unique_ptr<nano::wallets_store> wallets_store_impl;
	nano::wallets_store & wallets_store;
	nano::gap_cache gap_cache;
	nano::ledger ledger;
	nano::signature_checker checker;
	nano::network network;
	std::shared_ptr<nano::telemetry> telemetry;
	nano::bootstrap_initiator bootstrap_initiator;
	nano::bootstrap_listener bootstrap;
	boost::filesystem::path application_path;
	nano::node_observers observers;
	nano::port_mapping port_mapping;
	nano::online_reps online_reps;
	nano::rep_crawler rep_crawler;
	nano::vote_processor vote_processor;
	unsigned warmed_up;
	nano::block_processor block_processor;
	std::thread block_processor_thread;
	nano::block_arrival block_arrival;
	nano::local_vote_history history;
	nano::keypair node_id;
	nano::block_uniquer block_uniquer;
	nano::vote_uniquer vote_uniquer;
	nano::confirmation_height_processor confirmation_height_processor;
	nano::active_transactions active;
	nano::request_aggregator aggregator;
	nano::wallets wallets;
	std::chrono::steady_clock::time_point const startup_time;
	std::chrono::seconds unchecked_cutoff = std::chrono::seconds (7 * 24 * 60 * 60); // Week
	std::atomic<bool> unresponsive_work_peers{ false };
	std::atomic<bool> stopped{ false };
	static double constexpr price_max = 16.0;
	static double constexpr free_cutoff = 1024.0;
	// For tests only
	unsigned node_seq;
	// For tests only
	boost::optional<uint64_t> work_generate_blocking (nano::block &);
	// For tests only
	boost::optional<uint64_t> work_generate_blocking (nano::root const &, uint64_t);
	// For tests only
	boost::optional<uint64_t> work_generate_blocking (nano::root const &);

private:
	void long_inactivity_cleanup ();
	void epoch_upgrader_impl (nano::raw_key const &, nano::epoch, uint64_t, uint64_t);
	nano::locked<std::future<void>> epoch_upgrading;
};

std::unique_ptr<container_info_component> collect_container_info (node & node, std::string const & name);

nano::node_flags const & inactive_node_flag_defaults ();

class node_wrapper final
{
public:
	node_wrapper (boost::filesystem::path const & path_a, boost::filesystem::path const & config_path_a, nano::node_flags const & node_flags_a);
	~node_wrapper ();

	std::shared_ptr<boost::asio::io_context> io_context;
	nano::work_pool work;
	std::shared_ptr<nano::node> node;
};

class inactive_node final
{
public:
	inactive_node (boost::filesystem::path const & path_a, nano::node_flags const & node_flags_a);
	inactive_node (boost::filesystem::path const & path_a, boost::filesystem::path const & config_path_a, nano::node_flags const & node_flags_a);

	nano::node_wrapper node_wrapper;
	std::shared_ptr<nano::node> node;
};
std::unique_ptr<nano::inactive_node> default_inactive_node (boost::filesystem::path const &, boost::program_options::variables_map const &);
}
