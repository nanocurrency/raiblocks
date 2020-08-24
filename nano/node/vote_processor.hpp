#pragma once

#include <nano/lib/numbers.hpp>
#include <nano/lib/utility.hpp>
#include <nano/secure/common.hpp>

#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>

namespace nano
{
class active_transactions;
class block_store;
class environment_constants;
class ledger;
class logger_mt;
class node_config;
class node_flags;
class node_observers;
class online_reps;
class signature_checker;
class stats;
class transaction;
namespace transport
{
	class channel;
}

class vote_processor final
{
public:
	explicit vote_processor (nano::signature_checker & checker_a, nano::active_transactions & active_a, nano::node_observers & observers_a, nano::stat & stats_a, nano::node_config & config_a, nano::node_flags & flags_a, nano::logger_mt & logger_a, nano::online_reps & online_reps_a, nano::ledger & ledger_a, nano::environment_constants & constants_a);
	/** Returns false if the vote was processed */
	bool vote (std::shared_ptr<nano::vote>, std::shared_ptr<nano::transport::channel>);
	/** Note: node.active.mutex lock is required */
	nano::vote_code vote_blocking (std::shared_ptr<nano::vote>, std::shared_ptr<nano::transport::channel>, bool = false);
	void verify_votes (std::deque<std::pair<std::shared_ptr<nano::vote>, std::shared_ptr<nano::transport::channel>>> const &);
	void flush ();
	size_t size ();
	bool empty ();
	void calculate_weights ();
	void stop ();

private:
	void process_loop ();

	nano::signature_checker & checker;
	nano::active_transactions & active;
	nano::node_observers & observers;
	nano::stat & stats;
	nano::node_config & config;
	nano::logger_mt & logger;
	nano::online_reps & online_reps;
	nano::ledger & ledger;
	nano::environment_constants & constants;

	size_t max_votes;

	std::deque<std::pair<std::shared_ptr<nano::vote>, std::shared_ptr<nano::transport::channel>>> votes;
	/** Representatives levels for random early detection */
	std::unordered_set<nano::account> representatives_1;
	std::unordered_set<nano::account> representatives_2;
	std::unordered_set<nano::account> representatives_3;
	nano::condition_variable condition;
	std::mutex mutex;
	bool started;
	bool stopped;
	bool is_active;
	std::thread thread;

	friend std::unique_ptr<container_info_component> collect_container_info (vote_processor & vote_processor, const std::string & name);
	friend class vote_processor_weights_Test;
};

std::unique_ptr<container_info_component> collect_container_info (vote_processor & vote_processor, const std::string & name);
}
