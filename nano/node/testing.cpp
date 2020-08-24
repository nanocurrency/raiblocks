#include <nano/crypto_lib/random_pool.hpp>
#include <nano/node/common.hpp>
#include <nano/node/testing.hpp>
#include <nano/node/transport/udp.hpp>

#include <boost/property_tree/json_parser.hpp>

#include <cstdlib>

using namespace std::chrono_literals;

std::string nano::error_system_messages::message (int ev) const
{
	switch (static_cast<nano::error_system> (ev))
	{
		case nano::error_system::generic:
			return "Unknown error";
		case nano::error_system::deadline_expired:
			return "Deadline expired";
	}

	return "Invalid error code";
}

/** Returns the node added. */
std::shared_ptr<nano::node> nano::system::add_node (nano::node_config const & node_config_a, nano::transport::transport_type type_a)
{
	auto node (std::make_shared<nano::node> (env, node_config_a));
	node->node_seq = node_sequence++;
	debug_assert (!node->init_error ());
	node->start ();
	node->wallets.create (nano::random_wallet_id ());
	nodes.reserve (nodes.size () + 1);
	nodes.push_back (node);
	if (nodes.size () > 1)
	{
		debug_assert (nodes.size () - 1 <= node->env.constants.node.max_peers_per_ip || node->config.flags.disable_max_peers_per_ip); // Check that we don't start more nodes than limit for single IP address
		auto begin = nodes.end () - 2;
		for (auto i (begin), j (begin + 1), n (nodes.end ()); j != n; ++i, ++j)
		{
			auto node1 (*i);
			auto node2 (*j);
			auto starting1 (node1->network.size ());
			size_t starting_listener1 (node1->bootstrap.realtime_count);
			decltype (starting1) new1;
			auto starting2 (node2->network.size ());
			size_t starting_listener2 (node2->bootstrap.realtime_count);
			decltype (starting2) new2;
			if (type_a == nano::transport::transport_type::tcp)
			{
				(*j)->network.merge_peer ((*i)->network.endpoint ());
			}
			else
			{
				// UDP connection
				auto channel (std::make_shared<nano::transport::channel_udp> ((*j)->network.udp_channels, (*i)->network.endpoint (), node1->env.constants.protocol.protocol_version));
				(*j)->network.send_keepalive (channel);
			}
			do
			{
				poll ();
				new1 = node1->network.size ();
				new2 = node2->network.size ();
			} while (new1 == starting1 || new2 == starting2);
			if (type_a == nano::transport::transport_type::tcp && node_config_a.tcp_incoming_connections_max != 0 && !node_config_a.flags.disable_tcp_realtime)
			{
				// Wait for initial connection finish
				decltype (starting_listener1) new_listener1;
				decltype (starting_listener2) new_listener2;
				do
				{
					poll ();
					new_listener1 = node1->bootstrap.realtime_count;
					new_listener2 = node2->bootstrap.realtime_count;
				} while (new_listener1 == starting_listener1 || new_listener2 == starting_listener2);
			}
		}
		auto iterations1 (0);
		while (std::any_of (begin, nodes.end (), [](std::shared_ptr<nano::node> const & node_a) { return node_a->bootstrap_initiator.in_progress (); }))
		{
			poll ();
			++iterations1;
			debug_assert (iterations1 < 10000);
		}
	}
	else
	{
		auto iterations1 (0);
		while (node->bootstrap_initiator.in_progress ())
		{
			poll ();
			++iterations1;
			debug_assert (iterations1 < 10000);
		}
	}

	return node;
}

nano::system::system () :
env_impl{ std::make_shared<nano::environment> () },
env (*env_impl)
{
	auto scale_str = std::getenv ("DEADLINE_SCALE_FACTOR");
	if (scale_str)
	{
		deadline_scaling_factor = std::stod (scale_str);
	}
}

nano::system::system (uint16_t count_a, nano::transport::transport_type type_a, nano::node_config config_a) :
system ()
{
	nodes.reserve (count_a);
	for (uint16_t i (0); i < count_a; ++i)
	{
		add_node (config_a, type_a);
	}
}

nano::system::~system ()
{
	for (auto & i : nodes)
	{
		i->stop ();
	}
#ifndef _WIN32
	// Windows cannot remove the log and data files while they are still owned by this process.
	// They will be removed later

	// Clean up tmp directories created by the tests. Since it's sometimes useful to
	// see log files after test failures, an environment variable is supported to
	// retain the files.
	if (std::getenv ("TEST_KEEP_TMPDIRS") == nullptr)
	{
		nano::remove_temporary_directories ();
	}
#endif
}

std::shared_ptr<nano::wallet> nano::system::wallet (size_t index_a)
{
	debug_assert (nodes.size () > index_a);
	auto size (nodes[index_a]->wallets.items.size ());
	(void)size;
	debug_assert (size == 1);
	return nodes[index_a]->wallets.items.begin ()->second;
}

nano::account nano::system::account (nano::transaction const & transaction_a, size_t index_a)
{
	auto wallet_l (wallet (index_a));
	auto keys (wallet_l->store.begin (transaction_a));
	debug_assert (keys != wallet_l->store.end ());
	auto result (keys->first);
	debug_assert (++keys == wallet_l->store.end ());
	return nano::account (result);
}

uint64_t nano::system::work_generate_limited (nano::block_hash const & root_a, uint64_t min_a, uint64_t max_a)
{
	debug_assert (min_a > 0);
	uint64_t result = 0;
	do
	{
		result = *env.work.generate (root_a, min_a);
	} while (nano::work_difficulty (nano::work_version::work_1, root_a, result) >= max_a);
	return result;
}

std::unique_ptr<nano::state_block> nano::upgrade_epoch (nano::work_pool & pool_a, nano::ledger & ledger_a, nano::epoch epoch_a)
{
	auto transaction (ledger_a.store.tx_begin_write ());
	auto dev_genesis_key = nano::ledger_constants (nano::nano_networks::nano_dev_network).dev_genesis_key;
	auto account = dev_genesis_key.pub;
	auto latest = ledger_a.latest (transaction, account);
	auto balance = ledger_a.account_balance (transaction, account);

	nano::state_block_builder builder;
	std::error_code ec;
	auto epoch = builder
	             .account (dev_genesis_key.pub)
	             .previous (latest)
	             .balance (balance)
	             .link (ledger_a.epoch_link (epoch_a))
	             .representative (dev_genesis_key.pub)
	             .sign (dev_genesis_key.prv, dev_genesis_key.pub)
	             .work (*pool_a.generate (latest, nano::work_threshold (nano::work_version::work_1, nano::block_details (epoch_a, false, false, true))))
	             .build (ec);

	bool error{ true };
	if (!ec && epoch)
	{
		error = ledger_a.process (transaction, *epoch).code != nano::process_result::progress;
	}

	return !error ? std::move (epoch) : nullptr;
}

void nano::blocks_confirm (nano::node & node_a, std::vector<std::shared_ptr<nano::block>> const & blocks_a)
{
	// Finish processing all blocks
	node_a.block_processor.flush ();
	for (auto const & block : blocks_a)
	{
		// A sideband is required to start an election
		debug_assert (block->has_sideband ());
		node_a.block_confirm (block);
	}
}

std::unique_ptr<nano::state_block> nano::system::upgrade_genesis_epoch (nano::node & node_a, nano::epoch const epoch_a)
{
	return upgrade_epoch (env.work, node_a.ledger, epoch_a);
}

void nano::system::deadline_set (std::chrono::duration<double, std::nano> const & delta_a)
{
	deadline = std::chrono::steady_clock::now () + delta_a * deadline_scaling_factor;
}

std::error_code nano::system::poll (std::chrono::nanoseconds const & wait_time)
{
#if NANO_ASIO_HANDLER_TRACKING == 0
	env.ctx.run_one_for (wait_time);
#else
	nano::timer<> timer;
	timer.start ();
	auto count = io_ctx.poll_one ();
	if (count == 0)
	{
		std::this_thread::sleep_for (wait_time);
	}
	else if (count == 1 && timer.since_start ().count () >= NANO_ASIO_HANDLER_TRACKING)
	{
		auto timestamp = std::chrono::duration_cast<std::chrono::microseconds> (std::chrono::system_clock::now ().time_since_epoch ()).count ();
		std::cout << (boost::format ("[%1%] io_thread held for %2%ms") % timestamp % timer.since_start ().count ()).str () << std::endl;
	}
#endif

	std::error_code ec;
	if (std::chrono::steady_clock::now () > deadline)
	{
		ec = nano::error_system::deadline_expired;
		stop ();
	}
	return ec;
}

std::error_code nano::system::poll_until_true (std::chrono::nanoseconds deadline_a, std::function<bool()> predicate_a)
{
	std::error_code ec;
	deadline_set (deadline_a);
	while (!ec && !predicate_a ())
	{
		ec = poll ();
	}
	return ec;
}

namespace
{
class traffic_generator : public std::enable_shared_from_this<traffic_generator>
{
public:
	traffic_generator (uint32_t count_a, uint32_t wait_a, std::shared_ptr<nano::node> const & node_a, nano::system & system_a) :
	count (count_a),
	wait (wait_a),
	node (node_a),
	system (system_a)
	{
	}
	void run ()
	{
		auto count_l (count - 1);
		count = count_l - 1;
		system.generate_activity (*node, accounts);
		if (count_l > 0)
		{
			auto this_l (shared_from_this ());
			node->env.alarm.add (std::chrono::steady_clock::now () + std::chrono::milliseconds (wait), [this_l]() { this_l->run (); });
		}
	}
	std::vector<nano::account> accounts;
	uint32_t count;
	uint32_t wait;
	std::shared_ptr<nano::node> node;
	nano::system & system;
};
}

void nano::system::generate_usage_traffic (uint32_t count_a, uint32_t wait_a)
{
	for (size_t i (0), n (nodes.size ()); i != n; ++i)
	{
		generate_usage_traffic (count_a, wait_a, i);
	}
}

void nano::system::generate_usage_traffic (uint32_t count_a, uint32_t wait_a, size_t index_a)
{
	debug_assert (nodes.size () > index_a);
	debug_assert (count_a > 0);
	auto generate (std::make_shared<traffic_generator> (count_a, wait_a, nodes[index_a], *this));
	generate->run ();
}

void nano::system::generate_rollback (nano::node & node_a, std::vector<nano::account> & accounts_a)
{
	auto transaction (node_a.store.tx_begin_write ());
	debug_assert (std::numeric_limits<CryptoPP::word32>::max () > accounts_a.size ());
	auto index (random_pool::generate_word32 (0, static_cast<CryptoPP::word32> (accounts_a.size () - 1)));
	auto account (accounts_a[index]);
	nano::account_info info;
	auto error (node_a.store.account_get (transaction, account, info));
	if (!error)
	{
		auto hash (info.open_block);
		nano::genesis genesis;
		if (hash != genesis.hash ())
		{
			accounts_a[index] = accounts_a[accounts_a.size () - 1];
			accounts_a.pop_back ();
			std::vector<std::shared_ptr<nano::block>> rollback_list;
			auto error = node_a.ledger.rollback (transaction, hash, rollback_list);
			(void)error;
			debug_assert (!error);
			for (auto & i : rollback_list)
			{
				node_a.wallets.watcher->remove (*i);
				node_a.active.erase (*i);
			}
		}
	}
}

void nano::system::generate_receive (nano::node & node_a)
{
	std::shared_ptr<nano::block> send_block;
	{
		auto transaction (node_a.store.tx_begin_read ());
		nano::account random_account;
		random_pool::generate_block (random_account.bytes.data (), sizeof (random_account.bytes));
		auto i (node_a.store.pending_begin (transaction, nano::pending_key (random_account, 0)));
		if (i != node_a.store.pending_end ())
		{
			nano::pending_key const & send_hash (i->first);
			send_block = node_a.store.block_get (transaction, send_hash.hash);
		}
	}
	if (send_block != nullptr)
	{
		auto receive_error (wallet (0)->receive_sync (send_block, nano::ledger_constants (nano::nano_networks::nano_dev_network).genesis_account, std::numeric_limits<nano::uint128_t>::max ()));
		(void)receive_error;
	}
}

void nano::system::generate_activity (nano::node & node_a, std::vector<nano::account> & accounts_a)
{
	auto what (random_pool::generate_byte ());
	if (what < 0x1)
	{
		generate_rollback (node_a, accounts_a);
	}
	else if (what < 0x10)
	{
		generate_change_known (node_a, accounts_a);
	}
	else if (what < 0x20)
	{
		generate_change_unknown (node_a, accounts_a);
	}
	else if (what < 0x70)
	{
		generate_receive (node_a);
	}
	else if (what < 0xc0)
	{
		generate_send_existing (node_a, accounts_a);
	}
	else
	{
		generate_send_new (node_a, accounts_a);
	}
}

nano::account nano::system::get_random_account (std::vector<nano::account> & accounts_a)
{
	debug_assert (std::numeric_limits<CryptoPP::word32>::max () > accounts_a.size ());
	auto index (random_pool::generate_word32 (0, static_cast<CryptoPP::word32> (accounts_a.size () - 1)));
	auto result (accounts_a[index]);
	return result;
}

nano::uint128_t nano::system::get_random_amount (nano::transaction const & transaction_a, nano::node & node_a, nano::account const & account_a)
{
	nano::uint128_t balance (node_a.ledger.account_balance (transaction_a, account_a));
	nano::uint128_union random_amount;
	nano::random_pool::generate_block (random_amount.bytes.data (), sizeof (random_amount.bytes));
	return (((nano::uint256_t{ random_amount.number () } * balance) / nano::uint256_t{ std::numeric_limits<nano::uint128_t>::max () }).convert_to<nano::uint128_t> ());
}

void nano::system::generate_send_existing (nano::node & node_a, std::vector<nano::account> & accounts_a)
{
	nano::uint128_t amount;
	nano::account destination;
	nano::account source;
	{
		nano::account account;
		random_pool::generate_block (account.bytes.data (), sizeof (account.bytes));
		auto transaction (node_a.store.tx_begin_read ());
		nano::store_iterator<nano::account, nano::account_info> entry (node_a.store.latest_begin (transaction, account));
		if (entry == node_a.store.latest_end ())
		{
			entry = node_a.store.latest_begin (transaction);
		}
		debug_assert (entry != node_a.store.latest_end ());
		destination = nano::account (entry->first);
		source = get_random_account (accounts_a);
		amount = get_random_amount (transaction, node_a, source);
	}
	if (!amount.is_zero ())
	{
		auto hash (wallet (0)->send_sync (source, destination, amount));
		(void)hash;
		debug_assert (!hash.is_zero ());
	}
}

void nano::system::generate_change_known (nano::node & node_a, std::vector<nano::account> & accounts_a)
{
	nano::account source (get_random_account (accounts_a));
	if (!node_a.latest (source).is_zero ())
	{
		nano::account destination (get_random_account (accounts_a));
		auto change_error (wallet (0)->change_sync (source, destination));
		(void)change_error;
		debug_assert (!change_error);
	}
}

void nano::system::generate_change_unknown (nano::node & node_a, std::vector<nano::account> & accounts_a)
{
	nano::account source (get_random_account (accounts_a));
	if (!node_a.latest (source).is_zero ())
	{
		nano::keypair key;
		nano::account destination (key.pub);
		auto change_error (wallet (0)->change_sync (source, destination));
		(void)change_error;
		debug_assert (!change_error);
	}
}

void nano::system::generate_send_new (nano::node & node_a, std::vector<nano::account> & accounts_a)
{
	debug_assert (node_a.wallets.items.size () == 1);
	nano::uint128_t amount;
	nano::account source;
	{
		auto transaction (node_a.store.tx_begin_read ());
		source = get_random_account (accounts_a);
		amount = get_random_amount (transaction, node_a, source);
	}
	if (!amount.is_zero ())
	{
		auto pub (node_a.wallets.items.begin ()->second->deterministic_insert ());
		accounts_a.push_back (pub);
		auto hash (wallet (0)->send_sync (source, pub, amount));
		(void)hash;
		debug_assert (!hash.is_zero ());
	}
}

void nano::system::generate_mass_activity (uint32_t count_a, nano::node & node_a)
{
	std::vector<nano::account> accounts;
	auto dev_genesis_key = nano::ledger_constants (nano::nano_networks::nano_dev_network).dev_genesis_key;
	wallet (0)->insert_adhoc (dev_genesis_key.prv);
	accounts.push_back (dev_genesis_key.pub);
	auto previous (std::chrono::steady_clock::now ());
	for (uint32_t i (0); i < count_a; ++i)
	{
		if ((i & 0xff) == 0)
		{
			auto now (std::chrono::steady_clock::now ());
			auto us (std::chrono::duration_cast<std::chrono::microseconds> (now - previous).count ());
			auto count = node_a.ledger.cache.block_count.load ();
			std::cerr << boost::str (boost::format ("Mass activity iteration %1% us %2% us/t %3% block count: %4%\n") % i % us % (us / 256) % count);
			previous = now;
		}
		generate_activity (node_a, accounts);
	}
}

void nano::system::stop ()
{
	for (auto i : nodes)
	{
		i->stop ();
	}
	env.work.stop ();
}

uint16_t nano::get_available_port ()
{
	// Maximum possible sockets which may feasibly be used in 1 test
	constexpr auto max = 200;
	static uint16_t current = 0;
	// Read the TEST_BASE_PORT environment and override the default base port if it exists
	auto base_str = std::getenv ("TEST_BASE_PORT");
	uint16_t base_port = 24000;
	if (base_str)
	{
		base_port = boost::lexical_cast<uint16_t> (base_str);
	}

	uint16_t const available_port = base_port + current;
	++current;
	// Reset port number once we have reached the maximum
	if (current == max)
	{
		current = 0;
	}

	return available_port;
}

void nano::cleanup_dev_directories_on_exit ()
{
	// Makes sure everything is cleaned up
	nano::logging::release_file_sink ();
	// Clean up tmp directories created by the tests. Since it's sometimes useful to
	// see log files after test failures, an environment variable is supported to
	// retain the files.
	if (std::getenv ("TEST_KEEP_TMPDIRS") == nullptr)
	{
		nano::remove_temporary_directories ();
	}
}

bool nano::using_rocksdb_in_tests ()
{
	static nano::network_constants network_constants;
	auto use_rocksdb_str = std::getenv ("TEST_USE_ROCKSDB");
	return network_constants.is_dev_network () && use_rocksdb_str && (boost::lexical_cast<int> (use_rocksdb_str) == 1);
}
