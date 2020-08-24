#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/threading.hpp>
#include <nano/node/election.hpp>
#include <nano/node/testing.hpp>
#include <nano/node/transport/udp.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

#include <boost/format.hpp>

#include <numeric>
#include <random>

using namespace std::chrono_literals;

TEST (system, generate_mass_activity)
{
	nano::system system;
	nano::node_config node_config;
	node_config.enable_voting = false; // Prevent blocks cementing
	auto node = system.add_node (node_config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	uint32_t count (20);
	system.generate_mass_activity (count, *system.nodes[0]);
	auto transaction (system.nodes[0]->store.tx_begin_read ());
	for (auto i (system.nodes[0]->store.latest_begin (transaction)), n (system.nodes[0]->store.latest_end ()); i != n; ++i)
	{
	}
}

TEST (system, generate_mass_activity_long)
{
	nano::system system;
	nano::node_config node_config;
	node_config.enable_voting = false; // Prevent blocks cementing
	auto node = system.add_node (node_config);
	system.wallet (0)->wallets.watcher->stop (); // Stop work watcher
	nano::thread_runner runner (system.env.ctx, system.nodes[0]->config.io_threads);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	uint32_t count (1000000000);
	system.generate_mass_activity (count, *system.nodes[0]);
	auto transaction (system.nodes[0]->store.tx_begin_read ());
	for (auto i (system.nodes[0]->store.latest_begin (transaction)), n (system.nodes[0]->store.latest_end ()); i != n; ++i)
	{
	}
	system.stop ();
	runner.join ();
}

TEST (system, receive_while_synchronizing)
{
	std::vector<boost::thread> threads;
	{
		nano::system system;
		nano::node_config node_config;
		node_config.enable_voting = false; // Prevent blocks cementing
		auto node = system.add_node (node_config);
		nano::thread_runner runner (system.env.ctx, system.nodes[0]->config.io_threads);
		system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
		uint32_t count (1000);
		system.generate_mass_activity (count, *system.nodes[0]);
		nano::keypair key;
		auto node1 (std::make_shared<nano::node> (system.env, nano::node_config{}));
		ASSERT_FALSE (node1->init_error ());
		auto channel (std::make_shared<nano::transport::channel_udp> (node1->network.udp_channels, system.nodes[0]->network.endpoint (), node1->env.constants.protocol.protocol_version));
		node1->network.send_keepalive (channel);
		auto wallet (node1->wallets.create (1));
		wallet->insert_adhoc (nano::dev_genesis_key.prv); // For voting
		ASSERT_EQ (key.pub, wallet->insert_adhoc (key.prv));
		node1->start ();
		system.nodes.push_back (node1);
		system.env.alarm.add (std::chrono::steady_clock::now () + std::chrono::milliseconds (200), ([&system, &key]() {
			auto hash (system.wallet (0)->send_sync (nano::dev_genesis_key.pub, key.pub, system.nodes[0]->config.receive_minimum.number ()));
			auto transaction (system.nodes[0]->store.tx_begin_read ());
			auto block (system.nodes[0]->store.block_get (transaction, hash));
			std::string block_text;
			block->serialize_json (block_text);
		}));
		ASSERT_TIMELY (10s, !node1->balance (key.pub).is_zero ());
		node1->stop ();
		system.stop ();
		runner.join ();
	}
	for (auto i (threads.begin ()), n (threads.end ()); i != n; ++i)
	{
		i->join ();
	}
}

TEST (ledger, deep_account_compute)
{
	nano::logger_mt logger;
	auto store = nano::make_store (logger, nano::unique_path ());
	ASSERT_FALSE (store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::genesis genesis;
	auto transaction (store->tx_begin_write ());
	store->initialize (transaction, genesis, ledger.cache);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key;
	auto balance (nano::genesis_amount - 1);
	nano::send_block send (genesis.hash (), key.pub, balance, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *pool.generate (genesis.hash ()));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
	nano::open_block open (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, *pool.generate (key.pub));
	ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, open).code);
	auto sprevious (send.hash ());
	auto rprevious (open.hash ());
	for (auto i (0), n (100000); i != n; ++i)
	{
		balance -= 1;
		nano::send_block send (sprevious, key.pub, balance, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *pool.generate (sprevious));
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
		sprevious = send.hash ();
		nano::receive_block receive (rprevious, send.hash (), key.prv, key.pub, *pool.generate (rprevious));
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, receive).code);
		rprevious = receive.hash ();
		if (i % 100 == 0)
		{
			std::cerr << i << ' ';
		}
		ledger.account (transaction, sprevious);
		ledger.balance (transaction, rprevious);
	}
}

TEST (wallet, multithreaded_send_async)
{
	std::vector<boost::thread> threads;
	{
		nano::system system (1);
		nano::keypair key;
		auto wallet_l (system.wallet (0));
		wallet_l->insert_adhoc (nano::dev_genesis_key.prv);
		wallet_l->insert_adhoc (key.prv);
		for (auto i (0); i < 20; ++i)
		{
			threads.push_back (boost::thread ([wallet_l, &key]() {
				for (auto i (0); i < 1000; ++i)
				{
					wallet_l->send_async (nano::dev_genesis_key.pub, key.pub, 1000, [](std::shared_ptr<nano::block> block_a) {
						ASSERT_FALSE (block_a == nullptr);
						ASSERT_FALSE (block_a->hash ().is_zero ());
					});
				}
			}));
		}
		ASSERT_TIMELY (1000s, system.nodes[0]->balance (nano::dev_genesis_key.pub) == (nano::genesis_amount - 20 * 1000 * 1000));
	}
	for (auto i (threads.begin ()), n (threads.end ()); i != n; ++i)
	{
		i->join ();
	}
}

TEST (store, load)
{
	nano::system system (1);
	std::vector<boost::thread> threads;
	for (auto i (0); i < 100; ++i)
	{
		threads.push_back (boost::thread ([&system]() {
			for (auto i (0); i != 1000; ++i)
			{
				auto transaction (system.nodes[0]->store.tx_begin_write ());
				for (auto j (0); j != 10; ++j)
				{
					nano::account account;
					nano::random_pool::generate_block (account.bytes.data (), account.bytes.size ());
					system.nodes[0]->store.confirmation_height_put (transaction, account, { 0, nano::block_hash (0) });
					system.nodes[0]->store.account_put (transaction, account, nano::account_info ());
				}
			}
		}));
	}
	for (auto & i : threads)
	{
		i.join ();
	}
}

// ulimit -n increasing may be required
TEST (node, fork_storm)
{
	nano::node_config config;
	config.flags.disable_max_peers_per_ip = true;
	nano::system system (64, nano::transport::transport_type::tcp, config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto previous (system.nodes[0]->latest (nano::dev_genesis_key.pub));
	auto balance (system.nodes[0]->balance (nano::dev_genesis_key.pub));
	ASSERT_FALSE (previous.is_zero ());
	for (auto j (0); j != system.nodes.size (); ++j)
	{
		balance -= 1;
		nano::keypair key;
		nano::send_block send (previous, key.pub, balance, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, 0);
		system.nodes[j]->work_generate_blocking (send);
		previous = send.hash ();
		for (auto i (0); i != system.nodes.size (); ++i)
		{
			auto send_result (system.nodes[i]->process (send));
			ASSERT_EQ (nano::process_result::progress, send_result.code);
			nano::keypair rep;
			auto open (std::make_shared<nano::open_block> (previous, rep.pub, key.pub, key.prv, key.pub, 0));
			system.nodes[i]->work_generate_blocking (*open);
			auto open_result (system.nodes[i]->process (*open));
			ASSERT_EQ (nano::process_result::progress, open_result.code);
			auto transaction (system.nodes[i]->store.tx_begin_read ());
			system.nodes[i]->network.flood_block (open);
		}
	}
	auto again (true);

	int iteration (0);
	while (again)
	{
		auto empty = 0;
		auto single = 0;
		std::for_each (system.nodes.begin (), system.nodes.end (), [&](std::shared_ptr<nano::node> const & node_a) {
			if (node_a->active.empty ())
			{
				++empty;
			}
			else
			{
				nano::lock_guard<std::mutex> lock (node_a->active.mutex);
				if (node_a->active.roots.begin ()->election->last_votes_size () == 1)
				{
					++single;
				}
			}
		});
		ASSERT_NO_ERROR (system.poll ());
		if ((iteration & 0xff) == 0)
		{
			std::cerr << "Empty: " << empty << " single: " << single << std::endl;
		}
		again = empty != 0 || single != 0;
		++iteration;
	}
	ASSERT_TRUE (true);
}

namespace
{
size_t heard_count (std::vector<uint8_t> const & nodes)
{
	auto result (0);
	for (auto i (nodes.begin ()), n (nodes.end ()); i != n; ++i)
	{
		switch (*i)
		{
			case 0:
				break;
			case 1:
				++result;
				break;
			case 2:
				++result;
				break;
		}
	}
	return result;
}
}

TEST (broadcast, world_broadcast_simulate)
{
	auto node_count (10000);
	// 0 = starting state
	// 1 = heard transaction
	// 2 = repeated transaction
	std::vector<uint8_t> nodes;
	nodes.resize (node_count, 0);
	nodes[0] = 1;
	auto any_changed (true);
	auto message_count (0);
	while (any_changed)
	{
		any_changed = false;
		for (auto i (nodes.begin ()), n (nodes.end ()); i != n; ++i)
		{
			switch (*i)
			{
				case 0:
					break;
				case 1:
					for (auto j (nodes.begin ()), m (nodes.end ()); j != m; ++j)
					{
						++message_count;
						switch (*j)
						{
							case 0:
								*j = 1;
								any_changed = true;
								break;
							case 1:
								break;
							case 2:
								break;
						}
					}
					*i = 2;
					any_changed = true;
					break;
				case 2:
					break;
				default:
					ASSERT_FALSE (true);
					break;
			}
		}
	}
	auto count (heard_count (nodes));
	(void)count;
}

TEST (broadcast, sqrt_broadcast_simulate)
{
	auto node_count (10000);
	auto broadcast_count (std::ceil (std::sqrt (node_count)));
	// 0 = starting state
	// 1 = heard transaction
	// 2 = repeated transaction
	std::vector<uint8_t> nodes;
	nodes.resize (node_count, 0);
	nodes[0] = 1;
	auto any_changed (true);
	uint64_t message_count (0);
	while (any_changed)
	{
		any_changed = false;
		for (auto i (nodes.begin ()), n (nodes.end ()); i != n; ++i)
		{
			switch (*i)
			{
				case 0:
					break;
				case 1:
					for (auto j (0); j != broadcast_count; ++j)
					{
						++message_count;
						auto entry (nano::random_pool::generate_word32 (0, node_count - 1));
						switch (nodes[entry])
						{
							case 0:
								nodes[entry] = 1;
								any_changed = true;
								break;
							case 1:
								break;
							case 2:
								break;
						}
					}
					*i = 2;
					any_changed = true;
					break;
				case 2:
					break;
				default:
					ASSERT_FALSE (true);
					break;
			}
		}
	}
	auto count (heard_count (nodes));
	(void)count;
}

TEST (peer_container, random_set)
{
	nano::system system (1);
	auto old (std::chrono::steady_clock::now ());
	auto current (std::chrono::steady_clock::now ());
	for (auto i (0); i < 10000; ++i)
	{
		auto list (system.nodes[0]->network.random_set (15));
	}
	auto end (std::chrono::steady_clock::now ());
	(void)end;
	auto old_ms (std::chrono::duration_cast<std::chrono::milliseconds> (current - old));
	(void)old_ms;
	auto new_ms (std::chrono::duration_cast<std::chrono::milliseconds> (end - current));
	(void)new_ms;
}

// Can take up to 2 hours
TEST (store, unchecked_load)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	auto block (std::make_shared<nano::send_block> (0, 0, 0, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, 0));
	constexpr auto num_unchecked = 1000000;
	for (auto i (0); i < 1000000; ++i)
	{
		auto transaction (node.store.tx_begin_write ());
		node.store.unchecked_put (transaction, i, block);
	}
	auto transaction (node.store.tx_begin_read ());
	ASSERT_EQ (num_unchecked, node.store.unchecked_count (transaction));
}

TEST (store, vote_load)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	auto block (std::make_shared<nano::send_block> (0, 0, 0, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, 0));
	for (auto i (0); i < 1000000; ++i)
	{
		auto vote (std::make_shared<nano::vote> (nano::dev_genesis_key.pub, nano::dev_genesis_key.prv, i, block));
		node.vote_processor.vote (vote, std::make_shared<nano::transport::channel_udp> (system.nodes[0]->network.udp_channels, system.nodes[0]->network.endpoint (), system.nodes[0]->env.constants.protocol.protocol_version));
	}
}

TEST (wallets, rep_scan)
{
	nano::system system (1);
	auto & node (*system.nodes[0]);
	auto wallet (system.wallet (0));
	{
		auto transaction (node.wallets.tx_begin_write ());
		for (auto i (0); i < 10000; ++i)
		{
			wallet->deterministic_insert (transaction);
		}
	}
	auto begin (std::chrono::steady_clock::now ());
	node.wallets.foreach_representative ([](nano::public_key const & pub_a, nano::raw_key const & prv_a) {
	});
	ASSERT_LT (std::chrono::steady_clock::now () - begin, std::chrono::milliseconds (5));
}

TEST (node, mass_vote_by_hash)
{
	nano::system system (1);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::block_hash previous (nano::genesis_hash);
	nano::keypair key;
	std::vector<std::shared_ptr<nano::state_block>> blocks;
	for (auto i (0); i < 10000; ++i)
	{
		auto block (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, previous, nano::dev_genesis_key.pub, nano::genesis_amount - (i + 1) * nano::Gxrb_ratio, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (previous)));
		previous = block->hash ();
		blocks.push_back (block);
	}
	for (auto i (blocks.begin ()), n (blocks.end ()); i != n; ++i)
	{
		system.nodes[0]->block_processor.add (*i, nano::seconds_since_epoch ());
	}
}

namespace nano
{
TEST (confirmation_height, many_accounts_single_confirmation)
{
	nano::system system;
	nano::node_config node_config;
	node_config.online_weight_minimum = 100;
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto node = system.add_node (node_config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);

	// The number of frontiers should be more than the nano::confirmation_height::unbounded_cutoff to test the amount of blocks confirmed is correct.
	node->confirmation_height_processor.batch_write_size = 500;
	auto const num_accounts = nano::confirmation_height::unbounded_cutoff * 2 + 50;
	nano::keypair last_keypair = nano::dev_genesis_key;
	auto last_open_hash = node->latest (nano::dev_genesis_key.pub);
	{
		auto transaction = node->store.tx_begin_write ();
		for (auto i = num_accounts - 1; i > 0; --i)
		{
			nano::keypair key;
			system.wallet (0)->insert_adhoc (key.prv);

			nano::send_block send (last_open_hash, key.pub, node->config.online_weight_minimum.number (), last_keypair.prv, last_keypair.pub, *system.env.work.generate (last_open_hash));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
			nano::open_block open (send.hash (), last_keypair.pub, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open).code);
			last_open_hash = open.hash ();
			last_keypair = key;
		}
	}

	// Call block confirm on the last open block which will confirm everything
	{
		auto block = node->block (last_open_hash);
		ASSERT_NE (nullptr, block);
		auto election_insertion_result (node->active.insert (block));
		ASSERT_TRUE (election_insertion_result.inserted);
		ASSERT_NE (nullptr, election_insertion_result.election);
		nano::lock_guard<std::mutex> guard (node->active.mutex);
		election_insertion_result.election->confirm_once ();
	}

	ASSERT_TIMELY (120s, node->ledger.block_confirmed (node->store.tx_begin_read (), last_open_hash));

	// All frontiers (except last) should have 2 blocks and both should be confirmed
	auto transaction = node->store.tx_begin_read ();
	for (auto i (node->store.latest_begin (transaction)), n (node->store.latest_end ()); i != n; ++i)
	{
		auto & account = i->first;
		auto & account_info = i->second;
		auto count = (account != last_keypair.pub) ? 2 : 1;
		nano::confirmation_height_info confirmation_height_info;
		ASSERT_FALSE (node->store.confirmation_height_get (transaction, account, confirmation_height_info));
		ASSERT_EQ (count, confirmation_height_info.height);
		ASSERT_EQ (count, account_info.block_count);
	}

	auto cemented_count = 0;
	for (auto i (node->ledger.store.confirmation_height_begin (transaction)), n (node->ledger.store.confirmation_height_end ()); i != n; ++i)
	{
		cemented_count += i->second.height;
	}

	ASSERT_EQ (cemented_count, node->ledger.cache.cemented_count);
	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in), num_accounts * 2 - 2);
	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_bounded, nano::stat::dir::in), num_accounts * 2 - 2);
	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_unbounded, nano::stat::dir::in), 0);

	ASSERT_TIMELY (40s, (node->ledger.cache.cemented_count - 1) == node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::all, nano::stat::dir::out));
	ASSERT_TIMELY (10s, node->active.election_winner_details_size () == 0);
}

TEST (confirmation_height, many_accounts_many_confirmations)
{
	nano::system system;
	nano::node_config node_config;
	node_config.online_weight_minimum = 100;
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto node = system.add_node (node_config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);

	node->confirmation_height_processor.batch_write_size = 500;
	auto const num_accounts = nano::confirmation_height::unbounded_cutoff * 2 + 50;
	auto ladev_genesis = node->latest (nano::dev_genesis_key.pub);
	std::vector<std::shared_ptr<nano::open_block>> open_blocks;
	{
		auto transaction = node->store.tx_begin_write ();
		for (auto i = num_accounts - 1; i > 0; --i)
		{
			nano::keypair key;
			system.wallet (0)->insert_adhoc (key.prv);

			nano::send_block send (ladev_genesis, key.pub, node->config.online_weight_minimum.number (), nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (ladev_genesis));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
			auto open = std::make_shared<nano::open_block> (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *open).code);
			open_blocks.push_back (std::move (open));
			ladev_genesis = send.hash ();
		}
	}

	// Confirm all of the accounts
	for (auto & open_block : open_blocks)
	{
		auto election_insertion_result (node->active.insert (open_block));
		ASSERT_TRUE (election_insertion_result.inserted);
		ASSERT_NE (nullptr, election_insertion_result.election);
		nano::lock_guard<std::mutex> guard (node->active.mutex);
		election_insertion_result.election->confirm_once ();
	}

	auto const num_blocks_to_confirm = (num_accounts - 1) * 2;
	ASSERT_TIMELY (1500s, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in) == num_blocks_to_confirm);

	auto num_confirmed_bounded = node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_bounded, nano::stat::dir::in);
	ASSERT_GE (num_confirmed_bounded, nano::confirmation_height::unbounded_cutoff);
	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_unbounded, nano::stat::dir::in), num_blocks_to_confirm - num_confirmed_bounded);

	ASSERT_TIMELY (60s, (node->ledger.cache.cemented_count - 1) == node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::all, nano::stat::dir::out));

	auto transaction = node->store.tx_begin_read ();
	auto cemented_count = 0;
	for (auto i (node->ledger.store.confirmation_height_begin (transaction)), n (node->ledger.store.confirmation_height_end ()); i != n; ++i)
	{
		cemented_count += i->second.height;
	}

	ASSERT_EQ (num_blocks_to_confirm + 1, cemented_count);
	ASSERT_EQ (cemented_count, node->ledger.cache.cemented_count);

	ASSERT_TIMELY (20s, (node->ledger.cache.cemented_count - 1) == node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::all, nano::stat::dir::out));

	ASSERT_TIMELY (10s, node->active.election_winner_details_size () == 0);
}

TEST (confirmation_height, long_chains)
{
	nano::system system;
	nano::node_config node_config;
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto node = system.add_node (node_config);
	nano::keypair key1;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	nano::block_hash latest (node->latest (nano::dev_genesis_key.pub));
	system.wallet (0)->insert_adhoc (key1.prv);

	node->confirmation_height_processor.batch_write_size = 500;
	auto const num_blocks = nano::confirmation_height::unbounded_cutoff * 2 + 50;

	// First open the other account
	nano::send_block send (latest, key1.pub, nano::genesis_amount - nano::Gxrb_ratio + num_blocks + 1, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (latest));
	nano::open_block open (send.hash (), nano::genesis_account, key1.pub, key1.prv, key1.pub, *system.env.work.generate (key1.pub));
	{
		auto transaction = node->store.tx_begin_write ();
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open).code);
	}

	// Bulk send from genesis account to destination account
	auto previous_genesis_chain_hash = send.hash ();
	auto previous_destination_chain_hash = open.hash ();
	{
		auto transaction = node->store.tx_begin_write ();
		for (auto i = num_blocks - 1; i > 0; --i)
		{
			nano::send_block send (previous_genesis_chain_hash, key1.pub, nano::genesis_amount - nano::Gxrb_ratio + i + 1, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (previous_genesis_chain_hash));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
			nano::receive_block receive (previous_destination_chain_hash, send.hash (), key1.prv, key1.pub, *system.env.work.generate (previous_destination_chain_hash));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive).code);

			previous_genesis_chain_hash = send.hash ();
			previous_destination_chain_hash = receive.hash ();
		}
	}

	// Send one from destination to genesis and pocket it
	nano::send_block send1 (previous_destination_chain_hash, nano::dev_genesis_key.pub, nano::Gxrb_ratio - 2, key1.prv, key1.pub, *system.env.work.generate (previous_destination_chain_hash));
	auto receive1 (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, previous_genesis_chain_hash, nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio + 1, send1.hash (), nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (previous_genesis_chain_hash)));

	// Unpocketed. Send to a non-existing account to prevent auto receives from the wallet adjusting expected confirmation height
	nano::keypair key2;
	nano::state_block send2 (nano::genesis_account, receive1->hash (), nano::genesis_account, nano::genesis_amount - nano::Gxrb_ratio, key2.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (receive1->hash ()));

	{
		auto transaction = node->store.tx_begin_write ();
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *receive1).code);
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send2).code);
	}

	// Call block confirm on the existing receive block on the genesis account which will confirm everything underneath on both accounts
	{
		auto election_insertion_result (node->active.insert (receive1));
		ASSERT_TRUE (election_insertion_result.inserted);
		ASSERT_NE (nullptr, election_insertion_result.election);
		nano::lock_guard<std::mutex> guard (node->active.mutex);
		election_insertion_result.election->confirm_once ();
	}

	ASSERT_TIMELY (30s, node->ledger.block_confirmed (node->store.tx_begin_read (), receive1->hash ()));

	auto transaction (node->store.tx_begin_read ());
	nano::account_info account_info;
	ASSERT_FALSE (node->store.account_get (transaction, nano::dev_genesis_key.pub, account_info));
	nano::confirmation_height_info confirmation_height_info;
	ASSERT_FALSE (node->store.confirmation_height_get (transaction, nano::dev_genesis_key.pub, confirmation_height_info));
	ASSERT_EQ (num_blocks + 2, confirmation_height_info.height);
	ASSERT_EQ (num_blocks + 3, account_info.block_count); // Includes the unpocketed send

	ASSERT_FALSE (node->store.account_get (transaction, key1.pub, account_info));
	ASSERT_FALSE (node->store.confirmation_height_get (transaction, key1.pub, confirmation_height_info));
	ASSERT_EQ (num_blocks + 1, confirmation_height_info.height);
	ASSERT_EQ (num_blocks + 1, account_info.block_count);

	auto cemented_count = 0;
	for (auto i (node->ledger.store.confirmation_height_begin (transaction)), n (node->ledger.store.confirmation_height_end ()); i != n; ++i)
	{
		cemented_count += i->second.height;
	}

	ASSERT_EQ (cemented_count, node->ledger.cache.cemented_count);
	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in), num_blocks * 2 + 2);
	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_bounded, nano::stat::dir::in), num_blocks * 2 + 2);
	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_unbounded, nano::stat::dir::in), 0);

	ASSERT_TIMELY (40s, (node->ledger.cache.cemented_count - 1) == node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::all, nano::stat::dir::out));
	ASSERT_TIMELY (10s, node->active.election_winner_details_size () == 0);
}

TEST (confirmation_height, dynamic_algorithm)
{
	nano::system system;
	nano::node_config node_config;
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto node = system.add_node (node_config);
	nano::genesis genesis;
	nano::keypair key;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	auto const num_blocks = nano::confirmation_height::unbounded_cutoff;
	auto ladev_genesis = node->latest (nano::dev_genesis_key.pub);
	std::vector<std::shared_ptr<nano::state_block>> state_blocks;
	for (auto i = 0; i < num_blocks; ++i)
	{
		auto send (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, ladev_genesis, nano::dev_genesis_key.pub, nano::genesis_amount - i - 1, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (ladev_genesis)));
		ladev_genesis = send->hash ();
		state_blocks.push_back (send);
	}
	{
		auto transaction = node->store.tx_begin_write ();
		for (auto const & block : state_blocks)
		{
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *block).code);
		}
	}

	node->confirmation_height_processor.add (state_blocks.front ()->hash ());
	ASSERT_TIMELY (20s, node->ledger.cache.cemented_count == 2);

	node->confirmation_height_processor.add (ladev_genesis);

	ASSERT_TIMELY (20s, node->ledger.cache.cemented_count == num_blocks + 1);

	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in), num_blocks);
	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_bounded, nano::stat::dir::in), 1);
	ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_unbounded, nano::stat::dir::in), num_blocks - 1);
	ASSERT_TIMELY (10s, node->active.election_winner_details_size () == 0);
}

/*
 * This tests an issue of incorrect cemented block counts during the transition of conf height algorithms
 * The scenario was as follows:
 *  - There is at least 1 pending write entry in the unbounded conf height processor
 *  - 0 blocks currently awaiting processing in the main conf height processor class
 *  - A block was confirmed when hit the chain in the pending write above but was not a block higher than it.
 *  - It must be in `confirmation_height_processor::pause ()` function so that `pause` is set (and the difference between the number
 *    of blocks uncemented is > unbounded_cutoff so that it hits the bounded processor), the main `run` loop on the conf height processor is iterated.
 *
 * This cause unbounded pending entries not to be written, and then the bounded processor would write them, causing some inconsistencies.
*/
TEST (confirmation_height, dynamic_algorithm_no_transition_while_pending)
{
	// Repeat in case of intermittent issues not replicating the issue talked about above.
	for (auto _ = 0; _ < 3; ++_)
	{
		nano::system system;
		nano::node_config node_config;
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		node_config.flags.force_use_write_database_queue = true;
		auto node = system.add_node (node_config);
		nano::keypair key;
		system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);

		auto ladev_genesis = node->latest (nano::dev_genesis_key.pub);
		std::vector<std::shared_ptr<nano::state_block>> state_blocks;
		auto const num_blocks = nano::confirmation_height::unbounded_cutoff - 2;

		auto add_block_to_genesis_chain = [&](nano::write_transaction & transaction) {
			static int num = 0;
			auto send (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, ladev_genesis, nano::dev_genesis_key.pub, nano::genesis_amount - num - 1, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (ladev_genesis)));
			ladev_genesis = send->hash ();
			state_blocks.push_back (send);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send).code);
			++num;
		};

		for (auto i = 0; i < num_blocks; ++i)
		{
			auto transaction = node->store.tx_begin_write ();
			add_block_to_genesis_chain (transaction);
		}

		{
			auto write_guard = node->write_database_queue.wait (nano::writer::testing);
			// To limit any data races we are not calling node.block_confirm
			node->confirmation_height_processor.add (state_blocks.back ()->hash ());

			nano::timer<> timer;
			timer.start ();
			while (node->confirmation_height_processor.current ().is_zero ())
			{
				ASSERT_LT (timer.since_start (), 2s);
			}

			// Pausing prevents any writes in the outer while loop in the confirmation height processor (implementation detail)
			node->confirmation_height_processor.pause ();

			timer.restart ();
			ASSERT_TIMELY (10s, node->confirmation_height_processor.unbounded_processor.pending_writes_size != 0);

			{
				// Make it so that the number of blocks exceed the unbounded cutoff would go into the bounded processor (but shouldn't due to unbounded pending writes)
				auto transaction = node->store.tx_begin_write ();
				add_block_to_genesis_chain (transaction);
				add_block_to_genesis_chain (transaction);
			}
			// Make sure this is at a height lower than the block in the add () call above
			node->confirmation_height_processor.add (state_blocks.front ()->hash ());
			node->confirmation_height_processor.unpause ();
		}

		ASSERT_TIMELY (10s, node->ledger.cache.cemented_count == num_blocks + 1);

		ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in), num_blocks);
		ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_bounded, nano::stat::dir::in), 0);
		ASSERT_EQ (node->ledger.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_unbounded, nano::stat::dir::in), num_blocks);
		ASSERT_TIMELY (10s, node->active.election_winner_details_size () == 0);
	}
}

TEST (confirmation_height, many_accounts_send_receive_self)
{
	nano::system system;
	nano::node_config node_config;
	node_config.online_weight_minimum = 100;
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	node_config.active_elections_size = 400000;
	nano::node_flags node_flags;
	node_flags.confirmation_height_processor_mode = nano::confirmation_height_mode::unbounded;
	auto node = system.add_node (node_config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);

#ifndef NDEBUG
	auto const num_accounts = 10000;
#else
	auto const num_accounts = 100000;
#endif

	auto ladev_genesis = node->latest (nano::dev_genesis_key.pub);
	std::vector<nano::keypair> keys;
	std::vector<std::shared_ptr<nano::open_block>> open_blocks;
	{
		auto transaction = node->store.tx_begin_write ();
		for (auto i = 0; i < num_accounts; ++i)
		{
			nano::keypair key;
			keys.emplace_back (key);

			nano::send_block send (ladev_genesis, key.pub, nano::genesis_amount - 1 - i, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (ladev_genesis));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
			auto open = std::make_shared<nano::open_block> (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *open).code);
			open_blocks.push_back (std::move (open));
			ladev_genesis = send.hash ();
		}
	}

	// Confirm all of the accounts
	for (auto & open_block : open_blocks)
	{
		node->block_confirm (open_block);
		auto election = node->active.election (open_block->qualified_root ());
		ASSERT_NE (nullptr, election);
		nano::lock_guard<std::mutex> guard (node->active.mutex);
		election->confirm_once ();
	}

	system.deadline_set (100s);
	auto num_blocks_to_confirm = num_accounts * 2;
	while (node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in) != num_blocks_to_confirm)
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	std::vector<std::shared_ptr<nano::send_block>> send_blocks;
	std::vector<std::shared_ptr<nano::receive_block>> receive_blocks;

	for (int i = 0; i < open_blocks.size (); ++i)
	{
		auto open_block = open_blocks[i];
		auto & keypair = keys[i];
		send_blocks.emplace_back (std::make_shared<nano::send_block> (open_block->hash (), keypair.pub, 1, keypair.prv, keypair.pub, *system.env.work.generate (open_block->hash ())));
		receive_blocks.emplace_back (std::make_shared<nano::receive_block> (send_blocks.back ()->hash (), send_blocks.back ()->hash (), keypair.prv, keypair.pub, *system.env.work.generate (send_blocks.back ()->hash ())));
	}

	// Now send and receive to self
	for (int i = 0; i < open_blocks.size (); ++i)
	{
		node->process_active (send_blocks[i]);
		node->process_active (receive_blocks[i]);
	}

	system.deadline_set (300s);
	num_blocks_to_confirm = num_accounts * 4;
	while (node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in) != num_blocks_to_confirm)
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	system.deadline_set (200s);
	while ((node->ledger.cache.cemented_count - 1) != node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::all, nano::stat::dir::out))
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	auto transaction = node->store.tx_begin_read ();
	auto cemented_count = 0;
	for (auto i (node->ledger.store.confirmation_height_begin (transaction)), n (node->ledger.store.confirmation_height_end ()); i != n; ++i)
	{
		cemented_count += i->second.height;
	}

	ASSERT_EQ (num_blocks_to_confirm + 1, cemented_count);
	ASSERT_EQ (cemented_count, node->ledger.cache.cemented_count);

	system.deadline_set (60s);
	while ((node->ledger.cache.cemented_count - 1) != node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::all, nano::stat::dir::out))
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	system.deadline_set (60s);
	while (node->active.election_winner_details_size () > 0)
	{
		ASSERT_NO_ERROR (system.poll ());
	}
}

// Same as the many_accounts_send_receive_self test, except works on the confirmation height processor directly
// as opposed to active transactions which implicitly calls confirmation height processor.
TEST (confirmation_height, many_accounts_send_receive_self_no_elections)
{
	nano::logger_mt logger;
	auto path (nano::unique_path ());
	nano::mdb_store store (logger, path);
	ASSERT_TRUE (!store.init_error ());
	nano::genesis genesis;
	nano::stat stats;
	nano::ledger ledger (store, stats);
	nano::write_database_queue write_database_queue (false);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	std::atomic<bool> stopped{ false };
	boost::latch initialized_latch{ 0 };

	nano::block_hash block_hash_being_processed{ 0 };
	nano::confirmation_height_processor confirmation_height_processor{ ledger, write_database_queue, 10ms, logger, initialized_latch, confirmation_height_mode::automatic };

	auto const num_accounts = 100000;

	auto ladev_genesis = nano::genesis_hash;
	std::vector<nano::keypair> keys;
	std::vector<std::shared_ptr<nano::open_block>> open_blocks;

	nano::system system;

	{
		auto transaction (store.tx_begin_write ());
		store.initialize (transaction, genesis, ledger.cache);

		// Send from genesis account to all other accounts and create open block for them
		for (auto i = 0; i < num_accounts; ++i)
		{
			nano::keypair key;
			keys.emplace_back (key);
			nano::send_block send (ladev_genesis, key.pub, nano::genesis_amount - 1 - i, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *pool.generate (ladev_genesis));
			ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, send).code);
			auto open = std::make_shared<nano::open_block> (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, *pool.generate (key.pub));
			ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *open).code);
			open_blocks.push_back (std::move (open));
			ladev_genesis = send.hash ();
		}
	}

	for (auto & open_block : open_blocks)
	{
		confirmation_height_processor.add (open_block->hash ());
	}

	system.deadline_set (1000s);
	auto num_blocks_to_confirm = num_accounts * 2;
	while (stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in) != num_blocks_to_confirm)
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	std::vector<std::shared_ptr<nano::send_block>> send_blocks;
	std::vector<std::shared_ptr<nano::receive_block>> receive_blocks;

	// Now add all send/receive blocks
	{
		auto transaction (store.tx_begin_write ());
		for (int i = 0; i < open_blocks.size (); ++i)
		{
			auto open_block = open_blocks[i];
			auto & keypair = keys[i];
			send_blocks.emplace_back (std::make_shared<nano::send_block> (open_block->hash (), keypair.pub, 1, keypair.prv, keypair.pub, *system.env.work.generate (open_block->hash ())));
			receive_blocks.emplace_back (std::make_shared<nano::receive_block> (send_blocks.back ()->hash (), send_blocks.back ()->hash (), keypair.prv, keypair.pub, *system.env.work.generate (send_blocks.back ()->hash ())));

			ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *send_blocks.back ()).code);
			ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *receive_blocks.back ()).code);
		}
	}

	// Randomize the order that send and receive blocks are added to the confirmation height processor
	std::random_device rd;
	std::mt19937 g (rd ());
	std::shuffle (send_blocks.begin (), send_blocks.end (), g);
	std::mt19937 g1 (rd ());
	std::shuffle (receive_blocks.begin (), receive_blocks.end (), g1);

	// Now send and receive to self
	for (int i = 0; i < open_blocks.size (); ++i)
	{
		confirmation_height_processor.add (send_blocks[i]->hash ());
		confirmation_height_processor.add (receive_blocks[i]->hash ());
	}

	system.deadline_set (1000s);
	num_blocks_to_confirm = num_accounts * 4;
	while (stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in) != num_blocks_to_confirm)
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	while (!confirmation_height_processor.current ().is_zero ())
	{
		ASSERT_NO_ERROR (system.poll ());
	}

	auto transaction = store.tx_begin_read ();
	auto cemented_count = 0;
	for (auto i (ledger.store.confirmation_height_begin (transaction)), n (ledger.store.confirmation_height_end ()); i != n; ++i)
	{
		cemented_count += i->second.height;
	}

	ASSERT_EQ (num_blocks_to_confirm + 1, cemented_count);
	ASSERT_EQ (cemented_count, ledger.cache.cemented_count);
}

// Can take up to 1 hour (recommend modifying test work difficulty base level to speed this up)
TEST (confirmation_height, prioritize_frontiers_overwrite)
{
	nano::system system;
	nano::node_config node_config;
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto node = system.add_node (node_config);
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);

	auto num_accounts = node->active.max_priority_cementable_frontiers * 2;
	nano::keypair last_keypair = nano::dev_genesis_key;
	auto last_open_hash = node->latest (nano::dev_genesis_key.pub);
	// Clear confirmation height so that the genesis account has the same amount of uncemented blocks as the other frontiers
	{
		auto transaction = node->store.tx_begin_write ();
		node->store.confirmation_height_clear (transaction);
	}

	{
		auto transaction = node->store.tx_begin_write ();
		for (auto i = num_accounts - 1; i > 0; --i)
		{
			nano::keypair key;
			system.wallet (0)->insert_adhoc (key.prv);

			nano::send_block send (last_open_hash, key.pub, nano::Gxrb_ratio - 1, last_keypair.prv, last_keypair.pub, *system.env.work.generate (last_open_hash));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
			nano::open_block open (send.hash (), last_keypair.pub, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub));
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open).code);
			last_open_hash = open.hash ();
			last_keypair = key;
		}
	}

	auto transaction = node->store.tx_begin_read ();
	{
		// Fill both priority frontier collections.
		node->active.prioritize_frontiers_for_confirmation (transaction, std::chrono::seconds (60), std::chrono::seconds (60));
		ASSERT_EQ (node->active.priority_cementable_frontiers_size () + node->active.priority_wallet_cementable_frontiers_size (), num_accounts);

		// Confirm the last frontier has the least number of uncemented blocks
		auto last_frontier_it = node->active.priority_cementable_frontiers.get<1> ().end ();
		--last_frontier_it;
		ASSERT_EQ (last_frontier_it->account, last_keypair.pub);
		ASSERT_EQ (last_frontier_it->blocks_uncemented, 1);
	}

	// Add a new frontier with 1 block, it should not be added to the frontier container because it is not higher than any already in the maxed out container
	nano::keypair key;
	auto ladev_genesis = node->latest (nano::dev_genesis_key.pub);
	nano::send_block send (ladev_genesis, key.pub, nano::Gxrb_ratio - 1, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (ladev_genesis));
	nano::open_block open (send.hash (), nano::dev_genesis_key.pub, key.pub, key.prv, key.pub, *system.env.work.generate (key.pub));
	{
		auto transaction = node->store.tx_begin_write ();
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open).code);
	}
	transaction.refresh ();
	node->active.prioritize_frontiers_for_confirmation (transaction, std::chrono::seconds (60), std::chrono::seconds (60));
	ASSERT_EQ (node->active.priority_cementable_frontiers_size (), num_accounts / 2);
	ASSERT_EQ (node->active.priority_wallet_cementable_frontiers_size (), num_accounts / 2);

	// The account now has an extra block (2 in total) so has 1 more uncemented block than the next smallest frontier in the collection.
	nano::send_block send1 (send.hash (), key.pub, nano::Gxrb_ratio - 2, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (send.hash ()));
	nano::receive_block receive (open.hash (), send1.hash (), key.prv, key.pub, *system.env.work.generate (open.hash ()));
	{
		auto transaction = node->store.tx_begin_write ();
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive).code);
	}

	// Confirm that it gets replaced
	transaction.refresh ();
	node->active.prioritize_frontiers_for_confirmation (transaction, std::chrono::seconds (60), std::chrono::seconds (60));
	ASSERT_EQ (node->active.priority_cementable_frontiers_size (), num_accounts / 2);
	ASSERT_EQ (node->active.priority_wallet_cementable_frontiers_size (), num_accounts / 2);
	ASSERT_EQ (node->active.priority_cementable_frontiers.find (last_keypair.pub), node->active.priority_cementable_frontiers.end ());
	ASSERT_NE (node->active.priority_cementable_frontiers.find (key.pub), node->active.priority_cementable_frontiers.end ());

	// Check there are no matching accounts found in both containers
	for (auto it = node->active.priority_cementable_frontiers.begin (); it != node->active.priority_cementable_frontiers.end (); ++it)
	{
		ASSERT_EQ (node->active.priority_wallet_cementable_frontiers.find (it->account), node->active.priority_wallet_cementable_frontiers.end ());
	}
}
}

namespace
{
class data
{
public:
	std::atomic<bool> awaiting_cache{ false };
	std::atomic<bool> keep_requesting_metrics{ true };
	std::shared_ptr<nano::node> node;
	std::chrono::system_clock::time_point orig_time;
	std::atomic_flag orig_time_set = ATOMIC_FLAG_INIT;
};
class shared_data
{
public:
	nano::util::counted_completion write_completion{ 0 };
	std::atomic<bool> done{ false };
};

template <typename T>
void callback_process (shared_data & shared_data_a, data & data, T & all_node_data_a, std::chrono::system_clock::time_point last_updated)
{
	if (!data.orig_time_set.test_and_set ())
	{
		data.orig_time = last_updated;
	}

	if (data.awaiting_cache && data.orig_time != last_updated)
	{
		data.keep_requesting_metrics = false;
	}
	if (data.orig_time != last_updated)
	{
		data.awaiting_cache = true;
		data.orig_time = last_updated;
	}
	shared_data_a.write_completion.increment ();
};
}

TEST (telemetry, ongoing_requests)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_initial_telemetry_requests = true;
	auto node_client = system.add_node (config);
	auto node_server = system.add_node (config);

	wait_peer_connections (system);

	ASSERT_EQ (0, node_client->telemetry->telemetry_data_size ());
	ASSERT_EQ (0, node_server->telemetry->telemetry_data_size ());
	ASSERT_EQ (0, node_client->stats.count (nano::stat::type::bootstrap, nano::stat::detail::telemetry_ack, nano::stat::dir::in));
	ASSERT_EQ (0, node_client->stats.count (nano::stat::type::bootstrap, nano::stat::detail::telemetry_req, nano::stat::dir::out));

	ASSERT_TIMELY (20s, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_ack, nano::stat::dir::in) == 1 && node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_ack, nano::stat::dir::in) == 1);

	// Wait till the next ongoing will be called, and add a 1s buffer for the actual processing
	auto time = std::chrono::steady_clock::now ();
	ASSERT_TIMELY (10s, std::chrono::steady_clock::now () >= (time + node_client->telemetry->cache_plus_buffer_cutoff_time () + 1s));

	ASSERT_EQ (2, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_ack, nano::stat::dir::in));
	ASSERT_EQ (2, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));
	ASSERT_EQ (2, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::out));
	ASSERT_EQ (2, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_ack, nano::stat::dir::in));
	ASSERT_EQ (2, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));
	ASSERT_EQ (2, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::out));
}

namespace nano
{
namespace transport
{
	TEST (telemetry, simultaneous_requests)
	{
		nano::system system;
		nano::node_config config;
		config.flags.disable_initial_telemetry_requests = true;
		const auto num_nodes = 4;
		for (int i = 0; i < num_nodes; ++i)
		{
			system.add_node (config);
		}

		wait_peer_connections (system);

		std::vector<std::thread> threads;
		const auto num_threads = 4;

		std::array<data, num_nodes> node_data{};
		for (auto i = 0; i < num_nodes; ++i)
		{
			node_data[i].node = system.nodes[i];
		}

		shared_data shared_data;

		// Create a few threads where each node sends out telemetry request messages to all other nodes continuously, until the cache it reached and subsequently expired.
		// The test waits until all telemetry_ack messages have been received.
		for (int i = 0; i < num_threads; ++i)
		{
			threads.emplace_back ([&node_data, &shared_data]() {
				while (std::any_of (node_data.cbegin (), node_data.cend (), [](auto const & data) { return data.keep_requesting_metrics.load (); }))
				{
					for (auto & data : node_data)
					{
						// Keep calling get_metrics_async until the cache has been saved and then become outdated (after a certain period of time) for each node
						if (data.keep_requesting_metrics)
						{
							shared_data.write_completion.increment_required_count ();

							// Pick first peer to be consistent
							auto peer = data.node->network.tcp_channels.channels[0].channel;
							data.node->telemetry->get_metrics_single_peer_async (peer, [&shared_data, &data, &node_data](nano::telemetry_data_response const & telemetry_data_response_a) {
								ASSERT_FALSE (telemetry_data_response_a.error);
								callback_process (shared_data, data, node_data, telemetry_data_response_a.telemetry_data.timestamp);
							});
						}
						std::this_thread::sleep_for (1ms);
					}
				}

				shared_data.write_completion.await_count_for (20s);
				shared_data.done = true;
			});
		}

		ASSERT_TIMELY (30s, shared_data.done);

		ASSERT_TRUE (std::all_of (node_data.begin (), node_data.end (), [](auto const & data) { return !data.keep_requesting_metrics; }));

		for (auto & thread : threads)
		{
			thread.join ();
		}
	}
}
}

TEST (telemetry, under_load)
{
	nano::system system;
	nano::node_config node_config;
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	node_config.flags.disable_initial_telemetry_requests = true;
	auto node = system.add_node (node_config);
	auto node1 = system.add_node (node_config);
	nano::genesis genesis;
	nano::keypair key;
	nano::keypair key1;
	system.wallet (0)->insert_adhoc (nano::dev_genesis_key.prv);
	system.wallet (0)->insert_adhoc (key.prv);
	auto ladev_genesis = node->latest (nano::dev_genesis_key.pub);
	auto num_blocks = 150000;
	auto send (std::make_shared<nano::state_block> (nano::dev_genesis_key.pub, ladev_genesis, nano::dev_genesis_key.pub, nano::genesis_amount - num_blocks, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (ladev_genesis)));
	node->process_active (send);
	ladev_genesis = send->hash ();
	auto open (std::make_shared<nano::state_block> (key.pub, 0, key.pub, num_blocks, send->hash (), key.prv, key.pub, *system.env.work.generate (key.pub)));
	node->process_active (open);
	auto latest_key = open->hash ();

	auto thread_func = [key1, &system, node, num_blocks](nano::keypair const & keypair, nano::block_hash const & latest, nano::uint128_t const initial_amount) {
		auto latest_l = latest;
		for (int i = 0; i < num_blocks; ++i)
		{
			auto send (std::make_shared<nano::state_block> (keypair.pub, latest_l, keypair.pub, initial_amount - i - 1, key1.pub, keypair.prv, keypair.pub, *system.env.work.generate (latest_l)));
			latest_l = send->hash ();
			node->process_active (send);
		}
	};

	std::thread thread1 (thread_func, nano::dev_genesis_key, ladev_genesis, nano::genesis_amount - num_blocks);
	std::thread thread2 (thread_func, key, latest_key, num_blocks);

	ASSERT_TIMELY (200s, node1->ledger.cache.block_count == num_blocks * 2 + 3);

	thread1.join ();
	thread2.join ();

	for (auto const & node : system.nodes)
	{
		ASSERT_EQ (0, node->stats.count (nano::stat::type::telemetry, nano::stat::detail::failed_send_telemetry_req));
		ASSERT_EQ (0, node->stats.count (nano::stat::type::telemetry, nano::stat::detail::request_within_protection_cache_zone));
		ASSERT_EQ (0, node->stats.count (nano::stat::type::telemetry, nano::stat::detail::unsolicited_telemetry_ack));
		ASSERT_EQ (0, node->stats.count (nano::stat::type::telemetry, nano::stat::detail::no_response_received));
	}
}

TEST (telemetry, all_peers_use_single_request_cache)
{
	nano::system system;
	nano::node_config config;
	config.flags.disable_ongoing_telemetry_requests = true;
	config.flags.disable_initial_telemetry_requests = true;
	auto node_client = system.add_node (config);
	auto node_server = system.add_node (config);

	wait_peer_connections (system);

	// Request telemetry metrics
	nano::telemetry_data telemetry_data;
	{
		std::atomic<bool> done{ false };
		auto channel = node_client->network.find_channel (node_server->network.endpoint ());
		node_client->telemetry->get_metrics_single_peer_async (channel, [&done, &telemetry_data](nano::telemetry_data_response const & response_a) {
			telemetry_data = response_a.telemetry_data;
			done = true;
		});

		ASSERT_TIMELY (10s, done);
	}

	auto responses = node_client->telemetry->get_metrics ();
	ASSERT_EQ (telemetry_data, responses.begin ()->second);

	// Confirm only 1 request was made
	ASSERT_EQ (1, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_ack, nano::stat::dir::in));
	ASSERT_EQ (0, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));
	ASSERT_EQ (1, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::out));
	ASSERT_EQ (0, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_ack, nano::stat::dir::in));
	ASSERT_EQ (1, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));
	ASSERT_EQ (0, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::out));

	std::this_thread::sleep_for (node_server->telemetry->cache_plus_buffer_cutoff_time ());

	// Should be empty
	responses = node_client->telemetry->get_metrics ();
	ASSERT_TRUE (responses.empty ());

	{
		std::atomic<bool> done{ false };
		auto channel = node_client->network.find_channel (node_server->network.endpoint ());
		node_client->telemetry->get_metrics_single_peer_async (channel, [&done, &telemetry_data](nano::telemetry_data_response const & response_a) {
			telemetry_data = response_a.telemetry_data;
			done = true;
		});

		ASSERT_TIMELY (10s, done);
	}

	responses = node_client->telemetry->get_metrics ();
	ASSERT_EQ (telemetry_data, responses.begin ()->second);

	ASSERT_EQ (2, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_ack, nano::stat::dir::in));
	ASSERT_EQ (0, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));
	ASSERT_EQ (2, node_client->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::out));
	ASSERT_EQ (0, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_ack, nano::stat::dir::in));
	ASSERT_EQ (2, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::in));
	ASSERT_EQ (0, node_server->stats.count (nano::stat::type::message, nano::stat::detail::telemetry_req, nano::stat::dir::out));
}

TEST (telemetry, many_nodes)
{
	nano::system system;
	// The telemetry responses can timeout if using a large number of nodes under sanitizers, so lower the number.
	const auto num_nodes = (is_sanitizer_build || nano::running_within_valgrind ()) ? 4 : 10;
	for (auto i = 0; i < num_nodes; ++i)
	{
		nano::node_config config;
		config.flags.disable_ongoing_telemetry_requests = true;
		config.flags.disable_initial_telemetry_requests = true;
		config.flags.disable_request_loop = true;
		// Make a metric completely different for each node so we can check afterwards that there are no duplicates
		config.bandwidth_limit = 100000 + i;

		auto node = std::make_shared<nano::node> (system.env, config);
		node->start ();
		system.nodes.push_back (node);
	}

	// Merge peers after creating nodes as some backends (RocksDB) can take a while to initialize nodes (Windows/Debug for instance)
	// and timeouts can occur between nodes while starting up many nodes synchronously.
	for (auto const & node : system.nodes)
	{
		for (auto const & other_node : system.nodes)
		{
			if (node != other_node)
			{
				node->network.merge_peer (other_node->network.endpoint ());
			}
		}
	}

	wait_peer_connections (system);

	// Give all nodes a non-default number of blocks
	nano::keypair key;
	nano::genesis genesis;
	nano::state_block send (nano::dev_genesis_key.pub, genesis.hash (), nano::dev_genesis_key.pub, nano::genesis_amount - nano::Mxrb_ratio, key.pub, nano::dev_genesis_key.prv, nano::dev_genesis_key.pub, *system.env.work.generate (genesis.hash ()));
	for (auto node : system.nodes)
	{
		auto transaction (node->store.tx_begin_write ());
		ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
	}

	// This is the node which will request metrics from all other nodes
	auto node_client = system.nodes.front ();

	std::mutex mutex;
	std::vector<nano::telemetry_data> telemetry_datas;
	auto peers = node_client->network.list (num_nodes - 1);
	ASSERT_EQ (peers.size (), num_nodes - 1);
	for (auto const & peer : peers)
	{
		node_client->telemetry->get_metrics_single_peer_async (peer, [&telemetry_datas, &mutex](nano::telemetry_data_response const & response_a) {
			ASSERT_FALSE (response_a.error);
			nano::lock_guard<std::mutex> guard (mutex);
			telemetry_datas.push_back (response_a.telemetry_data);
		});
	}

	system.deadline_set (20s);
	nano::unique_lock<std::mutex> lk (mutex);
	while (telemetry_datas.size () != num_nodes - 1)
	{
		lk.unlock ();
		ASSERT_NO_ERROR (system.poll ());
		lk.lock ();
	}

	// Check the metrics
	nano::environment_constants constants;
	for (auto & data : telemetry_datas)
	{
		ASSERT_EQ (data.unchecked_count, 0);
		ASSERT_EQ (data.cemented_count, 1);
		ASSERT_LE (data.peer_count, 9);
		ASSERT_EQ (data.account_count, 1);
		ASSERT_TRUE (data.block_count == 2);
		ASSERT_EQ (data.protocol_version, constants.protocol.telemetry_protocol_version_min);
		ASSERT_GE (data.bandwidth_cap, 100000);
		ASSERT_LT (data.bandwidth_cap, 100000 + system.nodes.size ());
		ASSERT_EQ (data.major_version, nano::get_major_node_version ());
		ASSERT_EQ (data.minor_version, nano::get_minor_node_version ());
		ASSERT_EQ (data.patch_version, nano::get_patch_node_version ());
		ASSERT_EQ (data.pre_release_version, nano::get_pre_release_node_version ());
		ASSERT_EQ (data.maker, 0);
		ASSERT_LT (data.uptime, 100);
		ASSERT_EQ (data.genesis_block, genesis.hash ());
		ASSERT_LE (data.timestamp, std::chrono::system_clock::now ());
		ASSERT_EQ (data.active_difficulty, system.nodes.front ()->active.active_difficulty ());
	}

	// We gave some nodes different bandwidth caps, confirm they are not all the same
	auto bandwidth_cap = telemetry_datas.front ().bandwidth_cap;
	telemetry_datas.erase (telemetry_datas.begin ());
	auto all_bandwidth_limits_same = std::all_of (telemetry_datas.begin (), telemetry_datas.end (), [bandwidth_cap](auto & telemetry_data) {
		return telemetry_data.bandwidth_cap == bandwidth_cap;
	});
	ASSERT_FALSE (all_bandwidth_limits_same);
}

// Similar to signature_checker.boundary_checks but more exhaustive. Can take up to 1 minute
TEST (signature_checker, mass_boundary_checks)
{
	// sizes container must be in incrementing order
	std::vector<size_t> sizes{ 0, 1 };
	auto add_boundary = [&sizes](size_t boundary) {
		sizes.insert (sizes.end (), { boundary - 1, boundary, boundary + 1 });
	};

	for (auto i = 1; i <= 10; ++i)
	{
		add_boundary (nano::signature_checker::batch_size * i);
	}

	for (auto num_threads = 0; num_threads < 5; ++num_threads)
	{
		nano::signature_checker checker (num_threads);
		auto max_size = *(sizes.end () - 1);
		std::vector<nano::uint256_union> hashes;
		hashes.reserve (max_size);
		std::vector<unsigned char const *> messages;
		messages.reserve (max_size);
		std::vector<size_t> lengths;
		lengths.reserve (max_size);
		std::vector<unsigned char const *> pub_keys;
		pub_keys.reserve (max_size);
		std::vector<unsigned char const *> signatures;
		signatures.reserve (max_size);
		nano::keypair key;
		nano::state_block block (key.pub, 0, key.pub, 0, 0, key.prv, key.pub, 0);

		auto last_size = 0;
		for (auto size : sizes)
		{
			// The size needed to append to existing containers, saves re-initializing from scratch each iteration
			auto extra_size = size - last_size;

			std::vector<int> verifications;
			verifications.resize (size);
			for (auto i (0); i < extra_size; ++i)
			{
				hashes.push_back (block.hash ());
				messages.push_back (hashes.back ().bytes.data ());
				lengths.push_back (sizeof (decltype (hashes)::value_type));
				pub_keys.push_back (block.hashables.account.bytes.data ());
				signatures.push_back (block.signature.bytes.data ());
			}
			nano::signature_check_set check = { size, messages.data (), lengths.data (), pub_keys.data (), signatures.data (), verifications.data () };
			checker.verify (check);
			bool all_valid = std::all_of (verifications.cbegin (), verifications.cend (), [](auto verification) { return verification == 1; });
			ASSERT_TRUE (all_valid);
			last_size = size;
		}
	}
}

// Test the node epoch_upgrader with a large number of accounts and threads
// Possible to manually add work peers
TEST (node, mass_epoch_upgrader)
{
	auto perform_test = [](size_t const batch_size) {
		unsigned threads = 5;
		size_t total_accounts = 2500;

#ifndef NDEBUG
		total_accounts /= 5;
#endif

		struct info
		{
			nano::keypair key;
			nano::block_hash pending_hash;
		};

		std::vector<info> opened (total_accounts / 2);
		std::vector<info> unopened (total_accounts / 2);

		nano::system system;
		nano::node_config node_config;
		node_config.work_threads = 4;
		//node_config.work_peers = { { "192.168.1.101", 7000 } };
		auto & node = *system.add_node (node_config);

		auto balance = node.balance (nano::dev_genesis_key.pub);
		auto latest = node.latest (nano::dev_genesis_key.pub);
		nano::uint128_t amount = 1;

		// Send to all accounts
		std::array<std::vector<info> *, 2> all{ &opened, &unopened };
		for (auto & accounts : all)
		{
			for (auto & info : *accounts)
			{
				balance -= amount;
				nano::state_block_builder builder;
				std::error_code ec;
				auto block = builder
				             .account (nano::dev_genesis_key.pub)
				             .previous (latest)
				             .balance (balance)
				             .link (info.key.pub)
				             .representative (nano::dev_genesis_key.pub)
				             .sign (nano::dev_genesis_key.prv, nano::dev_genesis_key.pub)
				             .work (*node.work_generate_blocking (latest, nano::work_threshold (nano::work_version::work_1, nano::block_details (nano::epoch::epoch_0, false, false, false))))
				             .build (ec);
				ASSERT_FALSE (ec);
				ASSERT_NE (nullptr, block);
				ASSERT_EQ (nano::process_result::progress, node.process (*block).code);
				latest = block->hash ();
				info.pending_hash = block->hash ();
			}
		}
		ASSERT_EQ (1 + total_accounts, node.ledger.cache.block_count);
		ASSERT_EQ (1, node.ledger.cache.account_count);

		// Receive for half of accounts
		for (auto const & info : opened)
		{
			nano::state_block_builder builder;
			std::error_code ec;
			auto block = builder
			             .account (info.key.pub)
			             .previous (0)
			             .balance (amount)
			             .link (info.pending_hash)
			             .representative (info.key.pub)
			             .sign (info.key.prv, info.key.pub)
			             .work (*node.work_generate_blocking (info.key.pub, nano::work_threshold (nano::work_version::work_1, nano::block_details (nano::epoch::epoch_0, false, false, false))))
			             .build (ec);
			ASSERT_FALSE (ec);
			ASSERT_NE (nullptr, block);
			ASSERT_EQ (nano::process_result::progress, node.process (*block).code);
		}
		ASSERT_EQ (1 + total_accounts + opened.size (), node.ledger.cache.block_count);
		ASSERT_EQ (1 + opened.size (), node.ledger.cache.account_count);

		nano::keypair epoch_signer (nano::dev_genesis_key);

		auto const block_count_before = node.ledger.cache.block_count.load ();
		auto const total_to_upgrade = 1 + total_accounts;
		std::cout << "Mass upgrading " << total_to_upgrade << " accounts" << std::endl;
		while (node.ledger.cache.block_count != block_count_before + total_to_upgrade)
		{
			auto const pre_upgrade = node.ledger.cache.block_count.load ();
			auto upgrade_count = std::min<size_t> (batch_size, block_count_before + total_to_upgrade - pre_upgrade);
			ASSERT_FALSE (node.epoch_upgrader (epoch_signer.prv.as_private_key (), nano::epoch::epoch_1, upgrade_count, threads));
			// Already ongoing - should fail
			ASSERT_TRUE (node.epoch_upgrader (epoch_signer.prv.as_private_key (), nano::epoch::epoch_1, upgrade_count, threads));
			system.deadline_set (60s);
			while (node.ledger.cache.block_count != pre_upgrade + upgrade_count)
			{
				ASSERT_NO_ERROR (system.poll ());
				std::this_thread::sleep_for (200ms);
				std::cout << node.ledger.cache.block_count - block_count_before << " / " << total_to_upgrade << std::endl;
			}
			std::this_thread::sleep_for (50ms);
		}
		auto expected_blocks = block_count_before + total_accounts + 1;
		ASSERT_EQ (expected_blocks, node.ledger.cache.block_count);
		// Check upgrade
		{
			auto transaction (node.store.tx_begin_read ());
			auto block_count_sum = 0;
			for (auto i (node.store.latest_begin (transaction)); i != node.store.latest_end (); ++i)
			{
				nano::account_info info (i->second);
				ASSERT_EQ (info.epoch (), nano::epoch::epoch_1);
				block_count_sum += info.block_count;
			}
			ASSERT_EQ (expected_blocks, block_count_sum);
		}
	};
	// Test with a limited number of upgrades and an unlimited
	perform_test (42);
	perform_test (std::numeric_limits<size_t>::max ());
}

TEST (node, mass_block_new)
{
	nano::system system;
	nano::node_config node_config;
	node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
	auto & node = *system.add_node (node_config);
	node.env.constants.network.request_interval_ms = 500;

#ifndef NDEBUG
	auto const num_blocks = 5000;
#else
	auto const num_blocks = 50000;
#endif
	std::cout << num_blocks << " x4 blocks" << std::endl;

	// Upgrade to epoch_2
	system.upgrade_genesis_epoch (node, nano::epoch::epoch_1);
	system.upgrade_genesis_epoch (node, nano::epoch::epoch_2);

	auto next_block_count = num_blocks + 3;
	auto process_all = [&](std::vector<std::shared_ptr<nano::state_block>> const & blocks_a) {
		for (auto const & block : blocks_a)
		{
			node.process_active (block);
		}
		ASSERT_TIMELY (200s, node.ledger.cache.block_count == next_block_count);
		next_block_count += num_blocks;
		node.block_processor.flush ();
		// Clear all active
		{
			nano::lock_guard<std::mutex> guard (node.active.mutex);
			node.active.roots.clear ();
			node.active.blocks.clear ();
		}
	};

	nano::genesis genesis;
	nano::keypair key;
	std::vector<nano::keypair> keys (num_blocks);
	nano::state_block_builder builder;
	std::vector<std::shared_ptr<nano::state_block>> send_blocks;
	auto send_threshold (nano::work_threshold (nano::work_version::work_1, nano::block_details (nano::epoch::epoch_2, true, false, false)));
	auto ladev_genesis = node.latest (nano::dev_genesis_key.pub);
	for (auto i = 0; i < num_blocks; ++i)
	{
		auto send = builder.make_block ()
		            .account (nano::dev_genesis_key.pub)
		            .previous (ladev_genesis)
		            .balance (nano::genesis_amount - i - 1)
		            .representative (nano::dev_genesis_key.pub)
		            .link (keys[i].pub)
		            .sign (nano::dev_genesis_key.prv, nano::dev_genesis_key.pub)
		            .work (*system.env.work.generate (nano::work_version::work_1, ladev_genesis, send_threshold))
		            .build ();
		ladev_genesis = send->hash ();
		send_blocks.push_back (std::move (send));
	}
	std::cout << "Send blocks built, start processing" << std::endl;
	nano::timer<> timer;
	timer.start ();
	process_all (send_blocks);
	std::cout << "Send blocks time: " << timer.stop ().count () << " " << timer.unit () << "\n\n";

	std::vector<std::shared_ptr<nano::state_block>> open_blocks;
	auto receive_threshold (nano::work_threshold (nano::work_version::work_1, nano::block_details (nano::epoch::epoch_2, false, true, false)));
	for (auto i = 0; i < num_blocks; ++i)
	{
		auto const & key = keys[i];
		auto open = builder.make_block ()
		            .account (key.pub)
		            .previous (0)
		            .balance (1)
		            .representative (key.pub)
		            .link (send_blocks[i]->hash ())
		            .sign (key.prv, key.pub)
		            .work (*system.env.work.generate (nano::work_version::work_1, key.pub, receive_threshold))
		            .build ();
		open_blocks.push_back (std::move (open));
	}
	std::cout << "Open blocks built, start processing" << std::endl;
	timer.restart ();
	process_all (open_blocks);
	std::cout << "Open blocks time: " << timer.stop ().count () << " " << timer.unit () << "\n\n";

	// These blocks are from each key to themselves
	std::vector<std::shared_ptr<nano::state_block>> send_blocks2;
	for (auto i = 0; i < num_blocks; ++i)
	{
		auto const & key = keys[i];
		auto const & latest = open_blocks[i];
		auto send2 = builder.make_block ()
		             .account (key.pub)
		             .previous (latest->hash ())
		             .balance (0)
		             .representative (key.pub)
		             .link (key.pub)
		             .sign (key.prv, key.pub)
		             .work (*system.env.work.generate (nano::work_version::work_1, latest->hash (), send_threshold))
		             .build ();
		send_blocks2.push_back (std::move (send2));
	}
	std::cout << "Send2 blocks built, start processing" << std::endl;
	timer.restart ();
	process_all (send_blocks2);
	std::cout << "Send2 blocks time: " << timer.stop ().count () << " " << timer.unit () << "\n\n";

	// Each key receives the previously sent blocks
	std::vector<std::shared_ptr<nano::state_block>> receive_blocks;
	for (auto i = 0; i < num_blocks; ++i)
	{
		auto const & key = keys[i];
		auto const & latest = send_blocks2[i];
		auto send2 = builder.make_block ()
		             .account (key.pub)
		             .previous (latest->hash ())
		             .balance (1)
		             .representative (key.pub)
		             .link (latest->hash ())
		             .sign (key.prv, key.pub)
		             .work (*system.env.work.generate (nano::work_version::work_1, latest->hash (), receive_threshold))
		             .build ();
		receive_blocks.push_back (std::move (send2));
	}
	std::cout << "Receive blocks built, start processing" << std::endl;
	timer.restart ();
	process_all (receive_blocks);
	std::cout << "Receive blocks time: " << timer.stop ().count () << " " << timer.unit () << "\n\n";
}
