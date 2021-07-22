#include <nano/node/election.hpp>
#include <nano/test_common/system.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

#include <boost/format.hpp>

using namespace std::chrono_literals;

namespace
{
void add_callback_stats (nano::node & node, std::vector<nano::block_hash> * observer_order = nullptr, nano::mutex * mutex = nullptr)
{
	node.observers.blocks.add ([&stats = node.stats, observer_order, mutex] (nano::election_status const & status_a, std::vector<nano::vote_with_weight_info> const &, nano::account const &, nano::amount const &, bool) {
		stats.inc (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out);
		if (mutex)
		{
			nano::lock_guard<nano::mutex> guard (*mutex);
			debug_assert (observer_order);
			observer_order->push_back (status_a.winner->hash ());
		}
	});
}
nano::stat::detail get_stats_detail (nano::confirmation_height_mode mode_a)
{
	debug_assert (mode_a == nano::confirmation_height_mode::bounded || mode_a == nano::confirmation_height_mode::unbounded);
	return (mode_a == nano::confirmation_height_mode::bounded) ? nano::stat::detail::blocks_confirmed_bounded : nano::stat::detail::blocks_confirmed_unbounded;
}
}

TEST (confirmation_height, single)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		auto amount (std::numeric_limits<nano::uint128_t>::max ());
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		auto node = system.add_node (node_flags);
		nano::keypair key1;
		system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
		nano::block_hash latest1 (node->latest (nano::dev::genesis_key.pub));
		auto send1 (std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, latest1, nano::dev::genesis_key.pub, amount - 100, key1.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest1)));

		// Check confirmation heights before, should be uninitialized (1 for genesis).
		nano::confirmation_height_info confirmation_height_info;
		add_callback_stats (*node);
		auto transaction = node->store.tx_begin_read ();
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
		ASSERT_EQ (1, confirmation_height_info.height);
		ASSERT_EQ (nano::dev::genesis->hash (), confirmation_height_info.frontier);

		node->process_active (send1);
		node->block_processor.flush ();

		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 1);

		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_TRUE (node->ledger.block_confirmed (transaction, send1->hash ()));
			ASSERT_FALSE (node->store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
			ASSERT_EQ (2, confirmation_height_info.height);
			ASSERT_EQ (send1->hash (), confirmation_height_info.frontier);

			// Rollbacks should fail as these blocks have been cemented
			ASSERT_TRUE (node->ledger.rollback (transaction, latest1));
			ASSERT_TRUE (node->ledger.rollback (transaction, send1->hash ()));
			ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
			ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
			ASSERT_EQ (1, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
			ASSERT_EQ (2, node->ledger.cache.cemented_count);

			ASSERT_EQ (0, node->active.election_winner_details_size ());
		}
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

TEST (confirmation_height, multiple_accounts)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);
		nano::keypair key1;
		nano::keypair key2;
		nano::keypair key3;
		nano::block_hash latest1 (system.nodes[0]->latest (nano::dev::genesis_key.pub));

		// Send to all accounts
		nano::send_block send1 (latest1, key1.pub, node->online_reps.delta () + 300, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest1));
		nano::send_block send2 (send1.hash (), key2.pub, node->online_reps.delta () + 200, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1.hash ()));
		nano::send_block send3 (send2.hash (), key3.pub, node->online_reps.delta () + 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send2.hash ()));

		// Open all accounts
		nano::open_block open1 (send1.hash (), nano::dev::genesis->account (), key1.pub, key1.prv, key1.pub, *system.work.generate (key1.pub));
		nano::open_block open2 (send2.hash (), nano::dev::genesis->account (), key2.pub, key2.prv, key2.pub, *system.work.generate (key2.pub));
		nano::open_block open3 (send3.hash (), nano::dev::genesis->account (), key3.pub, key3.prv, key3.pub, *system.work.generate (key3.pub));

		// Send and receive various blocks to these accounts
		nano::send_block send4 (open1.hash (), key2.pub, 50, key1.prv, key1.pub, *system.work.generate (open1.hash ()));
		nano::send_block send5 (send4.hash (), key2.pub, 10, key1.prv, key1.pub, *system.work.generate (send4.hash ()));

		nano::receive_block receive1 (open2.hash (), send4.hash (), key2.prv, key2.pub, *system.work.generate (open2.hash ()));
		nano::send_block send6 (receive1.hash (), key3.pub, 10, key2.prv, key2.pub, *system.work.generate (receive1.hash ()));
		nano::receive_block receive2 (send6.hash (), send5.hash (), key2.prv, key2.pub, *system.work.generate (send6.hash ()));

		add_callback_stats (*node);

		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send3).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open3).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send4).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send5).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send6).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive2).code);

			// Check confirmation heights of all the accounts (except genesis) are uninitialized (0),
			// as we have any just added them to the ledger and not processed any live transactions yet.
			nano::confirmation_height_info confirmation_height_info;
			ASSERT_FALSE (node->store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
			ASSERT_EQ (1, confirmation_height_info.height);
			ASSERT_EQ (nano::dev::genesis->hash (), confirmation_height_info.frontier);
			ASSERT_TRUE (node->store.confirmation_height.get (transaction, key1.pub, confirmation_height_info));
			ASSERT_EQ (0, confirmation_height_info.height);
			ASSERT_EQ (nano::block_hash (0), confirmation_height_info.frontier);
			ASSERT_TRUE (node->store.confirmation_height.get (transaction, key2.pub, confirmation_height_info));
			ASSERT_EQ (0, confirmation_height_info.height);
			ASSERT_EQ (nano::block_hash (0), confirmation_height_info.frontier);
			ASSERT_TRUE (node->store.confirmation_height.get (transaction, key3.pub, confirmation_height_info));
			ASSERT_EQ (0, confirmation_height_info.height);
			ASSERT_EQ (nano::block_hash (0), confirmation_height_info.frontier);
		}

		// The nodes process a live receive which propagates across to all accounts
		auto receive3 = std::make_shared<nano::receive_block> (open3.hash (), send6.hash (), key3.prv, key3.pub, *system.work.generate (open3.hash ()));
		node->process_active (receive3);
		node->block_processor.flush ();
		node->block_confirm (receive3);
		auto election = node->active.election (receive3->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();

		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 10);

		nano::account_info account_info;
		nano::confirmation_height_info confirmation_height_info;
		auto & store = node->store;
		auto transaction = node->store.tx_begin_read ();
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, receive3->hash ()));
		ASSERT_FALSE (store.account.get (transaction, nano::dev::genesis_key.pub, account_info));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
		ASSERT_EQ (4, confirmation_height_info.height);
		ASSERT_EQ (send3.hash (), confirmation_height_info.frontier);
		ASSERT_EQ (4, account_info.block_count);
		ASSERT_FALSE (store.account.get (transaction, key1.pub, account_info));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, key1.pub, confirmation_height_info));
		ASSERT_EQ (2, confirmation_height_info.height);
		ASSERT_EQ (send4.hash (), confirmation_height_info.frontier);
		ASSERT_EQ (3, account_info.block_count);
		ASSERT_FALSE (store.account.get (transaction, key2.pub, account_info));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, key2.pub, confirmation_height_info));
		ASSERT_EQ (3, confirmation_height_info.height);
		ASSERT_EQ (send6.hash (), confirmation_height_info.frontier);
		ASSERT_EQ (4, account_info.block_count);
		ASSERT_FALSE (store.account.get (transaction, key3.pub, account_info));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, key3.pub, confirmation_height_info));
		ASSERT_EQ (2, confirmation_height_info.height);
		ASSERT_EQ (receive3->hash (), confirmation_height_info.frontier);
		ASSERT_EQ (2, account_info.block_count);

		// The accounts for key1 and key2 have 1 more block in the chain than is confirmed.
		// So this can be rolled back, but the one before that cannot. Check that this is the case
		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_FALSE (node->ledger.rollback (transaction, node->latest (key2.pub)));
			ASSERT_FALSE (node->ledger.rollback (transaction, node->latest (key1.pub)));
		}
		{
			// These rollbacks should fail
			auto transaction = node->store.tx_begin_write ();
			ASSERT_TRUE (node->ledger.rollback (transaction, node->latest (key1.pub)));
			ASSERT_TRUE (node->ledger.rollback (transaction, node->latest (key2.pub)));

			// Confirm the other latest can't be rolled back either
			ASSERT_TRUE (node->ledger.rollback (transaction, node->latest (key3.pub)));
			ASSERT_TRUE (node->ledger.rollback (transaction, node->latest (nano::dev::genesis_key.pub)));

			// Attempt some others which have been cemented
			ASSERT_TRUE (node->ledger.rollback (transaction, open1.hash ()));
			ASSERT_TRUE (node->ledger.rollback (transaction, send2.hash ()));
		}
		ASSERT_EQ (10, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (10, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (10, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
		ASSERT_EQ (11, node->ledger.cache.cemented_count);

		ASSERT_EQ (0, node->active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

TEST (confirmation_height, gap_bootstrap)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		auto & node1 = *system.add_node (node_flags);
		nano::keypair destination;
		auto send1 (std::make_shared<nano::state_block> (nano::dev::genesis->account (), nano::dev::genesis->hash (), nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio, destination.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, 0));
		node1.work_generate_blocking (*send1);
		auto send2 (std::make_shared<nano::state_block> (nano::dev::genesis->account (), send1->hash (), nano::dev::genesis->account (), nano::dev::genesis_amount - 2 * nano::Gxrb_ratio, destination.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, 0));
		node1.work_generate_blocking (*send2);
		auto send3 (std::make_shared<nano::state_block> (nano::dev::genesis->account (), send2->hash (), nano::dev::genesis->account (), nano::dev::genesis_amount - 3 * nano::Gxrb_ratio, destination.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, 0));
		node1.work_generate_blocking (*send3);
		auto open1 (std::make_shared<nano::open_block> (send1->hash (), destination.pub, destination.pub, destination.prv, destination.pub, 0));
		node1.work_generate_blocking (*open1);

		// Receive
		auto receive1 (std::make_shared<nano::receive_block> (open1->hash (), send2->hash (), destination.prv, destination.pub, 0));
		node1.work_generate_blocking (*receive1);
		auto receive2 (std::make_shared<nano::receive_block> (receive1->hash (), send3->hash (), destination.prv, destination.pub, 0));
		node1.work_generate_blocking (*receive2);

		node1.block_processor.add (send1);
		node1.block_processor.add (send2);
		node1.block_processor.add (send3);
		node1.block_processor.add (receive1);
		node1.block_processor.flush ();

		add_callback_stats (node1);

		// Receive 2 comes in on the live network, however the chain has not been finished so it gets added to unchecked
		node1.process_active (receive2);
		node1.block_processor.flush ();

		// Confirmation heights should not be updated
		{
			auto transaction (node1.store.tx_begin_read ());
			auto unchecked_count (node1.store.unchecked.count (transaction));
			ASSERT_EQ (unchecked_count, 2);

			nano::confirmation_height_info confirmation_height_info;
			ASSERT_FALSE (node1.store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
			ASSERT_EQ (1, confirmation_height_info.height);
			ASSERT_EQ (nano::dev::genesis->hash (), confirmation_height_info.frontier);
		}

		// Now complete the chain where the block comes in on the bootstrap network.
		node1.block_processor.add (open1);
		node1.block_processor.flush ();

		// Confirmation height should be unchanged and unchecked should now be 0
		{
			auto transaction (node1.store.tx_begin_read ());
			auto unchecked_count (node1.store.unchecked.count (transaction));
			ASSERT_EQ (unchecked_count, 0);

			nano::confirmation_height_info confirmation_height_info;
			ASSERT_FALSE (node1.store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
			ASSERT_EQ (1, confirmation_height_info.height);
			ASSERT_EQ (nano::dev::genesis->hash (), confirmation_height_info.frontier);
			ASSERT_TRUE (node1.store.confirmation_height.get (transaction, destination.pub, confirmation_height_info));
			ASSERT_EQ (0, confirmation_height_info.height);
			ASSERT_EQ (nano::block_hash (0), confirmation_height_info.frontier);
		}
		ASSERT_EQ (0, node1.stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (0, node1.stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (0, node1.stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
		ASSERT_EQ (1, node1.ledger.cache.cemented_count);

		ASSERT_EQ (0, node1.active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

TEST (confirmation_height, gap_live)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);
		node_config.peering_port = nano::get_available_port ();
		node_config.receive_minimum = nano::dev::genesis_amount; // Prevent auto-receive & open1/receive1/receive2 blocks conflicts
		system.add_node (node_config, node_flags);
		nano::keypair destination;
		system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
		system.wallet (1)->insert_adhoc (destination.prv);

		auto send1 (std::make_shared<nano::state_block> (nano::dev::genesis->account (), nano::dev::genesis->hash (), nano::dev::genesis->account (), nano::dev::genesis_amount - 1, destination.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, 0));
		node->work_generate_blocking (*send1);
		auto send2 (std::make_shared<nano::state_block> (nano::dev::genesis->account (), send1->hash (), nano::dev::genesis->account (), nano::dev::genesis_amount - 2, destination.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, 0));
		node->work_generate_blocking (*send2);
		auto send3 (std::make_shared<nano::state_block> (nano::dev::genesis->account (), send2->hash (), nano::dev::genesis->account (), nano::dev::genesis_amount - 3, destination.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, 0));
		node->work_generate_blocking (*send3);

		auto open1 (std::make_shared<nano::open_block> (send1->hash (), destination.pub, destination.pub, destination.prv, destination.pub, 0));
		node->work_generate_blocking (*open1);
		auto receive1 (std::make_shared<nano::receive_block> (open1->hash (), send2->hash (), destination.prv, destination.pub, 0));
		node->work_generate_blocking (*receive1);
		auto receive2 (std::make_shared<nano::receive_block> (receive1->hash (), send3->hash (), destination.prv, destination.pub, 0));
		node->work_generate_blocking (*receive2);

		node->block_processor.add (send1);
		node->block_processor.add (send2);
		node->block_processor.add (send3);
		node->block_processor.add (receive1);
		node->block_processor.flush ();

		add_callback_stats (*node);

		// Receive 2 comes in on the live network, however the chain has not been finished so it gets added to unchecked
		node->process_active (receive2);
		node->block_processor.flush ();

		// Confirmation heights should not be updated
		{
			auto transaction = node->store.tx_begin_read ();
			nano::confirmation_height_info confirmation_height_info;
			ASSERT_FALSE (node->store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
			ASSERT_EQ (1, confirmation_height_info.height);
			ASSERT_EQ (nano::dev::genesis->hash (), confirmation_height_info.frontier);
		}

		// Vote and confirm all existing blocks
		node->block_confirm (send1);
		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 3);

		// Now complete the chain where the block comes in on the live network
		node->process_active (open1);
		node->block_processor.flush ();

		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 6);

		// This should confirm the open block and the source of the receive blocks
		auto transaction (node->store.tx_begin_read ());
		auto unchecked_count (node->store.unchecked.count (transaction));
		ASSERT_EQ (unchecked_count, 0);

		nano::confirmation_height_info confirmation_height_info;
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, receive2->hash ()));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
		ASSERT_EQ (4, confirmation_height_info.height);
		ASSERT_EQ (send3->hash (), confirmation_height_info.frontier);
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, destination.pub, confirmation_height_info));
		ASSERT_EQ (3, confirmation_height_info.height);
		ASSERT_EQ (receive2->hash (), confirmation_height_info.frontier);

		ASSERT_EQ (6, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (6, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (6, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
		ASSERT_EQ (7, node->ledger.cache.cemented_count);

		ASSERT_EQ (0, node->active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

TEST (confirmation_height, send_receive_between_2_accounts)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);
		nano::keypair key1;
		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));

		nano::send_block send1 (latest, key1.pub, node->online_reps.delta () + 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));

		nano::open_block open1 (send1.hash (), nano::dev::genesis->account (), key1.pub, key1.prv, key1.pub, *system.work.generate (key1.pub));
		nano::send_block send2 (open1.hash (), nano::dev::genesis->account (), 1000, key1.prv, key1.pub, *system.work.generate (open1.hash ()));
		nano::send_block send3 (send2.hash (), nano::dev::genesis->account (), 900, key1.prv, key1.pub, *system.work.generate (send2.hash ()));
		nano::send_block send4 (send3.hash (), nano::dev::genesis->account (), 500, key1.prv, key1.pub, *system.work.generate (send3.hash ()));

		nano::receive_block receive1 (send1.hash (), send2.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1.hash ()));
		nano::receive_block receive2 (receive1.hash (), send3.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (receive1.hash ()));
		nano::receive_block receive3 (receive2.hash (), send4.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (receive2.hash ()));

		nano::send_block send5 (receive3.hash (), key1.pub, node->online_reps.delta () + 1, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (receive3.hash ()));
		auto receive4 = std::make_shared<nano::receive_block> (send4.hash (), send5.hash (), key1.prv, key1.pub, *system.work.generate (send4.hash ()));
		// Unpocketed send
		nano::keypair key2;
		nano::send_block send6 (send5.hash (), key2.pub, node->online_reps.delta (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send5.hash ()));
		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open1).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive1).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send3).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send4).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive3).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send5).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send6).code);
		}

		add_callback_stats (*node);

		node->process_active (receive4);
		node->block_processor.flush ();
		node->block_confirm (receive4);
		auto election = node->active.election (receive4->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();

		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 10);

		auto transaction (node->store.tx_begin_read ());
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, receive4->hash ()));
		nano::account_info account_info;
		nano::confirmation_height_info confirmation_height_info;
		ASSERT_FALSE (node->store.account.get (transaction, nano::dev::genesis_key.pub, account_info));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
		ASSERT_EQ (6, confirmation_height_info.height);
		ASSERT_EQ (send5.hash (), confirmation_height_info.frontier);
		ASSERT_EQ (7, account_info.block_count);

		ASSERT_FALSE (node->store.account.get (transaction, key1.pub, account_info));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, key1.pub, confirmation_height_info));
		ASSERT_EQ (5, confirmation_height_info.height);
		ASSERT_EQ (receive4->hash (), confirmation_height_info.frontier);
		ASSERT_EQ (5, account_info.block_count);

		ASSERT_EQ (10, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (10, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (10, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
		ASSERT_EQ (11, node->ledger.cache.cemented_count);

		ASSERT_EQ (0, node->active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

TEST (confirmation_height, send_receive_self)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);
		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));

		nano::send_block send1 (latest, nano::dev::genesis_key.pub, nano::dev::genesis_amount - 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
		nano::receive_block receive1 (send1.hash (), send1.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1.hash ()));
		nano::send_block send2 (receive1.hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (receive1.hash ()));
		nano::send_block send3 (send2.hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 3, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send2.hash ()));

		nano::receive_block receive2 (send3.hash (), send2.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send3.hash ()));
		auto receive3 = std::make_shared<nano::receive_block> (receive2.hash (), send3.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (receive2.hash ()));

		// Send to another account to prevent automatic receiving on the genesis account
		nano::keypair key1;
		nano::send_block send4 (receive3->hash (), key1.pub, node->online_reps.delta (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (receive3->hash ()));
		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send3).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *receive3).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send4).code);
		}

		add_callback_stats (*node);

		node->block_confirm (receive3);
		auto election = node->active.election (receive3->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();

		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 6);

		auto transaction (node->store.tx_begin_read ());
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, receive3->hash ()));
		nano::account_info account_info;
		ASSERT_FALSE (node->store.account.get (transaction, nano::dev::genesis_key.pub, account_info));
		nano::confirmation_height_info confirmation_height_info;
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
		ASSERT_EQ (7, confirmation_height_info.height);
		ASSERT_EQ (receive3->hash (), confirmation_height_info.frontier);
		ASSERT_EQ (8, account_info.block_count);
		ASSERT_EQ (6, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (6, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (6, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
		ASSERT_EQ (confirmation_height_info.height, node->ledger.cache.cemented_count);
		ASSERT_EQ (0, node->active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

TEST (confirmation_height, all_block_types)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);
		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));
		nano::keypair key1;
		nano::keypair key2;
		auto & store = node->store;
		nano::send_block send (latest, key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
		nano::send_block send1 (send.hash (), key2.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send.hash ()));

		nano::open_block open (send.hash (), nano::dev::genesis_key.pub, key1.pub, key1.prv, key1.pub, *system.work.generate (key1.pub));
		nano::state_block state_open (key2.pub, 0, 0, nano::Gxrb_ratio, send1.hash (), key2.prv, key2.pub, *system.work.generate (key2.pub));

		nano::send_block send2 (open.hash (), key2.pub, 0, key1.prv, key1.pub, *system.work.generate (open.hash ()));
		nano::state_block state_receive (key2.pub, state_open.hash (), 0, nano::Gxrb_ratio * 2, send2.hash (), key2.prv, key2.pub, *system.work.generate (state_open.hash ()));

		nano::state_block state_send (key2.pub, state_receive.hash (), 0, nano::Gxrb_ratio, key1.pub, key2.prv, key2.pub, *system.work.generate (state_receive.hash ()));
		nano::receive_block receive (send2.hash (), state_send.hash (), key1.prv, key1.pub, *system.work.generate (send2.hash ()));

		nano::change_block change (receive.hash (), key2.pub, key1.prv, key1.pub, *system.work.generate (receive.hash ()));

		nano::state_block state_change (key2.pub, state_send.hash (), nano::dev::genesis_key.pub, nano::Gxrb_ratio, 0, key2.prv, key2.pub, *system.work.generate (state_send.hash ()));

		nano::state_block epoch (key2.pub, state_change.hash (), nano::dev::genesis_key.pub, nano::Gxrb_ratio, node->ledger.epoch_link (nano::epoch::epoch_1), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (state_change.hash ()));

		nano::state_block epoch1 (key1.pub, change.hash (), key2.pub, nano::Gxrb_ratio, node->ledger.epoch_link (nano::epoch::epoch_1), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (change.hash ()));
		nano::state_block state_send1 (key1.pub, epoch1.hash (), 0, nano::Gxrb_ratio - 1, key2.pub, key1.prv, key1.pub, *system.work.generate (epoch1.hash ()));
		nano::state_block state_receive2 (key2.pub, epoch.hash (), 0, nano::Gxrb_ratio + 1, state_send1.hash (), key2.prv, key2.pub, *system.work.generate (epoch.hash ()));

		auto state_send2 = std::make_shared<nano::state_block> (key2.pub, state_receive2.hash (), 0, nano::Gxrb_ratio, key1.pub, key2.prv, key2.pub, *system.work.generate (state_receive2.hash ()));
		nano::state_block state_send3 (key2.pub, state_send2->hash (), 0, nano::Gxrb_ratio - 1, key1.pub, key2.prv, key2.pub, *system.work.generate (state_send2->hash ()));

		nano::state_block state_send4 (key1.pub, state_send1.hash (), 0, nano::Gxrb_ratio - 2, nano::dev::genesis_key.pub, key1.prv, key1.pub, *system.work.generate (state_send1.hash ()));
		nano::state_block state_receive3 (nano::dev::genesis->account (), send1.hash (), nano::dev::genesis->account (), nano::dev::genesis_amount - nano::Gxrb_ratio * 2 + 1, state_send4.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1.hash ()));

		{
			auto transaction (store.tx_begin_write ());
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, state_open).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, state_receive).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, state_send).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, change).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, state_change).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, epoch).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, epoch1).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, state_send1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, state_receive2).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *state_send2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, state_send3).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, state_send4).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, state_receive3).code);
		}

		add_callback_stats (*node);
		node->block_confirm (state_send2);
		auto election = node->active.election (state_send2->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();

		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 15);

		auto transaction (node->store.tx_begin_read ());
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, state_send2->hash ()));
		nano::account_info account_info;
		nano::confirmation_height_info confirmation_height_info;
		ASSERT_FALSE (node->store.account.get (transaction, nano::dev::genesis_key.pub, account_info));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, nano::dev::genesis_key.pub, confirmation_height_info));
		ASSERT_EQ (3, confirmation_height_info.height);
		ASSERT_EQ (send1.hash (), confirmation_height_info.frontier);
		ASSERT_LE (4, account_info.block_count);

		ASSERT_FALSE (node->store.account.get (transaction, key1.pub, account_info));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, key1.pub, confirmation_height_info));
		ASSERT_EQ (state_send1.hash (), confirmation_height_info.frontier);
		ASSERT_EQ (6, confirmation_height_info.height);
		ASSERT_LE (7, account_info.block_count);

		ASSERT_FALSE (node->store.account.get (transaction, key2.pub, account_info));
		ASSERT_FALSE (node->store.confirmation_height.get (transaction, key2.pub, confirmation_height_info));
		ASSERT_EQ (7, confirmation_height_info.height);
		ASSERT_EQ (state_send2->hash (), confirmation_height_info.frontier);
		ASSERT_LE (8, account_info.block_count);

		ASSERT_EQ (15, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (15, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (15, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
		ASSERT_EQ (16, node->ledger.cache.cemented_count);

		ASSERT_EQ (0, node->active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

/* Bulk of the this test was taken from the node.fork_flip test */
TEST (confirmation_height, conflict_rollback_cemented)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		boost::iostreams::stream_buffer<nano::stringstream_mt_sink> sb;
		sb.open (nano::stringstream_mt_sink{});
		nano::boost_log_cerr_redirect redirect_cerr (&sb);
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		auto node1 = system.add_node (node_flags);
		auto node2 = system.add_node (node_flags);
		ASSERT_EQ (1, node1->network.size ());
		nano::keypair key1;
		auto send1 (std::make_shared<nano::send_block> (nano::dev::genesis->hash (), key1.pub, nano::dev::genesis_amount - 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (nano::dev::genesis->hash ())));
		nano::publish publish1 (send1);
		nano::keypair key2;
		auto send2 (std::make_shared<nano::send_block> (nano::dev::genesis->hash (), key2.pub, nano::dev::genesis_amount - 100, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (nano::dev::genesis->hash ())));
		nano::publish publish2 (send2);
		auto channel1 (node1->network.udp_channels.create (node1->network.endpoint ()));
		node1->network.inbound (publish1, channel1);
		node1->block_processor.flush ();
		node1->scheduler.flush ();
		auto channel2 (node2->network.udp_channels.create (node1->network.endpoint ()));
		node2->network.inbound (publish2, channel2);
		node2->block_processor.flush ();
		node2->scheduler.flush ();
		ASSERT_EQ (1, node1->active.size ());
		ASSERT_EQ (1, node2->active.size ());
		system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
		node1->network.inbound (publish2, channel1);
		node1->block_processor.flush ();
		node2->network.inbound (publish1, channel2);
		node2->block_processor.flush ();
		auto election (node2->active.election (nano::qualified_root (nano::dev::genesis->hash (), nano::dev::genesis->hash ())));
		ASSERT_NE (nullptr, election);
		ASSERT_EQ (1, election->votes ().size ());
		// Force blocks to be cemented on both nodes
		{
			auto transaction (node1->store.tx_begin_write ());
			ASSERT_TRUE (node1->store.block.exists (transaction, publish1.block->hash ()));
			node1->store.confirmation_height.put (transaction, nano::dev::genesis->account (), nano::confirmation_height_info{ 2, send2->hash () });
		}
		{
			auto transaction (node2->store.tx_begin_write ());
			ASSERT_TRUE (node2->store.block.exists (transaction, publish2.block->hash ()));
			node2->store.confirmation_height.put (transaction, nano::dev::genesis->account (), nano::confirmation_height_info{ 2, send2->hash () });
		}

		auto rollback_log_entry = boost::str (boost::format ("Failed to roll back %1%") % send2->hash ().to_string ());
		ASSERT_TIMELY (20s, sb.component ()->str ().find (rollback_log_entry) != std::string::npos);
		auto winner (*election->tally ().begin ());
		ASSERT_EQ (*publish1.block, *winner.second);
		ASSERT_EQ (nano::dev::genesis_amount - 100, winner.first);
		ASSERT_TRUE (node1->ledger.block_or_pruned_exists (publish1.block->hash ()));
		ASSERT_TRUE (node2->ledger.block_or_pruned_exists (publish2.block->hash ()));
		ASSERT_FALSE (node2->ledger.block_or_pruned_exists (publish1.block->hash ()));
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

TEST (confirmation_heightDeathTest, rollback_added_block)
{
	if (nano::rocksdb_config::using_rocksdb_in_tests ())
	{
		// Don't test this in rocksdb mode
		return;
	}
	// For ASSERT_DEATH_IF_SUPPORTED
	testing::FLAGS_gtest_death_test_style = "threadsafe";

	// valgrind can be noisy with death tests
	if (!nano::running_within_valgrind ())
	{
		nano::logger_mt logger;
		nano::logging logging;
		auto path (nano::unique_path ());
		auto store = nano::make_store (logger, path);
		ASSERT_TRUE (!store->init_error ());
		nano::stat stats;
		nano::ledger ledger (*store, stats);
		nano::write_database_queue write_database_queue (false);
		nano::work_pool pool (std::numeric_limits<unsigned>::max ());
		nano::keypair key1;
		auto send = std::make_shared<nano::send_block> (nano::dev::genesis->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *pool.generate (nano::dev::genesis->hash ()));
		{
			auto transaction (store->tx_begin_write ());
			store->initialize (transaction, ledger.cache);
		}

		uint64_t batch_write_size = 2048;
		std::atomic<bool> stopped{ false };
		nano::confirmation_height_unbounded unbounded_processor (
		ledger, write_database_queue, 10ms, logging, logger, stopped, batch_write_size, [] (auto const &) {}, [] (auto const &) {}, [] () { return 0; });

		// Processing a block which doesn't exist should bail
		ASSERT_DEATH_IF_SUPPORTED (unbounded_processor.process (send), "");

		nano::confirmation_height_bounded bounded_processor (
		ledger, write_database_queue, 10ms, logging, logger, stopped, batch_write_size, [] (auto const &) {}, [] (auto const &) {}, [] () { return 0; });
		// Processing a block which doesn't exist should bail
		ASSERT_DEATH_IF_SUPPORTED (bounded_processor.process (send), "");
	}
}

TEST (confirmation_height, observers)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		auto amount (std::numeric_limits<nano::uint128_t>::max ());
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		auto node1 = system.add_node (node_flags);
		nano::keypair key1;
		system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
		nano::block_hash latest1 (node1->latest (nano::dev::genesis_key.pub));
		auto send1 (std::make_shared<nano::send_block> (latest1, key1.pub, amount - node1->config.receive_minimum.number (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest1)));

		add_callback_stats (*node1);

		node1->process_active (send1);
		node1->block_processor.flush ();
		ASSERT_TIMELY (10s, node1->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 1);
		auto transaction = node1->store.tx_begin_read ();
		ASSERT_TRUE (node1->ledger.block_confirmed (transaction, send1->hash ()));
		ASSERT_EQ (1, node1->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (1, node1->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (1, node1->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
		ASSERT_EQ (2, node1->ledger.cache.cemented_count);
		ASSERT_EQ (0, node1->active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

// This tests when a read has been done, but the block no longer exists by the time a write is done
TEST (confirmation_heightDeathTest, modified_chain)
{
	if (nano::rocksdb_config::using_rocksdb_in_tests ())
	{
		// Don't test this in rocksdb mode
		return;
	}
	// For ASSERT_DEATH_IF_SUPPORTED
	testing::FLAGS_gtest_death_test_style = "threadsafe";

	// valgrind can be noisy with death tests
	if (!nano::running_within_valgrind ())
	{
		nano::logging logging;
		nano::logger_mt logger;
		auto path (nano::unique_path ());
		auto store = nano::make_store (logger, path);
		ASSERT_TRUE (!store->init_error ());
		nano::stat stats;
		nano::ledger ledger (*store, stats);
		nano::write_database_queue write_database_queue (false);
		nano::work_pool pool (std::numeric_limits<unsigned>::max ());
		nano::keypair key1;
		auto send = std::make_shared<nano::send_block> (nano::dev::genesis->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *pool.generate (nano::dev::genesis->hash ()));
		{
			auto transaction (store->tx_begin_write ());
			store->initialize (transaction, ledger.cache);
			ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *send).code);
		}

		uint64_t batch_write_size = 2048;
		std::atomic<bool> stopped{ false };
		nano::confirmation_height_bounded bounded_processor (
		ledger, write_database_queue, 10ms, logging, logger, stopped, batch_write_size, [] (auto const &) {}, [] (auto const &) {}, [] () { return 0; });

		{
			// This reads the blocks in the account, but prevents any writes from occuring yet
			auto scoped_write_guard = write_database_queue.wait (nano::writer::testing);
			bounded_processor.process (send);
		}

		// Rollback the block and now try to write, the block no longer exists so should bail
		ledger.rollback (store->tx_begin_write (), send->hash ());
		{
			auto scoped_write_guard = write_database_queue.wait (nano::writer::confirmation_height);
			ASSERT_DEATH_IF_SUPPORTED (bounded_processor.cement_blocks (scoped_write_guard), "");
		}

		ASSERT_EQ (nano::process_result::progress, ledger.process (store->tx_begin_write (), *send).code);
		store->confirmation_height.put (store->tx_begin_write (), nano::dev::genesis->account (), { 1, nano::dev::genesis->hash () });

		nano::confirmation_height_unbounded unbounded_processor (
		ledger, write_database_queue, 10ms, logging, logger, stopped, batch_write_size, [] (auto const &) {}, [] (auto const &) {}, [] () { return 0; });

		{
			// This reads the blocks in the account, but prevents any writes from occuring yet
			auto scoped_write_guard = write_database_queue.wait (nano::writer::testing);
			unbounded_processor.process (send);
		}

		// Rollback the block and now try to write, the block no longer exists so should bail
		ledger.rollback (store->tx_begin_write (), send->hash ());
		{
			auto scoped_write_guard = write_database_queue.wait (nano::writer::confirmation_height);
			ASSERT_DEATH_IF_SUPPORTED (unbounded_processor.cement_blocks (scoped_write_guard), "");
		}
	}
}

// This tests when a read has been done, but the account no longer exists by the time a write is done
TEST (confirmation_heightDeathTest, modified_chain_account_removed)
{
	if (nano::rocksdb_config::using_rocksdb_in_tests ())
	{
		// Don't test this in rocksdb mode
		return;
	}
	// For ASSERT_DEATH_IF_SUPPORTED
	testing::FLAGS_gtest_death_test_style = "threadsafe";

	// valgrind can be noisy with death tests
	if (!nano::running_within_valgrind ())
	{
		nano::logging logging;
		nano::logger_mt logger;
		auto path (nano::unique_path ());
		auto store = nano::make_store (logger, path);
		ASSERT_TRUE (!store->init_error ());
		nano::stat stats;
		nano::ledger ledger (*store, stats);
		nano::write_database_queue write_database_queue (false);
		nano::work_pool pool (std::numeric_limits<unsigned>::max ());
		nano::keypair key1;
		auto send = std::make_shared<nano::send_block> (nano::dev::genesis->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *pool.generate (nano::dev::genesis->hash ()));
		auto open = std::make_shared<nano::state_block> (key1.pub, 0, 0, nano::Gxrb_ratio, send->hash (), key1.prv, key1.pub, *pool.generate (key1.pub));
		{
			auto transaction (store->tx_begin_write ());
			store->initialize (transaction, ledger.cache);
			ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *send).code);
			ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *open).code);
		}

		uint64_t batch_write_size = 2048;
		std::atomic<bool> stopped{ false };
		nano::confirmation_height_unbounded unbounded_processor (
		ledger, write_database_queue, 10ms, logging, logger, stopped, batch_write_size, [] (auto const &) {}, [] (auto const &) {}, [] () { return 0; });

		{
			// This reads the blocks in the account, but prevents any writes from occuring yet
			auto scoped_write_guard = write_database_queue.wait (nano::writer::testing);
			unbounded_processor.process (open);
		}

		// Rollback the block and now try to write, the send should be cemented but the account which the open block belongs no longer exists so should bail
		ledger.rollback (store->tx_begin_write (), open->hash ());
		{
			auto scoped_write_guard = write_database_queue.wait (nano::writer::confirmation_height);
			ASSERT_DEATH_IF_SUPPORTED (unbounded_processor.cement_blocks (scoped_write_guard), "");
		}

		// Reset conditions and test with the bounded processor
		ASSERT_EQ (nano::process_result::progress, ledger.process (store->tx_begin_write (), *open).code);
		store->confirmation_height.put (store->tx_begin_write (), nano::dev::genesis->account (), { 1, nano::dev::genesis->hash () });

		nano::confirmation_height_bounded bounded_processor (
		ledger, write_database_queue, 10ms, logging, logger, stopped, batch_write_size, [] (auto const &) {}, [] (auto const &) {}, [] () { return 0; });

		{
			// This reads the blocks in the account, but prevents any writes from occuring yet
			auto scoped_write_guard = write_database_queue.wait (nano::writer::testing);
			bounded_processor.process (open);
		}

		// Rollback the block and now try to write, the send should be cemented but the account which the open block belongs no longer exists so should bail
		ledger.rollback (store->tx_begin_write (), open->hash ());
		auto scoped_write_guard = write_database_queue.wait (nano::writer::confirmation_height);
		ASSERT_DEATH_IF_SUPPORTED (bounded_processor.cement_blocks (scoped_write_guard), "");
	}
}

namespace nano
{
TEST (confirmation_height, pending_observer_callbacks)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);

		system.wallet (0)->insert_adhoc (nano::dev::genesis_key.prv);
		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));

		nano::keypair key1;
		nano::send_block send (latest, key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
		auto send1 = std::make_shared<nano::send_block> (send.hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send.hash ()));

		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send1).code);
		}

		add_callback_stats (*node);

		node->confirmation_height_processor.add (send1);

		// Confirm the callback is not called under this circumstance because there is no election information
		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 1 && node->ledger.stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::all, nano::stat::dir::out) == 1);

		ASSERT_EQ (2, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (2, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (3, node->ledger.cache.cemented_count);
		ASSERT_EQ (0, node->active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}
}

// The callback and confirmation history should only be updated after confirmation height is set (and not just after voting)
TEST (confirmation_height, callback_confirmed_history)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.force_use_write_database_queue = true;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);

		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));

		nano::keypair key1;
		auto send = std::make_shared<nano::send_block> (latest, key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send).code);
		}

		auto send1 = std::make_shared<nano::send_block> (send->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send->hash ()));

		add_callback_stats (*node);

		node->process_active (send1);
		node->block_processor.flush ();
		node->block_confirm (send1);
		{
			node->process_active (send);
			node->block_processor.flush ();

			// The write guard prevents the confirmation height processor doing any writes
			auto write_guard = node->write_database_queue.wait (nano::writer::testing);

			// Confirm send1
			auto election = node->active.election (send1->qualified_root ());
			ASSERT_NE (nullptr, election);
			election->force_confirm ();
			ASSERT_TIMELY (10s, node->active.size () == 0);
			ASSERT_EQ (0, node->active.list_recently_cemented ().size ());
			{
				nano::lock_guard<nano::mutex> guard (node->active.mutex);
				ASSERT_EQ (0, node->active.blocks.size ());
			}

			auto transaction = node->store.tx_begin_read ();
			ASSERT_FALSE (node->ledger.block_confirmed (transaction, send->hash ()));

			ASSERT_TIMELY (10s, node->write_database_queue.contains (nano::writer::confirmation_height));

			// Confirm that no inactive callbacks have been called when the confirmation height processor has already iterated over it, waiting to write
			ASSERT_EQ (0, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::inactive_conf_height, nano::stat::dir::out));
		}

		ASSERT_TIMELY (10s, !node->write_database_queue.contains (nano::writer::confirmation_height));

		auto transaction = node->store.tx_begin_read ();
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, send->hash ()));

		ASSERT_TIMELY (10s, node->active.size () == 0);
		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::active_quorum, nano::stat::dir::out) == 1);

		ASSERT_EQ (1, node->active.list_recently_cemented ().size ());
		ASSERT_EQ (0, node->active.blocks.size ());

		// Confirm the callback is not called under this circumstance
		ASSERT_EQ (2, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
		ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::active_quorum, nano::stat::dir::out));
		ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::inactive_conf_height, nano::stat::dir::out));
		ASSERT_EQ (2, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (2, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (3, node->ledger.cache.cemented_count);
		ASSERT_EQ (0, node->active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

namespace nano
{
TEST (confirmation_height, dependent_election)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		node_flags.force_use_write_database_queue = true;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);

		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));

		nano::keypair key1;
		auto send = std::make_shared<nano::send_block> (latest, key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
		auto send1 = std::make_shared<nano::send_block> (send->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send->hash ()));
		auto send2 = std::make_shared<nano::send_block> (send1->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 3, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1->hash ()));
		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send2).code);
		}

		add_callback_stats (*node);

		// This election should be confirmed as active_conf_height
		node->block_confirm (send1);
		// Start an election and confirm it
		node->block_confirm (send2);
		auto election = node->active.election (send2->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();

		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 3);

		ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::active_quorum, nano::stat::dir::out));
		ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::active_conf_height, nano::stat::dir::out));
		ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::inactive_conf_height, nano::stat::dir::out));
		ASSERT_EQ (3, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (3, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (4, node->ledger.cache.cemented_count);

		ASSERT_EQ (0, node->active.election_winner_details_size ());
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

// This test checks that a receive block with uncemented blocks below cements them too.
TEST (confirmation_height, cemented_gap_below_receive)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);

		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));

		nano::keypair key1;
		system.wallet (0)->insert_adhoc (key1.prv);

		nano::send_block send (latest, key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
		nano::send_block send1 (send.hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send.hash ()));
		nano::keypair dummy_key;
		nano::send_block dummy_send (send1.hash (), dummy_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 3, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1.hash ()));

		nano::open_block open (send.hash (), nano::dev::genesis->account (), key1.pub, key1.prv, key1.pub, *system.work.generate (key1.pub));
		nano::receive_block receive1 (open.hash (), send1.hash (), key1.prv, key1.pub, *system.work.generate (open.hash ()));
		nano::send_block send2 (receive1.hash (), nano::dev::genesis_key.pub, nano::Gxrb_ratio, key1.prv, key1.pub, *system.work.generate (receive1.hash ()));

		nano::receive_block receive2 (dummy_send.hash (), send2.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (dummy_send.hash ()));
		nano::send_block dummy_send1 (receive2.hash (), dummy_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 3, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (receive2.hash ()));

		nano::keypair key2;
		system.wallet (0)->insert_adhoc (key2.prv);
		nano::send_block send3 (dummy_send1.hash (), key2.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 4, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (dummy_send1.hash ()));
		nano::send_block dummy_send2 (send3.hash (), dummy_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 5, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send3.hash ()));

		auto open1 = std::make_shared<nano::open_block> (send3.hash (), nano::dev::genesis->account (), key2.pub, key2.prv, key2.pub, *system.work.generate (key2.pub));

		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, dummy_send).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send2).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, dummy_send1).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send3).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, dummy_send2).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *open1).code);
		}

		std::vector<nano::block_hash> observer_order;
		nano::mutex mutex;
		add_callback_stats (*node, &observer_order, &mutex);

		node->block_confirm (open1);
		auto election = node->active.election (open1->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 10);

		auto transaction = node->store.tx_begin_read ();
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, open1->hash ()));
		ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::active_quorum, nano::stat::dir::out));
		ASSERT_EQ (0, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::active_conf_height, nano::stat::dir::out));
		ASSERT_EQ (9, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::inactive_conf_height, nano::stat::dir::out));
		ASSERT_EQ (10, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (10, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (11, node->ledger.cache.cemented_count);
		ASSERT_EQ (0, node->active.election_winner_details_size ());

		// Check that the order of callbacks is correct
		std::vector<nano::block_hash> expected_order = { send.hash (), open.hash (), send1.hash (), receive1.hash (), send2.hash (), dummy_send.hash (), receive2.hash (), dummy_send1.hash (), send3.hash (), open1->hash () };
		nano::lock_guard<nano::mutex> guard (mutex);
		ASSERT_EQ (observer_order, expected_order);
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

// This test checks that a receive block with uncemented blocks below cements them too, compared with the test above, this
// is the first write in this chain.
TEST (confirmation_height, cemented_gap_below_no_cache)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);

		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));

		nano::keypair key1;
		system.wallet (0)->insert_adhoc (key1.prv);

		nano::send_block send (latest, key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
		nano::send_block send1 (send.hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send.hash ()));
		nano::keypair dummy_key;
		nano::send_block dummy_send (send1.hash (), dummy_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 3, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1.hash ()));

		nano::open_block open (send.hash (), nano::dev::genesis->account (), key1.pub, key1.prv, key1.pub, *system.work.generate (key1.pub));
		nano::receive_block receive1 (open.hash (), send1.hash (), key1.prv, key1.pub, *system.work.generate (open.hash ()));
		nano::send_block send2 (receive1.hash (), nano::dev::genesis_key.pub, nano::Gxrb_ratio, key1.prv, key1.pub, *system.work.generate (receive1.hash ()));

		nano::receive_block receive2 (dummy_send.hash (), send2.hash (), nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (dummy_send.hash ()));
		nano::send_block dummy_send1 (receive2.hash (), dummy_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 3, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (receive2.hash ()));

		nano::keypair key2;
		system.wallet (0)->insert_adhoc (key2.prv);
		nano::send_block send3 (dummy_send1.hash (), key2.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 4, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (dummy_send1.hash ()));
		nano::send_block dummy_send2 (send3.hash (), dummy_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 5, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send3.hash ()));

		auto open1 = std::make_shared<nano::open_block> (send3.hash (), nano::dev::genesis->account (), key2.pub, key2.prv, key2.pub, *system.work.generate (key2.pub));

		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, dummy_send).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, open).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send2).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, receive2).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, dummy_send1).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, send3).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, dummy_send2).code);

			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *open1).code);
		}

		// Force some blocks to be cemented so that the cached confirmed info variable is empty
		{
			auto transaction (node->store.tx_begin_write ());
			node->store.confirmation_height.put (transaction, nano::dev::genesis->account (), nano::confirmation_height_info{ 3, send1.hash () });
			node->store.confirmation_height.put (transaction, key1.pub, nano::confirmation_height_info{ 2, receive1.hash () });
		}

		add_callback_stats (*node);

		node->block_confirm (open1);
		auto election = node->active.election (open1->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();
		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 6);

		auto transaction = node->store.tx_begin_read ();
		ASSERT_TRUE (node->ledger.block_confirmed (transaction, open1->hash ()));
		ASSERT_EQ (node->active.election_winner_details_size (), 0);
		ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::active_quorum, nano::stat::dir::out));
		ASSERT_EQ (0, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::active_conf_height, nano::stat::dir::out));
		ASSERT_EQ (5, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::inactive_conf_height, nano::stat::dir::out));
		ASSERT_EQ (6, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (6, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (7, node->ledger.cache.cemented_count);
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}

TEST (confirmation_height, election_winner_details_clearing)
{
	auto test_mode = [] (nano::confirmation_height_mode mode_a) {
		nano::system system;
		nano::node_flags node_flags;
		node_flags.confirmation_height_processor_mode = mode_a;
		nano::node_config node_config (nano::get_available_port (), system.logging);
		node_config.frontiers_confirmation = nano::frontiers_confirmation_mode::disabled;
		auto node = system.add_node (node_config, node_flags);

		nano::block_hash latest (node->latest (nano::dev::genesis_key.pub));

		nano::keypair key1;
		auto send = std::make_shared<nano::send_block> (latest, key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (latest));
		auto send1 = std::make_shared<nano::send_block> (send->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send->hash ()));
		auto send2 = std::make_shared<nano::send_block> (send1->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 3, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (send1->hash ()));

		{
			auto transaction = node->store.tx_begin_write ();
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send1).code);
			ASSERT_EQ (nano::process_result::progress, node->ledger.process (transaction, *send2).code);
		}

		add_callback_stats (*node);

		node->block_confirm (send1);
		auto election = node->active.election (send1->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();

		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 2);

		ASSERT_EQ (0, node->active.election_winner_details_size ());
		node->block_confirm (send);
		election = node->active.election (send->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();

		// Wait until this block is confirmed
		ASSERT_TIMELY (10s, node->active.election_winner_details_size () == 1 || node->confirmation_height_processor.current ().is_zero ());

		ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::inactive_conf_height, nano::stat::dir::out));

		node->block_confirm (send2);
		election = node->active.election (send2->qualified_root ());
		ASSERT_NE (nullptr, election);
		election->force_confirm ();

		ASSERT_TIMELY (10s, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out) == 3);

		// Add an already cemented block with fake election details. It should get removed
		node->active.add_election_winner_details (send2->hash (), nullptr);
		node->confirmation_height_processor.add (send2);

		ASSERT_TIMELY (10s, node->active.election_winner_details_size () == 0);

		ASSERT_EQ (1, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::inactive_conf_height, nano::stat::dir::out));
		ASSERT_EQ (3, node->stats.count (nano::stat::type::http_callback, nano::stat::detail::http_callback, nano::stat::dir::out));
		ASSERT_EQ (2, node->stats.count (nano::stat::type::confirmation_observer, nano::stat::detail::active_quorum, nano::stat::dir::out));
		ASSERT_EQ (3, node->stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
		ASSERT_EQ (3, node->stats.count (nano::stat::type::confirmation_height, get_stats_detail (mode_a), nano::stat::dir::in));
		ASSERT_EQ (4, node->ledger.cache.cemented_count);
	};

	test_mode (nano::confirmation_height_mode::bounded);
	test_mode (nano::confirmation_height_mode::unbounded);
}
}

TEST (confirmation_height, election_winner_details_clearing_node_process_confirmed)
{
	// Make sure election_winner_details is also cleared if the block never enters the confirmation height processor from node::process_confirmed
	nano::system system (1);
	auto node = system.nodes.front ();

	auto send = std::make_shared<nano::send_block> (nano::dev::genesis->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *system.work.generate (nano::dev::genesis->hash ()));
	// Add to election_winner_details. Use an unrealistic iteration so that it should fall into the else case and do a cleanup
	node->active.add_election_winner_details (send->hash (), nullptr);
	nano::election_status election;
	election.winner = send;
	node->process_confirmed (election, 1000000);
	ASSERT_EQ (0, node->active.election_winner_details_size ());
}

TEST (confirmation_height, unbounded_block_cache_iteration)
{
	if (nano::rocksdb_config::using_rocksdb_in_tests ())
	{
		// Don't test this in rocksdb mode
		return;
	}
	nano::logger_mt logger;
	auto path (nano::unique_path ());
	auto store = nano::make_store (logger, path);
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	nano::write_database_queue write_database_queue (false);
	boost::latch initialized_latch{ 0 };
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::logging logging;
	nano::keypair key1;
	auto send = std::make_shared<nano::send_block> (nano::dev::genesis->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *pool.generate (nano::dev::genesis->hash ()));
	auto send1 = std::make_shared<nano::send_block> (send->hash (), key1.pub, nano::dev::genesis_amount - nano::Gxrb_ratio * 2, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *pool.generate (send->hash ()));
	{
		auto transaction (store->tx_begin_write ());
		store->initialize (transaction, ledger.cache);
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *send).code);
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *send1).code);
	}

	nano::confirmation_height_processor confirmation_height_processor (ledger, write_database_queue, 10ms, logging, logger, initialized_latch, nano::confirmation_height_mode::unbounded);
	nano::timer<> timer;
	timer.start ();
	{
		// Prevent conf height processor doing any writes, so that we can query is_processing_block correctly
		auto write_guard = write_database_queue.wait (nano::writer::testing);
		// Add the frontier block
		confirmation_height_processor.add (send1);

		// The most uncemented block (previous block) should be seen as processing by the unbounded processor
		while (!confirmation_height_processor.is_processing_block (send->hash ()))
		{
			ASSERT_LT (timer.since_start (), 10s);
		}
	}

	// Wait until the current block is finished processing
	while (!confirmation_height_processor.current ().is_zero ())
	{
		ASSERT_LT (timer.since_start (), 10s);
	}

	ASSERT_EQ (2, stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed, nano::stat::dir::in));
	ASSERT_EQ (2, stats.count (nano::stat::type::confirmation_height, nano::stat::detail::blocks_confirmed_unbounded, nano::stat::dir::in));
	ASSERT_EQ (3, ledger.cache.cemented_count);
}

TEST (confirmation_height, pruned_source)
{
	nano::logger_mt logger;
	nano::logging logging;
	auto path (nano::unique_path ());
	auto store = nano::make_store (logger, path);
	ASSERT_TRUE (!store->init_error ());
	nano::stat stats;
	nano::ledger ledger (*store, stats);
	ledger.pruning = true;
	nano::write_database_queue write_database_queue (false);
	nano::work_pool pool (std::numeric_limits<unsigned>::max ());
	nano::keypair key1, key2;
	auto send1 = std::make_shared<nano::state_block> (nano::dev::genesis_key.pub, nano::dev::genesis->hash (), nano::dev::genesis_key.pub, nano::dev::genesis_amount - 100, key1.pub, nano::dev::genesis_key.prv, nano::dev::genesis_key.pub, *pool.generate (nano::dev::genesis->hash ()));
	auto open1 = std::make_shared<nano::state_block> (key1.pub, 0, key1.pub, 100, send1->hash (), key1.prv, key1.pub, *pool.generate (key1.pub));
	auto send2 = std::make_shared<nano::state_block> (key1.pub, open1->hash (), key1.pub, 50, key2.pub, key1.prv, key1.pub, *pool.generate (open1->hash ()));
	auto send3 = std::make_shared<nano::state_block> (key1.pub, send2->hash (), key1.pub, 25, key2.pub, key1.prv, key1.pub, *pool.generate (send2->hash ()));
	auto open2 = std::make_shared<nano::state_block> (key2.pub, 0, key1.pub, 50, send2->hash (), key2.prv, key2.pub, *pool.generate (key2.pub));
	{
		auto transaction (store->tx_begin_write ());
		store->initialize (transaction, ledger.cache);
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *send1).code);
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *open1).code);
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *send2).code);
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *send3).code);
		ASSERT_EQ (nano::process_result::progress, ledger.process (transaction, *open2).code);
	}
	uint64_t batch_write_size = 2;
	std::atomic<bool> stopped{ false };
	bool first_time{ true };
	nano::confirmation_height_bounded bounded_processor (
	ledger, write_database_queue, 10ms, logging, logger, stopped, batch_write_size, [&] (auto const & cemented_blocks_a) {
		if (first_time)
		{
			// Prune the send
			auto transaction (store->tx_begin_write ());
			ASSERT_EQ (2, ledger.pruning_action (transaction, send2->hash (), 2));
		}
		first_time = false; },
	[] (auto const &) {}, [] () { return 0; });
	bounded_processor.process (open2);
}
