#include "gtest/gtest.h"

#include <nano/node/common.hpp>
#include <nano/node/logging.hpp>

#include <boost/filesystem/path.hpp>
#include <nano/core_test/diskhash_test/helper_functions.hpp>

namespace nano
{
void cleanup_dev_directories_on_exit ();
void force_nano_dev_network ();
boost::filesystem::path unique_path ();
}

GTEST_API_ int main (int argc, char ** argv)
{
	printf ("Running main() from core_test_main.cc\n");
	nano::force_nano_dev_network ();
	nano::node_singleton_memory_pool_purge_guard memory_pool_cleanup_guard;
	// Setting up logging so that there aren't any piped to standard output.
	nano::logging logging;
	logging.init (nano::unique_path ());
	testing::InitGoogleTest (&argc, argv);
	auto res = RUN_ALL_TESTS ();
	nano::cleanup_dev_directories_on_exit ();
	delete_temp_db_path (get_temp_path());
	return res;
}
