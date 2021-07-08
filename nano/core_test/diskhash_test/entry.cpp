#include <nano/core_test/diskhash_test/helper_functions.hpp>

#include <gtest/gtest.h>

int __main (int argc, char ** argv)
{
	testing::InitGoogleTest (&argc, argv);
	auto res = RUN_ALL_TESTS ();
	delete_temp_db_path (get_temp_path());
	return res;
}
