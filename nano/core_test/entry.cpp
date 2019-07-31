#include <gtest/gtest.h>

TEST (basic, basic)
{
	ASSERT_TRUE (true);
}

TEST (asan, DISABLED_memory)
{
// Ignore warning with gcc/clang compilers
#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif
	uint8_t array[1];
	auto value (array[-0x800000]);
	(void)value;
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
}
