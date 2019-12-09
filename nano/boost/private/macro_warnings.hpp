#pragma once

// clang-format off
#ifdef _WIN32
#define DISABLE_ASIO_WARNINGS           \
    __pragma (warning (push))           \
    __pragma (warning (disable : 4191)) \
    __pragma (warning (disable : 4242))

#else
#define DISABLE_ASIO_WARNINGS
#endif

#ifdef _WIN32
#define REENABLE_WARNINGS \
	__pragma (warning (pop))
#else
#define REENABLE_WARNINGS
#endif

#define DISABLE_BEAST_WARNINGS DISABLE_ASIO_WARNINGS

#ifdef _WIN32
#define DISABLE_PROCESS_WARNINGS        \
    __pragma (warning (push))           \
    __pragma (warning (disable : 4191)) \
    __pragma (warning (disable : 4242)) \
    __pragma (warning (disable : 4244))
#else
#define DISABLE_PROCESS_WARNINGS
#endif
// clang-format on
