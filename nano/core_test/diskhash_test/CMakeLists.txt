add_executable (
        cpp_wrapper_tests
        entry.cpp
        helper_functions.cpp
        cpp_wrapper_tests.cpp
)

target_link_libraries (
    cpp_wrapper_tests
    diskhash
    gtest
    Boost::filesystem
    Boost::system
)

