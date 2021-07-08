#ifndef __CPP_WRAPPER_TESTS_H__
#define __CPP_WRAPPER_TESTS_H__

#include <boost/filesystem.hpp>
#include <string>
#include <random>

bool db_exists (const boost::filesystem::path & p);
boost::filesystem::path unique_path ();
boost::filesystem::path get_temp_path ();
std::string get_temp_db_path ();
void delete_temp_db_path (boost::filesystem::path temp_path);
std::string random_string(size_t size = 32);


#endif // __CPP_WRAPPER_TESTS_H__
