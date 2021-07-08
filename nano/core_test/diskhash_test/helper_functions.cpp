#include <nano/core_test/diskhash_test/helper_functions.hpp>

#include <boost/filesystem.hpp>

#include <string>

bool db_exists (const boost::filesystem::path & p)
{
	return boost::filesystem::exists (p);
}

boost::filesystem::path unique_path ()
{
	auto unique_path (get_temp_path () / boost::filesystem::unique_path ());
	boost::filesystem::create_directory (unique_path);
	return unique_path;
}

boost::filesystem::path get_temp_path ()
{
	auto current_path (boost::filesystem::current_path () / "temp_db");
	boost::filesystem::create_directory (current_path);
	return current_path;
}

std::string get_temp_db_path ()
{
	auto db_path (unique_path () / "testdb.dht");
	return std::string (db_path.c_str ());
}

void delete_temp_db_path (boost::filesystem::path temp_path)
{
	boost::filesystem::remove_all (temp_path);
}

std::string random_string(size_t size)
{
	std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
	std::random_device rd;
	std::mt19937 generator(rd());
	std::shuffle(str.begin(), str.end(), generator);
	return str.substr(0, (size-1));    // assumes 32 < number of characters in str
}
