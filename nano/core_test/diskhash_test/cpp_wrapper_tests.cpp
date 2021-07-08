#include <nano/core_test/diskhash_test/helper_functions.hpp>

#include <gtest/gtest.h>

#include <iostream>
#include <limits>
#include <list>
#include <memory>
#include <string>

#include <diskhash.hpp>

template <
	typename T,
	typename = typename std::enable_if<std::is_integral<T>::value, T>::type
>
std::shared_ptr<dht::DiskHash<T>> get_shared_ptr_to_dht_db (int key_size = 32, dht::OpenMode open_mode = dht::DHOpenRW)
{
	const auto db_path = get_temp_db_path ();
	auto dht_db = std::make_shared<dht::DiskHash<T>> (db_path.c_str (), key_size, open_mode);
	return dht_db;
}

TEST (cpp_wrapper, slow_test)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	uint64_t index = 0;
	std::list<std::string> keys;
	for (auto i = 0; i < 10000; ++i)
	{
		auto key (random_string (key_maxlen));
		keys.emplace_back (key);
		ht->insert (key.c_str (), index++);
	}

	for (auto k = keys.begin (); k != keys.end (); k++)
	{
		auto value = ht->lookup (k->c_str());
		if (!value)
		{
			std::cerr << "Value not found for key: " << *k << std::endl;
		}
	}
}

TEST (cpp_wrapper, successful_insert)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto status (ht->insert (key.c_str (), 1245));
	ASSERT_TRUE (status);
}

TEST (cpp_wrapper, inserting_repeated_key_returns_false)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	ht->insert (key.c_str (), 1245);
	auto status (ht->insert (key.c_str (), 3232));
	ASSERT_FALSE (status);
}

TEST (cpp_wrapper, empty_key_lookup_returns_null)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto value = ht->lookup (key.c_str());
	ASSERT_TRUE (value == nullptr);
}

TEST (cpp_wrapper, filled_key_lookup_returns_value)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto insert_value (uint64_t (123));
	ht->insert (key.c_str (), insert_value);
	auto lookup_value_ptr = ht->lookup (key.c_str());
	ASSERT_EQ (insert_value, *lookup_value_ptr);
}

TEST (cpp_wrapper, is_member_with_existing_key_returns_true)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto insert_value (uint64_t (123));
	ht->insert (key.c_str (), insert_value);

	auto found (ht->is_member (key.c_str ()));
	ASSERT_TRUE (found);
}

TEST (cpp_wrapper, is_member_with_unexisting_key_returns_false)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	auto key (random_string (key_maxlen));
	auto another_key (random_string (key_maxlen));
	ASSERT_NE (key, another_key);

	auto insert_value (uint64_t (123));
	ht->insert (key.c_str (), insert_value);

	auto found (ht->is_member (another_key.c_str ()));
	ASSERT_FALSE (found);
}

TEST (cpp_wrapper, db_creates_ok_with_DHOpenRW)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());

	const auto db_path = get_temp_db_path ();
	auto dht_db = dht::DiskHash<uint64_t> (db_path.c_str (), key_maxlen, dht::DHOpenRW);

	auto exists (db_exists (db_path));
	ASSERT_TRUE (exists);
}

TEST (cpp_wrapper, db_is_not_created_with_DHOpenRWNoCreate_and_returns_exception)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	const auto db_path = get_temp_db_path ();

	EXPECT_THROW (dht::DiskHash<uint64_t> (db_path.c_str (), key_maxlen, dht::DHOpenRWNoCreate), std::runtime_error);
}

TEST (cpp_wrapper, move_constructor)
{
	auto key_maxlen = static_cast<int> (std::to_string (std::numeric_limits<std::uint64_t>::max ()).size ());
	auto ht (get_shared_ptr_to_dht_db<uint64_t> (key_maxlen));

	ht->insert("abc", 123);
	auto another_ht (std::move(*ht));
	ASSERT_TRUE (another_ht.is_member("abc"));
}
