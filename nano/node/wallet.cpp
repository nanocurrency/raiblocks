#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/threading.hpp>
#include <nano/lib/utility.hpp>
#include <nano/node/election.hpp>
#include <nano/node/lmdb/lmdb_iterator.hpp>
#include <nano/node/node.hpp>
#include <nano/node/wallet.hpp>

#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/polymorphic_cast.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <future>

#include <argon2.h>

nano::uint256_union nano::wallet_store::check (nano::transaction const & transaction_a)
{
	nano::wallet_value value (entry_get_raw (transaction_a, nano::wallet_store::check_special));
	return value.key;
}

nano::uint256_union nano::wallet_store::salt (nano::transaction const & transaction_a)
{
	nano::wallet_value value (entry_get_raw (transaction_a, nano::wallet_store::salt_special));
	return value.key;
}

void nano::wallet_store::wallet_key (nano::raw_key & prv_a, nano::transaction const & transaction_a)
{
	nano::lock_guard<std::recursive_mutex> lock (mutex);
	nano::raw_key wallet_l;
	wallet_key_mem.value (wallet_l);
	nano::raw_key password_l;
	password.value (password_l);
	prv_a.decrypt (wallet_l.data, password_l, salt (transaction_a).owords[0]);
}

void nano::wallet_store::seed (nano::raw_key & prv_a, nano::transaction const & transaction_a)
{
	nano::wallet_value value (entry_get_raw (transaction_a, nano::wallet_store::seed_special));
	nano::raw_key password_l;
	wallet_key (password_l, transaction_a);
	prv_a.decrypt (value.key, password_l, salt (transaction_a).owords[seed_iv_index]);
}

void nano::wallet_store::seed_set (nano::transaction const & transaction_a, nano::raw_key const & prv_a)
{
	nano::raw_key password_l;
	wallet_key (password_l, transaction_a);
	nano::uint256_union ciphertext;
	ciphertext.encrypt (prv_a, password_l, salt (transaction_a).owords[seed_iv_index]);
	entry_put_raw (transaction_a, nano::wallet_store::seed_special, nano::wallet_value (ciphertext, 0));
	deterministic_clear (transaction_a);
}

nano::public_key nano::wallet_store::deterministic_insert (nano::transaction const & transaction_a)
{
	auto index (deterministic_index_get (transaction_a));
	auto prv = deterministic_key (transaction_a, index);
	nano::public_key result (nano::pub_key (prv));
	while (exists (transaction_a, result))
	{
		++index;
		prv = deterministic_key (transaction_a, index);
		result = nano::pub_key (prv);
	}
	uint64_t marker (1);
	marker <<= 32;
	marker |= index;
	entry_put_raw (transaction_a, result, nano::wallet_value (nano::uint256_union (marker), 0));
	++index;
	deterministic_index_set (transaction_a, index);
	return result;
}

nano::public_key nano::wallet_store::deterministic_insert (nano::transaction const & transaction_a, uint32_t const index)
{
	auto prv = deterministic_key (transaction_a, index);
	nano::public_key result (nano::pub_key (prv));
	uint64_t marker (1);
	marker <<= 32;
	marker |= index;
	entry_put_raw (transaction_a, result, nano::wallet_value (nano::uint256_union (marker), 0));
	return result;
}

nano::private_key nano::wallet_store::deterministic_key (nano::transaction const & transaction_a, uint32_t index_a)
{
	assert (valid_password (transaction_a));
	nano::raw_key seed_l;
	seed (seed_l, transaction_a);
	return nano::deterministic_key (seed_l, index_a);
}

uint32_t nano::wallet_store::deterministic_index_get (nano::transaction const & transaction_a)
{
	nano::wallet_value value (entry_get_raw (transaction_a, nano::wallet_store::deterministic_index_special));
	return static_cast<uint32_t> (value.key.number () & static_cast<uint32_t> (-1));
}

void nano::wallet_store::deterministic_index_set (nano::transaction const & transaction_a, uint32_t index_a)
{
	nano::uint256_union index_l (index_a);
	nano::wallet_value value (index_l, 0);
	entry_put_raw (transaction_a, nano::wallet_store::deterministic_index_special, value);
}

void nano::wallet_store::deterministic_clear (nano::transaction const & transaction_a)
{
	nano::uint256_union key (0);
	for (auto i (begin (transaction_a)), n (end ()); i != n;)
	{
		switch (key_type (nano::wallet_value (i->second)))
		{
			case nano::key_type::deterministic:
			{
				auto const & key (i->first);
				erase (transaction_a, key);
				i = begin (transaction_a, key);
				break;
			}
			default:
			{
				++i;
				break;
			}
		}
	}
	deterministic_index_set (transaction_a, 0);
}

bool nano::wallet_store::valid_password (nano::transaction const & transaction_a)
{
	nano::raw_key zero;
	zero.data.clear ();
	nano::raw_key wallet_key_l;
	wallet_key (wallet_key_l, transaction_a);
	nano::uint256_union check_l;
	check_l.encrypt (zero, wallet_key_l, salt (transaction_a).owords[check_iv_index]);
	bool ok = check (transaction_a) == check_l;
	return ok;
}

bool nano::wallet_store::attempt_password (nano::transaction const & transaction_a, std::string const & password_a)
{
	bool result = false;
	{
		nano::lock_guard<std::recursive_mutex> lock (mutex);
		nano::raw_key password_l;
		derive_key (password_l, transaction_a, password_a);
		password.value_set (password_l);
		result = !valid_password (transaction_a);
	}
	if (!result)
	{
		switch (version (transaction_a))
		{
			case version_1:
				upgrade_v1_v2 (transaction_a);
			case version_2:
				upgrade_v2_v3 (transaction_a);
			case version_3:
				upgrade_v3_v4 (transaction_a);
			case version_4:
				break;
			default:
				assert (false);
		}
	}
	return result;
}

bool nano::wallet_store::rekey (nano::transaction const & transaction_a, std::string const & password_a)
{
	nano::lock_guard<std::recursive_mutex> lock (mutex);
	bool result (false);
	if (valid_password (transaction_a))
	{
		nano::raw_key password_new;
		derive_key (password_new, transaction_a, password_a);
		nano::raw_key wallet_key_l;
		wallet_key (wallet_key_l, transaction_a);
		nano::raw_key password_l;
		password.value (password_l);
		password.value_set (password_new);
		nano::uint256_union encrypted;
		encrypted.encrypt (wallet_key_l, password_new, salt (transaction_a).owords[0]);
		nano::raw_key wallet_enc;
		wallet_enc.data = encrypted;
		wallet_key_mem.value_set (wallet_enc);
		entry_put_raw (transaction_a, nano::wallet_store::wallet_key_special, nano::wallet_value (encrypted, 0));
	}
	else
	{
		result = true;
	}
	return result;
}

void nano::wallet_store::derive_key (nano::raw_key & prv_a, nano::transaction const & transaction_a, std::string const & password_a)
{
	auto salt_l (salt (transaction_a));
	kdf.phs (prv_a, password_a, salt_l);
}

nano::fan::fan (nano::uint256_union const & key, size_t count_a)
{
	auto first (std::make_unique<nano::uint256_union> (key));
	for (auto i (1); i < count_a; ++i)
	{
		auto entry (std::make_unique<nano::uint256_union> ());
		nano::random_pool::generate_block (entry->bytes.data (), entry->bytes.size ());
		*first ^= *entry;
		values.push_back (std::move (entry));
	}
	values.push_back (std::move (first));
}

void nano::fan::value (nano::raw_key & prv_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	value_get (prv_a);
}

void nano::fan::value_get (nano::raw_key & prv_a)
{
	assert (!mutex.try_lock ());
	prv_a.data.clear ();
	for (auto & i : values)
	{
		prv_a.data ^= *i;
	}
}

void nano::fan::value_set (nano::raw_key const & value_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	nano::raw_key value_l;
	value_get (value_l);
	*(values[0]) ^= value_l.data;
	*(values[0]) ^= value_a.data;
}

// Wallet version number
nano::account const nano::wallet_store::version_special (0);
// Random number used to salt private key encryption
nano::account const nano::wallet_store::salt_special (1);
// Key used to encrypt wallet keys, encrypted itself by the user password
nano::account const nano::wallet_store::wallet_key_special (2);
// Check value used to see if password is valid
nano::account const nano::wallet_store::check_special (3);
// Representative account to be used if we open a new account
nano::account const nano::wallet_store::representative_special (4);
// Wallet seed for deterministic key generation
nano::account const nano::wallet_store::seed_special (5);
// Current key index for deterministic keys
nano::account const nano::wallet_store::deterministic_index_special (6);
int const nano::wallet_store::special_count (7);
size_t const nano::wallet_store::check_iv_index (0);
size_t const nano::wallet_store::seed_iv_index (1);

nano::wallet_store::wallet_store (bool & init_a, nano::kdf & kdf_a, nano::transaction & transaction_a, nano::account representative_a, unsigned fanout_a, std::string const & wallet_a, std::string const & json_a)
	: password (0, fanout_a)
	, wallet_key_mem (0, fanout_a)
	, kdf (kdf_a)
{
	init_a = false;
	initialize (transaction_a, init_a, wallet_a);
	if (!init_a)
	{
		MDB_val junk;
		assert (mdb_get (tx (transaction_a), handle, nano::mdb_val (version_special), &junk) == MDB_NOTFOUND);
		boost::property_tree::ptree wallet_l;
		std::stringstream istream (json_a);
		try
		{
			boost::property_tree::read_json (istream, wallet_l);
		}
		catch (...)
		{
			init_a = true;
		}
		for (auto i (wallet_l.begin ()), n (wallet_l.end ()); i != n; ++i)
		{
			nano::account key;
			init_a = key.decode_hex (i->first);
			if (!init_a)
			{
				nano::uint256_union value;
				init_a = value.decode_hex (wallet_l.get<std::string> (i->first));
				if (!init_a)
				{
					entry_put_raw (transaction_a, key, nano::wallet_value (value, 0));
				}
				else
				{
					init_a = true;
				}
			}
			else
			{
				init_a = true;
			}
		}
		init_a |= mdb_get (tx (transaction_a), handle, nano::mdb_val (version_special), &junk) != 0;
		init_a |= mdb_get (tx (transaction_a), handle, nano::mdb_val (wallet_key_special), &junk) != 0;
		init_a |= mdb_get (tx (transaction_a), handle, nano::mdb_val (salt_special), &junk) != 0;
		init_a |= mdb_get (tx (transaction_a), handle, nano::mdb_val (check_special), &junk) != 0;
		init_a |= mdb_get (tx (transaction_a), handle, nano::mdb_val (representative_special), &junk) != 0;
		nano::raw_key key;
		key.data.clear ();
		password.value_set (key);
		key.data = entry_get_raw (transaction_a, nano::wallet_store::wallet_key_special).key;
		wallet_key_mem.value_set (key);
	}
}

nano::wallet_store::wallet_store (bool & init_a, nano::kdf & kdf_a, nano::transaction & transaction_a, nano::account representative_a, unsigned fanout_a, std::string const & wallet_a)
	: password (0, fanout_a)
	, wallet_key_mem (0, fanout_a)
	, kdf (kdf_a)
{
	init_a = false;
	initialize (transaction_a, init_a, wallet_a);
	if (!init_a)
	{
		int version_status;
		MDB_val version_value;
		version_status = mdb_get (tx (transaction_a), handle, nano::mdb_val (version_special), &version_value);
		if (version_status == MDB_NOTFOUND)
		{
			version_put (transaction_a, version_current);
			nano::uint256_union salt_l;
			random_pool::generate_block (salt_l.bytes.data (), salt_l.bytes.size ());
			entry_put_raw (transaction_a, nano::wallet_store::salt_special, nano::wallet_value (salt_l, 0));
			// Wallet key is a fixed random key that encrypts all entries
			nano::raw_key wallet_key;
			random_pool::generate_block (wallet_key.data.bytes.data (), sizeof (wallet_key.data.bytes));
			nano::raw_key password_l;
			password_l.data.clear ();
			password.value_set (password_l);
			nano::raw_key zero;
			zero.data.clear ();
			// Wallet key is encrypted by the user's password
			nano::uint256_union encrypted;
			encrypted.encrypt (wallet_key, zero, salt_l.owords[0]);
			entry_put_raw (transaction_a, nano::wallet_store::wallet_key_special, nano::wallet_value (encrypted, 0));
			nano::raw_key wallet_key_enc;
			wallet_key_enc.data = encrypted;
			wallet_key_mem.value_set (wallet_key_enc);
			nano::uint256_union check;
			check.encrypt (zero, wallet_key, salt_l.owords[check_iv_index]);
			entry_put_raw (transaction_a, nano::wallet_store::check_special, nano::wallet_value (check, 0));
			entry_put_raw (transaction_a, nano::wallet_store::representative_special, nano::wallet_value (representative_a, 0));
			nano::raw_key seed;
			random_pool::generate_block (seed.data.bytes.data (), seed.data.bytes.size ());
			seed_set (transaction_a, seed);
			entry_put_raw (transaction_a, nano::wallet_store::deterministic_index_special, nano::wallet_value (nano::uint256_union (0), 0));
		}
	}
	nano::raw_key key;
	key.data = entry_get_raw (transaction_a, nano::wallet_store::wallet_key_special).key;
	wallet_key_mem.value_set (key);
}

std::vector<nano::account> nano::wallet_store::accounts (nano::transaction const & transaction_a)
{
	std::vector<nano::account> result;
	for (auto i (begin (transaction_a)), n (end ()); i != n; ++i)
	{
		nano::account const & account (i->first);
		result.push_back (account);
	}
	return result;
}

void nano::wallet_store::initialize (nano::transaction const & transaction_a, bool & init_a, std::string const & path_a)
{
	assert (strlen (path_a.c_str ()) == path_a.size ());
	auto error (0);
	error |= mdb_dbi_open (tx (transaction_a), path_a.c_str (), MDB_CREATE, &handle);
	init_a = error != 0;
}

bool nano::wallet_store::is_representative (nano::transaction const & transaction_a)
{
	return exists (transaction_a, representative (transaction_a));
}

void nano::wallet_store::representative_set (nano::transaction const & transaction_a, nano::account const & representative_a)
{
	entry_put_raw (transaction_a, nano::wallet_store::representative_special, nano::wallet_value (representative_a, 0));
}

nano::account nano::wallet_store::representative (nano::transaction const & transaction_a)
{
	nano::wallet_value value (entry_get_raw (transaction_a, nano::wallet_store::representative_special));
	return reinterpret_cast<nano::account const &> (value.key);
}

nano::public_key nano::wallet_store::insert_adhoc (nano::transaction const & transaction_a, nano::raw_key const & prv)
{
	assert (valid_password (transaction_a));
	nano::public_key pub (nano::pub_key (prv.as_private_key ()));
	nano::raw_key password_l;
	wallet_key (password_l, transaction_a);
	nano::private_key ciphertext;
	ciphertext.encrypt (prv, password_l, pub.owords[0].number ());
	entry_put_raw (transaction_a, pub, nano::wallet_value (ciphertext, 0));
	return pub;
}

bool nano::wallet_store::insert_watch (nano::transaction const & transaction_a, nano::account const & pub_a)
{
	bool error (!valid_public_key (pub_a));
	if (!error)
	{
		entry_put_raw (transaction_a, pub_a, nano::wallet_value (nano::private_key (0), 0));
	}
	return error;
}

void nano::wallet_store::erase (nano::transaction const & transaction_a, nano::account const & pub)
{
	auto status (mdb_del (tx (transaction_a), handle, nano::mdb_val (pub), nullptr));
	(void)status;
	assert (status == 0);
}

nano::wallet_value nano::wallet_store::entry_get_raw (nano::transaction const & transaction_a, nano::account const & pub_a)
{
	nano::wallet_value result;
	nano::mdb_val value;
	auto status (mdb_get (tx (transaction_a), handle, nano::mdb_val (pub_a), value));
	if (status == 0)
	{
		result = nano::wallet_value (value);
	}
	else
	{
		result.key.clear ();
		result.work = 0;
	}
	return result;
}

void nano::wallet_store::entry_put_raw (nano::transaction const & transaction_a, nano::account const & pub_a, nano::wallet_value const & entry_a)
{
	auto status (mdb_put (tx (transaction_a), handle, nano::mdb_val (pub_a), nano::mdb_val (sizeof (entry_a), const_cast<nano::wallet_value *> (&entry_a)), 0));
	(void)status;
	assert (status == 0);
}

nano::key_type nano::wallet_store::key_type (nano::wallet_value const & value_a)
{
	auto number (value_a.key.number ());
	nano::key_type result;
	auto text (number.convert_to<std::string> ());
	if (number > std::numeric_limits<uint64_t>::max ())
	{
		result = nano::key_type::adhoc;
	}
	else
	{
		if ((number >> 32).convert_to<uint32_t> () == 1)
		{
			result = nano::key_type::deterministic;
		}
		else
		{
			result = nano::key_type::unknown;
		}
	}
	return result;
}

bool nano::wallet_store::fetch (nano::transaction const & transaction_a, nano::account const & pub, nano::raw_key & prv)
{
	auto result (false);
	if (valid_password (transaction_a))
	{
		nano::wallet_value value (entry_get_raw (transaction_a, pub));
		if (!value.key.is_zero ())
		{
			switch (key_type (value))
			{
				case nano::key_type::deterministic:
				{
					nano::raw_key seed_l;
					seed (seed_l, transaction_a);
					uint32_t index (static_cast<uint32_t> (value.key.number () & static_cast<uint32_t> (-1)));
					prv.data = deterministic_key (transaction_a, index);
					break;
				}
				case nano::key_type::adhoc:
				{
					// Ad-hoc keys
					nano::raw_key password_l;
					wallet_key (password_l, transaction_a);
					prv.decrypt (value.key, password_l, pub.owords[0].number ());
					break;
				}
				default:
				{
					result = true;
					break;
				}
			}
		}
		else
		{
			result = true;
		}
	}
	else
	{
		result = true;
	}
	if (!result)
	{
		nano::public_key compare (nano::pub_key (prv.as_private_key ()));
		if (!(pub == compare))
		{
			result = true;
		}
	}
	return result;
}

bool nano::wallet_store::valid_public_key (nano::public_key const & pub)
{
	return pub.number () >= special_count;
}

bool nano::wallet_store::exists (nano::transaction const & transaction_a, nano::public_key const & pub)
{
	return valid_public_key (pub) && find (transaction_a, pub) != end ();
}

void nano::wallet_store::serialize_json (nano::transaction const & transaction_a, std::string & string_a)
{
	boost::property_tree::ptree tree;
	for (nano::store_iterator<nano::uint256_union, nano::wallet_value> i (std::make_unique<nano::mdb_iterator<nano::uint256_union, nano::wallet_value>> (transaction_a, handle)), n (nullptr); i != n; ++i)
	{
		tree.put (i->first.to_string (), i->second.key.to_string ());
	}
	std::stringstream ostream;
	boost::property_tree::write_json (ostream, tree);
	string_a = ostream.str ();
}

void nano::wallet_store::write_backup (nano::transaction const & transaction_a, boost::filesystem::path const & path_a)
{
	std::ofstream backup_file;
	backup_file.open (path_a.string ());
	if (!backup_file.fail ())
	{
		// Set permissions to 600
		boost::system::error_code ec;
		nano::set_secure_perm_file (path_a, ec);

		std::string json;
		serialize_json (transaction_a, json);
		backup_file << json;
	}
}

bool nano::wallet_store::move (nano::transaction const & transaction_a, nano::wallet_store & other_a, std::vector<nano::public_key> const & keys)
{
	assert (valid_password (transaction_a));
	assert (other_a.valid_password (transaction_a));
	auto result (false);
	for (auto i (keys.begin ()), n (keys.end ()); i != n; ++i)
	{
		nano::raw_key prv;
		auto error (other_a.fetch (transaction_a, *i, prv));
		result = result | error;
		if (!result)
		{
			insert_adhoc (transaction_a, prv);
			other_a.erase (transaction_a, *i);
		}
	}
	return result;
}

bool nano::wallet_store::import (nano::transaction const & transaction_a, nano::wallet_store & other_a)
{
	assert (valid_password (transaction_a));
	assert (other_a.valid_password (transaction_a));
	auto result (false);
	for (auto i (other_a.begin (transaction_a)), n (end ()); i != n; ++i)
	{
		nano::raw_key prv;
		auto error (other_a.fetch (transaction_a, i->first, prv));
		result = result | error;
		if (!result)
		{
			if (!prv.data.is_zero ())
			{
				insert_adhoc (transaction_a, prv);
			}
			else
			{
				insert_watch (transaction_a, i->first);
			}
			other_a.erase (transaction_a, i->first);
		}
	}
	return result;
}

bool nano::wallet_store::work_get (nano::transaction const & transaction_a, nano::public_key const & pub_a, uint64_t & work_a)
{
	auto result (false);
	auto entry (entry_get_raw (transaction_a, pub_a));
	if (!entry.key.is_zero ())
	{
		work_a = entry.work;
	}
	else
	{
		result = true;
	}
	return result;
}

void nano::wallet_store::work_put (nano::transaction const & transaction_a, nano::public_key const & pub_a, uint64_t work_a)
{
	auto entry (entry_get_raw (transaction_a, pub_a));
	assert (!entry.key.is_zero ());
	entry.work = work_a;
	entry_put_raw (transaction_a, pub_a, entry);
}

unsigned nano::wallet_store::version (nano::transaction const & transaction_a)
{
	nano::wallet_value value (entry_get_raw (transaction_a, nano::wallet_store::version_special));
	auto entry (value.key);
	auto result (static_cast<unsigned> (entry.bytes[31]));
	return result;
}

void nano::wallet_store::version_put (nano::transaction const & transaction_a, unsigned version_a)
{
	nano::uint256_union entry (version_a);
	entry_put_raw (transaction_a, nano::wallet_store::version_special, nano::wallet_value (entry, 0));
}

void nano::wallet_store::upgrade_v1_v2 (nano::transaction const & transaction_a)
{
	assert (version (transaction_a) == 1);
	nano::raw_key zero_password;
	nano::wallet_value value (entry_get_raw (transaction_a, nano::wallet_store::wallet_key_special));
	nano::raw_key kdf;
	kdf.data.clear ();
	zero_password.decrypt (value.key, kdf, salt (transaction_a).owords[0]);
	derive_key (kdf, transaction_a, "");
	nano::raw_key empty_password;
	empty_password.decrypt (value.key, kdf, salt (transaction_a).owords[0]);
	for (auto i (begin (transaction_a)), n (end ()); i != n; ++i)
	{
		nano::public_key const & key (i->first);
		nano::raw_key prv;
		if (fetch (transaction_a, key, prv))
		{
			// Key failed to decrypt despite valid password
			nano::wallet_value data (entry_get_raw (transaction_a, key));
			prv.decrypt (data.key, zero_password, salt (transaction_a).owords[0]);
			nano::public_key compare (nano::pub_key (prv.as_private_key ()));
			if (compare == key)
			{
				// If we successfully decrypted it, rewrite the key back with the correct wallet key
				insert_adhoc (transaction_a, prv);
			}
			else
			{
				// Also try the empty password
				nano::wallet_value data (entry_get_raw (transaction_a, key));
				prv.decrypt (data.key, empty_password, salt (transaction_a).owords[0]);
				nano::public_key compare (nano::pub_key (prv.as_private_key ()));
				if (compare == key)
				{
					// If we successfully decrypted it, rewrite the key back with the correct wallet key
					insert_adhoc (transaction_a, prv);
				}
			}
		}
	}
	version_put (transaction_a, 2);
}

void nano::wallet_store::upgrade_v2_v3 (nano::transaction const & transaction_a)
{
	assert (version (transaction_a) == 2);
	nano::raw_key seed;
	random_pool::generate_block (seed.data.bytes.data (), seed.data.bytes.size ());
	seed_set (transaction_a, seed);
	entry_put_raw (transaction_a, nano::wallet_store::deterministic_index_special, nano::wallet_value (nano::uint256_union (0), 0));
	version_put (transaction_a, 3);
}

void nano::wallet_store::upgrade_v3_v4 (nano::transaction const & transaction_a)
{
	assert (version (transaction_a) == 3);
	version_put (transaction_a, 4);
	assert (valid_password (transaction_a));
	nano::raw_key seed;
	nano::wallet_value value (entry_get_raw (transaction_a, nano::wallet_store::seed_special));
	nano::raw_key password_l;
	wallet_key (password_l, transaction_a);
	seed.decrypt (value.key, password_l, salt (transaction_a).owords[0]);
	nano::uint256_union ciphertext;
	ciphertext.encrypt (seed, password_l, salt (transaction_a).owords[seed_iv_index]);
	entry_put_raw (transaction_a, nano::wallet_store::seed_special, nano::wallet_value (ciphertext, 0));
	for (auto i (begin (transaction_a)), n (end ()); i != n; ++i)
	{
		nano::wallet_value value (i->second);
		if (!value.key.is_zero ())
		{
			switch (key_type (i->second))
			{
				case nano::key_type::adhoc:
				{
					nano::raw_key key;
					if (fetch (transaction_a, nano::public_key (i->first), key))
					{
						// Key failed to decrypt despite valid password
						key.decrypt (value.key, password_l, salt (transaction_a).owords[0]);
						nano::uint256_union new_key_ciphertext;
						new_key_ciphertext.encrypt (key, password_l, (nano::uint256_union (i->first)).owords[0].number ());
						nano::wallet_value new_value (new_key_ciphertext, value.work);
						erase (transaction_a, nano::public_key (i->first));
						entry_put_raw (transaction_a, nano::public_key (i->first), new_value);
					}
				}
				case nano::key_type::deterministic:
					break;
				default:
					assert (false);
			}
		}
	}
}

void nano::kdf::phs (nano::raw_key & result_a, std::string const & password_a, nano::uint256_union const & salt_a)
{
	static nano::network_params network_params;
	nano::lock_guard<std::mutex> lock (mutex);
	auto success (argon2_hash (1, network_params.kdf_work, 1, password_a.data (), password_a.size (), salt_a.bytes.data (), salt_a.bytes.size (), result_a.data.bytes.data (), result_a.data.bytes.size (), NULL, 0, Argon2_d, 0x10));
	assert (success == 0);
	(void)success;
}

nano::wallet::wallet (bool & init_a, nano::transaction & transaction_a, nano::wallets & wallets_a, std::string const & wallet_a)
	: lock_observer ([](bool, bool) {})
	, store (init_a, wallets_a.kdf, transaction_a, wallets_a.node.config.random_representative (), wallets_a.node.config.password_fanout, wallet_a)
	, wallets (wallets_a)
{
}

nano::wallet::wallet (bool & init_a, nano::transaction & transaction_a, nano::wallets & wallets_a, std::string const & wallet_a, std::string const & json)
	: lock_observer ([](bool, bool) {})
	, store (init_a, wallets_a.kdf, transaction_a, wallets_a.node.config.random_representative (), wallets_a.node.config.password_fanout, wallet_a, json)
	, wallets (wallets_a)
{
}

void nano::wallet::enter_initial_password ()
{
	nano::raw_key password_l;
	{
		nano::lock_guard<std::recursive_mutex> lock (store.mutex);
		store.password.value (password_l);
	}
	if (password_l.data.is_zero ())
	{
		auto transaction (wallets.tx_begin_write ());
		if (store.valid_password (transaction))
		{
			// Newly created wallets have a zero key
			store.rekey (transaction, "");
		}
		else
		{
			enter_password (transaction, "");
		}
	}
}

bool nano::wallet::enter_password (nano::transaction const & transaction_a, std::string const & password_a)
{
	auto result (store.attempt_password (transaction_a, password_a));
	if (!result)
	{
		auto this_l (shared_from_this ());
		wallets.node.background ([this_l]() {
			this_l->search_pending ();
		});
		wallets.node.logger.try_log ("Wallet unlocked");
	}
	else
	{
		wallets.node.logger.try_log ("Invalid password, wallet locked");
	}
	lock_observer (result, password_a.empty ());
	return result;
}

nano::public_key nano::wallet::deterministic_insert (nano::transaction const & transaction_a, bool generate_work_a)
{
	nano::public_key key (0);
	if (store.valid_password (transaction_a))
	{
		key = store.deterministic_insert (transaction_a);
		if (generate_work_a)
		{
			work_ensure (key, key);
		}
		auto half_principal_weight (wallets.node.minimum_principal_weight () / 2);
		if (wallets.check_rep (key, half_principal_weight))
		{
			nano::lock_guard<std::mutex> lock (representatives_mutex);
			representatives.insert (key);
		}
	}
	return key;
}

nano::public_key nano::wallet::deterministic_insert (uint32_t const index, bool generate_work_a)
{
	auto transaction (wallets.tx_begin_write ());
	nano::public_key key (0);
	if (store.valid_password (transaction))
	{
		key = store.deterministic_insert (transaction, index);
		if (generate_work_a)
		{
			work_ensure (key, key);
		}
	}
	return key;
}

nano::public_key nano::wallet::deterministic_insert (bool generate_work_a)
{
	auto transaction (wallets.tx_begin_write ());
	auto result (deterministic_insert (transaction, generate_work_a));
	return result;
}

nano::public_key nano::wallet::insert_adhoc (nano::transaction const & transaction_a, nano::raw_key const & key_a, bool generate_work_a)
{
	nano::public_key key (0);
	if (store.valid_password (transaction_a))
	{
		key = store.insert_adhoc (transaction_a, key_a);
		auto block_transaction (wallets.node.store.tx_begin_read ());
		if (generate_work_a)
		{
			work_ensure (key, wallets.node.ledger.latest_root (block_transaction, key));
		}
		auto half_principal_weight (wallets.node.minimum_principal_weight () / 2);
		if (wallets.check_rep (key, half_principal_weight))
		{
			nano::lock_guard<std::mutex> lock (representatives_mutex);
			representatives.insert (key);
		}
	}
	return key;
}

nano::public_key nano::wallet::insert_adhoc (nano::raw_key const & account_a, bool generate_work_a)
{
	auto transaction (wallets.tx_begin_write ());
	auto result (insert_adhoc (transaction, account_a, generate_work_a));
	return result;
}

bool nano::wallet::insert_watch (nano::transaction const & transaction_a, nano::public_key const & pub_a)
{
	return store.insert_watch (transaction_a, pub_a);
}

bool nano::wallet::exists (nano::public_key const & account_a)
{
	auto transaction (wallets.tx_begin_read ());
	return store.exists (transaction, account_a);
}

bool nano::wallet::import (std::string const & json_a, std::string const & password_a)
{
	auto error (false);
	std::unique_ptr<nano::wallet_store> temp;
	{
		auto transaction (wallets.tx_begin_write ());
		nano::uint256_union id;
		random_pool::generate_block (id.bytes.data (), id.bytes.size ());
		temp = std::make_unique<nano::wallet_store> (error, wallets.node.wallets.kdf, transaction, 0, 1, id.to_string (), json_a);
	}
	if (!error)
	{
		auto transaction (wallets.tx_begin_write ());
		error = temp->attempt_password (transaction, password_a);
	}
	auto transaction (wallets.tx_begin_write ());
	if (!error)
	{
		error = store.import (transaction, *temp);
	}
	temp->destroy (transaction);
	return error;
}

void nano::wallet::serialize (std::string & json_a)
{
	auto transaction (wallets.tx_begin_read ());
	store.serialize_json (transaction, json_a);
}

void nano::wallet_store::destroy (nano::transaction const & transaction_a)
{
	auto status (mdb_drop (tx (transaction_a), handle, 1));
	(void)status;
	assert (status == 0);
	handle = 0;
}

std::shared_ptr<nano::block> nano::wallet::receive_action (nano::block const & send_a, nano::account const & representative_a, nano::uint128_union const & amount_a, uint64_t work_a, bool generate_work_a)
{
	nano::account account;
	auto hash (send_a.hash ());
	std::shared_ptr<nano::block> block;
	if (wallets.node.config.receive_minimum.number () <= amount_a.number ())
	{
		auto block_transaction (wallets.node.ledger.store.tx_begin_read ());
		auto transaction (wallets.tx_begin_read ());
		nano::pending_info pending_info;
		if (wallets.node.store.block_exists (block_transaction, hash))
		{
			account = wallets.node.ledger.block_destination (block_transaction, send_a);
			if (!wallets.node.ledger.store.pending_get (block_transaction, nano::pending_key (account, hash), pending_info))
			{
				nano::raw_key prv;
				if (!store.fetch (transaction, account, prv))
				{
					if (work_a == 0)
					{
						store.work_get (transaction, account, work_a);
					}
					nano::account_info info;
					auto new_account (wallets.node.ledger.store.account_get (block_transaction, account, info));
					if (!new_account)
					{
						block = std::make_shared<nano::state_block> (account, info.head, info.representative, info.balance.number () + pending_info.amount.number (), hash, prv, account, work_a);
					}
					else
					{
						block = std::make_shared<nano::state_block> (account, 0, representative_a, pending_info.amount, reinterpret_cast<nano::link const &> (hash), prv, account, work_a);
					}
				}
				else
				{
					wallets.node.logger.try_log ("Unable to receive, wallet locked");
				}
			}
			else
			{
				// Ledger doesn't have this marked as available to receive anymore
			}
		}
		else
		{
			// Ledger doesn't have this block anymore.
		}
	}
	else
	{
		wallets.node.logger.try_log (boost::str (boost::format ("Not receiving block %1% due to minimum receive threshold") % hash.to_string ()));
		// Someone sent us something below the threshold of receiving
	}
	if (block != nullptr)
	{
		if (action_complete (block, account, generate_work_a))
		{
			// Return null block after work generation or ledger process error
			block = nullptr;
		}
	}
	return block;
}

std::shared_ptr<nano::block> nano::wallet::change_action (nano::account const & source_a, nano::account const & representative_a, uint64_t work_a, bool generate_work_a)
{
	std::shared_ptr<nano::block> block;
	{
		auto transaction (wallets.tx_begin_read ());
		auto block_transaction (wallets.node.store.tx_begin_read ());
		if (store.valid_password (transaction))
		{
			auto existing (store.find (transaction, source_a));
			if (existing != store.end () && !wallets.node.ledger.latest (block_transaction, source_a).is_zero ())
			{
				nano::account_info info;
				auto error1 (wallets.node.ledger.store.account_get (block_transaction, source_a, info));
				(void)error1;
				assert (!error1);
				nano::raw_key prv;
				auto error2 (store.fetch (transaction, source_a, prv));
				(void)error2;
				assert (!error2);
				if (work_a == 0)
				{
					store.work_get (transaction, source_a, work_a);
				}
				block = std::make_shared<nano::state_block> (source_a, info.head, representative_a, info.balance, 0, prv, source_a, work_a);
			}
		}
	}
	if (block != nullptr)
	{
		if (action_complete (block, source_a, generate_work_a))
		{
			// Return null block after work generation or ledger process error
			block = nullptr;
		}
	}
	return block;
}

std::shared_ptr<nano::block> nano::wallet::send_action (nano::account const & source_a, nano::account const & account_a, nano::uint128_t const & amount_a, uint64_t work_a, bool generate_work_a, boost::optional<std::string> id_a)
{
	boost::optional<nano::mdb_val> id_mdb_val;
	if (id_a)
	{
		id_mdb_val = nano::mdb_val (id_a->size (), const_cast<char *> (id_a->data ()));
	}

	// clang-format off
	auto prepare_send = [&id_mdb_val, &wallets = this->wallets, &store = this->store, &source_a, &amount_a, &work_a, &account_a] (const auto & transaction) {
		auto block_transaction (wallets.node.store.tx_begin_read ());
		auto error (false);
		auto cached_block (false);
		std::shared_ptr<nano::block> block;
		if (id_mdb_val)
		{
			nano::mdb_val result;
			auto status (mdb_get (wallets.env.tx (transaction), wallets.node.wallets.send_action_ids, *id_mdb_val, result));
			if (status == 0)
			{
				nano::block_hash hash (result);
				block = wallets.node.store.block_get (block_transaction, hash);
				if (block != nullptr)
				{
					cached_block = true;
					wallets.node.network.flood_block (block, false);
				}
			}
			else if (status != MDB_NOTFOUND)
			{
				error = true;
			}
		}
		if (!error && block == nullptr)
		{
			if (store.valid_password (transaction))
			{
				auto existing (store.find (transaction, source_a));
				if (existing != store.end ())
				{
					auto balance (wallets.node.ledger.account_balance (block_transaction, source_a));
					if (!balance.is_zero () && balance >= amount_a)
					{
						nano::account_info info;
						auto error1 (wallets.node.ledger.store.account_get (block_transaction, source_a, info));
						(void)error1;
						assert (!error1);
						nano::raw_key prv;
						auto error2 (store.fetch (transaction, source_a, prv));
						(void)error2;
						assert (!error2);
						if (work_a == 0)
						{
							store.work_get (transaction, source_a, work_a);
						}
						block = std::make_shared <nano::state_block> (source_a, info.head, info.representative, balance - amount_a, account_a, prv, source_a, work_a);
						if (id_mdb_val && block != nullptr)
						{
							auto status (mdb_put (wallets.env.tx (transaction), wallets.node.wallets.send_action_ids, *id_mdb_val, nano::mdb_val (block->hash ()), 0));
							if (status != 0)
							{
								block = nullptr;
								error = true;
							}
						}
					}
				}
			}
		}
		return std::make_tuple (block, error, cached_block);
	};
	// clang-format on

	std::tuple<std::shared_ptr<nano::block>, bool, bool> result;
	{
		if (id_mdb_val)
		{
			result = prepare_send (wallets.tx_begin_write ());
		}
		else
		{
			result = prepare_send (wallets.tx_begin_read ());
		}
	}

	std::shared_ptr<nano::block> block;
	bool error;
	bool cached_block;
	std::tie (block, error, cached_block) = result;

	if (!error && block != nullptr && !cached_block)
	{
		if (action_complete (block, source_a, generate_work_a))
		{
			// Return null block after work generation or ledger process error
			block = nullptr;
		}
	}
	return block;
}

bool nano::wallet::action_complete (std::shared_ptr<nano::block> const & block_a, nano::account const & account_a, bool const generate_work_a)
{
	bool error{ false };
	if (block_a != nullptr)
	{
		if (nano::work_validate (*block_a))
		{
			wallets.node.logger.try_log (boost::str (boost::format ("Cached or provided work for block %1% account %2% is invalid, regenerating") % block_a->hash ().to_string () % account_a.to_account ()));
			error = !wallets.node.work_generate_blocking (*block_a, wallets.node.active.limited_active_difficulty ()).is_initialized ();
		}
		if (!error)
		{
			wallets.watcher->add (block_a);
			error = wallets.node.process_local (block_a).code != nano::process_result::progress;
		}
		if (!error && generate_work_a)
		{
			work_ensure (account_a, block_a->hash ());
		}
	}
	return error;
}

bool nano::wallet::change_sync (nano::account const & source_a, nano::account const & representative_a)
{
	std::promise<bool> result;
	std::future<bool> future = result.get_future ();
	// clang-format off
	change_async (source_a, representative_a, [&result](std::shared_ptr<nano::block> block_a) {
		result.set_value (block_a == nullptr);
	},
	true);
	// clang-format on
	return future.get ();
}

void nano::wallet::change_async (nano::account const & source_a, nano::account const & representative_a, std::function<void(std::shared_ptr<nano::block>)> const & action_a, uint64_t work_a, bool generate_work_a)
{
	auto this_l (shared_from_this ());
	wallets.node.wallets.queue_wallet_action (nano::wallets::high_priority, this_l, [this_l, source_a, representative_a, action_a, work_a, generate_work_a](nano::wallet & wallet_a) {
		auto block (wallet_a.change_action (source_a, representative_a, work_a, generate_work_a));
		action_a (block);
	});
}

bool nano::wallet::receive_sync (std::shared_ptr<nano::block> block_a, nano::account const & representative_a, nano::uint128_t const & amount_a)
{
	std::promise<bool> result;
	std::future<bool> future = result.get_future ();
	// clang-format off
	receive_async (block_a, representative_a, amount_a, [&result](std::shared_ptr<nano::block> block_a) {
		result.set_value (block_a == nullptr);
	},
	true);
	// clang-format on
	return future.get ();
}

void nano::wallet::receive_async (std::shared_ptr<nano::block> block_a, nano::account const & representative_a, nano::uint128_t const & amount_a, std::function<void(std::shared_ptr<nano::block>)> const & action_a, uint64_t work_a, bool generate_work_a)
{
	auto this_l (shared_from_this ());
	wallets.node.wallets.queue_wallet_action (amount_a, this_l, [this_l, block_a, representative_a, amount_a, action_a, work_a, generate_work_a](nano::wallet & wallet_a) {
		auto block (wallet_a.receive_action (*block_a, representative_a, amount_a, work_a, generate_work_a));
		action_a (block);
	});
}

nano::block_hash nano::wallet::send_sync (nano::account const & source_a, nano::account const & account_a, nano::uint128_t const & amount_a)
{
	std::promise<nano::block_hash> result;
	std::future<nano::block_hash> future = result.get_future ();
	// clang-format off
	send_async (source_a, account_a, amount_a, [&result](std::shared_ptr<nano::block> block_a) {
		result.set_value (block_a->hash ());
	},
	true);
	// clang-format on
	return future.get ();
}

void nano::wallet::send_async (nano::account const & source_a, nano::account const & account_a, nano::uint128_t const & amount_a, std::function<void(std::shared_ptr<nano::block>)> const & action_a, uint64_t work_a, bool generate_work_a, boost::optional<std::string> id_a)
{
	auto this_l (shared_from_this ());
	wallets.node.wallets.queue_wallet_action (nano::wallets::high_priority, this_l, [this_l, source_a, account_a, amount_a, action_a, work_a, generate_work_a, id_a](nano::wallet & wallet_a) {
		auto block (wallet_a.send_action (source_a, account_a, amount_a, work_a, generate_work_a, id_a));
		action_a (block);
	});
}

// Update work for account if latest root is root_a
void nano::wallet::work_update (nano::transaction const & transaction_a, nano::account const & account_a, nano::root const & root_a, uint64_t work_a)
{
	assert (!nano::work_validate (root_a, work_a));
	assert (store.exists (transaction_a, account_a));
	auto block_transaction (wallets.node.store.tx_begin_read ());
	auto latest (wallets.node.ledger.latest_root (block_transaction, account_a));
	if (latest == root_a)
	{
		store.work_put (transaction_a, account_a, work_a);
	}
	else
	{
		wallets.node.logger.try_log ("Cached work no longer valid, discarding");
	}
}

void nano::wallet::work_ensure (nano::account const & account_a, nano::root const & root_a)
{
	wallets.node.wallets.queue_wallet_action (nano::wallets::generate_priority, shared_from_this (), [account_a, root_a](nano::wallet & wallet_a) {
		wallet_a.work_cache_blocking (account_a, root_a);
	});
}

bool nano::wallet::search_pending ()
{
	auto transaction (wallets.tx_begin_read ());
	auto result (!store.valid_password (transaction));
	if (!result)
	{
		wallets.node.logger.try_log ("Beginning pending block search");
		for (auto i (store.begin (transaction)), n (store.end ()); i != n; ++i)
		{
			auto block_transaction (wallets.node.store.tx_begin_read ());
			nano::account const & account (i->first);
			// Don't search pending for watch-only accounts
			if (!nano::wallet_value (i->second).key.is_zero ())
			{
				for (auto j (wallets.node.store.pending_begin (block_transaction, nano::pending_key (account, 0))); nano::pending_key (j->first).account == account; ++j)
				{
					nano::pending_key key (j->first);
					auto hash (key.hash);
					nano::pending_info pending (j->second);
					auto amount (pending.amount.number ());
					if (wallets.node.config.receive_minimum.number () <= amount)
					{
						wallets.node.logger.try_log (boost::str (boost::format ("Found a pending block %1% for account %2%") % hash.to_string () % pending.source.to_account ()));
						auto block (wallets.node.store.block_get (block_transaction, hash));
						if (wallets.node.ledger.block_confirmed (block_transaction, hash))
						{
							// Receive confirmed block
							auto node_l (wallets.node.shared ());
							wallets.node.background ([node_l, block, hash]() {
								auto transaction (node_l->store.tx_begin_read ());
								node_l->receive_confirmed (transaction, block, hash);
							});
						}
						else
						{
							// Request confirmation for unconfirmed block
							wallets.node.block_confirm (block);
						}
					}
				}
			}
		}
		wallets.node.logger.try_log ("Pending block search phase complete");
	}
	else
	{
		wallets.node.logger.try_log ("Stopping search, wallet is locked");
	}
	return result;
}

void nano::wallet::init_free_accounts (nano::transaction const & transaction_a)
{
	free_accounts.clear ();
	for (auto i (store.begin (transaction_a)), n (store.end ()); i != n; ++i)
	{
		free_accounts.insert (i->first);
	}
}

uint32_t nano::wallet::deterministic_check (nano::transaction const & transaction_a, uint32_t index)
{
	auto block_transaction (wallets.node.store.tx_begin_read ());
	for (uint32_t i (index + 1), n (index + 64); i < n; ++i)
	{
		auto prv = store.deterministic_key (transaction_a, i);
		nano::keypair pair (prv.to_string ());
		// Check if account received at least 1 block
		auto latest (wallets.node.ledger.latest (block_transaction, pair.pub));
		if (!latest.is_zero ())
		{
			index = i;
			// i + 64 - Check additional 64 accounts
			// i/64 - Check additional accounts for large wallets. I.e. 64000/64 = 1000 accounts to check
			n = i + 64 + (i / 64);
		}
		else
		{
			// Check if there are pending blocks for account
			for (auto ii (wallets.node.store.pending_begin (block_transaction, nano::pending_key (pair.pub, 0))); nano::pending_key (ii->first).account == pair.pub; ++ii)
			{
				index = i;
				n = i + 64 + (i / 64);
				break;
			}
		}
	}
	return index;
}

nano::public_key nano::wallet::change_seed (nano::transaction const & transaction_a, nano::raw_key const & prv_a, uint32_t count)
{
	store.seed_set (transaction_a, prv_a);
	auto account = deterministic_insert (transaction_a);
	if (count == 0)
	{
		count = deterministic_check (transaction_a, 0);
	}
	for (uint32_t i (0); i < count; ++i)
	{
		// Disable work generation to prevent weak CPU nodes stuck
		account = deterministic_insert (transaction_a, false);
	}
	return account;
}

void nano::wallet::deterministic_restore (nano::transaction const & transaction_a)
{
	auto index (store.deterministic_index_get (transaction_a));
	auto new_index (deterministic_check (transaction_a, index));
	for (uint32_t i (index); i <= new_index && index != new_index; ++i)
	{
		// Disable work generation to prevent weak CPU nodes stuck
		deterministic_insert (transaction_a, false);
	}
}

bool nano::wallet::live ()
{
	return store.handle != 0;
}

void nano::wallet::work_cache_blocking (nano::account const & account_a, nano::root const & root_a)
{
	if (wallets.node.work_generation_enabled ())
	{
		auto opt_work_l (wallets.node.work_generate_blocking (root_a, account_a));
		if (opt_work_l.is_initialized ())
		{
			auto transaction_l (wallets.tx_begin_write ());
			if (live () && store.exists (transaction_l, account_a))
			{
				work_update (transaction_l, account_a, root_a, *opt_work_l);
			}
		}
		else if (!wallets.node.stopped)
		{
			wallets.node.logger.try_log (boost::str (boost::format ("Could not precache work for root %1% due to work generation failure") % root_a.to_string ()));
		}
	}
}

nano::work_watcher::work_watcher (nano::node & node_a)
	: node (node_a)
	, stopped (false)
{
	node.observers.blocks.add ([this](nano::election_status const & status_a, nano::account const & account_a, nano::amount const & amount_a, bool is_state_send_a) {
		this->remove (status_a.winner);
	});
}

nano::work_watcher::~work_watcher ()
{
	stop ();
}

void nano::work_watcher::stop ()
{
	nano::unique_lock<std::mutex> lock (mutex);
	watched.clear ();
	stopped = true;
}

void nano::work_watcher::add (std::shared_ptr<nano::block> block_a)
{
	auto block_l (std::dynamic_pointer_cast<nano::state_block> (block_a));
	if (!stopped && block_l != nullptr)
	{
		auto root_l (block_l->qualified_root ());
		nano::unique_lock<std::mutex> lock (mutex);
		watched[root_l] = block_l;
		lock.unlock ();
		watching (root_l, block_l);
	}
}

void nano::work_watcher::update (nano::qualified_root const & root_a, std::shared_ptr<nano::state_block> block_a)
{
	nano::lock_guard<std::mutex> guard (mutex);
	watched[root_a] = block_a;
}

void nano::work_watcher::watching (nano::qualified_root const & root_a, std::shared_ptr<nano::state_block> block_a)
{
	std::weak_ptr<nano::work_watcher> watcher_w (shared_from_this ());
	node.alarm.add (std::chrono::steady_clock::now () + node.config.work_watcher_period, [block_a, root_a, watcher_w]() {
		auto watcher_l = watcher_w.lock ();
		if (watcher_l && !watcher_l->stopped && block_a != nullptr)
		{
			nano::unique_lock<std::mutex> lock (watcher_l->mutex);
			if (watcher_l->watched.find (root_a) != watcher_l->watched.end ()) // not yet confirmed or cancelled
			{
				lock.unlock ();
				uint64_t difficulty (0);
				auto root_l (block_a->root ());
				nano::work_validate (root_l, block_a->block_work (), &difficulty);
				auto active_difficulty (watcher_l->node.active.limited_active_difficulty ());
				/*
				 * Work watcher should still watch blocks even without work generation, although no rework is done
				 * Functionality may be added in the future that does not require updating work
				 */
				if (active_difficulty > difficulty && watcher_l->node.work_generation_enabled ())
				{
					watcher_l->node.work_generate (
						root_l, [watcher_l, block_a, root_a](boost::optional<uint64_t> work_a) {
							if (block_a != nullptr && watcher_l != nullptr && !watcher_l->stopped)
							{
								bool updated_l{ false };
								if (work_a.is_initialized ())
								{
									nano::state_block_builder builder;
									std::error_code ec;
									std::shared_ptr<nano::state_block> block (builder.from (*block_a).work (*work_a).build (ec));

									if (!ec)
									{
										{
											auto hash (block_a->hash ());
											nano::lock_guard<std::mutex> active_guard (watcher_l->node.active.mutex);
											auto existing (watcher_l->node.active.roots.find (root_a));
											if (existing != watcher_l->node.active.roots.end ())
											{
												auto election (existing->election);
												if (election->status.winner->hash () == hash)
												{
													election->status.winner = block;
												}
												auto current (election->blocks.find (hash));
												assert (current != election->blocks.end ());
												current->second = block;
											}
										}
										watcher_l->node.network.flood_block (block, false);
										watcher_l->node.active.update_difficulty (block);
										watcher_l->update (root_a, block);
										updated_l = true;
										watcher_l->watching (root_a, block);
									}
								}
								if (!updated_l)
								{
									watcher_l->watching (root_a, block_a);
								}
							}
						},
						active_difficulty, block_a->account ());
				}
				else
				{
					watcher_l->watching (root_a, block_a);
				}
			}
		}
	});
}

void nano::work_watcher::remove (std::shared_ptr<nano::block> block_a)
{
	auto root_l (block_a->qualified_root ());
	nano::lock_guard<std::mutex> lock (mutex);
	auto existing (watched.find (root_l));
	if (existing != watched.end () && existing->second->hash () == block_a->hash ())
	{
		watched.erase (existing);
		node.observers.work_cancel.notify (block_a->root ());
	}
}

bool nano::work_watcher::is_watched (nano::qualified_root const & root_a)
{
	nano::lock_guard<std::mutex> guard (mutex);
	auto exists (watched.find (root_a));
	return exists != watched.end ();
}

size_t nano::work_watcher::size ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	return watched.size ();
}

void nano::wallets::do_wallet_actions ()
{
	nano::unique_lock<std::mutex> action_lock (action_mutex);
	while (!stopped)
	{
		if (!actions.empty ())
		{
			auto first (actions.begin ());
			auto wallet (first->second.first);
			auto current (std::move (first->second.second));
			actions.erase (first);
			if (wallet->live ())
			{
				action_lock.unlock ();
				observer (true);
				current (*wallet);
				observer (false);
				action_lock.lock ();
			}
		}
		else
		{
			condition.wait (action_lock);
		}
	}
}

nano::wallets::wallets (bool error_a, nano::node & node_a)
	: observer ([](bool) {})
	, node (node_a)
	, env (boost::polymorphic_downcast<nano::mdb_wallets_store *> (node_a.wallets_store_impl.get ())->environment)
	, stopped (false)
	, watcher (std::make_shared<nano::work_watcher> (node_a))
	, thread ([this]() {
		nano::thread_role::set (nano::thread_role::name::wallet_actions);
		do_wallet_actions ();
	})
{
	nano::unique_lock<std::mutex> lock (mutex);
	if (!error_a)
	{
		auto transaction (tx_begin_write ());
		auto status (mdb_dbi_open (env.tx (transaction), nullptr, MDB_CREATE, &handle));
		split_if_needed (transaction, node.store);
		status |= mdb_dbi_open (env.tx (transaction), "send_action_ids", MDB_CREATE, &send_action_ids);
		assert (status == 0);
		std::string beginning (nano::uint256_union (0).to_string ());
		std::string end ((nano::uint256_union (nano::uint256_t (0) - nano::uint256_t (1))).to_string ());
		nano::store_iterator<std::array<char, 64>, nano::no_value> i (std::make_unique<nano::mdb_iterator<std::array<char, 64>, nano::no_value>> (transaction, handle, nano::mdb_val (beginning.size (), const_cast<char *> (beginning.c_str ()))));
		nano::store_iterator<std::array<char, 64>, nano::no_value> n (std::make_unique<nano::mdb_iterator<std::array<char, 64>, nano::no_value>> (transaction, handle, nano::mdb_val (end.size (), const_cast<char *> (end.c_str ()))));
		for (; i != n; ++i)
		{
			nano::wallet_id id;
			std::string text (i->first.data (), i->first.size ());
			auto error (id.decode_hex (text));
			assert (!error);
			assert (items.find (id) == items.end ());
			auto wallet (std::make_shared<nano::wallet> (error, transaction, *this, text));
			if (!error)
			{
				items[id] = wallet;
			}
			else
			{
				// Couldn't open wallet
			}
		}
	}
	// Backup before upgrade wallets
	bool backup_required (false);
	if (node.config.backup_before_upgrade)
	{
		auto transaction (tx_begin_read ());
		for (auto & item : items)
		{
			if (item.second->store.version (transaction) != nano::wallet_store::version_current)
			{
				backup_required = true;
				break;
			}
		}
	}
	if (backup_required)
	{
		const char * store_path;
		mdb_env_get_path (env, &store_path);
		const boost::filesystem::path path (store_path);
		nano::mdb_store::create_backup_file (env, path, node_a.logger);
	}
	for (auto & item : items)
	{
		item.second->enter_initial_password ();
	}
	if (node_a.config.enable_voting)
	{
		lock.unlock ();
		ongoing_compute_reps ();
	}
}

nano::wallets::~wallets ()
{
	stop ();
}

std::shared_ptr<nano::wallet> nano::wallets::open (nano::wallet_id const & id_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	std::shared_ptr<nano::wallet> result;
	auto existing (items.find (id_a));
	if (existing != items.end ())
	{
		result = existing->second;
	}
	return result;
}

std::shared_ptr<nano::wallet> nano::wallets::create (nano::wallet_id const & id_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	assert (items.find (id_a) == items.end ());
	std::shared_ptr<nano::wallet> result;
	bool error;
	{
		auto transaction (tx_begin_write ());
		result = std::make_shared<nano::wallet> (error, transaction, *this, id_a.to_string ());
	}
	if (!error)
	{
		items[id_a] = result;
		result->enter_initial_password ();
	}
	return result;
}

bool nano::wallets::search_pending (nano::wallet_id const & wallet_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	auto result (false);
	auto existing (items.find (wallet_a));
	result = existing == items.end ();
	if (!result)
	{
		auto wallet (existing->second);
		result = wallet->search_pending ();
	}
	return result;
}

void nano::wallets::search_pending_all ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	for (auto i : items)
	{
		i.second->search_pending ();
	}
}

void nano::wallets::destroy (nano::wallet_id const & id_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	auto transaction (tx_begin_write ());
	// action_mutex should be after transactions to prevent deadlocks in deterministic_insert () & insert_adhoc ()
	nano::lock_guard<std::mutex> action_lock (action_mutex);
	auto existing (items.find (id_a));
	assert (existing != items.end ());
	auto wallet (existing->second);
	items.erase (existing);
	wallet->store.destroy (transaction);
}

void nano::wallets::reload ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	auto transaction (tx_begin_write ());
	std::unordered_set<nano::uint256_union> stored_items;
	std::string beginning (nano::uint256_union (0).to_string ());
	std::string end ((nano::uint256_union (nano::uint256_t (0) - nano::uint256_t (1))).to_string ());
	nano::store_iterator<std::array<char, 64>, nano::no_value> i (std::make_unique<nano::mdb_iterator<std::array<char, 64>, nano::no_value>> (transaction, handle, nano::mdb_val (beginning.size (), const_cast<char *> (beginning.c_str ()))));
	nano::store_iterator<std::array<char, 64>, nano::no_value> n (std::make_unique<nano::mdb_iterator<std::array<char, 64>, nano::no_value>> (transaction, handle, nano::mdb_val (end.size (), const_cast<char *> (end.c_str ()))));
	for (; i != n; ++i)
	{
		nano::wallet_id id;
		std::string text (i->first.data (), i->first.size ());
		auto error (id.decode_hex (text));
		assert (!error);
		// New wallet
		if (items.find (id) == items.end ())
		{
			auto wallet (std::make_shared<nano::wallet> (error, transaction, *this, text));
			if (!error)
			{
				items[id] = wallet;
			}
		}
		// List of wallets on disk
		stored_items.insert (id);
	}
	// Delete non existing wallets from memory
	std::vector<nano::wallet_id> deleted_items;
	for (auto i : items)
	{
		if (stored_items.find (i.first) == stored_items.end ())
		{
			deleted_items.push_back (i.first);
		}
	}
	for (auto & i : deleted_items)
	{
		assert (items.find (i) == items.end ());
		items.erase (i);
	}
}

void nano::wallets::queue_wallet_action (nano::uint128_t const & amount_a, std::shared_ptr<nano::wallet> wallet_a, std::function<void(nano::wallet &)> const & action_a)
{
	{
		nano::lock_guard<std::mutex> action_lock (action_mutex);
		actions.insert (std::make_pair (amount_a, std::make_pair (wallet_a, std::move (action_a))));
	}
	condition.notify_all ();
}

void nano::wallets::foreach_representative (std::function<void(nano::public_key const & pub_a, nano::raw_key const & prv_a)> const & action_a)
{
	if (node.config.enable_voting)
	{
		nano::lock_guard<std::mutex> lock (mutex);
		auto transaction_l (tx_begin_read ());
		for (auto i (items.begin ()), n (items.end ()); i != n; ++i)
		{
			auto & wallet (*i->second);
			nano::lock_guard<std::recursive_mutex> store_lock (wallet.store.mutex);
			nano::lock_guard<std::mutex> representatives_lock (wallet.representatives_mutex);
			for (auto ii (wallet.representatives.begin ()), nn (wallet.representatives.end ()); ii != nn; ++ii)
			{
				nano::account account (*ii);
				if (wallet.store.exists (transaction_l, account))
				{
					if (!node.ledger.weight (account).is_zero ())
					{
						if (wallet.store.valid_password (transaction_l))
						{
							nano::raw_key prv;
							auto error (wallet.store.fetch (transaction_l, account, prv));
							(void)error;
							assert (!error);
							action_a (account, prv);
						}
						else
						{
							static auto last_log = std::chrono::steady_clock::time_point ();
							if (last_log < std::chrono::steady_clock::now () - std::chrono::seconds (60))
							{
								last_log = std::chrono::steady_clock::now ();
								node.logger.always_log (boost::str (boost::format ("Representative locked inside wallet %1%") % i->first.to_string ()));
							}
						}
					}
				}
			}
		}
	}
}

bool nano::wallets::exists (nano::transaction const & transaction_a, nano::account const & account_a)
{
	nano::lock_guard<std::mutex> lock (mutex);
	auto result (false);
	for (auto i (items.begin ()), n (items.end ()); !result && i != n; ++i)
	{
		result = i->second->store.exists (transaction_a, account_a);
	}
	return result;
}

void nano::wallets::stop ()
{
	{
		nano::lock_guard<std::mutex> action_lock (action_mutex);
		stopped = true;
		actions.clear ();
	}
	condition.notify_all ();
	if (thread.joinable ())
	{
		thread.join ();
	}
	watcher->stop ();
}

nano::write_transaction nano::wallets::tx_begin_write ()
{
	return env.tx_begin_write ();
}

nano::read_transaction nano::wallets::tx_begin_read ()
{
	return env.tx_begin_read ();
}

void nano::wallets::clear_send_ids (nano::transaction const & transaction_a)
{
	auto status (mdb_drop (env.tx (transaction_a), send_action_ids, 0));
	(void)status;
	assert (status == 0);
}

bool nano::wallets::check_rep (nano::account const & account_a, nano::uint128_t const & half_principal_weight_a)
{
	bool result (false);
	auto weight (node.ledger.weight (account_a));
	if (weight >= node.config.vote_minimum.number ())
	{
		result = true;
		++reps_count;
		if (weight >= half_principal_weight_a)
		{
			++half_principal_reps_count;
		}
	}
	return result;
}

void nano::wallets::compute_reps ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	reps_count = 0;
	half_principal_reps_count = 0;
	auto half_principal_weight (node.minimum_principal_weight () / 2);
	auto transaction (tx_begin_read ());
	for (auto i (items.begin ()), n (items.end ()); i != n; ++i)
	{
		auto & wallet (*i->second);
		decltype (wallet.representatives) representatives_l;
		for (auto ii (wallet.store.begin (transaction)), nn (wallet.store.end ()); ii != nn; ++ii)
		{
			auto account (ii->first);
			if (check_rep (account, half_principal_weight))
			{
				representatives_l.insert (account);
			}
		}
		nano::lock_guard<std::mutex> representatives_lock (wallet.representatives_mutex);
		wallet.representatives.swap (representatives_l);
	}
}

void nano::wallets::ongoing_compute_reps ()
{
	compute_reps ();
	auto & node_l (node);
	auto compute_delay (network_params.network.is_test_network () ? std::chrono::milliseconds (10) : std::chrono::milliseconds (15 * 60 * 1000)); // Representation drifts quickly on the test network but very slowly on the live network
	node.alarm.add (std::chrono::steady_clock::now () + compute_delay, [&node_l]() {
		node_l.wallets.ongoing_compute_reps ();
	});
}

void nano::wallets::split_if_needed (nano::transaction & transaction_destination, nano::block_store & store_a)
{
	auto store_l (dynamic_cast<nano::mdb_store *> (&store_a));
	if (store_l != nullptr)
	{
		if (items.empty ())
		{
			std::string beginning (nano::uint256_union (0).to_string ());
			std::string end ((nano::uint256_union (nano::uint256_t (0) - nano::uint256_t (1))).to_string ());

			// clang-format off
			auto get_store_it = [&handle = handle](nano::transaction const & transaction_source, std::string const & hash) {
				return nano::store_iterator<std::array<char, 64>, nano::no_value> (std::make_unique<nano::mdb_iterator<std::array<char, 64>, nano::no_value>> (transaction_source, handle, nano::mdb_val (hash.size (), const_cast<char *> (hash.c_str ()))));
			};
			// clang-format on

			// First do a read pass to check if there are any wallets that need extracting (to save holding a write lock and potentially being blocked)
			auto wallets_need_splitting (false);
			{
				auto transaction_source (store_l->tx_begin_read ());
				auto i = get_store_it (transaction_source, beginning);
				auto n = get_store_it (transaction_source, end);
				wallets_need_splitting = (i != n);
			}

			if (wallets_need_splitting)
			{
				auto transaction_source (store_l->tx_begin_write ());
				auto i = get_store_it (transaction_source, beginning);
				auto n = get_store_it (transaction_source, end);
				auto tx_source = static_cast<MDB_txn *> (transaction_source.get_handle ());
				auto tx_destination = static_cast<MDB_txn *> (transaction_destination.get_handle ());
				for (; i != n; ++i)
				{
					nano::uint256_union id;
					std::string text (i->first.data (), i->first.size ());
					auto error1 (id.decode_hex (text));
					(void)error1;
					assert (!error1);
					assert (strlen (text.c_str ()) == text.size ());
					move_table (text, tx_source, tx_destination);
				}
			}
		}
	}
}

void nano::wallets::move_table (std::string const & name_a, MDB_txn * tx_source, MDB_txn * tx_destination)
{
	MDB_dbi handle_source;
	auto error2 (mdb_dbi_open (tx_source, name_a.c_str (), MDB_CREATE, &handle_source));
	(void)error2;
	assert (!error2);
	MDB_dbi handle_destination;
	auto error3 (mdb_dbi_open (tx_destination, name_a.c_str (), MDB_CREATE, &handle_destination));
	(void)error3;
	assert (!error3);
	MDB_cursor * cursor;
	auto error4 (mdb_cursor_open (tx_source, handle_source, &cursor));
	(void)error4;
	assert (!error4);
	MDB_val val_key;
	MDB_val val_value;
	auto cursor_status (mdb_cursor_get (cursor, &val_key, &val_value, MDB_FIRST));
	while (cursor_status == MDB_SUCCESS)
	{
		auto error5 (mdb_put (tx_destination, handle_destination, &val_key, &val_value, 0));
		(void)error5;
		assert (!error5);
		cursor_status = mdb_cursor_get (cursor, &val_key, &val_value, MDB_NEXT);
	}
	auto error6 (mdb_drop (tx_source, handle_source, 1));
	(void)error6;
	assert (!error6);
}

nano::uint128_t const nano::wallets::generate_priority = std::numeric_limits<nano::uint128_t>::max ();
nano::uint128_t const nano::wallets::high_priority = std::numeric_limits<nano::uint128_t>::max () - 1;

nano::store_iterator<nano::account, nano::wallet_value> nano::wallet_store::begin (nano::transaction const & transaction_a)
{
	nano::store_iterator<nano::account, nano::wallet_value> result (std::make_unique<nano::mdb_iterator<nano::account, nano::wallet_value>> (transaction_a, handle, nano::mdb_val (nano::account (special_count))));
	return result;
}

nano::store_iterator<nano::account, nano::wallet_value> nano::wallet_store::begin (nano::transaction const & transaction_a, nano::account const & key)
{
	nano::store_iterator<nano::account, nano::wallet_value> result (std::make_unique<nano::mdb_iterator<nano::account, nano::wallet_value>> (transaction_a, handle, nano::mdb_val (key)));
	return result;
}

nano::store_iterator<nano::account, nano::wallet_value> nano::wallet_store::find (nano::transaction const & transaction_a, nano::account const & key)
{
	auto result (begin (transaction_a, key));
	nano::store_iterator<nano::account, nano::wallet_value> end (nullptr);
	if (result != end)
	{
		if (result->first == key)
		{
			return result;
		}
		else
		{
			return end;
		}
	}
	else
	{
		return end;
	}
	return result;
}

nano::store_iterator<nano::account, nano::wallet_value> nano::wallet_store::end ()
{
	return nano::store_iterator<nano::account, nano::wallet_value> (nullptr);
}
nano::mdb_wallets_store::mdb_wallets_store (boost::filesystem::path const & path_a, int lmdb_max_dbs)
	: environment (error, path_a, lmdb_max_dbs, false, 1ULL * 1024 * 1024 * 1024)
{
}

bool nano::mdb_wallets_store::init_error () const
{
	return error;
}

MDB_txn * nano::wallet_store::tx (nano::transaction const & transaction_a) const
{
	return static_cast<MDB_txn *> (transaction_a.get_handle ());
}

namespace nano
{
std::unique_ptr<seq_con_info_component> collect_seq_con_info (wallets & wallets, const std::string & name)
{
	size_t items_count = 0;
	size_t actions_count = 0;
	{
		nano::lock_guard<std::mutex> guard (wallets.mutex);
		items_count = wallets.items.size ();
		actions_count = wallets.actions.size ();
	}

	auto composite = std::make_unique<seq_con_info_composite> (name);
	auto sizeof_item_element = sizeof (decltype (wallets.items)::value_type);
	auto sizeof_actions_element = sizeof (decltype (wallets.actions)::value_type);
	auto sizeof_watcher_element = sizeof (decltype (wallets.watcher->watched)::value_type);
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "items", items_count, sizeof_item_element }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "actions", actions_count, sizeof_actions_element }));
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "work_watcher", wallets.watcher->size (), sizeof_watcher_element }));
	return composite;
}
}
