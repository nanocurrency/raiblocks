#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/rocksdbconfig.hpp>
#include <nano/node/rocksdb/rocksdb.hpp>
#include <nano/node/rocksdb/rocksdb_iterator.hpp>
#include <nano/node/rocksdb/rocksdb_txn.hpp>

#include <boost/endian/conversion.hpp>
#include <boost/format.hpp>
#include <boost/polymorphic_cast.hpp>

#include <rocksdb/merge_operator.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/backupable_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>

namespace nano
{
template <>
void * rocksdb_val::data () const
{
	return (void *)value.data ();
}

template <>
size_t rocksdb_val::size () const
{
	return value.size ();
}

template <>
rocksdb_val::db_val (size_t size_a, void * data_a)
	: value (static_cast<const char *> (data_a), size_a)
{
}

template <>
void rocksdb_val::convert_buffer_to_value ()
{
	value = rocksdb::Slice (reinterpret_cast<const char *> (buffer->data ()), buffer->size ());
}
}

nano::rocksdb_store::rocksdb_store (nano::logger_mt & logger_a, boost::filesystem::path const & path_a, nano::rocksdb_config const & rocksdb_config_a, bool open_read_only_a)
	: logger (logger_a)
	, rocksdb_config (rocksdb_config_a)
{
	boost::system::error_code error_mkdir, error_chmod;
	boost::filesystem::create_directories (path_a, error_mkdir);
	nano::set_secure_perm_directory (path_a, error_chmod);
	error = static_cast<bool> (error_mkdir);

	if (!error)
	{
		auto table_options = get_table_options ();
		table_factory.reset (rocksdb::NewBlockBasedTableFactory (table_options));
		if (!open_read_only_a)
		{
			construct_column_family_mutexes ();
		}
		open (error, path_a, open_read_only_a);
	}
}

nano::rocksdb_store::~rocksdb_store ()
{
	for (auto handle : handles)
	{
		delete handle;
	}

	delete db;
}

void nano::rocksdb_store::open (bool & error_a, boost::filesystem::path const & path_a, bool open_read_only_a)
{
	std::initializer_list<const char *> names{ rocksdb::kDefaultColumnFamilyName.c_str (), "frontiers", "accounts", "send", "receive", "open", "change", "state_blocks", "pending", "representation", "unchecked", "vote", "online_weight", "meta", "peers", "cached_counts", "confirmation_height" };
	std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
	for (const auto & cf_name : names)
	{
		column_families.emplace_back (cf_name, get_cf_options ());
	}

	auto options = get_db_options ();
	rocksdb::Status s;

	if (open_read_only_a)
	{
		s = rocksdb::DB::OpenForReadOnly (options, path_a.string (), column_families, &handles, &db);
	}
	else
	{
		s = rocksdb::OptimisticTransactionDB::Open (options, path_a.string (), column_families, &handles, &optimistic_db);
		if (optimistic_db)
		{
			db = optimistic_db;
		}
	}

	// Assign handles to supplied
	error_a |= !s.ok ();

	if (!error_a)
	{
		auto transaction = tx_begin_read ();
		auto version_l = version_get (transaction);
		if (version_l > version)
		{
			error_a = true;
			logger.always_log (boost::str (boost::format ("The version of the ledger (%1%) is too high for this node") % version_l));
		}
	}
}

nano::write_transaction nano::rocksdb_store::tx_begin_write (std::vector<nano::tables> const & tables_requiring_locks_a, std::vector<nano::tables> const & tables_no_locks_a)
{
	std::unique_ptr<nano::write_rocksdb_txn> txn;
	release_assert (optimistic_db != nullptr);
	if (tables_requiring_locks_a.empty () && tables_no_locks_a.empty ())
	{
		// Use all tables if none are specified
		txn = std::make_unique<nano::write_rocksdb_txn> (optimistic_db, all_tables (), tables_no_locks_a, write_lock_mutexes);
	}
	else
	{
		txn = std::make_unique<nano::write_rocksdb_txn> (optimistic_db, tables_requiring_locks_a, tables_no_locks_a, write_lock_mutexes);
	}

	// Tables must be kept in alphabetical order. These can be used for mutex locking, so order is important to prevent deadlocking
	assert (std::is_sorted (tables_requiring_locks_a.begin (), tables_requiring_locks_a.end ()));

	return nano::write_transaction{ std::move (txn) };
}

nano::read_transaction nano::rocksdb_store::tx_begin_read ()
{
	return nano::read_transaction{ std::make_unique<nano::read_rocksdb_txn> (db) };
}

std::string nano::rocksdb_store::vendor_get () const
{
	return boost::str (boost::format ("RocksDB %1%.%2%.%3%") % ROCKSDB_MAJOR % ROCKSDB_MINOR % ROCKSDB_PATCH);
}

rocksdb::ColumnFamilyHandle * nano::rocksdb_store::table_to_column_family (tables table_a) const
{
	auto & handles_l = handles;
	auto get_handle = [&handles_l](const char * name) {
		auto iter = std::find_if (handles_l.begin (), handles_l.end (), [name](auto handle) {
			return (handle->GetName () == name);
		});
		assert (iter != handles_l.end ());
		return *iter;
	};

	switch (table_a)
	{
		case tables::frontiers:
			return get_handle ("frontiers");
		case tables::accounts:
			return get_handle ("accounts");
		case tables::send_blocks:
			return get_handle ("send");
		case tables::receive_blocks:
			return get_handle ("receive");
		case tables::open_blocks:
			return get_handle ("open");
		case tables::change_blocks:
			return get_handle ("change");
		case tables::state_blocks:
			return get_handle ("state_blocks");
		case tables::pending:
			return get_handle ("pending");
		case tables::blocks_info:
			assert (false);
		case tables::representation:
			return get_handle ("representation");
		case tables::unchecked:
			return get_handle ("unchecked");
		case tables::vote:
			return get_handle ("vote");
		case tables::online_weight:
			return get_handle ("online_weight");
		case tables::meta:
			return get_handle ("meta");
		case tables::peers:
			return get_handle ("peers");
		case tables::cached_counts:
			return get_handle ("cached_counts");
		case tables::confirmation_height:
			return get_handle ("confirmation_height");
		default:
			release_assert (false);
			return get_handle ("peers");
	}
}

bool nano::rocksdb_store::exists (nano::transaction const & transaction_a, tables table_a, nano::rocksdb_val const & key_a) const
{
	rocksdb::PinnableSlice slice;
	rocksdb::Status status;
	if (is_read (transaction_a))
	{
		status = db->Get (snapshot_options (transaction_a), table_to_column_family (table_a), key_a, &slice);
	}
	else
	{
		rocksdb::ReadOptions options;
		status = tx (transaction_a)->Get (options, table_to_column_family (table_a), key_a, &slice);
	}

	return (status.ok ());
}

int nano::rocksdb_store::del (nano::write_transaction const & transaction_a, tables table_a, nano::rocksdb_val const & key_a)
{
	assert (transaction_a.contains (table_a));
	// RocksDB errors when trying to delete an entry which doesn't exist. It is a pre-condition that the key exists
	assert (exists (transaction_a, table_a, key_a));

	// Removing an entry so counts may need adjusting
	if (is_caching_counts (table_a))
	{
		decrement (transaction_a, tables::cached_counts, rocksdb_val (rocksdb::Slice (table_to_column_family (table_a)->GetName ())), 1);
	}

	return tx (transaction_a)->Delete (table_to_column_family (table_a), key_a).code ();
}

bool nano::rocksdb_store::block_info_get (nano::transaction const &, nano::block_hash const &, nano::block_info &) const
{
	// Should not be called as the RocksDB backend does not use this table
	assert (false);
	return true;
}

void nano::rocksdb_store::version_put (nano::write_transaction const & transaction_a, int version_a)
{
	assert (transaction_a.contains (tables::meta));
	nano::uint256_union version_key (1);
	nano::uint256_union version_value (version_a);
	auto status (put (transaction_a, tables::meta, version_key, nano::rocksdb_val (version_value)));
	release_assert (success (status));
}

rocksdb::Transaction * nano::rocksdb_store::tx (nano::transaction const & transaction_a) const
{
	assert (!is_read (transaction_a));
	return static_cast<rocksdb::Transaction *> (transaction_a.get_handle ());
}

int nano::rocksdb_store::get (nano::transaction const & transaction_a, tables table_a, nano::rocksdb_val const & key_a, nano::rocksdb_val & value_a) const
{
	rocksdb::ReadOptions options;
	rocksdb::PinnableSlice slice;
	auto handle = table_to_column_family (table_a);
	rocksdb::Status status;
	if (is_read (transaction_a))
	{
		status = db->Get (snapshot_options (transaction_a), handle, key_a, &slice);
	}
	else
	{
		status = tx (transaction_a)->Get (options, handle, key_a, &slice);
	}

	if (status.ok ())
	{
		value_a.buffer = std::make_shared<std::vector<uint8_t>> (slice.size ());
		std::memcpy (value_a.buffer->data (), slice.data (), slice.size ());
		value_a.convert_buffer_to_value ();
	}
	return status.code ();
}

/** The column families which need to have their counts cached for later querying */
bool nano::rocksdb_store::is_caching_counts (nano::tables table_a) const
{
	switch (table_a)
	{
		case tables::accounts:
		case tables::unchecked:
		case tables::send_blocks:
		case tables::receive_blocks:
		case tables::open_blocks:
		case tables::change_blocks:
		case tables::state_blocks:
			return true;
		default:
			return false;
	}
}

int nano::rocksdb_store::increment (nano::write_transaction const & transaction_a, tables table_a, nano::rocksdb_val const & key_a, uint64_t amount_a)
{
	release_assert (transaction_a.contains (table_a));
	uint64_t base;
	nano::rocksdb_val value;
	if (!success (get (transaction_a, table_a, key_a, value)))
	{
		base = 0;
	}
	else
	{
		base = static_cast<uint64_t> (value);
	}

	return put (transaction_a, table_a, key_a, nano::rocksdb_val (base + amount_a));
}

int nano::rocksdb_store::decrement (nano::write_transaction const & transaction_a, tables table_a, nano::rocksdb_val const & key_a, uint64_t amount_a)
{
	release_assert (transaction_a.contains (table_a));
	nano::rocksdb_val value;
	auto status = get (transaction_a, table_a, key_a, value);
	release_assert (success (status));
	auto base = static_cast<uint64_t> (value);
	return put (transaction_a, table_a, key_a, nano::rocksdb_val (base - amount_a));
}

int nano::rocksdb_store::put (nano::write_transaction const & transaction_a, tables table_a, nano::rocksdb_val const & key_a, nano::rocksdb_val const & value_a)
{
	assert (transaction_a.contains (table_a));

	auto txn = tx (transaction_a);
	if (is_caching_counts (table_a))
	{
		if (!exists (transaction_a, table_a, key_a))
		{
			// Adding a new entry so counts need adjusting
			increment (transaction_a, tables::cached_counts, rocksdb_val (rocksdb::Slice (table_to_column_family (table_a)->GetName ())), 1);
		}
	}

	return txn->Put (table_to_column_family (table_a), key_a, value_a).code ();
}

bool nano::rocksdb_store::not_found (int status) const
{
	return (status_code_not_found () == status);
}

bool nano::rocksdb_store::success (int status) const
{
	return (static_cast<int> (rocksdb::Status::Code::kOk) == status);
}

int nano::rocksdb_store::status_code_not_found () const
{
	return static_cast<int> (rocksdb::Status::Code::kNotFound);
}

uint64_t nano::rocksdb_store::count (nano::transaction const & transaction_a, rocksdb::ColumnFamilyHandle * handle) const
{
	uint64_t count = 0;
	nano::rocksdb_val val;
	auto const & key = handle->GetName ();
	auto status = get (transaction_a, tables::cached_counts, nano::rocksdb_val (key.size (), (void *)key.data ()), val);
	if (success (status))
	{
		count = static_cast<uint64_t> (val);
	}

	release_assert (success (status) || not_found (status));
	return count;
}

size_t nano::rocksdb_store::count (nano::transaction const & transaction_a, tables table_a) const
{
	size_t sum = 0;
	// Some column families are small enough that they can just be iterated, rather than doing extra io caching counts
	if (table_a == tables::peers)
	{
		for (auto i (peers_begin (transaction_a)), n (peers_end ()); i != n; ++i)
		{
			++sum;
		}
	}
	else if (table_a == tables::online_weight)
	{
		for (auto i (online_weight_begin (transaction_a)), n (online_weight_end ()); i != n; ++i)
		{
			++sum;
		}
	}
	else
	{
		return count (transaction_a, table_to_column_family (table_a));
	}

	return sum;
}

int nano::rocksdb_store::drop (nano::write_transaction const & transaction_a, tables table_a)
{
	assert (transaction_a.contains (table_a));
	auto col = table_to_column_family (table_a);

	int status = static_cast<int> (rocksdb::Status::Code::kOk);
	if (is_caching_counts (table_a))
	{
		// Reset counter to 0
		status = put (transaction_a, tables::cached_counts, nano::rocksdb_val (rocksdb::Slice (col->GetName ())), nano::rocksdb_val (uint64_t{ 0 }));
	}

	if (success (status))
	{
		// Dropping/Creating families like in node::ongoing_peer_clear can cause write stalls, just delete them manually.
		if (table_a == tables::peers)
		{
			int status = 0;
			for (auto i = peers_begin (transaction_a), n = peers_end (); i != n; ++i)
			{
				status = del (transaction_a, tables::peers, nano::rocksdb_val (i->first));
				release_assert (success (status));
			}
			return status;
		}
		else
		{
			return clear (col);
		}
	}
	return status;
}

int nano::rocksdb_store::clear (rocksdb::ColumnFamilyHandle * column_family)
{
	// Dropping completely removes the column
	auto name = column_family->GetName ();
	auto status = db->DropColumnFamily (column_family);
	release_assert (status.ok ());
	delete column_family;

	// Need to add it back as we just want to clear the contents
	auto handle_it = std::find (handles.begin (), handles.end (), column_family);
	assert (handle_it != handles.cend ());
	status = db->CreateColumnFamily (get_cf_options (), name, &column_family);
	release_assert (status.ok ());
	*handle_it = column_family;
	return status.code ();
}

void nano::rocksdb_store::construct_column_family_mutexes ()
{
	for (auto table : all_tables ())
	{
		write_lock_mutexes.emplace (std::piecewise_construct, std::forward_as_tuple (table), std::forward_as_tuple ());
	}
}

rocksdb::Options nano::rocksdb_store::get_db_options () const
{
	rocksdb::Options db_options;
	db_options.create_if_missing = true;
	db_options.create_missing_column_families = true;

	// Sets the compaction priority
	db_options.compaction_pri = rocksdb::CompactionPri::kMinOverlappingRatio;

	// Start aggressively flushing WAL files when they reach over 1GB
	db_options.max_total_wal_size = 1 * 1024 * 1024 * 1024LL;

	// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
	db_options.IncreaseParallelism (rocksdb_config.io_threads);
	db_options.OptimizeLevelStyleCompaction ();

	// Adds a separate write queue for memtable/WAL
	db_options.enable_pipelined_write = rocksdb_config.enable_pipelined_write;

	// Total size of memtables across column families. This can be used to manage the total memory used by memtables.
	db_options.db_write_buffer_size = rocksdb_config.total_memtable_size * 1024 * 1024ULL;

	return db_options;
}

rocksdb::BlockBasedTableOptions nano::rocksdb_store::get_table_options () const
{
	rocksdb::BlockBasedTableOptions table_options;

	// Block cache for reads
	table_options.block_cache = rocksdb::NewLRUCache (rocksdb_config.block_cache * 1024 * 1024ULL);

	// Bloom filter to help with point reads
	auto bloom_filter_bits = rocksdb_config.bloom_filter_bits;
	if (bloom_filter_bits > 0)
	{
		table_options.filter_policy.reset (rocksdb::NewBloomFilterPolicy (bloom_filter_bits, false));
	}

	// Increasing block_size decreases memory usage and space amplification, but increases read amplification.
	table_options.block_size = rocksdb_config.block_size * 1024ULL;

	// Whether index and filter blocks are stored in block_cache. These settings should be synced
	table_options.cache_index_and_filter_blocks = rocksdb_config.cache_index_and_filter_blocks;
	table_options.pin_l0_filter_and_index_blocks_in_cache = rocksdb_config.cache_index_and_filter_blocks;

	return table_options;
}

rocksdb::ColumnFamilyOptions nano::rocksdb_store::get_cf_options () const
{
	rocksdb::ColumnFamilyOptions cf_options;
	cf_options.table_factory = table_factory;

	// Number of files in level which triggers compaction. Size of L0 and L1 should be kept similar as this is the only compaction which is single threaded
	cf_options.level0_file_num_compaction_trigger = 4;

	// L1 size, compaction is triggered for L0 at this size (4 SST files in L1)
	cf_options.max_bytes_for_level_base = 1024ULL * 1024 * 4 * rocksdb_config.memtable_size;

	// Each level is a multiple of the above. If L1 is 512MB. L2 will be 512 * 8 = 2GB. L3 will be 2GB * 8 = 16GB, and so on...
	cf_options.max_bytes_for_level_multiplier = 8;

	// Files older than this (1 day) will be scheduled for compaction when there is no other background work. This can lead to more writes however.
	cf_options.ttl = 1 * 24 * 60 * 60;

	// Size of level 1 sst files
	cf_options.target_file_size_base = 1024ULL * 1024 * rocksdb_config.memtable_size;

	// Size of each memtable
	cf_options.write_buffer_size = 1024ULL * 1024 * rocksdb_config.memtable_size;

	// Size target of levels are changed dynamically based on size of the last level
	cf_options.level_compaction_dynamic_level_bytes = true;

	// Number of memtables to keep in memory (1 active, rest inactive/immutable)
	cf_options.max_write_buffer_number = rocksdb_config.num_memtables;

	return cf_options;
}

std::vector<nano::tables> nano::rocksdb_store::all_tables () const
{
	return std::vector<nano::tables>{ tables::accounts, tables::cached_counts, tables::change_blocks, tables::confirmation_height, tables::frontiers, tables::meta, tables::online_weight, tables::open_blocks, tables::peers, tables::pending, tables::receive_blocks, tables::representation, tables::send_blocks, tables::state_blocks, tables::unchecked, tables::vote };
}

bool nano::rocksdb_store::copy_db (boost::filesystem::path const & destination_path)
{
	std::unique_ptr<rocksdb::BackupEngine> backup_engine;
	{
		rocksdb::BackupEngine * backup_engine_raw;
		rocksdb::BackupableDBOptions backup_options (destination_path.string ());
		// Use incremental backups (default)
		backup_options.share_table_files = true;

		// Increase number of threads used for copying
		backup_options.max_background_operations = std::thread::hardware_concurrency ();
		auto status = rocksdb::BackupEngine::Open (rocksdb::Env::Default (), backup_options, &backup_engine_raw);
		backup_engine.reset (backup_engine_raw);
		if (!status.ok ())
		{
			return false;
		}
	}

	auto status = backup_engine->CreateNewBackup (db);
	if (!status.ok ())
	{
		return false;
	}

	std::vector<rocksdb::BackupInfo> backup_infos;
	backup_engine->GetBackupInfo (&backup_infos);

	for (auto const & backup_info : backup_infos)
	{
		status = backup_engine->VerifyBackup (backup_info.backup_id);
		if (!status.ok ())
		{
			return false;
		}
	}

	rocksdb::BackupEngineReadOnly * backup_engine_read;
	status = rocksdb::BackupEngineReadOnly::Open (rocksdb::Env::Default (), rocksdb::BackupableDBOptions (destination_path.string ()), &backup_engine_read);
	if (!status.ok ())
	{
		delete backup_engine_read;
		return false;
	}

	// First remove all files (not directories) in the destination
	for (boost::filesystem::directory_iterator end_dir_it, it (destination_path); it != end_dir_it; ++it)
	{
		auto path = it->path ();
		if (boost::filesystem::is_regular_file (path))
		{
			boost::filesystem::remove (it->path ());
		}
	}

	// Now generate the relevant files from the backup
	status = backup_engine->RestoreDBFromLatestBackup (destination_path.string (), destination_path.string ());
	delete backup_engine_read;

	// Open it so that it flushes all WAL files
	if (status.ok ())
	{
		nano::rocksdb_store rocksdb_store (logger, destination_path.string (), rocksdb_config, false);
		return !rocksdb_store.init_error ();
	}
	return false;
}

bool nano::rocksdb_store::init_error () const
{
	return error;
}
// Explicitly instantiate
template class nano::block_store_partial<rocksdb::Slice, nano::rocksdb_store>;
