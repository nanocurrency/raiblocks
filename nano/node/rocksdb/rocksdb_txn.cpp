#include <nano/node/rocksdb/rocksdb_txn.hpp>

nano::read_rocksdb_txn::read_rocksdb_txn (rocksdb::DB * db_a)
	: db (db_a)
{
	options.snapshot = db_a->GetSnapshot ();
}

nano::read_rocksdb_txn::~read_rocksdb_txn ()
{
	reset ();
}

void nano::read_rocksdb_txn::reset ()
{
	db->ReleaseSnapshot (options.snapshot);
}

void nano::read_rocksdb_txn::renew ()
{
	options.snapshot = db->GetSnapshot ();
}

void * nano::read_rocksdb_txn::get_handle () const
{
	return (void *)&options;
}

nano::write_rocksdb_txn::write_rocksdb_txn (rocksdb::OptimisticTransactionDB * db_a, std::vector<nano::tables> const & tables_requiring_locks_a, std::vector<nano::tables> const & tables_no_locks_a, std::unordered_map<nano::tables, std::mutex> & mutexes_a)
	: db (db_a)
	, tables_requiring_locks (tables_requiring_locks_a)
	, tables_no_locks (tables_no_locks_a)
	, mutexes (mutexes_a)
{
	lock ();
	rocksdb::OptimisticTransactionOptions txn_options;
	txn_options.set_snapshot = true;
	txn = db->BeginTransaction (rocksdb::WriteOptions (), txn_options);
}

nano::write_rocksdb_txn::~write_rocksdb_txn ()
{
	commit ();
	delete txn;
	unlock ();
}

void nano::write_rocksdb_txn::commit () const
{
	auto status = txn->Commit ();

	// If there are no available memtables try again a few more times
	constexpr auto num_attempts = 10;
	auto attempt_num = 0;
	while (status.IsTryAgain () && attempt_num < num_attempts)
	{
		status = txn->Commit ();
		++attempt_num;
	}

	release_assert (status.ok ());
}

void nano::write_rocksdb_txn::renew ()
{
	rocksdb::OptimisticTransactionOptions txn_options;
	txn_options.set_snapshot = true;
	db->BeginTransaction (rocksdb::WriteOptions (), txn_options, txn);
}

void * nano::write_rocksdb_txn::get_handle () const
{
	return txn;
}

void nano::write_rocksdb_txn::lock ()
{
	for (auto table : tables_requiring_locks)
	{
		mutexes.at (table).lock ();
	}
}

void nano::write_rocksdb_txn::unlock ()
{
	for (auto table : tables_requiring_locks)
	{
		mutexes.at (table).unlock ();
	}
}

bool nano::write_rocksdb_txn::contains (nano::tables table_a) const
{
	return (std::find (tables_requiring_locks.begin (), tables_requiring_locks.end (), table_a) != tables_requiring_locks.end ()) || (std::find (tables_no_locks.begin (), tables_no_locks.end (), table_a) != tables_no_locks.end ());
}
