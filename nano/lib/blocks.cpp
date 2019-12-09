#include <nano/crypto_lib/random_pool.hpp>
#include <nano/lib/blocks.hpp>
#include <nano/lib/memory.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/lib/threading.hpp>

#include <crypto/cryptopp/words.h>

#include <boost/endian/conversion.hpp>
#include <boost/property_tree/json_parser.hpp>

/** Compare blocks, first by type, then content. This is an optimization over dynamic_cast, which is very slow on some platforms. */
namespace
{
template <typename T>
bool blocks_equal (T const & first, nano::block const & second)
{
	static_assert (std::is_base_of<nano::block, T>::value, "Input parameter is not a block type");
	return (first.type () == second.type ()) && (static_cast<T const &> (second)) == first;
}

template <typename block>
std::shared_ptr<block> deserialize_block (nano::stream & stream_a)
{
	auto error (false);
	auto result = nano::make_shared<block> (error, stream_a);
	if (error)
	{
		result = nullptr;
	}

	return result;
}
}

void nano::block_memory_pool_purge ()
{
	nano::purge_singleton_pool_memory<nano::open_block> ();
	nano::purge_singleton_pool_memory<nano::state_block> ();
	nano::purge_singleton_pool_memory<nano::send_block> ();
	nano::purge_singleton_pool_memory<nano::change_block> ();
}

std::string nano::block::to_json () const
{
	std::string result;
	serialize_json (result);
	return result;
}

size_t nano::block::size (nano::block_type type_a)
{
	size_t result (0);
	switch (type_a)
	{
		case nano::block_type::invalid:
		case nano::block_type::not_a_block:
			assert (false);
			break;
		case nano::block_type::send:
			result = nano::send_block::size;
			break;
		case nano::block_type::receive:
			result = nano::receive_block::size;
			break;
		case nano::block_type::change:
			result = nano::change_block::size;
			break;
		case nano::block_type::open:
			result = nano::open_block::size;
			break;
		case nano::block_type::state:
			result = nano::state_block::size;
			break;
	}
	return result;
}

nano::block_hash nano::block::hash () const
{
	nano::block_hash result;
	blake2b_state hash_l;
	auto status (blake2b_init (&hash_l, sizeof (result.bytes)));
	assert (status == 0);
	hash (hash_l);
	status = blake2b_final (&hash_l, result.bytes.data (), sizeof (result.bytes));
	assert (status == 0);
	return result;
}

nano::block_hash nano::block::full_hash () const
{
	nano::block_hash result;
	blake2b_state state;
	blake2b_init (&state, sizeof (result.bytes));
	blake2b_update (&state, hash ().bytes.data (), sizeof (hash ()));
	auto signature (block_signature ());
	blake2b_update (&state, signature.bytes.data (), sizeof (signature));
	auto work (block_work ());
	blake2b_update (&state, &work, sizeof (work));
	blake2b_final (&state, result.bytes.data (), sizeof (result.bytes));
	return result;
}

nano::account const & nano::block::representative () const
{
	static nano::account rep{ 0 };
	return rep;
}

nano::block_hash const & nano::block::source () const
{
	static nano::block_hash source{ 0 };
	return source;
}

nano::link const & nano::block::link () const
{
	static nano::link link{ 0 };
	return link;
}

nano::account const & nano::block::account () const
{
	static nano::account account{ 0 };
	return account;
}

nano::qualified_root nano::block::qualified_root () const
{
	return nano::qualified_root (previous (), root ());
}

nano::amount const & nano::block::balance () const
{
	static nano::amount amount{ 0 };
	return amount;
}

void nano::send_block::visit (nano::block_visitor & visitor_a) const
{
	visitor_a.send_block (*this);
}

void nano::send_block::hash (blake2b_state & hash_a) const
{
	hashables.hash (hash_a);
}

uint64_t nano::send_block::block_work () const
{
	return work;
}

void nano::send_block::block_work_set (uint64_t work_a)
{
	work = work_a;
}

nano::send_hashables::send_hashables (nano::block_hash const & previous_a, nano::account const & destination_a, nano::amount const & balance_a)
	: previous (previous_a)
	, destination (destination_a)
	, balance (balance_a)
{
}

nano::send_hashables::send_hashables (bool & error_a, nano::stream & stream_a)
{
	try
	{
		nano::read (stream_a, previous.bytes);
		nano::read (stream_a, destination.bytes);
		nano::read (stream_a, balance.bytes);
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

nano::send_hashables::send_hashables (bool & error_a, boost::property_tree::ptree const & tree_a)
{
	try
	{
		auto previous_l (tree_a.get<std::string> ("previous"));
		auto destination_l (tree_a.get<std::string> ("destination"));
		auto balance_l (tree_a.get<std::string> ("balance"));
		error_a = previous.decode_hex (previous_l);
		if (!error_a)
		{
			error_a = destination.decode_account (destination_l);
			if (!error_a)
			{
				error_a = balance.decode_hex (balance_l);
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

void nano::send_hashables::hash (blake2b_state & hash_a) const
{
	auto status (blake2b_update (&hash_a, previous.bytes.data (), sizeof (previous.bytes)));
	assert (status == 0);
	status = blake2b_update (&hash_a, destination.bytes.data (), sizeof (destination.bytes));
	assert (status == 0);
	status = blake2b_update (&hash_a, balance.bytes.data (), sizeof (balance.bytes));
	assert (status == 0);
}

void nano::send_block::serialize (nano::stream & stream_a) const
{
	write (stream_a, hashables.previous.bytes);
	write (stream_a, hashables.destination.bytes);
	write (stream_a, hashables.balance.bytes);
	write (stream_a, signature.bytes);
	write (stream_a, work);
}

bool nano::send_block::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		read (stream_a, hashables.previous.bytes);
		read (stream_a, hashables.destination.bytes);
		read (stream_a, hashables.balance.bytes);
		read (stream_a, signature.bytes);
		read (stream_a, work);
	}
	catch (std::exception const &)
	{
		error = true;
	}

	return error;
}

void nano::send_block::serialize_json (std::string & string_a, bool single_line) const
{
	boost::property_tree::ptree tree;
	serialize_json (tree);
	std::stringstream ostream;
	boost::property_tree::write_json (ostream, tree, !single_line);
	string_a = ostream.str ();
}

void nano::send_block::serialize_json (boost::property_tree::ptree & tree) const
{
	tree.put ("type", "send");
	std::string previous;
	hashables.previous.encode_hex (previous);
	tree.put ("previous", previous);
	tree.put ("destination", hashables.destination.to_account ());
	std::string balance;
	hashables.balance.encode_hex (balance);
	tree.put ("balance", balance);
	std::string signature_l;
	signature.encode_hex (signature_l);
	tree.put ("work", nano::to_string_hex (work));
	tree.put ("signature", signature_l);
}

bool nano::send_block::deserialize_json (boost::property_tree::ptree const & tree_a)
{
	auto error (false);
	try
	{
		assert (tree_a.get<std::string> ("type") == "send");
		auto previous_l (tree_a.get<std::string> ("previous"));
		auto destination_l (tree_a.get<std::string> ("destination"));
		auto balance_l (tree_a.get<std::string> ("balance"));
		auto work_l (tree_a.get<std::string> ("work"));
		auto signature_l (tree_a.get<std::string> ("signature"));
		error = hashables.previous.decode_hex (previous_l);
		if (!error)
		{
			error = hashables.destination.decode_account (destination_l);
			if (!error)
			{
				error = hashables.balance.decode_hex (balance_l);
				if (!error)
				{
					error = nano::from_string_hex (work_l, work);
					if (!error)
					{
						error = signature.decode_hex (signature_l);
					}
				}
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}
	return error;
}

nano::send_block::send_block (nano::block_hash const & previous_a, nano::account const & destination_a, nano::amount const & balance_a, nano::raw_key const & prv_a, nano::public_key const & pub_a, uint64_t work_a)
	: hashables (previous_a, destination_a, balance_a)
	, signature (nano::sign_message (prv_a, pub_a, hash ()))
	, work (work_a)
{
}

nano::send_block::send_block (bool & error_a, nano::stream & stream_a)
	: hashables (error_a, stream_a)
{
	if (!error_a)
	{
		try
		{
			nano::read (stream_a, signature.bytes);
			nano::read (stream_a, work);
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

nano::send_block::send_block (bool & error_a, boost::property_tree::ptree const & tree_a)
	: hashables (error_a, tree_a)
{
	if (!error_a)
	{
		try
		{
			auto signature_l (tree_a.get<std::string> ("signature"));
			auto work_l (tree_a.get<std::string> ("work"));
			error_a = signature.decode_hex (signature_l);
			if (!error_a)
			{
				error_a = nano::from_string_hex (work_l, work);
			}
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

bool nano::send_block::operator== (nano::block const & other_a) const
{
	return blocks_equal (*this, other_a);
}

bool nano::send_block::valid_predecessor (nano::block const & block_a) const
{
	bool result;
	switch (block_a.type ())
	{
		case nano::block_type::send:
		case nano::block_type::receive:
		case nano::block_type::open:
		case nano::block_type::change:
			result = true;
			break;
		default:
			result = false;
			break;
	}
	return result;
}

nano::block_type nano::send_block::type () const
{
	return nano::block_type::send;
}

bool nano::send_block::operator== (nano::send_block const & other_a) const
{
	auto result (hashables.destination == other_a.hashables.destination && hashables.previous == other_a.hashables.previous && hashables.balance == other_a.hashables.balance && work == other_a.work && signature == other_a.signature);
	return result;
}

nano::block_hash const & nano::send_block::previous () const
{
	return hashables.previous;
}

nano::root const & nano::send_block::root () const
{
	return hashables.previous;
}

nano::amount const & nano::send_block::balance () const
{
	return hashables.balance;
}

nano::signature const & nano::send_block::block_signature () const
{
	return signature;
}

void nano::send_block::signature_set (nano::signature const & signature_a)
{
	signature = signature_a;
}

nano::open_hashables::open_hashables (nano::block_hash const & source_a, nano::account const & representative_a, nano::account const & account_a)
	: source (source_a)
	, representative (representative_a)
	, account (account_a)
{
}

nano::open_hashables::open_hashables (bool & error_a, nano::stream & stream_a)
{
	try
	{
		nano::read (stream_a, source.bytes);
		nano::read (stream_a, representative.bytes);
		nano::read (stream_a, account.bytes);
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

nano::open_hashables::open_hashables (bool & error_a, boost::property_tree::ptree const & tree_a)
{
	try
	{
		auto source_l (tree_a.get<std::string> ("source"));
		auto representative_l (tree_a.get<std::string> ("representative"));
		auto account_l (tree_a.get<std::string> ("account"));
		error_a = source.decode_hex (source_l);
		if (!error_a)
		{
			error_a = representative.decode_account (representative_l);
			if (!error_a)
			{
				error_a = account.decode_account (account_l);
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

void nano::open_hashables::hash (blake2b_state & hash_a) const
{
	blake2b_update (&hash_a, source.bytes.data (), sizeof (source.bytes));
	blake2b_update (&hash_a, representative.bytes.data (), sizeof (representative.bytes));
	blake2b_update (&hash_a, account.bytes.data (), sizeof (account.bytes));
}

nano::open_block::open_block (nano::block_hash const & source_a, nano::account const & representative_a, nano::account const & account_a, nano::raw_key const & prv_a, nano::public_key const & pub_a, uint64_t work_a)
	: hashables (source_a, representative_a, account_a)
	, signature (nano::sign_message (prv_a, pub_a, hash ()))
	, work (work_a)
{
	assert (!representative_a.is_zero ());
}

nano::open_block::open_block (nano::block_hash const & source_a, nano::account const & representative_a, nano::account const & account_a, std::nullptr_t)
	: hashables (source_a, representative_a, account_a)
	, work (0)
{
	signature.clear ();
}

nano::open_block::open_block (bool & error_a, nano::stream & stream_a)
	: hashables (error_a, stream_a)
{
	if (!error_a)
	{
		try
		{
			nano::read (stream_a, signature);
			nano::read (stream_a, work);
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

nano::open_block::open_block (bool & error_a, boost::property_tree::ptree const & tree_a)
	: hashables (error_a, tree_a)
{
	if (!error_a)
	{
		try
		{
			auto work_l (tree_a.get<std::string> ("work"));
			auto signature_l (tree_a.get<std::string> ("signature"));
			error_a = nano::from_string_hex (work_l, work);
			if (!error_a)
			{
				error_a = signature.decode_hex (signature_l);
			}
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

void nano::open_block::hash (blake2b_state & hash_a) const
{
	hashables.hash (hash_a);
}

uint64_t nano::open_block::block_work () const
{
	return work;
}

void nano::open_block::block_work_set (uint64_t work_a)
{
	work = work_a;
}

nano::block_hash const & nano::open_block::previous () const
{
	static nano::block_hash result{ 0 };
	return result;
}

nano::account const & nano::open_block::account () const
{
	return hashables.account;
}

void nano::open_block::serialize (nano::stream & stream_a) const
{
	write (stream_a, hashables.source);
	write (stream_a, hashables.representative);
	write (stream_a, hashables.account);
	write (stream_a, signature);
	write (stream_a, work);
}

bool nano::open_block::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		read (stream_a, hashables.source);
		read (stream_a, hashables.representative);
		read (stream_a, hashables.account);
		read (stream_a, signature);
		read (stream_a, work);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

void nano::open_block::serialize_json (std::string & string_a, bool single_line) const
{
	boost::property_tree::ptree tree;
	serialize_json (tree);
	std::stringstream ostream;
	boost::property_tree::write_json (ostream, tree, !single_line);
	string_a = ostream.str ();
}

void nano::open_block::serialize_json (boost::property_tree::ptree & tree) const
{
	tree.put ("type", "open");
	tree.put ("source", hashables.source.to_string ());
	tree.put ("representative", representative ().to_account ());
	tree.put ("account", hashables.account.to_account ());
	std::string signature_l;
	signature.encode_hex (signature_l);
	tree.put ("work", nano::to_string_hex (work));
	tree.put ("signature", signature_l);
}

bool nano::open_block::deserialize_json (boost::property_tree::ptree const & tree_a)
{
	auto error (false);
	try
	{
		assert (tree_a.get<std::string> ("type") == "open");
		auto source_l (tree_a.get<std::string> ("source"));
		auto representative_l (tree_a.get<std::string> ("representative"));
		auto account_l (tree_a.get<std::string> ("account"));
		auto work_l (tree_a.get<std::string> ("work"));
		auto signature_l (tree_a.get<std::string> ("signature"));
		error = hashables.source.decode_hex (source_l);
		if (!error)
		{
			error = hashables.representative.decode_hex (representative_l);
			if (!error)
			{
				error = hashables.account.decode_hex (account_l);
				if (!error)
				{
					error = nano::from_string_hex (work_l, work);
					if (!error)
					{
						error = signature.decode_hex (signature_l);
					}
				}
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}
	return error;
}

void nano::open_block::visit (nano::block_visitor & visitor_a) const
{
	visitor_a.open_block (*this);
}

nano::block_type nano::open_block::type () const
{
	return nano::block_type::open;
}

bool nano::open_block::operator== (nano::block const & other_a) const
{
	return blocks_equal (*this, other_a);
}

bool nano::open_block::operator== (nano::open_block const & other_a) const
{
	return hashables.source == other_a.hashables.source && hashables.representative == other_a.hashables.representative && hashables.account == other_a.hashables.account && work == other_a.work && signature == other_a.signature;
}

bool nano::open_block::valid_predecessor (nano::block const & block_a) const
{
	return false;
}

nano::block_hash const & nano::open_block::source () const
{
	return hashables.source;
}

nano::root const & nano::open_block::root () const
{
	return hashables.account;
}

nano::account const & nano::open_block::representative () const
{
	return hashables.representative;
}

nano::signature const & nano::open_block::block_signature () const
{
	return signature;
}

void nano::open_block::signature_set (nano::signature const & signature_a)
{
	signature = signature_a;
}

nano::change_hashables::change_hashables (nano::block_hash const & previous_a, nano::account const & representative_a)
	: previous (previous_a)
	, representative (representative_a)
{
}

nano::change_hashables::change_hashables (bool & error_a, nano::stream & stream_a)
{
	try
	{
		nano::read (stream_a, previous);
		nano::read (stream_a, representative);
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

nano::change_hashables::change_hashables (bool & error_a, boost::property_tree::ptree const & tree_a)
{
	try
	{
		auto previous_l (tree_a.get<std::string> ("previous"));
		auto representative_l (tree_a.get<std::string> ("representative"));
		error_a = previous.decode_hex (previous_l);
		if (!error_a)
		{
			error_a = representative.decode_account (representative_l);
		}
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

void nano::change_hashables::hash (blake2b_state & hash_a) const
{
	blake2b_update (&hash_a, previous.bytes.data (), sizeof (previous.bytes));
	blake2b_update (&hash_a, representative.bytes.data (), sizeof (representative.bytes));
}

nano::change_block::change_block (nano::block_hash const & previous_a, nano::account const & representative_a, nano::raw_key const & prv_a, nano::public_key const & pub_a, uint64_t work_a)
	: hashables (previous_a, representative_a)
	, signature (nano::sign_message (prv_a, pub_a, hash ()))
	, work (work_a)
{
}

nano::change_block::change_block (bool & error_a, nano::stream & stream_a)
	: hashables (error_a, stream_a)
{
	if (!error_a)
	{
		try
		{
			nano::read (stream_a, signature);
			nano::read (stream_a, work);
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

nano::change_block::change_block (bool & error_a, boost::property_tree::ptree const & tree_a)
	: hashables (error_a, tree_a)
{
	if (!error_a)
	{
		try
		{
			auto work_l (tree_a.get<std::string> ("work"));
			auto signature_l (tree_a.get<std::string> ("signature"));
			error_a = nano::from_string_hex (work_l, work);
			if (!error_a)
			{
				error_a = signature.decode_hex (signature_l);
			}
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

void nano::change_block::hash (blake2b_state & hash_a) const
{
	hashables.hash (hash_a);
}

uint64_t nano::change_block::block_work () const
{
	return work;
}

void nano::change_block::block_work_set (uint64_t work_a)
{
	work = work_a;
}

nano::block_hash const & nano::change_block::previous () const
{
	return hashables.previous;
}

void nano::change_block::serialize (nano::stream & stream_a) const
{
	write (stream_a, hashables.previous);
	write (stream_a, hashables.representative);
	write (stream_a, signature);
	write (stream_a, work);
}

bool nano::change_block::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		read (stream_a, hashables.previous);
		read (stream_a, hashables.representative);
		read (stream_a, signature);
		read (stream_a, work);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

void nano::change_block::serialize_json (std::string & string_a, bool single_line) const
{
	boost::property_tree::ptree tree;
	serialize_json (tree);
	std::stringstream ostream;
	boost::property_tree::write_json (ostream, tree, !single_line);
	string_a = ostream.str ();
}

void nano::change_block::serialize_json (boost::property_tree::ptree & tree) const
{
	tree.put ("type", "change");
	tree.put ("previous", hashables.previous.to_string ());
	tree.put ("representative", representative ().to_account ());
	tree.put ("work", nano::to_string_hex (work));
	std::string signature_l;
	signature.encode_hex (signature_l);
	tree.put ("signature", signature_l);
}

bool nano::change_block::deserialize_json (boost::property_tree::ptree const & tree_a)
{
	auto error (false);
	try
	{
		assert (tree_a.get<std::string> ("type") == "change");
		auto previous_l (tree_a.get<std::string> ("previous"));
		auto representative_l (tree_a.get<std::string> ("representative"));
		auto work_l (tree_a.get<std::string> ("work"));
		auto signature_l (tree_a.get<std::string> ("signature"));
		error = hashables.previous.decode_hex (previous_l);
		if (!error)
		{
			error = hashables.representative.decode_hex (representative_l);
			if (!error)
			{
				error = nano::from_string_hex (work_l, work);
				if (!error)
				{
					error = signature.decode_hex (signature_l);
				}
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}
	return error;
}

void nano::change_block::visit (nano::block_visitor & visitor_a) const
{
	visitor_a.change_block (*this);
}

nano::block_type nano::change_block::type () const
{
	return nano::block_type::change;
}

bool nano::change_block::operator== (nano::block const & other_a) const
{
	return blocks_equal (*this, other_a);
}

bool nano::change_block::operator== (nano::change_block const & other_a) const
{
	return hashables.previous == other_a.hashables.previous && hashables.representative == other_a.hashables.representative && work == other_a.work && signature == other_a.signature;
}

bool nano::change_block::valid_predecessor (nano::block const & block_a) const
{
	bool result;
	switch (block_a.type ())
	{
		case nano::block_type::send:
		case nano::block_type::receive:
		case nano::block_type::open:
		case nano::block_type::change:
			result = true;
			break;
		default:
			result = false;
			break;
	}
	return result;
}

nano::root const & nano::change_block::root () const
{
	return hashables.previous;
}

nano::account const & nano::change_block::representative () const
{
	return hashables.representative;
}

nano::signature const & nano::change_block::block_signature () const
{
	return signature;
}

void nano::change_block::signature_set (nano::signature const & signature_a)
{
	signature = signature_a;
}

nano::state_hashables::state_hashables (nano::account const & account_a, nano::block_hash const & previous_a, nano::account const & representative_a, nano::amount const & balance_a, nano::link const & link_a)
	: account (account_a)
	, previous (previous_a)
	, representative (representative_a)
	, balance (balance_a)
	, link (link_a)
{
}

nano::state_hashables::state_hashables (bool & error_a, nano::stream & stream_a)
{
	try
	{
		nano::read (stream_a, account);
		nano::read (stream_a, previous);
		nano::read (stream_a, representative);
		nano::read (stream_a, balance);
		nano::read (stream_a, link);
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

nano::state_hashables::state_hashables (bool & error_a, boost::property_tree::ptree const & tree_a)
{
	try
	{
		auto account_l (tree_a.get<std::string> ("account"));
		auto previous_l (tree_a.get<std::string> ("previous"));
		auto representative_l (tree_a.get<std::string> ("representative"));
		auto balance_l (tree_a.get<std::string> ("balance"));
		auto link_l (tree_a.get<std::string> ("link"));
		error_a = account.decode_account (account_l);
		if (!error_a)
		{
			error_a = previous.decode_hex (previous_l);
			if (!error_a)
			{
				error_a = representative.decode_account (representative_l);
				if (!error_a)
				{
					error_a = balance.decode_dec (balance_l);
					if (!error_a)
					{
						error_a = link.decode_account (link_l) && link.decode_hex (link_l);
					}
				}
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

void nano::state_hashables::hash (blake2b_state & hash_a) const
{
	blake2b_update (&hash_a, account.bytes.data (), sizeof (account.bytes));
	blake2b_update (&hash_a, previous.bytes.data (), sizeof (previous.bytes));
	blake2b_update (&hash_a, representative.bytes.data (), sizeof (representative.bytes));
	blake2b_update (&hash_a, balance.bytes.data (), sizeof (balance.bytes));
	blake2b_update (&hash_a, link.bytes.data (), sizeof (link.bytes));
}

nano::state_block::state_block (nano::account const & account_a, nano::block_hash const & previous_a, nano::account const & representative_a, nano::amount const & balance_a, nano::link const & link_a, nano::raw_key const & prv_a, nano::public_key const & pub_a, uint64_t work_a)
	: hashables (account_a, previous_a, representative_a, balance_a, link_a)
	, signature (nano::sign_message (prv_a, pub_a, hash ()))
	, work (work_a)
{
}

nano::state_block::state_block (bool & error_a, nano::stream & stream_a)
	: hashables (error_a, stream_a)
{
	if (!error_a)
	{
		try
		{
			nano::read (stream_a, signature);
			nano::read (stream_a, work);
			boost::endian::big_to_native_inplace (work);
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

nano::state_block::state_block (bool & error_a, boost::property_tree::ptree const & tree_a)
	: hashables (error_a, tree_a)
{
	if (!error_a)
	{
		try
		{
			auto type_l (tree_a.get<std::string> ("type"));
			auto signature_l (tree_a.get<std::string> ("signature"));
			auto work_l (tree_a.get<std::string> ("work"));
			error_a = type_l != "state";
			if (!error_a)
			{
				error_a = nano::from_string_hex (work_l, work);
				if (!error_a)
				{
					error_a = signature.decode_hex (signature_l);
				}
			}
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

void nano::state_block::hash (blake2b_state & hash_a) const
{
	nano::uint256_union preamble (static_cast<uint64_t> (nano::block_type::state));
	blake2b_update (&hash_a, preamble.bytes.data (), preamble.bytes.size ());
	hashables.hash (hash_a);
}

uint64_t nano::state_block::block_work () const
{
	return work;
}

void nano::state_block::block_work_set (uint64_t work_a)
{
	work = work_a;
}

nano::block_hash const & nano::state_block::previous () const
{
	return hashables.previous;
}

nano::account const & nano::state_block::account () const
{
	return hashables.account;
}

void nano::state_block::serialize (nano::stream & stream_a) const
{
	write (stream_a, hashables.account);
	write (stream_a, hashables.previous);
	write (stream_a, hashables.representative);
	write (stream_a, hashables.balance);
	write (stream_a, hashables.link);
	write (stream_a, signature);
	write (stream_a, boost::endian::native_to_big (work));
}

bool nano::state_block::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		read (stream_a, hashables.account);
		read (stream_a, hashables.previous);
		read (stream_a, hashables.representative);
		read (stream_a, hashables.balance);
		read (stream_a, hashables.link);
		read (stream_a, signature);
		read (stream_a, work);
		boost::endian::big_to_native_inplace (work);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

void nano::state_block::serialize_json (std::string & string_a, bool single_line) const
{
	boost::property_tree::ptree tree;
	serialize_json (tree);
	std::stringstream ostream;
	boost::property_tree::write_json (ostream, tree, !single_line);
	string_a = ostream.str ();
}

void nano::state_block::serialize_json (boost::property_tree::ptree & tree) const
{
	tree.put ("type", "state");
	tree.put ("account", hashables.account.to_account ());
	tree.put ("previous", hashables.previous.to_string ());
	tree.put ("representative", representative ().to_account ());
	tree.put ("balance", hashables.balance.to_string_dec ());
	tree.put ("link", hashables.link.to_string ());
	tree.put ("link_as_account", hashables.link.to_account ());
	std::string signature_l;
	signature.encode_hex (signature_l);
	tree.put ("signature", signature_l);
	tree.put ("work", nano::to_string_hex (work));
}

bool nano::state_block::deserialize_json (boost::property_tree::ptree const & tree_a)
{
	auto error (false);
	try
	{
		assert (tree_a.get<std::string> ("type") == "state");
		auto account_l (tree_a.get<std::string> ("account"));
		auto previous_l (tree_a.get<std::string> ("previous"));
		auto representative_l (tree_a.get<std::string> ("representative"));
		auto balance_l (tree_a.get<std::string> ("balance"));
		auto link_l (tree_a.get<std::string> ("link"));
		auto work_l (tree_a.get<std::string> ("work"));
		auto signature_l (tree_a.get<std::string> ("signature"));
		error = hashables.account.decode_account (account_l);
		if (!error)
		{
			error = hashables.previous.decode_hex (previous_l);
			if (!error)
			{
				error = hashables.representative.decode_account (representative_l);
				if (!error)
				{
					error = hashables.balance.decode_dec (balance_l);
					if (!error)
					{
						error = hashables.link.decode_account (link_l) && hashables.link.decode_hex (link_l);
						if (!error)
						{
							error = nano::from_string_hex (work_l, work);
							if (!error)
							{
								error = signature.decode_hex (signature_l);
							}
						}
					}
				}
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}
	return error;
}

void nano::state_block::visit (nano::block_visitor & visitor_a) const
{
	visitor_a.state_block (*this);
}

nano::block_type nano::state_block::type () const
{
	return nano::block_type::state;
}

bool nano::state_block::operator== (nano::block const & other_a) const
{
	return blocks_equal (*this, other_a);
}

bool nano::state_block::operator== (nano::state_block const & other_a) const
{
	return hashables.account == other_a.hashables.account && hashables.previous == other_a.hashables.previous && hashables.representative == other_a.hashables.representative && hashables.balance == other_a.hashables.balance && hashables.link == other_a.hashables.link && signature == other_a.signature && work == other_a.work;
}

bool nano::state_block::valid_predecessor (nano::block const & block_a) const
{
	return true;
}

nano::root const & nano::state_block::root () const
{
	if (!hashables.previous.is_zero ())
	{
		return hashables.previous;
	}
	else
	{
		return hashables.account;
	}
}

nano::link const & nano::state_block::link () const
{
	return hashables.link;
}

nano::account const & nano::state_block::representative () const
{
	return hashables.representative;
}

nano::amount const & nano::state_block::balance () const
{
	return hashables.balance;
}

nano::signature const & nano::state_block::block_signature () const
{
	return signature;
}

void nano::state_block::signature_set (nano::signature const & signature_a)
{
	signature = signature_a;
}

std::shared_ptr<nano::block> nano::deserialize_block_json (boost::property_tree::ptree const & tree_a, nano::block_uniquer * uniquer_a)
{
	std::shared_ptr<nano::block> result;
	try
	{
		auto type (tree_a.get<std::string> ("type"));
		bool error (false);
		std::unique_ptr<nano::block> obj;
		if (type == "receive")
		{
			obj = std::make_unique<nano::receive_block> (error, tree_a);
		}
		else if (type == "send")
		{
			obj = std::make_unique<nano::send_block> (error, tree_a);
		}
		else if (type == "open")
		{
			obj = std::make_unique<nano::open_block> (error, tree_a);
		}
		else if (type == "change")
		{
			obj = std::make_unique<nano::change_block> (error, tree_a);
		}
		else if (type == "state")
		{
			obj = std::make_unique<nano::state_block> (error, tree_a);
		}

		if (!error)
		{
			result = std::move (obj);
		}
	}
	catch (std::runtime_error const &)
	{
	}
	if (uniquer_a != nullptr)
	{
		result = uniquer_a->unique (result);
	}
	return result;
}

std::shared_ptr<nano::block> nano::deserialize_block (nano::stream & stream_a)
{
	nano::block_type type;
	auto error (try_read (stream_a, type));
	std::shared_ptr<nano::block> result;
	if (!error)
	{
		result = nano::deserialize_block (stream_a, type);
	}
	return result;
}

std::shared_ptr<nano::block> nano::deserialize_block (nano::stream & stream_a, nano::block_type type_a, nano::block_uniquer * uniquer_a)
{
	std::shared_ptr<nano::block> result;
	switch (type_a)
	{
		case nano::block_type::receive:
		{
			result = ::deserialize_block<nano::receive_block> (stream_a);
			break;
		}
		case nano::block_type::send:
		{
			result = ::deserialize_block<nano::send_block> (stream_a);
			break;
		}
		case nano::block_type::open:
		{
			result = ::deserialize_block<nano::open_block> (stream_a);
			break;
		}
		case nano::block_type::change:
		{
			result = ::deserialize_block<nano::change_block> (stream_a);
			break;
		}
		case nano::block_type::state:
		{
			result = ::deserialize_block<nano::state_block> (stream_a);
			break;
		}
		default:
			assert (false);
			break;
	}
	if (uniquer_a != nullptr)
	{
		result = uniquer_a->unique (result);
	}
	return result;
}

void nano::receive_block::visit (nano::block_visitor & visitor_a) const
{
	visitor_a.receive_block (*this);
}

bool nano::receive_block::operator== (nano::receive_block const & other_a) const
{
	auto result (hashables.previous == other_a.hashables.previous && hashables.source == other_a.hashables.source && work == other_a.work && signature == other_a.signature);
	return result;
}

void nano::receive_block::serialize (nano::stream & stream_a) const
{
	write (stream_a, hashables.previous.bytes);
	write (stream_a, hashables.source.bytes);
	write (stream_a, signature.bytes);
	write (stream_a, work);
}

bool nano::receive_block::deserialize (nano::stream & stream_a)
{
	auto error (false);
	try
	{
		read (stream_a, hashables.previous.bytes);
		read (stream_a, hashables.source.bytes);
		read (stream_a, signature.bytes);
		read (stream_a, work);
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}

	return error;
}

void nano::receive_block::serialize_json (std::string & string_a, bool single_line) const
{
	boost::property_tree::ptree tree;
	serialize_json (tree);
	std::stringstream ostream;
	boost::property_tree::write_json (ostream, tree, !single_line);
	string_a = ostream.str ();
}

void nano::receive_block::serialize_json (boost::property_tree::ptree & tree) const
{
	tree.put ("type", "receive");
	std::string previous;
	hashables.previous.encode_hex (previous);
	tree.put ("previous", previous);
	std::string source;
	hashables.source.encode_hex (source);
	tree.put ("source", source);
	std::string signature_l;
	signature.encode_hex (signature_l);
	tree.put ("work", nano::to_string_hex (work));
	tree.put ("signature", signature_l);
}

bool nano::receive_block::deserialize_json (boost::property_tree::ptree const & tree_a)
{
	auto error (false);
	try
	{
		assert (tree_a.get<std::string> ("type") == "receive");
		auto previous_l (tree_a.get<std::string> ("previous"));
		auto source_l (tree_a.get<std::string> ("source"));
		auto work_l (tree_a.get<std::string> ("work"));
		auto signature_l (tree_a.get<std::string> ("signature"));
		error = hashables.previous.decode_hex (previous_l);
		if (!error)
		{
			error = hashables.source.decode_hex (source_l);
			if (!error)
			{
				error = nano::from_string_hex (work_l, work);
				if (!error)
				{
					error = signature.decode_hex (signature_l);
				}
			}
		}
	}
	catch (std::runtime_error const &)
	{
		error = true;
	}
	return error;
}

nano::receive_block::receive_block (nano::block_hash const & previous_a, nano::block_hash const & source_a, nano::raw_key const & prv_a, nano::public_key const & pub_a, uint64_t work_a)
	: hashables (previous_a, source_a)
	, signature (nano::sign_message (prv_a, pub_a, hash ()))
	, work (work_a)
{
}

nano::receive_block::receive_block (bool & error_a, nano::stream & stream_a)
	: hashables (error_a, stream_a)
{
	if (!error_a)
	{
		try
		{
			nano::read (stream_a, signature);
			nano::read (stream_a, work);
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

nano::receive_block::receive_block (bool & error_a, boost::property_tree::ptree const & tree_a)
	: hashables (error_a, tree_a)
{
	if (!error_a)
	{
		try
		{
			auto signature_l (tree_a.get<std::string> ("signature"));
			auto work_l (tree_a.get<std::string> ("work"));
			error_a = signature.decode_hex (signature_l);
			if (!error_a)
			{
				error_a = nano::from_string_hex (work_l, work);
			}
		}
		catch (std::runtime_error const &)
		{
			error_a = true;
		}
	}
}

void nano::receive_block::hash (blake2b_state & hash_a) const
{
	hashables.hash (hash_a);
}

uint64_t nano::receive_block::block_work () const
{
	return work;
}

void nano::receive_block::block_work_set (uint64_t work_a)
{
	work = work_a;
}

bool nano::receive_block::operator== (nano::block const & other_a) const
{
	return blocks_equal (*this, other_a);
}

bool nano::receive_block::valid_predecessor (nano::block const & block_a) const
{
	bool result;
	switch (block_a.type ())
	{
		case nano::block_type::send:
		case nano::block_type::receive:
		case nano::block_type::open:
		case nano::block_type::change:
			result = true;
			break;
		default:
			result = false;
			break;
	}
	return result;
}

nano::block_hash const & nano::receive_block::previous () const
{
	return hashables.previous;
}

nano::block_hash const & nano::receive_block::source () const
{
	return hashables.source;
}

nano::root const & nano::receive_block::root () const
{
	return hashables.previous;
}

nano::signature const & nano::receive_block::block_signature () const
{
	return signature;
}

void nano::receive_block::signature_set (nano::signature const & signature_a)
{
	signature = signature_a;
}

nano::block_type nano::receive_block::type () const
{
	return nano::block_type::receive;
}

nano::receive_hashables::receive_hashables (nano::block_hash const & previous_a, nano::block_hash const & source_a)
	: previous (previous_a)
	, source (source_a)
{
}

nano::receive_hashables::receive_hashables (bool & error_a, nano::stream & stream_a)
{
	try
	{
		nano::read (stream_a, previous.bytes);
		nano::read (stream_a, source.bytes);
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

nano::receive_hashables::receive_hashables (bool & error_a, boost::property_tree::ptree const & tree_a)
{
	try
	{
		auto previous_l (tree_a.get<std::string> ("previous"));
		auto source_l (tree_a.get<std::string> ("source"));
		error_a = previous.decode_hex (previous_l);
		if (!error_a)
		{
			error_a = source.decode_hex (source_l);
		}
	}
	catch (std::runtime_error const &)
	{
		error_a = true;
	}
}

void nano::receive_hashables::hash (blake2b_state & hash_a) const
{
	blake2b_update (&hash_a, previous.bytes.data (), sizeof (previous.bytes));
	blake2b_update (&hash_a, source.bytes.data (), sizeof (source.bytes));
}

std::shared_ptr<nano::block> nano::block_uniquer::unique (std::shared_ptr<nano::block> block_a)
{
	auto result (block_a);
	if (result != nullptr)
	{
		nano::uint256_union key (block_a->full_hash ());
		nano::lock_guard<std::mutex> lock (mutex);
		auto & existing (blocks[key]);
		if (auto block_l = existing.lock ())
		{
			result = block_l;
		}
		else
		{
			existing = block_a;
		}
		release_assert (std::numeric_limits<CryptoPP::word32>::max () > blocks.size ());
		for (auto i (0); i < cleanup_count && !blocks.empty (); ++i)
		{
			auto random_offset (nano::random_pool::generate_word32 (0, static_cast<CryptoPP::word32> (blocks.size () - 1)));
			auto existing (std::next (blocks.begin (), random_offset));
			if (existing == blocks.end ())
			{
				existing = blocks.begin ();
			}
			if (existing != blocks.end ())
			{
				if (auto block_l = existing->second.lock ())
				{
					// Still live
				}
				else
				{
					blocks.erase (existing);
				}
			}
		}
	}
	return result;
}

size_t nano::block_uniquer::size ()
{
	nano::lock_guard<std::mutex> lock (mutex);
	return blocks.size ();
}

namespace nano
{
std::unique_ptr<seq_con_info_component> collect_seq_con_info (block_uniquer & block_uniquer, const std::string & name)
{
	auto count = block_uniquer.size ();
	auto sizeof_element = sizeof (block_uniquer::value_type);
	auto composite = std::make_unique<seq_con_info_composite> (name);
	composite->add_component (std::make_unique<seq_con_info_leaf> (seq_con_info{ "blocks", count, sizeof_element }));
	return composite;
}
}
