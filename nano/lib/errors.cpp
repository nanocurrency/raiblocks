#include "nano/lib/errors.hpp"

#include <boost/system/error_code.hpp>

#include <cassert>

std::string nano::error_common_messages::message (int ev) const
{
	switch (static_cast<nano::error_common> (ev))
	{
		case nano::error_common::generic:
			return "Unknown error";
		case nano::error_common::missing_account:
			return "Missing account";
		case nano::error_common::missing_balance:
			return "Missing balance";
		case nano::error_common::missing_link:
			return "Missing link, source or destination";
		case nano::error_common::missing_previous:
			return "Missing previous";
		case nano::error_common::missing_representative:
			return "Missing representative";
		case nano::error_common::missing_signature:
			return "Missing signature";
		case nano::error_common::missing_work:
			return "Missing work";
		case nano::error_common::exception:
			return "Exception thrown";
		case nano::error_common::account_exists:
			return "Account already exists";
		case nano::error_common::account_not_found:
			return "Account not found";
		case nano::error_common::account_not_found_wallet:
			return "Account not found in wallet";
		case nano::error_common::bad_account_number:
			return "Bad account number";
		case nano::error_common::bad_balance:
			return "Bad balance";
		case nano::error_common::bad_link:
			return "Bad link value";
		case nano::error_common::bad_previous:
			return "Bad previous hash";
		case nano::error_common::bad_representative_number:
			return "Bad representative";
		case nano::error_common::bad_source:
			return "Bad source";
		case nano::error_common::bad_signature:
			return "Bad signature";
		case nano::error_common::bad_private_key:
			return "Bad private key";
		case nano::error_common::bad_public_key:
			return "Bad public key";
		case nano::error_common::bad_seed:
			return "Bad seed";
		case nano::error_common::bad_threshold:
			return "Bad threshold number";
		case nano::error_common::bad_wallet_number:
			return "Bad wallet number";
		case nano::error_common::bad_work_format:
			return "Bad work";
		case nano::error_common::disabled_local_work_generation:
			return "Local work generation is disabled";
		case nano::error_common::disabled_work_generation:
			return "Work generation is disabled";
		case nano::error_common::failure_work_generation:
			return "Work generation cancellation or failure";
		case nano::error_common::insufficient_balance:
			return "Insufficient balance";
		case nano::error_common::invalid_amount:
			return "Invalid amount number";
		case nano::error_common::invalid_amount_big:
			return "Amount too big";
		case nano::error_common::invalid_count:
			return "Invalid count";
		case nano::error_common::invalid_ip_address:
			return "Invalid IP address";
		case nano::error_common::invalid_port:
			return "Invalid port";
		case nano::error_common::invalid_index:
			return "Invalid index";
		case nano::error_common::invalid_type_conversion:
			return "Invalid type conversion";
		case nano::error_common::invalid_work:
			return "Invalid work";
		case nano::error_common::numeric_conversion:
			return "Numeric conversion error";
		case nano::error_common::tracking_not_enabled:
			return "Database transaction tracking is not enabled in the config";
		case nano::error_common::wallet_lmdb_max_dbs:
			return "Failed to create wallet. Increase lmdb_max_dbs in node config";
		case nano::error_common::wallet_locked:
			return "Wallet is locked";
		case nano::error_common::wallet_not_found:
			return "Wallet not found";
	}

	return "Invalid error code";
}

std::string nano::error_blocks_messages::message (int ev) const
{
	switch (static_cast<nano::error_blocks> (ev))
	{
		case nano::error_blocks::generic:
			return "Unknown error";
		case nano::error_blocks::bad_hash_number:
			return "Bad hash number";
		case nano::error_blocks::invalid_block:
			return "Block is invalid";
		case nano::error_blocks::invalid_block_hash:
			return "Invalid block hash";
		case nano::error_blocks::invalid_type:
			return "Invalid block type";
		case nano::error_blocks::not_found:
			return "Block not found";
		case nano::error_blocks::work_low:
			return "Block work is less than threshold";
	}

	return "Invalid error code";
}

std::string nano::error_rpc_messages::message (int ev) const
{
	switch (static_cast<nano::error_rpc> (ev))
	{
		case nano::error_rpc::generic:
			return "Unknown error";
		case nano::error_rpc::bad_destination:
			return "Bad destination account";
		case nano::error_rpc::bad_difficulty_format:
			return "Bad difficulty";
		case nano::error_rpc::bad_key:
			return "Bad key";
		case nano::error_rpc::bad_link:
			return "Bad link number";
		case nano::error_rpc::bad_multiplier_format:
			return "Bad multiplier";
		case nano::error_rpc::bad_previous:
			return "Bad previous";
		case nano::error_rpc::bad_representative_number:
			return "Bad representative number";
		case nano::error_rpc::bad_source:
			return "Bad source";
		case nano::error_rpc::bad_timeout:
			return "Bad timeout number";
		case nano::error_rpc::block_create_balance_mismatch:
			return "Balance mismatch for previous block";
		case nano::error_rpc::block_create_key_required:
			return "Private key or local wallet and account required";
		case nano::error_rpc::block_create_public_key_mismatch:
			return "Incorrect key for given account";
		case nano::error_rpc::block_create_requirements_state:
			return "Previous, representative, final balance and link (source or destination) are required";
		case nano::error_rpc::block_create_requirements_open:
			return "Representative account and source hash required";
		case nano::error_rpc::block_create_requirements_receive:
			return "Previous hash and source hash required";
		case nano::error_rpc::block_create_requirements_change:
			return "Representative account and previous hash required";
		case nano::error_rpc::block_create_requirements_send:
			return "Destination account, previous hash, current balance and amount required";
		case nano::error_rpc::confirmation_height_not_processing:
			return "There are no blocks currently being processed for adding confirmation height";
		case nano::error_rpc::confirmation_not_found:
			return "Active confirmation not found";
		case nano::error_rpc::difficulty_limit:
			return "Difficulty above config limit or below publish threshold";
		case nano::error_rpc::disabled_bootstrap_lazy:
			return "Lazy bootstrap is disabled";
		case nano::error_rpc::disabled_bootstrap_legacy:
			return "Legacy bootstrap is disabled";
		case nano::error_rpc::invalid_balance:
			return "Invalid balance number";
		case nano::error_rpc::invalid_destinations:
			return "Invalid destinations number";
		case nano::error_rpc::invalid_epoch:
			return "Invalid epoch number";
		case nano::error_rpc::invalid_epoch_signer:
			return "Incorrect epoch signer";
		case nano::error_rpc::invalid_offset:
			return "Invalid offset";
		case nano::error_rpc::invalid_missing_type:
			return "Invalid or missing type argument";
		case nano::error_rpc::invalid_root:
			return "Invalid root hash";
		case nano::error_rpc::invalid_sources:
			return "Invalid sources number";
		case nano::error_rpc::invalid_subtype:
			return "Invalid block subtype";
		case nano::error_rpc::invalid_subtype_balance:
			return "Invalid block balance for given subtype";
		case nano::error_rpc::invalid_subtype_epoch_link:
			return "Invalid epoch link";
		case nano::error_rpc::invalid_subtype_previous:
			return "Invalid previous block for given subtype";
		case nano::error_rpc::invalid_timestamp:
			return "Invalid timestamp";
		case nano::error_rpc::payment_account_balance:
			return "Account has non-zero balance";
		case nano::error_rpc::payment_unable_create_account:
			return "Unable to create transaction account";
		case nano::error_rpc::rpc_control_disabled:
			return "RPC control is disabled";
		case nano::error_rpc::sign_hash_disabled:
			return "Signing by block hash is disabled";
		case nano::error_rpc::source_not_found:
			return "Source not found";
	}

	return "Invalid error code";
}

std::string nano::error_process_messages::message (int ev) const
{
	switch (static_cast<nano::error_process> (ev))
	{
		case nano::error_process::generic:
			return "Unknown error";
		case nano::error_process::bad_signature:
			return "Bad signature";
		case nano::error_process::old:
			return "Old block";
		case nano::error_process::negative_spend:
			return "Negative spend";
		case nano::error_process::fork:
			return "Fork";
		case nano::error_process::unreceivable:
			return "Unreceivable";
		case nano::error_process::gap_previous:
			return "Gap previous block";
		case nano::error_process::gap_source:
			return "Gap source block";
		case nano::error_process::opened_burn_account:
			return "Burning account";
		case nano::error_process::balance_mismatch:
			return "Balance and amount delta do not match";
		case nano::error_process::block_position:
			return "This block cannot follow the previous block";
		case nano::error_process::other:
			return "Error processing block";
	}

	return "Invalid error code";
}

std::string nano::error_config_messages::message (int ev) const
{
	switch (static_cast<nano::error_config> (ev))
	{
		case nano::error_config::generic:
			return "Unknown error";
		case nano::error_config::invalid_value:
			return "Invalid configuration value";
		case nano::error_config::missing_value:
			return "Missing value in configuration";
		case nano::error_config::rocksdb_enabled_but_not_supported:
			return "RocksDB has been enabled, but the node has not been built with RocksDB support. Set the CMake flag -DNANO_ROCKSDB=ON";
	}

	return "Invalid error code";
}

const char * nano::error_conversion::detail::generic_category::name () const noexcept
{
	return boost::system::generic_category ().name ();
}
std::string nano::error_conversion::detail::generic_category::message (int value) const
{
	return boost::system::generic_category ().message (value);
}

const std::error_category & nano::error_conversion::generic_category ()
{
	static detail::generic_category instance;
	return instance;
}

std::error_code nano::error_conversion::convert (const boost::system::error_code & error)
{
	if (error.category () == boost::system::generic_category ())
	{
		return std::error_code (error.value (),
			nano::error_conversion::generic_category ());
	}
	assert (false);

	return nano::error_common::invalid_type_conversion;
}

nano::error::error (std::error_code code_a)
{
	code = code_a;
}

nano::error::error (boost::system::error_code const & code_a)
{
	code = std::make_error_code (static_cast<std::errc> (code_a.value ()));
}

nano::error::error (std::string message_a)
{
	code = nano::error_common::generic;
	message = std::move (message_a);
}

nano::error::error (std::exception const & exception_a)
{
	code = nano::error_common::exception;
	message = exception_a.what ();
}

nano::error & nano::error::operator= (nano::error const & err_a)
{
	code = err_a.code;
	message = err_a.message;
	return *this;
}

nano::error & nano::error::operator= (nano::error && err_a)
{
	code = err_a.code;
	message = std::move (err_a.message);
	return *this;
}

/** Assign error code */
nano::error & nano::error::operator= (const std::error_code code_a)
{
	code = code_a;
	message.clear ();
	return *this;
}

/** Assign boost error code (as converted to std::error_code) */
nano::error & nano::error::operator= (const boost::system::error_code & code_a)
{
	code = nano::error_conversion::convert (code_a);
	message.clear ();
	return *this;
}

/** Assign boost error code (as converted to std::error_code) */
nano::error & nano::error::operator= (const boost::system::errc::errc_t & code_a)
{
	code = nano::error_conversion::convert (boost::system::errc::make_error_code (code_a));
	message.clear ();
	return *this;
}

/** Set the error to nano::error_common::generic and the error message to \p message_a */
nano::error & nano::error::operator= (const std::string message_a)
{
	code = nano::error_common::generic;
	message = std::move (message_a);
	return *this;
}

/** Sets the error to nano::error_common::exception and adopts the exception error message. */
nano::error & nano::error::operator= (std::exception const & exception_a)
{
	code = nano::error_common::exception;
	message = exception_a.what ();
	return *this;
}

/** Return true if this#error_code equals the parameter */
bool nano::error::operator== (const std::error_code code_a) const
{
	return code == code_a;
}

/** Return true if this#error_code equals the parameter */
bool nano::error::operator== (const boost::system::error_code code_a) const
{
	return code.value () == code_a.value ();
}

/** Call the function iff the current error is zero */
nano::error & nano::error::then (std::function<nano::error &()> next)
{
	return code ? *this : next ();
}

/** Implicit error_code conversion */
nano::error::operator std::error_code () const
{
	return code;
}

/** Implicit bool conversion; true if there's an error */
nano::error::operator bool () const
{
	return code.value () != 0;
}

/** Implicit string conversion; returns the error message or an empty string. */
nano::error::operator std::string () const
{
	return get_message ();
}

/**
	 * Get error message, or an empty string if there's no error. If a custom error message is set,
	 * that will be returned, otherwise the error_code#message() is returned.
	 */
std::string nano::error::get_message () const
{
	std::string res = message;
	if (code && res.empty ())
	{
		res = code.message ();
	}
	return res;
}

/** Set an error message, but only if the error code is already set */
nano::error & nano::error::on_error (std::string message_a)
{
	if (code)
	{
		message = std::move (message_a);
	}
	return *this;
}

/** Set an error message if the current error code matches \p code_a */
nano::error & nano::error::on_error (std::error_code code_a, std::string message_a)
{
	if (code == code_a)
	{
		message = std::move (message_a);
	}
	return *this;
}

/** Set an error message and an error code */
nano::error & nano::error::set (std::string message_a, std::error_code code_a)
{
	message = message_a;
	code = code_a;
	return *this;
}

/** Set a custom error message. If the error code is not set, it will be set to nano::error_common::generic. */
nano::error & nano::error::set_message (std::string message_a)
{
	if (!code)
	{
		code = nano::error_common::generic;
	}
	message = std::move (message_a);
	return *this;
}

/** Clear an errors */
nano::error & nano::error::clear ()
{
	code.clear ();
	message.clear ();
	return *this;
}

namespace std
{
std::error_code make_error_code (boost::system::errc::errc_t const & e)
{
	return std::error_code (static_cast<int> (e), ::nano::error_conversion::generic_category ());
}
}
