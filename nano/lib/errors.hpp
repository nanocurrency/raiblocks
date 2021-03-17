#pragma once

#include <boost/filesystem/operations.hpp>
#include <boost/system/error_code.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <type_traits>

namespace nano
{
/** Common error codes */
enum class error_common
{
	generic = 1,
	exception,
	access_denied,
	account_not_found,
	account_not_found_wallet,
	account_exists,
	bad_account_number,
	bad_balance,
	bad_link,
	bad_previous,
	bad_representative_number,
	bad_source,
	bad_signature,
	bad_private_key,
	bad_public_key,
	bad_seed,
	bad_threshold,
	bad_wallet_number,
	bad_work_format,
	disabled_local_work_generation,
	disabled_work_generation,
	failure_work_generation,
	missing_account,
	missing_balance,
	missing_link,
	missing_previous,
	missing_representative,
	missing_signature,
	missing_work,
	invalid_amount,
	invalid_amount_big,
	invalid_count,
	invalid_index,
	invalid_ip_address,
	invalid_port,
	invalid_type_conversion,
	invalid_work,
	insufficient_balance,
	is_not_state_block,
	numeric_conversion,
	tracking_not_enabled,
	wallet_lmdb_max_dbs,
	wallet_locked,
	wallet_not_found
};

/** Block related errors */
enum class error_blocks
{
	generic = 1,
	bad_hash_number,
	invalid_block,
	invalid_block_hash,
	invalid_type,
	not_found,
	work_low
};

/** RPC related errors */
enum class error_rpc
{
	generic = 1,
	empty_response,
	bad_destination,
	bad_difficulty_format,
	bad_key,
	bad_link,
	bad_multiplier_format,
	bad_previous,
	bad_representative_number,
	bad_source,
	bad_timeout,
	bad_work_version,
	block_create_balance_mismatch,
	block_create_key_required,
	block_create_public_key_mismatch,
	block_create_requirements_state,
	block_create_requirements_open,
	block_create_requirements_receive,
	block_create_requirements_change,
	block_create_requirements_send,
	block_root_mismatch,
	block_work_enough,
	block_work_version_mismatch,
	confirmation_height_not_processing,
	confirmation_not_found,
	difficulty_limit,
	disabled_bootstrap_lazy,
	disabled_bootstrap_legacy,
	invalid_balance,
	invalid_destinations,
	invalid_epoch,
	invalid_epoch_signer,
	invalid_offset,
	invalid_missing_type,
	invalid_root,
	invalid_sources,
	invalid_subtype,
	invalid_subtype_balance,
	invalid_subtype_epoch_link,
	invalid_subtype_previous,
	invalid_timestamp,
	invalid_threads_count,
	peer_not_found,
	pruning_disabled,
	requires_port_and_address,
	rpc_control_disabled,
	sign_hash_disabled,
	source_not_found
};

/** process_result related errors */
enum class error_process
{
	generic = 1,
	bad_signature, // Signature was bad, forged or transmission error
	old, // Already seen and was valid
	negative_spend, // Malicious attempt to spend a negative amount
	fork, // Malicious fork based on previous
	unreceivable, // Source block doesn't exist or has already been received
	gap_previous, // Block marked as previous is unknown
	gap_source, // Block marked as source is unknown
	opened_burn_account, // The impossible happened, someone found the private key associated with the public key '0'.
	balance_mismatch, // Balance and amount delta don't match
	block_position, // This block cannot follow the previous block
	insufficient_work, // Insufficient work for this block, even though it passed the minimal validation
	other
};

/** config.json deserialization related errors */
enum class error_config
{
	generic = 1,
	invalid_value,
	missing_value
};
} // nano namespace

// Convenience macro to implement the standard boilerplate for using std::error_code with enums
// Use this at the end of any header defining one or more error code enums.
#define REGISTER_ERROR_CODES(namespace_name, enum_type)                                                        \
	namespace namespace_name                                                                                   \
	{                                                                                                          \
		static_assert (static_cast<int> (enum_type::generic) > 0, "The first error enum must be generic = 1"); \
		class enum_type##_messages : public std::error_category                                                \
		{                                                                                                      \
		public:                                                                                                \
			char const * name () const noexcept override                                                       \
			{                                                                                                  \
				return #enum_type;                                                                             \
			}                                                                                                  \
                                                                                                               \
			std::string message (int ev) const override;                                                       \
		};                                                                                                     \
                                                                                                               \
		inline const std::error_category & enum_type##_category ()                                             \
		{                                                                                                      \
			static enum_type##_messages instance;                                                              \
			return instance;                                                                                   \
		}                                                                                                      \
                                                                                                               \
		inline std::error_code make_error_code (::namespace_name::enum_type err)                               \
		{                                                                                                      \
			return { static_cast<int> (err), enum_type##_category () };                                        \
		}                                                                                                      \
	}                                                                                                          \
	namespace std                                                                                              \
	{                                                                                                          \
		template <>                                                                                            \
		struct is_error_code_enum<::namespace_name::enum_type> : std::true_type                                \
		{                                                                                                      \
		};                                                                                                     \
	}

REGISTER_ERROR_CODES (nano, error_common);
REGISTER_ERROR_CODES (nano, error_blocks);
REGISTER_ERROR_CODES (nano, error_rpc);
REGISTER_ERROR_CODES (nano, error_process);
REGISTER_ERROR_CODES (nano, error_config);

/* boost->std error_code bridge */
namespace nano::error_conversion
{
std::error_category const & generic_category ();
}

namespace std
{
template <>
struct is_error_code_enum<boost::system::errc::errc_t>
: public std::true_type
{
};

std::error_code make_error_code (boost::system::errc::errc_t const & e);
}
namespace nano::error_conversion
{
namespace detail
{
	class generic_category : public std::error_category
	{
	public:
		char const * name () const noexcept override;
		std::string message (int value) const override;
	};
}
std::error_category const & generic_category ();
std::error_code convert (const boost::system::error_code & error);
}

namespace nano
{
/** Adapter for std/boost::error_code, std::exception and bool flags to facilitate unified error handling */
class error
{
public:
	error () = default;
	error (nano::error const & error_a) = default;
	error (nano::error && error_a) = default;

	explicit error (std::error_code code_a);
	explicit error (boost::system::error_code const & code_a);
	explicit error (std::string message_a);
	explicit error (std::exception const & exception_a);
	error & operator= (nano::error const & err_a);
	error & operator= (nano::error && err_a);
	error & operator= (std::error_code code_a);
	error & operator= (const boost::system::error_code & code_a);
	error & operator= (const boost::system::errc::errc_t & code_a);
	error & operator= (std::string message_a);
	error & operator= (std::exception const & exception_a);
	bool operator== (std::error_code code_a) const;
	bool operator== (boost::system::error_code code_a) const;
	error & then (std::function<nano::error &()> next);
	template <typename... ErrorCode>
	error & accept (ErrorCode... err)
	{
		// Convert variadic arguments to std::error_code
		auto codes = { std::error_code (err)... };
		if (std::any_of (codes.begin (), codes.end (), [this, &codes](auto & code_a) { return code == code_a; }))
		{
			code.clear ();
		}

		return *this;
	}

	explicit operator std::error_code () const;
	explicit operator bool () const;
	explicit operator std::string () const;
	std::string get_message () const;
	/**
	 * The error code as an integer. Note that some error codes have platform dependent values.
	 * A return value of 0 signifies there is no error.
	 */
	int error_code_as_int () const;
	error & on_error (std::string message_a);
	error & on_error (std::error_code code_a, std::string message_a);
	error & set (std::string message_a, std::error_code code_a = nano::error_common::generic);
	error & set_message (std::string message_a);
	error & clear ();

private:
	std::error_code code;
	std::string message;
};

/**
 * A type that manages a nano::error.
 * The default return type is nano::error&, though shared_ptr<nano::error> is a good option in cases
 * where shared error state is desirable.
 */
template <typename RET_TYPE = nano::error &>
class error_aware
{
	static_assert (std::is_same<RET_TYPE, nano::error &>::value || std::is_same<RET_TYPE, std::shared_ptr<nano::error>>::value, "Must be nano::error& or shared_ptr<nano::error>");

public:
	/** Returns the error object managed by this object */
	virtual RET_TYPE get_error () = 0;
};
}
