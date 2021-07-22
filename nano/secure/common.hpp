#pragma once

#include <nano/crypto/blake2/blake2.h>
#include <nano/lib/blockbuilders.hpp>
#include <nano/lib/blocks.hpp>
#include <nano/lib/config.hpp>
#include <nano/lib/epoch.hpp>
#include <nano/lib/numbers.hpp>
#include <nano/lib/rep_weights.hpp>
#include <nano/lib/utility.hpp>

#include <boost/iterator/transform_iterator.hpp>
#include <boost/optional/optional.hpp>
#include <boost/property_tree/ptree_fwd.hpp>
#include <boost/variant/variant.hpp>

#include <unordered_map>

namespace boost
{
template <>
struct hash<::nano::uint256_union>
{
	size_t operator() (::nano::uint256_union const & value_a) const
	{
		return std::hash<::nano::uint256_union> () (value_a);
	}
};

template <>
struct hash<::nano::block_hash>
{
	size_t operator() (::nano::block_hash const & value_a) const
	{
		return std::hash<::nano::block_hash> () (value_a);
	}
};

template <>
struct hash<::nano::public_key>
{
	size_t operator() (::nano::public_key const & value_a) const
	{
		return std::hash<::nano::public_key> () (value_a);
	}
};
template <>
struct hash<::nano::uint512_union>
{
	size_t operator() (::nano::uint512_union const & value_a) const
	{
		return std::hash<::nano::uint512_union> () (value_a);
	}
};
template <>
struct hash<::nano::qualified_root>
{
	size_t operator() (::nano::qualified_root const & value_a) const
	{
		return std::hash<::nano::qualified_root> () (value_a);
	}
};
template <>
struct hash<::nano::root>
{
	size_t operator() (::nano::root const & value_a) const
	{
		return std::hash<::nano::root> () (value_a);
	}
};
}
namespace nano
{
/**
 * A key pair. The private key is generated from the random pool, or passed in
 * as a hex string. The public key is derived using ed25519.
 */
class keypair
{
public:
	keypair ();
	keypair (std::string const &);
	keypair (nano::raw_key &&);
	nano::public_key pub;
	nano::raw_key prv;
};

/**
 * Latest information about an account
 */
class account_info final
{
public:
	account_info () = default;
	account_info (nano::block_hash const &, nano::account const &, nano::block_hash const &, nano::amount const &, uint64_t, uint64_t, epoch);
	bool deserialize (nano::stream &);
	bool operator== (nano::account_info const &) const;
	bool operator!= (nano::account_info const &) const;
	size_t db_size () const;
	nano::epoch epoch () const;
	nano::block_hash head{ 0 };
	nano::account representative{ 0 };
	nano::block_hash open_block{ 0 };
	nano::amount balance{ 0 };
	/** Seconds since posix epoch */
	uint64_t modified{ 0 };
	uint64_t block_count{ 0 };
	nano::epoch epoch_m{ nano::epoch::epoch_0 };
};

/**
 * Information on an uncollected send
 */
class pending_info final
{
public:
	pending_info () = default;
	pending_info (nano::account const &, nano::amount const &, nano::epoch);
	size_t db_size () const;
	bool deserialize (nano::stream &);
	bool operator== (nano::pending_info const &) const;
	nano::account source{ 0 };
	nano::amount amount{ 0 };
	nano::epoch epoch{ nano::epoch::epoch_0 };
};
class pending_key final
{
public:
	pending_key () = default;
	pending_key (nano::account const &, nano::block_hash const &);
	bool deserialize (nano::stream &);
	bool operator== (nano::pending_key const &) const;
	nano::account const & key () const;
	nano::account account{ 0 };
	nano::block_hash hash{ 0 };
};

class endpoint_key final
{
public:
	endpoint_key () = default;

	/*
	 * @param address_a This should be in network byte order
	 * @param port_a This should be in host byte order
	 */
	endpoint_key (const std::array<uint8_t, 16> & address_a, uint16_t port_a);

	/*
	 * @return The ipv6 address in network byte order
	 */
	const std::array<uint8_t, 16> & address_bytes () const;

	/*
	 * @return The port in host byte order
	 */
	uint16_t port () const;

private:
	// Both stored internally in network byte order
	std::array<uint8_t, 16> address;
	uint16_t network_port{ 0 };
};

enum class no_value
{
	dummy
};

class unchecked_key final
{
public:
	unchecked_key () = default;
	unchecked_key (nano::hash_or_account const &, nano::block_hash const &);
	unchecked_key (nano::uint512_union const &);
	bool deserialize (nano::stream &);
	bool operator== (nano::unchecked_key const &) const;
	nano::block_hash const & key () const;
	nano::block_hash previous{ 0 };
	nano::block_hash hash{ 0 };
};

/**
 * Tag for block signature verification result
 */
enum class signature_verification : uint8_t
{
	unknown = 0,
	invalid = 1,
	valid = 2,
	valid_epoch = 3 // Valid for epoch blocks
};

/**
 * Information on an unchecked block
 */
class unchecked_info final
{
public:
	unchecked_info () = default;
	unchecked_info (std::shared_ptr<nano::block> const &, nano::account const &, uint64_t, nano::signature_verification = nano::signature_verification::unknown, bool = false);
	void serialize (nano::stream &) const;
	bool deserialize (nano::stream &);
	std::shared_ptr<nano::block> block;
	nano::account account{ 0 };
	/** Seconds since posix epoch */
	uint64_t modified{ 0 };
	nano::signature_verification verified{ nano::signature_verification::unknown };
	bool confirmed{ false };
};

class block_info final
{
public:
	block_info () = default;
	block_info (nano::account const &, nano::amount const &);
	nano::account account{ 0 };
	nano::amount balance{ 0 };
};

class confirmation_height_info final
{
public:
	confirmation_height_info () = default;
	confirmation_height_info (uint64_t, nano::block_hash const &);
	void serialize (nano::stream &) const;
	bool deserialize (nano::stream &);
	uint64_t height;
	nano::block_hash frontier;
};

namespace confirmation_height
{
	/** When the uncemented count (block count - cemented count) is less than this use the unbounded processor */
	uint64_t const unbounded_cutoff{ 16384 };
}

using vote_blocks_vec_iter = std::vector<boost::variant<std::shared_ptr<nano::block>, nano::block_hash>>::const_iterator;
class iterate_vote_blocks_as_hash final
{
public:
	iterate_vote_blocks_as_hash () = default;
	nano::block_hash operator() (boost::variant<std::shared_ptr<nano::block>, nano::block_hash> const & item) const;
};
class vote final
{
public:
	vote () = default;
	vote (nano::vote const &);
	vote (bool &, nano::stream &, nano::block_uniquer * = nullptr);
	vote (bool &, nano::stream &, nano::block_type, nano::block_uniquer * = nullptr);
	vote (nano::account const &, nano::raw_key const &, uint64_t, std::shared_ptr<nano::block> const &);
	vote (nano::account const &, nano::raw_key const &, uint64_t, std::vector<nano::block_hash> const &);
	std::string hashes_string () const;
	nano::block_hash hash () const;
	nano::block_hash full_hash () const;
	bool operator== (nano::vote const &) const;
	bool operator!= (nano::vote const &) const;
	void serialize (nano::stream &, nano::block_type) const;
	void serialize (nano::stream &) const;
	void serialize_json (boost::property_tree::ptree & tree) const;
	bool deserialize (nano::stream &, nano::block_uniquer * = nullptr);
	bool validate () const;
	boost::transform_iterator<nano::iterate_vote_blocks_as_hash, nano::vote_blocks_vec_iter> begin () const;
	boost::transform_iterator<nano::iterate_vote_blocks_as_hash, nano::vote_blocks_vec_iter> end () const;
	std::string to_json () const;
	// Vote timestamp
	uint64_t timestamp;
	// The blocks, or block hashes, that this vote is for
	std::vector<boost::variant<std::shared_ptr<nano::block>, nano::block_hash>> blocks;
	// Account that's voting
	nano::account account;
	// Signature of timestamp + block hashes
	nano::signature signature;
	static const std::string hash_prefix;
};
/**
 * This class serves to find and return unique variants of a vote in order to minimize memory usage
 */
class vote_uniquer final
{
public:
	using value_type = std::pair<const nano::block_hash, std::weak_ptr<nano::vote>>;

	vote_uniquer (nano::block_uniquer &);
	std::shared_ptr<nano::vote> unique (std::shared_ptr<nano::vote> const &);
	size_t size ();

private:
	nano::block_uniquer & uniquer;
	nano::mutex mutex{ mutex_identifier (mutexes::vote_uniquer) };
	std::unordered_map<std::remove_const_t<value_type::first_type>, value_type::second_type> votes;
	static unsigned constexpr cleanup_count = 2;
};

std::unique_ptr<container_info_component> collect_container_info (vote_uniquer & vote_uniquer, std::string const & name);

enum class vote_code
{
	invalid, // Vote is not signed correctly
	replay, // Vote does not have the highest timestamp, it's a replay
	vote, // Vote has the highest timestamp
	indeterminate // Unknown if replay or vote
};

enum class process_result
{
	progress, // Hasn't been seen before, signed correctly
	bad_signature, // Signature was bad, forged or transmission error
	old, // Already seen and was valid
	negative_spend, // Malicious attempt to spend a negative amount
	fork, // Malicious fork based on previous
	unreceivable, // Source block doesn't exist, has already been received, or requires an account upgrade (epoch blocks)
	gap_previous, // Block marked as previous is unknown
	gap_source, // Block marked as source is unknown
	gap_epoch_open_pending, // Block marked as pending blocks required for epoch open block are unknown
	opened_burn_account, // The impossible happened, someone found the private key associated with the public key '0'.
	balance_mismatch, // Balance and amount delta don't match
	representative_mismatch, // Representative is changed when it is not allowed
	block_position, // This block cannot follow the previous block
	insufficient_work // Insufficient work for this block, even though it passed the minimal validation
};
class process_return final
{
public:
	nano::process_result code;
	nano::signature_verification verified;
	nano::amount previous_balance;
};
enum class tally_result
{
	vote,
	changed,
	confirm
};

class network_params;

/** Protocol versions whose value may depend on the active network */
class protocol_constants
{
public:
	/** Current protocol version */
	uint8_t const protocol_version = 0x12;

	/** Minimum accepted protocol version */
	uint8_t protocol_version_min () const;

private:
	/* Minimum protocol version we will establish connections to */
	uint8_t const protocol_version_min_m = 0x12;
};

// Some places use the decltype of protocol_version instead of protocol_version_min. To keep those checks simpler we check that the decltypes match ignoring differences in const
static_assert (std::is_same<std::remove_const_t<decltype (protocol_constants ().protocol_version)>, decltype (protocol_constants ().protocol_version_min ())>::value, "protocol_min should match");

/** Genesis keys and ledger constants for network variants */
class ledger_constants
{
public:
	ledger_constants (nano::network_constants & network_constants);
	ledger_constants (nano::networks network_a);
	nano::account genesis_account () const;
	nano::block_hash genesis_hash () const;
	nano::keypair zero_key;
	nano::account nano_beta_account;
	nano::account nano_live_account;
	nano::account nano_test_account;
	std::shared_ptr<nano::block> nano_dev_genesis;
	std::shared_ptr<nano::block> nano_beta_genesis;
	std::shared_ptr<nano::block> nano_live_genesis;
	std::shared_ptr<nano::block> nano_test_genesis;
	std::shared_ptr<nano::block> genesis;
	nano::account burn_account;
	nano::account nano_dev_final_votes_canary_account;
	nano::account nano_beta_final_votes_canary_account;
	nano::account nano_live_final_votes_canary_account;
	nano::account nano_test_final_votes_canary_account;
	nano::account final_votes_canary_account;
	uint64_t nano_dev_final_votes_canary_height;
	uint64_t nano_beta_final_votes_canary_height;
	uint64_t nano_live_final_votes_canary_height;
	uint64_t nano_test_final_votes_canary_height;
	uint64_t final_votes_canary_height;
	nano::epochs epochs;
};

namespace dev
{
	extern nano::ledger_constants constants;
	extern std::shared_ptr<nano::block> & genesis;
	extern nano::keypair genesis_key;
	extern nano::uint128_t genesis_amount;
}

/** Constants which depend on random values (this class should never be used globally due to CryptoPP globals potentially not being initialized) */
class random_constants
{
public:
	random_constants ();
	nano::account not_an_account;
	nano::uint128_union random_128;
};

/** Node related constants whose value depends on the active network */
class node_constants
{
public:
	node_constants (nano::network_constants & network_constants);
	std::chrono::seconds period;
	std::chrono::milliseconds half_period;
	/** Default maximum idle time for a socket before it's automatically closed */
	std::chrono::seconds idle_timeout;
	std::chrono::seconds cutoff;
	std::chrono::seconds syn_cookie_cutoff;
	std::chrono::minutes backup_interval;
	std::chrono::seconds bootstrap_interval;
	std::chrono::seconds search_pending_interval;
	std::chrono::seconds peer_interval;
	std::chrono::minutes unchecked_cleaning_interval;
	std::chrono::milliseconds process_confirmed_interval;
	/** Maximum number of peers per IP */
	size_t max_peers_per_ip;
	/** Maximum number of peers per subnetwork */
	size_t max_peers_per_subnetwork;

	/** The maximum amount of samples for a 2 week period on live or 1 day on beta */
	uint64_t max_weight_samples;
	uint64_t weight_period;
};

/** Voting related constants whose value depends on the active network */
class voting_constants
{
public:
	voting_constants (nano::network_constants & network_constants);
	size_t const max_cache;
	std::chrono::seconds const delay;
};

/** Port-mapping related constants whose value depends on the active network */
class portmapping_constants
{
public:
	portmapping_constants (nano::network_constants & network_constants);
	// Timeouts are primes so they infrequently happen at the same time
	std::chrono::seconds lease_duration;
	std::chrono::seconds health_check_period;
};

/** Bootstrap related constants whose value depends on the active network */
class bootstrap_constants
{
public:
	bootstrap_constants (nano::network_constants & network_constants);
	uint32_t lazy_max_pull_blocks;
	uint32_t lazy_min_pull_blocks;
	unsigned frontier_retry_limit;
	unsigned lazy_retry_limit;
	unsigned lazy_destinations_retry_limit;
	std::chrono::milliseconds gap_cache_bootstrap_start_interval;
	uint32_t default_frontiers_age_seconds;
};

/** Constants whose value depends on the active network */
class network_params
{
public:
	/** Populate values based on the current active network */
	network_params ();

	/** Populate values based on \p network_a */
	network_params (nano::networks network_a);

	unsigned kdf_work;
	network_constants network;
	protocol_constants protocol;
	ledger_constants ledger;
	random_constants random;
	voting_constants voting;
	node_constants node;
	portmapping_constants portmapping;
	bootstrap_constants bootstrap;
};

enum class confirmation_height_mode
{
	automatic,
	unbounded,
	bounded
};

/* Holds flags for various cacheable data. For most CLI operations caching is unnecessary
 * (e.g getting the cemented block count) so it can be disabled for performance reasons. */
class generate_cache
{
public:
	bool reps = true;
	bool cemented_count = true;
	bool unchecked_count = true;
	bool account_count = true;
	bool block_count = true;

	void enable_all ();
};

/* Holds an in-memory cache of various counts */
class ledger_cache
{
public:
	nano::rep_weights rep_weights;
	std::atomic<uint64_t> cemented_count{ 0 };
	std::atomic<uint64_t> block_count{ 0 };
	std::atomic<uint64_t> pruned_count{ 0 };
	std::atomic<uint64_t> account_count{ 0 };
	std::atomic<bool> final_votes_confirmation_canary{ false };
};

/* Defines the possible states for an election to stop in */
enum class election_status_type : uint8_t
{
	ongoing = 0,
	active_confirmed_quorum = 1,
	active_confirmation_height = 2,
	inactive_confirmation_height = 3,
	stopped = 5
};

/* Holds a summary of an election */
class election_status final
{
public:
	std::shared_ptr<nano::block> winner;
	nano::amount tally;
	nano::amount final_tally;
	std::chrono::milliseconds election_end;
	std::chrono::milliseconds election_duration;
	unsigned confirmation_request_count;
	unsigned block_count;
	unsigned voter_count;
	election_status_type type;
};

nano::wallet_id random_wallet_id ();
}
