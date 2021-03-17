
#pragma once

#include <nano/lib/numbers.hpp>

#include <crypto/cryptopp/seckey.h>
#include <crypto/cryptopp/siphash.h>

#include <mutex>

namespace nano
{
/**
 * A probabilistic duplicate filter based on directed map caches, using SipHash 2/4/128
 * The probability of false negatives (unique packet marked as duplicate) is the probability of a 128-bit SipHash collision.
 * The probability of false positives (duplicate packet marked as unique) shrinks with a larger filter.
 * @note This class is thread-safe.
 */
class network_filter final
{
public:
	network_filter () = delete;
	explicit network_filter (size_t size_a);
	/**
	 * Reads \p count_a bytes starting from \p bytes_a and inserts the siphash digest in the filter.
	 * @param \p digest_a if given, will be set to the resulting siphash digest
	 * @warning will read out of bounds if [ \p bytes_a, \p bytes_a + \p count_a ] is not a valid range
	 * @return a boolean representing the previous existence of the hash in the filter.
	 **/
	bool apply (uint8_t const * bytes_a, size_t count_a, nano::uint128_t * digest_a = nullptr);

	/**
	 * Sets the corresponding element in the filter to zero, if it matches \p digest_a exactly.
	 **/
	void clear (nano::uint128_t const & digest_a);

	/**
	 * Clear many digests from the filter
	 **/
	void clear (std::vector<nano::uint128_t> const &);

	/**
	 * Reads \p count_a bytes starting from \p bytes_a and digests the contents.
	 * Then, sets the corresponding element in the filter to zero, if it matches the digest exactly.
	 * @warning will read out of bounds if [ \p bytes_a, \p bytes_a + \p count_a ] is not a valid range
	 **/
	void clear (uint8_t const * bytes_a, size_t count_a);

	/**
	 * Serializes \p object_a and clears the resulting siphash digest from the filter.
	 * @return a boolean representing the previous existence of the hash in the filter.
	 **/
	template <typename OBJECT>
	void clear (OBJECT const & object_a);

	/** Sets every element of the filter to zero, keeping its size and capacity. */
	void clear ();

	/**
	 * Serializes \p object_a and returns the resulting siphash digest
	 */
	template <typename OBJECT>
	nano::uint128_t hash (OBJECT const & object_a) const;

private:
	using siphash_t = CryptoPP::SipHash<2, 4, true>;

	/**
	 * Get element from digest.
	 * @note must have a lock on mutex
	 * @return a reference to the element with key \p hash_a
	 **/
	nano::uint128_t & get_element (nano::uint128_t const & hash_a);

	/**
	 * Hashes \p count_a bytes starting from \p bytes_a .
	 * @return the siphash digest of the contents in \p bytes_a .
	 **/
	nano::uint128_t hash (uint8_t const * bytes_a, size_t count_a) const;

	std::vector<nano::uint128_t> items;
	CryptoPP::SecByteBlock key{ siphash_t::KEYLENGTH };
	nano::mutex mutex{ mutex_identifier (mutexes::network_filter) };
};
}
