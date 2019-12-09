#pragma once

#include <nano/lib/errors.hpp>

#include <boost/lexical_cast.hpp>
#include <boost/type_traits.hpp>

#include <istream>
#include <string>
#include <type_traits>

namespace nano
{
/** Type trait to determine if T is compatible with boost's lexical_cast */
template <class T>
struct is_lexical_castable : std::integral_constant<bool,
								 (std::is_default_constructible<T>::value && (boost::has_right_shift<std::basic_istream<wchar_t>, T>::value || boost::has_right_shift<std::basic_istream<char>, T>::value))>
{
};

/* Type descriptions are used to automatically construct configuration error messages */
// clang-format off
template <typename T> inline std::string type_desc (void) { return "an unknown type"; }
template <> inline std::string type_desc<int8_t> (void) { return "an integer between -128 and 127"; }
template <> inline std::string type_desc<uint8_t> (void) { return "an integer between 0 and 255"; }
template <> inline std::string type_desc<int16_t> (void) { return "an integer between -32768 and 32767"; }
template <> inline std::string type_desc<uint16_t> (void) { return "an integer between 0 and 65535"; }
template <> inline std::string type_desc<int32_t> (void) { return "a 32-bit signed integer"; }
template <> inline std::string type_desc<uint32_t> (void) { return "a 32-bit unsigned integer"; }
template <> inline std::string type_desc<int64_t> (void) { return "a 64-bit signed integer"; }
template <> inline std::string type_desc<uint64_t> (void) { return "a 64-bit unsigned integer"; }
template <> inline std::string type_desc<float> (void) { return "a single precision floating point number"; }
template <> inline std::string type_desc<double> (void) { return "a double precison floating point number"; }
template <> inline std::string type_desc<char> (void) { return "a character"; }
template <> inline std::string type_desc<std::string> (void) { return "a string"; }
template <> inline std::string type_desc<bool> (void) { return "a boolean"; }
// clang-format on

/** Base type for configuration wrappers */
class configbase : public nano::error_aware<>
{
public:
	configbase () = default;
	configbase (std::shared_ptr<nano::error> const & error_a)
		: error (error_a)
	{
	}

	/** Returns the current error */
	nano::error & get_error () override
	{
		return *error;
	}

	/** Turn on or off automatic error message generation */
	void set_auto_error_message (bool auto_a)
	{
		auto_error_message = auto_a;
	}

protected:
	template <typename T>
	void construct_error_message (bool optional, std::string const & key)
	{
		if (auto_error_message && *error)
		{
			if (optional)
			{
				error->set_message (key + " is not " + type_desc<T> ());
			}
			else
			{
				error->set_message (key + " is required and must be " + type_desc<T> ());
			}
		}
	}

	/** Set error if not already set. That is, first error remains until get_error().clear() is called. */
	template <typename T, typename V>
	void conditionally_set_error (V error_a, bool optional, std::string const & key)
	{
		if (!*error)
		{
			*error = error_a;
			construct_error_message<T> (optional, key);
		}
	}

	/** We're a nano::error_aware type. Child nodes share the error state. */
	std::shared_ptr<nano::error> error;

	/** If set, automatically construct error messages based on parameters and type information. */
	bool auto_error_message{ true };
};
}
