#include <nano/lib/cli.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

#include <sstream>

std::vector<std::string> nano::config_overrides (std::vector<config_key_value_pair> const & key_value_pairs_a)
{
	std::vector<std::string> overrides;
	auto format (boost::format ("%1%=%2%"));
	auto format_add_escaped_quotes (boost::format ("%1%=\"%2%\""));
	for (auto const & pair : key_value_pairs_a)
	{
		auto const start = pair.value.find ('[');

		std::string value;
		auto const is_array = (start != std::string::npos);
		if (is_array)
		{
			// Trim off the square brackets [] of the array
			auto const end = pair.value.find (']');
			auto const array_values = pair.value.substr (start + 1, end - start - 1);

			// Split the string by comma
			std::vector<std::string> split_elements;
			boost::split (split_elements, array_values, boost::is_any_of (","));

			auto format (boost::format ("%1%"));
			auto format_add_escaped_quotes (boost::format ("\"%1%\""));

			// Rebuild the array string adding escaped quotes if necessary
			std::ostringstream ss;
			ss << "[";
			for (auto i = 0; i < split_elements.size (); ++i)
			{
				auto & elem = split_elements[i];
				auto already_escaped = elem.find ('\"') != std::string::npos;
				ss << ((!already_escaped ? format_add_escaped_quotes : format) % elem).str ();
				if (i != split_elements.size () - 1)
				{
					ss << ",";
				}
			}
			ss << "]";
			value = ss.str ();
		}
		else
		{
			value = pair.value;
		}
		auto already_escaped = value.find ('\"') != std::string::npos;
		overrides.push_back (((!already_escaped ? format_add_escaped_quotes : format) % pair.key % value).str ());
	}
	return overrides;
}

std::istream & nano::operator>> (std::istream & is, nano::config_key_value_pair & into)
{
	char ch;
	while (is >> ch && ch != '=')
	{
		into.key += ch;
	}
	return is >> into.value;
}
