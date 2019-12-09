#include <nano/boost/asio/ip/address_v6.hpp>
#include <nano/lib/jsonconfig.hpp>

#include <boost/filesystem/convenience.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <cstddef>

nano::jsonconfig::jsonconfig ()
	: tree (tree_default)
{
	error = std::make_shared<nano::error> ();
}

nano::jsonconfig::jsonconfig (boost::property_tree::ptree & tree_a, std::shared_ptr<nano::error> error_a)
	: nano::configbase (error_a)
	, tree (tree_a)
{
	if (!error)
	{
		error = std::make_shared<nano::error> ();
	}
}

/**
 * Reads a json object from the stream 
 * @return nano::error&, including a descriptive error message if the config file is malformed.
 */
nano::error & nano::jsonconfig::read (boost::filesystem::path const & path_a)
{
	std::fstream stream;
	open_or_create (stream, path_a.string ());
	if (!stream.fail ())
	{
		try
		{
			boost::property_tree::read_json (stream, tree);
		}
		catch (std::runtime_error const & ex)
		{
			auto pos (stream.tellg ());
			if (pos != std::streampos (0))
			{
				*error = ex;
			}
		}
		stream.close ();
	}
	return *error;
}

void nano::jsonconfig::write (boost::filesystem::path const & path_a)
{
	std::fstream stream;
	open_or_create (stream, path_a.string ());
	write (stream);
}

void nano::jsonconfig::write (std::ostream & stream_a) const
{
	boost::property_tree::write_json (stream_a, tree);
}

void nano::jsonconfig::read (std::istream & stream_a)
{
	boost::property_tree::read_json (stream_a, tree);
}

/** Open configuration file, create if necessary */
void nano::jsonconfig::open_or_create (std::fstream & stream_a, std::string const & path_a)
{
	if (!boost::filesystem::exists (path_a))
	{
		// Create temp stream to first create the file
		std::ofstream stream (path_a);

		// Set permissions before opening otherwise Windows only has read permissions
		nano::set_secure_perm_file (path_a);
	}

	stream_a.open (path_a);
}

/** Takes a filepath, appends '_backup_<timestamp>' to the end (but before any extension) and saves that file in the same directory */
void nano::jsonconfig::create_backup_file (boost::filesystem::path const & filepath_a)
{
	auto extension = filepath_a.extension ();
	auto filename_without_extension = filepath_a.filename ().replace_extension ("");
	auto orig_filepath = filepath_a;
	auto & backup_path = orig_filepath.remove_filename ();
	auto backup_filename = filename_without_extension;
	backup_filename += "_backup_";
	backup_filename += std::to_string (std::chrono::system_clock::now ().time_since_epoch ().count ());
	backup_filename += extension;
	auto backup_filepath = backup_path / backup_filename;

	boost::filesystem::copy_file (filepath_a, backup_filepath);
}

/** Returns the boost property node managed by this instance */
boost::property_tree::ptree const & nano::jsonconfig::get_tree ()
{
	return tree;
}

/** Returns true if the property tree node is empty */
bool nano::jsonconfig::empty () const
{
	return tree.empty ();
}

boost::optional<nano::jsonconfig> nano::jsonconfig::get_optional_child (std::string const & key_a)
{
	boost::optional<jsonconfig> child_config;
	auto child = tree.get_child_optional (key_a);
	if (child)
	{
		return jsonconfig (child.get (), error);
	}
	return child_config;
}

nano::jsonconfig nano::jsonconfig::get_required_child (std::string const & key_a)
{
	auto child = tree.get_child_optional (key_a);
	if (!child)
	{
		*error = nano::error_config::missing_value;
		error->set_message ("Missing configuration node: " + key_a);
	}
	return child ? jsonconfig (child.get (), error) : *this;
}

nano::jsonconfig & nano::jsonconfig::put_child (std::string const & key_a, nano::jsonconfig & conf_a)
{
	tree.add_child (key_a, conf_a.get_tree ());
	return *this;
}

nano::jsonconfig & nano::jsonconfig::replace_child (std::string const & key_a, nano::jsonconfig & conf_a)
{
	tree.erase (key_a);
	put_child (key_a, conf_a);
	return *this;
}

/** Returns true if \p key_a is present */
bool nano::jsonconfig::has_key (std::string const & key_a)
{
	return tree.find (key_a) != tree.not_found ();
}

/** Erase the property of given key */
nano::jsonconfig & nano::jsonconfig::erase (std::string const & key_a)
{
	tree.erase (key_a);
	return *this;
}

// boost's lexical cast doesn't handle (u)int8_t
nano::jsonconfig & nano::jsonconfig::get_config (bool optional, std::string key, uint8_t & target, uint8_t default_value)
{
	int64_t tmp;
	try
	{
		auto val (tree.get<std::string> (key));
		if (!boost::conversion::try_lexical_convert<int64_t> (val, tmp) || tmp < 0 || tmp > 255)
		{
			conditionally_set_error<uint8_t> (nano::error_config::invalid_value, optional, key);
		}
		else
		{
			target = static_cast<uint8_t> (tmp);
		}
	}
	catch (boost::property_tree::ptree_bad_path const &)
	{
		if (!optional)
		{
			conditionally_set_error<uint8_t> (nano::error_config::missing_value, optional, key);
		}
		else
		{
			target = default_value;
		}
	}
	catch (std::runtime_error & ex)
	{
		conditionally_set_error<uint8_t> (ex, optional, key);
	}
	return *this;
}

nano::jsonconfig & nano::jsonconfig::get_config (bool optional, std::string key, bool & target, bool default_value)
{
	auto bool_conv = [this, &target, &key, optional](std::string val) {
		if (val == "true")
		{
			target = true;
		}
		else if (val == "false")
		{
			target = false;
		}
		else if (!*error)
		{
			conditionally_set_error<bool> (nano::error_config::invalid_value, optional, key);
		}
	};
	try
	{
		auto val (tree.get<std::string> (key));
		bool_conv (val);
	}
	catch (boost::property_tree::ptree_bad_path const &)
	{
		if (!optional)
		{
			conditionally_set_error<bool> (nano::error_config::missing_value, optional, key);
		}
		else
		{
			target = default_value;
		}
	}
	catch (std::runtime_error & ex)
	{
		conditionally_set_error<bool> (ex, optional, key);
	}
	return *this;
}

nano::jsonconfig & nano::jsonconfig::get_config (bool optional, std::string key, boost::asio::ip::address_v6 & target, boost::asio::ip::address_v6 const & default_value)
{
	try
	{
		auto address_l (tree.get<std::string> (key));
		boost::system::error_code bec;
		target = boost::asio::ip::make_address_v6 (address_l, bec);
		if (bec)
		{
			conditionally_set_error<boost::asio::ip::address_v6> (nano::error_config::invalid_value, optional, key);
		}
	}
	catch (boost::property_tree::ptree_bad_path const &)
	{
		if (!optional)
		{
			conditionally_set_error<boost::asio::ip::address_v6> (nano::error_config::missing_value, optional, key);
		}
		else
		{
			target = default_value;
		}
	}
	return *this;
}

void nano::jsonconfig::write_json (std::fstream & stream)
{
	boost::property_tree::write_json (stream, tree);
}
