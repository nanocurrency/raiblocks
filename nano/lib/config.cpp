#include <nano/lib/config.hpp>

#include <boost/filesystem/path.hpp>
#include <boost/lexical_cast.hpp>

#include <valgrind/valgrind.h>

namespace
{
// useful for boost_lexical cast to allow conversion of hex strings
template <typename ElemT>
struct HexTo
{
	ElemT value;
	operator ElemT () const
	{
		return value;
	}
	friend std::istream & operator>> (std::istream & in, HexTo & out)
	{
		in >> std::hex >> out.value;
		return in;
	}
};
} // namespace

namespace nano
{
work_thresholds const network_constants::publish_full (
0xffffffc000000000,
0xfffffff800000000, // 8x higher than epoch_1
0xfffffe0000000000 // 8x lower than epoch_1
);

work_thresholds const network_constants::publish_beta (
0xfffff00000000000, // 64x lower than publish_full.epoch_1
0xfffff00000000000, // same as epoch_1
0xffffe00000000000 // 2x lower than epoch_1
);

work_thresholds const network_constants::publish_dev (
0xfe00000000000000, // Very low for tests
0xffc0000000000000, // 8x higher than epoch_1
0xf000000000000000 // 8x lower than epoch_1
);

work_thresholds const network_constants::publish_test ( //defaults to live network levels
get_env_threshold_or_default ("NANO_TEST_EPOCH_1", 0xffffffc000000000),
get_env_threshold_or_default ("NANO_TEST_EPOCH_2", 0xfffffff800000000), // 8x higher than epoch_1
get_env_threshold_or_default ("NANO_TEST_EPOCH_2_RECV", 0xfffffe0000000000) // 8x lower than epoch_1
);

const char * network_constants::active_network_err_msg = "Invalid network. Valid values are live, test, beta and dev.";

uint8_t get_major_node_version ()
{
	return boost::numeric_cast<uint8_t> (boost::lexical_cast<int> (NANO_MAJOR_VERSION_STRING));
}
uint8_t get_minor_node_version ()
{
	return boost::numeric_cast<uint8_t> (boost::lexical_cast<int> (NANO_MINOR_VERSION_STRING));
}
uint8_t get_patch_node_version ()
{
	return boost::numeric_cast<uint8_t> (boost::lexical_cast<int> (NANO_PATCH_VERSION_STRING));
}
uint8_t get_pre_release_node_version ()
{
	return boost::numeric_cast<uint8_t> (boost::lexical_cast<int> (NANO_PRE_RELEASE_VERSION_STRING));
}

std::string get_env_or_default (char const * variable_name, std::string default_value)
{
	auto value = getenv (variable_name);
	return value ? value : default_value;
}

uint64_t get_env_threshold_or_default (char const * variable_name, uint64_t const default_value)
{
	auto * value = getenv (variable_name);
	return value ? boost::lexical_cast<HexTo<uint64_t>> (value) : default_value;
}

uint16_t test_node_port ()
{
	auto test_env = nano::get_env_or_default ("NANO_TEST_NODE_PORT", "17075");
	return boost::lexical_cast<uint16_t> (test_env);
}
uint16_t test_rpc_port ()
{
	auto test_env = nano::get_env_or_default ("NANO_TEST_RPC_PORT", "17076");
	return boost::lexical_cast<uint16_t> (test_env);
}
uint16_t test_ipc_port ()
{
	auto test_env = nano::get_env_or_default ("NANO_TEST_IPC_PORT", "17077");
	return boost::lexical_cast<uint16_t> (test_env);
}
uint16_t test_websocket_port ()
{
	auto test_env = nano::get_env_or_default ("NANO_TEST_WEBSOCKET_PORT", "17078");
	return boost::lexical_cast<uint16_t> (test_env);
}

std::array<uint8_t, 2> test_magic_number ()
{
	auto test_env = get_env_or_default ("NANO_TEST_MAGIC_NUMBER", "RX");
	std::array<uint8_t, 2> ret;
	std::copy (test_env.begin (), test_env.end (), ret.data ());
	return ret;
}

void force_nano_dev_network ()
{
	nano::network_constants::set_active_network (nano::nano_networks::nano_dev_network);
}

bool running_within_valgrind ()
{
	return (RUNNING_ON_VALGRIND > 0);
}

std::string get_config_path (boost::filesystem::path const & data_path)
{
	return (data_path / "config.json").string ();
}

std::string get_rpc_config_path (boost::filesystem::path const & data_path)
{
	return (data_path / "rpc_config.json").string ();
}

std::string get_node_toml_config_path (boost::filesystem::path const & data_path)
{
	return (data_path / "config-node.toml").string ();
}

std::string get_rpc_toml_config_path (boost::filesystem::path const & data_path)
{
	return (data_path / "config-rpc.toml").string ();
}

std::string get_qtwallet_toml_config_path (boost::filesystem::path const & data_path)
{
	return (data_path / "config-qtwallet.toml").string ();
}

std::string get_access_toml_config_path (boost::filesystem::path const & data_path)
{
	return (data_path / "config-access.toml").string ();
}

std::string get_tls_toml_config_path (boost::filesystem::path const & data_path)
{
	return (data_path / "config-tls.toml").string ();
}
} // namespace nano
