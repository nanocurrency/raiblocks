#pragma once

#include <nano/lib/alarm.hpp>
#include <nano/lib/work.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/utility.hpp>

#include <boost/asio/io_context.hpp>
#include <boost/program_options.hpp>

namespace nano
{
class node_flags;
/**
 * Resources that can be shared across node instances
 * *These are mostly useful in a testing context where more than one node may be running in a process
 * Sharing resources like worker threads removes the need for duplicating these resources
*/
class environment final
{
public:
	enum class purpose
	{
		normal,
		inactive,
	};

public:
	explicit environment (boost::filesystem::path const & = nano::working_path ());
	std::error_code apply_overrides (nano::node_flags & flags_a, purpose purpose_a, boost::program_options::variables_map const & = boost::program_options::variables_map{});
	boost::filesystem::path path;
	boost::asio::io_context ctx;
	nano::alarm alarm;
	std::unique_ptr<nano::work_pool> work_impl;
	nano::work_pool & work;
	nano::environment_constants constants;

private:
	void apply_purpose_overrides (nano::node_flags & flags_a, purpose purpose_a) const;
	std::error_code apply_command_line_overrides (nano::node_flags & flags_a, boost::program_options::variables_map const &);
};
}
