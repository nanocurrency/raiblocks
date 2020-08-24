#include <nano/lib/cli.hpp>
#include <nano/node/cli.hpp>
#include <nano/node/environment.hpp>
#include <nano/node/nodeconfig.hpp>

#include <boost/program_options.hpp>

#include <algorithm>
#include <thread>

nano::environment::environment (boost::filesystem::path const & path_a) :
path{ path_a },
alarm{ ctx },
work_impl{ std::make_unique<nano::work_pool> (std::max (std::thread::hardware_concurrency (), 1u)) },
work{ *work_impl }
{
	/*
	 * @warning May throw a filesystem exception
	 */
	boost::system::error_code error_chmod;
	boost::filesystem::create_directories (path_a);
	nano::set_secure_perm_directory (path_a, error_chmod);
}

std::error_code nano::environment::apply_overrides (nano::node_flags & flags_a, purpose purpose_a, boost::program_options::variables_map const & vm)
{
	std::error_code result{};
	apply_purpose_overrides (flags_a, purpose_a);
	result = apply_command_line_overrides (flags_a, vm);
	return result;
}

std::error_code nano::environment::apply_command_line_overrides (nano::node_flags & flags_a, boost::program_options::variables_map const & vm)
{
	std::error_code ec;
	flags_a.disable_backup = (vm.count ("disable_backup") > 0);
	flags_a.disable_lazy_bootstrap = (vm.count ("disable_lazy_bootstrap") > 0);
	flags_a.disable_legacy_bootstrap = (vm.count ("disable_legacy_bootstrap") > 0);
	flags_a.disable_wallet_bootstrap = (vm.count ("disable_wallet_bootstrap") > 0);
	if (!flags_a.inactive_node)
	{
		flags_a.disable_bootstrap_listener = (vm.count ("disable_bootstrap_listener") > 0);
		flags_a.disable_tcp_realtime = (vm.count ("disable_tcp_realtime") > 0);
	}
	flags_a.disable_providing_telemetry_metrics = (vm.count ("disable_providing_telemetry_metrics") > 0);
	if ((vm.count ("disable_udp") > 0) && (vm.count ("enable_udp") > 0))
	{
		ec = nano::error_cli::ambiguous_udp_options;
	}
	flags_a.disable_udp = (vm.count ("enable_udp") == 0);
	if (flags_a.disable_tcp_realtime && flags_a.disable_udp)
	{
		ec = nano::error_cli::disable_all_network;
	}
	flags_a.disable_unchecked_cleanup = (vm.count ("disable_unchecked_cleanup") > 0);
	flags_a.disable_unchecked_drop = (vm.count ("disable_unchecked_drop") > 0);
	flags_a.disable_block_processor_unchecked_deletion = (vm.count ("disable_block_processor_unchecked_deletion") > 0);
	flags_a.allow_bootstrap_peers_duplicates = (vm.count ("allow_bootstrap_peers_duplicates") > 0);
	flags_a.fast_bootstrap = (vm.count ("fast_bootstrap") > 0);
	if (flags_a.fast_bootstrap)
	{
		flags_a.disable_block_processor_unchecked_deletion = true;
		flags_a.block_processor_batch_size = 256 * 1024;
		flags_a.block_processor_full_size = 1024 * 1024;
		flags_a.block_processor_verification_size = std::numeric_limits<size_t>::max ();
	}
	auto block_processor_batch_size_it = vm.find ("block_processor_batch_size");
	if (block_processor_batch_size_it != vm.end ())
	{
		flags_a.block_processor_batch_size = block_processor_batch_size_it->second.as<size_t> ();
	}
	auto block_processor_full_size_it = vm.find ("block_processor_full_size");
	if (block_processor_full_size_it != vm.end ())
	{
		flags_a.block_processor_full_size = block_processor_full_size_it->second.as<size_t> ();
	}
	auto block_processor_verification_size_it = vm.find ("block_processor_verification_size");
	if (block_processor_verification_size_it != vm.end ())
	{
		flags_a.block_processor_verification_size = block_processor_verification_size_it->second.as<size_t> ();
	}
	auto inactive_votes_cache_size_it = vm.find ("inactive_votes_cache_size");
	if (inactive_votes_cache_size_it != vm.end ())
	{
		flags_a.inactive_votes_cache_size = inactive_votes_cache_size_it->second.as<size_t> ();
	}
	auto vote_processor_capacity_it = vm.find ("vote_processor_capacity");
	if (vote_processor_capacity_it != vm.end ())
	{
		flags_a.vote_processor_capacity = vote_processor_capacity_it->second.as<size_t> ();
	}
	// Config overriding
	auto config (vm.find ("config"));
	if (config != vm.end ())
	{
		flags_a.config_overrides = nano::config_overrides (config->second.as<std::vector<nano::config_key_value_pair>> ());
	}
	return ec;
}

void nano::environment::apply_purpose_overrides (nano::node_flags & flags_a, purpose purpose_a) const
{
	switch (purpose_a)
	{
		case purpose::inactive:
			flags_a.inactive_node = true;
			flags_a.read_only = true;
			flags_a.generate_cache.reps = false;
			flags_a.generate_cache.cemented_count = false;
			flags_a.generate_cache.unchecked_count = false;
			flags_a.generate_cache.account_count = false;
			flags_a.generate_cache.epoch_2 = false;
			flags_a.disable_bootstrap_listener = true;
			flags_a.disable_tcp_realtime = true;
			break;
		case purpose::normal:
			break;
	}
}
