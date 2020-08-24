#include <nano/lib/alarm.hpp>
#include <nano/lib/stats.hpp>
#include <nano/lib/worker.hpp>
#include <nano/node/network.hpp>
#include <nano/node/nodeconfig.hpp>
#include <nano/node/telemetry.hpp>
#include <nano/node/transport/transport.hpp>
#include <nano/secure/blockstore.hpp>
#include <nano/secure/buffer.hpp>

#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <cstdint>
#include <future>
#include <numeric>
#include <set>

using namespace std::chrono_literals;

nano::telemetry::telemetry (nano::network & network_a, nano::alarm & alarm_a, nano::worker & worker_a, nano::observer_set<nano::telemetry_data const &, nano::endpoint const &> & observers_a, nano::stat & stats_a, nano::environment_constants & constants_a, bool disable_ongoing_requests_a) :
network (network_a),
alarm (alarm_a),
worker (worker_a),
observers (observers_a),
stats (stats_a),
constants (constants_a),
disable_ongoing_requests (disable_ongoing_requests_a)
{
}

void nano::telemetry::start ()
{
	// Cannot be done in the constructor as a shared_from_this () call is made in ongoing_req_all_peers
	if (!disable_ongoing_requests)
	{
		ongoing_req_all_peers (std::chrono::milliseconds (0));
	}
}

void nano::telemetry::stop ()
{
	stopped = true;
}

void nano::telemetry::set (nano::telemetry_ack const & message_a, nano::transport::channel const & channel_a)
{
	if (!stopped)
	{
		nano::unique_lock<std::mutex> lk (mutex);
		nano::endpoint endpoint = channel_a.get_endpoint ();
		auto it = recent_or_initial_request_telemetry_data.find (endpoint);
		if (it == recent_or_initial_request_telemetry_data.cend () || !it->undergoing_request)
		{
			// Not requesting telemetry data from this peer so ignore it
			stats.inc (nano::stat::type::telemetry, nano::stat::detail::unsolicited_telemetry_ack);
			return;
		}

		recent_or_initial_request_telemetry_data.modify (it, [&message_a](nano::telemetry_info & telemetry_info_a) {
			telemetry_info_a.data = message_a.data;
		});

		// This can also remove the peer
		auto error = verify_message (message_a, channel_a);

		if (!error)
		{
			// Received telemetry data from a peer which hasn't disabled providing telemetry metrics and there's no errors with the data
			lk.unlock ();
			observers.notify (message_a.data, endpoint);
			lk.lock ();
		}
		channel_processed (endpoint, error);
	}
}

bool nano::telemetry::verify_message (nano::telemetry_ack const & message_a, nano::transport::channel const & channel_a)
{
	if (message_a.is_empty_payload ())
	{
		return true;
	}

	auto remove_channel = false;
	// We want to ensure that the node_id of the channel matches that in the message before attempting to
	// use the data to remove any peers.
	auto node_id_mismatch = (channel_a.get_node_id () != message_a.data.node_id);
	if (!node_id_mismatch)
	{
		// The data could be correctly signed but for a different node id
		remove_channel = message_a.data.validate_signature (message_a.size ());
		if (!remove_channel)
		{
			// Check for different genesis blocks
			remove_channel = (message_a.data.genesis_block != constants.ledger.genesis_hash);
			if (remove_channel)
			{
				stats.inc (nano::stat::type::telemetry, nano::stat::detail::different_genesis_hash);
			}
		}
		else
		{
			stats.inc (nano::stat::type::telemetry, nano::stat::detail::invalid_signature);
		}
	}
	else
	{
		stats.inc (nano::stat::type::telemetry, nano::stat::detail::node_id_mismatch);
	}

	if (remove_channel)
	{
		// Add to peer exclusion list
		network.excluded_peers.add (channel_a.get_tcp_endpoint (), network.size ());

		// Disconnect from peer with incorrect telemetry data
		network.erase (channel_a);
	}

	return remove_channel || node_id_mismatch;
}

std::chrono::milliseconds nano::telemetry::cache_plus_buffer_cutoff_time () const
{
	// This include the waiting time for the response as well as a buffer (1 second) waiting for the alarm operation to be scheduled and completed
	return cache_cutoff + response_time_cutoff + 1s;
}

bool nano::telemetry::within_cache_plus_buffer_cutoff (telemetry_info const & telemetry_info) const
{
	auto is_within = (telemetry_info.last_response + cache_plus_buffer_cutoff_time ()) >= std::chrono::steady_clock::now ();
	return !telemetry_info.awaiting_first_response () && is_within;
}

bool nano::telemetry::within_cache_cutoff (telemetry_info const & telemetry_info) const
{
	auto is_within = (telemetry_info.last_response + cache_cutoff) >= std::chrono::steady_clock::now ();
	return !telemetry_info.awaiting_first_response () && is_within;
}

void nano::telemetry::ongoing_req_all_peers (std::chrono::milliseconds next_request_interval)
{
	alarm.add (std::chrono::steady_clock::now () + next_request_interval, [this_w = std::weak_ptr<telemetry> (shared_from_this ())]() {
		if (auto this_l = this_w.lock ())
		{
			// Check if there are any peers which are in the peers list which haven't been request, or any which are below or equal to the cache cutoff time
			if (!this_l->stopped)
			{
				class tag_channel
				{
				};

				struct channel_wrapper
				{
					std::shared_ptr<nano::transport::channel> channel;
					channel_wrapper (std::shared_ptr<nano::transport::channel> const & channel_a) :
					channel (channel_a)
					{
					}
					nano::endpoint endpoint () const
					{
						return channel->get_endpoint ();
					}
				};

				// clang-format off
				namespace mi = boost::multi_index;
				boost::multi_index_container<channel_wrapper,
				mi::indexed_by<
					mi::hashed_unique<mi::tag<tag_endpoint>,
						mi::const_mem_fun<channel_wrapper, nano::endpoint, &channel_wrapper::endpoint>>,
					mi::hashed_unique<mi::tag<tag_channel>,
						mi::member<channel_wrapper, std::shared_ptr<nano::transport::channel>, &channel_wrapper::channel>>>> peers;
				// clang-format on

				{
					// Copy peers to the multi index container so can get better asymptotic complexity in future operations
					auto temp_peers = this_l->network.list (std::numeric_limits<size_t>::max (), this_l->constants.protocol.telemetry_protocol_version_min);
					peers.insert (temp_peers.begin (), temp_peers.end ());
				}

				{
					// Cleanup any stale saved telemetry data for non-existent peers
					nano::lock_guard<std::mutex> guard (this_l->mutex);
					for (auto it = this_l->recent_or_initial_request_telemetry_data.begin (); it != this_l->recent_or_initial_request_telemetry_data.end ();)
					{
						if (!it->undergoing_request && !this_l->within_cache_cutoff (*it) && peers.count (it->endpoint) == 0)
						{
							it = this_l->recent_or_initial_request_telemetry_data.erase (it);
						}
						else
						{
							++it;
						}
					}

					// Remove from peers list if it exists and is within the cache cutoff
					for (auto peers_it = peers.begin (); peers_it != peers.end ();)
					{
						auto it = this_l->recent_or_initial_request_telemetry_data.find (peers_it->endpoint ());
						if (it != this_l->recent_or_initial_request_telemetry_data.cend () && this_l->within_cache_cutoff (*it))
						{
							peers_it = peers.erase (peers_it);
						}
						else
						{
							++peers_it;
						}
					}
				}

				// Request data from new peers, or ones which are out of date
				for (auto const & peer : boost::make_iterator_range (peers))
				{
					this_l->get_metrics_single_peer_async (peer.channel, [](auto const &) {
						// Intentionally empty, just using to refresh the cache
					});
				}

				// Schedule the next request; Use the default request time unless a telemetry request cache expires sooner
				nano::lock_guard<std::mutex> guard (this_l->mutex);
				long long next_round = std::chrono::duration_cast<std::chrono::milliseconds> (this_l->cache_cutoff + this_l->response_time_cutoff).count ();
				if (!this_l->recent_or_initial_request_telemetry_data.empty ())
				{
					auto range = boost::make_iterator_range (this_l->recent_or_initial_request_telemetry_data.get<tag_last_updated> ());
					for (auto telemetry_info : range)
					{
						if (!telemetry_info.undergoing_request && peers.count (telemetry_info.endpoint) == 0)
						{
							auto const last_response = telemetry_info.last_response;
							auto now = std::chrono::steady_clock::now ();
							if (now > last_response + this_l->cache_cutoff)
							{
								next_round = std::min<long long> (next_round, std::chrono::duration_cast<std::chrono::milliseconds> (now - (last_response + this_l->cache_cutoff)).count ());
							}
							// We are iterating in sorted order from last_updated, so can break once we have found the first valid one.
							break;
						}
					}
				}

				this_l->ongoing_req_all_peers (std::chrono::milliseconds (next_round));
			}
		}
	});
}

std::unordered_map<nano::endpoint, nano::telemetry_data> nano::telemetry::get_metrics ()
{
	std::unordered_map<nano::endpoint, nano::telemetry_data> telemetry_data;

	nano::lock_guard<std::mutex> guard (mutex);
	auto range = boost::make_iterator_range (recent_or_initial_request_telemetry_data);
	// clang-format off
	nano::transform_if (range.begin (), range.end (), std::inserter (telemetry_data, telemetry_data.end ()),
		[this](auto const & telemetry_info) { return this->within_cache_plus_buffer_cutoff (telemetry_info); },
		[](auto const & telemetry_info) { return std::pair<const nano::endpoint, nano::telemetry_data>{ telemetry_info.endpoint, telemetry_info.data }; });
	// clang-format on

	return telemetry_data;
}

void nano::telemetry::get_metrics_single_peer_async (std::shared_ptr<nano::transport::channel> const & channel_a, std::function<void(telemetry_data_response const &)> const & callback_a)
{
	auto invoke_callback_with_error = [&callback_a, &worker = this->worker, channel_a]() {
		nano::endpoint endpoint;
		if (channel_a)
		{
			endpoint = channel_a->get_endpoint ();
		}
		worker.push_task ([callback_a, endpoint]() {
			auto const error = true;
			callback_a ({ nano::telemetry_data{}, endpoint, error });
		});
	};

	if (!stopped)
	{
		if (channel_a && (channel_a->get_network_version () >= constants.protocol.telemetry_protocol_version_min))
		{
			auto add_callback_async = [& worker = this->worker, &callback_a](telemetry_data const & telemetry_data_a, nano::endpoint const & endpoint_a) {
				telemetry_data_response telemetry_data_response_l{ telemetry_data_a, endpoint_a, false };
				worker.push_task ([telemetry_data_response_l, callback_a]() {
					callback_a (telemetry_data_response_l);
				});
			};

			// Check if this is within the cache
			nano::lock_guard<std::mutex> guard (mutex);
			auto it = recent_or_initial_request_telemetry_data.find (channel_a->get_endpoint ());
			if (it != recent_or_initial_request_telemetry_data.cend () && within_cache_cutoff (*it))
			{
				add_callback_async (it->data, it->endpoint);
			}
			else
			{
				if (it != recent_or_initial_request_telemetry_data.cend () && it->undergoing_request)
				{
					// A request is currently undergoing, add the callback
					debug_assert (callbacks.count (it->endpoint) > 0);
					callbacks[it->endpoint].push_back (callback_a);
				}
				else
				{
					if (it == recent_or_initial_request_telemetry_data.cend ())
					{
						// Insert dummy values, it's important not to use "last_response" time here without first checking that awaiting_first_response () returns false.
						recent_or_initial_request_telemetry_data.emplace (channel_a->get_endpoint (), nano::telemetry_data (), std::chrono::steady_clock::now (), true);
						it = recent_or_initial_request_telemetry_data.find (channel_a->get_endpoint ());
					}
					else
					{
						recent_or_initial_request_telemetry_data.modify (it, [](nano::telemetry_info & telemetry_info_a) {
							telemetry_info_a.undergoing_request = true;
						});
					}
					callbacks[it->endpoint].push_back (callback_a);
					fire_request_message (channel_a);
				}
			}
		}
		else
		{
			invoke_callback_with_error ();
		}
	}
	else
	{
		invoke_callback_with_error ();
	}
}

nano::telemetry_data_response nano::telemetry::get_metrics_single_peer (std::shared_ptr<nano::transport::channel> const & channel_a)
{
	std::promise<telemetry_data_response> promise;
	get_metrics_single_peer_async (channel_a, [&promise](telemetry_data_response const & single_metric_data_a) {
		promise.set_value (single_metric_data_a);
	});

	return promise.get_future ().get ();
}

void nano::telemetry::fire_request_message (std::shared_ptr<nano::transport::channel> const & channel_a)
{
	// Fire off a telemetry request to all passed in channels
	debug_assert (channel_a->get_network_version () >= constants.protocol.telemetry_protocol_version_min);

	uint64_t round_l;
	{
		auto it = recent_or_initial_request_telemetry_data.find (channel_a->get_endpoint ());
		recent_or_initial_request_telemetry_data.modify (it, [](nano::telemetry_info & telemetry_info_a) {
			++telemetry_info_a.round;
		});
		round_l = it->round;
	}

	std::weak_ptr<nano::telemetry> this_w (shared_from_this ());
	nano::telemetry_req message;
	// clang-format off
	channel_a->send (message, [this_w, endpoint = channel_a->get_endpoint (), round_l](boost::system::error_code const & ec, size_t size_a) {
		if (auto this_l = this_w.lock ())
		{
			if (ec)
			{
				// Error sending the telemetry_req message
				this_l->stats.inc (nano::stat::type::telemetry, nano::stat::detail::failed_send_telemetry_req);
				nano::lock_guard<std::mutex> guard (this_l->mutex);
				this_l->channel_processed (endpoint, true);
			}
			else
			{
				// If no response is seen after a certain period of time remove it
				this_l->alarm.add (std::chrono::steady_clock::now () + this_l->response_time_cutoff, [round_l, this_w, endpoint]() {
					if (auto this_l = this_w.lock ())
					{
						nano::lock_guard<std::mutex> guard (this_l->mutex);
						auto it = this_l->recent_or_initial_request_telemetry_data.find (endpoint);
						if (it != this_l->recent_or_initial_request_telemetry_data.cend () && it->undergoing_request && round_l == it->round)
						{
							this_l->stats.inc (nano::stat::type::telemetry, nano::stat::detail::no_response_received);
							this_l->channel_processed (endpoint, true);
						}
					}
				});			
			}
		}
	},
	nano::buffer_drop_policy::no_socket_drop);
	// clang-format on
}

void nano::telemetry::channel_processed (nano::endpoint const & endpoint_a, bool error_a)
{
	auto it = recent_or_initial_request_telemetry_data.find (endpoint_a);
	if (it != recent_or_initial_request_telemetry_data.end ())
	{
		if (!error_a)
		{
			recent_or_initial_request_telemetry_data.modify (it, [](nano::telemetry_info & telemetry_info_a) {
				telemetry_info_a.last_response = std::chrono::steady_clock::now ();
				telemetry_info_a.undergoing_request = false;
			});
		}
		else
		{
			recent_or_initial_request_telemetry_data.erase (endpoint_a);
		}
		flush_callbacks_async (endpoint_a, error_a);
	}
}

void nano::telemetry::flush_callbacks_async (nano::endpoint const & endpoint_a, bool error_a)
{
	// Post to worker so that it's truly async and not on the calling thread (same problem as std::async otherwise)
	worker.push_task ([endpoint_a, error_a, this_w = std::weak_ptr<nano::telemetry> (shared_from_this ())]() {
		if (auto this_l = this_w.lock ())
		{
			nano::unique_lock<std::mutex> lk (this_l->mutex);
			while (!this_l->callbacks[endpoint_a].empty ())
			{
				lk.unlock ();
				this_l->invoke_callbacks (endpoint_a, error_a);
				lk.lock ();
			}
		}
	});
}

void nano::telemetry::invoke_callbacks (nano::endpoint const & endpoint_a, bool error_a)
{
	std::vector<std::function<void(telemetry_data_response const &)>> callbacks_l;
	telemetry_data_response response_data{ nano::telemetry_data (), endpoint_a, error_a };
	{
		// Copy data so that it can be used outside of holding the lock
		nano::lock_guard<std::mutex> guard (mutex);

		callbacks_l = callbacks[endpoint_a];
		auto it = recent_or_initial_request_telemetry_data.find (endpoint_a);
		if (it != recent_or_initial_request_telemetry_data.end ())
		{
			response_data.telemetry_data = it->data;
		}
		callbacks.erase (endpoint_a);
	}

	// Need to account for nodes which disable telemetry data in responses
	for (auto & callback : callbacks_l)
	{
		callback (response_data);
	}
}

size_t nano::telemetry::telemetry_data_size ()
{
	nano::lock_guard<std::mutex> guard (mutex);
	return recent_or_initial_request_telemetry_data.size ();
}

nano::telemetry_info::telemetry_info (nano::endpoint const & endpoint_a, nano::telemetry_data const & data_a, std::chrono::steady_clock::time_point last_response_a, bool undergoing_request_a) :
endpoint (endpoint_a),
data (data_a),
last_response (last_response_a),
undergoing_request (undergoing_request_a)
{
}

bool nano::telemetry_info::awaiting_first_response () const
{
	return data == nano::telemetry_data ();
}

std::unique_ptr<nano::container_info_component> nano::collect_container_info (telemetry & telemetry, const std::string & name)
{
	auto composite = std::make_unique<container_info_composite> (name);
	size_t callbacks_count;
	{
		nano::lock_guard<std::mutex> guard (telemetry.mutex);
		std::unordered_map<nano::endpoint, std::vector<std::function<void(telemetry_data_response const &)>>> callbacks;
		callbacks_count = std::accumulate (callbacks.begin (), callbacks.end (), static_cast<size_t> (0), [](auto total, auto const & callback_a) {
			return total += callback_a.second.size ();
		});
	}

	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "recent_or_initial_request_telemetry_data", telemetry.telemetry_data_size (), sizeof (decltype (telemetry.recent_or_initial_request_telemetry_data)::value_type) }));
	composite->add_component (std::make_unique<container_info_leaf> (container_info{ "callbacks", callbacks_count, sizeof (decltype (telemetry.callbacks)::value_type::second_type) }));

	return composite;
}

nano::telemetry_data nano::consolidate_telemetry_data (std::vector<nano::telemetry_data> const & telemetry_datas)
{
	if (telemetry_datas.empty ())
	{
		return {};
	}
	else if (telemetry_datas.size () == 1)
	{
		// Only 1 element in the collection, so just return it.
		return telemetry_datas.front ();
	}

	std::unordered_map<uint8_t, int> protocol_versions;
	std::unordered_map<std::string, int> vendor_versions;
	std::unordered_map<uint64_t, int> bandwidth_caps;
	std::unordered_map<nano::block_hash, int> genesis_blocks;

	// Use a trimmed average which excludes the upper and lower 10% of the results
	std::multiset<uint64_t> account_counts;
	std::multiset<uint64_t> block_counts;
	std::multiset<uint64_t> cemented_counts;
	std::multiset<uint32_t> peer_counts;
	std::multiset<uint64_t> unchecked_counts;
	std::multiset<uint64_t> uptimes;
	std::multiset<uint64_t> bandwidths;
	std::multiset<uint64_t> timestamps;
	std::multiset<uint64_t> active_difficulties;

	for (auto const & telemetry_data : telemetry_datas)
	{
		account_counts.insert (telemetry_data.account_count);
		block_counts.insert (telemetry_data.block_count);
		cemented_counts.insert (telemetry_data.cemented_count);

		std::ostringstream ss;
		ss << telemetry_data.major_version << "." << telemetry_data.minor_version << "." << telemetry_data.patch_version << "." << telemetry_data.pre_release_version << "." << telemetry_data.maker;
		++vendor_versions[ss.str ()];
		timestamps.insert (std::chrono::duration_cast<std::chrono::milliseconds> (telemetry_data.timestamp.time_since_epoch ()).count ());
		++protocol_versions[telemetry_data.protocol_version];
		peer_counts.insert (telemetry_data.peer_count);
		unchecked_counts.insert (telemetry_data.unchecked_count);
		uptimes.insert (telemetry_data.uptime);
		// 0 has a special meaning (unlimited), don't include it in the average as it will be heavily skewed
		if (telemetry_data.bandwidth_cap != 0)
		{
			bandwidths.insert (telemetry_data.bandwidth_cap);
		}

		++bandwidth_caps[telemetry_data.bandwidth_cap];
		++genesis_blocks[telemetry_data.genesis_block];
		active_difficulties.insert (telemetry_data.active_difficulty);
	}

	// Remove 10% of the results from the lower and upper bounds to catch any outliers. Need at least 10 responses before any are removed.
	auto num_either_side_to_remove = telemetry_datas.size () / 10;

	auto strip_outliers_and_sum = [num_either_side_to_remove](auto & counts) {
		counts.erase (counts.begin (), std::next (counts.begin (), num_either_side_to_remove));
		counts.erase (std::next (counts.rbegin (), num_either_side_to_remove).base (), counts.end ());
		return std::accumulate (counts.begin (), counts.end (), nano::uint128_t (0), [](nano::uint128_t total, auto count) {
			return total += count;
		});
	};

	auto account_sum = strip_outliers_and_sum (account_counts);
	auto block_sum = strip_outliers_and_sum (block_counts);
	auto cemented_sum = strip_outliers_and_sum (cemented_counts);
	auto peer_sum = strip_outliers_and_sum (peer_counts);
	auto unchecked_sum = strip_outliers_and_sum (unchecked_counts);
	auto uptime_sum = strip_outliers_and_sum (uptimes);
	auto bandwidth_sum = strip_outliers_and_sum (bandwidths);
	auto active_difficulty_sum = strip_outliers_and_sum (active_difficulties);

	nano::telemetry_data consolidated_data;
	auto size = telemetry_datas.size () - num_either_side_to_remove * 2;
	consolidated_data.account_count = boost::numeric_cast<decltype (consolidated_data.account_count)> (account_sum / size);
	consolidated_data.block_count = boost::numeric_cast<decltype (consolidated_data.block_count)> (block_sum / size);
	consolidated_data.cemented_count = boost::numeric_cast<decltype (consolidated_data.cemented_count)> (cemented_sum / size);
	consolidated_data.peer_count = boost::numeric_cast<decltype (consolidated_data.peer_count)> (peer_sum / size);
	consolidated_data.uptime = boost::numeric_cast<decltype (consolidated_data.uptime)> (uptime_sum / size);
	consolidated_data.unchecked_count = boost::numeric_cast<decltype (consolidated_data.unchecked_count)> (unchecked_sum / size);
	consolidated_data.active_difficulty = boost::numeric_cast<decltype (consolidated_data.unchecked_count)> (active_difficulty_sum / size);

	if (!timestamps.empty ())
	{
		auto timestamp_sum = strip_outliers_and_sum (timestamps);
		consolidated_data.timestamp = std::chrono::system_clock::time_point (std::chrono::milliseconds (boost::numeric_cast<uint64_t> (timestamp_sum / timestamps.size ())));
	}

	auto set_mode_or_average = [](auto const & collection, auto & var, auto const & sum, size_t size) {
		auto max = std::max_element (collection.begin (), collection.end (), [](auto const & lhs, auto const & rhs) {
			return lhs.second < rhs.second;
		});
		if (max->second > 1)
		{
			var = max->first;
		}
		else
		{
			var = (sum / size).template convert_to<std::remove_reference_t<decltype (var)>> ();
		}
	};

	auto set_mode = [](auto const & collection, auto & var, size_t size) {
		auto max = std::max_element (collection.begin (), collection.end (), [](auto const & lhs, auto const & rhs) {
			return lhs.second < rhs.second;
		});
		if (max->second > 1)
		{
			var = max->first;
		}
		else
		{
			// Just pick the first one
			var = collection.begin ()->first;
		}
	};

	// Use the mode of protocol version and vendor version. Also use it for bandwidth cap if there is 2 or more of the same cap.
	set_mode_or_average (bandwidth_caps, consolidated_data.bandwidth_cap, bandwidth_sum, size);
	set_mode (protocol_versions, consolidated_data.protocol_version, size);
	set_mode (genesis_blocks, consolidated_data.genesis_block, size);

	// Vendor version, needs to be parsed out of the string
	std::string version;
	set_mode (vendor_versions, version, size);

	// May only have major version, but check for optional parameters as well, only output if all are used
	std::vector<std::string> version_fragments;
	boost::split (version_fragments, version, boost::is_any_of ("."));
	debug_assert (version_fragments.size () == 5);
	consolidated_data.major_version = boost::lexical_cast<uint8_t> (version_fragments.front ());
	consolidated_data.minor_version = boost::lexical_cast<uint8_t> (version_fragments[1]);
	consolidated_data.patch_version = boost::lexical_cast<uint8_t> (version_fragments[2]);
	consolidated_data.pre_release_version = boost::lexical_cast<uint8_t> (version_fragments[3]);
	consolidated_data.maker = boost::lexical_cast<uint8_t> (version_fragments[4]);

	return consolidated_data;
}

nano::telemetry_data nano::local_telemetry_data (nano::block_store & store_a, nano::ledger_cache const & ledger_cache_a, nano::network & network_a, uint64_t bandwidth_limit_a, nano::environment_constants const & constants_a, std::chrono::steady_clock::time_point statup_time_a, uint64_t active_difficulty_a, nano::keypair const & node_id_a)
{
	nano::telemetry_data telemetry_data;
	telemetry_data.node_id = node_id_a.pub;
	telemetry_data.block_count = ledger_cache_a.block_count;
	telemetry_data.cemented_count = ledger_cache_a.cemented_count;
	telemetry_data.bandwidth_cap = bandwidth_limit_a;
	telemetry_data.protocol_version = constants_a.protocol.protocol_version;
	telemetry_data.uptime = std::chrono::duration_cast<std::chrono::seconds> (std::chrono::steady_clock::now () - statup_time_a).count ();
	telemetry_data.unchecked_count = store_a.unchecked_count (store_a.tx_begin_read ());
	telemetry_data.genesis_block = constants_a.ledger.genesis_hash;
	telemetry_data.peer_count = nano::narrow_cast<decltype (telemetry_data.peer_count)> (network_a.size ());
	telemetry_data.account_count = ledger_cache_a.account_count;
	telemetry_data.major_version = nano::get_major_node_version ();
	telemetry_data.minor_version = nano::get_minor_node_version ();
	telemetry_data.patch_version = nano::get_patch_node_version ();
	telemetry_data.pre_release_version = nano::get_pre_release_node_version ();
	telemetry_data.maker = 0; // 0 Indicates it originated from the NF
	telemetry_data.timestamp = std::chrono::system_clock::now ();
	telemetry_data.active_difficulty = active_difficulty_a;
	// Make sure this is the final operation!
	telemetry_data.sign (node_id_a);
	return telemetry_data;
}
