#pragma once

#include <cstdint>

namespace nano
{
class environment_constants;
class keypair;
class telemetry_data;

void compare_default_telemetry_response_data_excluding_signature (nano::telemetry_data const & telemetry_data_a, nano::environment_constants const & constants_a, uint64_t bandwidth_limit_a, uint64_t active_difficulty_a);
void compare_default_telemetry_response_data (nano::telemetry_data const & telemetry_data_a, nano::environment_constants const & constants_a, uint64_t bandwidth_limit_a, uint64_t active_difficulty_a, nano::keypair const & node_id_a);
}
