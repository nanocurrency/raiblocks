#pragma once
#include <nano/rpc/rpc.hpp>

namespace boost
{
namespace asio
{
	class io_context;
}
}

namespace nano
{
/**
 * Specialization of nano::rpc with TLS support
 */
class rpc_secure : public rpc
{
public:
	rpc_secure (boost::asio::io_context & context_a, nano::rpc_config const & config_a, nano::rpc_handler_interface & rpc_handler_interface_a);

	/** Starts accepting connections */
	void accept () override;
};
}
