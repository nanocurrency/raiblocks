#include <nano/node/node.hpp>
#include <nano/node/portmapping.hpp>

#include <boost/format.hpp>

#include <upnpcommands.h>
#include <upnperrors.h>

nano::port_mapping::port_mapping (nano::node & node_a)
	: node (node_a)
	, protocols ({ { { "TCP", 0, boost::asio::ip::address_v4::any (), 0 }, { "UDP", 0, boost::asio::ip::address_v4::any (), 0 } } })
{
}

void nano::port_mapping::start ()
{
	on = true;
	node.background ([this] {
		this->check_mapping_loop ();
	});
}

std::string nano::port_mapping::get_config_port (std::string const & node_port_a)
{
	return node.config.external_port != 0 ? std::to_string (node.config.external_port) : node_port_a;
}

void nano::port_mapping::refresh_devices ()
{
	if (!network_params.network.is_test_network ())
	{
		upnp_state upnp_l;
		int discover_error_l = 0;
		upnp_l.devices = upnpDiscover (2000, nullptr, nullptr, UPNP_LOCAL_PORT_ANY, false, 2, &discover_error_l);
		std::array<char, 64> local_address_l;
		local_address_l.fill (0);
		auto igd_error_l (UPNP_GetValidIGD (upnp_l.devices, &upnp_l.urls, &upnp_l.data, local_address_l.data (), sizeof (local_address_l)));
		if (check_count % 15 == 0)
		{
			node.logger.always_log (boost::str (boost::format ("UPnP local address: %1%, discovery: %2%, IGD search: %3%") % local_address_l.data () % discover_error_l % igd_error_l));
			if (node.config.logging.upnp_details_logging ())
			{
				for (auto i (upnp_l.devices); i != nullptr; i = i->pNext)
				{
					node.logger.always_log (boost::str (boost::format ("UPnP device url: %1% st: %2% usn: %3%") % i->descURL % i->st % i->usn));
				}
			}
		}
		// Update port mapping
		nano::lock_guard<std::mutex> guard_l (mutex);
		upnp = std::move (upnp_l);
		if (igd_error_l == 1 || igd_error_l == 2)
		{
			boost::system::error_code ec;
			address = boost::asio::ip::address_v4::from_string (local_address_l.data (), ec);
		}
	}
}

nano::endpoint nano::port_mapping::external_address ()
{
	nano::endpoint result_l (boost::asio::ip::address_v6{}, 0);
	nano::lock_guard<std::mutex> guard_l (mutex);
	for (auto & protocol : protocols)
	{
		if (protocol.external_port != 0)
		{
			result_l = nano::endpoint (protocol.external_address, protocol.external_port);
		}
	}
	return result_l;
}

void nano::port_mapping::refresh_mapping ()
{
	if (!network_params.network.is_test_network ())
	{
		nano::lock_guard<std::mutex> guard_l (mutex);
		auto node_port_l (std::to_string (node.network.endpoint ().port ()));
		auto config_port_l (get_config_port (node_port_l));

		// We don't map the RPC port because, unless RPC authentication was added, this would almost always be a security risk
		for (auto & protocol : protocols)
		{
			auto upnp_description = std::string ("Nano Node (") + network_params.network.get_current_network_as_string () + ")";
			auto add_port_mapping_error_l (UPNP_AddPortMapping (upnp.urls.controlURL, upnp.data.first.servicetype, config_port_l.c_str (), node_port_l.c_str (), address.to_string ().c_str (), upnp_description.c_str (), protocol.name, nullptr, nullptr));
			if (node.config.logging.upnp_details_logging ())
			{
				node.logger.always_log (boost::str (boost::format ("UPnP %1% port mapping response: %2%") % protocol.name % add_port_mapping_error_l));
			}
			if (add_port_mapping_error_l == UPNPCOMMAND_SUCCESS)
			{
				protocol.external_port = static_cast<uint16_t> (std::atoi (config_port_l.data ()));
				if (node.config.logging.upnp_details_logging ())
				{
					node.logger.always_log (boost::str (boost::format ("%1% mapped to %2%") % config_port_l % node_port_l));
				}
			}
			else
			{
				protocol.external_port = 0;
				node.logger.always_log (boost::str (boost::format ("UPnP failed %1%: %2%") % add_port_mapping_error_l % strupnperror (add_port_mapping_error_l)));
			}
		}
	}
}

int nano::port_mapping::check_mapping ()
{
	int result_l (3600);
	if (!network_params.network.is_test_network ())
	{
		// Long discovery time and fast setup/teardown make this impractical for testing
		nano::lock_guard<std::mutex> guard_l (mutex);
		auto node_port_l (std::to_string (node.network.endpoint ().port ()));
		auto config_port_l (get_config_port (node_port_l));
		for (auto & protocol : protocols)
		{
			std::array<char, 64> int_client_l;
			std::array<char, 6> int_port_l;
			std::array<char, 16> remaining_mapping_duration_l;
			remaining_mapping_duration_l.fill (0);
			auto verify_port_mapping_error_l (UPNP_GetSpecificPortMappingEntry (upnp.urls.controlURL, upnp.data.first.servicetype, config_port_l.c_str (), protocol.name, nullptr, int_client_l.data (), int_port_l.data (), nullptr, nullptr, remaining_mapping_duration_l.data ()));
			if (verify_port_mapping_error_l == UPNPCOMMAND_SUCCESS)
			{
				protocol.remaining = std::atoi (remaining_mapping_duration_l.data ());
			}
			else
			{
				protocol.remaining = 0;
				node.logger.always_log (boost::str (boost::format ("UPNP_GetSpecificPortMappingEntry failed %1%: %2%") % verify_port_mapping_error_l % strupnperror (verify_port_mapping_error_l)));
			}
			result_l = std::min (result_l, protocol.remaining);
			std::array<char, 64> external_address_l;
			external_address_l.fill (0);
			auto external_ip_error_l (UPNP_GetExternalIPAddress (upnp.urls.controlURL, upnp.data.first.servicetype, external_address_l.data ()));
			if (external_ip_error_l == UPNPCOMMAND_SUCCESS)
			{
				boost::system::error_code ec;
				protocol.external_address = boost::asio::ip::address_v4::from_string (external_address_l.data (), ec);
			}
			else
			{
				protocol.external_address = boost::asio::ip::address_v4::any ();
				node.logger.always_log (boost::str (boost::format ("UPNP_GetExternalIPAddress failed %1%: %2%") % verify_port_mapping_error_l % strupnperror (verify_port_mapping_error_l)));
			}
			if (node.config.logging.upnp_details_logging ())
			{
				node.logger.always_log (boost::str (boost::format ("UPnP %1% mapping verification response: %2%, external ip response: %3%, external ip: %4%, internal ip: %5%, remaining lease: %6%") % protocol.name % verify_port_mapping_error_l % external_ip_error_l % external_address_l.data () % address.to_string () % remaining_mapping_duration_l.data ()));
			}
		}
	}
	return result_l;
}

void nano::port_mapping::check_mapping_loop ()
{
	int wait_duration_l = network_params.portmapping.check_timeout;
	refresh_devices ();
	if (upnp.devices != nullptr)
	{
		auto remaining (check_mapping ());
		// If the mapping is lost, refresh it
		if (remaining == 0)
		{
			refresh_mapping ();
		}
	}
	else
	{
		wait_duration_l = 300;
		if (check_count < 10)
		{
			node.logger.always_log (boost::str (boost::format ("UPnP No IGD devices found")));
		}
	}
	++check_count;
	if (on)
	{
		auto node_l (node.shared ());
		node.alarm.add (std::chrono::steady_clock::now () + std::chrono::seconds (wait_duration_l), [node_l]() {
			node_l->port_mapping.check_mapping_loop ();
		});
	}
}

void nano::port_mapping::stop ()
{
	on = false;
	nano::lock_guard<std::mutex> guard_l (mutex);
	for (auto & protocol : protocols)
	{
		if (protocol.external_port != 0)
		{
			// Be a good citizen for the router and shut down our mapping
			auto delete_error_l (UPNP_DeletePortMapping (upnp.urls.controlURL, upnp.data.first.servicetype, std::to_string (protocol.external_port).c_str (), protocol.name, address.to_string ().c_str ()));
			if (delete_error_l)
			{
				node.logger.always_log (boost::str (boost::format ("Shutdown port mapping response: %1%") % delete_error_l));
			}
		}
	}
}

nano::upnp_state::~upnp_state ()
{
	if (devices)
	{
		freeUPNPDevlist (devices);
	}
	FreeUPNPUrls (&urls);
}

nano::upnp_state & nano::upnp_state::operator= (nano::upnp_state && other_a)
{
	if (this == &other_a)
	{
		return *this;
	}
	if (devices)
	{
		freeUPNPDevlist (devices);
	}
	devices = other_a.devices;
	other_a.devices = nullptr;
	FreeUPNPUrls (&urls);
	urls = other_a.urls;
	other_a.urls = { 0 };
	data = other_a.data;
	other_a.data = { { 0 } };
	return *this;
}
