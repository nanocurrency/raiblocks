#include <nano/node/node.hpp>
#include <nano/node/portmapping.hpp>

#include <upnpcommands.h>
#include <upnperrors.h>

nano::port_mapping::port_mapping (nano::node & node_a) :
node (node_a),
protocols ({ { { "TCP", 0, boost::asio::ip::address_v4::any (), 0 }, { "UDP", 0, boost::asio::ip::address_v4::any (), 0 } } })
{
}

void nano::port_mapping::start ()
{
	on = true;
	node.background ([this] {
		this->check_mapping_loop ();
	});
}

void nano::port_mapping::refresh_devices ()
{
	if (!network_params.network.is_test_network ())
	{
		upnp_state upnp_l;
		int discover_error_l = 0;
		upnp_l.devices = upnpDiscover (2000, nullptr, nullptr, UPNP_LOCAL_PORT_ANY, false, 2, &discover_error_l);
		std::array<char, 64> local_address;
		local_address.fill (0);
		auto igd_error_l (UPNP_GetValidIGD (upnp_l.devices, &upnp_l.urls, &upnp_l.data, local_address.data (), sizeof (local_address)));
		if (check_count % 15 == 0)
		{
			node.logger.always_log (boost::str (boost::format ("UPnP local address: %1%, discovery: %2%, IGD search: %3%") % local_address.data () % discover_error_l % igd_error_l));
			if (node.config.logging.upnp_details_logging ())
			{
				for (auto i (upnp_l.devices); i != nullptr; i = i->pNext)
				{
					node.logger.always_log (boost::str (boost::format ("UPnP device url: %1% st: %2% usn: %3%") % i->descURL % i->st % i->usn));
				}
			}
		}
		// Update port mapping
		nano::lock_guard<std::mutex> lock (mutex);
		upnp = std::move (upnp_l);
		if (igd_error_l == 1 || igd_error_l == 2)
		{
			boost::system::error_code ec;
			address = boost::asio::ip::address_v4::from_string (local_address.data (), ec);
		}
	}
}

nano::endpoint nano::port_mapping::external_address ()
{
	nano::endpoint result (boost::asio::ip::address_v6{}, 0);
	nano::lock_guard<std::mutex> lock (mutex);
	for (auto & protocol : protocols)
	{
		if (protocol.external_port != 0)
		{
			result = nano::endpoint (protocol.external_address, protocol.external_port);
		}
	}
	return result;
}

void nano::port_mapping::refresh_mapping ()
{
	if (!network_params.network.is_test_network ())
	{
		nano::lock_guard<std::mutex> lock (mutex);
		auto node_port (std::to_string (node.network.endpoint ().port ()));
		auto config_port (node.config.external_port != 0 ? std::to_string (node.config.external_port) : node_port);

		// We don't map the RPC port because, unless RPC authentication was added, this would almost always be a security risk
		for (auto & protocol : protocols)
		{
			auto add_port_mapping_error (UPNP_AddPortMapping (upnp.urls.controlURL, upnp.data.first.servicetype, config_port.c_str (), node_port.c_str (), address.to_string ().c_str (), nullptr, protocol.name, nullptr, nullptr));
			if (node.config.logging.upnp_details_logging ())
			{
				node.logger.always_log (boost::str (boost::format ("UPnP %1% port mapping response: %2%") % protocol.name % add_port_mapping_error));
			}
			if (add_port_mapping_error == UPNPCOMMAND_SUCCESS)
			{
				protocol.external_port = static_cast<uint16_t> (std::atoi (config_port.data ()));
				if (node.config.logging.upnp_details_logging ())
				{
					node.logger.always_log (boost::str (boost::format ("%1% mapped to %2%") % config_port % node_port));
				}
			}
			else
			{
				protocol.external_port = 0;
				node.logger.always_log (boost::str (boost::format ("UPnP failed %1%: %2%") % add_port_mapping_error % strupnperror (add_port_mapping_error)));
			}
		}
	}
}

int nano::port_mapping::check_mapping ()
{
	int result (3600);
	if (!network_params.network.is_test_network ())
	{
		// Long discovery time and fast setup/teardown make this impractical for testing
		nano::lock_guard<std::mutex> lock (mutex);
		auto node_port (std::to_string (node.network.endpoint ().port ()));
		auto config_port (node.config.external_port != 0 ? std::to_string (node.config.external_port) : node_port);
		for (auto & protocol : protocols)
		{
			std::array<char, 64> int_client;
			std::array<char, 6> int_port;
			std::array<char, 16> remaining_mapping_duration;
			remaining_mapping_duration.fill (0);
			auto verify_port_mapping_error (UPNP_GetSpecificPortMappingEntry (upnp.urls.controlURL, upnp.data.first.servicetype, config_port.c_str (), protocol.name, nullptr, int_client.data (), int_port.data (), nullptr, nullptr, remaining_mapping_duration.data ()));
			if (verify_port_mapping_error == UPNPCOMMAND_SUCCESS)
			{
				protocol.remaining = std::atoi (remaining_mapping_duration.data ());
			}
			else
			{
				protocol.remaining = 0;
				node.logger.always_log (boost::str (boost::format ("UPNP_GetSpecificPortMappingEntry failed %1%: %2%") % verify_port_mapping_error % strupnperror (verify_port_mapping_error)));
			}
			result = std::min (result, protocol.remaining);
			std::array<char, 64> external_address;
			external_address.fill (0);
			auto external_ip_error (UPNP_GetExternalIPAddress (upnp.urls.controlURL, upnp.data.first.servicetype, external_address.data ()));
			if (external_ip_error == UPNPCOMMAND_SUCCESS)
			{
				boost::system::error_code ec;
				protocol.external_address = boost::asio::ip::address_v4::from_string (external_address.data (), ec);
			}
			else
			{
				protocol.external_address = boost::asio::ip::address_v4::any ();
				node.logger.always_log (boost::str (boost::format ("UPNP_GetExternalIPAddress failed %1%: %2%") % verify_port_mapping_error % strupnperror (verify_port_mapping_error)));
			}
			if (node.config.logging.upnp_details_logging ())
			{
				node.logger.always_log (boost::str (boost::format ("UPnP %1% mapping verification response: %2%, external ip response: %3%, external ip: %4%, internal ip: %5%, remaining lease: %6%") % protocol.name % verify_port_mapping_error % external_ip_error % external_address.data () % address.to_string () % remaining_mapping_duration.data ()));
			}
		}
	}
	return result;
}

void nano::port_mapping::check_mapping_loop ()
{
	int wait_duration = network_params.portmapping.check_timeout;
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
		wait_duration = 300;
		if (check_count < 10)
		{
			node.logger.always_log (boost::str (boost::format ("UPnP No IGD devices found")));
		}
	}
	++check_count;
	if (on)
	{
		auto node_l (node.shared ());
		node.alarm.add (std::chrono::steady_clock::now () + std::chrono::seconds (wait_duration), [node_l]() {
			node_l->port_mapping.check_mapping_loop ();
		});
	}
}

void nano::port_mapping::stop ()
{
	on = false;
	nano::lock_guard<std::mutex> lock (mutex);
	for (auto & protocol : protocols)
	{
		if (protocol.external_port != 0)
		{
			// Be a good citizen for the router and shut down our mapping
			auto delete_error (UPNP_DeletePortMapping (upnp.urls.controlURL, upnp.data.first.servicetype, std::to_string (protocol.external_port).c_str (), protocol.name, address.to_string ().c_str ()));
			if (delete_error)
			{
				node.logger.always_log (boost::str (boost::format ("Shutdown port mapping response: %1%") % delete_error));
			}
		}
	}
}

nano::upnp_state::~upnp_state ()
{
	if (devices)
	{
		freeUPNPDevlist (devices);
		devices = nullptr;
	}
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
	urls = other_a.urls;
	other_a.urls = { 0 };
	data = other_a.data;
	other_a.data = { { 0 } };
	return *this;
}
