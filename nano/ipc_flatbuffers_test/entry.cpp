#include <nano/ipc_flatbuffers_lib/flatbuffer_producer.hpp>
#include <nano/ipc_flatbuffers_lib/generated/flatbuffers/nanoapi_generated.h>
#include <nano/lib/asio.hpp>
#include <nano/lib/ipc.hpp>
#include <nano/lib/ipc_client.hpp>

#include <boost/endian/conversion.hpp>
#include <boost/program_options.hpp>

#include <iostream>

#include <flatbuffers/flatbuffers.h>

namespace
{
void read_message_loop (std::shared_ptr<nano::ipc::ipc_client> const & connection)
{
	auto buffer (std::make_shared<std::vector<uint8_t>> ());
	connection->async_read_message (buffer, std::chrono::seconds::max (), [buffer, connection](nano::error error_a, size_t size_a) {
		if (!error_a)
		{
			auto verifier (flatbuffers::Verifier (buffer->data (), buffer->size ()));
			if (!nanoapi::VerifyEnvelopeBuffer (verifier))
			{
				std::cerr << "Invalid message" << std::endl;
			}
			else
			{
				auto const envelope (nanoapi::GetEnvelope (buffer->data ()));
				if (envelope->message_type () == nanoapi::Message_EventConfirmation)
				{
					std::cout << "Confirmation at " << envelope->time () << std::endl;
					auto const conf (envelope->message_as_EventConfirmation ());
					std::cout << "  Account    : " << conf->account ()->str () << std::endl;
					std::cout << "  Amount     : " << conf->amount ()->str () << std::endl;
					std::cout << "  Block type : " << nanoapi::EnumNamesBlock ()[conf->block_type ()] << std::endl;
					auto const state_block = conf->block_as_BlockState ();
					if (state_block)
					{
						std::cout << "  Balance    : " << state_block->balance ()->str () << std::endl;
					}
				}

				read_message_loop (connection);
			}
		}
	});
}
}

/** A sample IPC/flatbuffers client that subscribes to confirmations from a local node */
int main (int argc, char * const * argv)
{
	boost::asio::io_context io_ctx;
	auto connection (std::make_shared<nano::ipc::ipc_client> (io_ctx));
	// The client only connects to a local live node for now; the test will
	// be improved later to handle various options, including port and address.
	std::string const ipc_address = "::1";
	uint16_t const ipc_port = 7077;
	connection->async_connect (ipc_address, ipc_port, [connection](nano::error err) {
		if (!err)
		{
			nanoapi::TopicConfirmationT conf;
			connection->async_write (nano::ipc::shared_buffer_from (conf), [connection](nano::error err, size_t size) {
				if (!err)
				{
					std::cout << "Awaiting confirmations..." << std::endl;
					read_message_loop (connection);
				}
			});
		}
		else
		{
			std::cerr << err.get_message () << std::endl;
		}
	});

	io_ctx.run ();
	return 0;
}
