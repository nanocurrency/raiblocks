#pragma once
#include <nano/ipc_flatbuffers_lib/generated/flatbuffers/nanoapi_generated.h>

#include <chrono>
#include <memory>

#include <flatbuffers/flatbuffers.h>

namespace nano
{
namespace ipc
{
	/** Produces Nano API compliant Flatbuffers from objects and builders */
	class flatbuffer_producer
	{
	public:
		flatbuffer_producer ();
		flatbuffer_producer (std::shared_ptr<flatbuffers::FlatBufferBuilder> const & builder_a);

		template <typename T>
		static std::shared_ptr<flatbuffers::FlatBufferBuilder> make_buffer (T & object_a, std::string const & correlation_id_a = {}, std::string const & credentials_a = {})
		{
			nano::ipc::flatbuffer_producer producer;
			producer.set_correlation_id (correlation_id_a);
			producer.set_credentials (credentials_a);
			producer.create_response (object_a);
			return producer.fbb;
		}

		void make_error (int code, std::string const & message);

		/** Every message is put in an envelope, which contains the message type and other sideband information */
		template <typename T>
		auto make_envelope (flatbuffers::Offset<T> obj)
		{
			auto const correlation_id_string = fbb->CreateString (correlation_id);
			auto const credentials_string = fbb->CreateString (credentials);
			nanoapi::EnvelopeBuilder envelope_builder (*fbb);
			envelope_builder.add_time (std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now ().time_since_epoch ()).count ());
			envelope_builder.add_message_type (nanoapi::MessageTraits<T>::enum_value);
			envelope_builder.add_message (obj.Union ());

			if (!correlation_id.empty ())
			{
				envelope_builder.add_correlation_id (correlation_id_string);
			}
			if (!credentials.empty ())
			{
				envelope_builder.add_credentials (credentials_string);
			}
			return envelope_builder.Finish ();
		}

		template <typename T>
		void create_response (flatbuffers::Offset<T> offset)
		{
			auto const root = make_envelope (offset);
			fbb->Finish (root);
		}

		template <typename T, typename = std::enable_if_t<std::is_base_of<flatbuffers::NativeTable, T>::value>>
		void create_response (T const & obj)
		{
			create_response (T::TableType::Pack (*fbb, &obj));
		}

		template <typename T>
		void create_builder_response (T builder)
		{
			auto const offset = builder.Finish ();
			auto const root = make_envelope (offset);
			fbb->Finish (root);
		}

		/** Set the correlation id. This will be added to the envelope. */
		void set_correlation_id (std::string const & correlation_id_a);
		/** Set the credentials. This will be added to the envelope. */
		void set_credentials (std::string const & credentials_a);
		/** Returns the flatbuffer */
		std::shared_ptr<flatbuffers::FlatBufferBuilder> get_shared_flatbuffer () const;

	private:
		/** The builder managed by this instance */
		std::shared_ptr<flatbuffers::FlatBufferBuilder> fbb;
		/** Correlation id, if available */
		std::string correlation_id;
		/** Credentials, if available */
		std::string credentials;
	};
}
}
