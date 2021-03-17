#pragma once

#include <nano/boost/asio/write.hpp>

namespace nano
{
class shared_const_buffer
{
public:
	using value_type = boost::asio::const_buffer;
	using const_iterator = const boost::asio::const_buffer *;

	explicit shared_const_buffer (std::vector<uint8_t> const & data);
	explicit shared_const_buffer (uint8_t data);
	explicit shared_const_buffer (std::string const & data);
	explicit shared_const_buffer (std::vector<uint8_t> && data);
	explicit shared_const_buffer (std::shared_ptr<std::vector<uint8_t>> const & data);

	boost::asio::const_buffer const * begin () const;
	boost::asio::const_buffer const * end () const;

	size_t size () const;

private:
	std::shared_ptr<std::vector<uint8_t>> m_data;
	boost::asio::const_buffer m_buffer;
};

static_assert (boost::asio::is_const_buffer_sequence<shared_const_buffer>::value, "Not ConstBufferSequence compliant");

template <typename AsyncWriteStream, typename WriteHandler>
BOOST_ASIO_INITFN_RESULT_TYPE (WriteHandler, void(boost::system::error_code, std::size_t))
async_write (AsyncWriteStream & s, nano::shared_const_buffer const & buffer, WriteHandler && handler)
{
	return boost::asio::async_write (s, buffer, std::move (handler));
}

/**
 * Alternative to nano::async_write where scatter/gather is desired for best performance, and where
 * the buffer originates from Flatbuffers.
 * @warning The buffers must be captured in the handler to ensure their lifetimes are properly extended.
 */
template <typename AsyncWriteStream, typename BufferType, typename WriteHandler>
BOOST_ASIO_INITFN_RESULT_TYPE (WriteHandler, void(boost::system::error_code, std::size_t))
unsafe_async_write (AsyncWriteStream & s, BufferType && buffer, WriteHandler && handler)
{
	return boost::asio::async_write (s, buffer, std::move (handler));
}
}
