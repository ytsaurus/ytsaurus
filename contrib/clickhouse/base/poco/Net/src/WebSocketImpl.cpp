//
// WebSocketImpl.cpp
//
// Library: Net
// Package: WebSocket
// Module:  WebSocketImpl
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#define NOMINMAX
#include "DBPoco/Net/WebSocketImpl.h"
#include "DBPoco/Net/NetException.h"
#include "DBPoco/Net/WebSocket.h"
#include "DBPoco/Net/HTTPSession.h"
#include "DBPoco/Buffer.h"
#include "DBPoco/BinaryWriter.h"
#include "DBPoco/BinaryReader.h"
#include "DBPoco/MemoryStream.h"
#include "DBPoco/Format.h"
#include <limits>
#include <cstring>


namespace DBPoco {
namespace Net {


WebSocketImpl::WebSocketImpl(StreamSocketImpl* pStreamSocketImpl, HTTPSession& session, bool mustMaskPayload):
	StreamSocketImpl(pStreamSocketImpl->sockfd()),
	_pStreamSocketImpl(pStreamSocketImpl),
	_maxPayloadSize(std::numeric_limits<int>::max()),
	_buffer(0),
	_bufferOffset(0),
	_frameFlags(0),
	_mustMaskPayload(mustMaskPayload)
{
	DB_poco_check_ptr(pStreamSocketImpl);
	_pStreamSocketImpl->duplicate();
	session.drainBuffer(_buffer);
}


WebSocketImpl::~WebSocketImpl()
{
	try
	{
		_pStreamSocketImpl->release();
		reset();
	}
	catch (...)
	{
		DB_poco_unexpected();
	}
}


int WebSocketImpl::sendBytes(const void* buffer, int length, int flags)
{
	DBPoco::Buffer<char> frame(length + MAX_HEADER_LENGTH);
	DBPoco::MemoryOutputStream ostr(frame.begin(), frame.size());
	DBPoco::BinaryWriter writer(ostr, DBPoco::BinaryWriter::NETWORK_BYTE_ORDER);

	if (flags == 0) flags = WebSocket::FRAME_BINARY;
	flags &= 0xff;
	writer << static_cast<DBPoco::UInt8>(flags);
	DBPoco::UInt8 lengthByte(0);
	if (_mustMaskPayload)
	{
		lengthByte |= FRAME_FLAG_MASK;
	}
	if (length < 126)
	{
		lengthByte |= static_cast<DBPoco::UInt8>(length);
		writer << lengthByte;
	}
	else if (length < 65536)
	{
		lengthByte |= 126;
		writer << lengthByte << static_cast<DBPoco::UInt16>(length);
	}
	else
	{
		lengthByte |= 127;
		writer << lengthByte << static_cast<DBPoco::UInt64>(length);
	}
	if (_mustMaskPayload)
	{
		const DBPoco::UInt32 mask = _rnd.next();
		const char* m = reinterpret_cast<const char*>(&mask);
		const char* b = reinterpret_cast<const char*>(buffer);
		writer.writeRaw(m, 4);
		char* p = frame.begin() + ostr.charsWritten();
		for (int i = 0; i < length; i++)
		{
			p[i] = b[i] ^ m[i % 4];
		}
	}
	else
	{
		std::memcpy(frame.begin() + ostr.charsWritten(), buffer, length);
	}
	_pStreamSocketImpl->sendBytes(frame.begin(), length + static_cast<int>(ostr.charsWritten()));
	return length;
}


int WebSocketImpl::receiveHeader(char mask[4], bool& useMask)
{
	char header[MAX_HEADER_LENGTH];
	int n = receiveNBytes(header, 2);
	if (n <= 0)
	{
		_frameFlags = 0;
		return n;
	}
	DB_poco_assert (n == 2);
	DBPoco::UInt8 flags = static_cast<DBPoco::UInt8>(header[0]);
	_frameFlags = flags;
	DBPoco::UInt8 lengthByte = static_cast<DBPoco::UInt8>(header[1]);
	useMask = ((lengthByte & FRAME_FLAG_MASK) != 0);
	int payloadLength;
	lengthByte &= 0x7f;
	if (lengthByte == 127)
	{
		n = receiveNBytes(header + 2, 8);
		if (n <= 0)
		{
			_frameFlags = 0;
			return n;
		}
		DBPoco::MemoryInputStream istr(header + 2, 8);
		DBPoco::BinaryReader reader(istr, DBPoco::BinaryReader::NETWORK_BYTE_ORDER);
		DBPoco::UInt64 l;
		reader >> l;
		if (l > _maxPayloadSize) throw WebSocketException("Payload too big", WebSocket::WS_ERR_PAYLOAD_TOO_BIG);
		payloadLength = static_cast<int>(l);
	}
	else if (lengthByte == 126)
	{
		n = receiveNBytes(header + 2, 2);
		if (n <= 0)
		{
			_frameFlags = 0;
			return n;
		}
		DBPoco::MemoryInputStream istr(header + 2, 2);
		DBPoco::BinaryReader reader(istr, DBPoco::BinaryReader::NETWORK_BYTE_ORDER);
		DBPoco::UInt16 l;
		reader >> l;
		if (l > _maxPayloadSize) throw WebSocketException("Payload too big", WebSocket::WS_ERR_PAYLOAD_TOO_BIG);
		payloadLength = static_cast<int>(l);
	}
	else
	{
		if (lengthByte > _maxPayloadSize) throw WebSocketException("Payload too big", WebSocket::WS_ERR_PAYLOAD_TOO_BIG);
		payloadLength = lengthByte;
	}

	if (useMask)
	{
		n = receiveNBytes(mask, 4);
		if (n <= 0)
		{
			_frameFlags = 0;
			return n;
		}
	}

	return payloadLength;
}


void WebSocketImpl::setMaxPayloadSize(int maxPayloadSize)
{
	DB_poco_assert (maxPayloadSize > 0);

	_maxPayloadSize = maxPayloadSize;
}


int WebSocketImpl::receivePayload(char *buffer, int payloadLength, char mask[4], bool useMask)
{
	int received = receiveNBytes(reinterpret_cast<char*>(buffer), payloadLength);
	if (received <= 0) throw WebSocketException("Incomplete frame received", WebSocket::WS_ERR_INCOMPLETE_FRAME);

	if (useMask)
	{
		for (int i = 0; i < received; i++)
		{
			buffer[i] ^= mask[i % 4];
		}
	}
	return received;
}


int WebSocketImpl::receiveBytes(void* buffer, int length, int)
{
	char mask[4];
	bool useMask;
	int payloadLength = receiveHeader(mask, useMask);
	if (payloadLength <= 0)
		return payloadLength;
	if (payloadLength > length)
		throw WebSocketException(DBPoco::format("Insufficient buffer for payload size %d", payloadLength), WebSocket::WS_ERR_PAYLOAD_TOO_BIG);
	return receivePayload(reinterpret_cast<char*>(buffer), payloadLength, mask, useMask);
}


int WebSocketImpl::receiveBytes(DBPoco::Buffer<char>& buffer, int)
{
	char mask[4];
	bool useMask;
	int payloadLength = receiveHeader(mask, useMask);
	if (payloadLength <= 0)
		return payloadLength;
	std::size_t oldSize = buffer.size();
	buffer.resize(oldSize + payloadLength);
	return receivePayload(buffer.begin() + oldSize, payloadLength, mask, useMask);
}


int WebSocketImpl::receiveNBytes(void* buffer, int bytes)
{
	int received = receiveSomeBytes(reinterpret_cast<char*>(buffer), bytes);
	if (received > 0)
	{
		while (received < bytes)
		{
			int n = receiveSomeBytes(reinterpret_cast<char*>(buffer) + received, bytes - received);
			if (n > 0)
				received += n;
			else
				throw WebSocketException("Incomplete frame received", WebSocket::WS_ERR_INCOMPLETE_FRAME);
		}
	}
	return received;
}


int WebSocketImpl::receiveSomeBytes(char* buffer, int bytes)
{
	int n = static_cast<int>(_buffer.size()) - _bufferOffset;
	if (n > 0)
	{
		if (bytes < n) n = bytes;
		std::memcpy(buffer, _buffer.begin() + _bufferOffset, n);
		_bufferOffset += n;
		return n;
	}
	else
	{
		return _pStreamSocketImpl->receiveBytes(buffer, bytes);
	}
}


SocketImpl* WebSocketImpl::acceptConnection(SocketAddress& clientAddr)
{
	throw DBPoco::InvalidAccessException("Cannot acceptConnection() on a WebSocketImpl");
}


void WebSocketImpl::connect(const SocketAddress& address)
{
	throw DBPoco::InvalidAccessException("Cannot connect() a WebSocketImpl");
}


void WebSocketImpl::connect(const SocketAddress& address, const DBPoco::Timespan& timeout)
{
	throw DBPoco::InvalidAccessException("Cannot connect() a WebSocketImpl");
}


void WebSocketImpl::connectNB(const SocketAddress& address)
{
	throw DBPoco::InvalidAccessException("Cannot connectNB() a WebSocketImpl");
}


void WebSocketImpl::bind(const SocketAddress& address, bool reuseAddress)
{
	throw DBPoco::InvalidAccessException("Cannot bind() a WebSocketImpl");
}


void WebSocketImpl::bind(const SocketAddress& address, bool reuseAddress, bool reusePort)
{
	throw DBPoco::InvalidAccessException("Cannot bind() a WebSocketImpl");
}


void WebSocketImpl::bind6(const SocketAddress& address, bool reuseAddress, bool ipV6Only)
{
	throw DBPoco::InvalidAccessException("Cannot bind6() a WebSocketImpl");
}


void WebSocketImpl::bind6(const SocketAddress& address, bool reuseAddress, bool reusePort, bool ipV6Only)
{
	throw DBPoco::InvalidAccessException("Cannot bind6() a WebSocketImpl");
}


void WebSocketImpl::listen(int backlog)
{
	throw DBPoco::InvalidAccessException("Cannot listen() on a WebSocketImpl");
}


void WebSocketImpl::close()
{
	_pStreamSocketImpl->close();
	reset();
}


void WebSocketImpl::shutdownReceive()
{
	_pStreamSocketImpl->shutdownReceive();
}


void WebSocketImpl::shutdownSend()
{
	_pStreamSocketImpl->shutdownSend();
}


void WebSocketImpl::shutdown()
{
	_pStreamSocketImpl->shutdown();
}


int WebSocketImpl::sendTo(const void* buffer, int length, const SocketAddress& address, int flags)
{
	throw DBPoco::InvalidAccessException("Cannot sendTo() on a WebSocketImpl");
}


int WebSocketImpl::receiveFrom(void* buffer, int length, SocketAddress& address, int flags)
{
	throw DBPoco::InvalidAccessException("Cannot receiveFrom() on a WebSocketImpl");
}


void WebSocketImpl::sendUrgent(unsigned char data)
{
	throw DBPoco::InvalidAccessException("Cannot sendUrgent() on a WebSocketImpl");
}


bool WebSocketImpl::secure() const
{
	return _pStreamSocketImpl->secure();
}


void WebSocketImpl::setSendTimeout(const DBPoco::Timespan& timeout)
{
	_pStreamSocketImpl->setSendTimeout(timeout);
}


DBPoco::Timespan WebSocketImpl::getSendTimeout()
{
	return _pStreamSocketImpl->getSendTimeout();
}


void WebSocketImpl::setReceiveTimeout(const DBPoco::Timespan& timeout)
{
	_pStreamSocketImpl->setReceiveTimeout(timeout);
}


DBPoco::Timespan WebSocketImpl::getReceiveTimeout()
{
	return _pStreamSocketImpl->getReceiveTimeout();
}


int WebSocketImpl::available()
{
	int n = static_cast<int>(_buffer.size()) - _bufferOffset;
	if (n > 0)
		return n + _pStreamSocketImpl->available();
	else
		return _pStreamSocketImpl->available();
}


} } // namespace DBPoco::Net
