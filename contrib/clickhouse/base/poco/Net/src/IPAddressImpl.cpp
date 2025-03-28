//
// IPAddress.cpp
//
// Library: Net
// Package: NetCore
// Module:  IPAddress
//
// Copyright (c) 2005-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/IPAddressImpl.h"
#include "DBPoco/Net/NetException.h"
#include "DBPoco/RefCountedObject.h"
#include "DBPoco/NumberFormatter.h"
#include "DBPoco/ByteOrder.h"
#include "DBPoco/String.h"
#include "DBPoco/Types.h"


using DBPoco::RefCountedObject;
using DBPoco::NumberFormatter;
using DBPoco::toLower;
using DBPoco::UInt8;
using DBPoco::UInt16;
using DBPoco::UInt32;


namespace {


template <typename T>
unsigned maskBits(T val, unsigned size)
	/// Returns the length of the mask (number of bits set in val).
	/// The val should be either all zeros or two contiguous areas of 1s and 0s. 
	/// The algorithm ignores invalid non-contiguous series of 1s and treats val 
	/// as if all bits between MSb and last non-zero bit are set to 1.
{
	unsigned count = 0;
	if (val)
	{
		val = (val ^ (val - 1)) >> 1;
		for (count = 0; val; ++count) val >>= 1;
	}
	else count = size;
	return size - count;
}


} // namespace


namespace DBPoco {
namespace Net {
namespace Impl {


//
// IPAddressImpl
//


IPAddressImpl::IPAddressImpl()
{
}


IPAddressImpl::~IPAddressImpl()
{
}


//
// IPv4AddressImpl
//


IPv4AddressImpl::IPv4AddressImpl()
{
	std::memset(&_addr, 0, sizeof(_addr));
}


IPv4AddressImpl::IPv4AddressImpl(const void* addr)
{
	std::memcpy(&_addr, addr, sizeof(_addr));
}


IPv4AddressImpl::IPv4AddressImpl(unsigned prefix)
{
	UInt32 addr = (prefix == 32) ? 0xffffffff : ~(0xffffffff >> prefix);
	_addr.s_addr = ByteOrder::toNetwork(addr);
}


IPv4AddressImpl::IPv4AddressImpl(const IPv4AddressImpl& addr)
{
	std::memcpy(&_addr, &addr._addr, sizeof(_addr));
}


IPv4AddressImpl& IPv4AddressImpl::operator = (const IPv4AddressImpl& addr)
{
	if (this == &addr)
		return *this;

	std::memcpy(&_addr, &addr._addr, sizeof(_addr));
	return *this;
}


std::string IPv4AddressImpl::toString() const
{
	const UInt8* bytes = reinterpret_cast<const UInt8*>(&_addr);
	std::string result;
	result.reserve(16);
	NumberFormatter::append(result, bytes[0]);
	result.append(".");
	NumberFormatter::append(result, bytes[1]);
	result.append(".");
	NumberFormatter::append(result, bytes[2]);
	result.append(".");
	NumberFormatter::append(result, bytes[3]);
	return result;
}


DB_poco_socklen_t IPv4AddressImpl::length() const
{
	return sizeof(_addr);
}


const void* IPv4AddressImpl::addr() const
{
	return &_addr;
}


IPAddressImpl::Family IPv4AddressImpl::family() const
{
	return AddressFamily::IPv4;
}


int IPv4AddressImpl::af() const
{
	return AF_INET;
}


unsigned IPv4AddressImpl::prefixLength() const
{
	return maskBits(ntohl(_addr.s_addr), 32);
}


DBPoco::UInt32 IPv4AddressImpl::scope() const
{
	return 0;
}


bool IPv4AddressImpl::isWildcard() const
{
	return _addr.s_addr == INADDR_ANY;
}


bool IPv4AddressImpl::isBroadcast() const
{
	return _addr.s_addr == INADDR_NONE;
}


bool IPv4AddressImpl::isLoopback() const
{
	return (ntohl(_addr.s_addr) & 0xFF000000) == 0x7F000000; // 127.0.0.1 to 127.255.255.255
}


bool IPv4AddressImpl::isMulticast() const
{
	return (ntohl(_addr.s_addr) & 0xF0000000) == 0xE0000000; // 224.0.0.0/24 to 239.0.0.0/24
}


bool IPv4AddressImpl::isLinkLocal() const
{
	return (ntohl(_addr.s_addr) & 0xFFFF0000) == 0xA9FE0000; // 169.254.0.0/16
}


bool IPv4AddressImpl::isSiteLocal() const
{
	UInt32 addr = ntohl(_addr.s_addr);
	return (addr & 0xFF000000) == 0x0A000000 ||     // 10.0.0.0/24
		(addr & 0xFFFF0000) == 0xC0A80000 ||        // 192.68.0.0/16
		(addr >= 0xAC100000 && addr <= 0xAC1FFFFF); // 172.16.0.0 to 172.31.255.255
}


bool IPv4AddressImpl::isIPv4Compatible() const
{
	return true;
}


bool IPv4AddressImpl::isIPv4Mapped() const
{
	return true;
}


bool IPv4AddressImpl::isWellKnownMC() const
{
	return (ntohl(_addr.s_addr) & 0xFFFFFF00) == 0xE0000000; // 224.0.0.0/8
}


bool IPv4AddressImpl::isNodeLocalMC() const
{
	return false;
}


bool IPv4AddressImpl::isLinkLocalMC() const
{
	return (ntohl(_addr.s_addr) & 0xFF000000) == 0xE0000000; // 244.0.0.0/24
}


bool IPv4AddressImpl::isSiteLocalMC() const
{
	return (ntohl(_addr.s_addr) & 0xFFFF0000) == 0xEFFF0000; // 239.255.0.0/16
}


bool IPv4AddressImpl::isOrgLocalMC() const
{
	return (ntohl(_addr.s_addr) & 0xFFFF0000) == 0xEFC00000; // 239.192.0.0/16
}


bool IPv4AddressImpl::isGlobalMC() const
{
	UInt32 addr = ntohl(_addr.s_addr);
	return addr >= 0xE0000100 && addr <= 0xEE000000; // 224.0.1.0 to 238.255.255.255
}


IPv4AddressImpl IPv4AddressImpl::parse(const std::string& addr)
{
	if (addr.empty()) return IPv4AddressImpl();
#if __GNUC__ < 3 || defined(POCO_VXWORKS)
	struct in_addr ia;
	ia.s_addr = inet_addr(const_cast<char*>(addr.c_str()));
	if (ia.s_addr == INADDR_NONE && addr != "255.255.255.255")
		return IPv4AddressImpl();
	else
		return IPv4AddressImpl(&ia);
#else
	struct in_addr ia;
	if (inet_aton(addr.c_str(), &ia))
		return IPv4AddressImpl(&ia);
	else
		return IPv4AddressImpl();
#endif
}


void IPv4AddressImpl::mask(const IPAddressImpl* pMask, const IPAddressImpl* pSet)
{
	DB_poco_assert (pMask->af() == AF_INET && pSet->af() == AF_INET);
	
	_addr.s_addr &= static_cast<const IPv4AddressImpl*>(pMask)->_addr.s_addr;
	_addr.s_addr |= static_cast<const IPv4AddressImpl*>(pSet)->_addr.s_addr & ~static_cast<const IPv4AddressImpl*>(pMask)->_addr.s_addr;
}


IPAddressImpl* IPv4AddressImpl::clone() const
{
	return new IPv4AddressImpl(&_addr);
}


IPv4AddressImpl IPv4AddressImpl::operator & (const IPv4AddressImpl& addr) const
{
	IPv4AddressImpl result(&_addr);
	result._addr.s_addr &= addr._addr.s_addr;
	return result;
}


IPv4AddressImpl IPv4AddressImpl::operator | (const IPv4AddressImpl& addr) const
{
	IPv4AddressImpl result(&_addr);
	result._addr.s_addr |= addr._addr.s_addr;
	return result;
}


IPv4AddressImpl IPv4AddressImpl::operator ^ (const IPv4AddressImpl& addr) const
{
	IPv4AddressImpl result(&_addr);
	result._addr.s_addr ^= addr._addr.s_addr;
	return result;
}


IPv4AddressImpl IPv4AddressImpl::operator ~ () const
{
IPv4AddressImpl result(&_addr);
	result._addr.s_addr ^= 0xffffffff;
	return result;
}


bool IPv4AddressImpl::operator == (const IPv4AddressImpl& addr) const
{
	return 0 == std::memcmp(&addr._addr, &_addr, sizeof(_addr));
}


bool IPv4AddressImpl::operator != (const IPv4AddressImpl& addr) const
{
	return !(*this == addr);
}


#if defined(DB_POCO_HAVE_IPv6)


//
// IPv6AddressImpl
//


IPv6AddressImpl::IPv6AddressImpl(): _scope(0)
{
	std::memset(&_addr, 0, sizeof(_addr));
}


IPv6AddressImpl::IPv6AddressImpl(const void* addr): _scope(0)
{
	std::memcpy(&_addr, addr, sizeof(_addr));
}


IPv6AddressImpl::IPv6AddressImpl(const void* addr, DBPoco::UInt32 scope): _scope(scope)
{
	std::memcpy(&_addr, addr, sizeof(_addr));
}


IPv6AddressImpl::IPv6AddressImpl(const IPv6AddressImpl& addr): _scope(addr._scope)
{
	std::memcpy((void*) &_addr, (void*) &addr._addr, sizeof(_addr));
}


IPv6AddressImpl& IPv6AddressImpl::operator = (const IPv6AddressImpl& addr)
{
	if (this == &addr)
		return *this;

	_scope = addr._scope;
	std::memcpy(&_addr, &addr._addr, sizeof(_addr));
	return *this;
}


IPv6AddressImpl::IPv6AddressImpl(unsigned prefix):
	_scope(0)
{
	unsigned i = 0;
	for (; prefix >= 32; ++i, prefix -= 32) 
	{
		_addr.s6_addr32[i] = 0xffffffff;
	}
	if (prefix > 0)
	{
		_addr.s6_addr32[i++] = ByteOrder::toNetwork(~(0xffffffffU >> prefix));
	}
	while (i < 4)
	{
		_addr.s6_addr32[i++] = 0;
	}
}


std::string IPv6AddressImpl::toString() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	if ((isIPv4Compatible() && !isLoopback()) || isIPv4Mapped())
	{
		std::string result;
		result.reserve(24);
		if (words[5] == 0)
			result.append("::");
		else
			result.append("::ffff:");
		const UInt8* bytes = reinterpret_cast<const UInt8*>(&_addr);
		if (bytes[12] != 0) // only 0.0.0.0 can start with zero
		{
			NumberFormatter::append(result, bytes[12]);
			result.append(".");
			NumberFormatter::append(result, bytes[13]);
			result.append(".");
			NumberFormatter::append(result, bytes[14]);
			result.append(".");
			NumberFormatter::append(result, bytes[15]);
		}
		return result;
	}
	else
	{
		std::string result;
		result.reserve(64);
		bool zeroSequence = false;
		int i = 0;
		while (i < 8)
		{
			if (!zeroSequence && words[i] == 0)
			{
				int zi = i;
				while (zi < 8 && words[zi] == 0) ++zi;
				if (zi > i + 1)
				{
					i = zi;
					result.append(":");
					zeroSequence = true;
				}
			}
			if (i > 0) result.append(":");
			if (i < 8) NumberFormatter::appendHex(result, ByteOrder::fromNetwork(words[i++]));
		}
		if (_scope > 0)
		{
			result.append("%");
			char buffer[IFNAMSIZ];
			if (if_indextoname(_scope, buffer))
			{
				result.append(buffer);
			}
			else
			{
				NumberFormatter::append(result, _scope);
			}
		}
		return toLower(result);
	}
}


DB_poco_socklen_t IPv6AddressImpl::length() const
{
	return sizeof(_addr);
}


const void* IPv6AddressImpl::addr() const
{
	return &_addr;
}


IPAddressImpl::Family IPv6AddressImpl::family() const
{
	return AddressFamily::IPv6;
}


int IPv6AddressImpl::af() const
{
	return AF_INET6;
}


unsigned IPv6AddressImpl::prefixLength() const
{
	unsigned bits = 0;
	unsigned bitPos = 128;
#if defined(DB_POCO_OS_FAMILY_UNIX)
	for (int i = 3; i >= 0; --i)
	{
		unsigned addr = ntohl(_addr.s6_addr32[i]);
		if ((bits = maskBits(addr, 32))) return (bitPos - (32 - bits));
		bitPos -= 32;
	}
	return 0;
#else
#warning prefixLength() not implemented
	throw NotImplementedException("prefixLength() not implemented");
#endif
}


DBPoco::UInt32 IPv6AddressImpl::scope() const
{
	return _scope;
}


bool IPv6AddressImpl::isWildcard() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return words[0] == 0 && words[1] == 0 && words[2] == 0 && words[3] == 0 && 
		words[4] == 0 && words[5] == 0 && words[6] == 0 && words[7] == 0;
}


bool IPv6AddressImpl::isBroadcast() const
{
	return false;
}


bool IPv6AddressImpl::isLoopback() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return words[0] == 0 && words[1] == 0 && words[2] == 0 && words[3] == 0 && 
		words[4] == 0 && words[5] == 0 && words[6] == 0 && ByteOrder::fromNetwork(words[7]) == 0x0001;
}


bool IPv6AddressImpl::isMulticast() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return (ByteOrder::fromNetwork(words[0]) & 0xFFE0) == 0xFF00;
}


bool IPv6AddressImpl::isLinkLocal() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return (ByteOrder::fromNetwork(words[0]) & 0xFFE0) == 0xFE80;
}


bool IPv6AddressImpl::isSiteLocal() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return ((ByteOrder::fromNetwork(words[0]) & 0xFFE0) == 0xFEC0) || ((ByteOrder::fromNetwork(words[0]) & 0xFF00) == 0xFC00);
}


bool IPv6AddressImpl::isIPv4Compatible() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return words[0] == 0 && words[1] == 0 && words[2] == 0 && words[3] == 0 && words[4] == 0 && words[5] == 0;
}


bool IPv6AddressImpl::isIPv4Mapped() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return words[0] == 0 && words[1] == 0 && words[2] == 0 && words[3] == 0 && words[4] == 0 && ByteOrder::fromNetwork(words[5]) == 0xFFFF;
}


bool IPv6AddressImpl::isWellKnownMC() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return (ByteOrder::fromNetwork(words[0]) & 0xFFF0) == 0xFF00;
}


bool IPv6AddressImpl::isNodeLocalMC() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return (ByteOrder::fromNetwork(words[0]) & 0xFFEF) == 0xFF01;
}


bool IPv6AddressImpl::isLinkLocalMC() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return (ByteOrder::fromNetwork(words[0]) & 0xFFEF) == 0xFF02;
}


bool IPv6AddressImpl::isSiteLocalMC() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return (ByteOrder::fromNetwork(words[0]) & 0xFFEF) == 0xFF05;
}


bool IPv6AddressImpl::isOrgLocalMC() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return (ByteOrder::fromNetwork(words[0]) & 0xFFEF) == 0xFF08;
}


bool IPv6AddressImpl::isGlobalMC() const
{
	const UInt16* words = reinterpret_cast<const UInt16*>(&_addr);
	return (ByteOrder::fromNetwork(words[0]) & 0xFFEF) == 0xFF0F;
}


IPv6AddressImpl IPv6AddressImpl::parse(const std::string& addr)
{
	if (addr.empty()) return IPv6AddressImpl();
	struct in6_addr ia;
	std::string::size_type pos = addr.find('%');
	if (std::string::npos != pos)
	{
		std::string::size_type start = ('[' == addr[0]) ? 1 : 0;
		std::string unscopedAddr(addr, start, pos - start);
		std::string scope(addr, pos + 1, addr.size() - start - pos);
		DBPoco::UInt32 scopeId(0);
		if (!(scopeId = if_nametoindex(scope.c_str())))
			return IPv6AddressImpl();
		if (inet_pton(AF_INET6, unscopedAddr.c_str(), &ia) == 1)
			return IPv6AddressImpl(&ia, scopeId);
		else
			return IPv6AddressImpl();
	}
	else
	{
		if (inet_pton(AF_INET6, addr.c_str(), &ia) == 1)
			return IPv6AddressImpl(&ia);
		else
			return IPv6AddressImpl();
	}
}


void IPv6AddressImpl::mask(const IPAddressImpl* pMask, const IPAddressImpl* pSet)
{
	throw DBPoco::NotImplementedException("mask() is only supported for IPv4 addresses");
}


IPAddressImpl* IPv6AddressImpl::clone() const
{
	return new IPv6AddressImpl(*this);
}


IPv6AddressImpl IPv6AddressImpl::operator & (const IPv6AddressImpl& addr) const
{
	if (_scope != addr._scope)
		throw DBPoco::InvalidArgumentException("Scope ID of passed IPv6 address does not match with the source one.");

	IPv6AddressImpl result(*this);
	result._addr.s6_addr32[0] &= addr._addr.s6_addr32[0];
	result._addr.s6_addr32[1] &= addr._addr.s6_addr32[1];
	result._addr.s6_addr32[2] &= addr._addr.s6_addr32[2];
	result._addr.s6_addr32[3] &= addr._addr.s6_addr32[3];
	return result;
}


IPv6AddressImpl IPv6AddressImpl::operator | (const IPv6AddressImpl& addr) const
{
	if (_scope != addr._scope) 
		throw DBPoco::InvalidArgumentException("Scope ID of passed IPv6 address does not match with the source one.");
    
	IPv6AddressImpl result(*this);
	result._addr.s6_addr32[0] |= addr._addr.s6_addr32[0];
	result._addr.s6_addr32[1] |= addr._addr.s6_addr32[1];
	result._addr.s6_addr32[2] |= addr._addr.s6_addr32[2];
	result._addr.s6_addr32[3] |= addr._addr.s6_addr32[3];
	return result;
}


IPv6AddressImpl IPv6AddressImpl::operator ^ (const IPv6AddressImpl& addr) const
{
	if (_scope != addr._scope)
		throw DBPoco::InvalidArgumentException("Scope ID of passed IPv6 address does not match  with the source one.");
    
	IPv6AddressImpl result(*this);

	result._addr.s6_addr32[0] ^= addr._addr.s6_addr32[0];
	result._addr.s6_addr32[1] ^= addr._addr.s6_addr32[1];
	result._addr.s6_addr32[2] ^= addr._addr.s6_addr32[2];
	result._addr.s6_addr32[3] ^= addr._addr.s6_addr32[3];
	return result;
}


IPv6AddressImpl IPv6AddressImpl::operator ~ () const
{
	IPv6AddressImpl result(*this);
	result._addr.s6_addr32[0] ^= 0xffffffff;
	result._addr.s6_addr32[1] ^= 0xffffffff;
	result._addr.s6_addr32[2] ^= 0xffffffff;
	result._addr.s6_addr32[3] ^= 0xffffffff;
	return result;
}


bool IPv6AddressImpl::operator == (const IPv6AddressImpl& addr) const
{
	return _scope == addr._scope && 0 == std::memcmp(&addr._addr, &_addr, sizeof(_addr));
}


bool IPv6AddressImpl::operator != (const IPv6AddressImpl& addr) const
{
	return !(*this == addr);
}


#endif // DB_POCO_HAVE_IPv6


} } } // namespace DBPoco::Net::Impl
