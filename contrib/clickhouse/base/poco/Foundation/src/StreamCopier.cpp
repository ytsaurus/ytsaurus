//
// StreamCopier.cpp
//
// Library: Foundation
// Package: Streams
// Module:  StreamCopier
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/StreamCopier.h"
#include "DBPoco/Buffer.h"


namespace DBPoco {


std::streamsize StreamCopier::copyStream(std::istream& istr, std::ostream& ostr, std::size_t bufferSize)
{
	DB_poco_assert (bufferSize > 0);

	Buffer<char> buffer(bufferSize);
	std::streamsize len = 0;
	istr.read(buffer.begin(), bufferSize);
	std::streamsize n = istr.gcount();
	while (n > 0)
	{
		len += n;
		ostr.write(buffer.begin(), n);
		if (istr && ostr)
		{
			istr.read(buffer.begin(), bufferSize);
			n = istr.gcount();
		}
		else n = 0;
	}
	return len;
}


DBPoco::UInt64 StreamCopier::copyStream64(std::istream& istr, std::ostream& ostr, std::size_t bufferSize)
{
	DB_poco_assert (bufferSize > 0);

	Buffer<char> buffer(bufferSize);
	DBPoco::UInt64 len = 0;
	istr.read(buffer.begin(), bufferSize);
	std::streamsize n = istr.gcount();
	while (n > 0)
	{
		len += n;
		ostr.write(buffer.begin(), n);
		if (istr && ostr)
		{
			istr.read(buffer.begin(), bufferSize);
			n = istr.gcount();
		}
		else n = 0;
	}
	return len;
}


std::streamsize StreamCopier::copyToString(std::istream& istr, std::string& str, std::size_t bufferSize)
{
	DB_poco_assert (bufferSize > 0);

	Buffer<char> buffer(bufferSize);
	std::streamsize len = 0;
	istr.read(buffer.begin(), bufferSize);
	std::streamsize n = istr.gcount();
	while (n > 0)
	{
		len += n;
		str.append(buffer.begin(), static_cast<std::string::size_type>(n));
		if (istr)
		{
			istr.read(buffer.begin(), bufferSize);
			n = istr.gcount();
		}
		else n = 0;
	}
	return len;
}


DBPoco::UInt64 StreamCopier::copyToString64(std::istream& istr, std::string& str, std::size_t bufferSize)
{
	DB_poco_assert (bufferSize > 0);

	Buffer<char> buffer(bufferSize);
	DBPoco::UInt64 len = 0;
	istr.read(buffer.begin(), bufferSize);
	std::streamsize n = istr.gcount();
	while (n > 0)
	{
		len += n;
		str.append(buffer.begin(), static_cast<std::string::size_type>(n));
		if (istr)
		{
			istr.read(buffer.begin(), bufferSize);
			n = istr.gcount();
		}
		else n = 0;
	}
	return len;
}


std::streamsize StreamCopier::copyStreamUnbuffered(std::istream& istr, std::ostream& ostr)
{
	char c = 0;
	std::streamsize len = 0;
	istr.get(c);
	while (istr && ostr)
	{
		++len;
		ostr.put(c);
		istr.get(c);
	}
	return len;
}


DBPoco::UInt64 StreamCopier::copyStreamUnbuffered64(std::istream& istr, std::ostream& ostr)
{
	char c = 0;
	DBPoco::UInt64 len = 0;
	istr.get(c);
	while (istr && ostr)
	{
		++len;
		ostr.put(c);
		istr.get(c);
	}
	return len;
}


} // namespace DBPoco
