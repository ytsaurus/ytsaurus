//
// RandomStream.cpp
//
// Library: Foundation
// Package: Crypt
// Module:  RandomStream
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/RandomStream.h"
#include "DBPoco/Random.h"
#include "DBPoco/SHA1Engine.h"
#if   defined(DB_POCO_OS_FAMILY_UNIX)
#include <fcntl.h>
#include <unistd.h>
#endif
#include <ctime>


namespace DBPoco {


RandomBuf::RandomBuf(): BufferedStreamBuf(256, std::ios::in)
{
}


RandomBuf::~RandomBuf()
{
}


int RandomBuf::readFromDevice(char* buffer, std::streamsize length)
{
	int n = 0;

	#if defined(DB_POCO_OS_FAMILY_UNIX)
	int fd = open("/dev/urandom", O_RDONLY, 0);
	if (fd >= 0) 
	{
		n = read(fd, buffer, length);
		close(fd);
	}
	#endif
	if (n <= 0)
	{
		// x is here as a source of randomness, so it does not make
		// much sense to protect it with a Mutex.
		static UInt32 x = 0;
		Random rnd1(256);
		Random rnd2(64);
		x += rnd1.next();
 
		n = 0;
		SHA1Engine engine;
		UInt32 t = (UInt32) std::time(NULL);
		engine.update(&t, sizeof(t));
		void* p = this;
		engine.update(&p, sizeof(p));
		engine.update(buffer, length);
		UInt32 junk[32];
		engine.update(junk, sizeof(junk));
		while (n < length)
		{
			for (int i = 0; i < 100; ++i)
			{
				UInt32 r = rnd2.next();
				engine.update(&r, sizeof(r));
				engine.update(&x, sizeof(x));
				x += rnd1.next();
			}
			DigestEngine::Digest d = engine.digest();
			for (DigestEngine::Digest::const_iterator it = d.begin(); it != d.end() && n < length; ++it, ++n)
			{
				engine.update(*it);
				*buffer++ = *it++;
			}
		}
	}
	return n;
}


RandomIOS::RandomIOS()
{
	DB_poco_ios_init(&_buf);
}


RandomIOS::~RandomIOS()
{
}


RandomBuf* RandomIOS::rdbuf()
{
	return &_buf;
}


RandomInputStream::RandomInputStream(): std::istream(&_buf)
{
}


RandomInputStream::~RandomInputStream()
{
}


} // namespace DBPoco
