//
// SharedLibrary_UNIX.cpp
//
// Library: Foundation
// Package: SharedLibrary
// Module:  SharedLibrary
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/SharedLibrary_UNIX.h"
#include "DBPoco/Exception.h"
#include <dlfcn.h>


// Note: cygwin is missing RTLD_LOCAL, set it to 0


namespace DBPoco {


FastMutex SharedLibraryImpl::_mutex;


SharedLibraryImpl::SharedLibraryImpl()
{
	_handle = 0;
}


SharedLibraryImpl::~SharedLibraryImpl()
{
}


void SharedLibraryImpl::loadImpl(const std::string& path, int flags)
{
	FastMutex::ScopedLock lock(_mutex);

	if (_handle) throw LibraryAlreadyLoadedException(path);
	int realFlags = RTLD_LAZY;
	if (flags & SHLIB_LOCAL_IMPL)
		realFlags |= RTLD_LOCAL;
	else
		realFlags |= RTLD_GLOBAL;
	_handle = dlopen(path.c_str(), realFlags);
	if (!_handle)
	{
		const char* err = dlerror();
		throw LibraryLoadException(err ? std::string(err) : path);
	}
	_path = path;
}


void SharedLibraryImpl::unloadImpl()
{
	FastMutex::ScopedLock lock(_mutex);

	if (_handle)
	{
		dlclose(_handle);
		_handle = 0;
	}
}


bool SharedLibraryImpl::isLoadedImpl() const
{
	return _handle != 0;
}


void* SharedLibraryImpl::findSymbolImpl(const std::string& name)
{
	FastMutex::ScopedLock lock(_mutex);

	void* result = 0;
	if (_handle)
	{
		result = dlsym(_handle, name.c_str());
	}
	return result;
}


const std::string& SharedLibraryImpl::getPathImpl() const
{
	return _path;
}


std::string SharedLibraryImpl::suffixImpl()
{
#if defined(__APPLE__)
	#if defined(_DEBUG) && !defined(DB_POCO_NO_SHARED_LIBRARY_DEBUG_SUFFIX)
		return "d.dylib";
	#else
		return ".dylib";
	#endif
#elif defined(hpux) || defined(_hpux)
	#if defined(_DEBUG) && !defined(DB_POCO_NO_SHARED_LIBRARY_DEBUG_SUFFIX)
		return "d.sl";
	#else
		return ".sl";
	#endif
#else
	#if defined(_DEBUG) && !defined(DB_POCO_NO_SHARED_LIBRARY_DEBUG_SUFFIX)
		return "d.so";
	#else
		return ".so";
	#endif
#endif
}


bool SharedLibraryImpl::setSearchPathImpl(const std::string&)
{
	return false;
}


} // namespace DBPoco
