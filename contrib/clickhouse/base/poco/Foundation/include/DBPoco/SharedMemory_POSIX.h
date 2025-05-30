//
// SharedMemoryImpl.h
//
// Library: Foundation
// Package: Processes
// Module:  SharedMemoryImpl
//
// Definition of the SharedMemoryImpl class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_SharedMemoryImpl_INCLUDED
#define DB_Foundation_SharedMemoryImpl_INCLUDED


#include "DBPoco/Foundation.h"
#include "DBPoco/RefCountedObject.h"
#include "DBPoco/SharedMemory.h"


namespace DBPoco
{


class Foundation_API SharedMemoryImpl : public RefCountedObject
/// Shared memory implementation for POSIX platforms.
{
public:
    SharedMemoryImpl(const std::string & name, std::size_t size, SharedMemory::AccessMode mode, const void * addrHint, bool server);
    /// Creates or connects to a shared memory object with the given name.
    ///
    /// For maximum portability, name should be a valid Unix filename and not
    /// contain any slashes or backslashes.
    ///
    /// An address hint can be passed to the system, specifying the desired
    /// start address of the shared memory area. Whether the hint
    /// is actually honored is, however, up to the system. Windows platform
    /// will generally ignore the hint.
    ///
    /// If server is set to false, the shared memory region will be unlinked
    /// by calling shm_unlink when the SharedMemory object is destroyed.

    SharedMemoryImpl(const DBPoco::File & file, SharedMemory::AccessMode mode, const void * addrHint);
    /// Maps the entire contents of file into a shared memory segment.
    ///
    /// An address hint can be passed to the system, specifying the desired
    /// start address of the shared memory area. Whether the hint
    /// is actually honored is, however, up to the system. Windows platform
    /// will generally ignore the hint.

    char * begin() const;
    /// Returns the start address of the shared memory segment.

    char * end() const;
    /// Returns the one-past-end end address of the shared memory segment.

protected:
    void map(const void * addrHint);
    /// Maps the shared memory object.

    void unmap();
    /// Unmaps the shared memory object.

    void close();
    /// Releases the handle for the shared memory segment.

    ~SharedMemoryImpl();
    /// Destroys the SharedMemoryImpl.

private:
    SharedMemoryImpl();
    SharedMemoryImpl(const SharedMemoryImpl &);
    SharedMemoryImpl & operator=(const SharedMemoryImpl &);

    std::size_t _size;
    int _fd;
    char * _address;
    SharedMemory::AccessMode _access;
    std::string _name;
    bool _fileMapped;
    bool _server;
};


//
// inlines
//
inline char * SharedMemoryImpl::begin() const
{
    return _address;
}


inline char * SharedMemoryImpl::end() const
{
    return _address + _size;
}


} // namespace DBPoco


#endif // DB_Foundation_SharedMemoryImpl_INCLUDED
