//
// Mutex_POSIX.h
//
// Library: Foundation
// Package: Threading
// Module:  Mutex
//
// Definition of the MutexImpl and FastMutexImpl classes for POSIX Threads.
//
// Copyright (c) 2004-2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Mutex_POSIX_INCLUDED
#define DB_Foundation_Mutex_POSIX_INCLUDED


#include <errno.h>
#include <pthread.h>
#include "DBPoco/Exception.h"
#include "DBPoco/Foundation.h"


namespace DBPoco
{


class Foundation_API MutexImpl
{
protected:
    MutexImpl();
    MutexImpl(bool fast);
    ~MutexImpl();
    void lockImpl();
    bool tryLockImpl();
    bool tryLockImpl(long milliseconds);
    void unlockImpl();

private:
    pthread_mutex_t _mutex;
};


class Foundation_API FastMutexImpl : public MutexImpl
{
protected:
    FastMutexImpl();
    ~FastMutexImpl();
};


//
// inlines
//
inline void MutexImpl::lockImpl()
{
    if (pthread_mutex_lock(&_mutex))
        throw SystemException("cannot lock mutex");
}


inline bool MutexImpl::tryLockImpl()
{
    int rc = pthread_mutex_trylock(&_mutex);
    if (rc == 0)
        return true;
    else if (rc == EBUSY)
        return false;
    else
        throw SystemException("cannot lock mutex");
}


inline void MutexImpl::unlockImpl()
{
    if (pthread_mutex_unlock(&_mutex))
        throw SystemException("cannot unlock mutex");
}


} // namespace DBPoco


#endif // DB_Foundation_Mutex_POSIX_INCLUDED
