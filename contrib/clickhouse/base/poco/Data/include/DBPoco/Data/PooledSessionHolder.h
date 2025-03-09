//
// PooledSessionHolder.h
//
// Library: Data
// Package: SessionPooling
// Module:  PooledSessionHolder
//
// Definition of the PooledSessionHolder class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Data_PooledSessionHolder_INCLUDED
#define DB_Data_PooledSessionHolder_INCLUDED


#include "DBPoco/AutoPtr.h"
#include "DBPoco/Data/Data.h"
#include "DBPoco/Data/SessionImpl.h"
#include "DBPoco/Mutex.h"
#include "DBPoco/Timestamp.h"


namespace DBPoco
{
namespace Data
{


    class SessionPool;


    class Data_API PooledSessionHolder : public DBPoco::RefCountedObject
    /// This class is used by SessionPool to manage SessionImpl objects.
    {
    public:
        PooledSessionHolder(SessionPool & owner, SessionImpl * pSessionImpl);
        /// Creates the PooledSessionHolder.

        ~PooledSessionHolder();
        /// Destroys the PooledSessionHolder.

        SessionImpl * session();
        /// Returns a pointer to the SessionImpl.

        SessionPool & owner();
        /// Returns a reference to the SessionHolder's owner.

        void access();
        /// Updates the last access timestamp.

        int idle() const;
        /// Returns the number of seconds the session has not been used.

    private:
        SessionPool & _owner;
        DBPoco::AutoPtr<SessionImpl> _pImpl;
        DBPoco::Timestamp _lastUsed;
        mutable DBPoco::FastMutex _mutex;
    };


    //
    // inlines
    //
    inline SessionImpl * PooledSessionHolder::session()
    {
        return _pImpl;
    }


    inline SessionPool & PooledSessionHolder::owner()
    {
        return _owner;
    }


    inline void PooledSessionHolder::access()
    {
        DBPoco::FastMutex::ScopedLock lock(_mutex);

        _lastUsed.update();
    }


    inline int PooledSessionHolder::idle() const
    {
        DBPoco::FastMutex::ScopedLock lock(_mutex);

        return (int)(_lastUsed.elapsed() / DBPoco::Timestamp::resolution());
    }


}
} // namespace DBPoco::Data


#endif // DB_Data_PooledSessionHolder_INCLUDED
