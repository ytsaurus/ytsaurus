//
// PooledSessionImpl.h
//
// Library: Data
// Package: SessionPooling
// Module:  PooledSessionImpl
//
// Definition of the PooledSessionImpl class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Data_PooledSessionImpl_INCLUDED
#define DB_Data_PooledSessionImpl_INCLUDED


#include "DBPoco/AutoPtr.h"
#include "DBPoco/Data/Data.h"
#include "DBPoco/Data/PooledSessionHolder.h"
#include "DBPoco/Data/SessionImpl.h"


namespace DBPoco
{
namespace Data
{


    class SessionPool;


    class Data_API PooledSessionImpl : public SessionImpl
    /// PooledSessionImpl is a decorator created by
    /// SessionPool that adds session pool
    /// management to SessionImpl objects.
    {
    public:
        PooledSessionImpl(PooledSessionHolder * pHolder);
        /// Creates the PooledSessionImpl.

        ~PooledSessionImpl();
        /// Destroys the PooledSessionImpl.

        // SessionImpl
        StatementImpl * createStatementImpl();
        void begin();
        void commit();
        void rollback();
        void open(const std::string & connect = "");
        void close();
        bool isConnected();
        void setConnectionTimeout(std::size_t timeout);
        std::size_t getConnectionTimeout();
        bool canTransact();
        bool isTransaction();
        void setTransactionIsolation(DBPoco::UInt32);
        DBPoco::UInt32 getTransactionIsolation();
        bool hasTransactionIsolation(DBPoco::UInt32);
        bool isTransactionIsolation(DBPoco::UInt32);
        const std::string & connectorName() const;
        void setFeature(const std::string & name, bool state);
        bool getFeature(const std::string & name);
        void setProperty(const std::string & name, const DBPoco::Any & value);
        DBPoco::Any getProperty(const std::string & name);

    protected:
        SessionImpl * access() const;
        /// Updates the last access timestamp,
        /// verifies validity of the session
        /// and returns the session if it is valid.
        ///
        /// Throws an SessionUnavailableException if the
        /// session is no longer valid.

        SessionImpl * impl() const;
        /// Returns a pointer to the SessionImpl.

    private:
        mutable DBPoco::AutoPtr<PooledSessionHolder> _pHolder;
    };


    //
    // inlines
    //
    inline SessionImpl * PooledSessionImpl::impl() const
    {
        return _pHolder->session();
    }


}
} // namespace DBPoco::Data


#endif // DB_Data_PooledSessionImpl_INCLUDED
