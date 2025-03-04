//
// StatementCreator.h
//
// Library: Data
// Package: DataCore
// Module:  StatementCreator
//
// Definition of the StatementCreator class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Data_StatementCreator_INCLUDED
#define DB_Data_StatementCreator_INCLUDED


#include "DBPoco/AutoPtr.h"
#include "DBPoco/Data/Data.h"
#include "DBPoco/Data/SessionImpl.h"
#include "DBPoco/Data/Statement.h"


namespace DBPoco
{
namespace Data
{


    class Data_API StatementCreator
    /// A StatementCreator creates Statements.
    {
    public:
        StatementCreator();
        /// Creates an uninitialized StatementCreator.

        StatementCreator(DBPoco::AutoPtr<SessionImpl> ptrImpl);
        /// Creates a StatementCreator.

        StatementCreator(const StatementCreator & other);
        /// Creates a StatementCreator by copying another one.

        ~StatementCreator();
        /// Destroys the StatementCreator.

        StatementCreator & operator=(const StatementCreator & other);
        /// Assignment operator.

        void swap(StatementCreator & other);
        /// Swaps the StatementCreator with another one.

        template <typename T>
        Statement operator<<(const T & t)
        /// Creates a Statement.
        {
            if (!_ptrImpl->isConnected())
                throw NotConnectedException(_ptrImpl->connectionString());

            Statement stmt(_ptrImpl->createStatementImpl());
            stmt << t;
            return stmt;
        }

    private:
        DBPoco::AutoPtr<SessionImpl> _ptrImpl;
    };


}
} // namespace DBPoco::Data


#endif // DB_Data_StatementCreator_INCLUDED
