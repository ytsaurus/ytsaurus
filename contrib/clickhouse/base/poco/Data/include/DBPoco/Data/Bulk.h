//
// BulkExtraction.h
//
// Library: Data
// Package: DataCore
// Module:  Bulk
//
// Definition of the BulkExtraction class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Data_Bulk_INCLUDED
#define DB_Data_Bulk_INCLUDED


#include "DBPoco/Data/Limit.h"
#include "DBPoco/Void.h"


namespace DBPoco
{
namespace Data
{


    class Data_API Bulk
    {
    public:
        Bulk(const Limit & limit);
        /// Creates the Bulk.

        Bulk(DBPoco::UInt32 value);
        /// Creates the Bulk.

        ~Bulk();
        /// Destroys the bulk.

        const Limit & limit() const;
        /// Returns the limit associated with this bulk object.

        DBPoco::UInt32 size() const;
        /// Returns the value of the limit associated with
        /// this bulk object.

    private:
        Bulk();

        Limit _limit;
    };


    ///
    /// inlines
    ///
    inline const Limit & Bulk::limit() const
    {
        return _limit;
    }


    inline DBPoco::UInt32 Bulk::size() const
    {
        return _limit.value();
    }


    namespace Keywords
    {


        inline Bulk bulk(const Limit & limit = Limit(Limit::LIMIT_UNLIMITED, false, false))
        /// Convenience function for creation of bulk.
        {
            return Bulk(limit);
        }


        inline void bulk(Void)
        /// Dummy bulk function. Used for bulk binding creation
        /// (see BulkBinding) and bulk extraction signalling to Statement.
        {
        }


    } // namespace Keywords


    typedef void (*BulkFnType)(Void);


}
} // namespace DBPoco::Data


#endif // DB_Data_Bulk_INCLUDED
