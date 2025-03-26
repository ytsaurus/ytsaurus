//
// PositionExtraction.h
//
// Library: Data
// Package: DataCore
// Module:  Position
//
// Definition of the PositionExtraction class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Data_Position_INCLUDED
#define DB_Data_Position_INCLUDED


#include "DBPoco/Data/Limit.h"


namespace DBPoco
{
namespace Data
{


    class Data_API Position
    /// Utility class wrapping unsigned integer. Used to
    /// indicate the recordset position in batch SQL statements.
    {
    public:
        Position(DBPoco::UInt32 value);
        /// Creates the Position.

        ~Position();
        /// Destroys the Position.

        DBPoco::UInt32 value() const;
        /// Returns the position value.

    private:
        Position();

        DBPoco::UInt32 _value;
    };


    ///
    /// inlines
    ///
    inline DBPoco::UInt32 Position::value() const
    {
        return _value;
    }


    namespace Keywords
    {


        template <typename T>
        inline Position from(const T & value)
        /// Convenience function for creation of position.
        {
            return Position(value);
        }


    } // namespace Keywords


}
} // namespace DBPoco::Data


#endif // DB_Data_Position_INCLUDED
