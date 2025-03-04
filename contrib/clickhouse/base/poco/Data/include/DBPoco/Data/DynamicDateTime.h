//
// DynamicDateTime.h
//
// Library: Data
// Package: DataCore
// Module:  DynamicDateTime
//
// Definition of the Date and Time cast operators for DBPoco::Dynamic::Var.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Data_DynamicDateTime_INCLUDED
#define DB_Data_DynamicDateTime_INCLUDED


#include "DBPoco/Data/Data.h"
#include "DBPoco/Data/Date.h"
#include "DBPoco/Data/Time.h"
#include "DBPoco/Dynamic/Var.h"


namespace DBPoco
{
namespace Data
{

    class Date;
    class Time;

}
} // namespace DBPoco::Data


namespace DBPoco
{
namespace Dynamic
{


    template <>
    Data_API Var::operator DBPoco::Data::Date() const;
    template <>
    Data_API Var::operator DBPoco::Data::Time() const;


}
} // namespace DBPoco::Dynamic


#endif // DB_Data_DynamicDateTime_INCLUDED
