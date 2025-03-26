//
// Date.cpp
//
// Library: Data
// Package: DataCore
// Module:  Date
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Data/Date.h"
#include "DBPoco/DateTime.h"
#include "DBPoco/NumberFormatter.h"
#include "DBPoco/Data/DynamicDateTime.h"
#include "DBPoco/Dynamic/Var.h"


using DBPoco::DateTime;
using DBPoco::Dynamic::Var;
using DBPoco::NumberFormatter;


namespace DBPoco {
namespace Data {


Date::Date()
{
	DateTime dt;
	assign(dt.year(), dt.month(), dt.day());
}


Date::Date(int year, int month, int day)
{
	assign(year, month, day);
}


Date::Date(const DateTime& dt)
{
	assign(dt.year(), dt.month(), dt.day());
}


Date::~Date()
{
}


void Date::assign(int year, int month, int day)
{
	if (year < 0 || year > 9999)
		throw InvalidArgumentException("Year must be between 0 and 9999");

	if (month < 1 || month > 12)
		throw InvalidArgumentException("Month must be between 1 and 12");

	if (day < 1 || day > DateTime::daysOfMonth(year, month))
		throw InvalidArgumentException("Month must be between 1 and " + 
			NumberFormatter::format(DateTime::daysOfMonth(year, month)));

	_year = year;
	_month = month;
	_day = day;
}


bool Date::operator < (const Date& date) const
{
	int year = date.year();

	if (_year < year) return true;
	else if (_year > year) return false;
	else // years equal
	{
		int month = date.month();
		if (_month < month) return true;
		else 
		if (_month > month) return false;
		else // months equal
		if (_day < date.day()) return true;
	}

	return false;
}


Date& Date::operator = (const Var& var)
{
#ifndef __GNUC__
// g++ used to choke on this, newer versions seem to digest it fine
// TODO: determine the version able to handle it properly
	*this = var.extract<Date>();
#else
	*this = var.operator Date();
#endif
	return *this;
}


} } // namespace DBPoco::Data


#ifdef __GNUC__
// only needed for g++ (see comment in Date::operator = above)

namespace DBPoco {
namespace Dynamic {


using DBPoco::Data::Date;
using DBPoco::DateTime;


template <>
Var::operator Date () const
{
	VarHolder* pHolder = content();

	if (!pHolder)
		throw InvalidAccessException("Can not convert empty value.");

	if (typeid(Date) == pHolder->type())
		return extract<Date>();
	else
	{
		DBPoco::DateTime result;
		pHolder->convert(result);
		return Date(result);
	}
}


} } // namespace DBPoco::Dynamic


#endif // __GNUC__
