//
// LocalDateTime.cpp
//
// Library: Foundation
// Package: DateTime
// Module:  LocalDateTime
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/LocalDateTime.h"
#include "DBPoco/Timezone.h"
#include "DBPoco/Timespan.h"
#include "DBPoco/Exception.h"
#include <algorithm>
#include <ctime>


namespace DBPoco {


LocalDateTime::LocalDateTime()
{
	determineTzd(true);
}


LocalDateTime::LocalDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond, int microsecond):
	_dateTime(year, month, day, hour, minute, second, millisecond, microsecond)
{
	determineTzd();
}


LocalDateTime::LocalDateTime(int tzd, int year, int month, int day, int hour, int minute, int second, int millisecond, int microsecond):
	_dateTime(year, month, day, hour, minute, second, millisecond, microsecond),
	_tzd(tzd)
{
}


LocalDateTime::LocalDateTime(double julianDay):
	_dateTime(julianDay)
{
	determineTzd(true);
}


LocalDateTime::LocalDateTime(int tzd, double julianDay):
	_dateTime(julianDay),
	_tzd(tzd)
{
	adjustForTzd();
}


LocalDateTime::LocalDateTime(const DateTime& dateTime):
	_dateTime(dateTime)
{
	determineTzd(true);
}


LocalDateTime::LocalDateTime(int tzd, const DateTime& dateTime):
	_dateTime(dateTime),
	_tzd(tzd)
{
	adjustForTzd();
}


LocalDateTime::LocalDateTime(int tzd, const DateTime& dateTime, bool adjust):
	_dateTime(dateTime),
	_tzd(tzd)
{
	if (adjust)
		adjustForTzd();
}


LocalDateTime::LocalDateTime(const LocalDateTime& dateTime):
	_dateTime(dateTime._dateTime),
	_tzd(dateTime._tzd)
{
}


LocalDateTime::LocalDateTime(Timestamp::UtcTimeVal utcTime, Timestamp::TimeDiff diff, int tzd):
	_dateTime(utcTime, diff),
	_tzd(tzd)
{
	adjustForTzd();
}

	
LocalDateTime::~LocalDateTime()
{
}


LocalDateTime& LocalDateTime::operator = (const LocalDateTime& dateTime)
{
	if (&dateTime != this)
	{
		_dateTime = dateTime._dateTime;
		_tzd      = dateTime._tzd;
	}
	return *this;
}


LocalDateTime& LocalDateTime::operator = (const Timestamp& timestamp)
{
	if (timestamp != this->timestamp())
	{
		_dateTime = timestamp;
		determineTzd(true);
	}
	return *this;
}


LocalDateTime& LocalDateTime::operator = (double julianDay)
{
	_dateTime = julianDay;
	determineTzd(true);
	return *this;
}


LocalDateTime& LocalDateTime::assign(int year, int month, int day, int hour, int minute, int second, int millisecond, int microseconds)
{
	_dateTime.assign(year, month, day, hour, minute, second, millisecond, microseconds);
	determineTzd(false);
	return *this;
}


LocalDateTime& LocalDateTime::assign(int tzd, int year, int month, int day, int hour, int minute, int second, int millisecond, int microseconds)
{
	_dateTime.assign(year, month, day, hour, minute, second, millisecond, microseconds);
	_tzd = tzd;
	return *this;
}


LocalDateTime& LocalDateTime::assign(int tzd, double julianDay)
{
	_tzd      = tzd;
	_dateTime = julianDay;
	adjustForTzd();
	return *this;
}


void LocalDateTime::swap(LocalDateTime& dateTime)
{
	_dateTime.swap(dateTime._dateTime);
	std::swap(_tzd, dateTime._tzd);
}


DateTime LocalDateTime::utc() const
{
	return DateTime(_dateTime.utcTime(), -((Timestamp::TimeDiff) _tzd)*Timespan::SECONDS);
}


bool LocalDateTime::operator == (const LocalDateTime& dateTime) const
{
	return utcTime() == dateTime.utcTime();
}


bool LocalDateTime::operator != (const LocalDateTime& dateTime) const	
{
	return utcTime() != dateTime.utcTime();
}


bool LocalDateTime::operator <  (const LocalDateTime& dateTime) const	
{
	return utcTime() < dateTime.utcTime();
}


bool LocalDateTime::operator <= (const LocalDateTime& dateTime) const	
{
	return utcTime() <= dateTime.utcTime();
}


bool LocalDateTime::operator >  (const LocalDateTime& dateTime) const	
{
	return utcTime() > dateTime.utcTime();
}


bool LocalDateTime::operator >= (const LocalDateTime& dateTime) const	
{
	return utcTime() >= dateTime.utcTime();
}


LocalDateTime LocalDateTime::operator + (const Timespan& span) const
{
	// First calculate the adjusted UTC time, then calculate the
	// locally adjusted time by constructing a new LocalDateTime.
	DateTime tmp(utcTime(), span.totalMicroseconds());
	return LocalDateTime(tmp);
}


LocalDateTime LocalDateTime::operator - (const Timespan& span) const
{
	// First calculate the adjusted UTC time, then calculate the
	// locally adjusted time by constructing a new LocalDateTime.
	DateTime tmp(utcTime(), -span.totalMicroseconds());
	return LocalDateTime(tmp);
}


Timespan LocalDateTime::operator - (const LocalDateTime& dateTime) const
{
	return Timespan((utcTime() - dateTime.utcTime())/10);
}


LocalDateTime& LocalDateTime::operator += (const Timespan& span)
{
	// Use the same trick as in operator+. Create a UTC time, adjust
	// it for the span, and convert back to LocalDateTime. This will
	// recalculate the tzd correctly in the case where the addition
	// crosses a DST boundary.
	*this = DateTime(utcTime(), span.totalMicroseconds());
	return *this;
}


LocalDateTime& LocalDateTime::operator -= (const Timespan& span)
{
	// Use the same trick as in operator-. Create a UTC time, adjust
	// it for the span, and convert back to LocalDateTime. This will
	// recalculate the tzd correctly in the case where the subtraction
	// crosses a DST boundary.
	*this = DateTime(utcTime(), -span.totalMicroseconds());
	return *this;
}


void LocalDateTime::determineTzd(bool adjust)
{
	if (adjust)
	{
		std::time_t epochTime = _dateTime.timestamp().epochTime();
#if defined(_WIN32) || defined(POCO_NO_POSIX_TSF)
		std::tm* broken = std::localtime(&epochTime);
		if (!broken) throw DBPoco::SystemException("cannot get local time");
		_tzd = (Timezone::utcOffset() + ((broken->tm_isdst == 1) ? 3600 : 0));
#else
		std::tm broken;
		if (!localtime_r(&epochTime, &broken))
			throw DBPoco::SystemException("cannot get local time");
		_tzd = (Timezone::utcOffset() + ((broken.tm_isdst == 1) ? 3600 : 0));
#endif
		adjustForTzd();
	}
	else
	{
		int dst;
		dstOffset(dst);
		_tzd = (Timezone::utcOffset() + dst);
	}
}


std::time_t LocalDateTime::dstOffset(int& dstOffset) const
{
	std::time_t local;
	std::tm     broken;

	broken.tm_year  = (_dateTime.year() - 1900);
	broken.tm_mon   = (_dateTime.month() - 1);
	broken.tm_mday  = _dateTime.day();
	broken.tm_hour  = _dateTime.hour();
	broken.tm_min   = _dateTime.minute();
	broken.tm_sec   = _dateTime.second();
	broken.tm_isdst = -1;
	local = std::mktime(&broken);
	
	dstOffset = (broken.tm_isdst == 1) ? 3600 : 0;
	return local;
}


} // namespace DBPoco

