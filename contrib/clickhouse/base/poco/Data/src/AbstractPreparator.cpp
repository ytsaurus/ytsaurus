//
// AbstractPreparator.cpp
//
// Library: Data
// Package: DataCore
// Module:  AbstractPreparator
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Data/AbstractPreparator.h"


namespace DBPoco {
namespace Data {


AbstractPreparator::AbstractPreparator(DBPoco::UInt32 length): 
	_length(length),
	_bulk(false)
{
}


AbstractPreparator::~AbstractPreparator()
{
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DBPoco::Int8>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DBPoco::Int8>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DBPoco::Int8>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DBPoco::UInt8>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DBPoco::UInt8>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DBPoco::UInt8>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DBPoco::Int16>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DBPoco::Int16>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DBPoco::Int16>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DBPoco::UInt16>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DBPoco::UInt16>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DBPoco::UInt16>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DBPoco::Int32>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DBPoco::Int32>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DBPoco::Int32>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DBPoco::UInt32>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DBPoco::UInt32>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DBPoco::UInt32>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DBPoco::Int64>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DBPoco::Int64>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DBPoco::Int64>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DBPoco::UInt64>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DBPoco::UInt64>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DBPoco::UInt64>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


#ifndef DB_POCO_LONG_IS_64_BIT
void AbstractPreparator::prepare(std::size_t pos, const std::vector<long>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<long>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<long>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}
#endif


void AbstractPreparator::prepare(std::size_t pos, const std::vector<bool>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<bool>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<bool>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<float>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<float>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<float>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<double>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<double>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<double>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<char>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<char>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<char>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<std::string>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<std::string>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<std::string>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const UTF16String& val)
{
	throw NotImplementedException("UTF16String preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<UTF16String>& val)
{
	throw NotImplementedException("std::vector<UTF16String> preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<UTF16String>& val)
{
	throw NotImplementedException("std::deque<UTF16String> preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<UTF16String>& val)
{
	throw NotImplementedException("std::list<UTF16String> preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<BLOB>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<BLOB>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<BLOB>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CLOB>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CLOB>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CLOB>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DateTime>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DateTime>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DateTime>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<Date>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<Date>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<Date>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<Time>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<Time>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<Time>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<Any>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<Any>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<Any>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DBPoco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DBPoco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DBPoco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


} } // namespace DBPoco::Data
