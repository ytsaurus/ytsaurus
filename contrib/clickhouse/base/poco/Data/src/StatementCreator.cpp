//
// StatementCreator.cpp
//
// Library: Data
// Package: DataCore
// Module:  StatementCreator
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Data/StatementCreator.h"
#include <algorithm>


namespace DBPoco {
namespace Data {


StatementCreator::StatementCreator()
{
}


StatementCreator::StatementCreator(DBPoco::AutoPtr<SessionImpl> ptrImpl):
	_ptrImpl(ptrImpl)
{
}


StatementCreator::StatementCreator(const StatementCreator& other):
	_ptrImpl(other._ptrImpl)
{
}


StatementCreator& StatementCreator::operator = (const StatementCreator& other)
{
	StatementCreator tmp(other);
	swap(tmp);
	return *this;
}


void StatementCreator::swap(StatementCreator& other)
{
	using std::swap;
	swap(_ptrImpl, other._ptrImpl);
}


StatementCreator::~StatementCreator()
{
}


} } // namespace DBPoco::Data
