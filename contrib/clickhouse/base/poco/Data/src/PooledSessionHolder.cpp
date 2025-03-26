//
// PooledSessionHolder.cpp
//
// Library: Data
// Package: SessionPooling
// Module:  PooledSessionHolder
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Data/PooledSessionHolder.h"


namespace DBPoco {
namespace Data {


PooledSessionHolder::PooledSessionHolder(SessionPool& owner, SessionImpl* pSessionImpl):
	_owner(owner),
	_pImpl(pSessionImpl, true)
{
}


PooledSessionHolder::~PooledSessionHolder()
{
}


} } // namespace DBPoco::Data
