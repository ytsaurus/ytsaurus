//
// LogFile.cpp
//
// Library: Foundation
// Package: Logging
// Module:  LogFile
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/LogFile.h"


#include "LogFile_STD.cpp"


namespace DBPoco {


LogFile::LogFile(const std::string& path): LogFileImpl(path)
{
}


LogFile::~LogFile()
{
}


} // namespace DBPoco
