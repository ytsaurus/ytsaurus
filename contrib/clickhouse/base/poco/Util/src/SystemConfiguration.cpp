//
// SystemConfiguration.cpp
//
// Library: Util
// Package: Configuration
// Module:  SystemConfiguration
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/SystemConfiguration.h"
#include "DBPoco/Environment.h"
#include "DBPoco/Path.h"
#include "DBPoco/DateTime.h"
#include "DBPoco/DateTimeFormatter.h"
#include "DBPoco/DateTimeFormat.h"
#include "DBPoco/NumberFormatter.h"
#include "DBPoco/Process.h"
#include "DBPoco/Exception.h"
#include <cstdio>


using DBPoco::Environment;
using DBPoco::Path;


namespace DBPoco {
namespace Util {


const std::string SystemConfiguration::OSNAME         = "system.osName";
const std::string SystemConfiguration::OSVERSION      = "system.osVersion";
const std::string SystemConfiguration::OSARCHITECTURE = "system.osArchitecture";
const std::string SystemConfiguration::NODENAME       = "system.nodeName";
const std::string SystemConfiguration::NODEID         = "system.nodeId";
const std::string SystemConfiguration::CURRENTDIR     = "system.currentDir";
const std::string SystemConfiguration::HOMEDIR        = "system.homeDir";
const std::string SystemConfiguration::CONFIGHOMEDIR  = "system.configHomeDir";
const std::string SystemConfiguration::CACHEHOMEDIR   = "system.cacheHomeDir";
const std::string SystemConfiguration::DATAHOMEDIR    = "system.dataHomeDir";
const std::string SystemConfiguration::TEMPHOMEDIR    = "system.tempHomeDir";
const std::string SystemConfiguration::TEMPDIR        = "system.tempDir";
const std::string SystemConfiguration::CONFIGDIR      = "system.configDir";
const std::string SystemConfiguration::DATETIME       = "system.dateTime";
const std::string SystemConfiguration::PID            = "system.pid";
const std::string SystemConfiguration::ENV            = "system.env.";


SystemConfiguration::SystemConfiguration()
{
}


SystemConfiguration::~SystemConfiguration()
{
}


bool SystemConfiguration::getRaw(const std::string& key, std::string& value) const
{
	if (key == OSNAME)
	{
		value = Environment::osName();
	}
	else if (key == OSVERSION)
	{
		value = Environment::osVersion();
	}
	else if (key == OSARCHITECTURE)
	{
		value = Environment::osArchitecture();
	}
	else if (key == NODENAME)
	{
		value = Environment::nodeName();
	}
	else if (key == NODEID)
	{
		try
		{
			DBPoco::Environment::NodeId id;
			DBPoco::Environment::nodeId(id);
			char result[13];
			std::snprintf(result, 
                sizeof(result),
                "%02x%02x%02x%02x%02x%02x",
				id[0],
				id[1],
				id[2],
				id[3],
				id[4],
				id[5]);
			value = result;
		}
		catch (...)
		{
			value = "000000000000";
		}
	}
	else if (key == CURRENTDIR)
	{
		value = Path::current();
	}
	else if (key == HOMEDIR)
	{
		value = Path::home();
	}
	else if (key == CONFIGHOMEDIR)
	{
		value = Path::configHome();
	}
	else if (key == CACHEHOMEDIR)
	{
		value = Path::cacheHome();
	}
	else if (key == DATAHOMEDIR)
	{
		value = Path::dataHome();
	}

	else if (key == TEMPHOMEDIR)
	{
		value = Path::tempHome();
	}
	else if (key == TEMPDIR)
	{
		value = Path::temp();
	}
	else if (key == CONFIGDIR)
	{
		value = Path::config();
	}
	else if (key == DATETIME)
	{
		value = DBPoco::DateTimeFormatter::format(DBPoco::DateTime(), DBPoco::DateTimeFormat::ISO8601_FORMAT);
	}
	else if (key == PID)
	{
		value = "0";
		value = DBPoco::NumberFormatter::format(DBPoco::Process::id());
	}
	else if (key.compare(0, ENV.size(), ENV) == 0)
	{
		return getEnv(key.substr(ENV.size()), value);
	}
	else return false;
	return true;
}


void SystemConfiguration::setRaw(const std::string& key, const std::string& value)
{
	throw DBPoco::InvalidAccessException("Attempt to modify a system property", key);
}


void SystemConfiguration::enumerate(const std::string& key, Keys& range) const
{
	if (key.empty())
	{
		range.push_back("system");
	}
	else if (key == "system")
	{
		range.push_back("osName");
		range.push_back("osVersion");
		range.push_back("osArchitecture");
		range.push_back("nodeName");
		range.push_back("nodeId");
		range.push_back("currentDir");
		range.push_back("homeDir");
		range.push_back("configHomeDir");
		range.push_back("cacheHomeDir");
		range.push_back("dataHomeDir");
		range.push_back("tempHomeDir");
		range.push_back("tempDir");
		range.push_back("configDir");
		range.push_back("dateTime");
		range.push_back("pid");
		range.push_back("env");
	}
}


void SystemConfiguration::removeRaw(const std::string& key)
{
	throw DBPoco::NotImplementedException("Removing a key in a SystemConfiguration");
}


bool SystemConfiguration::getEnv(const std::string& name, std::string& value)
{
	if (Environment::has(name))
	{
		value = Environment::get(name);
		return true;
	}
	return false;
}


} } // namespace DBPoco::Util
