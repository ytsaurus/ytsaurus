#pragma once

#include "private.h"

#include <yt/yt/core/logging/log.h>

#include <DBPoco/AutoPtr.h>
#include <DBPoco/Logger.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DBPoco::AutoPtr<DBPoco::Channel> CreateLogChannel(const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
