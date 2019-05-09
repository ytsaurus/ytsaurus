#pragma once

#include "private.h"

#include <yt/core/logging/log.h>

#include <Poco/AutoPtr.h>
#include <Poco/Logger.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Channel> CreateLogChannel(const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
