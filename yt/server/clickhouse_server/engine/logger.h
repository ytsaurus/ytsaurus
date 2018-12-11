#pragma once

#include "clickhouse.h"

#include <yt/server/clickhouse_server/native/public.h>

//#include <Poco/AutoPtr.h>
//#include <Poco/Logger.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Channel> WrapToLogChannel(NNative::ILoggerPtr logger);

} // namespace NYT::NClickHouseServer::NEngine
