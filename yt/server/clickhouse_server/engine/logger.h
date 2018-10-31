#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <Poco/AutoPtr.h>
#include <Poco/Logger.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Channel> WrapToLogChannel(NNative::ILoggerPtr logger);

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
