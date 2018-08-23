#pragma once

#include "public.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <yt/core/logging/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::ILoggerPtr CreateLogger(const NLogging::TLogger& logger);

}   // namespace NYT
}   // namespace NClickHouse
