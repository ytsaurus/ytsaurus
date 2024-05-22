#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, TableServerLogger, "TableServer");
inline const NProfiling::TProfiler TableServerProfiler("/table_server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

