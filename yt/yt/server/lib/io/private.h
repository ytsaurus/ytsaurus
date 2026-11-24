#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <compare>
#include <optional>
#include <vector>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, IOLogger, "IO");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, StructuredIORawLogger, "IORaw");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, StructuredIOAggregateLogger, "IOAggregate");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, StructuredIOPathAggregateLogger, "IOPathAggr");

////////////////////////////////////////////////////////////////////////////////

struct TJobStateSnapshot
{
    int JobStateCount = 0;
    std::vector<TCpuDuration> QueuedPrivateJobWindows;
};

TJobStateSnapshot GetJobStateSnapshotForTesting(const IIOEnginePtr& engine);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
