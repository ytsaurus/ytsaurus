#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <optional>
#include <string>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

struct TContext
    : public TRefCounted
{
    NApi::IClientPtr Client;
    NYPath::TRichYPath PipelinePath;
    TLoadThroughputThrottlerPtr LoadThroughputThrottler;
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;

    // Throttling/profiling tag used by the table. Defaults to the table name when unset.
    std::optional<std::string> Tag;

    TContextPtr WithTableName(TStringBuf name) const;
};

DEFINE_REFCOUNTED_TYPE(TContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
