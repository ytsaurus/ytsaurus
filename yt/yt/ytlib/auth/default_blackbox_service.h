#pragma once

#include "public.h"

#include <yt/yt/core/profiling/profiler.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

IBlackboxServicePtr CreateDefaultBlackboxService(
    TDefaultBlackboxServiceConfigPtr config,
    ITvmServicePtr tvmService,
    NConcurrency::IPollerPtr poller,
    NProfiling::TRegistry profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
