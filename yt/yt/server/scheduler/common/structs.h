#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/structs.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Information retrieved during scheduler-master handshake.
struct TMasterHandshakeResult
{
    std::vector<TOperationPtr> Operations;
    TInstant LastMeteringLogTime;
};

////////////////////////////////////////////////////////////////////////////////

struct TBriefVanillaTaskSpec
{
    int JobCount;
};

using TBriefVanillaTaskSpecMap = THashMap<std::string, TBriefVanillaTaskSpec>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
