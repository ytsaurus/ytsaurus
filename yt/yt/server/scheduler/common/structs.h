#pragma once

#include <yt/yt/server/lib/scheduler/structs.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TBriefVanillaTaskSpec
{
    int JobCount;
};

using TBriefVanillaTaskSpecMap = THashMap<std::string, TBriefVanillaTaskSpec>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
