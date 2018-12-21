#pragma once

#include "public.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TJobToRelease
{
    TJobId JobId;
    bool ArchiveJobSpec = false;
    bool ArchiveStderr = false;
    bool ArchiveFailContext = false;
    bool ArchiveProfile = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
