#pragma once

#include "public.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TJobToRelease
{
    TJobId JobId;
    bool ArchiveJobSpec = false;
    bool ArchiveStderr = false;
    bool ArchiveFailContext = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
