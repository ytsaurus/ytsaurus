#include "stdafx.h"
#include "job_detail.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(IJobHost* host)
    : Host(MakeWeak(host))
    , StartTime(TInstant::Now())
{
    YCHECK(host);
}

TDuration TJob::GetElapsedTime() const
{
    return TInstant::Now() - StartTime;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

