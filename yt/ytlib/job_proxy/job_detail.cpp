#include "stdafx.h"
#include "job_detail.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(IJobHost* host)
    : Host(host)
{
    YCHECK(host);
}

NScheduler::NProto::TJobProgress TJob::GetProgress() const
{
    return NScheduler::NProto::TJobProgress();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

