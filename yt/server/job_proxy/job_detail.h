#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/chunk_client/public.h>

#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

//! Base class for all jobs inside job proxy.
class TJob
    : public IJob
{
public:
    explicit TJob(IJobHost* host);

protected:
    TWeakPtr<IJobHost> Host;

    TInstant StartTime;

    TDuration GetElapsedTime() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
