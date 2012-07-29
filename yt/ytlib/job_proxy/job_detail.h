#pragma once

#include "public.h"
#include "job.h"

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

    virtual NScheduler::NProto::TJobProgress GetProgress() const OVERRIDE;

protected:
    IJobHost* Host;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
