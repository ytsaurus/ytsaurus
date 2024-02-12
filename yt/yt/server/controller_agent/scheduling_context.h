#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct ISchedulingContext
{
    virtual ~ISchedulingContext() = default;

    virtual const NScheduler::TExecNodeDescriptorPtr& GetNodeDescriptor() const = 0;
    virtual const NScheduler::TDiskResources& DiskResources() const = 0;
    virtual const NScheduler::NProto::TScheduleJobSpec& GetScheduleJobSpec() const = 0;
    virtual const std::optional<TString>& GetPoolPath() const = 0;

    virtual TJobId GetJobId() const = 0;
    virtual NProfiling::TCpuInstant GetNow() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
