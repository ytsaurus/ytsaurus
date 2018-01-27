#pragma once

#include "public.h"

#include <yt/server/scheduler/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct ISchedulingContext
{
    virtual ~ISchedulingContext() = default;

    virtual const NScheduler::TExecNodeDescriptor& GetNodeDescriptor() const = 0;
    virtual const TJobResources& ResourceLimits() const = 0;
    virtual const NNodeTrackerClient::NProto::TDiskResources& DiskInfo() const = 0;

    virtual TJobId GetJobId() const = 0;
    virtual NProfiling::TCpuInstant GetNow() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
