#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContext
{
public:
    // TODO(pogorelov): Accept cpp types, not proto.
    TSchedulingContext(
        const NScheduler::NProto::TScheduleAllocationRequest* request,
        const NScheduler::TExecNodeDescriptorPtr& nodeDescriptor,
        const NScheduler::NProto::TScheduleAllocationSpec& scheduleAllocationSpec);

    const std::optional<TString>& GetPoolPath() const;

    const NScheduler::TExecNodeDescriptorPtr& GetNodeDescriptor() const;

    const NScheduler::TDiskResources& DiskResources() const;

    TAllocationId GetAllocationId() const;

    NProfiling::TCpuInstant GetNow() const;

    const NScheduler::NProto::TScheduleAllocationSpec& GetScheduleAllocationSpec() const;

private:
    const NScheduler::TDiskResources DiskResources_;
    const TAllocationId AllocationId_;
    const NScheduler::TExecNodeDescriptorPtr NodeDescriptor_;
    const NScheduler::NProto::TScheduleAllocationSpec ScheduleAllocationSpec_;
    const std::optional<TString> PoolPath_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
