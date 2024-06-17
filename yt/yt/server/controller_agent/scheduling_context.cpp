#include "scheduling_context.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <library/cpp/yt/cpu_clock/clock.h>

namespace NYT::NControllerAgent {

using namespace NScheduler;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TSchedulingContext::TSchedulingContext(
    const NScheduler::NProto::TScheduleAllocationRequest* request,
    const NScheduler::TExecNodeDescriptorPtr& nodeDescriptor,
    const NScheduler::NProto::TScheduleAllocationSpec& scheduleAllocationSpec)
    : DiskResources_(FromProto<NScheduler::TDiskResources>(request->node_disk_resources()))
    , AllocationId_(FromProto<TAllocationId>(request->allocation_id()))
    , NodeDescriptor_(nodeDescriptor)
    , ScheduleAllocationSpec_(scheduleAllocationSpec)
    , PoolPath_(YT_PROTO_OPTIONAL(*request, pool_path))
{ }

const std::optional<TString>& TSchedulingContext::GetPoolPath() const
{
    return PoolPath_;
}

const TExecNodeDescriptorPtr& TSchedulingContext::GetNodeDescriptor() const
{
    return NodeDescriptor_;
}

const TDiskResources& TSchedulingContext::DiskResources() const
{
    return DiskResources_;
}

TAllocationId TSchedulingContext::GetAllocationId() const
{
    return AllocationId_;
}

NProfiling::TCpuInstant TSchedulingContext::GetNow() const
{
    return NProfiling::GetCpuInstant();
}

const NScheduler::NProto::TScheduleAllocationSpec& TSchedulingContext::GetScheduleAllocationSpec() const
{
    return ScheduleAllocationSpec_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
