#include "scheduling_context.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <library/cpp/yt/cpu_clock/clock.h>

namespace NYT::NControllerAgent {

using namespace NScheduler;
using namespace NControllers;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TSchedulingContext::TSchedulingContext(
    TAllocationId allocationId,
    TJobNodeDescriptor nodeDescriptor,
    std::optional<TString> poolPath)
    : AllocationId_(allocationId)
    , NodeDescriptor_(std::move(nodeDescriptor))
    , PoolPath_(std::move(poolPath))
{ }

const std::optional<TString>& TSchedulingContext::GetPoolPath() const
{
    return PoolPath_;
}

const TJobNodeDescriptor& TSchedulingContext::GetNodeDescriptor() const
{
    return NodeDescriptor_;
}

TAllocationId TSchedulingContext::GetAllocationId() const
{
    return AllocationId_;
}

NProfiling::TCpuInstant TSchedulingContext::GetNow() const
{
    return NProfiling::GetCpuInstant();
}

void TSchedulingContext::FormatCommonPart(TStringBuilderBase& builder) const
{
    builder.AppendFormat(
        "AllocationId: %v, NodeAddress: %v, PoolPath: %v",
        AllocationId_,
        NodeDescriptor_.Address,
        PoolPath_);
}

////////////////////////////////////////////////////////////////////////////////

TAllocationSchedulingContext::TAllocationSchedulingContext(
    TAllocationId allocationId,
    TJobResources resourceLimits,
    NScheduler::TDiskResources diskResources,
    TJobNodeDescriptor nodeDescriptor,
    std::optional<TString> poolPath,
    const NScheduler::NProto::TScheduleAllocationSpec& scheduleAllocationSpec)
    : TSchedulingContext(allocationId, std::move(nodeDescriptor), std::move(poolPath))
    , ResourceLimits_(resourceLimits)
    , DiskResources_(std::move(diskResources))
    , ScheduleAllocationSpec_(scheduleAllocationSpec)
{ }

bool TAllocationSchedulingContext::CanSatisfyDemand(const NScheduler::TJobResourcesWithQuota& demand) const
{
    return Dominates(ResourceLimits_, demand.ToJobResources()) &&
        CanSatisfyDiskQuotaRequests(DiskResources_, {demand.DiskQuota()});
}

const NScheduler::NProto::TScheduleAllocationSpec* TAllocationSchedulingContext::GetScheduleAllocationSpec() const
{
    return &ScheduleAllocationSpec_;
}

TString TAllocationSchedulingContext::ToString(const NChunkClient::TMediumDirectoryPtr& mediumDirectory) const
{
    TStringBuilder builder;
    builder.Reserve(256);

    builder.AppendChar('{');

    FormatCommonPart(builder);

    builder.AppendFormat(
        ", ResourceLimits: %v, DiskResources: %v",
        ResourceLimits_,
        NScheduler::ToString(DiskResources_, mediumDirectory));

    builder.AppendChar('}');

    return builder.Flush();
}

TString TAllocationSchedulingContext::GetResourcesString(const NChunkClient::TMediumDirectoryPtr& mediumDirectory) const
{
    TStringBuilder builder;
    builder.Reserve(128);

    builder.AppendChar('{');

    builder.AppendFormat(
        "ResourceLimits: %v, DiskResources: %v",
        ResourceLimits_,
        NScheduler::ToString(DiskResources_, mediumDirectory));

    builder.AppendChar('}');

    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

TJobSchedulingContext::TJobSchedulingContext(
    TAllocationId allocationId,
    const NScheduler::TJobResourcesWithQuota& resources,
    TJobNodeDescriptor nodeDescriptor,
    std::optional<TString> poolPath)
    : TSchedulingContext(allocationId, std::move(nodeDescriptor), std::move(poolPath))
    , Resources_(resources)
{ }

bool TJobSchedulingContext::CanSatisfyDemand(const NScheduler::TJobResourcesWithQuota& demand) const
{
    return Dominates(Resources_, demand);
}

// COMPAT(pogorelov)
// Always returns nullptr.
const NScheduler::NProto::TScheduleAllocationSpec* TJobSchedulingContext::GetScheduleAllocationSpec() const
{
    return nullptr;
}

TString TJobSchedulingContext::ToString(const NChunkClient::TMediumDirectoryPtr& /*mediumDirectory*/) const
{
    TStringBuilder builder;
    builder.Reserve(256);

    builder.AppendChar('{');

    FormatCommonPart(builder);

    builder.AppendFormat(
        ", AvailableResources: %v",
        Resources_);

    builder.AppendChar('}');

    return builder.Flush();
}

TString TJobSchedulingContext::GetResourcesString(const NChunkClient::TMediumDirectoryPtr& /*mediumDirectory*/) const
{
    TStringBuilder builder;
    builder.Reserve(128);

    builder.AppendChar('{');

    builder.AppendFormat(
        "ResourceLimits: %v",
        Resources_);

    builder.AppendChar('}');

    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
