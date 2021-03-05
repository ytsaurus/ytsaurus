#pragma once

#include "public.h"
#include "scheduling_tag.h"

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/proto/scheduler_service.pb.h>
#include <yt/yt/ytlib/scheduler/job_resources.h>

#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TRunningJobStatistics
{
    //! In CPU*seconds.
    double TotalCpuTime = 0.0;

    //! In GPU*seconds.
    double TotalGpuTime = 0.0;
};

void FormatValue(TStringBuilderBase* builder, const TRunningJobStatistics& statistics, TStringBuf /* format */);
TString ToString(const TRunningJobStatistics& statistics);

////////////////////////////////////////////////////////////////////////////////

//! An immutable snapshot of TExecNode.
struct TExecNodeDescriptor
{
    TExecNodeDescriptor() = default;

    TExecNodeDescriptor(
        NNodeTrackerClient::TNodeId id,
        TString address,
        std::optional<TString> dataCenter,
        double ioWeight,
        bool online,
        const TJobResources& resourceUsage,
        const TJobResources& resourceLimits,
        const TBooleanFormulaTags& tags,
        const TRunningJobStatistics& runningJobStatistics,
        ESchedulingSegment schedulingSegment,
        bool schedulingSegmentFrozen);

    bool CanSchedule(const TSchedulingTagFilter& filter) const;

    NNodeTrackerClient::TNodeId Id = NNodeTrackerClient::InvalidNodeId;
    TString Address;
    std::optional<TString> DataCenter;
    double IOWeight = 0.0;
    bool Online = false;
    TJobResources ResourceUsage;
    TJobResources ResourceLimits;
    TBooleanFormulaTags Tags;
    TRunningJobStatistics RunningJobStatistics;
    ESchedulingSegment SchedulingSegment;
    bool SchedulingSegmentFrozen;

    void Persist(const TStreamPersistenceContext& context);
};

void ToProto(NScheduler::NProto::TExecNodeDescriptor* protoDescriptor, const NScheduler::TExecNodeDescriptor& descriptor);
void FromProto(NScheduler::TExecNodeDescriptor* descriptor, const NScheduler::NProto::TExecNodeDescriptor& protoDescriptor);

////////////////////////////////////////////////////////////////////////////////

//! An immutable ref-counted map of TExecNodeDescriptor-s.
struct TRefCountedExecNodeDescriptorMap
    : public TRefCounted
    , public TExecNodeDescriptorMap
{ };

DEFINE_REFCOUNTED_TYPE(TRefCountedExecNodeDescriptorMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
