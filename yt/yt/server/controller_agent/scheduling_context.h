#pragma once

#include "public.h"

#include <yt/yt/server/controller_agent/controllers/job_info.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContext
{
public:
    TSchedulingContext(
        TAllocationId allocationId,
        NControllers::TJobNodeDescriptor nodeDescriptor,
        std::optional<TString> poolPath);

    const std::optional<TString>& GetPoolPath() const;

    const NControllers::TJobNodeDescriptor& GetNodeDescriptor() const;

    virtual bool CanSatisfyDemand(const NScheduler::TJobResourcesWithQuota& demand) const = 0;

    TAllocationId GetAllocationId() const;

    NProfiling::TCpuInstant GetNow() const;

    // COMPAT(pogorelov)
    virtual const NScheduler::NProto::TScheduleAllocationSpec* GetScheduleAllocationSpec() const = 0;

    virtual TString ToString(const NChunkClient::TMediumDirectoryPtr& mediumDirectory) const = 0;
    virtual TString GetResourcesString(const NChunkClient::TMediumDirectoryPtr& mediumDirectory) const = 0;

protected:
    void FormatCommonPart(TStringBuilderBase& builder) const;

private:
    const TAllocationId AllocationId_;

    const NControllers::TJobNodeDescriptor NodeDescriptor_;
    const std::optional<TString> PoolPath_;
};

////////////////////////////////////////////////////////////////////////////////

class TAllocationSchedulingContext final
    : public TSchedulingContext
{
public:
    TAllocationSchedulingContext(
        TAllocationId allocationId,
        TJobResources resourceLimits,
        NScheduler::TDiskResources diskResources,
        NControllers::TJobNodeDescriptor nodeDescriptor,
        std::optional<TString> poolPath,
        const NScheduler::NProto::TScheduleAllocationSpec& scheduleAllocationSpec);

    bool CanSatisfyDemand(const NScheduler::TJobResourcesWithQuota& demand) const final;

    // COMPAT(pogorelov)
    const NScheduler::NProto::TScheduleAllocationSpec* GetScheduleAllocationSpec() const final;

    TString ToString(const NChunkClient::TMediumDirectoryPtr& mediumDirectory) const final;
    TString GetResourcesString(const NChunkClient::TMediumDirectoryPtr& mediumDirectory) const final;

private:
    TJobResources ResourceLimits_;
    const NScheduler::TDiskResources DiskResources_;
    const NScheduler::NProto::TScheduleAllocationSpec ScheduleAllocationSpec_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobSchedulingContext final
    : public TSchedulingContext
{
public:
    TJobSchedulingContext(
        TAllocationId allocationId,
        NScheduler::TDiskQuota diskQuota,
        NControllers::TJobNodeDescriptor nodeDescriptor,
        std::optional<TString> poolPath);

    bool CanSatisfyDemand(const NScheduler::TJobResourcesWithQuota& demand) const final;

    // COMPAT(pogorelov)
    // Always returns nullptr.
    const NScheduler::NProto::TScheduleAllocationSpec* GetScheduleAllocationSpec() const final;

    TString ToString(const NChunkClient::TMediumDirectoryPtr& mediumDirectory) const final;
    TString GetResourcesString(const NChunkClient::TMediumDirectoryPtr& mediumDirectory) const final;

private:
    NScheduler::TDiskQuota DiskQuota_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
