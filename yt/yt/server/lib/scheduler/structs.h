#pragma once

#include "public.h"

#include "scheduling_tag.h"

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/ytlib/scheduler/proto/allocation.pb.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TAllocationAttributes
{
    struct TDiskRequest
    {
        std::optional<i64> DiskSpace;
        std::optional<i64> InodeCount;
        std::optional<i32> MediumIndex;
    };

    std::optional<TDuration> WaitingForResourcesOnNodeTimeout;
    std::optional<TString> CudaToolkitVersion;
    TDiskRequest DiskRequest;
    bool AllowIdleCpuPolicy = false;
    int PortCount = 0;
};

void ToProto(
    NProto::TAllocationAttributes* protoAttributes,
    const TAllocationAttributes& attributes);

void FromProto(
    TAllocationAttributes* attributes,
    const NProto::TAllocationAttributes& protoAttributes);

////////////////////////////////////////////////////////////////////////////////

struct TAllocationStartDescriptor
{
    const TAllocationId Id;
    const TJobResourcesWithQuota ResourceLimits;
    TAllocationAttributes AllocationAttributes;
};

////////////////////////////////////////////////////////////////////////////////

struct TControllerScheduleAllocationResult
    : public TRefCounted
{
    void RecordFail(NControllerAgent::EScheduleAllocationFailReason reason);
    bool IsBackoffNeeded() const;
    bool IsScheduleStopNeeded() const;

    std::optional<TAllocationStartDescriptor> StartDescriptor;
    TEnumIndexedArray<NControllerAgent::EScheduleAllocationFailReason, int> Failed;
    TDuration Duration;
    std::optional<TDuration> NextDurationEstimate;
    TIncarnationId IncarnationId;
    TControllerEpoch ControllerEpoch;
};

DEFINE_REFCOUNTED_TYPE(TControllerScheduleAllocationResult)

void ToProto(
    NProto::TScheduleAllocationResponse* protoResponse,
    const TControllerScheduleAllocationResult& scheduleJobResult);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerInitializeAttributes
{
    NYson::TYsonString BriefSpec;
    NYson::TYsonString FullSpec;
    NYson::TYsonString UnrecognizedSpec;
};

////////////////////////////////////////////////////////////////////////////////

struct TPoolTreeControllerSettings
{
    TSchedulingTagFilter SchedulingTagFilter;
    bool Tentative;
    bool Probing;
    bool Offloading;
    EJobResourceType MainResource;
    bool AllowIdleCpuPolicy;
};

using TPoolTreeControllerSettingsMap = THashMap<TString, TPoolTreeControllerSettings>;

void ToProto(
    NControllerAgent::NProto::TPoolTreeControllerSettingsMap* protoPoolTreeControllerSettingsMap,
    const TPoolTreeControllerSettingsMap& poolTreeControllerSettingsMap);

void FromProto(
    TPoolTreeControllerSettingsMap* poolTreeControllerSettingsMap,
    const NControllerAgent::NProto::TPoolTreeControllerSettingsMap& protoPoolTreeControllerSettingsMap);

////////////////////////////////////////////////////////////////////////////////

struct TPreemptedFor
{
    TAllocationId AllocationId;
    TOperationId OperationId;

    bool operator==(const TPreemptedFor& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TPreemptedFor& preemptedFor, TStringBuf spec);

void ToProto(NProto::TPreemptedFor* proto, const TPreemptedFor& preemptedFor);
void FromProto(TPreemptedFor* preemptedFor, const NProto::TPreemptedFor& proto);

void Serialize(const TPreemptedFor& preemptedFor, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TCompositeNeededResources
{
    NVectorHdrf::TJobResources DefaultResources = {};
    THashMap<TString, NVectorHdrf::TJobResources> ResourcesByPoolTree = {};

    const TJobResources& GetNeededResourcesForTree(const TString& tree) const;

    void Persist(const TStreamPersistenceContext& context);
};

void FormatValue(TStringBuilderBase* builder, const TCompositeNeededResources& neededResources, TStringBuf /*format*/);

TCompositeNeededResources operator + (const TCompositeNeededResources& lhs, const TCompositeNeededResources& rhs);
TCompositeNeededResources operator - (const TCompositeNeededResources& lhs, const TCompositeNeededResources& rhs);
TCompositeNeededResources operator - (const TCompositeNeededResources& rhs);

TString FormatResources(const TCompositeNeededResources& resources);

void ToProto(NControllerAgent::NProto::TCompositeNeededResources* protoNeededResources, const TCompositeNeededResources& neededResources);
void FromProto(TCompositeNeededResources* neededResources, const NControllerAgent::NProto::TCompositeNeededResources& protoNeededResources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
