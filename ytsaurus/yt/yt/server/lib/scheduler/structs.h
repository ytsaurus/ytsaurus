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

struct TJobStartDescriptor
{
    TJobStartDescriptor(
        TJobId id,
        const TJobResourcesWithQuota& resourceLimits,
        bool interruptible);

    const TJobId Id;
    const TJobResourcesWithQuota ResourceLimits;
    const bool Interruptible;
};

////////////////////////////////////////////////////////////////////////////////

struct TControllerScheduleJobResult
    : public TRefCounted
{
    void RecordFail(NControllerAgent::EScheduleJobFailReason reason);
    bool IsBackoffNeeded() const;
    bool IsScheduleStopNeeded() const;

    std::optional<TJobStartDescriptor> StartDescriptor;
    TEnumIndexedVector<NControllerAgent::EScheduleJobFailReason, int> Failed;
    TDuration Duration;
    TIncarnationId IncarnationId;
    TControllerEpoch ControllerEpoch;
};

DEFINE_REFCOUNTED_TYPE(TControllerScheduleJobResult)

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerInitializeAttributes
{
    NYson::TYsonString Mutable;
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
    TJobId JobId;
    TOperationId OperationId;

    bool operator == (const TPreemptedFor& other) const noexcept;
    bool operator != (const TPreemptedFor& other) const noexcept;
};

TString ToString(const TPreemptedFor& preemptedFor);

void ToProto(NProto::TPreemptedFor* proto, const TPreemptedFor& preemptedFor);
void FromProto(TPreemptedFor* preemptedFor, const NProto::TPreemptedFor& proto);

void Serialize(const TPreemptedFor& preemptedFor, NYson::IYsonConsumer* consumer);
void Deserialize(TPreemptedFor& preemptedFor, const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

struct TCompositePendingJobCount
{
    int DefaultCount = 0;
    THashMap<TString, int> CountByPoolTree = {};

    int GetJobCountFor(const TString& tree) const;
    bool IsZero() const;

    void Persist(const TStreamPersistenceContext& context);
};

void Serialize(const TCompositePendingJobCount& jobCount, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const TCompositePendingJobCount& jobCount, TStringBuf /* format */);

bool operator == (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs);
bool operator != (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs);

TCompositePendingJobCount operator + (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs);
TCompositePendingJobCount operator - (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs);
TCompositePendingJobCount operator - (const TCompositePendingJobCount& count);

////////////////////////////////////////////////////////////////////////////////

struct TCompositeNeededResources
{
    NVectorHdrf::TJobResources DefaultResources = {};
    THashMap<TString, NVectorHdrf::TJobResources> ResourcesByPoolTree = {};

    const TJobResources& GetNeededResourcesForTree(const TString& tree) const;

    void Persist(const TStreamPersistenceContext& context);
};

void FormatValue(TStringBuilderBase* builder, const TCompositeNeededResources& neededResources, TStringBuf /* format */);

TCompositeNeededResources operator + (const TCompositeNeededResources& lhs, const TCompositeNeededResources& rhs);
TCompositeNeededResources operator - (const TCompositeNeededResources& lhs, const TCompositeNeededResources& rhs);
TCompositeNeededResources operator - (const TCompositeNeededResources& rhs);

TString FormatResources(const TCompositeNeededResources& resources);

void ToProto(NControllerAgent::NProto::TCompositeNeededResources* protoNeededResources, const TCompositeNeededResources& neededResources);
void FromProto(TCompositeNeededResources* neededResources, const NControllerAgent::NProto::TCompositeNeededResources& protoNeededResources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
