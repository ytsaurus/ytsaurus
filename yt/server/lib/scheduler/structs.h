#pragma once

#include "public.h"

#include "scheduling_tag.h"

#include <yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

#include <yt/server/lib/controller_agent/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TJobStartDescriptor
{
    TJobStartDescriptor(
        TJobId id,
        EJobType type,
        const TJobResourcesWithQuota& resourceLimits,
        bool interruptible);

    const TJobId Id;
    const EJobType Type;
    const TJobResourcesWithQuota ResourceLimits;
    const bool Interruptible;
};

////////////////////////////////////////////////////////////////////////////////

struct TControllerScheduleJobResult
    : public TIntrinsicRefCounted
{
    void RecordFail(NControllerAgent::EScheduleJobFailReason reason);
    bool IsBackoffNeeded() const;
    bool IsScheduleStopNeeded() const;

    std::optional<TJobStartDescriptor> StartDescriptor;
    TEnumIndexedVector<NControllerAgent::EScheduleJobFailReason, int> Failed;
    TDuration Duration;
    TIncarnationId IncarnationId;
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
};

void ToProto(NProto::TSchedulerToAgentJobEvent::TPreemptedFor* proto, const TPreemptedFor& preemptedFor);
void FromProto(TPreemptedFor* preemptedFor, const NProto::TSchedulerToAgentJobEvent::TPreemptedFor& proto);

void Serialize(const TPreemptedFor& preemptedFor, NYson::IYsonConsumer* consumer);
void Deserialize(TPreemptedFor& preemptedFor, const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
