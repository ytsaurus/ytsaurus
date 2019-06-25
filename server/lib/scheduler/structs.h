#pragma once

#include "public.h"

#include <yt/server/lib/controller_agent/public.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/ytlib/job_tracker_client/proto/job.pb.h>

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
    TEnumIndexedVector<int, NControllerAgent::EScheduleJobFailReason> Failed;
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

} // namespace NYT::NScheduler
