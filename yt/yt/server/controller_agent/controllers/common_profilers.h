#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/library/syncmap/map.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TJobProfiler
    : public TRefCounted
{
public:
    TJobProfiler() = default;

    void ProfileStartedJob(const NControllers::TJoblet& joblet);
    void ProfileRunningJob(const NControllers::TJoblet& joblet);
    void ProfileRevivedJob(const NControllers::TJoblet& joblet);
    void ProfileCompletedJob(const NControllers::TJoblet& joblet, const TCompletedJobSummary& jobSummary);
    void ProfileFailedJob(const NControllers::TJoblet& joblet, const TFailedJobSummary& jobSummary);
    void ProfileAbortedJob(const NControllers::TJoblet& joblet, const TAbortedJobSummary& jobSummary);

private:
    NProfiling::TTimeCounter TotalCompletedJobTime_ =
        ControllerAgentProfiler().TimeCounter("/jobs/total_completed_wall_time");
    NProfiling::TTimeCounter TotalFailedJobTime_ =
        ControllerAgentProfiler().TimeCounter("/jobs/total_failed_wall_time");
    NProfiling::TTimeCounter TotalAbortedJobTime_ =
        ControllerAgentProfiler().TimeCounter("/jobs/total_aborted_wall_time");

    using TStartedJobCounterKey = std::tuple<EJobType, TString>;
    using TAbortedJobCounterKey = std::tuple<EJobType, EAbortReason, TString>;
    using TAbortedJobByErrorCounterKey = std::tuple<EJobType, int, TString>;
    using TFailedJobCounterKey = std::tuple<EJobType, TString>;
    using TCompletedJobCounterKey = std::tuple<EJobType, EInterruptionReason, TString>;

    struct TInProgressJobCounter
        : public TRefCounted
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        i64 Count = 0;
        NProfiling::TGauge Gauge;
    };
    using TInProgressJobCounterPtr = TIntrusivePtr<TInProgressJobCounter>;
    using TInProgressJobCounterKey = std::tuple<EJobState, EJobType, TString>;

    NConcurrency::TSyncMap<TStartedJobCounterKey, NProfiling::TCounter> StartedJobCounters_;
    NConcurrency::TSyncMap<TAbortedJobCounterKey, NProfiling::TCounter> AbortedJobCounters_;
    NConcurrency::TSyncMap<TAbortedJobByErrorCounterKey, NProfiling::TCounter> AbortedJobByErrorCounters_;
    NConcurrency::TSyncMap<TFailedJobCounterKey, NProfiling::TCounter> FailedJobCounters_;
    NConcurrency::TSyncMap<TCompletedJobCounterKey, NProfiling::TCounter> CompletedJobCounters_;
    NConcurrency::TSyncMap<TInProgressJobCounterKey, TInProgressJobCounterPtr> InProgressJobCounters_;

    template <class EErrorCodeType>
    void ProfileAbortedJobByError(const TString& treeId, EJobType jobType, const TError& error, EErrorCodeType errorCode);

    void UpdateInProgressJobCount(EJobState jobState, EJobType jobType, const TString& treeId, bool increment);
    void ProfileFinishedJob(EJobType jobType, std::optional<EJobState> previousJobState, const TString& treeId);
};

DEFINE_REFCOUNTED_TYPE(TJobProfiler)

////////////////////////////////////////////////////////////////////////////////

class TScheduleJobProfiler
    : public TRefCounted
{
public:
    TScheduleJobProfiler() = default;

    void ProfileScheduleJobFailure(const std::string& treeId, EScheduleFailReason failReason);
    void ProfileScheduleJobFailure(const std::string& treeId, EJobType jobType, EScheduleFailReason failReason, bool isJobFirst);
    void ProfileScheduleJobSuccess(const std::string& treeId, EJobType jobType, bool isJobFirst, bool isLocal);

private:
    using TTypedScheduleFailureKey = std::tuple<std::string, EJobType, EScheduleFailReason, bool>;
    using TScheduleFailureKey = std::tuple<std::string, EScheduleFailReason>;
    using TScheduleSuccessKey = std::tuple<std::string, EJobType, bool, bool>;

    NConcurrency::TSyncMap<TTypedScheduleFailureKey, NProfiling::TCounter> TypedFailureCounters_;
    NConcurrency::TSyncMap<TScheduleFailureKey, NProfiling::TCounter> FailureCounters_;
    NConcurrency::TSyncMap<TScheduleSuccessKey, NProfiling::TCounter> SuccessCounters_;
};

DEFINE_REFCOUNTED_TYPE(TScheduleJobProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
