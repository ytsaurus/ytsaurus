#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/public.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TJobProfiler
    : public TRefCounted
{
public:
    explicit TJobProfiler(IInvokerPtr profilerInvoker);
    void ProfileStartedJob(const NControllers::TJoblet& joblet);
    void ProfileRunningJob(const NControllers::TJoblet& joblet);
    void ProfileRevivedJob(const NControllers::TJoblet& joblet);
    void ProfileCompletedJob(const NControllers::TJoblet& joblet, const TCompletedJobSummary& jobSummary);
    void ProfileFailedJob(const NControllers::TJoblet& joblet, const TFailedJobSummary& jobSummary);
    void ProfileAbortedJob(const NControllers::TJoblet& joblet, const TAbortedJobSummary& jobSummary);
private:
    IInvokerPtr ProfilerInvoker_;
    NProfiling::TTimeCounter TotalCompletedJobTime_;
    NProfiling::TTimeCounter TotalFailedJobTime_;
    NProfiling::TTimeCounter TotalAbortedJobTime_;

    using TStartedJobCounterKey = std::tuple<EJobType, TString>;
    using TStartedJobCounters = THashMap<TStartedJobCounterKey, NProfiling::TCounter>;

    using TAbortedJobCounterKey = std::tuple<EJobType, EAbortReason, TString>;
    using TAbortedJobCounters = THashMap<TAbortedJobCounterKey, NProfiling::TCounter>;

    using TAbortedJobByErrorCounterKey = std::tuple<EJobType, int, TString>;
    using TAbortedJobByErrorCounters = THashMap<TAbortedJobByErrorCounterKey, NProfiling::TCounter>;

    using TFailedJobCounterKey = std::tuple<EJobType, TString>;
    using TFailedJobCounters = THashMap<TFailedJobCounterKey, NProfiling::TCounter>;

    using TCompletedJobCounterKey = std::tuple<EJobType, EInterruptionReason, TString>;
    using TCompletedJobCounters = THashMap<TCompletedJobCounterKey, NProfiling::TCounter>;

    using TInProgressJobCounterKey = std::tuple<EJobState, EJobType, TString>;
    using TInProgressJobCounters = THashMap<TInProgressJobCounterKey, std::pair<i64, NProfiling::TGauge>>;

    TStartedJobCounters StartedJobCounters_;
    TAbortedJobCounters AbortedJobCounters_;
    TAbortedJobByErrorCounters AbortedJobByErrorCounters_;
    TFailedJobCounters FailedJobCounters_;
    TCompletedJobCounters CompletedJobCounters_;
    TInProgressJobCounters InProgressJobCounters_;

    template <class EErrorCodeType>
    void ProfileAbortedJobByError(const TString& treeId, EJobType jobType, const TError& error, EErrorCodeType errorCode);

    const IInvokerPtr& GetProfilerInvoker() const;

    void DoProfileStartedJob(EJobType jobType, TString treeId);
    void DoProfileRunningJob(EJobType jobType, TString treeId);
    void DoProfileRevivedJob(EJobType jobType, TString treeId, EJobState jobState);
    void DoProfileCompletedJob(
        EJobType jobType,
        EInterruptionReason interruptionReason,
        TString treeId,
        TDuration duration,
        std::optional<EJobState> previousJobState);
    void DoProfileFailedJob(
        EJobType jobType,
        TString treeId,
        TDuration duration,
        std::optional<EJobState> previousJobState);
    void DoProfileAbortedJob(
        EJobType jobType,
        EAbortReason abortReason,
        TString treeId,
        TDuration duration,
        TError error,
        std::optional<EJobState> previousJobState);

    void DoUpdateInProgressJobCount(
        EJobState jobState,
        EJobType jobType,
        TString treeId,
        bool increment);
    void DoProfileFinishedJob(
        EJobType jobType,
        std::optional<EJobState> previousJobState,
        const TString& treeId);
};

DEFINE_REFCOUNTED_TYPE(TJobProfiler)

////////////////////////////////////////////////////////////////////////////////

class TScheduleJobProfiler
    : public TRefCounted
{
public:
    explicit TScheduleJobProfiler(IInvokerPtr profilerInvoker);

    void ProfileScheduleJobFailure(const std::string& treeId, EScheduleFailReason failReason);
    void ProfileScheduleJobFailure(const std::string& treeId, EJobType jobType, EScheduleFailReason failReason, bool isJobFirst);
    void ProfileScheduleJobSuccess(const std::string& treeId, EJobType jobType, bool isJobFirst);

private:
    using TTypedScheduleFailureKey = std::tuple<std::string, EJobType, EScheduleFailReason, bool>;
    using TTypedScheduleFailureCounters = THashMap<TTypedScheduleFailureKey, NProfiling::TCounter>;
    using TScheduleFailureKey = std::tuple<std::string, EScheduleFailReason>;
    using TScheduleFailureCounters = THashMap<TScheduleFailureKey, NProfiling::TCounter>;

    using TScheduleSuccessKey = std::tuple<std::string, EJobType, bool>;
    using TScheduleSuccessCounters = THashMap<TScheduleSuccessKey, NProfiling::TCounter>;

    IInvokerPtr ProfilerInvoker_;
    TTypedScheduleFailureCounters TypedFailureCounters_;
    TScheduleFailureCounters FailureCounters_;
    TScheduleSuccessCounters SuccessCounters_;

    void DoProfileTypedScheduleJobFailure(TTypedScheduleFailureKey key);
    void DoProfileScheduleJobFailure(TScheduleFailureKey key);
    void DoProfileScheduleJobSuccess(TScheduleSuccessKey key);
};

DEFINE_REFCOUNTED_TYPE(TScheduleJobProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
