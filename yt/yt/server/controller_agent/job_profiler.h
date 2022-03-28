#pragma once

#include "private.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

namespace NControllers {

class TJoblet;

}

struct TStartedJobSummary;
struct TCompletedJobSummary;
struct TAbortedJobSummary;
struct TFailedJobSummary;

////////////////////////////////////////////////////////////////////////////////

class TJobProfiler
    : public TRefCounted
{
public:
    TJobProfiler();
    void ProfileStartedJob(const NControllers::TJoblet& joblet, const TStartedJobSummary& jobSummary);
    void ProfileCompletedJob(const NControllers::TJoblet& joblet, const TCompletedJobSummary& jobSummary);
    void ProfileFailedJob(const NControllers::TJoblet& joblet, const TFailedJobSummary& jobSummary);
    void ProfileAbortedJob(const NControllers::TJoblet& joblet, const TAbortedJobSummary& jobSummary);
private:
    NConcurrency::TActionQueuePtr ProfilerQueue_;

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

    using TCompletedJobCounterKey = std::tuple<EJobType, EInterruptReason, TString>;
    using TCompletedJobCounters = THashMap<TCompletedJobCounterKey, NProfiling::TCounter>;

    TStartedJobCounters StartedJobCounter_;
    TAbortedJobCounters AbortedJobCounter_;
    TAbortedJobByErrorCounters AbortedJobByErrorCounter_;
    TFailedJobCounters FailedJobCounter_;
    TCompletedJobCounters CompletedJobCounter_;

    template <class EErrorCodeType>
    void ProfileAbortedJobByError(const TString& treeId, EJobType jobType, const TError& error, EErrorCodeType errorCode);

    IInvokerPtr GetProfilerInvoker() const;

    void DoProfileStartedJob(TStartedJobCounterKey key);
    void DoProfileCompletedJob(TCompletedJobCounterKey key, TDuration duration);
    void DoProfileFailedJob(TFailedJobCounterKey key, TDuration duration);
    void DoProfileAbortedJob(TAbortedJobCounterKey key, TDuration duration, TError error);
};

DEFINE_REFCOUNTED_TYPE(TJobProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
