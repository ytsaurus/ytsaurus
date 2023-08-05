#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/atomic_ptr.h>

#include <library/cpp/yt/threading/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TOverloadedStatus
{
    bool Overloaded = false;
    bool SkipCall = false;
    TDuration ThrottleTime;
};

////////////////////////////////////////////////////////////////////////////////

class TOverloadController
    : public TRefCounted
{
public:
    struct TTrackerSensors
    {
        NProfiling::TSummary Overloaded;
        NProfiling::TEventTimer MeanWaitTime;
    };

    using TMethodIndex = std::pair<TString, TString>;
    using TMethodsCongestionControllers = THashMap<TMethodIndex, TCongestionControllerPtr>;

    struct TState final
    {
        static constexpr bool EnableHazard = true;

        TOverloadControllerConfigPtr Config;
        TMethodsCongestionControllers CongestionControllers;
        std::vector<TMeanWaitTimeTrackerPtr> Trackers;
        THashMap<TString, TTrackerSensors> TrackerSensors;
    };

    explicit TOverloadController(TOverloadControllerConfigPtr config);

    void TrackInvoker(const TString& name, const IInvokerPtr& invoker);
    void TrackFSHThreadPool(const TString& name, const NConcurrency::ITwoLevelFairShareThreadPoolPtr& threadPool);

    TOverloadedStatus GetOverloadStatus(
        TDuration totalThrottledTime,
        const TString& service,
        const TString& method,
        std::optional<TDuration> requestTimeout) const;

    void Reconfigure(TOverloadControllerConfigPtr config);

private:
    using TSpinLockGuard = TGuard<NThreading::TSpinLock>;

    const NConcurrency::TActionQueuePtr ControlThread_;
    const IInvokerPtr Invoker_;
    const NConcurrency::TPeriodicExecutorPtr Periodic_;

    NProfiling::TProfiler Profiler;

    TAtomicPtr<TState, /*EnableAcquireHazard*/ true> StateSnapshot_;

    NThreading::TSpinLock SpinLock_;
    TState State_;

    static TMethodsCongestionControllers CreateCongestionControllers(
        const TOverloadControllerConfigPtr& config,
        NProfiling::TProfiler profiler);

    void Adjust();
    void DoAdjust(const THazardPtr<TState>& state);

    void DoReconfigure(TOverloadControllerConfigPtr config);
    THazardPtr<TState> GetStateSnapshot() const;
    void UpdateStateSnapshot(const TState& state, TSpinLockGuard guard);

    template <class TExecutorPtr>
    void AddTracker(const TString& name, const TExecutorPtr& executor);
};

DEFINE_REFCOUNTED_TYPE(TOverloadController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
