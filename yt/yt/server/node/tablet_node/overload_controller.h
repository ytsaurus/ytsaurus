#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/atomic_ptr.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/threading/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// Overall rpc-method congestion state.
// Can be a a measure of allowed concurrency or probability of acceptance more calls.
struct TCongestionState
{
    std::optional<int> CurrentWindow;
    std::optional<int> MaxWindow;
    double WaitingTimeoutFraction = 0;

    using TTrackersList = TCompactVector<std::string, 4>;
    TTrackersList OverloadedTrackers;
};

// Probabilistic predicate based on congestion state of the the method.
bool ShouldThrottleCall(const TCongestionState& congestionState);

////////////////////////////////////////////////////////////////////////////////

class TOverloadController
    : public TRefCounted
{
public:
    struct TTrackerSensors
    {
        NProfiling::TSummary Overloaded;
        NProfiling::TEventTimer MeanWaitTime;
        NProfiling::TTimeGauge MeanWaitTimeThreshold;
    };

    using TMethodIndex = std::pair<TString, TString>;
    using TMethodsCongestionControllers = THashMap<TMethodIndex, TCongestionControllerPtr>;

    struct TState final
    {
        static constexpr bool EnableHazard = true;

        TOverloadControllerConfigPtr Config;
        TMethodsCongestionControllers CongestionControllers;
        THashMap<TString, TMeanWaitTimeTrackerPtr> Trackers;
        THashMap<TString, TTrackerSensors> TrackerSensors;
    };

    DEFINE_SIGNAL(void(), LoadAdjusted);

    explicit TOverloadController(TOverloadControllerConfigPtr config);

    void TrackInvoker(TStringBuf name, const IInvokerPtr& invoker);
    void TrackFSHThreadPool(TStringBuf name, const NConcurrency::ITwoLevelFairShareThreadPoolPtr& threadPool);

    using TWaitTimeObserver = std::function<void(TDuration)>;
    TWaitTimeObserver CreateGenericTracker(TStringBuf trackerType, std::optional<TStringBuf> id = {});

    TCongestionState GetCongestionState(TStringBuf service, TStringBuf method) const;

    void Reconfigure(TOverloadControllerConfigPtr config);
    void Start();

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
};

DEFINE_REFCOUNTED_TYPE(TOverloadController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
