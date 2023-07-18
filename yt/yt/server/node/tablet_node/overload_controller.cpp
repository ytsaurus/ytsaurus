#include "overload_controller.h"
#include "private.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/solomon/percpu.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NTabletNode {

using namespace NThreading;
using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static auto Logger = TabletNodeLogger.WithTag("OverloadController");

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TOverloadController::TState);

class TMeanWaitTimeTracker
    : public TRefCounted
{
public:
    explicit TMeanWaitTimeTracker(TString name)
        : Name_(std::move(name))
        , Counter_(New<TPerCpuDurationSummary>())
    { }

    void Record(TDuration waitTime)
    {
        Counter_->Record(waitTime);
    }

    TDuration GetAndReset()
    {
        auto summary = Counter_->GetSummaryAndReset();
        TDuration meanValue;

        if (summary.Count()) {
            meanValue = summary.Sum() / summary.Count();
        }

        YT_LOG_DEBUG("Reporting mean wait time for invoker "
            "(Invoker: %v, TotalWaitTime: %v, TotalCount: %v, MeanValue: %v)",
            Name_,
            summary.Sum(),
            summary.Count(),
            meanValue);

        return meanValue;
    }

    const TString& GetName() const
    {
        return Name_;
    }

private:
    using TPerCpuDurationSummary = TPerCpuSummary<TDuration>;

    const TString Name_;
    TIntrusivePtr<TPerCpuDurationSummary> Counter_;
};

DEFINE_REFCOUNTED_TYPE(TMeanWaitTimeTracker);

////////////////////////////////////////////////////////////////////////////////

class TCongestionController
    : public TRefCounted
{
public:
    TCongestionController(const TOverloadControllerConfigPtr& config, TProfiler profiler)
        : MaxWindow_(config->MaxWindow)
        , HeavilyOverloadedThrottleTime_(config->HeavilyOverloadedThrottleTime)
        , OverloadedThrottleTime_(config->OverloadedThrottleTime)
        , DoNotReplOnHeavyOverload_(config->DoNotReplyOnHeavyOverload)
        , Window_(MaxWindow_)
        , SkippedRequestCount_(profiler.Counter("/skipped_request_count"))
        , UnrepliedRequestCount_(profiler.Counter("/unreplied_request_count"))
        , WindowGauge_(profiler.Gauge("/window"))
        , SlowStartThresholdGauge_(profiler.Gauge("/slow_start_threshold_gauge"))
        , MaxWindowGauge_(profiler.Gauge("/max_window"))
    { }

    TOverloadedStatus GetOverloadedStatus()
    {
        auto window = Window_.load(std::memory_order::relaxed);
        bool skip = static_cast<int>(RandomNumber<ui32>(MaxWindow_)) + 1 > window;

        bool heavilyOverloaded = window == 0;
        bool doNotReply = DoNotReplOnHeavyOverload_ && heavilyOverloaded;

        SkippedRequestCount_.Increment(static_cast<int>(skip));
        UnrepliedRequestCount_.Increment(static_cast<int>(doNotReply));

        return TOverloadedStatus {
            .SkipCall = skip,
            .DoNotReply = doNotReply,
            .ThrottleTime = heavilyOverloaded ? HeavilyOverloadedThrottleTime_ : OverloadedThrottleTime_,
        };
    }

    void Adjust(bool overloaded)
    {
        auto window = Window_.load(std::memory_order::relaxed);

        // NB. We reporting here slightly outdated values but this makes code simpler a bit.
        WindowGauge_.Update(window);
        MaxWindowGauge_.Update(MaxWindow_);
        SlowStartThresholdGauge_.Update(SlowStartThreshold_);

        if (overloaded) {
            SlowStartThreshold_ = window / 2;
            Window_.store(0, std::memory_order::relaxed);

            YT_LOG_WARNING("System is overloaded (SlowStartThreshold: %v, Window: %v)",
                SlowStartThreshold_,
                window);
            return;
        }

        if (window >= SlowStartThreshold_) {
            ++window;
        } else {
            window *= 2;
            window = std::min(SlowStartThreshold_, window);
        }

        // Keeping window in sane limits.
        window = std::min(MaxWindow_, window);
        window = std::max(1, window);

        YT_LOG_DEBUG("Adjusting system load up (SlowStartThreshold: %v, CurrentWindow: %v)",
            SlowStartThreshold_,
            window);

        Window_.store(window, std::memory_order::relaxed);
    }

private:
    const int MaxWindow_;
    const TDuration HeavilyOverloadedThrottleTime_;
    const TDuration OverloadedThrottleTime_;
    const bool DoNotReplOnHeavyOverload_;

    std::atomic<int> Window_;
    int SlowStartThreshold_ = 0;

    TCounter SkippedRequestCount_;
    TCounter UnrepliedRequestCount_;
    TGauge WindowGauge_;
    TGauge SlowStartThresholdGauge_;
    TGauge MaxWindowGauge_;
};

DEFINE_REFCOUNTED_TYPE(TCongestionController);

////////////////////////////////////////////////////////////////////////////////

TOverloadController::TOverloadController(TOverloadControllerConfigPtr config)
    : ControlThread_(New<TActionQueue>("OverloadCtl"))
    , Invoker_(ControlThread_->GetInvoker())
    , Periodic_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TOverloadController::Adjust, MakeWeak(this)),
        config->LoadAdjustingPeriod))
    , Profiler(TabletNodeProfiler.WithPrefix("/overload_controller"))
{
    State_.Config = std::move(config);
    UpdateStateSnapshot(State_, Guard(SpinLock_));

    Periodic_->Start();
}

void TOverloadController::TrackInvoker(const TString& name, const IInvokerPtr& invoker)
{
    AddTracker(name, invoker);
}

void TOverloadController::TrackFSHThreadPool(const TString& name, const NConcurrency::ITwoLevelFairShareThreadPoolPtr& threadPool)
{
    AddTracker(name, threadPool);
}

template <class TExecutorPtr>
void TOverloadController::AddTracker(const TString& name, const TExecutorPtr& invoker)
{
    auto tracker = New<TMeanWaitTimeTracker>(name);

    invoker->RegisterWaitTimeObserver([tracker] (TDuration waitTime) {
        tracker->Record(waitTime);
    });

    auto profiler = Profiler.WithTag("tracker", name);

    auto guard = Guard(SpinLock_);
    State_.Trackers.push_back(tracker);
    auto& sensors = State_.TrackerSensors[name];
    sensors.Overloaded = profiler.GaugeSummary("/overloaded");
    sensors.MeanWaitTime = profiler.TimeGaugeSummary("/mean_wait_time");

    UpdateStateSnapshot(State_, std::move(guard));
}

TOverloadedStatus TOverloadController::GetOverloadStatus(
    const TString& service,
    const TString& method) const
{
    auto snapshot = GetStateSnapshot();

    if (!snapshot->Config->Enabled) {
        return {};
    }

    const auto& controllers = snapshot->CongestionControllers;
    if (auto it = controllers.find(std::make_pair(service, method)); it != controllers.end()) {
        return it->second->GetOverloadedStatus();
    }

    return {};
}

TOverloadController::TMethodsCongestionControllers TOverloadController::CreateCongestionControllers(
    const TOverloadControllerConfigPtr& config,
    NProfiling::TProfiler profiler)
{
    TMethodsCongestionControllers controllers;

    for (const auto& [_, tracker] : config->Trackers) {
        for (const auto& method : tracker->MethodsToThrottle) {
            auto controller = New<TCongestionController>(
                config,
                profiler.WithTag("method", method->Method));
            controllers[std::make_pair(method->Service, method->Method)] = controller;
        }
    }

    return controllers;
}

void TOverloadController::Reconfigure(TOverloadControllerConfigPtr config)
{
    Periodic_->SetPeriod(config->LoadAdjustingPeriod);

    auto guard = Guard(SpinLock_);
    State_.CongestionControllers = CreateCongestionControllers(config, Profiler);
    State_.Config = std::move(config);

    UpdateStateSnapshot(State_, std::move(guard));
}

void TOverloadController::Adjust()
{
    DoAdjust(GetStateSnapshot());
}

void TOverloadController::DoAdjust(const THazardPtr<TState>& state)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    THashMap<TMethodIndex, bool> methodOverloaded;

    const auto& config = state->Config;

    for (const auto& [_, tracker] : config->Trackers) {
        for (const auto& method : tracker->MethodsToThrottle) {
            methodOverloaded[std::make_pair(method->Service, method->Method)] = false;
        }
    }

    for (const auto& tracker : state->Trackers) {
        auto it = config->Trackers.find(tracker->GetName());
        if (it == config->Trackers.end()) {
            continue;
        }

        auto movingMeanValue = tracker->GetAndReset();
        bool trackerOverloaded = movingMeanValue > it->second->MeanWaitTimeThreshold;

        if (auto it = state->TrackerSensors.find(tracker->GetName()); it != state->TrackerSensors.end()) {
            it->second.Overloaded.Update(static_cast<int>(trackerOverloaded));
            it->second.MeanWaitTime.Update(movingMeanValue);
        }

        if (!trackerOverloaded) {
            continue;
        }

        for (const auto& method : it->second->MethodsToThrottle) {
            methodOverloaded[std::make_pair(method->Service, method->Method)] = true;
        }
    }

    for (const auto& [method, overloaded] : methodOverloaded) {
        auto it = state->CongestionControllers.find(method);
        if (it == state->CongestionControllers.end()) {
            YT_LOG_WARNING("Cannot find congestion controller for method (Service: %v, Method: %v)",
                method.first,
                method.second);

            continue;
        }

        it->second->Adjust(overloaded);
    }
}

THazardPtr<TOverloadController::TState> TOverloadController::GetStateSnapshot() const
{
    YT_VERIFY(StateSnapshot_);

    return StateSnapshot_.AcquireHazard();
}

void TOverloadController::UpdateStateSnapshot(const TState& state, TSpinLockGuard guard)
{
    auto snapshot = New<TState>(state);
    guard.Release();

    StateSnapshot_.Store(std::move(snapshot));
    ReclaimHazardPointers();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
