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
        , ThrottlingStepTime_(config->ThrottlingStepTime)
        , MaxThrottlingTime_(config->MaxThrottlingTime)
        , Window_(MaxWindow_)
        , ThrottledRequestCount_(profiler.Counter("/throttled_request_count"))
        , SkippedRequestCount_(profiler.Counter("/skipped_request_count"))
        , WindowGauge_(profiler.Gauge("/window"))
        , SlowStartThresholdGauge_(profiler.Gauge("/slow_start_threshold_gauge"))
        , MaxWindowGauge_(profiler.Gauge("/max_window"))
    { }

    TOverloadedStatus GetOverloadedStatus(TDuration totalThrottledTime, std::optional<TDuration> requestTimeout)
    {
        auto window = Window_.load(std::memory_order::relaxed);
        bool overloaded = static_cast<int>(RandomNumber<ui32>(MaxWindow_)) + 1 > window;
        bool skipCall = totalThrottledTime + ThrottlingStepTime_ >= requestTimeout.value_or(MaxThrottlingTime_);

        ThrottledRequestCount_.Increment(static_cast<int>(overloaded && totalThrottledTime == TDuration::Zero()));
        SkippedRequestCount_.Increment(static_cast<int>(skipCall));

        return TOverloadedStatus {
            .Overloaded = overloaded,
            .SkipCall = skipCall,
            .ThrottleTime = ThrottlingStepTime_,
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
            SlowStartThreshold_ = window > 0 ? window / 2 : SlowStartThreshold_;
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
    const TDuration ThrottlingStepTime_;
    const TDuration MaxThrottlingTime_;

    std::atomic<int> Window_;
    int SlowStartThreshold_ = 0;

    TCounter ThrottledRequestCount_;
    TCounter SkippedRequestCount_;
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
    sensors.Overloaded = profiler.Summary("/overloaded");
    sensors.MeanWaitTime = profiler.Timer("/mean_wait_time");

    UpdateStateSnapshot(State_, std::move(guard));
}

TOverloadedStatus TOverloadController::GetOverloadStatus(
    TDuration totalThrottledTime,
    const TString& service,
    const TString& method,
    std::optional<TDuration> requestTimeout) const
{
    auto snapshot = GetStateSnapshot();

    if (!snapshot->Config->Enabled) {
        return {};
    }

    const auto& controllers = snapshot->CongestionControllers;
    if (auto it = controllers.find(std::make_pair(service, method)); it != controllers.end()) {
        return it->second->GetOverloadedStatus(totalThrottledTime, requestTimeout);
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
            auto controllerProfiler = profiler
                .WithTag("yt_service", method->Service)
                .WithTag("method", method->Method);
            auto controller = New<TCongestionController>(config, std::move(controllerProfiler));
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
            it->second.Overloaded.Record(static_cast<int>(trackerOverloaded));
            it->second.MeanWaitTime.Record(movingMeanValue);
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
