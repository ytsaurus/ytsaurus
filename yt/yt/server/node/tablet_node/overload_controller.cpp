#include "overload_controller.h"
#include "private.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/timing.h>

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
    TMeanWaitTimeTracker(TStringBuf trackerType, TStringBuf trackerId)
        : Type_(trackerType)
        , Id_(trackerId)
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

        YT_LOG_DEBUG("Reporting mean wait time for tracker "
            "(Tracker: %v, TotalWaitTime: %v, TotalCount: %v, MeanValue: %v)",
            Id_,
            summary.Sum(),
            summary.Count(),
            meanValue);

        return meanValue;
    }

    const TString& GetType() const
    {
        return Type_;
    }

private:
    using TPerCpuDurationSummary = TPerCpuSummary<TDuration>;

    const TString Type_;
    const TString Id_;
    TIntrusivePtr<TPerCpuDurationSummary> Counter_;
};

DEFINE_REFCOUNTED_TYPE(TMeanWaitTimeTracker);

////////////////////////////////////////////////////////////////////////////////

bool ShouldThrottleCall(const TCongestionState& congestionState)
{
    if (!congestionState.MaxWindow || !congestionState.CurrentWindow) {
        return false;
    }

    return static_cast<int>(RandomNumber<ui32>(*congestionState.MaxWindow)) + 1 > *congestionState.CurrentWindow;
}

////////////////////////////////////////////////////////////////////////////////

class TCongestionController
    : public TRefCounted
{
public:
    TCongestionController(TServiceMethodConfigPtr methodConfig, TOverloadControllerConfigPtr config, TProfiler profiler)
        : MethodConfig_(std::move(methodConfig))
        , Config_(std::move(config))
        , MaxWindow_(MethodConfig_->MaxWindow)
        , Window_(MaxWindow_)
        , ThrottledRequestCount_(profiler.Counter("/throttled_request_count"))
        , WindowGauge_(profiler.Gauge("/window"))
        , SlowStartThresholdGauge_(profiler.Gauge("/slow_start_threshold_gauge"))
        , MaxWindowGauge_(profiler.Gauge("/max_window"))
    { }

    TCongestionState GetCongestionState()
    {
        auto result = TCongestionState{
            .CurrentWindow = Window_.load(std::memory_order::relaxed),
            .MaxWindow = MaxWindow_,
            .WaitingTimeoutFraction = MethodConfig_->WaitingTimeoutFraction,
        };

        auto now = NProfiling::GetCpuInstant();
        auto recentlyOverloadedThreshold = Config_->LoadAdjustingPeriod * MethodConfig_->MaxWindow;

        for (const auto& [trackerType, lastOverloaded] : OverloadedTrackers_) {
            auto sinceLastOverloaded = CpuDurationToDuration(now - lastOverloaded.load(std::memory_order::relaxed));
            if (sinceLastOverloaded < recentlyOverloadedThreshold) {
                result.OverloadedTrackers.push_back(trackerType);
            }
        }

        return result;
    }

    void Adjust(const THashSet<std::string>& overloadedTrackers, TCpuInstant timestamp)
    {
        auto window = Window_.load(std::memory_order::relaxed);

        // NB. We reporting here slightly outdated values but this makes code simpler a bit.
        WindowGauge_.Update(window);
        MaxWindowGauge_.Update(MaxWindow_);
        SlowStartThresholdGauge_.Update(SlowStartThreshold_);

        for (const auto& tracker : overloadedTrackers) {
            auto it = OverloadedTrackers_.find(tracker);
            if (it != OverloadedTrackers_.end()) {
                it->second.store(timestamp, std::memory_order::relaxed);
            }
        }

        auto overloaded = !overloadedTrackers.empty();

        if (overloaded) {
            SlowStartThreshold_ = window > 0 ? window / 2 : SlowStartThreshold_;
            Window_.store(0, std::memory_order::relaxed);

            YT_LOG_WARNING("System is overloaded (SlowStartThreshold: %v, Window: %v, OverloadedTrackers: %v)",
                SlowStartThreshold_,
                window,
                overloadedTrackers);
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

    void AddTrackerType(std::string trackerType)
    {
        // Called only during initial construction of controllers,
        // so we do not have to serialize here.
        OverloadedTrackers_[trackerType] = {};
    }

private:
    const TServiceMethodConfigPtr MethodConfig_;
    const TOverloadControllerConfigPtr Config_;
    const int MaxWindow_;

    std::atomic<int> Window_;
    int SlowStartThreshold_ = 0;
    THashMap<std::string, std::atomic<TCpuInstant>> OverloadedTrackers_;

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
}

void TOverloadController::Start()
{
    Periodic_->Start();
}

void TOverloadController::TrackInvoker(TStringBuf name, const IInvokerPtr& invoker)
{
    invoker->RegisterWaitTimeObserver(CreateGenericTracker(name));
}

void TOverloadController::TrackFSHThreadPool(TStringBuf name, const NConcurrency::ITwoLevelFairShareThreadPoolPtr& threadPool)
{
    threadPool->RegisterWaitTimeObserver(CreateGenericTracker(name));
}

TOverloadController::TWaitTimeObserver TOverloadController::CreateGenericTracker(TStringBuf trackerType, std::optional<TStringBuf> id)
{
    YT_LOG_DEBUG("Creating overload tracker (TrackerType: %v, Id: %v)",
        trackerType,
        id);

    auto trackerId = id.value_or(trackerType);

    auto tracker = New<TMeanWaitTimeTracker>(trackerType, trackerId);
    auto profiler = Profiler.WithTag("tracker", TString(trackerId));

    auto guard = Guard(SpinLock_);
    State_.Trackers[trackerId] = tracker;
    auto& sensors = State_.TrackerSensors[trackerId];
    sensors.Overloaded = profiler.Summary("/overloaded");
    sensors.MeanWaitTime = profiler.Timer("/mean_wait_time");
    sensors.MeanWaitTimeThreshold = profiler.TimeGauge("/mean_wait_time_threshold");

    UpdateStateSnapshot(State_, std::move(guard));

    return [tracker] (TDuration waitTime) {
        tracker->Record(waitTime);
    };
}

TCongestionState TOverloadController::GetCongestionState(TStringBuf service, TStringBuf method) const
{
    auto snapshot = GetStateSnapshot();
    if (!snapshot->Config->Enabled) {
        return {};
    }

    const auto& controllers = snapshot->CongestionControllers;
    if (auto it = controllers.find(std::make_pair(service, method)); it != controllers.end()) {
        return it->second->GetCongestionState();
    }

    return {};
}

TOverloadController::TMethodsCongestionControllers TOverloadController::CreateCongestionControllers(
    const TOverloadControllerConfigPtr& config,
    NProfiling::TProfiler profiler)
{
    TMethodsCongestionControllers controllers;

    THashMap<TMethodIndex, TServiceMethodConfigPtr> configIndex;
    for (const auto& methodConfig : config->Methods) {
        configIndex[std::make_pair(methodConfig->Service, methodConfig->Method)] = methodConfig;
    }

    auto getConfig = [&configIndex] (const TStringBuf& service, const TStringBuf& method) {
        auto it = configIndex.find(std::make_pair(service, method));
        if (it != configIndex.end()) {
            return it->second;
        }

        auto defaultConfig = New<TServiceMethodConfig>();
        defaultConfig->Service = service;
        defaultConfig->Method = method;
        return defaultConfig;
    };

    for (const auto& [trackerType, tracker] : config->Trackers) {
        for (const auto& method : tracker->MethodsToThrottle) {
            auto& controller = controllers[std::make_pair(method->Service, method->Method)];

            if (!controller) {
                auto methodConfig = getConfig(method->Service, method->Method);
                auto controllerProfiler = profiler
                    .WithTag("yt_service", method->Service)
                    .WithTag("method", method->Method);

                controller = New<TCongestionController>(std::move(methodConfig), config, std::move(controllerProfiler));
            }

            controller->AddTrackerType(trackerType);
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
    LoadAdjusted_.Fire();
}

void TOverloadController::DoAdjust(const THazardPtr<TState>& state)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    auto now = NProfiling::GetCpuInstant();

    using TOverloadedTrackers = THashSet<std::string>;
    THashMap<TMethodIndex, TOverloadedTrackers> methodOverloaded;

    const auto& config = state->Config;

    for (const auto& [_, trackerConfig] : config->Trackers) {
        for (const auto& method : trackerConfig->MethodsToThrottle) {
            methodOverloaded[std::make_pair(method->Service, method->Method)] = {};
        }
    }

    for (const auto& [trackerId, tracker] : state->Trackers) {
        auto trackerIt = config->Trackers.find(tracker->GetType());
        if (trackerIt == config->Trackers.end()) {
            continue;
        }

        auto movingMeanValue = tracker->GetAndReset();
        bool trackerOverloaded = movingMeanValue > trackerIt->second->MeanWaitTimeThreshold;

        if (auto it = state->TrackerSensors.find(trackerId); it != state->TrackerSensors.end()) {
            it->second.Overloaded.Record(static_cast<int>(trackerOverloaded));
            it->second.MeanWaitTime.Record(movingMeanValue);
            it->second.MeanWaitTimeThreshold.Update(trackerIt->second->MeanWaitTimeThreshold);
        }

        if (!trackerOverloaded) {
            continue;
        }

        for (const auto& method : trackerIt->second->MethodsToThrottle) {
            auto& overloadedTrackers = methodOverloaded[std::make_pair(method->Service, method->Method)];
            overloadedTrackers.insert(tracker->GetType());
        }
    }

    for (const auto& [method, overloadedTrackers] : methodOverloaded) {
        auto it = state->CongestionControllers.find(method);
        if (it == state->CongestionControllers.end()) {
            YT_LOG_WARNING("Cannot find congestion controller for method (Service: %v, Method: %v)",
                method.first,
                method.second);

            continue;
        }

        it->second->Adjust(overloadedTrackers, now);
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
