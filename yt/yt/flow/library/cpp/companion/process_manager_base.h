#pragma once

#include "companion_client.h"
#include "public.h"

#include <yt/yt/core/misc/config.h>

#include <functional>
#include <vector>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Sums RSS over the parent and its descendants; pids that vanish mid-probe are skipped.
i64 SumProcessFamilyRss(
    int parentPid,
    const std::vector<int>& descendantPids,
    const std::function<i64(int)>& getRss);

i64 SumCompanionFamilyRss(int parentPid);

//////////////////////////////////////////////////////////////////////////////////

class TProcessManagerBase
    : public TRefCounted
{
public:
    TProcessManagerBase(
        const IInvokerPtr& invoker,
        const ICompanionClientPtr& companionClient,
        const TExponentialBackoffOptions& backoffOptions,
        const TDuration restartDelay,
        const TDuration healthCheckInterval,
        const TDuration startupGracePeriod,
        const TDuration metricsCollectionInterval,
        const NLogging::TLogger logger,
        const NProfiling::TProfiler profiler,
        const IStatusProfilerPtr& statusProfiler);

    //! Starts auto-restartable companion process.
    void Start();

    //! Restarts companion process by stopping current incarnation and scheduling a new one.
    void Restart();

    //! Stops auto-restartable companion process.
    void Shutdown();

protected:
    //! Validates the process manager configuration before spawning.
    //! Each subclass must verify that its required fields (e.g. binary paths) are valid.
    virtual void ValidateParameters() const = 0;

    //! Create a new process incarnation.
    //! Implementation would be specific for each language and runtime: Java, Python, etc.
    //! This method should not call process::Spawn() method inside.
    virtual TIntrusivePtr<TProcessBase> CreateProcessIncarnation() = 0;

    //! Checks if the companion process is available.
    //! Default implementation uses CompanionInfo endpoint.
    virtual TFuture<void> HealthCheck();

private:
    //! Checks if the companion process is available and takes actions if not.
    void CheckCompanionAvailability();

    //! Returns whether the manager is running, i.e. Start() has been called and
    //! Shutdown() has not.
    bool IsStarted();

    //! Returns true while the current incarnation is alive and younger than the
    //! startup grace period. Health check failures are expected within this window
    //! and must not trigger a restart.
    bool IsWithinStartupGracePeriod();

    //! Starts a new process incarnation and subscribes to its completion for auto-restart.
    void DoStart();

    //! Invoked when a process incarnation exits.
    void OnProcessStopped(const TError& error, const std::string& commandLine);

    //! Schedules companion process incarnation start after configured delay.
    void ScheduleRestart();

    //! Stops current incarnation of companion process.
    void StopIncarnation();

    //! Finds and kills zombie processes on the companion port.
    void KillZombieProcessesOnPort();

    //! Kills a process and all its children.
    void KillProcessWithChildren(int pid);

    //! Collects metrics from companion process.
    void CollectCompanionMetrics();

private:
    const IInvokerPtr Invoker_;
    const ICompanionClientPtr CompanionClient_;
    const TExponentialBackoffOptions BackoffOptions_;
    const TDuration RestartDelay_;
    const TDuration HealthCheckInterval_;
    const TDuration StartupGracePeriod_;
    const TDuration MetricsCollectionInterval_;
    const NConcurrency::TPeriodicExecutorPtr HealthCheckExecutor_;
    const NConcurrency::TPeriodicExecutorPtr MetricsCollectionExecutor_;
    // Mutable state and lock.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    bool Started_ = false;
    bool RestartOnStop_ = false;
    TIntrusivePtr<TProcessBase> CurrentProcess_;
    TFuture<void> CurrentSpawnFuture_;
    TInstant IncarnationSpawnTime_;

protected:
    const TCompanionExecutionConfigPtr CompanionConfig_;
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler_;
    NProfiling::TCounter RestartCounter_;
    NProfiling::TGauge MemoryUsageGauge_;
    NProfiling::TGauge ThreadCountGauge_;
    const IStatusErrorStatePtr ErrorState_;
};

DEFINE_REFCOUNTED_TYPE(TProcessManagerBase);

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
