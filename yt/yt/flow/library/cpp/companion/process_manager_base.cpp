#include "process_manager_base.h"

#include "companion_singleton_state.h"
#include "config.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/library/process/process.h>

#include <util/string/strip.h>
#include <util/system/getpid.h>
#include <util/system/mem_info.h>
#include <util/system/shellcommand.h>

namespace NYT::NFlow::NCompanion {

//////////////////////////////////////////////////////////////////////////////////

i64 SumProcessFamilyRss(
    int parentPid,
    const std::vector<int>& descendantPids,
    const std::function<i64(int)>& getRss)
{
    i64 totalRss = 0;
    try {
        totalRss += getRss(parentPid);
    } catch (const std::exception&) {
        // Parent vanished between sampling moments; descendants are still worth summing.
    }

    for (auto pid : descendantPids) {
        if (pid == parentPid) {
            continue;
        }
        try {
            totalRss += getRss(pid);
        } catch (const std::exception&) {
            // Single child died between the listing and the RSS read; skip it.
        }
    }
    return totalRss;
}

i64 SumCompanionFamilyRss(int parentPid)
{
    return SumProcessFamilyRss(
        parentPid,
        GetPidsUnderParent(parentPid),
        [] (int pid) {
            return static_cast<i64>(NMemInfo::GetMemInfo(pid).RSS);
        });
}

//////////////////////////////////////////////////////////////////////////////////

TProcessManagerBase::TProcessManagerBase(
    const IInvokerPtr& invoker,
    const ICompanionClientPtr& companionClient,
    const TExponentialBackoffOptions& backoffOptions,
    const TDuration restartDelay,
    const TDuration healthCheckInterval,
    const TDuration startupGracePeriod,
    const TDuration metricsCollectionInterval,
    const NLogging::TLogger logger,
    const NProfiling::TProfiler profiler,
    const IStatusProfilerPtr& statusProfiler)
    : Invoker_(invoker)
    , CompanionClient_(companionClient)
    , BackoffOptions_(backoffOptions)
    , RestartDelay_(restartDelay)
    , HealthCheckInterval_(healthCheckInterval)
    , StartupGracePeriod_(startupGracePeriod)
    , MetricsCollectionInterval_(metricsCollectionInterval)
    , HealthCheckExecutor_(New<NConcurrency::TPeriodicExecutor>(
        Invoker_,
        BIND(&TProcessManagerBase::CheckCompanionAvailability, MakeWeak(this)),
        HealthCheckInterval_))
    , MetricsCollectionExecutor_(New<NConcurrency::TPeriodicExecutor>(
        Invoker_,
        BIND(&TProcessManagerBase::CollectCompanionMetrics, MakeWeak(this)),
        MetricsCollectionInterval_))
    , CompanionConfig_(GetCompanionExecutionConfig())
    , Logger(logger)
    , Profiler_(profiler)
    , RestartCounter_(Profiler_.Counter("/restarts"))
    , MemoryUsageGauge_(Profiler_.Gauge("/memory_usage"))
    , ThreadCountGauge_(Profiler_.Gauge("/thread_count"))
    , ErrorState_(statusProfiler->ErrorState("/companion_process"))
{ }

//////////////////////////////////////////////////////////////////////////////////

void TProcessManagerBase::Start()
{
    {
        auto guard = Guard(Lock_);
        if (Started_) {
            YT_TLOG_DEBUG("Auto-restartable companion process already started");
            return;
        }
        Started_ = true;
        RestartOnStop_ = true;
    }

    // Check if the companion port is already in use and kill any zombie process.
    KillZombieProcessesOnPort();

    YT_TLOG_DEBUG("Starting auto-restartable companion process");
    DoStart();

    // Start async health checks without waiting for the first response.
    HealthCheckExecutor_->Start();
    // Start async metrics collection.
    MetricsCollectionExecutor_->Start();
    YT_TLOG_DEBUG("Started successfully");
}

void TProcessManagerBase::Restart()
{
    {
        auto guard = Guard(Lock_);
        if (!Started_) {
            YT_TLOG_DEBUG("Auto-restartable companion process not started");
            return;
        }
        RestartOnStop_ = true;
    }
    StopIncarnation();
}

void TProcessManagerBase::Shutdown()
{
    try {
        {
            auto guard = Guard(Lock_);
            // Set RestartOnStop_ to false first to prevent any pending restarts.
            RestartOnStop_ = false;
            // Set Started_ to false to signal all loops and callbacks to exit.
            // This must be done before StopIncarnation() to ensure that:
            // 1. ScheduleRestart() callbacks will not start new processes.
            // 2. CheckCompanionAvailability() loop will exit.
            // 3. DoStart() will not create new processes.
            Started_ = false;
        }
        StopIncarnation();
        Y_UNUSED(NConcurrency::WaitFor(HealthCheckExecutor_->Stop()));
        Y_UNUSED(NConcurrency::WaitFor(MetricsCollectionExecutor_->Stop()));
    } catch (const std::exception& ex) {
        YT_TLOG_ERROR("Failed to shutdown auto-restartable companion process")
            .With(ex);
    }
}

//////////////////////////////////////////////////////////////////////////////////

TFuture<void> TProcessManagerBase::HealthCheck()
{
    return BIND([this, weakThis = MakeWeak(this)] () {
        if (auto strongThis = weakThis.Lock()) {
            CompanionClient_->GetCompanionInfo();
        }
    })
        .AsyncVia(Invoker_)
        .Run();
}

/////////////////////////////////////////////////////////////////////////////////

void TProcessManagerBase::CheckCompanionAvailability()
{
    // A freshly spawned companion may not answer health checks immediately.
    // While it is starting up, failed checks are expected and must not restart it,
    // nor be reported as an error.
    while (IsStarted() && IsWithinStartupGracePeriod()) {
        auto resultOrError = NConcurrency::WaitFor(HealthCheck());
        if (resultOrError.IsOK()) {
            YT_TLOG_DEBUG("Received heartbeat from companion");
            ErrorState_->ClearError();
            return;
        }
        YT_TLOG_DEBUG("Health check failed within startup grace period, waiting for companion to start")
            .With("StartupGracePeriod", StartupGracePeriod_)
            .With(resultOrError);
        NConcurrency::TDelayedExecutor::WaitForDuration(BackoffOptions_.MinBackoff);
    }

    // The startup grace period is over: a failed health check now means the companion
    // is unhealthy and must be restarted, escalating the backoff on each attempt.
    auto backoffStrategy = TBackoffStrategy(BackoffOptions_);
    while (IsStarted()) {
        auto resultOrError = NConcurrency::WaitFor(HealthCheck());
        if (resultOrError.IsOK()) {
            YT_TLOG_DEBUG("Received heartbeat from companion");
            ErrorState_->ClearError();
            return;
        }
        ErrorState_->SetError(resultOrError);
        if (backoffStrategy.Next()) {
            auto backoff = backoffStrategy.GetBackoff();
            YT_TLOG_WARNING("Restarting companion process after a failed health check")
                .With("Attempt", backoffStrategy.GetInvocationIndex())
                .With("MaxRestarts", backoffStrategy.GetInvocationCount())
                .With("SleepDuration", backoff);
            Restart();
            NConcurrency::TDelayedExecutor::WaitForDuration(backoff);
        } else {
            YT_TLOG_FATAL("Failed to connect to companion process")
                .With(resultOrError);
        }
    }
}

bool TProcessManagerBase::IsStarted()
{
    auto guard = Guard(Lock_);
    return Started_;
}

bool TProcessManagerBase::IsWithinStartupGracePeriod()
{
    auto guard = Guard(Lock_);
    if (!CurrentProcess_ || CurrentProcess_->IsFinished()) {
        // There is no live incarnation to wait for.
        return false;
    }
    auto incarnationAge = TInstant::Now() - IncarnationSpawnTime_;
    return incarnationAge < StartupGracePeriod_;
}

void TProcessManagerBase::DoStart()
{
    TFuture<void> spawnFuture;
    std::string commandLine;
    {
        auto guard = Guard(Lock_);
        // Check if we should still start the process.
        if (!Started_ || !RestartOnStop_) {
            YT_TLOG_DEBUG("Skipping process start")
                .With("Started", Started_)
                .With("RestartOnStop", RestartOnStop_);
            return;
        }
        try {
            ValidateParameters();
        } catch (const std::exception& ex) {
            auto error = TError("Companion process parameters are invalid") << TError(ex);
            ErrorState_->SetError(error);
            THROW_ERROR error;
        }
        auto processIncarnation = CreateProcessIncarnation();
        // Publish restarts metric.
        RestartCounter_.Increment();
        // Create process group for robust process termination.
        processIncarnation->CreateProcessGroup();
        CurrentProcess_ = processIncarnation;
        IncarnationSpawnTime_ = TInstant::Now();
        CurrentSpawnFuture_ = processIncarnation->Spawn();
        spawnFuture = CurrentSpawnFuture_;
        commandLine = processIncarnation->GetCommandLine();
    }
    spawnFuture.Subscribe(BIND([this, weakThis = MakeWeak(this), commandLine] (const TError& error) {
        if (auto strongThis = weakThis.Lock()) {
            OnProcessStopped(error, commandLine);
            return;
        }
    }));
}

void TProcessManagerBase::OnProcessStopped(const TError& error, const std::string& commandLine)
{
    {
        auto guard = Guard(Lock_);
        if (!Started_ || !RestartOnStop_) {
            YT_TLOG_INFO("Companion process stopped during shutdown")
                .With(error);
            return;
        }
    }

    auto exitCode = error.Attributes().Find<int>("exit_code");

    auto stopError = TError("Companion process was stopped")
        << error;
    if (exitCode) {
        stopError <<= TErrorAttribute("exit_code", *exitCode);
    }

    ErrorState_->SetError(stopError);

    YT_TLOG_WARNING("Companion process was stopped, scheduling restart")
        .With("ExitCode", exitCode)
        .With("CommandLine", commandLine);

    // Schedule companion start even if it fails to avoid crashing worker.
    // User might be scared of worker coredump, instead we show error status in
    // UI through StatusProfiler.
    ScheduleRestart();
}

void TProcessManagerBase::ScheduleRestart()
{
    // Check if we should still be restarting.
    bool shouldRestart = false;
    {
        auto guard = Guard(Lock_);
        shouldRestart = Started_ && RestartOnStop_;
    }
    if (shouldRestart) {
        // Use a delay to avoid rapid restart loops.
        // Note: DoStart() will re-check Started_ and RestartOnStop_ under lock
        // to handle race conditions with Shutdown().
        NConcurrency::TDelayedExecutor::Submit(
            BIND([this, weakThis = MakeWeak(this)] () {
                if (auto strongThis = weakThis.Lock()) {
                    DoStart();
                }
            }),
            RestartDelay_);
    }
}

void TProcessManagerBase::StopIncarnation()
{
    TIntrusivePtr<TProcessBase> processToKill;
    TFuture<void> spawnFutureToReset;

    {
        auto guard = Guard(Lock_);
        if (CurrentProcess_) {
            processToKill = CurrentProcess_;
            CurrentProcess_.Reset();
        }
        if (CurrentSpawnFuture_) {
            spawnFutureToReset = CurrentSpawnFuture_;
            CurrentSpawnFuture_.Reset();
        }
    }

    if (processToKill && processToKill->IsStarted()) {
        try {
            processToKill->Kill(SIGKILL);
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to kill process properly")
                .With("Pid", processToKill->GetProcessId())
                .With(TError(ex));
            KillProcessWithChildren(processToKill->GetProcessId());
        }
    }
}

void TProcessManagerBase::KillZombieProcessesOnPort()
{
    std::vector<int> zombiePids;
    // Call lsof -ti:<port> command in order to find all processes using the port.
    try {
        // Get current process PID to avoid killing worker process.
        auto currentPid = GetPID();
        auto command = TShellCommand("lsof", {Format("-ti:%v", CompanionConfig_->Port)});
        command.Run().Wait();
        const auto& output = command.GetOutput();
        if (!output.empty()) {
            // Split output by newlines to get all PIDs.
            std::istringstream iss(output);
            std::string line;
            while (std::getline(iss, line)) {
                try {
                    int pid = std::stoi(line);
                    if (pid != currentPid) {
                        zombiePids.push_back(pid);
                    }
                } catch (const std::exception&) {
                    // Skip invalid PID lines.
                    continue;
                }
            }
        }
    } catch (const std::exception&) {
        // Command failed, do not add anything into zombiePids.
    }
    if (zombiePids.empty()) {
        return;
    }
    YT_TLOG_DEBUG("Zombie processes found. Going to kill them")
        .With("Pids", zombiePids);
    for (auto pid : zombiePids) {
        KillProcessWithChildren(pid);
    }
}

void TProcessManagerBase::KillProcessWithChildren(int pid)
{
    // Stop the main process for preventing new child processes spawning.
    YT_TLOG_DEBUG("Stopping process")
        .With("Pid", pid);
    if (!TryKillProcessByPid(pid, SIGSTOP)) {
        YT_TLOG_WARNING("Failed to stop parent process")
            .With("Pid", pid);
    }
    // Get all child processes.
    auto childPids = GetPidsUnderParent(pid);
    // Kill all child processes one by one.
    for (auto childPid : childPids) {
        if (childPid != pid) {
            YT_TLOG_DEBUG("Killing child job process")
                .With("Pid", childPid);
            if (!TryKillProcessByPid(childPid, SIGKILL)) {
                YT_TLOG_DEBUG("Failed to kill child job process")
                    .With("Pid", childPid);
            }
        }
    }
    // Kill the main process.
    YT_TLOG_DEBUG("Killing process")
        .With("Pid", pid);
    if (!TryKillProcessByPid(pid, SIGKILL)) {
        YT_TLOG_DEBUG("Failed to kill companion process")
            .With("Pid", pid);
    }
}

void TProcessManagerBase::CollectCompanionMetrics()
{
    try {
        int pid;
        {
            auto guard = Guard(Lock_);
            if (!CurrentProcess_) {
                return;
            }
            pid = CurrentProcess_->GetProcessId();
        }
        // RSS summed across the companion family; over-reports due to COW-shared
        // pages — switch to smaps_rollup Pss if an exact figure is needed.
        auto totalRss = SumCompanionFamilyRss(pid);
        MemoryUsageGauge_.Update(totalRss);

        // Get the number of threads in the process using command: ps -o thcount= -p <pid>.
        auto command = TShellCommand(Format("ps -o thcount= -p %v", pid));
        command.Run().Wait();
        const auto& output = command.GetOutput();
        int threadCount = -1;
        if (!output.empty()) {
            // ps command output contains leading and trailing spaces.
            auto threadCountStr = Strip(output);
            threadCount = std::stoi(threadCountStr);
            ThreadCountGauge_.Update(threadCount);
        }
        YT_TLOG_DEBUG("Collected companion process metrics")
            .With("Pid", pid)
            .With("RSS", totalRss)
            .With("ThreadCount", threadCount);
    } catch (const std::exception& ex) {
        YT_TLOG_DEBUG("Failed to collect companion process metrics")
            .With(ex);
    }
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
