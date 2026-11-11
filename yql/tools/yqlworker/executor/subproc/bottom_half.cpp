#include "bottom_half.h"
#include "control_protocol.h"
#include "data_protocol.h"

#include <yql/tools/yqlworker/executor/common/runnable_tasks.h>
#include <yql/tools/yqlworker/executor/task_executor.h>
#include <yql/tools/yqlworker/misc/expiration_tracker.h>
#include <yql/tools/yqlworker/misc/multiprocessing.h>
#include <yql/tools/yqlworker/yt_runtime/yt_runtime.h>
#include <yql/tools/yqlworker/logging/interface.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/signals/signals.h>
#include <yql/essentials/utils/signals/utils.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/sighandler/async_signals_handler.h>
#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <util/network/poller.h>
#include <util/generic/queue.h>
#include <util/generic/yexception.h>
#include <util/generic/scope.h>
#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>
#include <util/system/fs.h>
#include <util/system/file.h>
#include <util/stream/file.h>
#include <util/string/cast.h>

#ifdef __unix__
#include <sys/resource.h>
#endif

namespace NYql {
namespace NSubprocessTaskExecutor {
namespace {

static constexpr ui32 TASK = 0x1234;

//////////////////////////////////////////////////////////////////////////////
// TTaskProcMetricsRegistry
//////////////////////////////////////////////////////////////////////////////
class TTaskProcMetricsRegistry final: public IMetricsRegistry
{
public:
    TTaskProcMetricsRegistry(
            IMetricsRegistryPtr slave,
            TDataClient& dataClient)
        : Slave_(std::move(slave))
        , DataClient_(dataClient)
    {
    }

private:
    void SetCounter(
        const TString& labelName,
        const TString& labelValue,
        i64 value,
        bool derivative) override
    {
        Slave_->SetCounter(labelName, labelValue, value, derivative);
    }

    void IncCounter(
            const TString& labelName,
            const TString& labelValue,
            bool derivative) override
    {
        Slave_->IncCounter(labelName, labelValue, derivative);
    }

    void AddCounter(
            const TString& labelName,
            const TString& labelValue,
            i64 value,
            bool derivative) override
    {
        Slave_->AddCounter(labelName, labelValue, value, derivative);
    }

    bool TakeSnapshot(NProto::TMetricsRegistrySnapshot* snapshot) const override {
        return Slave_->TakeSnapshot(snapshot);
    }

    void MergeSnapshot(const NProto::TMetricsRegistrySnapshot& snapshot) override {
        Slave_->MergeSnapshot(snapshot);
    }

    IMetricsRegistryPtr Personalized(
            const TString& userName) const override
    {
        return new TTaskProcMetricsRegistry(
                    Slave_->Personalized(userName), DataClient_);
    }

    void Flush() override {
        NProto::TMetricsRegistrySnapshotRequest req;
        if (TakeSnapshot(req.MutableSnapshot())) {
            DataClient_.SendMetricsRegistrySnapshot(std::move(req));
        }
    }

    TSensorsGroupPtr GetSensors() override {
        return Slave_->GetSensors();
    }

private:
    IMetricsRegistryPtr Slave_;
    TDataClient& DataClient_;
};

//////////////////////////////////////////////////////////////////////////////
// TProxyingWatcher
//////////////////////////////////////////////////////////////////////////////
class TProxyingWatcher final: public ITaskWatcher {
public:
    TProxyingWatcher(pid_t pid, const TString& taskId, TDataClient& dataClient, const TString& username)
        : Pid_(pid)
        , TaskId_(taskId)
        , DataClient_(dataClient)
        , Username_(username)
    {
    }

private:
    void OnStart() override {
        SyncSendImpl(CreateRequest(NProto::TTaskWatchEventRequest::STARTED));
    }

    void OnFinish(NProto::ETaskStatus status) override {
        auto req = CreateRequest(NProto::TTaskWatchEventRequest::FINISHED);
        req.SetFinishStatus(status);
        SyncSendImpl(std::move(req));
    }

    void OnResultChange(
            TTaskResultChanges::TMask changesMask,
            TTaskResultPtr changes, int workerPid, NBus::TOnMessageContext*) override
    {
        Y_UNUSED(workerPid);
        auto req = CreateRequest(NProto::TTaskWatchEventRequest::RESULTS_CHANGED);
        req.SetChangesMask(changesMask);
        *req.MutableChanges() = *changes;
        try {
            SyncSendImpl(std::move(req));
        } catch (...) {
            // Progress updates are best-effort: the core process may disappear while
            // the task is still running. Losing a result or a final status must still
            // fail the task.
            if (changesMask != TTaskResultChanges::PROGRESS) {
                throw;
            }
            YQL_LOG(ERROR) << "failed to report progress for task " << TaskId_ << ": "
                           << CurrentExceptionMessage();
        }
    }

    bool WillWatch(ui32 /*changesMask*/) const override {
        return true;
    }

    NProto::TTaskWatchEventRequest CreateRequest(
            NProto::TTaskWatchEventRequest::EWatchEvent event)
    {
        NProto::TTaskWatchEventRequest req;
        req.SetPid(Pid_);
        req.SetTaskId(TaskId_);
        req.SetEvent(event);
        req.SetUsername(Username_);
        return req;
    }

    void SyncSendImpl(NProto::TTaskWatchEventRequest&& req) {
        auto future = DataClient_.SendTaskWatchEvent(std::move(req));
        future.GetValue(TDuration::Max());
    }

private:
    const pid_t Pid_;
    const TString TaskId_;
    TDataClient& DataClient_;
    const TString Username_;
};

//////////////////////////////////////////////////////////////////////////////
// TTaskProc
//////////////////////////////////////////////////////////////////////////////
class TTaskProc
{
public:
    TTaskProc(TYqlRunnableTaskFactory& runnableTaskFactory, int dataServerPort)
        : RunnableTaskFactory_(runnableTaskFactory)
        , DataClient_(dataServerPort)
        , RunnableTask_(nullptr)
        , Pid_()
    {
    }

    int Run() {
        SetAsyncSignalFunction(ABORT_SIGNAL, [this](int) { Abort(); });
        SetAsyncSignalFunction(SIGUSR1, [](int) { NLog::ReopenLog(); });

        // other signals disposition stays the same as in parent (forker)
        // process
        AllowAnySignals();

        ConfigureYtRuntime(/*enableYpDiscovery*/ true);

        Pid_ = getpid();

        ITaskWatcherPtr taskWatcher;
        int exitCode = 0;
        try {
            using namespace std::placeholders;

            NProto::TGetTaskRequest req;
            req.SetPid(Pid_);
            auto future = DataClient_.GetTask(std::move(req));
            auto& resp = const_cast<NProto::TGetTaskResponse&>(
                        future.GetValue(TDuration::Seconds(60)));

            if (resp.GetStatus() == NProto::TGetTaskResponse::NO_MORE_TASKS) {
                YQL_LOG(WARN) << "data server respond with NO_MORE_TASKS";
                return 0;
            }

            TaskId_ = resp.GetData().GetId();
            taskWatcher.Reset(new TProxyingWatcher(Pid_, TaskId_, DataClient_, resp.GetData().GetUsername()));
            if (IsHealthCheckTask(TaskId_)) {
                SuppressLogs();
            }

            TString procSuffix = "(" + TaskId_ + ")";
            AddProcTitleSuffix(procSuffix.data());

            YQL_LOG(INFO) << "running task " << TaskId_;

            // init user affiliation
            RunnableTaskFactory_.SetUserAffiliation(resp.GetData().HasStaffAffiliation() ?
                resp.GetData().GetStaffAffiliation() : NProto::EStaffAffiliation::YANDEX);

            TryUseGatewaysConfigFromTask(resp.GetData());
            TMaybe<NProto::TFunctionRegistryData> registryData = TryUseFunctionRegistryDataFromTask(resp.GetData());

            RunnableTaskFactory_.InitGateways(std::make_shared<NProto::TTaskData>(resp.GetData()), registryData.Get());
            RunnableTaskFactory_.SetMetricsDecorator(
                        std::bind(&TTaskProc::DecorateMetrics, this, _1, _2));

            RunnableTask_ = RunnableTaskFactory_.Create(
                    resp.GetAction(), TTaskDataPtr(resp.release_data()), taskWatcher);
            RunnableTask_->Run();

            auto status = RunnableTask_->GetResult().GetStatus();
            YQL_LOG(INFO) << "done task " << TaskId_ << ' '
                          << NProto::ETaskStatus_Name(status);

            RunnableTask_ = nullptr;
        } catch (const TErrorException& exc) {
            HandleError(taskWatcher, exc.what(), false, exitCode);
        } catch (...) {
            TString message = CurrentExceptionMessage();
            YQL_LOG(ERROR) << "unhandled exception on task " << TaskId_
                           << ": " << message;
            HandleError(taskWatcher, std::move(message), true, exitCode);
        }

        YQL_LOG(INFO) << "task process shutdown";
        return exitCode;
    }

private:
    void HandleError(ITaskWatcherPtr taskWatcher, TString&& errorMessage, bool addFatalMessage, int& exitCode) {
        if (!TaskId_.empty()) {
            if (!taskWatcher) {
                throw;
            }
            OnError(taskWatcher.Get(), std::move(errorMessage), addFatalMessage);
        }
        exitCode = 1;
    }

    TMaybe<NProto::TFunctionRegistryData> TryUseFunctionRegistryDataFromTask(const NProto::TTaskData& data) {
        if (!data.HasFunctionRegistryData()) {
            return Nothing();
        }

        TString content = data.GetFunctionRegistryData();
        TStringInput in(content);
        TMaybe<NProto::TFunctionRegistryData> registryData;
        registryData.ConstructInPlace();

        ParseFromTextFormat(in, *registryData, EParseFromTextFormatOption::AllowUnknownField);

        RunnableTaskFactory_.SetFunctionRegistryData(*registryData);
        return registryData;
    }

    TGatewaysConfig MergeGatewayConfigs(const TString& configStr, const TString& configPatchStr) {
        TGatewaysConfig config;
        TStringInput configInput(configStr);
        ParseFromTextFormat(configInput, config, EParseFromTextFormatOption::AllowUnknownField);

        if (configPatchStr.empty()) {
            return config;
        }

        TGatewaysConfig configPatch;
        TStringInput configPatchInput(configPatchStr);
        ParseFromTextFormat(configPatchInput, configPatch, EParseFromTextFormatOption::AllowUnknownField);

        // Merge Kikimr clusters
        if (config.HasKikimr() && configPatch.HasKikimr()) {
            THashSet<TString> kikimrClusters;
            for (auto& cluster : config.GetKikimr().GetClusterMapping()) {
                kikimrClusters.emplace(cluster.GetName());
            }

            for (auto& cluster : configPatch.GetKikimr().GetClusterMapping()) {
                if (kikimrClusters.contains(cluster.GetName())) {
                    continue;
                }

                config.MutableKikimr()->AddClusterMapping()->CopyFrom(cluster);
            }
        }

        // Merge Ydb clusters
        if (config.HasYdb() && configPatch.HasYdb()) {
            THashSet<TString> ydbClusters;
            for (const auto& cluster : config.GetYdb().GetClusterMapping()) {
                ydbClusters.emplace(cluster.GetName());
            }

            for (const auto& cluster : configPatch.GetYdb().GetClusterMapping()) {
                if (ydbClusters.contains(cluster.GetName())) {
                    continue;
                }

                config.MutableYdb()->AddClusterMapping()->CopyFrom(cluster);
            }
        }

        // Update CH Tokens
        if (config.HasClickHouse() && configPatch.HasClickHouse()) {
            THashMap<TString, TString> chTokens;
            for (auto& cluster : configPatch.GetClickHouse().GetClusterMapping()) {
                chTokens[cluster.GetName()] = cluster.GetCHToken();
            }

            for (auto& cluster : *config.MutableClickHouse()->MutableClusterMapping()) {
                if (chTokens.contains(cluster.GetName())) {
                    cluster.SetCHToken(chTokens[cluster.GetName()]);
                }
            }
        }

        return config;
    }

    TMaybe<TString> ValidateGatewaysConfig(const TGatewaysConfig& config) {
        if (!config.HasYt() && !config.HasKikimr() && !config.HasRtmr()) {
            return "No gateways configured in TryUseGatewaysConfigFromTask";
        }

        if (config.HasYt()) {
            if (auto logSize = config.GetYt().GetYtDebugLogSize(); logSize && (logSize < 10_KB || logSize > 10_MB)) {
                return TStringBuilder() << "Internal error. Invalid gateways config YtDebugLogSize value: " << config.GetYt().GetYtDebugLogSize();
            }
        }
        return {};
    }

    void TryUseGatewaysConfigFromTask(const NProto::TTaskData& data) {
        if (!data.HasGatewaysConfig()) {
            return;
        }

        auto config = MergeGatewayConfigs(data.GetGatewaysConfig(), data.GetGatewaysConfigPatch());

        if (auto err = ValidateGatewaysConfig(config)) {
            YQL_LOG(ERROR) << *err
                << ", config: " << data.GetGatewaysConfig()
                << ", config patch: " << data.GetGatewaysConfigPatch();
            ythrow yexception() << *err;
        }

        RunnableTaskFactory_.SetGatewaysConfig(config);
    }

    void Abort() {
        if (IYqlRunnableTaskPtr task = RunnableTask_) {
            YQL_LOG(INFO) << "will abort task " << TaskId_;
            task->Abort();
        }
    }

    IMetricsRegistryPtr DecorateMetrics(
            const IMetricsRegistryPtr& old, const TString& username)
    {
        auto metrics = old->Personalized(username);
        return new TTaskProcMetricsRegistry(std::move(metrics), DataClient_);
    }

    void OnError(ITaskWatcher* watcher, TString&& message, bool addFatalMessage) const {
        try {
            TTaskResultPtr fakeResults = std::make_shared<NProto::TTaskResult>();
            fakeResults->SetStatus(NProto::ETaskStatus::ERROR);
            if (addFatalMessage) {
                AddFatalMessageToTaskResult(fakeResults.get(), message);
            } else {
                AddErrorMessageToTaskResult(fakeResults.get(), message);
            }

            watcher->OnResultChange(
                    TTaskResultChanges::STATUS_ERROR | TTaskResultChanges::ERRORS,
                    std::move(fakeResults));

            watcher->OnFinish(NProto::ETaskStatus::ERROR);
        } catch (...) {
            YQL_LOG(ERROR) << CurrentExceptionMessage();
        }
    }

    static void SuppressLogs() {
        NLog::TComponentHelpers::ForEach([](NLog::EComponent c) {
            NLog::YqlLogger().SetComponentLevel(c, NLog::ELevel::WARN);
        });
    }

private:
    TYqlRunnableTaskFactory& RunnableTaskFactory_;
    TDataClient DataClient_;
    IYqlRunnableTaskPtr RunnableTask_;
    pid_t Pid_;
    TString TaskId_;
};

} // namespace

struct TForkerProcess::TImpl: public IWorkerControlServerHandler
{
    TMainConfig Config;
    TYqlRunnableTaskFactory RunnableTaskFactory;
    TControlServer ControlServer;
    TProcessPool ChildrenPool;
    TQueue<std::pair<ui32, void*>> PendingSpawnCommands;
    TExpirationTracker<pid_t> ExpirationTracker;
    const TDuration TaskGracefulExitTimeout;
    // these processes will receive SIGKILL, but we should treat this action as ABORT
    THashSet<pid_t> AbortedUsingKillChildren;

    TImpl(const TMainConfig& config,
        const TMaybe<NProto::TDqConfig>& dqConfig,
        TIpcChannelEndpoint& coreProcessCh,
        IMetricsRegistryPtr metrics)
        : Config(config)
        , RunnableTaskFactory(Config.Gateways, Config.Worker.GetStaticGateways(), Config.FileStorage, Config.Worker.GetExecutor(), dqConfig, std::move(metrics), &TForkerProcess::TImpl::InterruptionPointFunc)
        , ControlServer(coreProcessCh, SignalPipeR.GetHandle(), this)
        , TaskGracefulExitTimeout(TDuration::Seconds(Config.Worker.GetExecutor().GetTaskGracefulExitTimeoutSeconds()))
    {
        TString logLimit = Config.Worker.GetExecutor().GetLogLimit();
        const ui32 memLimitMb = Config.Worker.GetExecutor().GetMemoryLimitMb();
        const auto loggingConfig = Config.Worker.GetLogging();
        ChildrenPool.Add(TASK, "TASK", 0, [this, logLimit, loggingConfig, memLimitMb](void* param) {
            ExtLoggingBackends(loggingConfig);
            NLog::YqlLogger().UpdateProcInfo("task");
            SetProcTitle("task process");
            if (memLimitMb > 0) {
#if defined(__SANITIZE_ADDRESS__) || __has_feature(address_sanitizer)
                YQL_LOG(ERROR) << "Memory limit can not be used with address sanitizer";
#elif defined (__unix__)
                const ui64 memLimit = static_cast<ui64>(memLimitMb) * 1024ul * 1024ul;
                struct rlimit rl;

                if (getrlimit(RLIMIT_AS, &rl)) {
                    ythrow TSystemError() << "Cannot getrlimit(RLIMIT_AS)";
                }

                rl.rlim_cur = memLimit;
                if (setrlimit(RLIMIT_AS, &rl)) {
                    ythrow TSystemError() << "Cannot setrlimit(RLIMIT_AS) to " << memLimit << " bytes";
                }
                YQL_LOG(INFO) << "Set RLIMIT_AS to " << memLimit << " bytes";
#else
                YQL_LOG(ERROR) << "Memory limit can not be set on this platfrom";
#endif
            }

            if (logLimit) {
                ui64 limit = 0;
                try {
                    limit = NSize::ParseSize(logLimit);
                } catch (...) {
                    YQL_LOG(ERROR) << "Bad value of log limit: " << logLimit;
                }
                if (limit) {
                    NLog::YqlLogger().SetMaxLogLimit(limit);
                }
            }

            YQL_LOG(INFO) << "task process started";

            int dataServerPort = static_cast<int>(reinterpret_cast<intptr_t>(param));

            TTaskProc taskProc(RunnableTaskFactory, dataServerPort);
            return taskProc.Run();
        });
    }

    int Run() {
        YQL_LOG(DEBUG) << "forker server ready";
        // Core may survive a forker restart with tasks whose spawn requests were lost.
        // Notify it proactively so it can resubmit spawns for those pending tasks.
        try {
            ControlServer.NotifyAboutSelfReady();
        } catch (const TPeerDead&) {
            YQL_LOG(WARN) << "core process is dead, skip ready notification";
        }

        int exitCode = 0;
        try {
            bool terminating = false;
            while (!terminating || !ChildrenPool.Empty()) {
                try {
                    ControlServer.EventLoopCycle();
                } catch (const TPeerDead&) {
                    YQL_LOG(WARN) << "it seems that CORE process is dead wait "
                                     "3 seconds and will try again";
                    Sleep(TDuration::Seconds(3));
                }

                if (NeedTerminate || NeedQuit) {
                    NeedTerminate = NeedQuit = 0;
                    terminating = true;
                    AddProcTitleSuffix("[terminating]");
                    ChildrenPool.Shutdown(SIGTERM);
                    PendingSpawnCommands.clear();
                } else if (NeedReopenLog) {
                    NeedReopenLog = 0;
                    NLog::ReopenLog();
                    Config.ReopenStdErr();
                } else if (NeedReapZombies) {
                    NeedReapZombies = 0;
                    TVector<TProcessPool::TExitStatus> statuses;
                    ChildrenPool.ReapZombies(&statuses);
                    TVector<std::pair<pid_t, int>> pairStatues;
                    // no need to track interrupt signal expiration
                    for (auto& s : statuses) {
                        ExpirationTracker.Remove(s.Pid);
                        TProcessStatus procStatus(s.ExitCode);
                        if (AbortedUsingKillChildren.contains(s.Pid) && procStatus.IsTerminated() && procStatus.GetTerminationSignal() != ABORT_SIGNAL) {
                            // substitute signo
                            s.ExitCode = procStatus.ReplaceTerminationSignal(ABORT_SIGNAL).GetRawStatus();
                        }

                        AbortedUsingKillChildren.erase(s.Pid);
                        pairStatues.emplace_back(s.Pid, s.ExitCode);
                    }

                    ControlServer.NotifyAboutTerminatedWorkers(pairStatues);
                } else {
                    ProcessPendingSpawnCommands();
                    ProcessHungAbortedTasks();
                }
            }

            ControlServer.NotifyAboutSelfTermination();
        } catch (...) {
            YQL_LOG(ERROR) << "unhandled exception: "
                           << CurrentExceptionMessage();
            exitCode = 1;
        }

        // there must not be alive child processes at this point
        // but if any of them exists we must abort them, because they have
        // no ability to report status updates to core process anyway
        try {
            ChildrenPool.Shutdown(ABORT_SIGNAL);
            ChildrenPool.Wait();
        } catch (...) {
            YQL_LOG(ERROR) << "unexpected exception: "
                           << CurrentExceptionMessage();
            exitCode = 2;
        }

        YQL_LOG(INFO) << "forker process shutdown";
        return exitCode;
    }

private:
    static void InterruptionPointFunc() {
        if (NeedTerminate || NeedQuit || HasPendingQuitOrTerm()) {
            YQL_LOG(INFO) << "forker initialization has been cancelled";
            throw yexception() << "forker cancelled";
        }
    }

    void ProcessPendingSpawnCommands() {
        // fast exit
        // container is used in single thread
        if (PendingSpawnCommands.empty()) {
            return;
        }

        YQL_LOG(INFO) << "Trying to spawn pending tasks, have " << PendingSpawnCommands.size() << " commands";
        while (!PendingSpawnCommands.empty()) {
            auto& p = PendingSpawnCommands.front();
            const ui32 remaining = SpawnWorkers(p.first, p.second);

            if (remaining) {
                // no luck
                p.first = remaining;
                return;
            }

            PendingSpawnCommands.pop();
        }

        YQL_LOG(INFO) << "Pending tasks spawned completely";
    }

    ui32 SpawnWorkers(ui32 count, void* param) {
        if (!count) {
            YQL_LOG(WARN) << "Have request to spawn zero workers";
            return 0;
        }

        try {
            do {
                ChildrenPool.SpawnOneMore(TASK, param);
            } while (--count);
        } catch (const TSystemError&) {
            // interested in fork() error only
            YQL_LOG(ERROR) << "Failed to spawn task. Will try later. Error: " << CurrentExceptionMessage();
        }

        return count;
    }

    void OnSpawnWorkers(ui32 count, int dataServerPort) override {
        void* param = reinterpret_cast<void*>(dataServerPort);
        const ui32 remaining = SpawnWorkers(count, param);
        if (remaining) {
            PendingSpawnCommands.emplace(remaining, param);
        }
    }

    void OnKillWorker(pid_t pid) override {
        ChildrenPool.SendSignal(pid, ABORT_SIGNAL);
        TInstant expireAt = TInstant::Now() + TaskGracefulExitTimeout;
        ExpirationTracker.Add(expireAt, pid);
    }

    void OnRequestReady() override {
        ControlServer.NotifyAboutSelfReady();
    }

    void OnDisconnect() override {
        // forker does not have master and core in team starting from now
    }

    void ProcessHungAbortedTasks() {
        ExpirationTracker.Process(TInstant::Now(), [this](pid_t pid) {
            YQL_LOG(WARN) << "Task process (pid=" << pid << ") graceful exit timed out. Sending SIGKILL";
            ChildrenPool.SendSignal(pid, SIGKILL);
            AbortedUsingKillChildren.emplace(pid);
        });
    }
};

TForkerProcess::TForkerProcess(const TMainConfig& config,
    const TMaybe<NProto::TDqConfig>& dqConfig,
    TIpcChannelEndpoint& coreProcessCh,
    IMetricsRegistryPtr metrics)
    : Impl_(new TImpl(config, dqConfig, coreProcessCh, std::move(metrics)))
{
}

TForkerProcess::~TForkerProcess()
{
}

int TForkerProcess::Run()
{
    return Impl_->Run();
}

} // namespace NSubprocessTaskExecutor
} // namespace NYql
