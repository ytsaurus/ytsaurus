#include "bottom_half.h"
#include "control_protocol.h"
#include "data_protocol.h"
#include "task_index.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/tools/yqlworker/config/config.h>
#include <yql/tools/yqlworker/executor/task_executor.h>
#include <yql/tools/yqlworker/misc/multiprocessing.h>
#include <yql/essentials/utils/signals/signals.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>

#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <util/string/builder.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/system/event.h>
#include <util/system/file.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/folder/path.h>


namespace NYql {
namespace NSubprocessTaskExecutor {
namespace {

//////////////////////////////////////////////////////////////////////////////
// TSubprocessTaskExecutorTopHalf
//////////////////////////////////////////////////////////////////////////////
class TSubprocessTaskExecutorTopHalf final:
        public ITaskExecutor,
        public IDataServerHandler,
        public IWorkerTerminationHandler
{
public:
    TSubprocessTaskExecutorTopHalf(
            const NProto::TTaskExecutorConfig& executorConfig,
            TIpcChannelEndpoint& forkerProcessCh,
            IMetricsRegistryPtr metrics,
            const TVector<NBus::TBindResult>& serverPorts)
        : ExecutorConfig_(executorConfig)
        , Metrics_(std::move(metrics))
        , Tasks_(executorConfig.GetTaskProcessTrackDir())
        , DataServer_(serverPorts, 4, this)
        , ControlClient_(forkerProcessCh, this)
        , IsReady_(0)
        , Started_(false)
        , MaxRunningTasksRss(0)
    {
        if (ExecutorConfig_.HasCGroupPrefix()) {
            auto path = TFsPath(ExecutorConfig_.GetCGroupPrefix()) / "memory.stat";
            if (!path.Exists() || !path.IsFile()) {
                ythrow yexception() << "Invalid CGroupPrefix value. Path: " << path.GetPath() << " doesn't exist";
            }
            RssStatFile = path;

            try {
                path = TFsPath(ExecutorConfig_.GetCGroupPrefix()) / "memory.limit_in_bytes";
                auto file = TFile(path, OpenExisting | RdOnly | Seq);
                MaxRunningTasksRss = FromString<ui64>(StripString(TIFStream(file).ReadAll()));
            } catch (...) {
                ythrow yexception() << "Cannot read current RSS limit from the : " << path.GetPath() << ": " << CurrentExceptionMessage();
            }
        }
        if (ExecutorConfig_.HasMaxRunningTasksRssDelta()) {
            ui64 delta = 0;
            try {
                delta = NSize::ParseSize(ExecutorConfig_.GetMaxRunningTasksRssDelta());
            } catch (const yexception& e) {
                YQL_LOG(ERROR) << "Invalid MaxRunningTasksRssDelta value: " << e.what();
                ythrow yexception() << "Invalid MaxRunningTasksRssDelta value: " << e.what();
            }
            if (MaxRunningTasksRss < delta) {
                ythrow yexception() << "MaxRunningTasksRssDelta (" << delta << ") exceeds the current MaxRunningTasksRss value: " << MaxRunningTasksRss;
            }
            MaxRunningTasksRss -= delta;
        }
        if (ExecutorConfig_.HasRssStatFile()) {
            RssStatFile = ExecutorConfig_.GetRssStatFile();
        }
        if (ExecutorConfig_.HasMaxRunningTasksRss()) {
            try {
                MaxRunningTasksRss = NSize::ParseSize(ExecutorConfig_.GetMaxRunningTasksRss());
            } catch (const yexception& e) {
                YQL_LOG(ERROR) << "Invalid MaxRunningTasksRss value: " << e.what();
                ythrow yexception() << "Invalid MaxRunningTasksRss value: " << e.what();
            }
        }
        if (MaxRunningTasksRss) {
            if (auto rss = ReadCurrentRss()) {
                YQL_LOG(INFO) << "MaxRunningTasksRss: " << MaxRunningTasksRss << ", current RSS: " << *rss;
            } else {
                ythrow yexception() << "RssStatFile value is not defined or has invalid value: " << RssStatFile;
            }
        }
        for (const auto& userLimit : ExecutorConfig_.GetQueuePerUserLimit()) {
            if (userLimit.HasUsername() && !userLimit.GetUsername().empty()) {
                QueuePerUserLimits_.emplace(userLimit.GetUsername(), userLimit);
            }
        }
    }

    void Shutdown(std::function<void()> onIdle) override {
        if (!Started_) {
            AllTasksDone_.Signal();
            return;
        }

        if (!onIdle) {
            AllTasksDone_.WaitI();
            return;
        }

        const TDuration duration = TDuration::Seconds(5);
        while (!AllTasksDone_.WaitT(duration)) {
            onIdle();
        }
    }

private:

    // -- ITaskExecutor ------------------------------------------------------

    void Startup(TOrphanTaskWatcherFactory factory) override {
        OrphanWatcherFactory_ = factory;
        Started_ = true;
        ControlClient_.RequestReady();
        ControlClient_.RunEventLoopInBackground();
    }

    EProcessTaskStatus ProcessTask(
            NProto::ETaskAction action,
            TTaskDataPtr data,
            ITaskWatcherPtr watcher) override
    {
        Metrics_->SetCounter("worker_conf", "task_limit", ExecutorConfig_.GetQueueMaxSize());
        with_lock(TasksLock_) {
            ui32 userQueueLimit = 0;
            bool ignoreQueueMaxSize = false;
            if (const auto it = QueuePerUserLimits_.find(data->GetUsername()); it != QueuePerUserLimits_.cend()) {
                userQueueLimit = it->second.GetLimit() ? it->second.GetLimit() : ExecutorConfig_.GetQueuePerUserMaxSize();
                ignoreQueueMaxSize = it->second.GetIgnoreQueueMaxSize();
            } else if (ExecutorConfig_.GetQueuePerUserMaxSize()) {
                userQueueLimit = ExecutorConfig_.GetQueuePerUserMaxSize();
            }
            // QueueMaxSize == 0 -> unlimited queue
            if (ExecutorConfig_.GetQueueMaxSize() && (Tasks_.GetSize() >= ExecutorConfig_.GetQueueMaxSize()) && !ignoreQueueMaxSize) {
                YQL_LOG(WARN) << "Task " << data->GetId() << " is rejected, working queue is full, limit = " << ExecutorConfig_.GetQueueMaxSize();
                return NProto::TProcessTaskResponse::WORKING_QUEUE_IS_FULL;
            }
            if (userQueueLimit > 0) {
                if (Tasks_.GetSizeByUser(data->GetUsername()) >= userQueueLimit) {
                    YQL_LOG(WARN) << "Task " << data->GetId() << " is rejected, per-user working queue for " << data->GetUsername() << " is full, limit = " << userQueueLimit;
                    return NProto::TProcessTaskResponse::WORKING_USER_QUEUE_IS_FULL;
                }
            }

            if (MaxRunningTasksRss && Tasks_.GetSize()) {
                const bool auxTask = IsHealthCheckTask(data->GetId())
                    || data->GetIsSystemRequest()
                    || NProto::ETaskAction::EXTRACT_PARAMS_META == action;
                if (!auxTask) {
                    if (auto currentRss = ReadCurrentRss().GetOrElse(0ull); currentRss > MaxRunningTasksRss) {
                        YQL_LOG(INFO) << "Task " << data->GetId() << " is rejected, current running tasks RSS " << currentRss << " exceeds the limit :"
                            << MaxRunningTasksRss;
                        return NProto::TProcessTaskResponse::TOO_MANY_RUNNING_TASKS;
                    }
                }
            }

            if (!Tasks_.PushPendingTask(TInstant::Now(), action, data, watcher)) {
                YQL_LOG(WARN) << "Task " << data->GetId() << " already registered";
                return NProto::TProcessTaskResponse::ALREADY_RUNNING;
            }
        }

        UpdateMonitoringStats();
        YQL_LOG(DEBUG) << "Request spawn worker for task " << data->GetId();
        try {
            ControlClient_.SpawnWorkers(1, DataServer_.ActualPort());
        } catch (...) {
            YQL_LOG(WARN) << "Cannot spawn worker for task " << data->GetId()
                          << ": " << CurrentExceptionMessage();
            bool removed = false;
            with_lock(TasksLock_) {
                removed = Tasks_.RemovePendingTask(data->GetId());
            }
            UpdateMonitoringStats();

            if (removed) {
                return NProto::TProcessTaskResponse::FAIL;
            }

            YQL_LOG(WARN) << "Task " << data->GetId() << " was already dequeued while handling spawn failure";
        }
        return NProto::TProcessTaskResponse::SUBMITED;
    }

    EKillTaskStatus KillTask(const TString& taskId) override {
        pid_t pid = 0;
        with_lock(TasksLock_) {
            ITaskWatcherPtr unusedWatcher;
            bool unusedIsFinished = false;

            if (!Tasks_.FindRunningTaskWatcher(taskId, unusedWatcher, pid, unusedIsFinished)) {
                return NProto::TKillTaskResponse::NOT_RUNNING;
            }
        }

        ControlClient_.KillWorker(pid);
        UpdateMonitoringStats();
        return NProto::TKillTaskResponse::KILLED;
    }

    bool HasRunningTask(const TString& taskId) const override {
        // no need to allow subscription to existing finished task
        bool isFinished = false;
        with_lock(TasksLock_) {
            return Tasks_.HasRunningTask(taskId, isFinished) && !isFinished;
        }
    }

    bool IsReady() override {
        if (AtomicGet(IsReady_) == 0) {
            ControlClient_.RequestReady();
            return false;
        }
        return true;
    }

    ITaskWatcherPtr GetRunningTaskWatcher(const TString& taskId) const override {
        ITaskWatcherPtr result;
        with_lock(TasksLock_) {
            pid_t unusedPid = 0;
            bool unusedIsFinished = false;
            if (!Tasks_.FindRunningTaskWatcher(taskId, result, unusedPid, unusedIsFinished)) {
                return nullptr;
            }

            return result;
        }
    }

    ui32 GetRunningTasksCount() const override {
        with_lock(TasksLock_) {
            return Tasks_.GetSize();
        }
    }

    // -- IDataServerHandler -------------------------------------------------

    NProto::TGetTaskResponse OnGetTask(NProto::TGetTaskRequest&& req) override {
        TInstant pushTime;
        NProto::ETaskAction action = NProto::ETaskAction::UNKNOWN_ACTION;
        TTaskDataPtr data;
        const pid_t pid = req.GetPid();
        with_lock (TasksLock_) {
            if (!Tasks_.MarkNextPendingTaskAsRunning(pid, pushTime, action, data)) {
                YQL_LOG(WARN) << "task process (pid=" << pid << ") requested task,"
                    " but there is no more tasks to execute or process already took the task";

                NProto::TGetTaskResponse resp;
                resp.SetStatus(NProto::TGetTaskResponse::NO_MORE_TASKS);
                return resp;
            }
        }

        UpdateMonitoringStats();

        const TDuration d = TInstant::Now() - pushTime;
        Metrics_->SetCounter("delay_ms", "take_task", (int64_t)d.MilliSeconds());

        YQL_LOG(INFO) << "task process (pid=" << pid << ") took task " << data->GetId();

        NProto::TGetTaskResponse resp;
        resp.SetStatus(NProto::TGetTaskResponse::OK);
        resp.SetAction(action);
        *resp.MutableData() = *data;
        return resp;
    }

    void OnTaskWatchEvent(NProto::TTaskWatchEventRequest&& request, NBus::TOnMessageContext* taskWatchCtx) override {

        const TString& taskId = request.GetTaskId();
        const pid_t pid = request.GetPid();
        const auto event = request.GetEvent();
        const TString username = request.GetUsername();

        ITaskWatcherPtr watcher;
        with_lock (TasksLock_) {
            bool isFinished = false;
            pid_t foundPid = 0;
            if (!Tasks_.FindRunningTaskWatcher(taskId, watcher, foundPid, isFinished)) {
                // handle orphan task
                if (!OrphanWatcherFactory_) {
                    YQL_LOG(ERROR) << "Unable to handle orphan task " << taskId << " running on pid=" << pid << ": can not create watcher";
                    return;
                }
                YQL_LOG(WARN) << "Got task watch event from orphan task " << taskId << " running on pid=" << pid;
                watcher = OrphanWatcherFactory_(taskId, username);
                foundPid = pid;
                isFinished = false;
                if (!Tasks_.AdoptOrphanTask(pid, taskId, username, watcher)) {
                    return;
                }
            }

            if (isFinished || foundPid != pid) {
                YQL_LOG(WARN) << "task process (pid=" << pid
                    << ") sends watch event " << NProto::TTaskWatchEventRequest::EWatchEvent_Name(event) << " for not running task "
                    << taskId << ", found pid=" << foundPid << ", finished=" << isFinished;
                return;
            }
        }

        switch (event) {
        case NProto::TTaskWatchEventRequest::STARTED:
            watcher->OnStart();
            break;

        case NProto::TTaskWatchEventRequest::FINISHED:
            with_lock(TasksLock_) {
                if (!Tasks_.MarkRunningTaskAsFinished(TInstant::Now(), pid, taskId)) {
                    YQL_LOG(WARN) << "task process (pid=" << pid
                        << ") failed to mark as finished task "
                        << taskId;
                    return;
                }
            }

            watcher->OnFinish(request.GetFinishStatus());
            break;

        case NProto::TTaskWatchEventRequest::RESULTS_CHANGED:
            watcher->OnResultChange(
                        request.GetChangesMask(),
                        TTaskResultPtr(request.release_changes()),
                        pid,
                        taskWatchCtx);
            break;

        default:
            YQL_LOG(ERROR) << "unknown watch event: "
                    << NProto::TTaskWatchEventRequest::EWatchEvent_Name(event);
        }
    }

    void OnMetricsRegistrySnapshot(
            NProto::TMetricsRegistrySnapshotRequest&& request) override
    {
        Metrics_->MergeSnapshot(request.GetSnapshot());
    }


    // -- IWorkerTerminationHandler ------------------------------------------

    void OnWorkerTermination(pid_t pid, int status) override {
        const TProcessStatus procStatus(status);

        ITaskWatcherPtr watcher;
        bool isFinished = false;
        TString username;
        with_lock(TasksLock_) {
            if (!Tasks_.RemoveRunningTask(pid, username, watcher, isFinished)) {
                YQL_LOG(WARN) << "no task found for task process " << pid << ' ' << procStatus;
                // no return
            }
        }

        if (!watcher) {
            // Spare worker exits cleanly after NO_MORE_TASKS; it never ran a task
            // and must not grab a pending one on shutdown.
            if (procStatus.IsExited() && procStatus.GetExitStatus() == 0) {
                return;
            }

            if (!EmulateGetPendingTask(pid)) {
                return;
            }

            // recursion, yes
            OnWorkerTermination(pid, status);
            return;
        }

        UpdateMonitoringStats();
        Metrics_->SetCounter("worker_conf", "task_limit", 0);
        if (procStatus.IsExited()) {
            // in some corner cases task process is unable to call OnFinished
            // do it by ourselves
            if (!isFinished) {
                const TString message = TStringBuilder() << "Worker " << procStatus
                    << ", task(pid=" << pid << ") did not complete operation";

                FinishWithError(watcher, message);
                return;
            }
        } else if (procStatus.IsTerminated()) {
            // Task process is terminated by a signal (aborted by the user
            // created this task or crashed).

            if (procStatus.GetTerminationSignal() == ABORT_SIGNAL) {
                if (!isFinished) {
                    watcher->OnFinish(NProto::ETaskStatus::ABORTED);
                    return;
                }
            } else {
                auto metrics = Metrics_->Personalized(username);
                metrics->IncCounter("crash", TStringBuilder()
                        << "signal=" << procStatus.GetTerminationSignal());

                // TODO (jamel): grab backtrace from crashed process
                //       if procStatus.IsCoreDumped()

                const TString message = TStringBuilder() << "Worker " << procStatus
                        << ", task(pid=" << pid << ")." << Endl
                        << "This is abnormal situation, so consider opening a bug report because"
                        << " more details are only available in server side logs and/or coredumps.";

                FinishWithError(watcher, message);
            }
        }
    }

    void FinishAllUnfinishedTasksWithError(const TString& message) {
        TVector<ITaskWatcherPtr> watchers;
        with_lock(TasksLock_) {
            watchers = Tasks_.ExtractAllUnfinishedTaskWatchers();
        }

        for (const auto& watcher : watchers) {
            try {
                FinishWithError(watcher, message);
            } catch (...) {
                YQL_LOG(ERROR) << "Failed to finish task on disconnect: " << CurrentExceptionMessage();
            }
        }

        UpdateMonitoringStats();
    }

    void OnServerTermination() override {
        FinishAllUnfinishedTasksWithError("Task executor server disconnected");
        AllTasksDone_.Signal();
    }

    void OnServerReady(bool inTerminatingState) override {
        if (inTerminatingState) {
            NeedTerminate = 1;
        }
        AtomicSet(IsReady_, 1);
        if (!inTerminatingState) {
            ResubmitPendingSpawns();
        }
    }

    void OnDisconnect() override {
        OnServerTermination();
    }

    void FinishWithError(ITaskWatcherPtr watcher, const TString& message) {
        TTaskResultPtr fakeResult = std::make_shared<NProto::TTaskResult>();
        fakeResult->SetStatus(NProto::ETaskStatus::ERROR);
        AddFatalMessageToTaskResult(fakeResult.get(), message);
        watcher->OnResultChange(
            TTaskResultChanges::STATUS_ERROR |
            TTaskResultChanges::ERRORS,
            fakeResult);
        watcher->OnFinish(NProto::ETaskStatus::ERROR);
    }

    bool EmulateGetPendingTask(pid_t pid) {
        TTaskDataPtr data;
        with_lock(TasksLock_) {
            if (Tasks_.GetPendingSize() == 0) {
                return false;
            }

            TInstant pushTime;
            NProto::ETaskAction action = NProto::ETaskAction::UNKNOWN_ACTION;

            if (!Tasks_.MarkNextPendingTaskAsRunning(pid, pushTime, action, data)) {
                YQL_LOG(WARN) << "failed to pick next pending task for task process " << pid;
                return false;
            }
        }

        YQL_LOG(INFO) << "task " << data->GetId() << " has been associated with task process " << pid;
        return true;
    }

    void UpdateMonitoringStats() {
        with_lock(TasksLock_) {
            Metrics_->SetCounter("tasks", "all", Tasks_.GetSize());
            Metrics_->SetCounter("tasks", "pending", Tasks_.GetPendingSize());
            Metrics_->SetCounter("delay_ms", "pending_max_age", Tasks_.GetPendingTaskMaxAgeMs(TInstant::Now()));
        }
    }

    void ResubmitPendingSpawns() {
        ui64 pendingCount = 0;
        with_lock(TasksLock_) {
            pendingCount = Tasks_.GetPendingSize();
        }
        if (pendingCount == 0) {
            return;
        }

        YQL_LOG(INFO) << "Request respawn workers for " << pendingCount << " pending tasks";
        try {
            ControlClient_.SpawnWorkers(static_cast<ui32>(pendingCount), DataServer_.ActualPort());
        } catch (const TPeerDead&) {
            YQL_LOG(WARN) << "Forker is dead, cannot resubmit " << pendingCount << " pending tasks";
        }
    }

    TMaybe<ui64> ReadCurrentRss() {
        try {
            auto file = TFile(RssStatFile, OpenExisting | RdOnly | Seq);
            auto stream = TIFStream(file);
            TString line;
            while (stream.ReadLine(line)) {
                if (line) {
                    TString name;
                    TString value;
                    Split(line, ' ', name, value);
                    if (name == "rss") {
                        return FromString<ui64>(value);
                    }
                    line.clear();
                }
            }
        } catch (const std::exception& e) {
            YQL_LOG(ERROR) << "Error reading RSS from " << RssStatFile << ":" << e.what();
        }
        return Nothing();
    }

private:
    const NProto::TTaskExecutorConfig ExecutorConfig_;
    TManualEvent AllTasksDone_;
    IMetricsRegistryPtr Metrics_;

    TAdaptiveLock TasksLock_;
    TTaskIndex Tasks_;
    TOrphanTaskWatcherFactory OrphanWatcherFactory_;

    TDataServer DataServer_; // must be stopped first because this class implements IDataServerHandler
    TControlClient ControlClient_;
    TAtomic IsReady_;
    bool Started_;

    ui64 MaxRunningTasksRss;
    TString RssStatFile;
    THashMap<TString, const NProto::TQueuePerUserLimit&> QueuePerUserLimits_;
};

} // namespace
} // namespace NSubprocessTaskExecutor

ITaskExecutorPtr CreateSubprocessTaskExecutor(
        const NProto::TTaskExecutorConfig& executorConfig,
        TIpcChannelEndpoint& forkerProcessCh,
        IMetricsRegistryPtr metrics,
        const TVector<NBus::TBindResult>& serverPorts)
{
    using namespace NSubprocessTaskExecutor;
    return MakeIntrusive<TSubprocessTaskExecutorTopHalf>(
                executorConfig, forkerProcessCh, std::move(metrics), serverPorts);
}

} // namespace NYql
