#include "worker_core.h"
#include "task_subscriptions.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/signals/signals.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/essentials/providers/common/metrics/sensors_group.h>
#include <yql/tools/yqlworker/config/config.h>
#include <yql/tools/yqlworker/core/inspector_status_pusher.h>
#include <yql/tools/yqlworker/executor/common/task_progress_merger.h>
#include <yql/tools/yqlworker/executor/subproc/data_protocol.h>
#include <yql/tools/yqlworker/executor/task_executor.h>
#include <yql/tools/yqlworker/misc/background_threads.h>
#include <yql/tools/yqlworker/rpc/inspector_client.h>
#include <yql/tools/yqlworker/rpc/worker_server.h>
#include <yql/tools/yqlworker/solomon_agent/solomon_agent_client.h>

#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/guid.h>
#include <util/string/cast.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/hostname.h>
#include <util/system/file_lock.h>
#include <util/system/guard.h>
#include <util/system/fs.h>
#include <util/system/getpid.h>
#include <util/folder/path.h>
#include <util/thread/pool.h>

namespace NYql {
namespace {

static const ui64 DEFAULT_MAX_HEARTBEAT_GAP_MILLIS = 16000;
const TString WORKER_HOSTNAME = HostName();
const TString WORKER_VERSION = GetProgramCommitId();

void ReopenLogIfNeeded(TMainConfig& config) {
    if (!NeedReopenLog) {
        return;
    }

    NeedReopenLog = 0;
    NLog::ReopenLog();
    config.ReopenStdErr();
}

bool ShouldStopCore() {
    return NeedTerminate || NeedQuit;
}

TString FormatRequest(const NProto::TTaskResult& request) {
    NProto::TTaskResult copy(request);
    copy.ClearAst();
    copy.ClearPlan();
    copy.ClearErrors();
    copy.ClearIssues();
    copy.ClearResults();
    copy.ClearStatistics();
    return PbMessageToStr(copy);
}

//////////////////////////////////////////////////////////////////////////////
// TInspectorStatusPusher
//////////////////////////////////////////////////////////////////////////////
class TInspectorStatusPusher final: public ITaskWatcher {
    enum class TSubscriptionType {
        Username,
        TaskId,
    };

public:
    TInspectorStatusPusher(
            const TString& workerId,
            const NAddr::IRemoteAddr& sourceAddr,
            const TString& taskId,
            const TString& username,
            ui32 sendUpdatesMask,
            IInspectorClient& inspectorClient,
            TTaskSubscriptionsPtr streamTaskSubscriptions,
            TTaskSubscriptionsPtr finalTaskSubscriptions)
        : WorkerId_(workerId)
        , TaskId_(taskId)
        , Username_(username)
        , SendUpdatesMask_(sendUpdatesMask)
        , SourceAddr_(&sourceAddr)
        , InspectorClient_(inspectorClient)
        , ChangesTaskSubscriptions_(std::move(streamTaskSubscriptions))
        , CompletionTaskSubscriptions_(std::move(finalTaskSubscriptions))
    {
        AtomicSet(Terminating_, 0);
    }

    ~TInspectorStatusPusher() {
        if (TaskWatchCtx_.IsInWork()) {
            TaskWatchCtx_.ForgetRequest();
        }
    }

    TTaskProgressPtr GetCurrentProgress() {
        TTaskProgressPtr progress = std::make_shared<NProto::TTaskProgress>();
        TaskProgressMerger_.FlushTo(progress.get(), false);
        if (progress->NodesSize() == 0) {
            return {};
        }
        return progress;
    }

private:
    void OnStart() override {
        YQL_LOG(TRACE) << "task: " << TaskId_ << ", OnStart()";
    }

    void OnFinish(NProto::ETaskStatus status) override {
        YQL_LOG(TRACE) << "task: " << TaskId_ << ", OnFinish("
                       << NProto::ETaskStatus_Name(status) << ')';
        if (AtomicGet(Terminating_) == 0 && ChangesTaskSubscriptions_) {
            auto subscrs = ChangesTaskSubscriptions_->Unsubscribe(TaskId_);
            SendTaskSubscriptionsNotifications(
                        subscrs, nullptr, TSubscriptionType::TaskId, true);
        } else {
            YQL_LOG(TRACE) << "task: " << TaskId_ << ", OnFinish() - skipped notification";
        }
    }

    void OnResultChange(
            TTaskResultChanges::TMask changesMask,
            TTaskResultPtr changes,
            int workerPid = 0,
            NBus::TOnMessageContext* taskWatchCtx = nullptr) override
    {
        YQL_LOG(TRACE) << "task: " << TaskId_ << ", OnResultChange()"
            << ", mask: " << TTaskResultChanges(changesMask)
            << ", changes: {" << FormatRequest(*changes) << '}'
            << ", pid: " << workerPid;

        if (taskWatchCtx) {
            THolder<NBus::TBusMessage> Message(taskWatchCtx->ReleaseMessage());
        }

        // (1) update collected task progress state
        if (changesMask & TTaskResultChanges::PROGRESS) {
            TaskProgressMerger_.MergeWith(changes->GetProgress());
        }
        if (changesMask & (TTaskResultChanges::STATUS_ERROR | TTaskResultChanges::STATUS_ABORTED)) {
            TaskProgressMerger_.AbortAllUnfinishedNodes();
        }

        const bool isTerminal = IsTerminalTaskStatus(changes->GetStatus());
        const bool hasSubscriptions = ChangesTaskSubscriptions_ || CompletionTaskSubscriptions_ && isTerminal;
        if (ChangesTaskSubscriptions_ && isTerminal) {
            AtomicSet(Terminating_, 1);
        }
        const bool sendStatusUpdate = (SendUpdatesMask_ & changesMask) && (changesMask != TTaskResultChanges::PROGRESS);

        bool needFullProgress = false;
        if (CompletionTaskSubscriptions_ && isTerminal) {
            needFullProgress = true;
        }

        if (sendStatusUpdate) {
            // always send status update with full progress data if progresses
            // requested
            if ((SendUpdatesMask_ & TTaskResultChanges::PROGRESS)) {
                needFullProgress = true;
            }
        }

        if (needFullProgress) {
            FlushFullProgressTo(changes.get());
        }

        if (!sendStatusUpdate) {
            SendNotifications(changes.get(), changesMask);
            YQL_LOG(TRACE) << "not sent changes, taskId: " << TaskId_
                << ", mask: " << TTaskResultChanges(changesMask)
                << ", changes: {" << FormatRequest(*changes) << '}';
            return;
        }

        // will send a status update
        TTaskResultPtr clonedChanges;
        if (hasSubscriptions) {
            // a copy of changes if we have both status update and subscriptions
            clonedChanges = std::make_shared<NProto::TTaskResult>(*changes);
        }

        SendTaskStatusUpdate(changes, clonedChanges, changesMask, workerPid, taskWatchCtx);
    }

    void FlushFullProgressTo(NProto::TTaskResult* changes) {
        NProto::TTaskProgress* progress = changes->MutableProgress();
        progress->Clear();
        TaskProgressMerger_.FlushTo(progress, false);
        if (progress->NodesSize() == 0) {
            changes->ClearProgress();
        }
    }

    void SendNotifications(NProto::TTaskResult* changes, TTaskResultChanges::TMask changesMask) {
        YQL_LOG(DEBUG) << "SendNotifications of " << TaskId_
            << " revision " << changes->GetRevision()
            << " changes mask " << TTaskResultChanges(changesMask);
        if (ChangesTaskSubscriptions_) {
            // (2) send notifications to subscriptions by username
            auto userSubscrs = ChangesTaskSubscriptions_->Find(Username_, changesMask);
            SendTaskSubscriptionsNotifications(
                userSubscrs, changes, TSubscriptionType::Username, false);

            // (3) send notifications to subscriptions by taskId
            bool isTerminating = AtomicGet(Terminating_) == 1;
            auto taskSubscrs =  isTerminating ?
                ChangesTaskSubscriptions_->Unsubscribe(TaskId_) :
                ChangesTaskSubscriptions_->Find(TaskId_, changesMask);
            SendTaskSubscriptionsNotifications(
                taskSubscrs, changes, TSubscriptionType::TaskId, isTerminating);
        }

        if (CompletionTaskSubscriptions_) {
            if (IsTerminalTaskStatus(changes->GetStatus())) {
                auto taskSubscrs = CompletionTaskSubscriptions_->Unsubscribe(TaskId_);
                SendTaskSubscriptionsNotifications(
                    taskSubscrs, changes, TSubscriptionType::TaskId, true);
            }
        }
    }

    bool WillWatch(ui32 changesMask) const override {
        return SendUpdatesMask_ & changesMask;
    }

    void SendTaskStatusUpdate(
        TTaskResultPtr changes,
        TTaskResultPtr clonedChanges,
        TTaskResultChanges::TMask changesMask,
        int workerPid,
        NBus::TOnMessageContext* taskWatchCtx)
    {
        NProto::TStatusUpdateRequest statusUpdate;
        statusUpdate.SetWorkerId(WorkerId_);
        if (workerPid) {
            statusUpdate.SetWorkerPid(ToString(workerPid));
        }

        statusUpdate.SetWorkerHost(WORKER_HOSTNAME);
        statusUpdate.SetWorkerVersion(WORKER_VERSION);

        statusUpdate.SetTaskId(TaskId_);
        *statusUpdate.MutableResult() = *changes;
        TIntrusivePtr<TInspectorStatusPusher> self = this;

        if (taskWatchCtx) {
            with_lock(TaskWatchCtxLock_) {
                taskWatchCtx->Swap(TaskWatchCtx_);
            }
            if (taskWatchCtx->IsInWork()) {
                taskWatchCtx->ForgetRequest();
                YQL_LOG(ERROR) << "Previous StatusUpdate is probably failed, do not send TaskWatchEventResponse";
            }
        }
        InspectorClient_.SendStatusUpdate(
                SourceAddr_,
                std::move(statusUpdate),
                [self = std::move(self), taskId = TaskId_, clonedChanges = std::move(clonedChanges), changesMask](
                        const NAddr::IRemoteAddr& peerAddr,
                        NProto::TStatusUpdateResponse&& resp)
                {
                    TString peerHost = NAddr::PrintHost(peerAddr);
                    YQL_CLOG(DEBUG, Net)
                            << "status update response for " << taskId
                            << " from " << peerHost
                            << " : " << PbMessageToStr(resp);

                    NBus::TOnMessageContext ctx;
                    with_lock(self->TaskWatchCtxLock_) {
                        ctx.Swap(self->TaskWatchCtx_);
                    }

                    if (ctx.IsInWork()) {
                        ctx.SendReplyMove(new NSubprocessTaskExecutor::TBusTaskWatchEventResponse);
                    } else {
                        YQL_LOG(ERROR) << "TaskWatch context is invalid";
                    }

                    if (clonedChanges) {
                        self->SendNotifications(clonedChanges.get(), changesMask);
                    }
                });
    }

    void SendTaskSubscriptionsNotifications(
            const TVector<TTaskSubscriptionPtr>& subscrs,
            const NProto::TTaskResult* changes,
            TSubscriptionType subscriptionType,
            bool sendUnsubscribe)
    {
        for (const TTaskSubscriptionPtr& s: subscrs) {
            NProto::TTaskSubscriptionNotifyRequest notifyReq;
            notifyReq.SetChannelId(s->GetChannelId());
            notifyReq.SetWorkerId(WorkerId_);
            notifyReq.SetTaskId(TaskId_);

            if (subscriptionType == TSubscriptionType::Username) {
                notifyReq.SetUsername(Username_);
            }

            if (sendUnsubscribe) {
                notifyReq.SetUnsubscribe(true);
            }

            if (changes) {
                NProto::TTaskResult* result = notifyReq.MutableResult();
                TTaskResultChanges::Copy(result, *changes, s->GetMask());
            }

            YQL_CLOG(TRACE, Net)
                << "SendTaskSubscriptionsNotifications, notifyReq: {"
                << PbMessageToStr(notifyReq) << "}"
                << ", has changes: " << (changes ? "true" : " false")
                << ", mask: " << TTaskResultChanges(s->GetMask());

            try {
                InspectorClient_.NotifyTaskSubscription(
                    s->GetAddress(),
                    std::move(notifyReq));
            } catch (const std::exception&) {
                YQL_LOG(ERROR) << "Failed to send notification: " << CurrentExceptionMessage();

                // let's continue 'for' loop
            }
            // TODO: remove subscription if response.status == NOT_EXISTS
        }
    }

private:
    const TString WorkerId_;
    const TString TaskId_;
    const TString Username_;
    const ui32 SendUpdatesMask_;
    const NAddr::TOpaqueAddr SourceAddr_;
    IInspectorClient& InspectorClient_;
    TTaskSubscriptionsPtr ChangesTaskSubscriptions_;
    TTaskSubscriptionsPtr CompletionTaskSubscriptions_;
    TTaskProgressMerger TaskProgressMerger_;
    TAtomic Terminating_;
    NBus::TOnMessageContext TaskWatchCtx_;
    TAdaptiveLock TaskWatchCtxLock_;
};

//////////////////////////////////////////////////////////////////////////////
// TFutureResultWatcher
//////////////////////////////////////////////////////////////////////////////
    struct TFutureResultWatcher final: public ITaskWatcher {
        TFutureResultWatcher()
                : Promise_(NThreading::NewPromise<TTaskResultPtr>())
                , Result_(new NProto::TTaskResult)
        {
        }

        TTaskResultPtr TryGetResult() {
            const TDuration pollInterval = TDuration::Seconds(1);
            auto future = Promise_.GetFuture();

            while (true) {
                if (future.Wait(pollInterval)) {
                    return future.GetValue();
                }

                if (ShouldStopCore()) {
                    return nullptr;
                }
            }
        }

    private:
        void OnStart() override {
        }

        void OnFinish(NProto::ETaskStatus status) override {
            Result_->SetStatus(status);
            Promise_.SetValue(Result_);
        }

        void OnResultChange(
                TTaskResultChanges::TMask /*changesMask*/,
                TTaskResultPtr changes,
                int /*workerPid*/,
                NBus::TOnMessageContext*) override
        {
            Result_->MergeFrom(*changes);
        }

        bool WillWatch(ui32 /*changesMask*/) const override {
            return true;
        }

    private:
        NThreading::TPromise<TTaskResultPtr> Promise_;
        TTaskResultPtr Result_;
    };

//////////////////////////////////////////////////////////////////////////////
// TWorkerServerHandler
//////////////////////////////////////////////////////////////////////////////
class TWorkerServerHandler: public IWorkerServerHandler {
public:
    TWorkerServerHandler(
            const TString& workerId,
            ITaskExecutor& taskExecutor,
            IInspectorClient& inspectorClient,
            TSensorsGroupPtr& serverSensors)
        : WorkerId_(workerId)
        , TaskExecutor_(taskExecutor)
        , InspectorClient_(inspectorClient)
    {
        // TODO (jamel): reimplenet with sensor lazy gauge
        ChangesTaskSubscriptions_ = new TTaskSubscriptions(
                serverSensors->GetCounter("changes_subscriptions"));
        CompletionTaskSubscriptions_ = new TTaskSubscriptions(
                serverSensors->GetCounter("completion_subscriptions"));
    }

private:
    NProto::TProcessTaskResponse OnProcessTask(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TProcessTaskRequest&& request) override
    {
        TString taskId = request.GetData().GetId();
        NProto::TProcessTaskResponse response;
        try {
            // notify only persisted tasks
            bool canHaveSubscrs = !request.GetData().GetIsSystemRequest();
            if (canHaveSubscrs) {
                YQL_LOG(DEBUG) << "Got task to process: " << taskId;
            }
            auto watcher = MakeInspectorStatusPusher(
                        WorkerId_, peerAddr, request.GetData().GetId(), request.GetData().GetUsername(),
                        request.GetSendUpdatesMask(),
                        InspectorClient_,
                        canHaveSubscrs ? ChangesTaskSubscriptions_ : nullptr,
                        canHaveSubscrs ? CompletionTaskSubscriptions_ : nullptr);

            auto status = TaskExecutor_.ProcessTask(
                        request.GetAction(),
                        TTaskDataPtr(request.release_data()),
                        watcher);
            response.SetStatus(status);
        } catch (...) {
            TString message = CurrentExceptionMessage();
            YQL_LOG(ERROR) << "cannot run task " << taskId << ": " << message;
            response.SetStatus(NProto::TProcessTaskResponse::FAIL);
            response.MutableMessage()->swap(message);
        }

        return response;
    }

    NProto::TKillTaskResponse OnKillTask(
            const NAddr::IRemoteAddr& /*peerAddr*/,
            NProto::TKillTaskRequest&& request) override
    {
        NProto::TKillTaskResponse response;
        try {
            YQL_LOG(DEBUG) << "Got task to kill: " << request.GetId();
            auto status = TaskExecutor_.KillTask(request.GetId());
            response.SetStatus(status);
        } catch (...) {
            TString message = CurrentExceptionMessage();
            YQL_LOG(ERROR) << "cannot kill task " << request.GetId() << ": "
                           << message;
            response.SetStatus(NProto::TKillTaskResponse::FAIL);
            response.MutableMessage()->swap(message);
        }

        return response;
    }

    NProto::TSubscribeResponse OnSubscribe(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TSubscribeRequest&& request) override
    {
        NProto::TSubscribeResponse response;
        try {
            TChannelId channelId = request.GetChannelId();
            TSubscrMask mask = request.GetMask();

            if (request.HasTaskId()) {
                const TSubscrId& id = request.GetTaskId();
                auto& subscriptions = (request.GetType() == NProto::ESubscriptionType::CHANGES)
                        ? *ChangesTaskSubscriptions_
                        : *CompletionTaskSubscriptions_;
                bool subscribed = subscriptions.SubscribeIf(
                        peerAddr, channelId, id, mask,
                        [this, &id]() {
                            return TaskExecutor_.HasRunningTask(id);
                        });

                if (subscribed) {
                    response.SetStatus(NProto::TSubscribeResponse::OK);

                    //
                    // XXX: very ugly way
                    //
                    // One day we will have bidirectional RPC channel (mabe
                    // even gRPC) between CORE and TASK processes and it will
                    // be possible to request whole current task state from
                    // TASK process. Now mentioned channel is unidirectional
                    // (TASK -> CORE). Furtunately we store merged task progress
                    // state in TInspectorStatusPusher to avoid sending extra
                    // messages to Inspector server and here retrieve current
                    // task progress from it.
                    //
                    if (request.GetType() == NProto::ESubscriptionType::CHANGES) {
                        ITaskWatcherPtr watcher = TaskExecutor_.GetRunningTaskWatcher(id);
                        if (auto* pusher = dynamic_cast<TInspectorStatusPusher*>(watcher.Get())) {
                            if (TTaskProgressPtr progress = pusher->GetCurrentProgress()) {
                                // TODO (jamel): fill all fields of result
                                NProto::TTaskResult* result = response.MutableResult();
                                *result->MutableProgress() = *progress;
                            }
                        }
                    }
                } else {
                    response.SetStatus(NProto::TSubscribeResponse::NOT_RUNNING);
                }
            } else {
                // subscriptions on username always have a CHANGES type
                const TSubscrId& id = request.GetUsername();
                ChangesTaskSubscriptions_->Subscribe(peerAddr, channelId, id, mask);
                response.SetStatus(NProto::TSubscribeResponse::OK);
            }
        } catch (...) {
            TString message = CurrentExceptionMessage();
            YQL_LOG(ERROR) << "OnSubscribe: " << message;
            response.SetStatus(NProto::TSubscribeResponse::FAIL);
            response.SetMessage(message);
        }

        return response;
    }

    NProto::TUnsubscribeResponse OnUnsubscribe(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TUnsubscribeRequest&& request) override
    {
        try {
            auto& subscriptions = (request.GetType() == NProto::ESubscriptionType::CHANGES)
                    ? *ChangesTaskSubscriptions_
                    : *CompletionTaskSubscriptions_;
            if (request.HasSubscriptionId()) {
                subscriptions.Unsubscribe(peerAddr,
                        request.GetChannelId(), request.GetSubscriptionId());
            } else {
                subscriptions.Unsubscribe(peerAddr, request.GetChannelId());
            }
        } catch (...) {
            YQL_LOG(ERROR) << "OnUnsubscribe: " << CurrentExceptionMessage();
        }

        NProto::TUnsubscribeResponse response;
        return response;
    }

private:
    const TString WorkerId_;
    ITaskExecutor& TaskExecutor_;
    IInspectorClient& InspectorClient_;
    TTaskSubscriptionsPtr ChangesTaskSubscriptions_;
    TTaskSubscriptionsPtr CompletionTaskSubscriptions_;
};

class TRunPidFile {
public:
    TRunPidFile(const TString& dir) {
        auto pidDir = TFsPath(dir);
        try {
            auto lockFile = pidDir / ToString(GetPID());
            Lock.Reset(new TFileLock(lockFile));
            YQL_LOG(INFO) << "Locking pid file: " << lockFile.GetPath();
            Lock->Acquire();
            YQL_LOG(INFO) << "Pid file: " << lockFile.GetPath() << " is locked";
        } catch (...) {
            YQL_LOG(ERROR) << "Error creating pid file: " << CurrentExceptionMessage();
            throw;
        }
    }
    ~TRunPidFile() {
        if (!Lock) {
            return;
        }
        auto path = Lock->GetName();
        YQL_LOG(INFO) << "Releasing pid file: " << path;
        Lock->Release();
        Lock.Reset();
        NFs::Remove(path);
    }

private:
    THolder<TFileLock> Lock;
};

} // namespace


//////////////////////////////////////////////////////////////////////////////
// TYqlWorkerCore::TImpl
//////////////////////////////////////////////////////////////////////////////
struct TYqlWorkerCore::TImpl {
    TMainConfig Config;
    const TString WorkerId;
    ITaskExecutorPtr TaskExecutor;
    TAtomic MaxHeartbeatGapMillis_;
    IWorkerServerPtr Server;
    IInspectorClientPtr InspectorClient;
    TBackgroundThreads BackgroundThreads;
    TThreadPool ConnectionClosingQueue;
    TAtomic CurrentState;
    TSensorCounterPtr IsReady;
    TSolomonAgentClientPtr SolomonAgentClient;
    THolder<TRunPidFile> RunPid;

    TImpl(
            const TMainConfig& config,
            ITaskExecutorPtr taskExecutor,
            const TBindResultVector& serverPorts)
        : Config(config)
        , WorkerId(CreateGuidAsString())
        , TaskExecutor(taskExecutor)
        , MaxHeartbeatGapMillis_(DEFAULT_MAX_HEARTBEAT_GAP_MILLIS)
        , ConnectionClosingQueue(TThreadPoolParams{"ConnectionClosingQueue"})
        , SolomonAgentClient(BuildSolomonAgentClient(GetSolomonAgentConfig(config.Worker)))
    {
        SetCurrentState(NProto::EWorkerState::NEW, "OnStart");

        if (ShouldStopCore()) {
            return;
        }

        auto serverSensors = GetSensorsGroupFor(NSensorComponent::kWorkerServer);
        IsReady = serverSensors->GetNamedCounter("status", "ready");

        InitializeInspectorClient();
        if (!StartTaskExecutor() || !PerformInitialWarmup()) {
            return;
        }

        StartWorkerServer(serverPorts, serverSensors);
        StartBackgroundServices();
        PublishReadyState();
    }

    void InitializeInspectorClient() {
        InspectorClient = WrapWithProfilingClient(
            CreateMsgBusInspectorClient(Config.Worker.GetInspector()));
    }

    bool StartTaskExecutor() {
        auto orphanFactory = [this](const TString& taskId, const TString& username) {
            NAddr::TOpaqueAddr preferredPeer;
            return MakeInspectorStatusPusher(
                WorkerId, preferredPeer, taskId, username,
                ui32(NProto::ETaskResultChange::MAX_VALUE),
                *InspectorClient, nullptr, nullptr);
        };

        TaskExecutor->Startup(orphanFactory);

        const TDuration idleTime = TDuration::Seconds(1);
        while (!TaskExecutor->IsReady()) {
            if (ShouldStopCore()) {
                return false;
            }
            YQL_LOG(DEBUG) << "waiting for task server executor";
            Sleep(idleTime);
        }

        return true;
    }

    bool PerformInitialWarmup() {
        const TDuration idleTime = TDuration::Seconds(1);
        while (true) {
            if (ShouldStopCore()) {
                YQL_LOG(INFO) << "initial warmup loop stopped: core shutdown requested";
                return false;
            }

            TTaskDataPtr taskData = std::make_shared<NProto::TTaskData>();
            const TString taskId = "__INITIAL_WARMUP__" + CreateGuidAsString();
            taskData->SetId(taskId);
            taskData->SetSyntax(NProto::ESyntax::UDF_REGISTRY_SNAPSHOT);
            taskData->SetAttributes(NYT::NodeToYsonString(NYT::TNode()("blocking", true)));

            auto watcher = MakeIntrusive<TFutureResultWatcher>();
            auto taskStatus = TaskExecutor->ProcessTask(NProto::ETaskAction::RUN, std::move(taskData), watcher);
            if (taskStatus == NProto::TProcessTaskResponse::SUBMITED) {
                YQL_LOG(INFO) << "initial warmup started";
                if (auto result = watcher->TryGetResult()) {
                    const auto status = result->GetStatus();
                    if (status == NProto::ETaskStatus::COMPLETED) {
                        return true;
                    }
                    YQL_LOG(ERROR) << "initial warmup "
                                   << (status == NProto::ETaskStatus::ERROR ? "failed" : "aborted")
                                   << ", retrying";
                    Sleep(idleTime);
                } else {
                    try {
                        const auto killStatus = TaskExecutor->KillTask(taskId);
                        if (killStatus != NProto::TKillTaskResponse::KILLED) {
                            YQL_LOG(WARN) << "initial warmup task was not running during shutdown";
                        }
                    } catch (...) {
                        YQL_LOG(WARN) << "failed to abort initial warmup task during shutdown: "
                                      << CurrentExceptionMessage();
                    }
                    YQL_LOG(INFO) << "initial warmup loop stopped: warmup cancelled on shutdown";
                    return false;
                }
            } else {
                YQL_LOG(ERROR) << "initial warmup submit task failed, retrying";
                Sleep(idleTime);
            }
        }
    }

    void StartWorkerServer(
            const TBindResultVector& serverPorts,
            TSensorsGroupPtr& serverSensors)
    {
        auto serverHandler = WrapWithProfilingHandler(new TWorkerServerHandler(
                WorkerId, *TaskExecutor, *InspectorClient, serverSensors));
        Server = CreateMsgBusWorkerServer(
                Config.Worker.GetServer(), serverPorts, serverHandler);
    }

    void StartBackgroundServices() {
        ConnectionClosingQueue.Start(1);
        BackgroundThreads.Add(std::bind(&TImpl::DoSendHeartbeat, this));

        if (SolomonAgentClient) {
            SolomonAgentClient->AddPIDLabel();
            YQL_CLOG(DEBUG, Net)
                << "Start sending sensors to Solomon Agent (config: "
                << SolomonAgentClient->GetConfigAsJSON()
                << ')';
            BackgroundThreads.Add(std::bind(&TImpl::DoSendMetricsToAgent, this));
        }
    }

    void PublishReadyState() {
        IsReady->Inc();
        if (Config.Worker.GetExecutor().HasRunDir()) {
            RunPid.Reset(new TRunPidFile(Config.Worker.GetExecutor().GetRunDir()));
        }
    }

    ~TImpl() {
        PartialShutdown();
        // shutdown inspector client last to be able to send MUST_DIE status
        InspectorClient.reset();
    }

    void PartialShutdown(std::function<void()> onIdle = {}) {
        TaskExecutor->Shutdown(onIdle);
        if (Server) {
            Server->Shutdown();
        }
        BackgroundThreads.Stop();
        ConnectionClosingQueue.Stop();
    }

    int Run() {
        YQL_LOG(INFO) << "core process has id "<< WorkerId;

        if (!InspectorClient || !Server) {
            AddProcTitleSuffix("[terminating]");
            PartialShutdown([config = std::move(Config)]() mutable { ReopenLogIfNeeded(config); });
            return 0;
        }

        int exitCode = 0;
        try {
            const TDuration idleTime = TDuration::Seconds(1);
            while (true) {
                if (ShouldStopCore()) {
                    AddProcTitleSuffix("[terminating]");
                    break;
                }

                ReopenLogIfNeeded(Config);
                Sleep(idleTime);
            }

            YQL_LOG(INFO) << "core process will shutdown";

            RunPid.reset();
            IsReady->Set(0);
            if (SolomonAgentClient) {
                DoSendMetricsToAgent();
            }

            // immediately send TERMINATING state
            SetCurrentState(NProto::EWorkerState::TERMINATING, "OnShutdown");
            DoSendHeartbeat();

            // do gracefull shutdown
            PartialShutdown([config = std::move(Config)]() mutable { ReopenLogIfNeeded(config); });

            TBackgroundThreads temporaryBackground;
            if (SolomonAgentClient) {
                temporaryBackground.Add(std::bind(&TImpl::DoSendMetricsToAgent, this));
            }

            InspectorClient->WaitAllMessagesDelivered();

            if (SolomonAgentClient) {
                temporaryBackground.Stop();
                SolomonAgentClient->MutableConfig().WithRetryCount(0);
                Sleep(SolomonAgentClient->GetDurationUntilNextGridPoint() + SolomonAgentClient->GetUpdatePeriod() / 2);
                DoSendMetricsToAgent();
            }

            // immediately send MUST_DIE state
            SetCurrentState(NProto::EWorkerState::MUST_DIE, "OnShutdown");
            DoSendHeartbeat();
        } catch (...) {
            YQL_LOG(ERROR) << "unhandled exception: "
                           << CurrentExceptionMessage();
            exitCode = 1;
        }

        YQL_LOG(INFO) << "core process shutdown";
        return exitCode;
    }

    TDuration DoSendHeartbeat() {
        try {
            NProto::THeartbeatRequest heartbeatReq;
            heartbeatReq.SetWorkerId(WorkerId);
            heartbeatReq.SetPort(Server->ActualPort());
            heartbeatReq.SetWorkerState(GetCurrentState());
            heartbeatReq.SetRunningTasksCount(TaskExecutor->GetRunningTasksCount());
            heartbeatReq.SetSvnRevision(GetProgramSvnRevision());
            heartbeatReq.SetProgramCommitId(WORKER_VERSION);

            InspectorClient->SendHeartbeatToAll(
                    std::move(heartbeatReq),
                    [this](
                        const NAddr::IRemoteAddr& peerAddr,
                        NProto::THeartbeatResponse&& resp)
                    {
                        OnHeartbeatResponse(peerAddr, std::move(resp));
                    });
        } catch (...) {
            YQL_LOG(ERROR) << "Cannot send heartbeat request: "
                           << CurrentExceptionMessage();
        }

        auto sleepTime = AtomicGet(MaxHeartbeatGapMillis_);
        return TDuration::MilliSeconds(sleepTime / 4);
    }

    void OnHeartbeatResponse(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::THeartbeatResponse&& resp)
    {
        auto currState = GetCurrentState();
        auto newState = resp.GetWorkerState();

        if (newState == NProto::EWorkerState::MUST_DIE) {
            // Do not set current state to MUST_DIE to avoid propagating it
            // to other Inspector instances. Just close connection with author
            // of this MUST_DIE state.

            if (currState == NProto::EWorkerState::MUST_DIE) {
                // worker already is shutdowning so ignore this new state
                YQL_LOG(WARN) << WorkerId << " received heartbeat response from "
                              << peerAddr << " with status "
                              << NProto::EWorkerState_Name(newState)
                              << " but it is already in MUST_DIE state so request"
                                 " is ignored";
            } else {
                YQL_LOG(WARN) << WorkerId << " received heartbeat response from "
                              << peerAddr << " with status "
                              << NProto::EWorkerState_Name(newState)
                              << " so closing connection with that bad guy";

                // make sure to close client connection outside of request callback
                std::shared_ptr<NAddr::TOpaqueAddr> peerAddrCopy = std::make_shared<NAddr::TOpaqueAddr>(&peerAddr);
                ConnectionClosingQueue.SafeAddFunc([this, peerAddrCopy]() {
                    InspectorClient->CloseConnectionWith(*peerAddrCopy);

                    if (Y_UNLIKELY(InspectorClient->GetConnectionsCount() == 0)) {
                        YQL_LOG(FATAL) << "worker " << WorkerId
                                      << " has no live sessions with Inspector. Terminating process.";
                        Y_ABORT("worker %s has no live sessions with Inspector", WorkerId.data());
                    }
                });
            }
        } else if (currState != newState && currState != NProto::EWorkerState::MUST_DIE && currState != NProto::EWorkerState::TERMINATING) {
            SetCurrentState(newState, "OnHeartbeatResponse");
        }

        ui64 newGap = resp.GetMaxHeartbeatGap();
        ui64 currGap = AtomicGet(MaxHeartbeatGapMillis_);
        if (newGap > currGap) {
            AtomicCas(&MaxHeartbeatGapMillis_, newGap, currGap);
        }
    }

    TDuration DoSendMetricsToAgent() {
        SolomonAgentClient->SendMetrics(GetSensorsRootGroup());
        return SolomonAgentClient->GetDurationUntilNextGridPoint() + SolomonAgentClient->GetUpdatePeriod() / 2;
    }

    void SetCurrentState(NProto::EWorkerState state, TStringBuf action) {
        AtomicSet(CurrentState, static_cast<intptr_t>(state));
        YQL_LOG(DEBUG) << action << " worker " << WorkerId << " becomes "
                       << NProto::EWorkerState_Name(state);
    }

    NProto::EWorkerState GetCurrentState() const {
        return static_cast<NProto::EWorkerState>(AtomicGet(CurrentState));
    }
};

TYqlWorkerCore::TYqlWorkerCore(
        const TMainConfig& config,
        ITaskExecutorPtr taskExecutor,
        const TBindResultVector& serverPorts)
    : Impl_(new TImpl(config, taskExecutor, serverPorts))
{
}

TYqlWorkerCore::~TYqlWorkerCore()
{
}

int TYqlWorkerCore::Run()
{
    return Impl_->Run();
}

TBindResultVector TYqlWorkerCore::BindOn(int port)
{
    return NBus::BindOnPort(port, false).second;
}

ITaskWatcherPtr MakeInspectorStatusPusher(
    const TString& workerId,
    const NAddr::IRemoteAddr& sourceAddr,
    const TString& taskId,
    const TString& username,
    ui32 sendUpdatesMask,
    IInspectorClient& inspectorClient,
    TTaskSubscriptionsPtr streamTaskSubscriptions,
    TTaskSubscriptionsPtr finalTaskSubscriptions)
{
    return MakeIntrusive<TInspectorStatusPusher>(
        workerId, sourceAddr, taskId, username, sendUpdatesMask, inspectorClient,
        streamTaskSubscriptions, finalTaskSubscriptions);
}

int RunWorkerCore(
    const TMainConfig& config,
    ITaskExecutorPtr taskExecutor,
    const TBindResultVector& serverPorts)
{
    TYqlWorkerCore core(config, taskExecutor, serverPorts);
    if (config.Worker.GetProcessMode() != NProto::TProcessMode::SINGLE_PROCESS) {
        YQL_LOG(INFO) << "core process started";
    }
    return core.Run();
}

} // namespace NYql
