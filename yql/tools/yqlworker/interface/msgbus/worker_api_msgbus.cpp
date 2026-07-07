#include "worker_api_msgbus.h"

#include <yql/tools/yqlworker/proto/worker.pb.h>
#include <yql/tools/yqlworker/proto/inspector.pb.h>
#include <yql/tools/yqlworker/rpc/inspector_protocol.h>
#include <yql/tools/yqlworker/rpc/worker_protocol.h>
#include <yql/tools/yqlworker/misc/background_threads.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/public/issue/yql_issue_id.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/messagebus/handler.h>
#include <library/cpp/messagebus/network.h>

#include <util/stream/format.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/guid.h>
#include <util/string/cast.h>
#include <util/system/datetime.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <utility>
#include <unordered_map>

namespace NYql::NWorkerApi {

namespace {

inline ui64 CurrentTimeMillis() {
    return MilliSeconds();
}

using namespace NThreading;

class IInspectorHandler {
public:
    using TPtr = std::shared_ptr<IInspectorHandler>;
    using TWeakPtr = std::weak_ptr<IInspectorHandler>;

    virtual ~IInspectorHandler() = default;

    virtual TFuture<void> OnStatusUpdate(NProto::TStatusUpdateRequest&& request) = 0;
    virtual NProto::THeartbeatResponse OnHeartbeat(const NBus::TNetAddr& peerAddr, NProto::THeartbeatRequest&& request) = 0;
    virtual TFuture<NProto::TTaskSubscriptionNotifyResponse::EStatus> OnTaskSubscriptionNotify(NProto::TTaskSubscriptionNotifyRequest&& request) = 0;
};


class TMsgBusInspectorServer final: public NBus::IBusServerHandler, public std::enable_shared_from_this<TMsgBusInspectorServer> {
public:
    TMsgBusInspectorServer(TMsgBusWorkerApiConfig config, IInspectorHandler::TWeakPtr handler)
        : Protocol_(config.Port)
        , Handler_(std::move(handler))
    {
        NBus::TBusQueueConfig queueCfg;
        queueCfg.Name = "worker_api.inspector";
        queueCfg.NumWorkers = config.MsgBusThreads;
        BusQueue_ = NBus::CreateMessageQueue(queueCfg, "server");

        NBus::TBusClientSessionConfig sessionCfg;
        sessionCfg.MaxInFlight = config.MsgBusMaxInFlight;
        sessionCfg.SendTimeout = config.MsgBusSendTimeout.MilliSeconds();
        sessionCfg.TotalTimeout = config.MsgBusTotalTimeout.MilliSeconds();
        sessionCfg.TcpNoDelay = true;

        auto bindTo = config.BindLoopbackOnly
            ? NBus::BindOnLoopbackPort(config.Port, false).second
            : NBus::BindOnPort(config.Port, false).second;
        Session_ = NBus::TBusServerSession::Create(
                    &Protocol_, this, sessionCfg, BusQueue_, bindTo);
        YQL_ENSURE(Session_, "probably somebody is listening on the same port");
        YQL_LOG(INFO) << "MsgBusInspectorServer worker server listening on port "
                            << Session_->GetActualListenPort();
    }

    void Shutdown() {
        Session_->Shutdown();
    }

    int ActualPort() const {
        return Session_->GetActualListenPort();
    }

private:
    size_t CaptureContext(NBus::TOnMessageContext& ctx) {
        auto lock = Guard(Mutex_);
        size_t tag = MessageIndex_++;
        auto& context = IndexToContext_[tag];
        ctx.Swap(context);
        return tag;
    }

    void ExtractContext(size_t tag, NBus::TOnMessageContext& ctx) {
        auto lock = Guard(Mutex_);
        auto it = IndexToContext_.find(tag);
        YQL_ENSURE(it != IndexToContext_.end(), "Unexpected message tag");
        ctx.Swap(it->second);
        IndexToContext_.erase(it);
    }

    void SendUpdateStatusResponse(size_t tag, const TFuture<void>& r) {
        NBus::TOnMessageContext context;
        ExtractContext(tag, context);

        NProto::TStatusUpdateResponse resp;
        try {
            r.GetValueSync();
            resp.SetStatus(NProto::TStatusUpdateResponse_EStatus_OK);
        } catch (...) {
            resp.SetStatus(NProto::TStatusUpdateResponse_EStatus_FAILED);
            resp.SetMessage(CurrentExceptionMessage());
        }
        context.SendReplyMove(new TBusStatusUpdateResponse(std::move(resp)));
    }

    void HandleUpdateStatusRequest(NBus::TOnMessageContext& ctx, IInspectorHandler::TPtr handler) {
        auto reqMsg = static_cast<TBusStatusUpdateRequest*>(ctx.GetMessage());
        size_t tag = CaptureContext(ctx);
        handler->OnStatusUpdate(std::move(reqMsg->Record)).Apply([this_ = weak_from_this(), tag](const TFuture<void>& r) {
            if (auto pThis = this_.lock()) {
                pThis->SendUpdateStatusResponse(tag, r);
            }
        });
    }

    void HandleHeartbeatRequest(NBus::TOnMessageContext& ctx, IInspectorHandler::TPtr handler) {
        auto reqMsg = static_cast<TBusHeartbeatRequest*>(ctx.GetMessage());
        auto peerAddr = reqMsg->GetReplyTo();
        auto resp = handler->OnHeartbeat(peerAddr, std::move(reqMsg->Record));
        ctx.SendReplyMove(new TBusHeartbeatResponse(std::move(resp)));
    }

    void SendTaskSubscriptionNotifyResponse(size_t tag, const TFuture<NProto::TTaskSubscriptionNotifyResponse::EStatus>& r) {
        NBus::TOnMessageContext context;
        ExtractContext(tag, context);

        NProto::TTaskSubscriptionNotifyResponse resp;
        try {
            resp.SetStatus(r.GetValueSync());
        } catch (...) {
            resp.SetStatus(NProto::TTaskSubscriptionNotifyResponse_EStatus_FAILED);
            resp.SetMessage(CurrentExceptionMessage());
        }
        context.SendReplyMove(new TBusTaskSubscriptionNotifyResponse(std::move(resp)));
    }

    void HandleTaskSubscriptionNotifyRequest(NBus::TOnMessageContext& ctx, IInspectorHandler::TPtr handler) {
        auto reqMsg = static_cast<TBusTaskSubscriptionNotifyRequest*>(ctx.GetMessage());
        size_t tag = CaptureContext(ctx);
        handler->OnTaskSubscriptionNotify(std::move(reqMsg->Record)).Apply([this_ = weak_from_this(), tag](const TFuture<NProto::TTaskSubscriptionNotifyResponse::EStatus>& r) mutable {
            if (auto pThis = this_.lock()) {
                pThis->SendTaskSubscriptionNotifyResponse(tag, r);
            }
        });
    }

    void OnMessage(NBus::TOnMessageContext& ctx) override {
        auto type = static_cast<EInspectorMessageType>(ctx.GetMessage()->GetHeader()->Type);
        try {
            auto handler = Handler_.lock();
            if (!handler) {
                YQL_LOG(WARN) << "Handler is released. Skip message type: " << ToCString(type);
                return;
            }

            switch (type) {
            case MTYPE_UPDATE_STATUS_REQUEST:
                HandleUpdateStatusRequest(ctx, handler);
                break;
            case MTYPE_HEARTBEAT_REQUEST:
                HandleHeartbeatRequest(ctx, handler);
                break;
            case MTYPE_TASK_SUBSCR_NOTIFY_REQUEST:
                HandleTaskSubscriptionNotifyRequest(ctx, handler);
                break;
            default:
                YQL_LOG(WARN) << "Unknown message type: " << Hex(type);
            }
        } catch (...) {
            YQL_LOG(ERROR) << "Unhandled exception, message type: "  << Hex(type) << ", " << CurrentExceptionMessage();
        }
    }

    // called when message or reply can't be delivered
    void OnError(TAutoPtr<NBus::TBusMessage> msg, NBus::EMessageStatus status) override {

#define LOG_KNOWN_MESSAGE_CASE(name, ...) \
    case name: \
        YQL_LOG(ERROR) \
                << "Cannot deliver message " #name " " \
                << NBus::GetMessageStatus(status) << ": " \
                << NBus::MessageStatusDescription(status); \
        break;

        try {
            auto type = static_cast<EInspectorMessageType>(msg->GetHeader()->Type);
            switch (type) {
                INSPECTOR_MESSAGE_TYPES(LOG_KNOWN_MESSAGE_CASE)
            default:
                YQL_LOG(ERROR)
                        << "Cannot deliver unknown message " << Hex(type)
                        << " " << NBus::GetMessageStatus(status)
                        << ": " << NBus::MessageStatusDescription(status);
            }
        } catch (...) {
            YQL_LOG(ERROR) << "Unhandled exception: " << CurrentExceptionMessage();
        }
#undef LOG_KNOWN_MESSAGE_CASE
    }

private:
    TMsgBusInspectorProtocol Protocol_;
    NBus::TBusMessageQueuePtr BusQueue_;
    NBus::TBusServerSessionPtr Session_;
    IInspectorHandler::TWeakPtr Handler_;

    TMutex Mutex_;
    size_t MessageIndex_ = 0;
    std::unordered_map<size_t, NBus::TOnMessageContext> IndexToContext_;
};


class TBusAsyncProcessTaskRequest: public TBusProcessTaskRequest {
public:
    NThreading::TPromise<NProto::TProcessTaskResponse> Promise;
};

class TBusAsyncKillTaskRequest: public TBusKillTaskRequest {
public:
    NThreading::TPromise<NProto::TKillTaskResponse> Promise;
};

class TBusAsyncSubscribeRequest: public TBusSubscribeRequest {
public:
    NThreading::TPromise<NProto::TSubscribeResponse> Promise;
};

class TBusAsyncUnsubscribeRequest: public TBusUnsubscribeRequest {
public:
    NThreading::TPromise<NProto::TUnsubscribeResponse> Promise;
};

class TMsgBusWorkerClientProtocol: public NBus::TBusBufferProtocol {
public:
    explicit TMsgBusWorkerClientProtocol(int port)
        : NBus::TBusBufferProtocol("yqlworker_client", port)
    {
        RegisterType(new TBusAsyncProcessTaskRequest);
        RegisterType(new TBusProcessTaskResponse);
        RegisterType(new TBusAsyncKillTaskRequest);
        RegisterType(new TBusKillTaskResponse);
        RegisterType(new TBusAsyncSubscribeRequest);
        RegisterType(new TBusSubscribeResponse);
        RegisterType(new TBusAsyncUnsubscribeRequest);
        RegisterType(new TBusUnsubscribeResponse);
    }
};

class TWorkerClient: public NBus::IBusClientHandler {
public:
    using TPtr = std::shared_ptr<TWorkerClient>;
    using TWeakPtr = std::weak_ptr<TWorkerClient>;

    TWorkerClient(TString id, TString commitId, ui64 currentTimeMillis, NProto::EWorkerState state, NBus::TNetAddr addr, TMsgBusWorkerApiConfig config)
        : Id_(std::move(id))
        , CommitId_(std::move(commitId))
        , MaxHeartbeatGapMillis_(config.MaxHeartbeatGap.MilliSeconds())
        , MaxHeartbeatCheckGapMillis_(config.MaxHeartbeatCheckGap.MilliSeconds())
        , LoseUnhealthyWorkerAfterMillis_(config.LoseUnhealthyWorkerAfter.MilliSeconds())
        , Protocol_(addr.GetPort())
        , WorkerAddr_(std::move(addr))
        , State_(state)
        , TimeMillisLastAliveSeen_(currentTimeMillis)
        , TimeMillisLastHealthySeen_(0ul)
    {
        Y_UNUSED(CommitId_); // TODO: handle worker filters by host/version
        YQL_LOG(DEBUG) << "New worker " << Id_ << " on address "<< WorkerAddr_;
        NBus::TBusClientSessionConfig sessionConfig;
        sessionConfig.ConnectTimeout = config.WorkerConnectTimeout.MilliSeconds();
        sessionConfig.TotalTimeout = config.WorkerTotalTimeout.MilliSeconds();
        sessionConfig.ReconnectWhenIdle = true;

        NBus::TBusQueueConfig queueConfig;
        queueConfig.NumWorkers = 1;

        MsgQueue_ = NBus::CreateMessageQueue(queueConfig);
        Session_ = NBus::TBusClientSession::Create(&Protocol_, this, sessionConfig, MsgQueue_);

        if (State_ == NProto::EWorkerState::UNHEALTHY) {
            TimeMillisBecameUnhealthy_.store(currentTimeMillis);
        }
        UpdateState(currentTimeMillis);
    }

    ~TWorkerClient() {
        while (Session_->GetInFlight() > 0) {
            Sleep(TDuration::MilliSeconds(500));
        }
        Session_->Shutdown();
        MsgQueue_->Stop();
    }

#define HANDLE_REQUEST(Kind) \
    NThreading::TFuture<NProto::T##Kind##Response> Kind(NProto::T##Kind##Request&& request) { \
        auto promise = NThreading::NewPromise<NProto::T##Kind##Response>(); \
        auto future = promise.GetFuture(); \
        THolder<TBusAsync##Kind##Request> message(new TBusAsync##Kind##Request()); \
        message->Record.Swap(&request); \
        message->Promise = promise; \
        SendImpl(std::move(message)); \
        return future; \
    }

    HANDLE_REQUEST(ProcessTask)
    HANDLE_REQUEST(KillTask)
    HANDLE_REQUEST(Subscribe)
    HANDLE_REQUEST(Unsubscribe)
#undef HANDLE_REQUEST

    NProto::EWorkerState GetState() const {
        return State_.load();
    }

    void SetState(NProto::EWorkerState state, ui64 currentTimeMillis) {
        if (State_.exchange(state) != state) {
            YQL_LOG(DEBUG) << "Worker " << Id_ << " becomes " << NProto::EWorkerState_Name(state);
            if (state == NProto::EWorkerState::UNHEALTHY) {
                TimeMillisBecameUnhealthy_.store(currentTimeMillis);
            }
        }
    }

    const TString& GetId() const {
        return Id_;
    }

    bool IsHealthy() {
        return State_.load() == NProto::EWorkerState::HEALTHY;
    }

    bool IsNeedToDoHealthCheck(ui64 currentTimeMillis) {
        const auto state = State_.load();
        const auto maxGap = MaxHeartbeatCheckGapMillis_ / 2;

        return (state == NProto::EWorkerState::HEALTHY && currentTimeMillis > TimeMillisLastHealthySeen_.load() + maxGap)
            || (state == NProto::EWorkerState::UNHEALTHY && currentTimeMillis > TimeMillisLastHealthCheckSent_.load() + maxGap);
    }

    void SetTimeLastHealthCheckSent(ui64 timeLastHealthCheckSent) {
        TimeMillisLastHealthCheckSent_.store(timeLastHealthCheckSent);
    }

    NProto::EWorkerState MarkTerminating(ui64 currentTimeMillis) {
        TimeMillisLastAliveSeen_.store(currentTimeMillis);
        SetState(NProto::EWorkerState::TERMINATING, currentTimeMillis);
        return NProto::EWorkerState::TERMINATING;
    }

    NProto::EWorkerState MarkAlive(ui64 currentTimeMillis) {
        TimeMillisLastAliveSeen_.store(currentTimeMillis);
        return UpdateState(currentTimeMillis);
    }

    NProto::EWorkerState MarkHealthy(ui64 currentTimeMillis) {
        TimeMillisLastAliveSeen_.store(currentTimeMillis);
        TimeMillisLastHealthySeen_.store(currentTimeMillis);
        return UpdateState(currentTimeMillis);
    }

    NProto::EWorkerState UpdateState(ui64 currentTimeMillis) {
        auto oldState = State_.load();
        if (oldState == NProto::EWorkerState::MUST_DIE) {
            // can never leave this state
            return oldState;
        }

        if (oldState == NProto::EWorkerState::TERMINATING) {
            if ((currentTimeMillis > TimeMillisLastAliveSeen_.load() + LoseUnhealthyWorkerAfterMillis_)
                && (currentTimeMillis > TimeMillisLastHealthySeen_.load() + LoseUnhealthyWorkerAfterMillis_))
            {
                SetState(NProto::EWorkerState::MUST_DIE, currentTimeMillis);
                return NProto::EWorkerState::MUST_DIE;
            } else {
                return NProto::EWorkerState::TERMINATING;
            }
        }

        auto newState = NProto::EWorkerState::HEALTHY;
        if ((currentTimeMillis > TimeMillisLastAliveSeen_.load() + MaxHeartbeatGapMillis_)
            || (currentTimeMillis > TimeMillisLastHealthySeen_.load() + MaxHeartbeatCheckGapMillis_))
        {
            newState = NProto::EWorkerState::UNHEALTHY;
        }

        if (newState != NProto::EWorkerState::HEALTHY && oldState == NProto::EWorkerState::UNHEALTHY
            && (currentTimeMillis > TimeMillisBecameUnhealthy_.load() + LoseUnhealthyWorkerAfterMillis_))
        {
            newState = NProto::EWorkerState::MUST_DIE;
        }

        SetState(newState, currentTimeMillis);
        return newState;
    }

private:

    void SendImpl(THolder<NBus::TBusMessage>&& msg) {
        ui16 msgType = msg->GetHeader()->Type;
        NBus::EMessageStatus status = Session_->SendMessage(msg.Get(), &WorkerAddr_);

        if (status == NBus::EMessageStatus::MESSAGE_OK) {
            Y_UNUSED(msg.Release());
            YQL_LOG(TRACE) << "Message " << Hex(msgType) << " send to " <<  WorkerAddr_;
        } else {
            SetMessageError(std::move(msg), status);
        }
    }

    void OnReply(TAutoPtr<NBus::TBusMessage> req, TAutoPtr<NBus::TBusMessage> resp) override {
        ui16 messageType = resp->GetHeader()->Type;
        switch (messageType) {
        case MTYPE_PROCESS_TASK_RESPONSE: {
            auto typedReq = static_cast<TBusAsyncProcessTaskRequest*>(req.Get());
            auto typedResp = static_cast<TBusProcessTaskResponse*>(resp.Get());
            typedReq->Promise.SetValue(typedResp->Record);
            break;
        }
        case MTYPE_KILL_TASK_RESPONSE: {
            auto typedReq = static_cast<TBusAsyncKillTaskRequest*>(req.Get());
            auto typedResp = static_cast<TBusKillTaskResponse*>(resp.Get());
            typedReq->Promise.SetValue(typedResp->Record);
            break;
        }
        case MTYPE_SUBSCRIBE_RESPONSE: {
            auto typedReq = static_cast<TBusAsyncSubscribeRequest*>(req.Get());
            auto typedResp = static_cast<TBusSubscribeResponse*>(resp.Get());
            typedReq->Promise.SetValue(typedResp->Record);
            break;
        }
        case MTYPE_UNSUBSCRIBE_RESPONSE: {
            auto typedReq = static_cast<TBusAsyncUnsubscribeRequest*>(req.Get());
            auto typedResp = static_cast<TBusUnsubscribeResponse*>(resp.Get());
            typedReq->Promise.SetValue(typedResp->Record);
            break;
        }
        default:
            YQL_LOG(WARN) << "Unknown message type: " << Hex(messageType);
        }
    }

    void OnError(TAutoPtr<NBus::TBusMessage> msg, NBus::EMessageStatus status) override {
        SetMessageError(THolder<NBus::TBusMessage>(msg), status);
    }

    void SetMessageError(THolder<NBus::TBusMessage>&& msg, NBus::EMessageStatus status) {
        ui16 msgType = msg->GetHeader()->Type;
        YQL_LOG(ERROR) << "Can't send message " << Hex(msgType)
                       << " " << NBus::GetMessageStatus(status) << ": "
                       << NBus::MessageStatusDescription(status);
        switch (status) {
            case NBus::MESSAGE_TIMEOUT:
            case NBus::MESSAGE_DELIVERY_FAILED:
            case NBus::MESSAGE_CONNECT_FAILED:
            case NBus::MESSAGE_SHUTDOWN:
                SetState(NProto::EWorkerState::UNHEALTHY, CurrentTimeMillis());
                break;
            default:
                break;
        }

#define HANDLE_MSG(type, name) \
        case MTYPE_##type##_REQUEST: { \
            auto typedReq = static_cast<TBusAsync##name##Request*>(msg.Get()); \
            typedReq->Promise.SetException(TStringBuilder() << "Send MTYPE_" << #type << "_REQUEST message failed: " \
                << NBus::MessageStatusDescription(status)); \
            break; \
        }

        switch (msgType) {
        HANDLE_MSG(PROCESS_TASK, ProcessTask)
        HANDLE_MSG(KILL_TASK, KillTask)
        HANDLE_MSG(SUBSCRIBE, Subscribe)
        HANDLE_MSG(UNSUBSCRIBE, Unsubscribe)
        default:
            YQL_LOG(ERROR) << "Unknown message type: " << Hex(msgType);
        }
#undef HANDLE_MSG
    }

private:
    const TString Id_;
    const TString CommitId_;
    const ui64 MaxHeartbeatGapMillis_;
    const ui64 MaxHeartbeatCheckGapMillis_;
    const ui64 LoseUnhealthyWorkerAfterMillis_;
    TMsgBusWorkerClientProtocol Protocol_;
    NBus::TNetAddr WorkerAddr_;
    NBus::TBusMessageQueuePtr MsgQueue_;
    NBus::TBusClientSessionPtr Session_;
    std::atomic<NProto::EWorkerState> State_;
    std::atomic<ui64> TimeMillisLastAliveSeen_;
    std::atomic<ui64> TimeMillisLastHealthySeen_;
    std::atomic<ui64> TimeMillisLastHealthCheckSent_ = 0;
    std::atomic<ui64> TimeMillisBecameUnhealthy_ = 0;
};


class TMsgBusWorkerApi: public IWorkerApi, public IInspectorHandler, public std::enable_shared_from_this<TMsgBusWorkerApi> {
public:
    using TPtr = std::shared_ptr<TMsgBusWorkerApi>;
    using TWeakPtr = std::weak_ptr<TMsgBusWorkerApi>;

    explicit TMsgBusWorkerApi(TMsgBusWorkerApiConfig config)
        : Config_(std::move(config))
    {
    }

    ~TMsgBusWorkerApi() {
        Stop();
    }

    void Start() {
        InspectorServer_ = std::make_shared<TMsgBusInspectorServer>(Config_, weak_from_this());
        BackgroundThreads_.Add(std::bind(&TMsgBusWorkerApi::DoWorkerCheck, this));
        BackgroundThreads_.Add(std::bind(&TMsgBusWorkerApi::DoOperationCheck, this));
    }

    void Stop() {
        try {
            BackgroundThreads_.Stop();
            if (InspectorServer_) {
                InspectorServer_->Shutdown();
                InspectorServer_.reset();
            }
            WorkerClients_.clear();
        } catch (...) {
            YQL_LOG(ERROR) << "Error while stopping worker api: " << CurrentExceptionMessage();
        }
    }

    bool IsHealthy() const override {
        if (!InspectorServer_) {
            return false;
        }
        for (const auto& [_, client]: WorkerClients_) {
            if (client->IsHealthy()) {
                return true;
            }
        }
        return false;
    }

    std::expected<std::shared_ptr<ITaskHandle>, TRunTaskError> RunTask(NProto::ETaskAction taskAction, NProto::TTaskData&& task, std::weak_ptr<ITaskResultCallback> callback) override {
        auto id = task.GetId();
        auto username = task.GetUsername();
        YQL_LOG(INFO) << "Run task " << id;

        TWorkerClient::TPtr selectedWorker = SelectWorker(task);
        if (!selectedWorker) {
            YQL_LOG(ERROR) << "No healthy workers for task " << id;
            return std::unexpected(TRunTaskError(TRunTaskError::EReason::REJECTED, "No healthy workers"));
        }
        auto handle = std::make_shared<TTaskHandle>(id, username, callback, selectedWorker);
        with_lock (OperationLock_) {
            if (RunningOperations_.contains(id)) {
                YQL_LOG(ERROR) << "Task " << id << " is already running";
                return std::unexpected(TRunTaskError(TRunTaskError::EReason::ALREADY_RUNNING, TStringBuilder() << "Task " << id << " is already running"));
            }
            RunningOperations_[id] = handle;
        }

        NProto::TProcessTaskRequest processRequest;
        processRequest.SetAction(taskAction);
        processRequest.MutableData()->Swap(&task);
        processRequest.SetSendUpdatesMask(NProto::ETaskResultChange::MAX_VALUE);

        auto processResponse = selectedWorker->ProcessTask(std::move(processRequest)).ExtractValueSync();
        TRunTaskError::EReason reason = TRunTaskError::EReason::FAIL;
        switch (processResponse.GetStatus()) {
            case NProto::TProcessTaskResponse_EStatus_SUBMITED:
                return handle;
            case NProto::TProcessTaskResponse_EStatus_FAIL:
            case NProto::TProcessTaskResponse_EStatus_UNKNOWN_STATUS:
                reason = TRunTaskError::EReason::FAIL;
                break;
            case NProto::TProcessTaskResponse_EStatus_ALREADY_RUNNING:
                reason = TRunTaskError::EReason::ALREADY_RUNNING;
                break;
            case NProto::TProcessTaskResponse_EStatus_WORKING_QUEUE_IS_FULL:
            case NProto::TProcessTaskResponse_EStatus_WORKING_USER_QUEUE_IS_FULL:
            case NProto::TProcessTaskResponse_EStatus_TOO_MANY_RUNNING_TASKS:
                reason = TRunTaskError::EReason::REJECTED;
                break;
        }
        with_lock (OperationLock_) {
            RunningOperations_.erase(id);
        }
        YQL_LOG(ERROR) << "Error runing task " << id << ", status: "
            << NProto::TProcessTaskResponse_EStatus_Name(processResponse.GetStatus())
            << ", message: " << processResponse.GetMessage();
        return std::unexpected(TRunTaskError(reason, processResponse.GetMessage()));
    }

private:
    class TTaskHandle: public ITaskHandle, public std::enable_shared_from_this<TTaskHandle> {
    public:
        using TPtr = std::shared_ptr<TTaskHandle>;
        using TWeakPtr = std::weak_ptr<TTaskHandle>;

        TTaskHandle(TString id, TString username, std::weak_ptr<ITaskResultCallback> callback, TWorkerClient::TWeakPtr client)
            : TaskId_(std::move(id))
            , Username_(std::move(username))
            , Callback_(std::move(callback))
            , WorkerClient_(std::move(client))
        {
        }

        TString GetTaskId() final {
            return TaskId_;
        }

        TTaskWorkerInfo GetWorkerInfo() final {
            std::shared_ptr<TTaskWorkerInfo> info;
            with_lock (WorkerInfoLock_) {
                info = WorkerInfo_;
            }
            return info ? *info : TTaskWorkerInfo{};
        }

        std::shared_ptr<ITaskResultCallback> GetCallback() const {
            return Callback_.lock();
        }

        void UpdateWorkerInfo(const NProto::TStatusUpdateRequest& request) {
            std::shared_ptr<TTaskWorkerInfo> info = std::make_shared<TTaskWorkerInfo>();
            info->WorkerId = request.GetWorkerId();
            info->WorkerVersion = request.GetWorkerVersion();
            info->WorkerHost = request.GetWorkerHost();
            info->WorkerPid = request.GetWorkerPid();

            bool initial = false;
            with_lock (WorkerInfoLock_) {
                initial = !WorkerInfo_;
                WorkerInfo_ = info;
            }
            // Make subscription after initial status update from the worker
            // Before first update the operation may not be accepted yet
            if (initial) {
                YQL_LOG(INFO) << "Worker id=" << info->WorkerId << ", pid=" << info->WorkerPid << ", got the task " << TaskId_;
            } else {
                return;
            }

            switch (request.GetResult().GetStatus()) {
                case NProto::ETaskStatus::COMPLETED:
                case NProto::ETaskStatus::ABORTED:
                case NProto::ETaskStatus::ERROR:
                    // No subscription is required for terminal status
                    return;
                default:
                    break;
            }

            auto client = WorkerClient_.lock();
            if (!client) {
                return;
            }
            YQL_LOG(INFO) << "Make task " << TaskId_ << " subscription";
            try {
                NProto::TSubscribeRequest subscribeRequest;
                subscribeRequest.SetChannelId(0); // unused
                subscribeRequest.SetTaskId(TaskId_);
                subscribeRequest.SetUsername(Username_);
                subscribeRequest.SetMask(NProto::ETaskResultChange::MAX_VALUE);
                subscribeRequest.SetType(NProto::ESubscriptionType::CHANGES);
                auto subscribeResponse = client->Subscribe(std::move(subscribeRequest)).ExtractValueSync();
                if (subscribeResponse.GetStatus() != NProto::TSubscribeResponse_EStatus_OK) {
                    YQL_LOG(ERROR) << "Error subscribing to task " << TaskId_
                        << ", status=" << NProto::TSubscribeResponse_EStatus_Name(subscribeResponse.GetStatus())
                        << ", message=" << subscribeResponse.GetMessage();
                }
            } catch (...) {
                YQL_LOG(ERROR) << "Error subscribing to task " << TaskId_ << ": " << CurrentExceptionMessage();
            }
        }

        std::expected<void, TString> Cancel() final {
            YQL_LOG(INFO) << "Cancel task " << TaskId_;
            auto client = WorkerClient_.lock();
            if (!client) {
                return std::unexpected(TStringBuilder() << "Worker client for the task " << TaskId_ << " is not available");
            }

            NProto::TKillTaskRequest request;
            request.SetId(TaskId_);
            auto response = client->KillTask(std::move(request)).ExtractValueSync();
            if (response.GetStatus() != NProto::TKillTaskResponse_EStatus_KILLED) {
                return std::unexpected(TStringBuilder() << NProto::TKillTaskResponse_EStatus_Name(response.GetStatus()) << ": " << response.GetMessage());
            }
            WorkerClient_.reset();
            return {};
        }

    private:
        const TString TaskId_;
        const TString Username_;
        std::shared_ptr<TTaskWorkerInfo> WorkerInfo_;
        TSpinLock WorkerInfoLock_;
        std::weak_ptr<ITaskResultCallback> Callback_;
        TWorkerClient::TWeakPtr WorkerClient_;
    };

    static const ui32 MAX_VERSION = 1000000;
    static constexpr TStringBuf HEALTH_CHECK_TASK_ID_PREFIX = "__HEALTH_CHECK__"_sb;

    static NProto::TTaskResult MakeFatalErrorResult(TString message) {
        NProto::TTaskResult result;

        result.SetStatus(NProto::ETaskStatus::ERROR);
        result.SetRevision(MAX_VERSION);
        auto issue = result.MutableIssues()->Add();
        issue->Setmessage(std::move(message));
        issue->Setseverity(ESeverity::TSeverityIds_ESeverityId_S_FATAL);
        issue->Setissue_code(UNEXPECTED_ERROR);
        auto pos = issue->Mutableposition();
        pos->Setfile("");
        pos->Setcolumn(0);
        pos->Setrow(0);

        return result;
    }

    TWorkerClient::TPtr SelectWorker(const NProto::TTaskData& task) const {
        Y_UNUSED(task); // TODO: use task attributes to filter workers by host/version
        std::vector<TWorkerClient::TPtr> workers;
        with_lock (WorkerLock_) {
            for (const auto& [_, client]: WorkerClients_) {
                if (client->IsHealthy()) {
                    workers.push_back(client);
                }
            }
        }
        if (workers.size() > 1) {
            Shuffle(workers.begin(), workers.end());
        }
        if (!workers.empty()) {
            return workers.front();
        }
        return {};
    }

    TFuture<void> OnStatusUpdate(NProto::TStatusUpdateRequest&& request) override {
        auto id = request.GetTaskId();
        auto workerId = request.GetWorkerId();
        ui64 currentTimeMillis = request.GetSentTime();

        YQL_LOG(DEBUG) << "OnStatusUpdate, taskId=" << id << ", status=" << NProto::ETaskStatus_Name(request.GetResult().GetStatus());

        MarkHealthy(workerId, currentTimeMillis);

        if (id.StartsWith(HEALTH_CHECK_TASK_ID_PREFIX)) {
            return MakeFuture();
        }

        TTaskHandle::TWeakPtr op;
        with_lock (OperationLock_) {
            auto it = RunningOperations_.find(id);
            if (it != RunningOperations_.end()) {
                op = it->second;
            }
        }

        if (auto handle = op.lock()) {
            handle->UpdateWorkerInfo(request);
            if (auto callback = handle->GetCallback()) {
                if (request.HasResult()) {
                    return callback->Notify(request.GetResult(), currentTimeMillis);
                }
            }
        }
        return MakeFuture();
    }

    TFuture<NProto::TTaskSubscriptionNotifyResponse::EStatus> OnTaskSubscriptionNotify(NProto::TTaskSubscriptionNotifyRequest&& request) override {
        auto id = request.GetTaskId();
        YQL_LOG(DEBUG) << "OnTaskSubscriptionNotify, taskId=" << id;
        TTaskHandle::TWeakPtr op;
        with_lock (OperationLock_) {
            auto it = RunningOperations_.find(id);
            if (it != RunningOperations_.end()) {
                op = it->second;
            }
        }

        if (auto handle = op.lock()) {
            if (auto callback = handle->GetCallback()) {
                if (request.HasResult()) {
                    return callback->Notify(request.GetResult(), request.GetSentTime()).Return(NProto::TTaskSubscriptionNotifyResponse_EStatus_OK);
                }
                return MakeFuture<NProto::TTaskSubscriptionNotifyResponse::EStatus>(NProto::TTaskSubscriptionNotifyResponse_EStatus_OK);
            }
        }
        return MakeFuture<NProto::TTaskSubscriptionNotifyResponse::EStatus>(NProto::TTaskSubscriptionNotifyResponse_EStatus_NOT_EXISTS);
    }

    NProto::THeartbeatResponse OnHeartbeat(const NBus::TNetAddr& peerAddr, NProto::THeartbeatRequest&& request) override {
        TString workerId = request.GetWorkerId();
        ui32 workerPort = request.GetPort();
        ui64 currentTimeMillis = request.GetSentTime();
        NProto::EWorkerState workerState = request.GetWorkerState();
        TString commitId = request.GetProgramCommitId();
        if (!commitId && request.GetSvnRevision()) {
            commitId = ToString(request.GetSvnRevision());
        }
        YQL_LOG(TRACE) << "OnHeartbeat, workerId=" << workerId << ", state=" << NProto::EWorkerState_Name(workerState);

        NProto::EWorkerState updatedWorkerState;
        if (workerState == NProto::EWorkerState::MUST_DIE) {
            DoLostWorker(workerId);
            updatedWorkerState = workerState;
        } else {
            std::shared_ptr<TWorkerClient> worker;
            with_lock(WorkerLock_) {
                auto it = WorkerClients_.find(workerId);
                if (it != WorkerClients_.end()) {
                    worker = it->second;
                }
            }
            NBus::TNetAddr workerAddr = NBus::TNetAddr(NAddr::PrintHost(peerAddr), workerPort);
            if (!worker) {
                worker = std::make_shared<TWorkerClient>(workerId, commitId, currentTimeMillis, workerState, workerAddr, Config_);
                with_lock(WorkerLock_) {
                    WorkerClients_[workerId] = worker;
                }
                updatedWorkerState = worker->GetState();
            } else if (workerState == NProto::EWorkerState::TERMINATING) {
                updatedWorkerState = worker->MarkTerminating(currentTimeMillis);
            } else {
                updatedWorkerState = worker->MarkAlive(currentTimeMillis);
            }
        }

        NProto::THeartbeatResponse response;
        response.SetMaxHealthcheckGap(Config_.MaxHeartbeatGap.MilliSeconds());
        response.SetMaxHeartbeatGap(Config_.MaxHeartbeatGap.MilliSeconds());
        response.SetLoseUnhealthyWorkerAfter(Config_.LoseUnhealthyWorkerAfter.MilliSeconds());
        response.SetWorkerState(updatedWorkerState);

        return response;
    }

    NProto::EWorkerState MarkHealthy(const TString& workerId, ui64 currentTimeMillis) {
        std::shared_ptr<TWorkerClient> worker;
        with_lock (WorkerLock_) {
            if (auto it = WorkerClients_.find(workerId); it != WorkerClients_.end()) {
                worker = it->second;
            }
        }
        if (worker) {
            return worker->MarkHealthy(currentTimeMillis);
        }
        // unknown workers must die
        return NProto::EWorkerState::MUST_DIE;
    }

    void DoLostWorker(const TString& workerId) {
        YQL_LOG(WARN) << "Worker " << workerId << " becomes lost";

        size_t removed = 0;
        with_lock (WorkerLock_) {
            removed = WorkerClients_.erase(workerId);
        }
        if (0 == removed) {
            YQL_LOG(ERROR) << "Unknown worker lost " << workerId;
        }
    }

    void DoHealthCheck(const std::shared_ptr<TWorkerClient>& worker) {
        YQL_LOG(DEBUG) << "Run health check task on yqlworker " << worker->GetId();
        NProto::TProcessTaskRequest healthCheckRequest;
        healthCheckRequest.SetAction(NProto::ETaskAction::RUN);
        healthCheckRequest.SetSendUpdatesMask(NProto::ETaskResultChange::STATUS_COMPLETED | NProto::ETaskResultChange::STATUS_ERROR);
        auto data = healthCheckRequest.MutableData();
        data->SetId(TStringBuilder() << HEALTH_CHECK_TASK_ID_PREFIX << CreateGuidAsString());
        data->SetProgram("((return world))");
        data->SetSyntax(NProto::ESyntax::YQL);
        data->SetLangVer(*FormatLangVersion(GetMaxReleasedLangVersion()));
        data->SetUsername("yql");
        data->SetResultFormat(NProto::EDataFormat::YSON_BINARY);
        worker->ProcessTask(std::move(healthCheckRequest));
        worker->SetTimeLastHealthCheckSent(CurrentTimeMillis());
    }

    TDuration DoWorkerCheck() {
        try {
            std::vector<TString> toLost;
            std::vector<TWorkerClient::TPtr> toHealthCheck;
            ui64 currentTimeMillis = CurrentTimeMillis();
            with_lock (WorkerLock_) {
                for (const auto& [_, client]: WorkerClients_) {
                    if (client->UpdateState(currentTimeMillis) == NProto::EWorkerState::MUST_DIE) {
                        toLost.push_back(client->GetId());
                    } else if (client->IsNeedToDoHealthCheck(currentTimeMillis)) {
                        toHealthCheck.push_back(client);
                    }
                }
            }

            for (const auto& id: toLost) {
                DoLostWorker(id);
            }

            for (const auto& worker: toHealthCheck) {
                DoHealthCheck(worker);
            }

        } catch (...) {
            YQL_LOG(ERROR) << "Error in DoWorkerCheck: " << CurrentExceptionMessage();
        }
        return Config_.WorkerCheckPeriod;
    }

    TDuration DoOperationCheck() {
        with_lock (OperationLock_) {
            for (auto it = RunningOperations_.begin(); it != RunningOperations_.end();) {
                if (it->second.expired()) {
                    it = RunningOperations_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        return Config_.OperationCheckPeriod;
    }
private:
    const TMsgBusWorkerApiConfig Config_;
    std::shared_ptr<TMsgBusInspectorServer> InspectorServer_;
    TMutex WorkerLock_;
    std::unordered_map<TString, TWorkerClient::TPtr, THash<TString>> WorkerClients_;
    TMutex OperationLock_;
    std::unordered_map<TString, TTaskHandle::TWeakPtr, THash<TString>> RunningOperations_;
    TBackgroundThreads BackgroundThreads_;
};

}

std::shared_ptr<IWorkerApi> MakeMsgBusWorkerApi(TMsgBusWorkerApiConfig config) {
    auto res = std::make_shared<TMsgBusWorkerApi>(std::move(config));
    res->Start();
    return res;
}

} // namespace NYql::NWorkerApi
