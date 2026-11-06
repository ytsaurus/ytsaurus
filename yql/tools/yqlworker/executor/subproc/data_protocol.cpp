#include "data_protocol.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/common/metrics/sensors_group.h>

#include <util/stream/format.h>
#include <util/system/yassert.h>


namespace NYql {
namespace NSubprocessTaskExecutor {
namespace {

//////////////////////////////////////////////////////////////////////////////
// TDataProtocol
//////////////////////////////////////////////////////////////////////////////

template <typename TPromiseType, class TBufferRecord, int MType>
class TBusRequestBaseMessage: public NBus::TBusBufferMessage<TBufferRecord, MType> {
public:
    using TBase = NBus::TBusBufferMessage<TBufferRecord, MType>;

    NThreading::TPromise<TPromiseType> Promise;
    ui32 Retries = 0u;
};


//-- GetTaskRequest / Response --------------------------------

using TBusGetTaskRequest = TBusRequestBaseMessage<NProto::TGetTaskResponse, NProto::TGetTaskRequest, MTYPE_GET_TASK_REQUEST>;

using TBusGetTaskResponse = NBus::TBusBufferMessage<NProto::TGetTaskResponse, MTYPE_GET_TASK_RESPONSE>;

//-- UpdateTaskStatusRequest -----------------------

using TBusTaskWatchEventRequest = TBusRequestBaseMessage<void, NProto::TTaskWatchEventRequest, MTYPE_TASK_WATCH_EVENT_REQUEST>;

//-- MetricsRegistrySnapshotRequest / Response ------------------------

using TBusMetricsRegistrySnapshotRequest = TBusRequestBaseMessage<void, NProto::TMetricsRegistrySnapshotRequest, MTYPE_METRICS_REGISTRY_SNAPSHOT_REQUEST>;

using TBusMetricsRegistrySnapshotResponse = NBus::TBusBufferMessage<NProto::TMetricsRegistrySnapshotResponse, MTYPE_METRICS_REGISTRY_SNAPSHOT_RESPONSE>;

} // namespace

//////////////////////////////////////////////////////////////////////////////
// TDataProtocol
//////////////////////////////////////////////////////////////////////////////
TDataProtocol::TDataProtocol(int port)
    : NBus::TBusBufferProtocol("data", port)
{
    RegisterType(new TBusGetTaskRequest);
    RegisterType(new TBusGetTaskResponse);
    RegisterType(new TBusTaskWatchEventRequest);
    RegisterType(new TBusTaskWatchEventResponse);
    RegisterType(new TBusMetricsRegistrySnapshotRequest);
    RegisterType(new TBusMetricsRegistrySnapshotResponse);
}

//////////////////////////////////////////////////////////////////////////////
// TDataServer
//////////////////////////////////////////////////////////////////////////////
struct TDataServer::TImpl final: public NBus::IBusServerHandler
{
    TDataProtocol Protocol;
    NBus::TBusMessageQueuePtr BusQueue;
    NBus::TBusServerSessionPtr Session;
    IDataServerHandler* Handler;
    // counters
    TSensorCounterPtr GetTaskRps;
    TSensorCounterPtr TaskWatchEventRps;
    TSensorCounterPtr MetricsSnapshotRps;
    TSensorCounterPtr ErrorsRps;


    TImpl(
            const TVector<NBus::TBindResult>& bindTo,
            int nThreads,
            IDataServerHandler* handler)
        : Protocol(bindTo[0].Addr.GetPort())
        , Handler(handler)
    {
        InitCounters();

        NBus::TBusQueueConfig queueCfg;
        queueCfg.Name = "data";
        queueCfg.NumWorkers = nThreads;
        BusQueue = NBus::CreateMessageQueue(queueCfg, "data");

        NBus::TBusClientSessionConfig sessionCfg;
        Session = NBus::TBusServerSession::Create(
                    &Protocol, this, sessionCfg, BusQueue, bindTo);
        Y_ASSERT(Session && "probably somebody is listening on the same port");

        YQL_LOG(INFO) << "data server listening on port "
                      << Session->GetActualListenPort();
    }

    void Shutdown() {
        Session->Shutdown();
    }

    int ActualPort() const {
        return Session->GetActualListenPort();
    }

private:
    void OnMessage(NBus::TOnMessageContext& ctx) override {
        try {
            ui16 messageType = ctx.GetMessage()->GetHeader()->Type;
            switch (messageType) {
            case MTYPE_GET_TASK_REQUEST: {
                GetTaskRps->Inc();
                auto reqMsg = static_cast<TBusGetTaskRequest::TBase*>(ctx.GetMessage());
                auto response = Handler->OnGetTask(std::move(reqMsg->Record));
                ctx.SendReplyMove(new TBusGetTaskResponse(std::move(response)));
                break;
            }
            case MTYPE_TASK_WATCH_EVENT_REQUEST: {
                TaskWatchEventRps->Inc();
                auto reqMsg = static_cast<TBusTaskWatchEventRequest::TBase*>(ctx.GetMessage());
                Handler->OnTaskWatchEvent(std::move(reqMsg->Record), &ctx);
                if (ctx.IsInWork()) {
                    ctx.SendReplyMove(new TBusTaskWatchEventResponse);
                }
                break;
            }
            case MTYPE_METRICS_REGISTRY_SNAPSHOT_REQUEST: {
                MetricsSnapshotRps->Inc();
                auto reqMsg = static_cast<TBusMetricsRegistrySnapshotRequest::TBase*>(ctx.GetMessage());
                Handler->OnMetricsRegistrySnapshot(std::move(reqMsg->Record));
                ctx.SendReplyMove(new TBusMetricsRegistrySnapshotResponse);
                break;
            }
            default:
                YQL_LOG(WARN) << "Unknown message type: " << Hex(messageType);
            }
        } catch (...) {
            YQL_LOG(ERROR) << "unhandled exception in data server handler: "
                           << CurrentExceptionMessage();
            ctx.ForgetRequest();
        }
    }

    // called when message or reply can't be delivered
    void OnError(
            TAutoPtr<NBus::TBusMessage> msg,
            NBus::EMessageStatus status) override
    {
        ErrorsRps->Inc();
        YQL_LOG(ERROR) << "Cant deliver message " << Hex(msg->GetHeader()->Type)
                       << " " << NBus::GetMessageStatus(status) << ": "
                       << NBus::MessageStatusDescription(status);
    }

    void InitCounters() {
        auto serverSensors = GetSensorsGroupFor(NSensorComponent::kDataServer);
        auto newRpsCounter = [&serverSensors](const TString& name) {
            static TString rpsLabel("rps");
            return serverSensors->GetNamedCounter(rpsLabel, name, true);
        };

        GetTaskRps = newRpsCounter("get_task");
        TaskWatchEventRps = newRpsCounter("task_watch_event");
        MetricsSnapshotRps = newRpsCounter("metrics_snapshot");
        ErrorsRps = newRpsCounter("errors");
    }
};

TDataServer::TDataServer(
        const TVector<NBus::TBindResult>& bindTo,
        int nThreads,
        IDataServerHandler* handler)
    : Impl_(new TImpl(bindTo, nThreads, handler))
{
}

TDataServer::~TDataServer()
{
    Shutdown();
}

void TDataServer::Shutdown()
{
    if (Impl_) {
        Impl_->Shutdown();
        Impl_.Reset();
    }
}

int TDataServer::ActualPort() const
{
    if (Impl_) {
        return Impl_->ActualPort();
    }
    return -1;
}

//////////////////////////////////////////////////////////////////////////////
// TDataClient
//////////////////////////////////////////////////////////////////////////////
struct TDataClient::TImpl final: public NBus::IBusClientHandler
{
    TDataProtocol Protocol;
    NBus::TNetAddr ServerAddr;
    NBus::TBusMessageQueuePtr MsgQueue;
    NBus::TBusClientSessionPtr Session;
    ui32 NumRetries;
    TDuration RetryDelay;

    struct TDelayedSend: public NBus::NPrivate::IScheduleItem {
        TDelayedSend(TDuration delay, const TImpl* client, THolder<NBus::TBusMessage>&& msg)
            : NBus::NPrivate::IScheduleItem(TInstant::Now() + delay)
            , Client(client)
            , Msg(std::move(msg))
        {
        }
        void Do() override {
            Y_ENSURE(Client);
            Y_ENSURE(Msg);
            Client->SendImpl(std::move(Msg));
        }
        const TImpl* Client;
        THolder<NBus::TBusMessage> Msg;
    };

    explicit TImpl(int port)
        : Protocol(port)
        , ServerAddr(TStringBuf("localhost"), port)
        , NumRetries(10)
        , RetryDelay(TDuration::Seconds(2))
    {
        NBus::TBusClientSessionConfig config;
        config.ConnectTimeout = TDuration::Seconds(20).MilliSeconds();
        config.TotalTimeout = TDuration::Seconds(60).MilliSeconds();
        config.ReconnectWhenIdle = true;

        NBus::TBusQueueConfig queueConfig;
        queueConfig.NumWorkers = 1;

        MsgQueue = NBus::CreateMessageQueue(queueConfig);
        Session = NBus::TBusClientSession::Create(
                    &Protocol, this, config, MsgQueue);
    }

    ~TImpl() {
        while (Session->GetInFlight() > 0) {
            Sleep(TDuration::MilliSeconds(500));
        }
        Session->Shutdown();
        MsgQueue->Stop();
    }

    NThreading::TFuture<NProto::TGetTaskResponse>
    GetTask(NProto::TGetTaskRequest&& request) const {
        auto promise = NThreading::NewPromise<NProto::TGetTaskResponse>();
        auto future = promise.GetFuture();
        THolder<TBusGetTaskRequest> message(new TBusGetTaskRequest);
        message->Record.Swap(&request);
        message->Promise = promise;
        SendImpl(std::move(message));
        return future;
    }

    NThreading::TFuture<void>
    SendTaskWatchEvent(NProto::TTaskWatchEventRequest&& request) const {
        auto promise = NThreading::NewPromise();
        auto future = promise.GetFuture();
        THolder<TBusTaskWatchEventRequest> message(new TBusTaskWatchEventRequest);
        message->Record.Swap(&request);
        message->Promise = promise;
        SendImpl(std::move(message));
        return future;
    }

    NThreading::TFuture<void>
    SendMetricsSnapshot(NProto::TMetricsRegistrySnapshotRequest&& request) const {
        auto promise = NThreading::NewPromise();
        auto future = promise.GetFuture();
        THolder<TBusMetricsRegistrySnapshotRequest> message(new TBusMetricsRegistrySnapshotRequest);
        message->Record.Swap(&request);
        message->Promise = promise;
        SendImpl(std::move(message));
        return future;
    }

private:
    void SendImpl(THolder<NBus::TBusMessage>&& msg) const {
        // TODO (jamel): output human readable message types
        ui16 msgType = msg->GetHeader()->Type;
        NBus::EMessageStatus status = Session->SendMessage(msg.Get(), &ServerAddr);

        if (status == NBus::EMessageStatus::MESSAGE_OK) {
            Y_UNUSED(msg.Release());
            YQL_LOG(INFO) << "Message " << Hex(msgType) << " send to " <<  ServerAddr;
        } else {
            SetMessageError(std::move(msg), status);
        }
    }

    void OnReply(
            TAutoPtr<NBus::TBusMessage> req,
            TAutoPtr<NBus::TBusMessage> resp) override
    {
        ui16 messageType = resp->GetHeader()->Type;
        switch (messageType) {
        case MTYPE_GET_TASK_RESPONSE: {
            auto typedReq = static_cast<TBusGetTaskRequest*>(req.Get());
            auto typedResp = static_cast<TBusGetTaskResponse*>(resp.Get());
            typedReq->Promise.SetValue(typedResp->Record);
            break;
        }
        case MTYPE_TASK_WATCH_EVENT_RESPONSE: {
            auto typedReq = static_cast<TBusTaskWatchEventRequest*>(req.Get());
            typedReq->Promise.SetValue();
            break;
        }
        case MTYPE_METRICS_REGISTRY_SNAPSHOT_RESPONSE: {
            auto typedReq = static_cast<TBusMetricsRegistrySnapshotRequest*>(req.Get());
            typedReq->Promise.SetValue();
            break;
        }
        default:
            YQL_LOG(WARN) << "Unknown message type: " << Hex(messageType);
        }
    }

    void OnError(TAutoPtr<NBus::TBusMessage> msg, NBus::EMessageStatus status) override {
        SetMessageError(THolder<NBus::TBusMessage>(msg), status);
    }

    void SetMessageError(THolder<NBus::TBusMessage>&& msg, NBus::EMessageStatus status) const {
        ui16 msgType = msg->GetHeader()->Type;
        YQL_LOG(ERROR) << "Can't send message " << Hex(msgType)
                       << " " << NBus::GetMessageStatus(status) << ": "
                       << NBus::MessageStatusDescription(status);
        ui32 retry = Max<ui32>();
        switch (status) {
            case NBus::MESSAGE_TIMEOUT:
            case NBus::MESSAGE_DELIVERY_FAILED:
            case NBus::MESSAGE_CONNECT_FAILED:
                switch (msgType) {
                case MTYPE_GET_TASK_REQUEST: {
                    auto typedReq = static_cast<TBusGetTaskRequest*>(msg.Get());
                    retry = ++typedReq->Retries;
                    break;
                }
                case MTYPE_TASK_WATCH_EVENT_REQUEST: {
                    auto typedReq = static_cast<TBusTaskWatchEventRequest*>(msg.Get());
                    retry = ++typedReq->Retries;
                    break;
                }
                case MTYPE_METRICS_REGISTRY_SNAPSHOT_REQUEST: {
                    auto typedReq = static_cast<TBusMetricsRegistrySnapshotRequest*>(msg.Get());
                    retry = ++typedReq->Retries;
                    break;
                }
                default:
                    YQL_LOG(ERROR) << "Unknown message type: " << Hex(msgType);
                }
            default:
                break;
        }
        if (retry <= NumRetries) {
            YQL_LOG(INFO) << "Retry sending message " << Hex(msgType) << '(' << retry << ')';
            msg->Reset();
            MsgQueue->Schedule(new TDelayedSend(RetryDelay, this, std::move(msg)));
        } else {
            switch (msgType) {
            case MTYPE_GET_TASK_REQUEST: {
                auto typedReq = static_cast<TBusGetTaskRequest*>(msg.Get());
                typedReq->Promise.SetException("Send MTYPE_GET_TASK_REQUEST message failed: " + TString(NBus::MessageStatusDescription(status)));
                break;
            }
            case MTYPE_TASK_WATCH_EVENT_REQUEST: {
                auto typedReq = static_cast<TBusTaskWatchEventRequest*>(msg.Get());
                typedReq->Promise.SetException("Send MTYPE_TASK_WATCH_EVENT_REQUEST message failed: " + TString(NBus::MessageStatusDescription(status)));
                break;
            }
            case MTYPE_METRICS_REGISTRY_SNAPSHOT_REQUEST: {
                auto typedReq = static_cast<TBusMetricsRegistrySnapshotRequest*>(msg.Get());
                typedReq->Promise.SetException("Send MTYPE_METRICS_REGISTRY_SNAPSHOT_REQUEST message failed: " + TString(NBus::MessageStatusDescription(status)));
                break;
            }
            default:
                YQL_LOG(ERROR) << "Unknown message type: " << Hex(msgType);
            }
        }
    }
};

TDataClient::TDataClient(int port)
    : Impl_(new TImpl(port))
{
}

TDataClient::~TDataClient()
{
}

NThreading::TFuture<NProto::TGetTaskResponse> TDataClient::GetTask(
        NProto::TGetTaskRequest&& request) const
{
    return Impl_->GetTask(std::move(request));
}

NThreading::TFuture<void> TDataClient::SendTaskWatchEvent(
        NProto::TTaskWatchEventRequest&& request) const
{
    return Impl_->SendTaskWatchEvent(std::move(request));
}

NThreading::TFuture<void> TDataClient::SendMetricsRegistrySnapshot(
        NProto::TMetricsRegistrySnapshotRequest&& request) const
{
    return Impl_->SendMetricsSnapshot(std::move(request));
}

} // namespace NSubprocessTaskExecutor
} // namespace NYql
