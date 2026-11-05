#include "inspector_client.h"
#include "inspector_protocol.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/tools/yqlworker/config/config.h>
#include <yql/tools/yqlworker/misc/conductor.h>
#include <yql/tools/yqlworker/misc/deploy_sd.h>

#include <util/generic/utility.h>
#include <util/network/address.h>
#include <util/random/random.h>
#include <util/stream/format.h>
#include <util/system/rwlock.h>


namespace NYql {
namespace {

static const ui32 kDefaultMaxSendRetries = 5;
static const ui32 kDefaultRetryIntervalMillis = 500;
static const ui64 kMaxDelayMillis = 5'000;

TString FormatRequest(const NProto::TStatusUpdateRequest& request) {
    if (!request.HasResult()) {
        return PbMessageToStr(request);
    }

    NProto::TStatusUpdateRequest copy(request);
    auto data = copy.MutableResult();
    data->ClearAst();
    data->ClearPlan();
    data->ClearErrors();
    data->ClearIssues();
    data->ClearResults();
    data->ClearStatistics();
    return PbMessageToStr(copy);
}

//////////////////////////////////////////////////////////////////////////////
// TInspectorSession
//////////////////////////////////////////////////////////////////////////////

struct TInspectorSession;
using TInspectorSessionPtr = TIntrusivePtr<TInspectorSession>;

struct TInspectorSession: public TThrRefBase {
    TString Host;
    int Port = 0;
    NBus::TNetAddr PeerAddress;
    NBus::TBusClientSessionPtr Session;
    bool Moved = false;

    void Open(const NProto::TInspectorConfig& config,
              TStringBuf host,
              NBus::IBusClientHandler* handler,
              NBus::TBusProtocol* protocol,
              NBus::TBusMessageQueuePtr queue)
    {
        Host = host;
        Port = config.GetPort();
        PeerAddress = NBus::TNetAddr(Host, Port);
        YQL_CLOG(INFO, Net) << "open session with Inspector at "
                            << Host << ':' << Port << ", address: " << PeerAddress;

        NBus::TBusClientSessionConfig sessionConfig;
        sessionConfig.ConnectTimeout = config.GetSendTimeoutSeconds() * 1000;
        sessionConfig.TotalTimeout = config.GetTotalTimeoutSeconds() * 1000;
        sessionConfig.MaxInFlight = config.GetMaxInFlight();
        sessionConfig.ReconnectWhenIdle = true;
        sessionConfig.ExecuteOnReplyInWorkerPool = false;
        sessionConfig.NumRetries = 10;
        sessionConfig.RetryInterval = TDuration::Seconds(2).MilliSeconds();

        Session = NBus::TBusClientSession::Create(
                    protocol, handler, sessionConfig, queue);
    }

    bool NeedReopen() const {
        Y_ENSURE(Host, "Host is not specified");
        Y_ENSURE(Port, "Port is not specified");
        try {
            auto peerAddr = NBus::TNetAddr(Host, Port);
            return !NAddr::IsSame(PeerAddress, peerAddr);
        } catch (...) {
            YQL_CLOG(ERROR, Net) << "NeedReopen exception: " << CurrentExceptionMessage();
        }
        return false;
    }

    ~TInspectorSession() {
        if (Session && !Moved) {
            YQL_CLOG(INFO, Net) << "Shutdown session for " << PeerAddress;
            WaitAllMessagesDelivered();
            Session->Shutdown();
        }
    }

    void WaitAllMessagesDelivered() const {
        // wait till all messages are send
        while (Session->GetInFlight() > 0) {
            Sleep(TDuration::MilliSeconds(500));
        }
    }

    NBus::EMessageStatus Send(NBus::TBusMessage* msg) const {
        auto type = static_cast<EInspectorMessageType>(msg->GetHeader()->Type);
        auto status = Session->SendMessage(msg, &PeerAddress, false);

        if (status == NBus::EMessageStatus::MESSAGE_OK) {
            YQL_CLOG(TRACE, Net)
                    << "Message " << ToCString(type)
                    << " send to " << PeerAddress
                    << " " << NBus::GetMessageStatus(status);
        } else {
            YQL_CLOG(ERROR, Net)
                    << "Can't send message " << ToCString(type)
                    << " " << NBus::GetMessageStatus(status)
                    << ": " << NBus::MessageStatusDescription(status);
        }

        return status;
    }
};

using TInspectorSessionList = TList<TInspectorSessionPtr>;

//////////////////////////////////////////////////////////////////////////////
// TMsgBusInspectorClient
//////////////////////////////////////////////////////////////////////////////
class TMsgBusInspectorClient final:
        public IInspectorClient,
        public NBus::IBusClientHandler
{
public:
    TMsgBusInspectorClient(const NProto::TInspectorConfig& config)
        : Config_(config)
        , Protocol_(config.GetPort())
    {
        MaxSendRetries_ = Config_.GetMaxSendRetries() > 0
                ? Config_.GetMaxSendRetries()
                : kDefaultMaxSendRetries;
        RetryIntervalMillis_ = Config_.GetRetryIntervalMillis() > 0
                ? Config_.GetRetryIntervalMillis()
                : kDefaultRetryIntervalMillis;
        if (RetryIntervalMillis_ > 60'000) {
            ythrow yexception() << "Too big RetryIntervalMillis";
        }

        TSet<TString> hosts;
        for (const TString& host: Config_.GetHosts()) {
            if (host.empty()) {
                continue;
            } else if (host[0] == '%') {
                TStringBuf group(host);
                TVector<TString> groupHosts;
                ConductorGroupToHosts(group.Skip(1), &groupHosts);
                hosts.insert(groupHosts.begin(), groupHosts.end());
            } else if (host.StartsWith("sd@"_sb)) {
                TVector<TString> sdHosts;
                auto hostSb = TStringBuf(host);
                TStringBuf numSb;
                ui32 numHosts = 0;
                Y_ENSURE(hostSb.TryRSplit('=', hostSb, numSb) && hostSb && numSb && TryFromString(numSb, numHosts), "Expected SD service string in the format sd@service=num_hosts");
                DeployServiceToHosts(hostSb.Skip(3), numHosts, &sdHosts);
                hosts.insert(sdHosts.begin(), sdHosts.end());
            } else {
                hosts.insert(host);
            }
        }
        Y_ENSURE(hosts.size() > 0, "Expected more than 0 inspector hosts");
        YQL_CLOG(DEBUG, Net) << "Using inspector hosts: " << JoinSeq(' ', hosts);

        NBus::TBusQueueConfig queueConfig;
        queueConfig.Name = "yqlworker.inspector.client";
        queueConfig.NumWorkers = hosts.size();
        MsgQueue_ = NBus::CreateMessageQueue(queueConfig);

        for (const TString& host: hosts) {
            TInspectorSessionPtr session(new TInspectorSession);
            Sessions_.emplace_back(session);
            session->Open(Config_, host, this, &Protocol_, MsgQueue_);
        }
    }

    ~TMsgBusInspectorClient() {
        // explicitly drop sessions before stopping threads in MsgQueue_
        Sessions_.clear();
        MsgQueue_->Stop();
    }

private:

    struct TDelayedSend: public NBus::NPrivate::IScheduleItem {
        TDelayedSend(TInstant instant, std::weak_ptr<IInspectorClient> client, THolder<NBus::TBusMessage>&& msg)
            : NBus::NPrivate::IScheduleItem(instant)
            , Client(client)
            , Msg(std::move(msg))
        {
        }

        void Do() override {
            ui32 retries = Max<ui32>();
            auto messageType = static_cast<EInspectorMessageType>(Msg->GetHeader()->Type);
            switch (messageType) {
            case MTYPE_UPDATE_STATUS_REQUEST: {
                auto request = static_cast<TBusStatusUpdateRequest*>(Msg.Get());
                retries = ++(request->Retries);
                if (auto client = Client.lock()) {
                    auto mbClient = static_cast<TMsgBusInspectorClient*>(client.get());
                    mbClient->SendWithRetries(retries, std::move(Msg), mbClient->GetRandomSession(), request->Record.GetTaskId());
                }
              break;
            }
            default:
                YQL_CLOG(WARN, Net) << "Unknown message type " << messageType;
            }
        }

        std::weak_ptr<IInspectorClient> Client;
        THolder<NBus::TBusMessage> Msg;
    };

    struct TDelayedAddressRenew: public NBus::NPrivate::IScheduleItem {
        TDelayedAddressRenew(TInstant instant, std::weak_ptr<IInspectorClient> client, NBus::TNetAddr addr)
            : NBus::NPrivate::IScheduleItem(instant)
            , Client(client)
            , Addr(std::move(addr))
        {
            YQL_CLOG(TRACE, Net) << "schedule session address renew: " << Addr;
        }

        void Do() override {
            if (auto client = Client.lock()) {
                static_cast<TMsgBusInspectorClient*>(client.get())->RenewSessionWithAddress(Addr);
            }
        }

        std::weak_ptr<IInspectorClient> Client;
        NBus::TNetAddr Addr;
    };

    void SendHeartbeatToAll(
            NProto::THeartbeatRequest&& request,
            THeartbeatCallback callback) override
    {
        TReadGuard readGuard(SessionsMutex_);

        for (auto it = Sessions_.begin(); it != Sessions_.end(); ++it) {
            THolder<TBusHeartbeatRequest> message(new TBusHeartbeatRequest);
            message->Record = request;
            message->Record.SetSentTime(MicroSeconds() / 1000);
            message->Callback = callback;

            {
                // this will prevent from deleting session object after
                // releasing read mutex
                TInspectorSessionPtr session(*it);

                // release read mutex while sending request
                auto readUnguard = Unguard(readGuard);

                auto status = session->Send(message.Get());
                if (status == NBus::MESSAGE_OK) {
                    Y_UNUSED(message.Release());
                }
            }
        }
    }

    void SendStatusUpdate(
            const NAddr::IRemoteAddr& preferredAddr,
            NProto::TStatusUpdateRequest&& request,
            TStatusUpdateCallback callback) override
    {
        TInspectorSessionPtr session;
        {
            TReadGuard readGuard(SessionsMutex_);;
            auto sessionIt = FindIf(Sessions_, [&preferredAddr](const TInspectorSessionPtr& session) {
                return NAddr::IsSame(session->PeerAddress, preferredAddr);
            });
            if (sessionIt != Sessions_.end()) {
                session = *sessionIt;
            }
        }
        if (!session) {
            session = GetRandomSession();
        }

        THolder<TBusStatusUpdateRequest> message(new TBusStatusUpdateRequest);
        message->Record.Swap(&request);
        message->Record.SetSentTime(MicroSeconds() / 1000);
        message->Callback = callback;

        auto taskId = message->Record.GetTaskId();
        YQL_CLOG(TRACE, Net)
            << "sending status update of " << taskId
            << ", to: " << session->PeerAddress
            << ", request: {" << FormatRequest(message->Record) << '}';

        SendWithRetries(message->Retries, std::move(message), session, taskId);
    }

    void NotifyTaskSubscription(
            const NAddr::IRemoteAddr& peerAddress,
            NProto::TTaskSubscriptionNotifyRequest&& request) override
    {
        TInspectorSessionPtr session;
        {
            TReadGuard readGuard(SessionsMutex_);;
            auto sessionIt = FindIf(Sessions_.begin(), Sessions_.end(),
                    [&peerAddress](const TInspectorSessionPtr& session) {
                        return NAddr::IsSame(session->PeerAddress, peerAddress);
                    });
            if (sessionIt == Sessions_.end()) {
                ythrow yexception() << "unknown peer address: "
                                    << NAddr::PrintHostAndPort(peerAddress);
            }
            session = *sessionIt;
        }

        auto busMsg = MakeHolder<TBusTaskSubscriptionNotifyRequest>();
        busMsg->Record.Swap(&request);
        busMsg->Record.SetSentTime(MicroSeconds() / 1000);

        // TODO (jamel): send with retries
        auto status = session->Send(busMsg.Get());
        if (status != NBus::MESSAGE_OK) {
            auto type = static_cast<EInspectorMessageType>(busMsg->GetHeader()->Type);
            ythrow yexception() << "cannot send " << ToCString(type)
                                << " to " << NAddr::PrintHostAndPort(peerAddress);
        }
        Y_UNUSED(busMsg.Release());
    }

    void CloseConnectionWith(const NAddr::IRemoteAddr& peerAddr) override {
        TInspectorSessionPtr session;
        {
            TWriteGuard writeGuard(SessionsMutex_);
            for (auto it = Sessions_.begin(); it != Sessions_.end(); ++it) {
                if (NAddr::IsSame((*it)->PeerAddress, peerAddr)) {
                    // do not destroy session under holded write mutex
                    session = *it;
                    Sessions_.erase(it);
                    break;
                }
            }
        }
    }

    void RenewSessionWithAddress(const NBus::TNetAddr& peerAddr) {
        TInspectorSessionPtr oldSession;
        {
            TReadGuard readGuard(SessionsMutex_);;
            auto sessionIt = FindIf(Sessions_.begin(), Sessions_.end(),
                    [&peerAddr](const TInspectorSessionPtr& session) {
                        return NAddr::IsSame(session->PeerAddress, peerAddr);
                    });
            if (sessionIt != Sessions_.end()) {
                oldSession = *sessionIt;
            } else {
                YQL_CLOG(WARN, Net) << "Cannot find session for " << peerAddr << " to renew";
            }
        }
        if (oldSession && oldSession->NeedReopen()) {
            try {
                TInspectorSessionPtr newSession(new TInspectorSession);
                newSession->Open(Config_, oldSession->Host, this, &Protocol_, MsgQueue_);

                TWriteGuard writeGuard(SessionsMutex_);
                EraseIf(Sessions_, [&peerAddr](const TInspectorSessionPtr& session) {
                    return NAddr::IsSame(session->PeerAddress, peerAddr);
                });
                Sessions_.emplace_back(newSession);
            } catch (...) {
                YQL_CLOG(ERROR, Net) << "Failed to renew session for " << peerAddr << ": " << CurrentExceptionMessage();
            }
        }
    }

    size_t GetConnectionsCount() override {
        TReadGuard readGuard(SessionsMutex_);
        return Sessions_.size();
    }

    void WaitAllMessagesDelivered() const override {
        TReadGuard readGuard(SessionsMutex_);
        for (const TInspectorSessionPtr& session: Sessions_) {
            session->WaitAllMessagesDelivered();
        }
    }

    void SendWithRetries(
        ui32 retries, THolder<NBus::TBusMessage>&& message, TInspectorSessionPtr session, const TString& taskId)
    {
        Y_ASSERT(session && "session is null");
        if (session->Send(message.Get()) == NBus::MESSAGE_OK) {
            Y_UNUSED(message.Release());
            return;
        }
        SendWithDelay(retries, std::move(message), taskId);
    }

    void SendWithDelay(ui32 retries, THolder<NBus::TBusMessage>&& message, const TString& taskId) {
        auto messageType = static_cast<EInspectorMessageType>(message->GetHeader()->Type);
        if (retries >= MaxSendRetries_) {
            YQL_CLOG(ERROR, Net) << "Failed to send " << messageType << " for " << taskId;
            return;
        }
        ui64 delay = Min(kMaxDelayMillis, (ui64(1) << retries) * RetryIntervalMillis_);
        delay = delay - RandomNumber(delay / 2);
        YQL_CLOG(DEBUG, Net) << "Retrying #" << retries << " " << messageType << " for " << taskId
            << " in " << delay << " millis";
        auto deadline = TDuration::MilliSeconds(delay).ToDeadLine();
        message->Reset();
        MsgQueue_->Schedule(new TDelayedSend(deadline, weak_from_this(), std::move(message)));
    }

    TInspectorSessionPtr GetRandomSession() {
        TInspectorSessionPtr session;
        {
            TReadGuard readGuard(SessionsMutex_);;
            Y_ASSERT(Sessions_ && "Sessions_ must be non-empty");
            auto sessionNum = RandomNumber<size_t>(Sessions_.size());
            auto sessionIt = Sessions_.begin();
            std::advance(sessionIt, sessionNum);
            session = *sessionIt;
        }
        return session;
    }

    void OnReply(
            TAutoPtr<NBus::TBusMessage> req,
            TAutoPtr<NBus::TBusMessage> resp) override
    {
        try {
            auto messageType = static_cast<EInspectorMessageType>(resp->GetHeader()->Type);
            switch (messageType) {
            case MTYPE_HEARTBEAT_RESPONSE: {
                auto busReq = static_cast<TBusHeartbeatRequest*>(req.Get());
                auto busResp = static_cast<TBusHeartbeatResponse*>(resp.Get());
                NBus::TNetAddr peerAddr = req->GetReplyTo();
                busReq->Callback(peerAddr, std::move(busResp->Record));
                break;
            }
            case MTYPE_UPDATE_STATUS_RESPONSE: {
                auto busReq = static_cast<TBusStatusUpdateRequest*>(req.Get());
                auto busResp = static_cast<TBusStatusUpdateResponse*>(resp.Get());
                if (busResp->Record.GetStatus() == NProto::TStatusUpdateResponse::OK) {
                    busReq->Callback(req->GetReplyTo(), std::move(busResp->Record));
                } else {
                    YQL_CLOG(WARN, Net) << "Status update response status "
                        << NProto::TStatusUpdateResponse_EStatus_Name(busResp->Record.GetStatus())
                        << " for task " << busReq->Record.GetTaskId();
                    SendWithDelay(busReq->Retries, std::move(req), busReq->Record.GetTaskId());
                }
                break;
            }
            case MTYPE_TASK_SUBSCR_NOTIFY_RESPONSE: {
                break;
            }
            default:
                YQL_CLOG(WARN, Net) << "Unknown message type: " << messageType;
            }
        } catch (...) {
            YQL_CLOG(ERROR, Net) << "unhandled exception: "
                                 << CurrentExceptionMessage();
        }
    }

    void OnError(TAutoPtr<NBus::TBusMessage> req, NBus::EMessageStatus status) override {
        auto type = static_cast<EInspectorMessageType>(req->GetHeader()->Type);
        YQL_CLOG(WARN, Net)
                << "message send error type: " << type
                << ", status: " << status;
        if (NBus::MESSAGE_CONNECT_FAILED == status) {
            MsgQueue_->Schedule(new TDelayedAddressRenew(Now(), weak_from_this(), req->GetReplyTo()));
        }
        switch (type) {
        case MTYPE_HEARTBEAT_REQUEST:
        case MTYPE_HEARTBEAT_RESPONSE:
        case MTYPE_UPDATE_STATUS_RESPONSE:
        case MTYPE_TASK_SUBSCR_NOTIFY_REQUEST:
        case MTYPE_TASK_SUBSCR_NOTIFY_RESPONSE:
            break;
        case MTYPE_UPDATE_STATUS_REQUEST: {
            auto busReq = static_cast<TBusStatusUpdateRequest*>(req.Get());
            SendWithDelay(busReq->Retries, std::move(req), busReq->Record.GetTaskId());
            break;
        }
        default:
            YQL_CLOG(WARN, Net) << "Unknown message type: " << type;
        }
    }

    void OnClientConnectionEvent(const NBus::TClientConnectionEvent& ev) override {
        switch (ev.GetType()) {
        case NBus::TClientConnectionEvent::CONNECTED:
            YQL_CLOG(DEBUG, Net) << "Established connection with " << ev.GetAddr();
            break;
        case NBus::TClientConnectionEvent::DISCONNECTED:
            YQL_CLOG(WARN, Net) << "Lost connection with " << ev.GetAddr();
            MsgQueue_->Schedule(new TDelayedAddressRenew(Now(), weak_from_this(), ev.GetAddr()));
        }
    }

private:
    const NProto::TInspectorConfig Config_;
    mutable TRWMutex SessionsMutex_;
    TInspectorSessionList Sessions_;
    TMsgBusInspectorProtocol Protocol_;
    NBus::TBusMessageQueuePtr MsgQueue_;
    ui32 MaxSendRetries_;
    ui32 RetryIntervalMillis_;
};

} // namspace

IInspectorClientPtr CreateMsgBusInspectorClient(
        const NProto::TInspectorConfig& config)
{
    return std::make_shared<TMsgBusInspectorClient>(config);
}

} // namspace NYql
