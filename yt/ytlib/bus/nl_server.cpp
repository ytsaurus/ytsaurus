#include "stdafx.h"
#include "nl_server.h"
#include "message.h"
#include "packet.h"
#include "message_rearranger.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/logging/log.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/ytree/fluent.h>

#include <util/generic/list.h>
#include <util/generic/deque.h>
#include <util/generic/utility.h>

#include <quality/netliba_v6/udp_http.h>

namespace NYT {
namespace NBus {

using namespace NYTree;
using namespace NNetliba;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

////////////////////////////////////////////////////////////////////////////////

class TNLBusServer
    : public IBusServer
{
public:
    typedef TIntrusivePtr<TNLBusServer> TPtr;

    TNLBusServer(TNLBusServerConfig* config);
    virtual ~TNLBusServer();

    virtual void Start(IMessageHandler* handler);
    virtual void Stop();

    virtual void GetMonitoringInfo(IYsonConsumer* consumer);
    virtual TBusStatistics GetStatistics();

private:
    class TSession;
    struct TOutcomingResponse;

    friend class TSession;

    typedef yhash_map<TSessionId, TIntrusivePtr<TSession> > TSessionMap;
    typedef yhash_map<TGuid, TIntrusivePtr<TSession> > TPingMap;

    TNLBusServerConfig::TPtr Config;
    IMessageHandler::TPtr Handler;
    bool Started;
    volatile bool Stopped;
    ::TIntrusivePtr<IRequester> Requester;
    TThread Thread;
    TSessionMap SessionMap;
    TPingMap PingMap;
    TLockFreeQueue< TIntrusivePtr<TSession> > SessionsWithPendingResponses;

    TSpinLock StatisticsLock;
    TBusStatistics Statistics;
    TInstant StatisticsTimestamp;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    Event& GetEvent();

    bool ProcessIncomingNLRequests();
    void ProcessIncomingNLRequest(TUdpHttpRequest* nlRequest);

    bool ProcessIncomingNLResponses();
    void ProcessIncomingNLResponse(TUdpHttpResponse* nlResponse);
    void ProcessFailedNLResponse(TUdpHttpResponse* nlResponse);

    void EnqueueOutcomingResponse(TSession* session, TOutcomingResponse* response);
    bool ProcessOutcomingResponses();
    void ProcessOutcomingResponse(TSession* session, TOutcomingResponse* response);

    void ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse);
    void ProcessPing(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse);

    TIntrusivePtr<TSession> DoProcessMessage(
        TPacketHeader* header,
        const TGuid& requestId,
        const TUdpAddress& address,
        TBlob&& data,
        bool isRequest);

    TIntrusivePtr<TSession> RegisterSession(
        const TSessionId& sessionId,
        const TUdpAddress& address);
    void UnregisterSession(TSession* session);
};

IBusServer::TPtr CreateNLBusServer(TNLBusServerConfig* config)
{
    return New<TNLBusServer>(config);
}

////////////////////////////////////////////////////////////////////////////////

struct TNLBusServer::TOutcomingResponse
    : public TRefCounted
{
    typedef TIntrusivePtr<TOutcomingResponse> TPtr;

    TOutcomingResponse(TBlob* data)
    {
        Data.swap(*data);
    }

    TBlob Data;
};

////////////////////////////////////////////////////////////////////////////////

class TNLBusServer::TSession
    : public IBus
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TNLBusServer* server,
        const TSessionId& sessionId,
        const TUdpAddress& address,
        const TGuid& pingId)
        : Server(server)
        , SessionId(sessionId)
        , Address(address)
        , PingId(pingId)
        , Terminated(false)
        , SequenceId(0)
        , MessageRearranger(New<TMessageRearranger>(
            SessionId,
            ~FromMethod(&TSession::OnMessageDequeued, TPtr(this)),
            server->Config->MessageRearrangeTimeout))
    { }

    void Finalize()
    {
        // Kill cyclic dependencies.
        MessageRearranger.Reset();
        Server.Reset();
    }

    void ProcessIncomingMessage(
        IMessage* message,
        const TGuid& requestId,
        TSequenceId sequenceId)
    {
        MessageRearranger->EnqueueMessage(message, requestId, sequenceId);
    }

    TSessionId GetSessionId() const
    {
        return SessionId;
    }

    TGuid GetPingId() const
    {
        return PingId;
    }

    TUdpAddress GetAddress() const
    {
        return Address;
    }

    // IBus implementation.
    virtual TSendResult::TPtr Send(IMessage::TPtr message)
    {
        // Load to a local since the other thread may be calling Finalize.
        auto server = Server;
        if (!server) {
            LOG_WARNING("Attempt to reply via a detached bus");
            return NULL;
        }

        TSequenceId sequenceId = GenerateSequenceId();

        TBlob data;
        EncodeMessagePacket(~message, SessionId, sequenceId, &data);
        int dataSize = data.ysize();

        auto response = New<TOutcomingResponse>(&data);
        server->EnqueueOutcomingResponse(this, ~response);

        LOG_DEBUG("Response enqueued (SessionId: %s, Response: %p, PacketSize: %d)",
            ~SessionId.ToString(),
            ~response,
            dataSize);

        return NULL;
    }

    void EnqueueResponse(TOutcomingResponse* response)
    {
        PendingResponses.Enqueue(response);
    }

    TOutcomingResponse::TPtr DequeueResponse()
    {
        TOutcomingResponse::TPtr response;
        PendingResponses.Dequeue(&response);
        return response;
    }

    virtual void Terminate()
    {
        // Terminate has no effect for a reply bus.
    }

private:
    typedef yvector<TGuid> TRequestIds;
    typedef std::deque<IMessage::TPtr> TResponseMessages;

    TNLBusServer::TPtr Server;
    TSessionId SessionId;
    TUdpAddress Address;
    TGuid PingId;
    bool Terminated;
    TAtomic SequenceId;
    TMessageRearranger::TPtr MessageRearranger;
    TLockFreeQueue<TOutcomingResponse::TPtr> PendingResponses;

    TSequenceId GenerateSequenceId()
    {
        return AtomicIncrement(SequenceId);
    }

    void OnMessageDequeued(IMessage* message)
    {
        Server->Handler->OnMessage(message, this);
    }
};

////////////////////////////////////////////////////////////////////////////////

TNLBusServer::TNLBusServer(TNLBusServerConfig* config)
    : Config(config)
    , Started(false)
    , Stopped(false)
    , Thread(ThreadFunc, (void*) this)
{
    YASSERT(config->Port >= 0);
}

TNLBusServer::~TNLBusServer()
{
    Stop();
}

void TNLBusServer::Start(IMessageHandler* handler)
{
    YASSERT(handler);
    YASSERT(!Started);

    Requester = CreateHttpUdpRequester(Config->Port);
    if (!Requester) {
        ythrow yexception() << Sprintf("Failed to create a bus server on port %d",
            Config->Port);
    }

    Handler = handler;
    Started = true;
    Thread.Start();

    LOG_INFO("Started a server bus listener on port %d", Config->Port);
}

void TNLBusServer::Stop()
{
    if (!Started || Stopped)
        return;

    Stopped = true;
    Thread.Join();

    Requester->StopNoWait();
    Requester = NULL;

    FOREACH(auto& pair, SessionMap) {
        pair.second->Finalize();
    }
    SessionMap.clear();

    PingMap.clear();

    Handler.Reset();

    LOG_INFO("Bus listener stopped");
}

void* TNLBusServer::ThreadFunc(void* param)
{
    auto* server = reinterpret_cast<TNLBusServer*>(param);
    server->ThreadMain();
    return NULL;
}

Event& TNLBusServer::GetEvent()
{
    return Requester->GetAsyncEvent();
}

void TNLBusServer::ThreadMain()
{
    while (!Stopped) {
        // NB: "&", not "&&" since we want every type of processing to happen on each iteration.
        if (!ProcessIncomingNLRequests() &
            !ProcessIncomingNLResponses() &
            !ProcessOutcomingResponses())
        {
            LOG_TRACE("Server is idle");
            GetEvent().WaitT(Config->SleepQuantum);
        }
    }
}

bool TNLBusServer::ProcessIncomingNLRequests()
{
    LOG_TRACE("Processing incoming server NetLiba requests");

    int callCount = 0;
    while (callCount < Config->MaxNLCallsPerIteration) {
        TAutoPtr<TUdpHttpRequest> nlRequest = Requester->GetRequest();
        if (!nlRequest)
            break;

        ++callCount;
        ProcessIncomingNLRequest(~nlRequest);
    }
    return callCount > 0;
}

void TNLBusServer::ProcessIncomingNLRequest(TUdpHttpRequest* nlRequest)
{
    auto* header = ParsePacketHeader<TPacketHeader>(nlRequest->Data);
    if (!header)
        return;

    switch (header->Type) {
        case TPacketHeader::EType::Message:
            ProcessMessage(header, nlRequest);
            break;

        default:
            LOG_ERROR("Invalid request packet type (RequestId: %s, Type: %s)",
                ~((TGuid) nlRequest->ReqId).ToString(),
                ~header->Type.ToString());
            return;
    }
}

bool TNLBusServer::ProcessIncomingNLResponses()
{
    LOG_TRACE("Processing incoming server NetLiba responses");

    int callCount = 0;
    while (callCount < Config->MaxNLCallsPerIteration) {
        TAutoPtr<TUdpHttpResponse> nlResponse = Requester->GetResponse();
        if (!nlResponse)
            break;

        ++callCount;
        ProcessIncomingNLResponse(~nlResponse);
    }
    return callCount > 0;
}

void TNLBusServer::ProcessIncomingNLResponse(TUdpHttpResponse* nlResponse)
{
    if (nlResponse->Ok != TUdpHttpResponse::OK) {
        ProcessFailedNLResponse(nlResponse);
        return;
    }

    auto* header = ParsePacketHeader<TPacketHeader>(nlResponse->Data);
    if (!header)
        return;

    switch (header->Type) {
        case TPacketHeader::EType::Ack:
            ProcessAck(header, nlResponse);
            break;

        case TPacketHeader::EType::Message:
            ProcessMessage(header, nlResponse);
            break;

        default:
            LOG_ERROR("Invalid response packet type (RequestId: %s, Type: %s)",
                ~((TGuid) nlResponse->ReqId).ToString(),
                ~header->Type.ToString());
    }
}

void TNLBusServer::ProcessFailedNLResponse(TUdpHttpResponse* nlResponse)
{
    auto pingIt = PingMap.find(nlResponse->ReqId);
    if (pingIt == PingMap.end()) {
        LOG_DEBUG("Request failed (RequestId: %s)",
            ~((TGuid) nlResponse->ReqId).ToString());
    } else {
        LOG_DEBUG("Ping failed (RequestId: %s)",
            ~((TGuid) nlResponse->ReqId).ToString());

        auto session = pingIt->second;
        UnregisterSession(~session);
    }
}

bool TNLBusServer::ProcessOutcomingResponses()
{
    LOG_TRACE("Processing outcoming server responses");

    int callCount = 0;
    while (callCount < Config->MaxNLCallsPerIteration) {
        TSession::TPtr session;
        if (!SessionsWithPendingResponses.Dequeue(&session))
            break;

        auto response = session->DequeueResponse();
        if (response) {
            ++callCount;
            ProcessOutcomingResponse(~session, ~response);
        }
    }
    return callCount > 0;
}

void TNLBusServer::ProcessOutcomingResponse(TSession* session, TOutcomingResponse* response)
{
    TGuid requestId = Requester->SendRequest(
        session->GetAddress(),
        "",
        &response->Data);
    LOG_DEBUG("Message sent (IsRequest: 1, SessionId: %s, RequestId: %s, Response: %p)",
        ~session->GetSessionId().ToString(),
        ~requestId.ToString(),
        response);
}

void TNLBusServer::ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse)
{
    TGuid requestId = nlResponse->ReqId;
    auto pingIt = PingMap.find(requestId);
    if (pingIt == PingMap.end()) {
        LOG_DEBUG("Ack received (SessionId: %s, RequestId: %s)",
            ~header->SessionId.ToString(),
            ~requestId.ToString());
    } else {
        LOG_DEBUG("Ping ack received (RequestId: %s)",
            ~requestId.ToString());

        auto session = pingIt->second;
        UnregisterSession(~session);
    }
}

void TNLBusServer::ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest)
{
    TGuid requestId = nlRequest->ReqId;
    auto sessionId = header->SessionId;

    auto session = DoProcessMessage(
        header,
        requestId,
        nlRequest->PeerAddress,
        MoveRV(nlRequest->Data),
        true);

    if (session) {
        TBlob ackData;
        CreatePacket(sessionId, TPacketHeader::EType::Ack, &ackData);
        Requester->SendResponse(requestId, &ackData);

        LOG_DEBUG("Ack sent (SessionId: %s, RequestId: %s)",
            ~sessionId.ToString(),
            ~requestId.ToString());
    }

    // TODO(babenko): this is "request-via-reply", which is currently switched off
    //auto response = session->DequeueResponse();
    //if (response) {
    //    Requester->SendResponse(nlRequest->ReqId, &response->Data);

    //    LOG_DEBUG("Message sent (IsRequest: 0, SessionId: %s, RequestId: %s, Response: %p)",
    //        ~session->GetSessionId().ToString(),
    //        ~requestId.ToString(),
    //        ~response);
    //} else {
    //}
}

void TNLBusServer::ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse)
{
    DoProcessMessage(
        header,
        nlResponse->ReqId,
        nlResponse->PeerAddress,
        MoveRV(nlResponse->Data),
        false);
}

TNLBusServer::TSession::TPtr TNLBusServer::DoProcessMessage(
    TPacketHeader* header,
    const TGuid& requestId,
    const TUdpAddress& address,
    TBlob&& data,
    bool isRequest)
{
    // Save the size, data will be swapped out soon.
    int dataSize = data.ysize();

    IMessage::TPtr message;
    TSequenceId sequenceId;;
    if (!DecodeMessagePacket(MoveRV(data), &message, &sequenceId)) {
        LOG_WARNING("Error parsing message packet (RequestId: %s)", ~requestId.ToString());
        return NULL;
    }

    TSession::TPtr session;
    auto sessionIt = SessionMap.find(header->SessionId);
    if (sessionIt == SessionMap.end()) {
        if (isRequest) {
            // Check if a new session is initiated.
            if (sequenceId == 0) {
                session = RegisterSession(header->SessionId, address);
            } else {
                LOG_DEBUG("Request message for broken session received (SessionId: %s, RequestId: %s, SequenceId: %" PRId64 ", PacketSize: %d)",
                    ~header->SessionId.ToString(),
                    ~requestId.ToString(),
                    sequenceId,
                    dataSize);

                TBlob errorData;
                CreatePacket(header->SessionId, TPacketHeader::EType::BrokenSession, &errorData);
                Requester->SendResponse(requestId, &errorData);
                return NULL;
            }
        } else {
            LOG_DEBUG("Response message for unknown session received (SessionId: %s, RequestId: %s, SequenceId: %" PRId64 ", PacketSize: %d)",
                ~header->SessionId.ToString(),
                ~requestId.ToString(),
                sequenceId,
                dataSize);
            return NULL;
        }
    } else {
        session = sessionIt->second;
    }

    LOG_DEBUG("Message received (IsRequest: %d, SessionId: %s, RequestId: %s, SequenceId: %" PRId64", PacketSize: %d)",
        (int) isRequest,
        ~header->SessionId.ToString(),
        ~requestId.ToString(),
        sequenceId,
        dataSize);

    session->ProcessIncomingMessage(~message, requestId, sequenceId);

    return session;
}

void TNLBusServer::EnqueueOutcomingResponse(TSession* session, TOutcomingResponse* response)
{
    session->EnqueueResponse(response);
    SessionsWithPendingResponses.Enqueue(session);
    GetEvent().Signal();
}

TIntrusivePtr<TNLBusServer::TSession> TNLBusServer::RegisterSession(
    const TSessionId& sessionId,
    const TUdpAddress& address)
{
    TBlob data;
    CreatePacket(sessionId, TPacketHeader::EType::Ping, &data);
    TGuid pingId = Requester->SendRequest(address, "", &data);

    auto session = New<TSession>(
        this,
        sessionId,
        address,
        pingId);

    PingMap.insert(MakePair(pingId, session));
    SessionMap.insert(MakePair(sessionId, session));

    LOG_DEBUG("Session registered (SessionId: %s, Address: %s, PingId: %s)",
        ~sessionId.ToString(),
        ~GetAddressAsString(address),
        ~pingId.ToString());

    return session;
}

void TNLBusServer::UnregisterSession(TSession* session)
{
    LOG_DEBUG("Session unregistered (SessionId: %s)",
        ~session->GetSessionId().ToString());

    session->Finalize();

    // Copy the ids since the session may die any moment now.
    auto sessionId = session->GetSessionId();
    auto pingId = session->GetPingId();

    YVERIFY(SessionMap.erase(sessionId) == 1);
    YVERIFY(PingMap.erase(pingId) == 1);
}

void TNLBusServer::GetMonitoringInfo(IYsonConsumer* consumer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("port").Scalar(Config->Port)
            .Do([=] (TFluentMap fluent)
                {
                    auto statistics = GetStatistics();
                    fluent.Item("request_count").Scalar(statistics.RequestCount);
                    fluent.Item("request_data_size").Scalar(statistics.RequestDataSize);
                    fluent.Item("response_count").Scalar(statistics.ResponseCount);
                    fluent.Item("response_data_size").Scalar(statistics.ResponseDataSize);
                })
         .EndMap();
}

TBusStatistics TNLBusServer::GetStatistics()
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        TGuard<TSpinLock> guard(StatisticsLock);
        // TODO: refactor or get rid of this code
        if (TInstant::Now() < StatisticsTimestamp + TDuration::MilliSeconds(100)) {
            return Statistics;
        }
    }

    TBusStatistics statistics;

    auto requester = Requester;
    if (requester.Get()) {
        TRequesterPendingDataStats nlStatistics;
        requester->GetPendingDataSize(&nlStatistics);
        statistics.RequestCount = static_cast<i64>(nlStatistics.InpCount);
        statistics.RequestDataSize = static_cast<i64>(nlStatistics.InpDataSize);
        statistics.ResponseCount = static_cast<i64>(nlStatistics.OutCount);
        statistics.ResponseDataSize = static_cast<i64>(nlStatistics.OutDataSize);
    }

    {
        TGuard<TSpinLock> guard(StatisticsLock);
        Statistics = statistics;
        StatisticsTimestamp = TInstant::Now();
    }

    return statistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
