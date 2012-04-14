#include "stdafx.h"
#include "nl_server.h"
#include "message.h"
#include "packet.h"
#include "message_rearranger.h"

#include <ytlib/actions/bind.h>
#include <ytlib/logging/log.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/lease_manager.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/misc/thread.h>
#include <ytlib/profiling/profiler.h>

#include <util/thread/lfqueue.h>

#include <quality/netliba_v6/udp_http.h>

namespace NYT {
namespace NBus {

using namespace NYTree;
using namespace NNetliba;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bus");
static NProfiling::TProfiler Profiler("/bus/server");

////////////////////////////////////////////////////////////////////////////////

class TNLBusServer
    : public IBusServer
{
public:
    typedef TIntrusivePtr<TNLBusServer> TPtr;

    explicit TNLBusServer(TNLBusServerConfig* config);
    virtual ~TNLBusServer();

    virtual void Start(IMessageHandler* handler);
    virtual void Stop();

    virtual void GetMonitoringInfo(IYsonConsumer* consumer);
    virtual TBusStatistics GetStatistics();

private:
    class TSession;
    typedef TIntrusivePtr<TSession> TSessionPtr;

    struct TOutcomingResponse;

    friend class TSession;

    typedef yhash_map<TSessionId, TSessionPtr> TSessionMap;
    typedef yhash_map<TGuid, TSessionPtr> TPingMap;

    TNLBusServerConfig::TPtr Config;
    bool Started;
    volatile bool Stopped;
    TThread Thread;
    NProfiling::TRateCounter InCounter;
    NProfiling::TRateCounter InSizeCounter;
    NProfiling::TRateCounter OutCounter;
    NProfiling::TRateCounter OutSizeCounter;
    TAtomic SessionCount;

    IMessageHandler::TPtr Handler;
    ::TIntrusivePtr<IRequester> Requester;
    TSessionMap SessionMap;
    TPingMap PingMap;
    TLockFreeQueue<TSessionPtr> SessionsWithPendingResponses;
    TLockFreeQueue<TSessionId> ExpiredSessionIds;

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

    void EnqueueOutcomingResponse(TSessionPtr session, TOutcomingResponse* response);
    bool ProcessOutcomingResponses();
    void ProcessOutcomingResponse(TSessionPtr session, TOutcomingResponse* response);

    bool ProcessExpiredSessions();
    void ProcessExpiredSession(TSessionPtr session);

    void ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse);
    void ProcessPing(TPacketHeader* header, TUdpHttpRequest* nlRequest);
    void ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse);

    TSessionPtr DoProcessMessage(
        TPacketHeader* header,
        const TGuid& requestId,
        const TUdpAddress& address,
        TBlob&& data,
        bool isRequest);

    TSessionPtr RegisterSession(
        const TSessionId& sessionId,
        const TUdpAddress& address);
    void UnregisterSession(TSessionPtr session);
    void OnSessionExpired(TSessionPtr session);

    void ProfileIn(int size);
    void ProfileOut(int size);
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
    typedef TSessionPtr TPtr;

    TSession(
        TNLBusServer* server,
        const TSessionId& sessionId,
        const TUdpAddress& address)
        : Server(server)
        , SessionId(sessionId)
        , Address(address)
        , SequenceId(0)
        , MessageRearranger(New<TMessageRearranger>(
            SessionId,
            BIND(&TSession::OnMessageDequeued, MakeWeak(this)),
            server->Config->MessageRearrangeTimeout))
    { }

    void OnRegistered()
    {
        auto server = Server.Lock();
        YASSERT(server);
        SendPing();
        Lease = TLeaseManager::CreateLease(
            server->Config->SessionTimeout,
            BIND(&TSession::OnLeaseExpired, MakeWeak(this)));
    }

    void OnUnregistered()
    {
        CancelPing();
        if (Lease) {
            TLeaseManager::CloseLease(Lease);
            Lease.Reset();
        }
        Server.Reset();
    }

    void EnqueueIncomingMessage(
        IMessage* message,
        const TGuid& requestId,
        TSequenceId sequenceId)
    {
        RenewLease();
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
        auto server = Server.Lock();
        if (!server) {
            LOG_WARNING("Attempt to reply via a detached bus");
            return NULL;
        }

        RenewLease();

        auto sequenceId = GenerateSequenceId();

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

    TWeakPtr<TNLBusServer> Server;
    TSessionId SessionId;
    TUdpAddress Address;
    TGuid PingId;
    TAtomic SequenceId;
    TMessageRearranger::TPtr MessageRearranger;
    TLeaseManager::TLease Lease;

    TLockFreeQueue<TOutcomingResponse::TPtr> PendingResponses;

    void SendPing()
    {
        auto server = Server.Lock();

        YASSERT(server);
        YASSERT(PingId == TGuid());

        TBlob data;
        CreatePacket(SessionId, TPacketHeader::EType::Ping, &data);
        PingId = server->Requester->SendRequest(Address, "", &data);
    }

    void CancelPing()
    {
        auto server = Server.Lock();

        YASSERT(server);
        YASSERT(PingId != TGuid());

        server->Requester->CancelRequest(PingId);
        PingId = TGuid();
    }

    TSequenceId GenerateSequenceId()
    {
        return AtomicIncrement(SequenceId);
    }

    void OnMessageDequeued(IMessage* message)
    {
        auto server = Server.Lock();

        if (server) {
            server->Handler->OnMessage(message, this);
        }
    }

    void RenewLease()
    {
        TLeaseManager::RenewLease(Lease);
    }

    void OnLeaseExpired()
    {
        LOG_DEBUG("Session expired (SessionId: %s)", ~SessionId.ToString());

        auto server = Server.Lock();
        if (server) {
            server->OnSessionExpired(this);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TNLBusServer::TNLBusServer(TNLBusServerConfig* config)
    : Config(config)
    , Started(false)
    , Stopped(false)
    , Thread(ThreadFunc, (void*) this)
    , InCounter("/in_rate")
    , InSizeCounter("/in_throughput")
    , OutCounter("/out_rate")
    , OutSizeCounter("/out_throughput")
    , SessionCount(0)
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
    NThread::SetCurrentThreadName("BusServer");
    while (!Stopped) {
        // NB: "&", not "&&" since we want every type of processing to happen on each iteration.
        if (!ProcessIncomingNLRequests() &
            !ProcessIncomingNLResponses() &
            !ProcessOutcomingResponses() &
            !ProcessExpiredSessions())
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
        UnregisterSession(MoveRV(session));
    }
}

bool TNLBusServer::ProcessOutcomingResponses()
{
    LOG_TRACE("Processing outcoming server responses");

    int callCount = 0;
    while (callCount < Config->MaxNLCallsPerIteration) {
        TSessionPtr session;
        if (!SessionsWithPendingResponses.Dequeue(&session)) {
            break;
        }
        auto response = session->DequeueResponse();
        if (response) {
            ++callCount;
            ProcessOutcomingResponse(MoveRV(session), ~response);
        }
    }
    return callCount > 0;
}

void TNLBusServer::ProcessOutcomingResponse(TSessionPtr session, TOutcomingResponse* response)
{
    ProfileOut(response->Data.size());
    TGuid requestId = Requester->SendRequest(
        session->GetAddress(),
        "",
        &response->Data);
    LOG_DEBUG("Message sent (IsRequest: 1, SessionId: %s, RequestId: %s, Response: %p)",
        ~session->GetSessionId().ToString(),
        ~requestId.ToString(),
        response);
}

bool TNLBusServer::ProcessExpiredSessions()
{
    LOG_TRACE("Processing expired sessions");

    bool foundExpiredSessions = false;
    TSessionId sessionId;
    while (ExpiredSessionIds.Dequeue(&sessionId)) {
        auto it = SessionMap.find(sessionId);
        if (it != SessionMap.end()) {
            auto session = it->second;
            ProcessExpiredSession(MoveRV(session));
        }
        foundExpiredSessions = true;
    }
    return foundExpiredSessions;
}

void TNLBusServer::ProcessExpiredSession(TSessionPtr session)
{
    UnregisterSession(MoveRV(session));
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
        UnregisterSession(MoveRV(session));
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

TNLBusServer::TSessionPtr TNLBusServer::DoProcessMessage(
    TPacketHeader* header,
    const TGuid& requestId,
    const TUdpAddress& address,
    TBlob&& data,
    bool isRequest)
{
    // Save the size, data will be swapped out soon.
    int dataSize = static_cast<int>(data.size());

    ProfileIn(dataSize);

    IMessage::TPtr message;
    TSequenceId sequenceId;;
    if (!DecodeMessagePacket(MoveRV(data), &message, &sequenceId)) {
        LOG_WARNING("Error parsing message packet (RequestId: %s)", ~requestId.ToString());
        return NULL;
    }

    TSessionPtr session;
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

    session->EnqueueIncomingMessage(~message, requestId, sequenceId);

    return session;
}

void TNLBusServer::EnqueueOutcomingResponse(TSessionPtr session, TOutcomingResponse* response)
{
    session->EnqueueResponse(response);
    SessionsWithPendingResponses.Enqueue(MoveRV(session));
    GetEvent().Signal();
}

TIntrusivePtr<TNLBusServer::TSession> TNLBusServer::RegisterSession(
    const TSessionId& sessionId,
    const TUdpAddress& address)
{
    auto session = New<TSession>(
        this,
        sessionId,
        address);
    session->OnRegistered();

    AtomicIncrement(SessionCount);

    auto pingId = session->GetPingId();

    SessionMap.insert(MakePair(sessionId, session));
    PingMap.insert(MakePair(pingId, session));

    LOG_DEBUG("Session registered (SessionId: %s, Address: %s, PingId: %s)",
        ~sessionId.ToString(),
        ~GetAddressAsString(address),
        ~pingId.ToString());

    return session;
}

void TNLBusServer::UnregisterSession(TSessionPtr session)
{
    LOG_DEBUG("Session unregistered (SessionId: %s)", ~session->GetSessionId().ToString());

    YVERIFY(SessionMap.erase(session->GetSessionId()) == 1);
    YVERIFY(PingMap.erase(session->GetPingId()) == 1);

    session->OnUnregistered();

    AtomicDecrement(SessionCount);
}

void TNLBusServer::OnSessionExpired(TSessionPtr session)
{
    ExpiredSessionIds.Enqueue(session->GetSessionId());
    GetEvent().Signal();
}

void TNLBusServer::GetMonitoringInfo(IYsonConsumer* consumer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("port").Scalar(Config->Port)
            .Do([=] (TFluentMap fluent) {
                auto statistics = GetStatistics();
                fluent.Item("request_count").Scalar(statistics.RequestCount);
                fluent.Item("request_data_size").Scalar(statistics.RequestDataSize);
                fluent.Item("response_count").Scalar(statistics.ResponseCount);
                fluent.Item("response_data_size").Scalar(statistics.ResponseDataSize);
            })
            //.DoIf(Requester.Get(), [=] (TFluentMap fluent) {
            //    fluent.Item("debug_info").Scalar(Requester->GetDebugInfo());
            //})
            .Item("session_count").Scalar(static_cast<i64>(SessionCount))
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

void TNLBusServer::ProfileIn(int size)
{
    Profiler.Increment(InCounter);
    Profiler.Increment(InSizeCounter, size);
}

void TNLBusServer::ProfileOut(int size)
{
    Profiler.Increment(OutCounter);
    Profiler.Increment(OutSizeCounter, size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
