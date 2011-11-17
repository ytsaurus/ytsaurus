#include "stdafx.h"
#include "bus_server.h"
#include "message.h"
#include "message_rearranger.h"

#include "../actions/action_util.h"
#include "../logging/log.h"
#include "../misc/assert.h"

#include <util/generic/singleton.h>
#include <util/generic/list.h>
#include <util/generic/deque.h>
#include <util/generic/utility.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

// TODO: make configurable
static const int MaxNLCallsPerIteration = 10;
static const TDuration ServerSleepQuantum = TDuration::MilliSeconds(10);
static const TDuration MessageRearrangeTimeout = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

struct TBusServer::TOutcomingResponse
    : public TRefCountedBase
{
    typedef TIntrusivePtr<TOutcomingResponse> TPtr;

    TOutcomingResponse(TBlob* data)
    {
        Data.swap(*data);
    }

    TBlob Data;
};

////////////////////////////////////////////////////////////////////////////////

class TBusServer::TSession
    : public IBus
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TBusServer::TPtr server,
        const TSessionId& sessionId,
        const TUdpAddress& clientAddress,
        const TGuid& pingId)
        : Server(server)
        , SessionId(sessionId)
        , ClientAddress(clientAddress)
        , PingId(pingId)
        , Terminated(false)
        , SequenceId(0)
        , MessageRearranger(New<TMessageRearranger>(
            FromMethod(&TSession::OnMessageDequeued, TPtr(this)),
            MessageRearrangeTimeout))
    { }

    void Finalize()
    {
        // Drop the rearranger to kill the cyclic dependency.
        MessageRearranger.Reset();

        // Also forget about the server.
        Server.Reset();
    }

    void ProcessIncomingMessage(IMessage::TPtr message, TSequenceId sequenceId)
    {
        MessageRearranger->EnqueueMessage(message, sequenceId);
    }

    TSessionId GetSessionId() const
    {
        return SessionId;
    }

    TGuid GetPingId() const
    {
        return PingId;
    }

    TUdpAddress GetClientAddress() const
    {
        return ClientAddress;
    }

    // IBus implementation.
    virtual TSendResult::TPtr Send(IMessage::TPtr message)
    {
        // Load to a local since the other thread may be calling Finalize.
        auto server = Server;
        if (~server == NULL) {
            LOG_WARNING("Attempt to reply via a detached bus");
            return NULL;
        }

        TSequenceId sequenceId = GenerateSequenceId();

        TBlob data;
        EncodeMessagePacket(~message, SessionId, sequenceId, &data);
        int dataSize = data.ysize();

        auto response = New<TOutcomingResponse>(&data);
        server->EnqueueOutcomingResponse(this, response);

        LOG_DEBUG("Response enqueued (SessionId: %s, Response: %p, PacketSize: %d)",
            ~SessionId.ToString(),
            ~response,
            dataSize);

        return NULL;
    }

    void EnqueueResponse(TOutcomingResponse::TPtr response)
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

    TBusServer::TPtr Server;
    TSessionId SessionId;
    TUdpAddress ClientAddress;
    TGuid PingId;
    bool Terminated;
    TAtomic SequenceId;
    TMessageRearranger::TPtr MessageRearranger;
    TLockFreeQueue<TOutcomingResponse::TPtr> PendingResponses;

    TSequenceId GenerateSequenceId()
    {
        return AtomicIncrement(SequenceId);
    }

    void OnMessageDequeued(IMessage::TPtr message)
    {
        Server->Handler->OnMessage(message, this);
    }
};

////////////////////////////////////////////////////////////////////////////////

TBusServer::TBusServer(int port, IMessageHandler::TPtr handler)
    : Handler(handler)
    , Terminated(false)
    , Thread(ThreadFunc, (void*) this)
{
    YASSERT(~Handler != NULL);

    Requester = CreateHttpUdpRequester(port);
    if (~Requester == NULL) {
        ythrow yexception() << Sprintf("Failed to create a bus server on port %d",
            port);
    }

    Thread.Start();

    LOG_INFO("Started a server bus listener on port %d", port);
}

TBusServer::~TBusServer()
{
    Terminate();
}

void TBusServer::Terminate()
{
    if (Terminated)
        return;

    Terminated = true;
    Thread.Join();

    Requester->StopNoWait();

    FOREACH(auto& pair, SessionMap) {
        pair.second->Finalize();
    }
    SessionMap.clear();

    PingMap.clear();

    Handler.Reset();
}

void* TBusServer::ThreadFunc(void* param)
{
    auto* server = reinterpret_cast<TBusServer*>(param);
    server->ThreadMain();
    return NULL;
}

Event& TBusServer::GetEvent()
{
    return Requester->GetAsyncEvent();
}

void TBusServer::ThreadMain()
{
    while (!Terminated) {
        // NB: "&", not "&&" since we want every type of processing to happen on each iteration.
        if (!ProcessIncomingNLRequests() &
            !ProcessIncomingNLResponses() &
            !ProcessOutcomingResponses())
        {
            LOG_TRACE("Server is idle");
            GetEvent().WaitT(ServerSleepQuantum);
        }
    }
}

bool TBusServer::ProcessIncomingNLRequests()
{
    LOG_TRACE("Processing incoming server NetLiba requests");

    int callCount = 0;
    while (callCount < MaxNLCallsPerIteration) {
        TAutoPtr<TUdpHttpRequest> nlRequest = Requester->GetRequest();
        if (~nlRequest == NULL)
            break;

        ++callCount;
        ProcessIncomingNLRequest(~nlRequest);
    }
    return callCount > 0;
}

void TBusServer::ProcessIncomingNLRequest(TUdpHttpRequest* nlRequest)
{
    auto* header = ParsePacketHeader<TPacketHeader>(nlRequest->Data);
    if (header == NULL)
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

bool TBusServer::ProcessIncomingNLResponses()
{
    LOG_TRACE("Processing incoming server NetLiba responses");

    int callCount = 0;
    while (callCount < MaxNLCallsPerIteration) {
        TAutoPtr<TUdpHttpResponse> nlResponse = Requester->GetResponse();
        if (~nlResponse == NULL)
            break;

        ++callCount;
        ProcessIncomingNLResponse(~nlResponse);
    }
    return callCount > 0;
}

void TBusServer::ProcessIncomingNLResponse(TUdpHttpResponse* nlResponse)
{
    if (nlResponse->Ok != TUdpHttpResponse::OK) {
        ProcessFailedNLResponse(nlResponse);
        return;
    }

    auto* header = ParsePacketHeader<TPacketHeader>(nlResponse->Data);
    if (header == NULL)
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

void TBusServer::ProcessFailedNLResponse(TUdpHttpResponse* nlResponse)
{
    auto pingIt = PingMap.find(nlResponse->ReqId);
    if (pingIt == PingMap.end()) {
        LOG_DEBUG("Request failed (RequestId: %s)",
            ~((TGuid) nlResponse->ReqId).ToString());
    } else {
        LOG_DEBUG("Ping failed (RequestId: %s)",
            ~((TGuid) nlResponse->ReqId).ToString());

        TSession::TPtr session = pingIt->Second();
        UnregisterSession(session);
    }
}

bool TBusServer::ProcessOutcomingResponses()
{
    LOG_TRACE("Processing outcoming server responses");

    int callCount = 0;
    while (callCount < MaxNLCallsPerIteration) {
        TSession::TPtr session;
        if (!SessionsWithPendingResponses.Dequeue(&session))
            break;

        auto response = session->DequeueResponse();
        if (~response != NULL) {
            ++callCount;
            ProcessOutcomingResponse(session, response);
        }
    }
    return callCount > 0;
}

void TBusServer::ProcessOutcomingResponse(TSession::TPtr session, TOutcomingResponse::TPtr response)
{
    TGuid requestId = Requester->SendRequest(
        session->GetClientAddress(),
        "",
        &response->Data);
    LOG_DEBUG("Message sent (IsRequest: 1, SessionId: %s, RequestId: %s, Response: %p)",
        ~session->GetSessionId().ToString(),
        ~requestId.ToString(),
        ~response);
}

void TBusServer::ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse)
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

        TSession::TPtr session = pingIt->Second();
        UnregisterSession(session);
    }
}

void TBusServer::ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest)
{
    TGuid requestId = nlRequest->ReqId;
    auto session = DoProcessMessage(
        header,
        requestId,
        nlRequest->PeerAddress,
        MoveRV(nlRequest->Data),
        true);

    //auto response = session->DequeueResponse();
    //if (~response != NULL) {
    //    Requester->SendResponse(nlRequest->ReqId, &response->Data);

    //    LOG_DEBUG("Message sent (IsRequest: 0, SessionId: %s, RequestId: %s, Response: %p)",
    //        ~session->GetSessionId().ToString(),
    //        ~requestId.ToString(),
    //        ~response);
    //} else {
        TBlob ackData;
        CreatePacket(session->GetSessionId(), TPacketHeader::EType::Ack, &ackData);

        Requester->SendResponse(nlRequest->ReqId, &ackData);

        LOG_DEBUG("Ack sent (SessionId: %s, RequestId: %s)",
            ~session->GetSessionId().ToString(),
            ~requestId.ToString());
    //}
}

void TBusServer::ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse)
{
    DoProcessMessage(
        header,
        nlResponse->ReqId,
        nlResponse->PeerAddress,
        MoveRV(nlResponse->Data),
        false);
}

TBusServer::TSession::TPtr TBusServer::DoProcessMessage(
    TPacketHeader* header,
    const TGuid& requestId,
    const TUdpAddress& address,
    TBlob&& data,
    bool isRequest)
{
    int dataSize = data.ysize();

    TSession::TPtr session;
    auto sessionIt = SessionMap.find(header->SessionId);
    if (sessionIt == SessionMap.end()) {
        if (isRequest) {
            session = RegisterSession(header->SessionId, address);
        } else {
            LOG_DEBUG("Message for an obsolete session is dropped (SessionId: %s, RequestId: %s, PacketSize: %d)",
                ~header->SessionId.ToString(),
                ~requestId.ToString(),
                dataSize);
            return NULL;
        }
    } else {
        session = sessionIt->Second();
    }

    IMessage::TPtr message;
    TSequenceId sequenceId;;
    if (!DecodeMessagePacket(MoveRV(data), &message, &sequenceId))
        return session;

    LOG_DEBUG("Message received (IsRequest: %d, SessionId: %s, RequestId: %s, SequenceId: %" PRId64", PacketSize: %d)",
        (int) isRequest,
        ~header->SessionId.ToString(),
        ~requestId.ToString(),
        sequenceId,
        dataSize);

    session->ProcessIncomingMessage(message, sequenceId);

    return session;
}

void TBusServer::EnqueueOutcomingResponse(TSession::TPtr session, TOutcomingResponse::TPtr response)
{
    session->EnqueueResponse(response);
    SessionsWithPendingResponses.Enqueue(session);
    GetEvent().Signal();
}

TIntrusivePtr<TBusServer::TSession> TBusServer::RegisterSession(
    const TSessionId& sessionId,
    const TUdpAddress& clientAddress)
{
    TBlob data;
    CreatePacket(sessionId, TPacketHeader::EType::Ping, &data);
    TGuid pingId = Requester->SendRequest(clientAddress, "", &data);

    auto session = New<TSession>(
        this,
        sessionId,
        clientAddress,
        pingId);

    PingMap.insert(MakePair(pingId, session));
    SessionMap.insert(MakePair(sessionId, session));

    LOG_DEBUG("Session registered (SessionId: %s, ClientAddress: %s, PingId: %s)",
        ~sessionId.ToString(),
        ~GetAddressAsString(clientAddress),
        ~pingId.ToString());

    return session;
}

void TBusServer::UnregisterSession(TIntrusivePtr<TSession> session)
{
    session->Finalize();

    YVERIFY(SessionMap.erase(session->GetSessionId()) == 1);
    YVERIFY(PingMap.erase(session->GetPingId()) == 1);

    LOG_DEBUG("Session unregistered (SessionId: %s)",
        ~session->GetSessionId().ToString());
}

Stroka TBusServer::GetDebugInfo()
{
    return "BusServer info:\n" + Requester->GetDebugInfo();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
