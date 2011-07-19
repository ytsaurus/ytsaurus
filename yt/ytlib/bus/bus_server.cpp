#include "bus_server.h"
#include "message.h"
#include "message_rearranger.h"

#include "../actions/action_util.h"
#include "../logging/log.h"

#include <util/generic/singleton.h>
#include <util/generic/list.h>
#include <util/generic/deque.h>
#include <util/generic/utility.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

// TODO: make configurable
static const int MaxRequestsPerCall = 100;
static const TDuration ServerSleepQuantum = TDuration::MilliSeconds(10);
static const TDuration MessageRearrangeTimeout = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

struct TBusServer::TReply
    : public TRefCountedBase
{
    typedef TIntrusivePtr<TReply> TPtr;

    TReply(TBlob& data)
    {
        Data.swap(data);
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
    { }

    void Initialize()
    {
        // Cannot do this in ctor since a smartpointer for this is needed.
        MessageRearranger.Reset(new TMessageRearranger(
            FromMethod(&TSession::OnMessageDequeued, TPtr(this)),
            MessageRearrangeTimeout));
    }

    void Finalize()
    {
        // Drop the rearranger to kill the cyclic dependency.
        MessageRearranger.Destroy();

        // Also forget about the server.
        Server.Drop();
    }

    TGUID SendReply(TReply::TPtr reply)
    {
        return Server->Requester->SendRequest(ClientAddress, "", &reply->Data);
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

    // IBus implementation.
    virtual TSendResult::TPtr Send(IMessage::TPtr message)
    {
        // Load to a local since the other thread may be calling Finalize.
        TBusServer::TPtr server = Server;
        if (~server == NULL) {
            LOG_WARNING("Attempt to reply via a detached bus");
            return NULL;
        }

        TSequenceId sequenceId = GenerateSequenceId();

        TBlob data;
        EncodeMessagePacket(message, SessionId, sequenceId, &data);
        int dataSize = data.ysize();

        TReply::TPtr reply = new TReply(data);
        server->EnqueueReply(this, reply);

        LOG_DEBUG("Reply enqueued (SessionId: %s, Reply: %p, PacketSize: %d)",
            ~SessionId.ToString(),
            ~reply,
            dataSize);

        return NULL;
    }

    void EnqueueReply(TReply::TPtr reply)
    {
        PendingReplies.Enqueue(reply);
    }

    TReply::TPtr DequeueReply()
    {
        TReply::TPtr reply;
        PendingReplies.Dequeue(&reply);
        return reply;
    }

    virtual void Terminate()
    {
        // Terminate has no effect for a reply bus.
    }

private:
    typedef yvector<TGuid> TRequestIds;
    typedef NStl::deque<IMessage::TPtr> TResponseMessages;

    TBusServer::TPtr Server;
    TSessionId SessionId;
    TUdpAddress ClientAddress;
    TGuid PingId;
    bool Terminated;
    TAtomic SequenceId;
    THolder<TMessageRearranger> MessageRearranger;
    TLockFreeQueue<TReply::TPtr> PendingReplies;

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
    if (~Requester == NULL)
        throw yexception() << "Failed to create a bus server on port " << port;

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

    Requester->StopNoWait();

    Terminated = true;
    Thread.Join();

    for (TSessionMap::iterator it = SessionMap.begin();
         it != SessionMap.end();
         ++it)
    {
        it->second->Finalize();
    }
    SessionMap.clear();

    PingMap.clear();
}

void* TBusServer::ThreadFunc(void* param)
{
    TBusServer* listener = reinterpret_cast<TBusServer*>(param);
    listener->ThreadMain();
    return NULL;
}

Event& TBusServer::GetEvent()
{
    return Requester->GetAsyncEvent();
}

void TBusServer::ThreadMain()
{
    while (!Terminated) {
        if (!ProcessNLRequests() &&
            !ProcessNLResponses() &&
            !ProcessReplies())
        {
            GetEvent().WaitT(ServerSleepQuantum);
        }
    }
}

bool TBusServer::ProcessNLRequests()
{
    bool result = false;
    for (int i = 0; i < MaxRequestsPerCall; ++i) {
        TAutoPtr<TUdpHttpRequest> nlRequest = Requester->GetRequest();
        if (~nlRequest == NULL)
            break;
        result = true;
        ProcessNLRequest(~nlRequest);
    }
    return result;
}

void TBusServer::ProcessNLRequest(TUdpHttpRequest* nlRequest)
{
    TPacketHeader* header = ParsePacketHeader<TPacketHeader>(nlRequest->Data);
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

bool TBusServer::ProcessNLResponses()
{
    bool result = false;
    for (int i = 0; i < MaxRequestsPerCall; ++i) {
        TAutoPtr<TUdpHttpResponse> nlResponse = Requester->GetResponse();
        if (~nlResponse== NULL)
            break;
        result = true;
        ProcessNLResponse(~nlResponse);
    }
    return result;
}

void TBusServer::ProcessNLResponse(TUdpHttpResponse* nlResponse)
{
    if (nlResponse->Ok != TUdpHttpResponse::OK) {
        ProcessFailedNLResponse(nlResponse);
        return;
    }

    TPacketHeader* header = ParsePacketHeader<TPacketHeader>(nlResponse->Data);
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
    TPingMap::iterator pingIt = PingMap.find(nlResponse->ReqId);
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

bool TBusServer::ProcessReplies()
{
    bool result = false;
    for (int i = 0; i < MaxRequestsPerCall; ++i) {
        TSession::TPtr session;
        if (!PendingReplySessions.Dequeue(&session))
            break;

        TReply::TPtr reply = session->DequeueReply();
        if (~reply == NULL)
            break;

        result = true;
        ProcessReply(session, reply);
    }
    return result;
}

void TBusServer::ProcessReply(TSession::TPtr session, TReply::TPtr reply)
{
    TGuid requestId = session->SendReply(reply);
    LOG_DEBUG("Message sent (IsRequest: 1, SessionId: %s, RequestId: %s, Reply: %p)",
        ~session->GetSessionId().ToString(),
        ~requestId.ToString(),
        ~reply);
}

void TBusServer::ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse)
{
    TGuid requestId = nlResponse->ReqId;
    TPingMap::iterator pingIt = PingMap.find(requestId);
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
    TSession::TPtr session = DoProcessMessage(
        header,
        requestId,
        nlRequest->PeerAddress,
        nlRequest->Data,
        true);

    TReply::TPtr reply = session->DequeueReply();
    if (~reply != NULL) {
        Requester->SendResponse(nlRequest->ReqId, &reply->Data);

        LOG_DEBUG("Message sent (IsRequest: 0, SessionId: %s, RequestId: %s, Reply: %p)",
            ~session->GetSessionId().ToString(),
            ~requestId.ToString(),
            ~reply);
    } else {
        TBlob ackData;
        CreatePacket(session->GetSessionId(), TPacketHeader::EType::Ack, &ackData);

        Requester->SendResponse(nlRequest->ReqId, &ackData);

        LOG_DEBUG("Ack sent (SessionId: %s, RequestId: %s)",
            ~session->GetSessionId().ToString(),
            ~requestId.ToString());
    }
}

void TBusServer::ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse)
{
    DoProcessMessage(
        header,
        nlResponse->ReqId,
        nlResponse->PeerAddress,
        nlResponse->Data,
        false);
}

TBusServer::TSession::TPtr TBusServer::DoProcessMessage(
    TPacketHeader* header,
    const TGuid& requestId,
    const TUdpAddress& address,
    TBlob& data,
    bool isRequest)
{
    int dataSize = data.ysize();

    TSession::TPtr session;
    TSessionMap::iterator sessionIt = SessionMap.find(header->SessionId);
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
    if (!DecodeMessagePacket(data, &message, &sequenceId))
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

void TBusServer::EnqueueReply(TSession::TPtr session, TReply::TPtr reply)
{
    session->EnqueueReply(reply);
    PendingReplySessions.Enqueue(session);
    GetEvent().Signal();
}

TIntrusivePtr<TBusServer::TSession> TBusServer::RegisterSession(
    const TSessionId& sessionId,
    const TUdpAddress& clientAddress)
{
    TBlob data;
    CreatePacket(sessionId, TPacketHeader::EType::Ping, &data);
    TGuid pingId = Requester->SendRequest(clientAddress, "", &data);

    TSession::TPtr session = new TSession(
        this,
        sessionId,
        clientAddress,
        pingId);
    session->Initialize();

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

    VERIFY(SessionMap.erase(session->GetSessionId()) == 1, "Failed to erase a session");
    VERIFY(PingMap.erase(session->GetPingId()) == 1, "Failed to erase a session ping");

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
