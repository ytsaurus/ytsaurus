#include "bus.h"
#include "rpc.pb.h"

#include "../actions/action_util.h"
#include "../logging/log.h"
#include "../misc/string.h"

#include <util/generic/singleton.h>
#include <util/generic/list.h>
#include <util/generic/utility.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TRpcManager::Get()->GetLogger();

// TODO: make configurable
static const TDuration ServerSleepQuantum = TDuration::MilliSeconds(10);
static const TDuration ClientSleepQuantum = TDuration::MilliSeconds(10);
static const int MaxRequestsPerCall = 100;

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TPacketHeader
{
    static const ui32 ExpectedSignature = 0x78616d6f;

    enum EType
    {
        Message,
        Ping,
        Ack
    };

    ui32 Signature;
    i32 Type;
    TSessionId SessionId;
    
    static const int FixedSize;
};

const int TPacketHeader::FixedSize = sizeof (TPacketHeader);

struct TMultipartPacketHeader
    : public TPacketHeader
{
    static const i32 MaxParts = 1 << 14;
    static const i32 MaxPartSize = 1 << 24;

    i32 PartCount;
    i32 PartSizes[MaxParts];

    static const int FixedSize;
};

const int TMultipartPacketHeader::FixedSize = (size_t) &(((TMultipartPacketHeader*) NULL)->PartSizes);

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
T* ParsePacketHeader(TBlob& data)
{
    if (data.ysize() < T::FixedSize) {
        LOG_ERROR("Packet is too short (Size: %d)", data.ysize());
        return NULL;
    }

    T* header = reinterpret_cast<T*>(data.begin());
    if (header->Signature != TPacketHeader::ExpectedSignature) {
        LOG_ERROR("Invalid packet signature (Signature: %X)", header->Signature);
        return NULL;
    }

    return header;
}

IMessage::TPtr DecodeMessagePacket(TBlob& data)
{
    TMultipartPacketHeader* header = ParsePacketHeader<TMultipartPacketHeader>(data);
    if (header == NULL)
        return NULL;

    if (header->PartCount < 0 || header->PartCount > TMultipartPacketHeader::MaxParts) {
        LOG_ERROR("Invalid part count in a multipart packet (PartCount: %d)", header->PartCount);
        return NULL;
    }

    yvector<TRef> parts(header->PartCount);

    char* ptr = reinterpret_cast<char*>(&header->PartSizes[header->PartCount]);
    char* dataEnd = data.end();
    for (int i = 0; i < header->PartCount; ++i) {
        i32 partSize = header->PartSizes[i];
        if (partSize < 0 || partSize > TMultipartPacketHeader::MaxPartSize)
        {
            LOG_ERROR("Invalid part size in a multipart packet (PartIndex: %d, PartSize: %d)",
                i, partSize);
            return NULL;
        }
        if (ptr + partSize > dataEnd) {
            LOG_ERROR("Buffer overrun in a multipart packet (PartIndex: %d)", i);
            return NULL;
        }
        parts[i] = TRef(ptr, partSize);
        ptr += partSize;
    }

    return new TBlobMessage(data, parts);
}

bool EncodeMessagePacket(
    IMessage::TPtr message,
    const TSessionId& sessionId,
    TBlob* data)
{
    const yvector<TSharedRef>& parts = message->GetParts();

    if (parts.ysize() > TMultipartPacketHeader::MaxParts) {
        LOG_ERROR("Multipart message contains too many parts (PartCount: %d)",
            parts.ysize());
        return false;
    }

    i64 dataSize = 0;
    dataSize += TMultipartPacketHeader::FixedSize;
    dataSize += sizeof (i32) * parts.ysize();
    for (int index = 0; index < parts.ysize(); ++index)
    {
        const TSharedRef& part = parts[index];
        i32 partSize = static_cast<i32>(part.Size());
        if (partSize > TMultipartPacketHeader::MaxPartSize) {
            LOG_ERROR("Multipart message part is too large (PartIndex: %d, PartSize: %d)",
                index,
                partSize);
            return false;
        }
        dataSize += partSize;
    }

    data->resize(static_cast<size_t>(dataSize));

    TMultipartPacketHeader* header = reinterpret_cast<TMultipartPacketHeader*>(data->begin());
    header->Signature = TPacketHeader::ExpectedSignature;
    header->Type = TPacketHeader::Message;
    header->SessionId = sessionId;
    header->PartCount = parts.ysize();
    for (int i = 0; i < header->PartCount; ++i) {
        header->PartSizes[i] = static_cast<i32>(parts[i].Size());
    }

    char* current = reinterpret_cast<char*>(&header->PartSizes[parts.ysize()]);
    for (int i = 0; i < header->PartCount; ++i) {
        const TRef& part = parts[i];
        NStl::copy(part.Begin(), part.End(), current);
        current += part.Size();
    }

    return true;
}

void CreatePacket(const TSessionId& sessionId, TPacketHeader::EType type, TBlob* data)
{
    data->resize(TPacketHeader::FixedSize);
    TPacketHeader* header = reinterpret_cast<TPacketHeader*>(data->begin());
    header->Signature = TPacketHeader::ExpectedSignature;
    header->Type = type;
    header->SessionId = sessionId;
}

} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

struct TBusServer::TReply
    : public TRefCountedBase
{
    typedef TIntrusivePtr<TReply> TPtr;

    TReply(TSessionId sessionId, TBlob& data)
        : SessionId(sessionId)
    {
        Data.swap(data);
    }

    TSessionId SessionId;
    TBlob Data;
};

////////////////////////////////////////////////////////////////////////////////

class TBusServer::TReplyBus
    : public IBus
{
public:
    typedef TIntrusivePtr<TReplyBus> TPtr;

    TReplyBus(TBusServer::TPtr server, TSessionId sessionId)
        : Server(server)
        , SessionId(sessionId)
    { }

    virtual TSendResult::TPtr Send(IMessage::TPtr message)
    {
        // Load to a local since the other thread may be calling Terminate.
        TBusServer::TPtr server = Server;
        if (~server == NULL) {
            LOG_WARNING("Attempt to reply via a detached bus");
            return NULL;
        }

        TBlob data;
        EncodeMessagePacket(message, SessionId, &data);
        
        TReply::TPtr reply = new TReply(SessionId, data);
        server->EnqueueReply(reply);
        return NULL;

    }

    void Terminate()
    {
        Server.Drop();
    }

private:
    TBusServer::TPtr Server;
    TSessionId SessionId;
};

////////////////////////////////////////////////////////////////////////////////

class TBusServer::TSession
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TBusServer::TPtr server,
        const TSessionId& sessionId,
        const TUdpAddress& clientAddress,
        const TGUID& pingId)
        : Server(server)
        , SessionId(sessionId)
        , ClientAddress(clientAddress)
        , PingId(pingId)
        , ReplyBus(new TReplyBus(server, sessionId))
        , Terminated(false)
    { }

    TGUID SendReply(TReply::TPtr reply)
    {
        return Server->Requester->SendRequest(ClientAddress, "", &reply->Data);
    }

    void ProcessMessage(IMessage::TPtr message)
    {
        Server->Handler->OnMessage(message, ~ReplyBus);
    }

    TSessionId GetSessionId() const
    {
        return SessionId;
    }

    TGUID GetPingId() const
    {
        return PingId;
    }

private:
    typedef yvector<TGUID> TRequestIds;
    typedef NStl::deque<IMessage::TPtr> TResponseMessages;

    TBusServer::TPtr Server;
    TSessionId SessionId;
    TUdpAddress ClientAddress;
    TGUID PingId;
    TReplyBus::TPtr ReplyBus;
    bool Terminated;
};

////////////////////////////////////////////////////////////////////////////////

TBusServer::TBusServer(int port, IMessageHandler* handler)
    : Handler(handler)
    , Terminated(false)
    , Thread(ThreadFunc, (void*) this)
{
    YASSERT(Handler != NULL);

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
        case TPacketHeader::Message:
            ProcessMessage(header, nlRequest);
            break;

        default:
            LOG_ERROR("Invalid request packet type (RequestId: %s, Type: %d)",
                ~StringFromGuid(nlRequest->ReqId), (int) header->Type);
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
        case TPacketHeader::Ack:
            ProcessAck(header, nlResponse);
            break;

        case TPacketHeader::Message:
            ProcessMessage(header, nlResponse);
            break;

        default:
            LOG_ERROR("Invalid response packet type (RequestId: %s, Type: %d)",
                ~StringFromGuid(nlResponse->ReqId), (int) header->Type);
    }
}

void TBusServer::ProcessFailedNLResponse(TUdpHttpResponse* nlResponse)
{
    TPingMap::iterator pingIt = PingMap.find(nlResponse->ReqId);
    if (pingIt == PingMap.end()) {
        LOG_DEBUG("Request failed (RequestId: %s)",
            ~StringFromGuid(nlResponse->ReqId));
    } else {
        LOG_DEBUG("Ping failed (RequestId: %s)",
            ~StringFromGuid(nlResponse->ReqId));

        TSession::TPtr session = pingIt->Second();
        UnregisterSession(session);
    }
}

bool TBusServer::ProcessReplies()
{
    bool result = false;
    for (int i = 0; i < MaxRequestsPerCall; ++i) {
        TReply::TPtr reply;
        if (!ReplyQueue.Dequeue(&reply))
            break;
        result = true;
        ProcessReply(reply);
    }
    return result;
}

void TBusServer::ProcessReply(TReply::TPtr reply)
{
    TSessionMap::iterator sessionIt = SessionMap.find(reply->SessionId);
    if (sessionIt == SessionMap.end()) {
        LOG_DEBUG("Reply to an obsolete session is dropped (SessionId: %s, Reply: %p)",
            ~StringFromGuid(reply->SessionId), ~reply);
        return;
    }
    
    TSession::TPtr session = sessionIt->Second();
    TGUID requestId = session->SendReply(reply);

    LOG_DEBUG("Message sent (IsRequest: 1, SessionId: %s, RequestId: %s, Reply: %p)",
        ~StringFromGuid(reply->SessionId), ~StringFromGuid(requestId), ~reply);
}

void TBusServer::ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse)
{
    TPingMap::iterator pingIt = PingMap.find(nlResponse->ReqId);
    if (pingIt == PingMap.end()) {
        LOG_DEBUG("Ack received (SessionId: %s, RequestId: %s)",
            ~StringFromGuid(header->SessionId), ~StringFromGuid(nlResponse->ReqId));
    } else {
        LOG_DEBUG("Ping ack received (RequestId: %s)",
            ~StringFromGuid(nlResponse->ReqId));

        TSession::TPtr session = pingIt->Second();
        UnregisterSession(session);
    }
}

void TBusServer::ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest)
{
    TSessionId sessionId =  header->SessionId;

    DoProcessMessage(
        header,
        nlRequest->ReqId,
        nlRequest->PeerAddress,
        nlRequest->Data,
        true);
    
    TReply::TPtr reply;
    // TODO
    if (false/*ReplyQueue.Dequeue(&reply)*/) {
        Requester->SendResponse(nlRequest->ReqId, &reply->Data);

        LOG_DEBUG("Message sent (IsRequest: 0, SessionId: %s, RequestId: %s, Reply: %p)",
            ~StringFromGuid(reply->SessionId), ~StringFromGuid(nlRequest->ReqId), ~reply);
    } else {
        TBlob ackData;
        CreatePacket(sessionId, TPacketHeader::Ack, &ackData);

        Requester->SendResponse(nlRequest->ReqId, &ackData);

        LOG_DEBUG("Ack sent (SessionId: %s, RequestId: %s)",
            ~StringFromGuid(sessionId), ~StringFromGuid(nlRequest->ReqId));
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

void TBusServer::DoProcessMessage(
    TPacketHeader* header,
    const TGUID& requestId,
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
                ~StringFromGuid(header->SessionId), ~StringFromGuid(requestId), dataSize);
            return;
        }
    } else {
        session = sessionIt->Second();
    }

    IMessage::TPtr message = DecodeMessagePacket(data);
    if (~message == NULL)
        return;

    LOG_DEBUG("Message received (IsRequest: %d, SessionId: %s, RequestId: %s, PacketSize: %d)",
        (int) isRequest, ~StringFromGuid(header->SessionId), ~StringFromGuid(requestId), dataSize);

    session->ProcessMessage(message);
}

void TBusServer::EnqueueReply(TReply::TPtr reply)
{
    int dataSize = reply->Data.ysize();
    ReplyQueue.Enqueue(reply);
    GetEvent().Signal();
    LOG_DEBUG("Reply enqueued (SessionId: %s, Reply: %p, PacketSize: %d)",
        ~StringFromGuid(reply->SessionId), ~reply, dataSize);
}

TIntrusivePtr<TBusServer::TSession> TBusServer::RegisterSession(
    const TSessionId& sessionId,
    const TUdpAddress& clientAddress)
{
    TBlob data;
    CreatePacket(sessionId, TPacketHeader::Ping, &data);
    TGUID pingId = Requester->SendRequest(clientAddress, "", &data);

    TSession::TPtr session = new TSession(
        this,
        sessionId,
        clientAddress,
        pingId);

    PingMap.insert(MakePair(pingId, session));
    SessionMap.insert(MakePair(sessionId, session));

    LOG_DEBUG("Session registered (SessionId: %s, ClientAddress: %s, PingId: %s)",
        ~StringFromGuid(sessionId), ~GetAddressAsString(clientAddress), ~StringFromGuid(pingId));

    return session;
}

void TBusServer::UnregisterSession(TIntrusivePtr<TSession> session)
{
    VERIFY(SessionMap.erase(session->GetSessionId()) == 1, "Failed to erase a session");
    VERIFY(PingMap.erase(session->GetPingId()) == 1, "Failed to erase a session ping");

    LOG_DEBUG("Session unregistered (SessionId: %s)",
        ~StringFromGuid(session->GetSessionId()));
}

Stroka TBusServer::GetDebugInfo()
{
    return "BusServer info:\n" + Requester->GetDebugInfo();
}

////////////////////////////////////////////////////////////////////////////////
class TClientDispatcher;

class TBusClient::TBus
    : public IBus
{
public:
    typedef TIntrusivePtr<TBus> TPtr;

    TBus(TBusClient::TPtr client, IMessageHandler* handler);

    virtual TSendResult::TPtr Send(IMessage::TPtr message);
    virtual void Terminate();

private:
    friend class TClientDispatcher;

    typedef yhash_set<TGUID, TGUIDHash> TRequestIdSet;

    TBusClient::TPtr Client;
    IMessageHandler* Handler;
    volatile bool Terminated;
    TSessionId SessionId;
    TRequestIdSet RequestIds;
    TRequestIdSet PingIds;
};

////////////////////////////////////////////////////////////////////////////////

class TClientDispatcher
{
    struct TRequest
        : public TRefCountedBase
    {
        typedef TIntrusivePtr<TRequest> TPtr;

        TRequest(
            const TSessionId& sessionId,
            TBlob& data)
            : SessionId(sessionId)
            , Result(new IBus::TSendResult())
        {
            Data.swap(data);
        }

        TSessionId SessionId;
        IBus::TSendResult::TPtr Result;
        TBlob Data;
    };

    typedef yhash_map<TSessionId, TBusClient::TBus::TPtr, TGUIDHash > TBusMap;
    typedef yhash_map<TGUID, TRequest::TPtr, TGUIDHash> TRequestMap;

    TThread Thread;
    volatile bool Terminated;
    TIntrusivePtr<IRequester> Requester;
   
    TBusMap BusMap;
    TRequestMap RequestMap;

    TLockFreeQueue<TRequest::TPtr> RequestQueue;
    TLockFreeQueue<TBusClient::TBus::TPtr> BusRegisterQueue;
    TLockFreeQueue<TBusClient::TBus::TPtr> BusUnregisterQueue;


    static void* ThreadFunc(void* param)
    {
        TClientDispatcher* dispatcher = reinterpret_cast<TClientDispatcher*>(param);
        dispatcher->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        while (!Terminated) {
            if (!ProcessBusRegistrations() &&
                !ProcessBusUnregistrations() &&
                !ProcessRequests() &&
                !ProcessNLRequests() &&
                !ProcessNLResponses())
            {
                GetEvent().WaitT(ClientSleepQuantum);
            }
        }
    }

    bool ProcessBusRegistrations()
    {
        bool result = false;
        TBusClient::TBus::TPtr bus;
        while (BusRegisterQueue.Dequeue(&bus)) {
            result = true;
            RegisterBus(bus);
        }
        return result;
    }

    bool ProcessBusUnregistrations()
    {
        bool result = false;
        TBusClient::TBus::TPtr bus;
        while (BusUnregisterQueue.Dequeue(&bus)) {
            result = true;
            UnregisterBus(bus);
        }
        return result;
    }

    void RegisterBus(TBusClient::TBus::TPtr bus)
    {
        BusMap.insert(MakePair(bus->SessionId, bus));
        LOG_DEBUG("Bus is registered (SessionId: %s, Bus: %p)",
            ~StringFromGuid(bus->SessionId), ~bus);
    }

    void UnregisterBus(TBusClient::TBus::TPtr bus)
    {
        BusMap.erase(bus->SessionId);

        for (TBusClient::TBus::TRequestIdSet::iterator i = bus->RequestIds.begin();
            i != bus->RequestIds.end();
            ++i)
        {
            const TGUID& requestId = *i;
            Requester->CancelRequest(requestId);
            RequestMap.erase(requestId);
        }
        bus->RequestIds.clear();

        TBlob ackData;
        CreatePacket(bus->SessionId, TPacketHeader::Ack, &ackData);

        for (TBusClient::TBus::TRequestIdSet::iterator i = bus->PingIds.begin();
            i != bus->PingIds.end();
            ++i)
        {
            const TGUID& requestId = *i;
            Requester->SendResponse(requestId, &ackData);
        }
        bus->PingIds.clear();

        LOG_DEBUG("Bus is unregistered (SessionId: %s, Bus: %p)",
            ~StringFromGuid(bus->SessionId), ~bus);
    }

    bool ProcessNLResponses()
    {
        bool result = false;
        for (int i = 0; i < MaxRequestsPerCall; ++i) {
            TAutoPtr<TUdpHttpResponse> nlResponse = Requester->GetResponse();
            if (~nlResponse == NULL)
                break;
            result = true;
            ProcessNLResponse(~nlResponse);
        }
        return result;
    }

    void ProcessNLResponse(TUdpHttpResponse* nlResponse)
    {
        if (nlResponse->Ok != TUdpHttpResponse::OK)
        {
            ProcessFailedNLResponse(nlResponse);
            return;
        }

        TPacketHeader* header = ParsePacketHeader<TPacketHeader>(nlResponse->Data);
        if (header == NULL)
            return;

        switch (header->Type) { 
            case TPacketHeader::Ack:
                ProcessAck(header, nlResponse);
                break;

            case TPacketHeader::Message:
                ProcessMessage(header, nlResponse);
                break;

            default:
                LOG_ERROR("Invalid response packet type (RequestId: %s, Type: %d)",
                    ~StringFromGuid(nlResponse->ReqId), (int) header->Type);
        }
    }

    void ProcessFailedNLResponse(TUdpHttpResponse* nlResponse)
    {
        TRequestMap::iterator requestIt = RequestMap.find(nlResponse->ReqId);
        if (requestIt == RequestMap.end()) {
            LOG_DEBUG("An obsolete request failed (RequestId: %s)",
                ~StringFromGuid(nlResponse->ReqId));
            return;
        }
        
        TRequest::TPtr request = requestIt->Second();

        LOG_DEBUG("Request failed (SessionId: %s, RequestId: %s)",
            ~StringFromGuid(request->SessionId), ~StringFromGuid(nlResponse->ReqId));

        request->Result->Set(IBus::Failed);
        RequestMap.erase(requestIt);
    }

    bool ProcessNLRequests()
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

    void ProcessNLRequest(TUdpHttpRequest* nlRequest)
    {
        TPacketHeader* header = ParsePacketHeader<TPacketHeader>(nlRequest->Data);
        if (header == NULL)
            return;

        switch (header->Type) { 
        case TPacketHeader::Ping:
            ProcessPing(header, nlRequest);
            break;

        case TPacketHeader::Message:
            ProcessMessage(header, nlRequest);
            break;

        default:
            LOG_ERROR("Invalid request packet type (RequestId: %s, Type: %d)",
                ~StringFromGuid(nlRequest->ReqId), (int) header->Type);
            return;
        }
    }

    void ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse)
    {
        TRequestMap::iterator requestIt = RequestMap.find(nlResponse->ReqId);
        if (requestIt == RequestMap.end()) {
            LOG_DEBUG("An obsolete request ack received (SessionId: %s, RequestId: %s)",
                ~StringFromGuid(header->SessionId), ~StringFromGuid(nlResponse->ReqId));
            return;
        }

        LOG_DEBUG("Request ack received (SessionId: %s, RequestId: %s)",
            ~StringFromGuid(header->SessionId), ~StringFromGuid(nlResponse->ReqId));

        TRequest::TPtr request = requestIt->Second();
        request->Result->Set(IBus::OK);
        RequestMap.erase(requestIt);
    }

    void ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest)
    {
        DoProcessMessage(header, nlRequest->ReqId, nlRequest->Data, true);
    }

    void ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse)
    {
        DoProcessMessage(header, nlResponse->ReqId, nlResponse->Data, false);
    }

    void DoProcessMessage(TPacketHeader* header, const TGUID& requestId, TBlob& data, bool isRequest)
    {
        int dataSize = data.ysize();

        TBusMap::iterator busIt = BusMap.find(header->SessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Message for an obsolete session is dropped (SessionId: %s, RequestId: %s, PacketSize: %d)",
                ~StringFromGuid(header->SessionId), ~StringFromGuid(requestId), dataSize);
            return;
        }

        IMessage::TPtr message = DecodeMessagePacket(data);
        if (~message == NULL)
            return;

        LOG_DEBUG("Message received (IsRequest: %d, SessionId: %s, RequestId: %s, PacketSize: %d)",
            (int) isRequest, ~StringFromGuid(header->SessionId), ~StringFromGuid(requestId), dataSize);

        TBusClient::TBus::TPtr bus = busIt->Second();
        bus->Handler->OnMessage(message, ~bus);

        if (!isRequest) {
            RequestMap.erase(requestId);
        } else {
            TBlob ackData;
            CreatePacket(bus->SessionId, TPacketHeader::Ack, &ackData);
            Requester->SendResponse(requestId, &ackData);

            LOG_DEBUG("Ack sent (SessionId: %s, RequestId: %s)",
                ~StringFromGuid(bus->SessionId), ~StringFromGuid(requestId));
        }
    }

    void ProcessPing(TPacketHeader* header, TUdpHttpRequest* nlRequest)
    {
        TBusMap::iterator busIt = BusMap.find(header->SessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Ping for an obsolete session received (SessionId: %s, RequestId: %s)",
                ~StringFromGuid(header->SessionId), ~StringFromGuid(nlRequest->ReqId));

            TBlob data;
            CreatePacket(header->SessionId, TPacketHeader::Ack, &data);
            Requester->SendResponse(nlRequest->ReqId, &data);

            return;
        }

        LOG_DEBUG("Ping received (SessionId: %s, RequestId: %s)",
            ~StringFromGuid(header->SessionId), ~StringFromGuid(nlRequest->ReqId));
        
        // Don't reply to a ping, just register it.
        TBusClient::TBus::TPtr bus = busIt->Second();
        bus->PingIds.insert(nlRequest->ReqId);
    }

    bool ProcessRequests()
    {
        bool result = false;
        for (int i = 0; i < MaxRequestsPerCall; ++i) {
            TRequest::TPtr request;
            if (!RequestQueue.Dequeue(&request))
                break;
            result = true;
            ProcessRequest(request);
        }
        return result;
    }

    void ProcessRequest(TRequest::TPtr request)
    {
        TBusMap::iterator busIt = BusMap.find(request->SessionId);
        if (busIt == BusMap.end()) {
            // Process all pending registrations and try once again.
            ProcessBusRegistrations();
            busIt = BusMap.find(request->SessionId);
            if (busIt == BusMap.end()) {
                // Still no luck.
                LOG_DEBUG("Request via an obsolete session is dropped (SessionId: %s, Request: %p)",
                    ~StringFromGuid(request->SessionId), ~request);
                return;
            }
        }

        TBusClient::TBus::TPtr bus = busIt->Second();

        TGUID requestId = Requester->SendRequest(bus->Client->ServerAddress, "", &request->Data);

        bus->RequestIds.insert(requestId);
        RequestMap.insert(MakePair(requestId, request));

        LOG_DEBUG("Request sent (SessionId: %s, RequestId: %s, Request: %p)",
            ~StringFromGuid(request->SessionId), ~StringFromGuid(requestId), ~request);
    }

    Event& GetEvent()
    {
        return Requester->GetAsyncEvent();
    }

public:
    TClientDispatcher()
        : Thread(ThreadFunc, (void*) this)
        , Terminated(false)
    {
        Requester = CreateHttpUdpRequester(0);
        if (~Requester == NULL)
            throw yexception() << "Failed to create a client dispatcher";

        Thread.Start();

        LOG_DEBUG("Client dispatcher is started");
    }

    ~TClientDispatcher()
    {
        Terminated = true;
        Thread.Join();

        Requester->StopNoWait();

        // NB: cannot use log here
    }

    static TClientDispatcher* Get()
    {
        return Singleton<TClientDispatcher>();
    }

    IBus::TSendResult::TPtr EnqueueRequest(TBusClient::TBus* bus, IMessage::TPtr message)
    {
        TBlob data;
        if (!EncodeMessagePacket(message, bus->SessionId, &data))
            throw yexception() << "Failed to encode a message";

        int dataSize = data.ysize();
        TRequest::TPtr request = new TRequest(bus->SessionId, data);
        RequestQueue.Enqueue(request);
        GetEvent().Signal();

        LOG_DEBUG("Request enqueued (SessionId: %s, Request: %p, PacketSize: %d)",
            ~StringFromGuid(bus->SessionId), ~request, dataSize);

        return request->Result;
    }

    void EnqueueBusRegister(TBusClient::TBus::TPtr bus)
    {
        BusRegisterQueue.Enqueue(bus);
        GetEvent().Signal();
        
        LOG_DEBUG("Bus registration enqueued (SessionId: %s, Bus: %p)",
            ~StringFromGuid(bus->SessionId), ~bus);
    }

    void EnqueueBusUnregister(TBusClient::TBus::TPtr bus)
    {
        BusUnregisterQueue.Enqueue(bus);
        GetEvent().Signal();

        LOG_DEBUG("Bus unregistration enqueued (SessionId: %s, Bus: %p)",
            ~StringFromGuid(bus->SessionId), ~bus);
    }

    Stroka GetDebugInfo()
    {
        return "ClientDispatcher info:\n" + Requester->GetDebugInfo();
    }
};

////////////////////////////////////////////////////////////////////////////////

TBusClient::TBus::TBus(TBusClient::TPtr client, IMessageHandler* handler)
    : Client(client)
    , Handler(handler)
    , Terminated(false)
{
    CreateGuid(&SessionId);
}

IBus::TSendResult::TPtr TBusClient::TBus::Send(IMessage::TPtr message)
{
    return TClientDispatcher::Get()->EnqueueRequest(this, message);
};

void TBusClient::TBus::Terminate()
{
    Terminated = true;
    TClientDispatcher::Get()->EnqueueBusUnregister(this);
};

////////////////////////////////////////////////////////////////////////////////

TBusClient::TBusClient(Stroka address)
{
    ServerAddress = CreateAddress(address, 0);
    if (ServerAddress == TUdpAddress())
        throw yexception() << "Failed to resolve the address " << address;   
}

IBus::TPtr TBusClient::CreateBus(IMessageHandler* handler)
{
    TBus::TPtr bus = new TBus(this, handler);
    TClientDispatcher::Get()->EnqueueBusRegister(bus);
    return ~bus;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
