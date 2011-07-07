#include "bus_client.h"
#include "rpc.pb.h"
#include "message_rearranger.h"
#include "packet.h"

#include "../actions/action_util.h"
#include "../logging/log.h"
#include "../misc/string.h"

#include <quality/NetLiba/UdpHttp.h>

#include <util/generic/singleton.h>
#include <util/generic/list.h>
#include <util/generic/utility.h>


namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

// TODO: make configurable
static const TDuration ClientSleepQuantum = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

class TBusClient::TBus
    : public IBus
{
public:
    typedef TIntrusivePtr<TBus> TPtr;

    TBus(TBusClient::TPtr client, IMessageHandler* handler);
    void Initialize();

    void ProcessIncomingMessage(IMessage::TPtr message, TSequenceId sequenceId);

    virtual TSendResult::TPtr Send(IMessage::TPtr message);
    virtual void Terminate();

    TSequenceId GenerateSequenceId();

private:
    friend class TClientDispatcher;

    typedef yhash_set<TGUID, TGUIDHash> TRequestIdSet;

    TBusClient::TPtr Client;
    IMessageHandler* Handler;
    volatile bool Terminated;
    TAtomic SequenceId;
    TSessionId SessionId;
    TRequestIdSet RequestIds;
    TRequestIdSet PingIds;
    THolder<TMessageRearranger> MessageRearranger;

    void OnMessageDequeued(IMessage::TPtr message);

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
            ~StringFromGuid(bus->SessionId),
            ~bus);
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
            ~StringFromGuid(bus->SessionId),
            ~bus);
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
                    ~StringFromGuid(nlResponse->ReqId),
                    (int) header->Type);
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
            ~StringFromGuid(request->SessionId),
            ~StringFromGuid(nlResponse->ReqId));

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
                ~StringFromGuid(nlRequest->ReqId),
                (int) header->Type);
            return;
        }
    }

    void ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse)
    {
        TRequestMap::iterator requestIt = RequestMap.find(nlResponse->ReqId);
        if (requestIt == RequestMap.end()) {
            LOG_DEBUG("An obsolete request ack received (SessionId: %s, RequestId: %s)",
                ~StringFromGuid(header->SessionId),
                ~StringFromGuid(nlResponse->ReqId));
            return;
        }

        LOG_DEBUG("Request ack received (SessionId: %s, RequestId: %s)",
            ~StringFromGuid(header->SessionId),
            ~StringFromGuid(nlResponse->ReqId));

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
                ~StringFromGuid(header->SessionId),
                ~StringFromGuid(requestId),
                dataSize);
            return;
        }

        IMessage::TPtr message;
        TSequenceId sequenceId;;
        if (!DecodeMessagePacket(data, &message, &sequenceId))
            return;

        LOG_DEBUG("Message received (IsRequest: %d, SessionId: %s, RequestId: %s, PacketSize: %d)",
            (int) isRequest,
            ~StringFromGuid(header->SessionId),
            ~StringFromGuid(requestId),
            dataSize);

        TBusClient::TBus::TPtr bus = busIt->Second();
        bus->ProcessIncomingMessage(message, sequenceId);

        if (!isRequest) {
            RequestMap.erase(requestId);
        } else {
            TBlob ackData;
            CreatePacket(bus->SessionId, TPacketHeader::Ack, &ackData);
            Requester->SendResponse(requestId, &ackData);

            LOG_DEBUG("Ack sent (SessionId: %s, RequestId: %s)",
                ~StringFromGuid(bus->SessionId),
                ~StringFromGuid(requestId));
        }
    }

    void ProcessPing(TPacketHeader* header, TUdpHttpRequest* nlRequest)
    {
        TBusMap::iterator busIt = BusMap.find(header->SessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Ping for an obsolete session received (SessionId: %s, RequestId: %s)",
                ~StringFromGuid(header->SessionId),
                ~StringFromGuid(nlRequest->ReqId));

            TBlob data;
            CreatePacket(header->SessionId, TPacketHeader::Ack, &data);
            Requester->SendResponse(nlRequest->ReqId, &data);

            return;
        }

        LOG_DEBUG("Ping received (SessionId: %s, RequestId: %s)",
            ~StringFromGuid(header->SessionId),
            ~StringFromGuid(nlRequest->ReqId));

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
                    ~StringFromGuid(request->SessionId),
                    ~request);
                return;
            }
        }

        TBusClient::TBus::TPtr bus = busIt->Second();

        TGUID requestId = Requester->SendRequest(bus->Client->ServerAddress, "", &request->Data);

        bus->RequestIds.insert(requestId);
        RequestMap.insert(MakePair(requestId, request));

        LOG_DEBUG("Request sent (SessionId: %s, RequestId: %s, Request: %p)",
            ~StringFromGuid(request->SessionId),
            ~StringFromGuid(requestId),
            ~request);
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
        TSequenceId sequenceId = bus->GenerateSequenceId();

        TBlob data;
        if (!EncodeMessagePacket(message, bus->SessionId, sequenceId, &data))
            throw yexception() << "Failed to encode a message";

        int dataSize = data.ysize();
        TRequest::TPtr request = new TRequest(bus->SessionId, data);
        RequestQueue.Enqueue(request);
        GetEvent().Signal();

        LOG_DEBUG("Request enqueued (SessionId: %s, Request: %p, PacketSize: %d)",
            ~StringFromGuid(bus->SessionId),
            ~request,
            dataSize);

        return request->Result;
    }

    void EnqueueBusRegister(TBusClient::TBus::TPtr bus)
    {
        BusRegisterQueue.Enqueue(bus);
        GetEvent().Signal();

        LOG_DEBUG("Bus registration enqueued (SessionId: %s, Bus: %p)",
            ~StringFromGuid(bus->SessionId),
            ~bus);
    }

    void EnqueueBusUnregister(TBusClient::TBus::TPtr bus)
    {
        BusUnregisterQueue.Enqueue(bus);
        GetEvent().Signal();

        LOG_DEBUG("Bus unregistration enqueued (SessionId: %s, Bus: %p)",
            ~StringFromGuid(bus->SessionId),
            ~bus);
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
    , SequenceId(0)
{
    CreateGuid(&SessionId);
}

void TBusClient::TBus::Initialize()
{
    // Cannot do this in ctor since a smartpointer for this is needed.
    MessageRearranger.Reset(new TMessageRearranger(
        FromMethod(&TBus::OnMessageDequeued, TPtr(this)),
        MessageRearrangeTimeout));
}

IBus::TSendResult::TPtr TBusClient::TBus::Send(IMessage::TPtr message)
{
    return TClientDispatcher::Get()->EnqueueRequest(this, message);
}

void TBusClient::TBus::Terminate()
{
    Terminated = true;
    MessageRearranger.Destroy();
    TClientDispatcher::Get()->EnqueueBusUnregister(this);
}

TSequenceId TBusClient::TBus::GenerateSequenceId()
{
    return AtomicIncrement(SequenceId);
}

void TBusClient::TBus::ProcessIncomingMessage(IMessage::TPtr message, TSequenceId sequenceId)
{
    // TODO: rearrangement is switched off, see YT-95
    //MessageRearranger->EnqueueMessage(message, sequenceId);
    Handler->OnMessage(message, this);
}

void TBusClient::TBus::OnMessageDequeued(IMessage::TPtr message)
{
    Handler->OnMessage(message, this);
}

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
    bus->Initialize();
    TClientDispatcher::Get()->EnqueueBusRegister(bus);
    return ~bus;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
