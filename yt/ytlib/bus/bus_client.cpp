#include "bus_client.h"
#include "rpc.pb.h"
#include "message_rearranger.h"
#include "packet.h"

#include "../actions/action_util.h"
#include "../logging/log.h"
#include "../misc/thread_affinity.h"

#include <quality/NetLiba/UdpHttp.h>

#include <util/generic/singleton.h>
#include <util/generic/list.h>
#include <util/generic/utility.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

// TODO: make configurable
static const int MaxNLCallsPerIteration = 10;
static const TDuration ClientSleepQuantum = TDuration::MilliSeconds(10);
static const TDuration MessageRearrangeTimeout = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TBusClient::TBus
    : public IBus
{
public:
    typedef TIntrusivePtr<TBus> TPtr;

    TBus(TBusClient::TPtr client, IMessageHandler::TPtr handler);

    void ProcessIncomingMessage(IMessage::TPtr message, TSequenceId sequenceId);

    virtual TSendResult::TPtr Send(IMessage::TPtr message);
    virtual void Terminate();

    TSequenceId GenerateSequenceId();

private:
    friend class TClientDispatcher;

    typedef yhash_set<TGuid> TRequestIdSet;

    TBusClient::TPtr Client;
    IMessageHandler::TPtr Handler;
    volatile bool Terminated;
    TAtomic SequenceId;
    TSessionId SessionId;
    TRequestIdSet RequestIds;
    TRequestIdSet PingIds;
    THolder<TMessageRearranger> MessageRearranger;
    //! Protects #Terminated.
    TSpinLock SpinLock;

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
            TBlob* data)
            : SessionId(sessionId)
            , Result(New<IBus::TSendResult>())
        {
            Data.swap(*data);
        }

        TSessionId SessionId;
        IBus::TSendResult::TPtr Result;
        TBlob Data;
    };

    typedef yhash_map<TSessionId, TBusClient::TBus::TPtr> TBusMap;
    typedef yhash_map<TGuid, TRequest::TPtr> TRequestMap;

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
        auto* dispatcher = reinterpret_cast<TClientDispatcher*>(param);
        dispatcher->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        while (!Terminated) {
            if (!ProcessBusRegistrations() &&
                !ProcessBusUnregistrations() &&
                !ProcessOutcomingRequests() &&
                !ProcessIncomingNLRequests() &&
                !ProcessIncomingNLResponses())
            {
                LOG_TRACE("Client is idle");
                GetEvent().WaitT(ClientSleepQuantum);
            }
        }
    }

    bool ProcessBusRegistrations()
    {
        LOG_TRACE("Processing client bus registrations");

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
        LOG_TRACE("Processing client bus unregistrations");

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
            ~bus->SessionId.ToString(),
            ~bus);
    }

    void UnregisterBus(TBusClient::TBus::TPtr bus)
    {
        BusMap.erase(bus->SessionId);

        FOREACH(const auto& requestId, bus->RequestIds) {
            Requester->CancelRequest((TGUID) requestId);
            RequestMap.erase(requestId);
        }
        bus->RequestIds.clear();

        TBlob ackData;
        CreatePacket(bus->SessionId, TPacketHeader::EType::Ack, &ackData);

        FOREACH(const TGuid& requestId, bus->PingIds) {
            Requester->SendResponse((TGUID) requestId, &ackData);
        }
        bus->PingIds.clear();

        LOG_DEBUG("Bus is unregistered (SessionId: %s, Bus: %p)",
            ~bus->SessionId.ToString(),
            ~bus);
    }

    bool ProcessIncomingNLResponses()
    {
        LOG_TRACE("Processing incoming client NetLiba responses");

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

    void ProcessIncomingNLResponse(TUdpHttpResponse* nlResponse)
    {
        if (nlResponse->Ok != TUdpHttpResponse::OK)
        {
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
                    ~TGuid(nlResponse->ReqId).ToString(),
                    ~header->Type.ToString());
        }
    }

    void ProcessFailedNLResponse(TUdpHttpResponse* nlResponse)
    {
        TGuid requestId = nlResponse->ReqId;
        auto requestIt = RequestMap.find(requestId);
        if (requestIt == RequestMap.end()) {
            LOG_DEBUG("An obsolete request failed (RequestId: %s)",
                ~requestId.ToString());
            return;
        }

        auto request = requestIt->Second();

        LOG_DEBUG("Request failed (SessionId: %s, RequestId: %s)",
            ~request->SessionId.ToString(),
            ~requestId.ToString());

        request->Result->Set(IBus::ESendResult::Failed);
        RequestMap.erase(requestIt);
    }

    bool ProcessIncomingNLRequests()
    {
        LOG_TRACE("Processing incoming client NetLiba requests");

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

    void ProcessIncomingNLRequest(TUdpHttpRequest* nlRequest)
    {
        auto* header = ParsePacketHeader<TPacketHeader>(nlRequest->Data);
        if (header == NULL)
            return;

        switch (header->Type) {
            case TPacketHeader::EType::Ping:
                ProcessPing(header, nlRequest);
                break;

            case TPacketHeader::EType::Message:
                ProcessMessage(header, nlRequest);
                break;

            default:
                LOG_ERROR("Invalid request packet type (RequestId: %s, Type: %s)",
                    ~TGuid(nlRequest->ReqId).ToString(),
                    ~header->Type.ToString());
            return;
        }
    }

    void ProcessAck(TPacketHeader* header, TUdpHttpResponse* nlResponse)
    {
        TGuid requestId = nlResponse->ReqId;
        auto requestIt = RequestMap.find(requestId);
        if (requestIt == RequestMap.end()) {
            LOG_DEBUG("An obsolete request ack received (SessionId: %s, RequestId: %s)",
                ~header->SessionId.ToString(),
                ~requestId.ToString());
            return;
        }

        LOG_DEBUG("Request ack received (SessionId: %s, RequestId: %s)",
            ~header->SessionId.ToString(),
            ~requestId.ToString());

        TRequest::TPtr request = requestIt->Second();
        request->Result->Set(IBus::ESendResult::OK);
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

    void DoProcessMessage(TPacketHeader* header, const TGuid& requestId, TBlob& data, bool isRequest)
    {
        int dataSize = data.ysize();

        auto busIt = BusMap.find(header->SessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Message for an obsolete session is dropped (SessionId: %s, RequestId: %s, PacketSize: %d)",
                ~header->SessionId.ToString(),
                ~requestId.ToString(),
                dataSize);
            return;
        }

        IMessage::TPtr message;
        TSequenceId sequenceId;;
        if (!DecodeMessagePacket(data, &message, &sequenceId))
            return;

        LOG_DEBUG("Message received (IsRequest: %d, SessionId: %s, RequestId: %s, PacketSize: %d)",
            (int) isRequest,
            ~header->SessionId.ToString(),
            ~requestId.ToString(),
            dataSize);

        auto& bus = busIt->Second();
        bus->ProcessIncomingMessage(message, sequenceId);

        if (!isRequest) {
            RequestMap.erase(requestId);
        } else {
            TBlob ackData;
            CreatePacket(bus->SessionId, TPacketHeader::EType::Ack, &ackData);
            Requester->SendResponse((TGUID) requestId, &ackData);

            LOG_DEBUG("Ack sent (SessionId: %s, RequestId: %s)",
                ~bus->SessionId.ToString(),
                ~requestId.ToString());
        }
    }

    void ProcessPing(TPacketHeader* header, TUdpHttpRequest* nlRequest)
    {
        TGuid requestId = nlRequest->ReqId;
        auto busIt = BusMap.find(header->SessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Ping for an obsolete session received (SessionId: %s, RequestId: %s)",
                ~header->SessionId.ToString(),
                ~requestId.ToString());

            TBlob data;
            CreatePacket(header->SessionId, TPacketHeader::EType::Ack, &data);
            Requester->SendResponse(nlRequest->ReqId, &data);

            return;
        }

        LOG_DEBUG("Ping received (SessionId: %s, RequestId: %s)",
            ~header->SessionId.ToString(),
            ~requestId.ToString());

        // Don't reply to a ping, just register it.
        auto& bus = busIt->Second();
        bus->PingIds.insert(requestId);
    }

    bool ProcessOutcomingRequests()
    {
        LOG_TRACE("Processing outcoming client NetLiba requests");

        int callCount = 0;
        while (callCount < MaxNLCallsPerIteration) {
            TRequest::TPtr request;
            if (!RequestQueue.Dequeue(&request))
                break;

            ++callCount;
            ProcessOutcomingRequest(request);
        }
        return callCount > 0;
    }

    void ProcessOutcomingRequest(TRequest::TPtr request)
    {
        auto busIt = BusMap.find(request->SessionId);
        if (busIt == BusMap.end()) {
            // Process all pending registrations and try once again.
            ProcessBusRegistrations();
            busIt = BusMap.find(request->SessionId);
            if (busIt == BusMap.end()) {
                // Still no luck.
                LOG_DEBUG("Request via an obsolete session is dropped (SessionId: %s, Request: %p)",
                    ~request->SessionId.ToString(),
                    ~request);
                return;
            }
        }

        auto& bus = busIt->Second();

        TGuid requestId = Requester->SendRequest(bus->Client->ServerAddress, "", &request->Data);

        bus->RequestIds.insert(requestId);
        RequestMap.insert(MakePair(requestId, request));

        LOG_DEBUG("Request sent (SessionId: %s, RequestId: %s, Request: %p)",
            ~request->SessionId.ToString(),
            ~requestId.ToString(),
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
        if (~Requester == NULL) {
            throw yexception() << "Failed to create a client NetLiba requester";
        }

        Thread.Start();

        LOG_DEBUG("Client dispatcher is started");
    }

    ~TClientDispatcher()
    {
        Shutdown();
    }

    static TClientDispatcher* Get()
    {
        return Singleton<TClientDispatcher>();
    }

    void Shutdown()
    {
        Terminated = true;
        Thread.Join();

        // NB: This doesn't actually stop NetLiba threads.
        Requester->StopNoWait();

        // TODO: Consider moving somewhere else.
        StopAllNetLibaThreads();

        // NB: cannot use log here
    }

    IBus::TSendResult::TPtr EnqueueRequest(TBusClient::TBus* bus, IMessage::TPtr message)
    {
        auto sequenceId = bus->GenerateSequenceId();

        TBlob data;
        if (!EncodeMessagePacket(message, bus->SessionId, sequenceId, &data))
            throw yexception() << "Failed to encode a message";

        int dataSize = data.ysize();
        auto request = New<TRequest>(bus->SessionId, &data);
        RequestQueue.Enqueue(request);
        GetEvent().Signal();

        LOG_DEBUG("Request enqueued (SessionId: %s, Request: %p, PacketSize: %d)",
            ~bus->SessionId.ToString(),
            ~request,
            dataSize);

        return request->Result;
    }

    void EnqueueBusRegister(TBusClient::TBus::TPtr bus)
    {
        BusRegisterQueue.Enqueue(bus);
        GetEvent().Signal();

        LOG_DEBUG("Bus registration enqueued (SessionId: %s, Bus: %p)",
            ~bus->SessionId.ToString(),
            ~bus);
    }

    void EnqueueBusUnregister(TBusClient::TBus::TPtr bus)
    {
        BusUnregisterQueue.Enqueue(bus);
        GetEvent().Signal();

        LOG_DEBUG("Bus unregistration enqueued (SessionId: %s, Bus: %p)",
            ~bus->SessionId.ToString(),
            ~bus);
    }

    Stroka GetDebugInfo()
    {
        return
            "ClientDispatcher info:\n" + Requester->GetDebugInfo() + "\n" +
            "Pending data size: " + ToString(Requester->GetPendingDataSize());
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: hack
void ShutdownClientDispatcher()
{
    TClientDispatcher::Get()->Shutdown();
}

Stroka GetClientDispatcherDebugInfo()
{
    return TClientDispatcher::Get()->GetDebugInfo();
}

////////////////////////////////////////////////////////////////////////////////

TBusClient::TBus::TBus(TBusClient::TPtr client, IMessageHandler::TPtr handler)
    : Client(client)
    , Handler(handler)
    , Terminated(false)
    , SequenceId(0)
    , MessageRearranger(new TMessageRearranger(
        FromMethod(&TBus::OnMessageDequeued, TPtr(this)),
        MessageRearrangeTimeout))
{
    VERIFY_THREAD_AFFINITY_ANY();

    SessionId = TGuid::Create();
}

IBus::TSendResult::TPtr TBusClient::TBus::Send(IMessage::TPtr message)
{
    VERIFY_THREAD_AFFINITY_ANY();
    // NB: We may actually need a barrier here but
    // since Terminate is used for debugging purposes mainly, we omit it.
    YASSERT(!Terminated);

    return TClientDispatcher::Get()->EnqueueRequest(this, message);
}

void TBusClient::TBus::Terminate()
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (Terminated)
            return;

        Terminated = true;
    }

    MessageRearranger.Destroy();
    TClientDispatcher::Get()->EnqueueBusUnregister(this);
}

TSequenceId TBusClient::TBus::GenerateSequenceId()
{
    return AtomicIncrement(SequenceId);
}

void TBusClient::TBus::ProcessIncomingMessage(IMessage::TPtr message, TSequenceId sequenceId)
{
    UNUSED(sequenceId);
    // TODO: rearrangement is switched off, see YT-95
    // MessageRearranger->EnqueueMessage(message, sequenceId);
    Handler->OnMessage(message, this);
}

void TBusClient::TBus::OnMessageDequeued(IMessage::TPtr message)
{
    Handler->OnMessage(message, this);
}

////////////////////////////////////////////////////////////////////////////////

TBusClient::TBusClient(Stroka address)
{
    VERIFY_THREAD_AFFINITY_ANY();

    ServerAddress = CreateAddress(address, 0);
    if (ServerAddress == TUdpAddress()) {
        throw yexception() << Sprintf("Failed to resolve the address %s",
            ~address.Quote());
    }
}

IBus::TPtr TBusClient::CreateBus(IMessageHandler::TPtr handler)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(~handler != NULL);

    auto bus = New<TBus>(this, handler);
    TClientDispatcher::Get()->EnqueueBusRegister(bus);
    return ~bus;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
