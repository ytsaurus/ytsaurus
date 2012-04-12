#include "stdafx.h"
#include "nl_client.h"
#include <ytlib/rpc/rpc.pb.h>
#include "message_rearranger.h"
#include "packet.h"

#include <ytlib/logging/log.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/profiling/profiler.h>

#include <quality/netliba_v6/udp_http.h>

#include <util/thread/lfqueue.h>

namespace NYT {
namespace NBus {

using namespace NNetliba;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bus");
static NProfiling::TProfiler Profiler("/bus/client");

// TODO: make configurable
static const int MaxNLCallsPerIteration = 10;
static const TDuration ClientSleepQuantum = TDuration::MilliSeconds(10);
static const TDuration MessageRearrangeTimeout = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TClientDispatcher;

class TNLBusClient
    : public IBusClient
{
public:
    typedef TIntrusivePtr<TNLBusClient> TPtr;

    TNLBusClient(TNLBusClientConfig* config);

    void Start();

    virtual ~TNLBusClient()
    { }

    virtual IBus::TPtr CreateBus(IMessageHandler* handler);

private:
    class TBus;
    friend class TClientDispatcher;

    TNLBusClientConfig::TPtr Config;
    TUdpAddress ServerAddress;
};

IBusClient::TPtr CreateNLBusClient(TNLBusClientConfig* config)
{
    auto client = New<TNLBusClient>(config);
    client->Start();
    return client;
}

////////////////////////////////////////////////////////////////////////////////

class TNLBusClient::TBus
    : public IBus
{
public:
    typedef TIntrusivePtr<TBus> TPtr;
    typedef yhash_set<TGuid> TRequestIdSet;

    TBus(const TUdpAddress& address, IMessageHandler* handler)
        : Address(address)
        , Handler(handler)
        , Terminated(false)
    {
        RestartSession();
    }


    TUdpAddress GetAddress()
    {
        return Address;
    }

    void ProcessIncomingMessage(IMessage* message, TSequenceId sequenceId)
    {
        UNUSED(sequenceId);
        Handler->OnMessage(message, this);
    }

    virtual TSendResult::TPtr Send(IMessage::TPtr message);
    virtual void Terminate();


    void RestartSession()
    {
        SequenceId = 0;
        SessionId = TSessionId::Create();
    }

    const TSessionId& GetSessionId()
    {
        return SessionId;
    }


    TSequenceId GenerateSequenceId()
    {
        return SequenceId++;
    }


    TRequestIdSet& PendingRequestIds()
    {
        return PendingRequestIds_;
    }

    TRequestIdSet& PingIds()
    {
        return PingIds_;
    }

private:
    TUdpAddress Address;
    IMessageHandler::TPtr Handler;
    volatile bool Terminated;
    TSequenceId SequenceId;
    TSessionId SessionId;
    TRequestIdSet PendingRequestIds_;
    TRequestIdSet PingIds_;

};

////////////////////////////////////////////////////////////////////////////////

class TClientDispatcher
{
    struct TRequest
        : public TIntrinsicRefCounted
    {
        typedef TIntrusivePtr<TRequest> TPtr;

        TRequest(
            const TSessionId& sessionId,
            IMessage* message,
            TBlob&& data,
            TSequenceId sequenceId)
            : SessionId(sessionId)
            , Message(message)
            , Result(New<IBus::TSendResult>())
            , SequenceId(sequenceId)
        {
            // TODO(babenko): replace with movement ctor
            Data.swap(data);
        }

        TSessionId SessionId;
        TGuid RequestId;
        IMessage::TPtr Message;
        IBus::TSendResult::TPtr Result;
        TBlob Data;
        TSequenceId SequenceId;
    };

    TThread Thread;
    volatile bool Terminated;
    NProfiling::TRateCounter InCounter;
    NProfiling::TRateCounter InSizeCounter;
    NProfiling::TRateCounter OutCounter;
    NProfiling::TRateCounter OutSizeCounter;

    // IRequester has to be stored by Arcadia's IntrusivePtr.
    ::TIntrusivePtr<IRequester> Requester;

    yhash_map<TSessionId, TNLBusClient::TBus::TPtr> BusMap;
    yhash_map<TGuid, TRequest::TPtr> RequestMap;

    TLockFreeQueue<TRequest::TPtr> RequestQueue;
    TLockFreeQueue<TNLBusClient::TBus::TPtr> BusRegisterQueue;
    TLockFreeQueue<TNLBusClient::TBus::TPtr> BusUnregisterQueue;

    static void* ThreadFunc(void* param)
    {
        auto* dispatcher = reinterpret_cast<TClientDispatcher*>(param);
        dispatcher->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        while (!Terminated) {
            // NB: "&", not "&&" since we want every type of processing to happen on each iteration.
            if (!ProcessBusRegistrations() &
                !ProcessBusUnregistrations() &
                !ProcessOutcomingRequests() &
                !ProcessIncomingNLRequests() &
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
        TNLBusClient::TBus::TPtr bus;
        while (BusRegisterQueue.Dequeue(&bus)) {
            result = true;
            RegisterBus(~bus);
        }
        return result;
    }

    bool ProcessBusUnregistrations()
    {
        LOG_TRACE("Processing client bus unregistrations");

        bool result = false;
        TNLBusClient::TBus::TPtr bus;
        while (BusUnregisterQueue.Dequeue(&bus)) {
            result = true;
            UnregisterBus(~bus);
        }
        return result;
    }

    void RegisterBus(TNLBusClient::TBus* bus)
    {
        const auto& sessionId = bus->GetSessionId();
        BusMap.insert(MakePair(sessionId, bus));
        LOG_DEBUG("Bus is registered (SessionId: %s, Bus: %p, Address: %s)",
            ~sessionId.ToString(),
            bus,
            ~GetAddressAsString(bus->GetAddress()));
    }

    void UnregisterBus(TNLBusClient::TBus* bus)
    {
        const auto& sessionId = bus->GetSessionId();

        LOG_DEBUG("Bus is unregistered (SessionId: %s, Bus: %p)",
            ~sessionId.ToString(),
            bus);

        FOREACH(const auto& requestId, bus->PendingRequestIds()) {
            Requester->CancelRequest((TGUID) requestId);
            RequestMap.erase(requestId);
        }
        bus->PendingRequestIds().clear();

        TBlob ackData;
        CreatePacket(sessionId, TPacketHeader::EType::Ack, &ackData);

        FOREACH(const auto& requestId, bus->PingIds()) {
            Requester->SendResponse(requestId, &ackData);
        }
        bus->PingIds().clear();

        YVERIFY(BusMap.erase(sessionId) == 1);
    }

    bool ProcessIncomingNLResponses()
    {
        LOG_TRACE("Processing incoming client NetLiba responses");

        int callCount = 0;
        while (callCount < MaxNLCallsPerIteration) {
            TAutoPtr<TUdpHttpResponse> nlResponse = Requester->GetResponse();
            if (!nlResponse)
                break;

            ++callCount;
            ProcessIncomingNLResponse(~nlResponse);
        }
        return callCount > 0;
    }

    void ProcessIncomingNLResponse(NNetliba::TUdpHttpResponse* nlResponse)
    {
        if (nlResponse->Ok != TUdpHttpResponse::OK)
        {
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
                //ProcessAck(header, nlResponse);
                ProcessMessage(header, nlResponse);
                break;

            case TPacketHeader::EType::BrokenSession:
                ProcessBrokenSession(header, nlResponse);
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

        auto& request = requestIt->second;
        const auto& sessionId = request->SessionId;
        auto busIt = BusMap.find(sessionId);
        YASSERT(busIt != BusMap.end());
        auto& bus = busIt->second;

        LOG_DEBUG("Request failed (SessionId: %s, RequestId: %s)",
            ~sessionId.ToString(),
            ~requestId.ToString());

        request->Result->Set(ESendResult::Failed);

        YVERIFY(bus->PendingRequestIds().erase(requestId) == 1);
        RequestMap.erase(requestIt);
    }

    bool ProcessIncomingNLRequests()
    {
        LOG_TRACE("Processing incoming client NetLiba requests");

        int callCount = 0;
        while (callCount < MaxNLCallsPerIteration) {
            TAutoPtr<TUdpHttpRequest> nlRequest = Requester->GetRequest();
            if (!nlRequest)
                break;

            ++callCount;
            ProcessIncomingNLRequest(~nlRequest);
        }
        return callCount > 0;
    }

    void ProcessIncomingNLRequest(TUdpHttpRequest* nlRequest)
    {
        auto* header = ParsePacketHeader<TPacketHeader>(nlRequest->Data);
        if (!header)
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
        const auto& sessionId = header->SessionId;
        TGuid requestId = nlResponse->ReqId;
        auto requestIt = RequestMap.find(requestId);
        auto busIt = BusMap.find(sessionId);
        if (requestIt == RequestMap.end() || busIt == BusMap.end()) {
            LOG_DEBUG("Ack for obsolete request received (SessionId: %s, RequestId: %s)",
                ~header->SessionId.ToString(),
                ~requestId.ToString());
            return;
        }

        LOG_DEBUG("Request ack received (SessionId: %s, RequestId: %s)",
            ~header->SessionId.ToString(),
            ~requestId.ToString());

        auto& bus = busIt->second; 
        auto& request = requestIt->second;

        request->Result->Set(ESendResult::OK);

        YVERIFY(bus->PendingRequestIds().erase(requestId) == 1);
        RequestMap.erase(requestIt);
    }

    void ProcessBrokenSession(TPacketHeader* header, TUdpHttpResponse* nlResponse)
    {
        const auto& oldSessionId = header->SessionId;
        TGuid brokenRequestId = nlResponse->ReqId;
        auto busIt = BusMap.find(oldSessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Broken session reply for obsolete session received (SessionId: %s, RequestId: %s)",
                ~oldSessionId.ToString(),
                ~brokenRequestId.ToString());
            return;
        }

        auto bus = busIt->second; 
        bus->RestartSession();
        const auto& newSessionId = bus->GetSessionId();
        YVERIFY(BusMap.insert(MakePair(newSessionId, bus)).second);
        BusMap.erase(busIt);
         
        LOG_DEBUG("Broken session reply received, resending %d requests (SessionId: %s, RequestId: %s, NewSessionId: %s)",
            static_cast<int>(bus->PendingRequestIds().size()),
            ~oldSessionId.ToString(),
            ~brokenRequestId.ToString(),
            ~newSessionId.ToString());

        std::vector<TRequest*> pendingRequests;
        pendingRequests.reserve(bus->PendingRequestIds().size());
        FOREACH (const auto& requestId, bus->PendingRequestIds()) {
            auto pendingRequestIt = RequestMap.find(requestId);
            YASSERT(pendingRequestIt != RequestMap.end());
            pendingRequests.push_back(~pendingRequestIt->second);
        }

        std::sort(
            pendingRequests.begin(),
            pendingRequests.end(),
            [] (TRequest* lhs, TRequest* rhs) { return lhs->SequenceId < rhs->SequenceId; });

        bus->PendingRequestIds().clear();

        for (int index = 0; index < static_cast<int>(pendingRequests.size()); ++index) {
            auto request = pendingRequests[index];

            auto oldSequenceId = request->SequenceId;
            auto oldRequestId = request->RequestId;

            auto newSequenceId = bus->GenerateSequenceId();
            YVERIFY(EncodeMessagePacket(~request->Message, newSessionId, newSequenceId, &request->Data));
            Requester->CancelRequest(oldRequestId);
            ProfileOut(request->Data.size());
            TGuid newRequestId = Requester->SendRequest(bus->GetAddress(), "", &request->Data);

            request->SequenceId = newSequenceId;
            request->RequestId = newRequestId;
            request->SessionId = newSessionId;

            YVERIFY(RequestMap.insert(MakePair(newRequestId, request)).second);
            YVERIFY(RequestMap.erase(oldRequestId) == 1);
            YVERIFY(bus->PendingRequestIds().insert(newRequestId).second);

            LOG_DEBUG("Request resent (SessionId: %s, OldRequestId: %s, OldSequenceId: %" PRId64 ", NewRequestId: %s, NewSequenceId: %" PRId64 ")",
                ~request->SessionId.ToString(),
                ~oldRequestId.ToString(),
                oldSequenceId,
                ~newRequestId.ToString(),
                newSequenceId);        
        }
    }

    void ProcessMessage(TPacketHeader* header, TUdpHttpRequest* nlRequest)
    {
        DoProcessMessage(header, nlRequest->ReqId, MoveRV(nlRequest->Data), true);
    }

    void ProcessMessage(TPacketHeader* header, TUdpHttpResponse* nlResponse)
    {
        DoProcessMessage(header, nlResponse->ReqId, MoveRV(nlResponse->Data), false);
    }

    void DoProcessMessage(TPacketHeader* header, const TGuid& requestId, TBlob&& data, bool isRequest)
    {
        int dataSize = data.ysize();
        const auto& sessionId = header->SessionId;

        ProfileIn(dataSize);

        auto busIt = BusMap.find(sessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Message for an obsolete session is dropped (SessionId: %s, RequestId: %s, PacketSize: %d)",
                ~sessionId.ToString(),
                ~requestId.ToString(),
                dataSize);
            return;
        }

        IMessage::TPtr message;
        TSequenceId sequenceId;;
        if (!DecodeMessagePacket(MoveRV(data), &message, &sequenceId)) {
            LOG_WARNING("Error parsing message packet (RequestId: %s)", ~requestId.ToString());
            return;
        }

        LOG_DEBUG("Message received (IsRequest: %d, SessionId: %s, RequestId: %s, SequenceId: %" PRId64 ", PacketSize: %d)",
            (int) isRequest,
            ~sessionId.ToString(),
            ~requestId.ToString(),
            sequenceId,
            dataSize);

        auto& bus = busIt->second;
        bus->ProcessIncomingMessage(~message, sequenceId);

        if (!isRequest) {
            YVERIFY(bus->PendingRequestIds().erase(requestId) == 1);
            RequestMap.erase(requestId);
        } else {
            TBlob ackData;
            CreatePacket(sessionId, TPacketHeader::EType::Ack, &ackData);
            Requester->SendResponse((TGUID) requestId, &ackData);

            LOG_DEBUG("Ack sent (SessionId: %s, RequestId: %s)",
                ~sessionId.ToString(),
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
        auto& bus = busIt->second;
        bus->PingIds().insert(requestId);
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

        auto& bus = busIt->second;

        ProfileOut(request->Data.size());
        TGuid requestId = Requester->SendRequest(bus->GetAddress(), "", &request->Data);
        request->RequestId = requestId;
        bus->PendingRequestIds().insert(requestId);
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

    void ProfileIn(int size)
    {
        Profiler.Increment(InCounter);
        Profiler.Increment(InSizeCounter, size);
    }

    void ProfileOut(int size)
    {
        Profiler.Increment(OutCounter);
        Profiler.Increment(OutSizeCounter, size);
    }

public:
    TClientDispatcher()
        : Thread(ThreadFunc, (void*) this)
        , Terminated(false)
        , InCounter("/in_rate")
        , InSizeCounter("/in_throughput")
        , OutCounter("/out_rate")
        , OutSizeCounter("/out_throughput")
    {
        Requester = CreateHttpUdpRequester(0);
        if (!Requester) {
            ythrow yexception() << "Failed to create a client NetLiba requester";
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

    IBus::TSendResult::TPtr EnqueueRequest(TNLBusClient::TBus* bus, IMessage* message)
    {
        auto sequenceId = bus->GenerateSequenceId();
        const auto& sessionId = bus->GetSessionId();

        TBlob data;
        if (!EncodeMessagePacket(message, sessionId, sequenceId, &data))
            ythrow yexception() << "Failed to encode a message";

        int dataSize = data.ysize();
        auto request = New<TRequest>(sessionId, message, MoveRV(data), sequenceId);
        RequestQueue.Enqueue(request);
        GetEvent().Signal();

        LOG_DEBUG("Request enqueued (SessionId: %s, Request: %p, SequenceId: %" PRId64 ", PacketSize: %d)",
            ~sessionId.ToString(),
            ~request,
            sequenceId,
            dataSize);

        return request->Result;
    }

    void EnqueueBusRegister(const TNLBusClient::TBus::TPtr& bus)
    {
        const auto& sessionId = bus->GetSessionId();

        BusRegisterQueue.Enqueue(bus);
        GetEvent().Signal();

        LOG_DEBUG("Bus registration enqueued (SessionId: %s, Bus: %p)",
            ~sessionId.ToString(),
            ~bus);
    }

    void EnqueueBusUnregister(const TNLBusClient::TBus::TPtr& bus)
    {
        const auto& sessionId = bus->GetSessionId();

        BusUnregisterQueue.Enqueue(bus);
        GetEvent().Signal();

        LOG_DEBUG("Bus unregistration enqueued (SessionId: %s, Bus: %p)",
            ~sessionId.ToString(),
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

TNLBusClient::TNLBusClient(TNLBusClientConfig* config)
    : Config(config)
{ }

void TNLBusClient::Start()
{
    ServerAddress = CreateAddress(Config->Address, 0);
    if (ServerAddress == TUdpAddress()) {
        ythrow yexception() << Sprintf("Failed to resolve the address %s",
            ~Config->Address.Quote());
    }
}

IBus::TPtr TNLBusClient::CreateBus(IMessageHandler* handler)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(handler);

    auto bus = New<TBus>(ServerAddress, handler);
    TClientDispatcher::Get()->EnqueueBusRegister(bus);
    return bus;
}

////////////////////////////////////////////////////////////////////////////////

IBus::TSendResult::TPtr TNLBusClient::TBus::Send(IMessage::TPtr message)
{
    VERIFY_THREAD_AFFINITY_ANY();
    // NB: We may actually need a barrier here but
    // since Terminate is used for debugging purposes mainly, we omit it.
    YASSERT(!Terminated);

    return TClientDispatcher::Get()->EnqueueRequest(this, ~message);
}

void TNLBusClient::TBus::Terminate()
{
    if (Terminated)
        return;

    Terminated = true;

    TClientDispatcher::Get()->EnqueueBusUnregister(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
