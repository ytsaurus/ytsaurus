#include "stdafx.h"
#include "nl_client.h"
#include "message_rearranger.h"
#include "packet.h"

#include <ytlib/logging/log.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/thread.h>
#include <ytlib/profiling/profiler.h>
#include <ytlib/rpc/rpc.pb.h>

#include <quality/netliba_v6/udp_http.h>

#include <util/thread/lfqueue.h>
#include <util/generic/singleton.h>

namespace NYT {
namespace NBus {

using namespace NNetliba;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bus");
static NProfiling::TProfiler Profiler("/bus/client");

// TODO: make configurable
static const int MaxNLCallsPerIteration = 10;
static const TDuration ClientSleepQuantum = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

class TNLBusClient
    : public IBusClient
{
public:
    typedef TIntrusivePtr<TNLBusClient> TPtr;

    TNLBusClient(TNLBusClientConfig::TPtr config);

    void Start();

    virtual ~TNLBusClient()
    { }

    virtual IBus::TPtr CreateBus(IMessageHandler::TPtr handler);

private:
    class TBus;
    friend class TNLClientManager::TImpl;

    TNLBusClientConfig::TPtr Config;
    TUdpAddress ServerAddress;
};

IBusClient::TPtr CreateNLBusClient(TNLBusClientConfig::TPtr config)
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

    TBus(const TUdpAddress& address, IMessageHandler::TPtr handler)
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

    void ProcessIncomingMessage(IMessage::TPtr message, TSequenceId sequenceId)
    {
        UNUSED(sequenceId);
        Handler->OnMessage(message, this);
    }

    virtual TSendResult Send(IMessage::TPtr message);
    virtual void Terminate();


    void RestartSession()
    {
        TGuard<TSpinLock> guard(SpinLock);
        SessionId = TSessionId::Create();
        SequenceId = 0;
    }

    TSessionId GetSessionId()
    {
        return SessionId;
    }

    void AllocateSequenceId(TSessionId* sessionId, TSequenceId* sequenceId)
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (sessionId) {
            *sessionId = SessionId;
        }
        if (sequenceId) {
            *sequenceId = SequenceId++;
        }
    }
    
    TRequestIdSet& PendingRequestIds()
    {
        return PendingRequestIds_;
    }

private:
    TUdpAddress Address;
    IMessageHandler::TPtr Handler;
    
    volatile bool Terminated;
    TRequestIdSet PendingRequestIds_;

    TSpinLock SpinLock;
    TAtomic SequenceId;
    TSessionId SessionId;
};

////////////////////////////////////////////////////////////////////////////////

class TNLClientManager::TImpl
{
    struct TOutcomingRequest
        : public TIntrinsicRefCounted
    {
        typedef TIntrusivePtr<TOutcomingRequest> TPtr;

        TOutcomingRequest(
            const TSessionId& sessionId,
            TSequenceId sequenceId,
            const TGuid& requestId,
            IMessage::TPtr message)
            : SessionId(sessionId)
            , SequenceId(sequenceId)
            , RequestId(requestId)
            , Message(MoveRV(message))
            , Promise(NewPromise<IBus::TSendPromise::TValueType>())
        { }

        TSessionId SessionId;
        TSequenceId SequenceId;
        TGuid RequestId;
        IMessage::TPtr Message;
        IBus::TSendPromise Promise;
    };

    TThread Thread;
    volatile bool Terminated;
    NProfiling::TRateCounter InCounter;
    NProfiling::TRateCounter InSizeCounter;
    NProfiling::TRateCounter OutCounter;
    NProfiling::TRateCounter OutSizeCounter;

    // IRequester has to be stored by Arcadia's IntrusivePtr.
    ::TIntrusivePtr<IRequester> Requester;

    typedef yhash_map<TSessionId, TNLBusClient::TBus::TPtr> TBusMap;
    TBusMap BusMap;

    typedef yhash_map<TGuid, TOutcomingRequest::TPtr> TRequestMap;
    TRequestMap RequestMap;

    TLockFreeQueue<TOutcomingRequest::TPtr> OutcomingRequestQueue;
    yhash_map<TGuid, TUdpHttpResponse*> UnmatchedResponseMap;
    TLockFreeQueue<TNLBusClient::TBus::TPtr> BusRegisterQueue;
    TLockFreeQueue<TNLBusClient::TBus::TPtr> BusUnregisterQueue;

    static void* ThreadFunc(void* param)
    {
        auto* impl = reinterpret_cast<TImpl*>(param);
        impl->ThreadMain();
        return NULL;
    }

    void ThreadMain()
    {
        NThread::SetCurrentThreadName("BusClient");
        while (!Terminated) {
            ProcessBusRegistrations();
            ProcessBusUnregistrations();
            ProcessOutcomingRequests();

            // NB: "&", not "&&" since we want every type of processing to happen on each iteration.
            if (!ProcessIncomingNLRequests() &
                !ProcessIncomingNLResponses())
            {
                LOG_TRACE("Client is idle");
                Requester->GetAsyncEvent().WaitT(ClientSleepQuantum);
            }
        }
    }


    void ProcessBusRegistrations()
    {
        LOG_TRACE("Processing client bus registrations");

        TNLBusClient::TBus::TPtr bus;
        while (BusRegisterQueue.Dequeue(&bus)) {
            ProcessBusRegistration(bus);
        }
    }

    void ProcessBusRegistration(TNLBusClient::TBus::TPtr bus)
    {
        const auto& sessionId = bus->GetSessionId();
        BusMap.insert(MakePair(sessionId, bus));
        LOG_DEBUG("Bus is registered (SessionId: %s, Bus: %p, Address: %s)",
            ~sessionId.ToString(),
            ~bus,
            ~GetAddressAsString(bus->GetAddress()));
    }


    void ProcessBusUnregistrations()
    {
        LOG_TRACE("Processing client bus unregistrations");

        TNLBusClient::TBus::TPtr bus;
        while (BusUnregisterQueue.Dequeue(&bus)) {
            ProcessBusUnregistration(bus);
        }
    }

    void ProcessBusUnregistration(TNLBusClient::TBus::TPtr bus)
    {
        const auto& sessionId = bus->GetSessionId();

        LOG_DEBUG("Bus is unregistered (SessionId: %s, Bus: %p)",
            ~sessionId.ToString(),
            ~bus);

        FOREACH (const auto& requestId, bus->PendingRequestIds()) {
            Requester->CancelRequest((TGUID) requestId);
            auto requestIt = FindRequest(requestId);
            YASSERT(requestIt != RequestMap.end());
            RequestMap.erase(requestIt);
        }
        bus->PendingRequestIds().clear();

        TBlob ackData;
        CreatePacket(sessionId, TPacketHeader::EType::Ack, &ackData);

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
            ProcessIncomingNLResponse(nlResponse);
        }
        return callCount > 0;
    }

    void ProcessIncomingNLResponse(TAutoPtr<TUdpHttpResponse> nlResponse)
    {
        TGuid requestId = nlResponse->ReqId;
        auto requestIt = FindRequest(requestId);
        if (requestIt == RequestMap.end()) {
            LOG_DEBUG("Response is unmatched (RequestId: %s)",
                ~requestId.ToString());
            YVERIFY(UnmatchedResponseMap.insert(MakePair(requestId, nlResponse.Release())).second);
            return;
        }

        if (nlResponse->Ok != TUdpHttpResponse::OK)
        {
            ProcessFailedNLResponse(requestIt, nlResponse);
            return;
        }

        auto* header = ParsePacketHeader<TPacketHeader>(nlResponse->Data);
        if (!header)
            return;

        switch (header->Type) {
            case TPacketHeader::EType::Ack:
                ProcessAck(header, requestIt, nlResponse);
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

    void ProcessFailedNLResponse(TRequestMap::iterator requestIt, TAutoPtr<TUdpHttpResponse> nlResponse)
    {
        auto& request = requestIt->second;
        const auto& sessionId = request->SessionId;
        const auto& requestId = request->RequestId;
        auto busIt = FindBus(sessionId);
        YASSERT(busIt != BusMap.end());
        auto& bus = busIt->second;

        LOG_DEBUG("Request failed (SessionId: %s, RequestId: %s)",
            ~sessionId.ToString(),
            ~requestId.ToString());

        request->Promise.Set(ESendResult::Failed);

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
            ProcessIncomingNLRequest(nlRequest);
        }
        return callCount > 0;
    }

    void ProcessIncomingNLRequest(TAutoPtr<TUdpHttpRequest> nlRequest)
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
                    ~TGuid(nlRequest->ReqId).ToString(),
                    ~header->Type.ToString());
            return;
        }
    }

    void ProcessAck(TPacketHeader* header, TRequestMap::iterator requestIt, TAutoPtr<TUdpHttpResponse> nlResponse)
    {
        auto& request = requestIt->second;
        const auto& sessionId = header->SessionId;
        const auto& requestId = request->RequestId;

        auto busIt = FindBus(sessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Ack for obsolete request received (SessionId: %s, RequestId: %s)",
                ~header->SessionId.ToString(),
                ~requestId.ToString());
            return;
        }

        LOG_DEBUG("Request ack received (SessionId: %s, RequestId: %s)",
            ~header->SessionId.ToString(),
            ~requestId.ToString());

        auto& bus = busIt->second;

        request->Promise.Set(ESendResult::OK);

        YVERIFY(bus->PendingRequestIds().erase(requestId) == 1);
        RequestMap.erase(requestIt);
    }

    void ProcessBrokenSession(TPacketHeader* header, TAutoPtr<TUdpHttpResponse> nlResponse)
    {
        const auto& oldSessionId = header->SessionId;
        TGuid brokenRequestId = nlResponse->ReqId;
        auto busIt = FindBus(oldSessionId);
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

        std::vector<TOutcomingRequest*> pendingRequests;
        pendingRequests.reserve(bus->PendingRequestIds().size());
        FOREACH (const auto& requestId, bus->PendingRequestIds()) {
            auto pendingRequestIt = FindRequest(requestId);
            YASSERT(pendingRequestIt != RequestMap.end());
            pendingRequests.push_back(~pendingRequestIt->second);
        }

        std::sort(
            pendingRequests.begin(),
            pendingRequests.end(),
            [] (TOutcomingRequest* lhs, TOutcomingRequest* rhs) { return lhs->SequenceId < rhs->SequenceId; });

        bus->PendingRequestIds().clear();

        for (int index = 0; index < static_cast<int>(pendingRequests.size()); ++index) {
            auto request = pendingRequests[index];

            auto oldSequenceId = request->SequenceId;
            auto oldRequestId = request->RequestId;

            TSequenceId newSequenceId;
            bus->AllocateSequenceId(NULL, &newSequenceId);

            TBlob data;
            if (!EncodeMessagePacket(request->Message, newSessionId, newSequenceId, &data)) {
                LOG_FATAL("Failed to encode a message");
            }
            Requester->CancelRequest(oldRequestId);
            ProfileOut(data.size());
            TGuid newRequestId = Requester->SendRequest(bus->GetAddress(), "", &data);

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

    void ProcessMessage(TPacketHeader* header, TAutoPtr<TUdpHttpRequest> nlRequest)
    {
        DoProcessMessage(header, nlRequest->ReqId, MoveRV(nlRequest->Data), true);
    }

    void ProcessMessage(TPacketHeader* header, TAutoPtr<TUdpHttpResponse> nlResponse)
    {
        DoProcessMessage(header, nlResponse->ReqId, MoveRV(nlResponse->Data), false);
    }

    void DoProcessMessage(TPacketHeader* header, const TGuid& requestId, TBlob&& data, bool isRequest)
    {
        size_t size = data.size();
        const auto& sessionId = header->SessionId;

        ProfileIn(size);

        auto busIt = FindBus(sessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Message for an obsolete session is dropped (SessionId: %s, RequestId: %s, Size: %" PRISZT ")",
                ~sessionId.ToString(),
                ~requestId.ToString(),
                size);
            return;
        }

        IMessage::TPtr message;
        TSequenceId sequenceId;;
        if (!DecodeMessagePacket(MoveRV(data), &message, &sequenceId)) {
            LOG_WARNING("Error parsing message packet (RequestId: %s)", ~requestId.ToString());
            return;
        }

        LOG_DEBUG("Message received (IsRequest: %d, SessionId: %s, RequestId: %s, SequenceId: %" PRId64 ", Size: %" PRISZT ")",
            (int) isRequest,
            ~sessionId.ToString(),
            ~requestId.ToString(),
            sequenceId,
            size);

        auto& bus = busIt->second;
        bus->ProcessIncomingMessage(message, sequenceId);

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


    void ProcessOutcomingRequests()
    {
        LOG_TRACE("Processing outcoming client NetLiba requests");

        TOutcomingRequest::TPtr request;
        while (OutcomingRequestQueue.Dequeue(&request)) {
            ProcessOutcomingRequest(request);
        }
    }

    void ProcessOutcomingRequest(TOutcomingRequest::TPtr request)
    {
        auto busIt = FindBus(request->SessionId);
        if (busIt == BusMap.end()) {
            LOG_DEBUG("Outcoming request via an obsolete session is dropped (RequestId: %s)",
                ~request->RequestId.ToString());
            return;
        }

        auto& bus = busIt->second;
        const auto& requestId = request->RequestId;
        bus->PendingRequestIds().insert(requestId);
        RequestMap.insert(MakePair(requestId, request));

        auto unmatchedIt = UnmatchedResponseMap.find(requestId);
        if (unmatchedIt == UnmatchedResponseMap.end()) {
            LOG_DEBUG("Outcoming request registered (RequestId: %s)",
                ~requestId.ToString());
        } else {
            LOG_DEBUG("Outcoming request matched (RequestId: %s)",
                ~requestId.ToString());

            TAutoPtr<TUdpHttpResponse> nlResponse(unmatchedIt->second);
            UnmatchedResponseMap.erase(unmatchedIt);

            ProcessIncomingNLResponse(nlResponse);
        }
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


    TBusMap::iterator FindBus(const TSessionId& sessionId)
    {
        auto it = BusMap.find(sessionId);
        if (it == BusMap.end()) {
            ProcessBusRegistrations();
            it = BusMap.find(sessionId);
        }
        return it;
    }

    TRequestMap::iterator FindRequest(const TGuid& requestId)
    {
        auto it = RequestMap.find(requestId);
        if (it == RequestMap.end()) {
            ProcessOutcomingRequests();
            it = RequestMap.find(requestId);
        }
        return it;
    }

public:
    TImpl()
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

    ~TImpl()
    {
        Shutdown();

        FOREACH (const auto& pair, UnmatchedResponseMap) {
            delete pair.second;
        }
    }

    void Shutdown()
    {
        if (!Requester)
            return;

        Terminated = true;
        Thread.Join();

        // XXX(babenko): just drop the reference, this should force the requester
        // to send all pending packets.
        Requester->StopNoWait();
        Requester = NULL;

        // NB: Cannot use log here!
    }

    IBus::TSendResult SendRequest(TNLBusClient::TBus::TPtr bus, IMessage::TPtr message)
    {
        TSessionId sessionId;
        TSequenceId sequenceId;
        bus->AllocateSequenceId(&sessionId, &sequenceId);

        TBlob data;
        if (!EncodeMessagePacket(message, sessionId, sequenceId, &data)) {
            LOG_FATAL("Failed to encode a message");
        }

        size_t size = data.size();
        ProfileOut(size);

        TGuid requestId = Requester->SendRequest(bus->GetAddress(), "", &data);

        auto request = New<TOutcomingRequest>(
            sessionId,
            sequenceId,
            requestId,
            message);
        OutcomingRequestQueue.Enqueue(request);

        LOG_DEBUG("Request sent (SessionId: %s, RequestId: %s, SequenceId: %" PRId64 ", Size: %" PRISZT ")",
            ~request->SessionId.ToString(),
            ~requestId.ToString(),
            request->SequenceId,
            size);

        return request->Promise;
    }

    void RegisterBus(TNLBusClient::TBus::TPtr bus)
    {
        const auto& sessionId = bus->GetSessionId();
        BusRegisterQueue.Enqueue(bus);

        LOG_DEBUG("Bus registration enqueued (SessionId: %s, Bus: %p)",
            ~sessionId.ToString(),
            ~bus);
    }

    void UnregisterBus(TNLBusClient::TBus::TPtr bus)
    {
        const auto& sessionId = bus->GetSessionId();
        BusUnregisterQueue.Enqueue(bus);
        
        LOG_DEBUG("Bus unregistration enqueued (SessionId: %s, Bus: %p)",
            ~sessionId.ToString(),
            ~bus);
    }
};

////////////////////////////////////////////////////////////////////////////////

TNLClientManager::TNLClientManager()
    : Impl(new TImpl())
{ }

TNLClientManager* TNLClientManager::Get()
{
    return Singleton<TNLClientManager>();
}

void TNLClientManager::Shutdown()
{
    Impl->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

TNLBusClient::TNLBusClient(TNLBusClientConfig::TPtr config)
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

IBus::TPtr TNLBusClient::CreateBus(IMessageHandler::TPtr handler)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(handler);

    auto bus = New<TBus>(ServerAddress, handler);
    TNLClientManager::Get()->Impl->RegisterBus(bus);
    return bus;
}

////////////////////////////////////////////////////////////////////////////////

IBus::TSendResult TNLBusClient::TBus::Send(IMessage::TPtr message)
{
    VERIFY_THREAD_AFFINITY_ANY();
    // NB: We may actually need a barrier here but
    // since Terminate is used for debugging purposes mainly, we omit it.
    YASSERT(!Terminated);

    return TNLClientManager::Get()->Impl->SendRequest(this, message);
}

void TNLBusClient::TBus::Terminate()
{
    if (Terminated)
        return;

    Terminated = true;

    TNLClientManager::Get()->Impl->UnregisterBus(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
