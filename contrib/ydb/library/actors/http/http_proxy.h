#pragma once
#include <contrib/ydb/library/actors/core/actorsystem.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/event_local.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/interconnect/poller_actor.h>
#include <library/cpp/dns/cache.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <util/generic/variant.h>
#include "http.h"
#include "http_proxy_sock64.h"
#include "http_proxy_ssl.h"

namespace NHttp {

const ui32 DEFAULT_MAX_RECYCLED_REQUESTS_COUNT = 1000;

struct TSocketDescriptor : NActors::TSharedDescriptor, THttpConfig {
    SocketType Socket;

    TSocketDescriptor() = default;

    TSocketDescriptor(int af)
        : Socket(af)
    {
    }

    TSocketDescriptor(SocketType&& s)
        : Socket(std::move(s))
    {}

    int GetDescriptor() override {
        return static_cast<SOCKET>(Socket);
    }
};

struct TEvHttpProxy {
    enum EEv {
        EvAddListeningPort = EventSpaceBegin(NActors::TEvents::ES_HTTP),
        EvConfirmListen,
        EvRegisterHandler,
        EvHttpIncomingRequest,
        EvHttpOutgoingRequest,
        EvHttpIncomingResponse,
        EvHttpIncompleteIncomingResponse,
        EvHttpOutgoingResponse,
        EvHttpIncomingConnectionClosed,
        EvHttpOutgoingConnectionClosed,
        EvHttpOutgoingConnectionAvailable,
        EvHttpAcceptorClosed,
        EvResolveHostRequest,
        EvResolveHostResponse,
        EvReportSensors,
        EvHttpIncomingDataChunk,
        EvHttpOutgoingDataChunk,
        EvSubscribeForCancel,
        EvRequestCancelled,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_HTTP), "ES_HTTP event space is too small.");

    struct TEvAddListeningPort : NActors::TEventLocal<TEvAddListeningPort, EvAddListeningPort> {
        TString Address;
        TIpPort Port;
        TString WorkerName;
        bool Secure = false;
        TString CertificateFile;
        TString PrivateKeyFile;
        TString SslCertificatePem;
        std::vector<TString> CompressContentTypes;
        ui32 MaxRequestsPerSecond = 0;
        ui32 MaxRecycledRequestsCount = DEFAULT_MAX_RECYCLED_REQUESTS_COUNT;
        TDuration InactivityTimeout = TDuration::Minutes(2);

        TEvAddListeningPort() = default;

        TEvAddListeningPort(TIpPort port)
            : Port(port)
        {}

        TEvAddListeningPort(TIpPort port, const TString& workerName)
            : Port(port)
            , WorkerName(workerName)
        {}
    };

    struct TEvConfirmListen : NActors::TEventLocal<TEvConfirmListen, EvConfirmListen> {
        THttpConfig::SocketAddressType Address;
        std::shared_ptr<THttpEndpointInfo> Endpoint;

        TEvConfirmListen(const THttpConfig::SocketAddressType& address, std::shared_ptr<THttpEndpointInfo> endpoint)
            : Address(address)
            , Endpoint(std::move(endpoint))
        {}
    };

    struct TEvRegisterHandler : NActors::TEventLocal<TEvRegisterHandler, EvRegisterHandler> {
        TString Path;
        TActorId Handler;

        TEvRegisterHandler(const TString& path, const TActorId& handler)
            : Path(path)
            , Handler(handler)
        {}
    };

    struct TEvHttpIncomingRequest : NActors::TEventLocal<TEvHttpIncomingRequest, EvHttpIncomingRequest> {
        THttpIncomingRequestPtr Request;
        TString UserToken; // built and serialized

        TEvHttpIncomingRequest(THttpIncomingRequestPtr request)
            : Request(std::move(request))
        {}
    };

    struct TEvHttpOutgoingRequest : NActors::TEventLocal<TEvHttpOutgoingRequest, EvHttpOutgoingRequest> {
        THttpOutgoingRequestPtr Request;
        TDuration Timeout;
        bool AllowConnectionReuse = false;
        std::vector<TString> StreamContentTypes;

        TEvHttpOutgoingRequest(THttpOutgoingRequestPtr request)
            : Request(std::move(request))
        {}

        TEvHttpOutgoingRequest(THttpOutgoingRequestPtr request, TDuration timeout)
            : Request(std::move(request))
            , Timeout(timeout)
        {}

        TEvHttpOutgoingRequest(THttpOutgoingRequestPtr request, bool allowConnectionReuse)
            : Request(std::move(request))
            , AllowConnectionReuse(allowConnectionReuse)
        {}
    };

    struct TEvHttpIncomingResponse : NActors::TEventLocal<TEvHttpIncomingResponse, EvHttpIncomingResponse> {
        THttpOutgoingRequestPtr Request;
        THttpIncomingResponsePtr Response;
        TString Error;

        TEvHttpIncomingResponse(THttpOutgoingRequestPtr request, THttpIncomingResponsePtr response, const TString& error)
            : Request(std::move(request))
            , Response(std::move(response))
            , Error(error)
        {}

        TEvHttpIncomingResponse(THttpOutgoingRequestPtr request, THttpIncomingResponsePtr response)
            : Request(std::move(request))
            , Response(std::move(response))
        {}

        TString GetError() const {
            TStringBuilder error;
            if (Response != nullptr && !Response->Status.StartsWith('2')) {
                error << Response->Status << ' ' << Response->Message;
            }
            if (!Error.empty()) {
                if (!error.empty()) {
                    error << ';';
                }
                error << Error;
            }
            return error;
        }
    };

    struct TEvHttpIncompleteIncomingResponse : NActors::TEventLocal<TEvHttpIncompleteIncomingResponse, EvHttpIncompleteIncomingResponse> {
        THttpOutgoingRequestPtr Request;
        THttpIncomingResponsePtr Response;

        TEvHttpIncompleteIncomingResponse(THttpOutgoingRequestPtr request, THttpIncomingResponsePtr response)
            : Request(std::move(request))
            , Response(std::move(response))
        {}
    };

    struct TEvHttpOutgoingResponse : NActors::TEventLocal<TEvHttpOutgoingResponse, EvHttpOutgoingResponse> {
        THttpOutgoingResponsePtr Response;

        TEvHttpOutgoingResponse(THttpOutgoingResponsePtr response)
            : Response(std::move(response))
        {}
    };

    struct TEvHttpOutgoingDataChunk : NActors::TEventLocal<TEvHttpOutgoingDataChunk, EvHttpOutgoingDataChunk> {
        THttpOutgoingDataChunkPtr DataChunk;
        TString Error;

        TEvHttpOutgoingDataChunk(THttpOutgoingDataChunkPtr dataChunk)
            : DataChunk(std::move(dataChunk))
        {}

        TEvHttpOutgoingDataChunk(const TString& error)
            : Error(error)
        {}
    };

    struct TEvHttpIncomingDataChunk : NActors::TEventLocal<TEvHttpIncomingDataChunk, EvHttpIncomingDataChunk> {
        THttpIncomingResponsePtr Response;
        TString Data;
        TString Error;
        bool EndOfData = false;

        TEvHttpIncomingDataChunk(THttpIncomingResponsePtr response)
            : Response(std::move(response))
        {}

        void SetData(TString&& data) {
            Data = data;
        }

        void SetEndOfData() {
            EndOfData = true;
        }

        bool IsEndOfData() const {
            return EndOfData;
        }
    };

    struct TEvHttpIncomingConnectionClosed : NActors::TEventLocal<TEvHttpIncomingConnectionClosed, EvHttpIncomingConnectionClosed> {
        TActorId ConnectionID;
        TDeque<THttpIncomingRequestPtr> RecycledRequests;

        TEvHttpIncomingConnectionClosed(const TActorId& connectionID, TDeque<THttpIncomingRequestPtr> recycledRequests)
            : ConnectionID(connectionID)
            , RecycledRequests(std::move(recycledRequests))
        {}
    };

    struct TEvHttpOutgoingConnectionClosed : NActors::TEventLocal<TEvHttpOutgoingConnectionClosed, EvHttpOutgoingConnectionClosed> {
        TActorId ConnectionID;
        TString Destination;

        TEvHttpOutgoingConnectionClosed(const TActorId& connectionID, const TString& destination)
            : ConnectionID(connectionID)
            , Destination(destination)
        {}
    };

    struct TEvHttpOutgoingConnectionAvailable : NActors::TEventLocal<TEvHttpOutgoingConnectionAvailable, EvHttpOutgoingConnectionAvailable> {
        TActorId ConnectionID;
        TString Destination;

        TEvHttpOutgoingConnectionAvailable(const TActorId& connectionID, const TString& destination)
            : ConnectionID(connectionID)
            , Destination(destination)
        {}
    };

    struct TEvHttpAcceptorClosed : NActors::TEventLocal<TEvHttpAcceptorClosed, EvHttpAcceptorClosed> {
        TActorId ConnectionID;

        TEvHttpAcceptorClosed(const TActorId& connectionID)
            : ConnectionID(connectionID)
        {}
    };

    struct TEvResolveHostRequest : NActors::TEventLocal<TEvResolveHostRequest, EvResolveHostRequest> {
        TString Host;

        TEvResolveHostRequest(const TString& host)
            : Host(host)
        {}
    };

    struct TEvResolveHostResponse : NActors::TEventLocal<TEvResolveHostResponse, EvResolveHostResponse> {
        TString Host;
        THttpConfig::SocketAddressType Address;
        TString Error;

        TEvResolveHostResponse(const TString& host, THttpConfig::SocketAddressType address)
            : Host(host)
            , Address(address)
        {}

        TEvResolveHostResponse(const TString& error)
            : Error(error)
        {}
    };

    struct TEvReportSensors : TSensors, NActors::TEventLocal<TEvReportSensors, EvReportSensors> {
        using TSensors::TSensors;

        TEvReportSensors(const TSensors& sensors)
            : TSensors(sensors)
        {}
    };

    struct TEvSubscribeForCancel : NActors::TEventLocal<TEvSubscribeForCancel, EvSubscribeForCancel> {};
    struct TEvRequestCancelled : NActors::TEventLocal<TEvRequestCancelled, EvRequestCancelled> {};
};

struct TRateLimiter {
    TDuration Period = TDuration::Seconds(1);
    ui32 Limit = 0;
    std::atomic<ui32> Count = 0;
    std::atomic<TInstant::TValue> LastReset = 0;

    bool Check(TInstant now) {
        if (Limit == 0) {
            return true;
        }
        TInstant::TValue lastResetValue = LastReset;
        if (now - TInstant::FromValue(lastResetValue) >= Period) {
            if (LastReset.compare_exchange_strong(lastResetValue, now.GetValue())) {
                Count = 0;
            }
        }
        return Count++ < Limit;
    }
};

struct TPrivateEndpointInfo : THttpEndpointInfo {
    TActorId Proxy;
    TActorId Owner;
    TSslHelpers::TSslHolder<SSL_CTX> SecureContext;
    TRateLimiter RateLimiter;
    TDuration InactivityTimeout;

    TPrivateEndpointInfo(const std::vector<TString>& compressContentTypes)
        : THttpEndpointInfo(compressContentTypes)
    {}
};

NActors::IActor* CreateHttpProxy(std::weak_ptr<NMonitoring::IMetricFactory> registry = NMonitoring::TMetricRegistry::SharedInstance());
NActors::IActor* CreateHttpAcceptorActor(const TActorId& owner);
NActors::IActor* CreateOutgoingConnectionActor(const TActorId& owner, TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event);
NActors::IActor* CreateIncomingConnectionActor(
        std::shared_ptr<TPrivateEndpointInfo> endpoint,
        TIntrusivePtr<TSocketDescriptor> socket,
        THttpConfig::SocketAddressType address,
        THttpIncomingRequestPtr recycledRequest = nullptr);
TEvHttpProxy::TEvReportSensors* BuildOutgoingRequestSensors(const THttpOutgoingRequestPtr& request, const THttpIncomingResponsePtr& response);
TEvHttpProxy::TEvReportSensors* BuildIncomingRequestSensors(const THttpIncomingRequestPtr& request, const THttpOutgoingResponsePtr& response);

}
