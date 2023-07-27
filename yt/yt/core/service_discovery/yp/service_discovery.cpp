#include "service_discovery.h"

#include "config.h"
#include "private.h"
#include "service_discovery_service_proxy.h"

#include <infra/yp_service_discovery/api/api.pb.h>

#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NServiceDiscovery {

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TEndpoint* endpoint,
    const ::NYP::NServiceDiscovery::NApi::TEndpoint& protoEndpoint)
{
    endpoint->Id = protoEndpoint.id();
    endpoint->Protocol = protoEndpoint.protocol();
    endpoint->Fqdn = protoEndpoint.fqdn();
    endpoint->IP4Address = protoEndpoint.ip4_address();
    endpoint->IP6Address = protoEndpoint.ip6_address();
    endpoint->Port = protoEndpoint.port();
    endpoint->Ready = protoEndpoint.ready();
}

void FromProto(
    TEndpointSet* endpointSet,
    const ::NYP::NServiceDiscovery::NApi::TEndpointSet& protoEndpointSet)
{
    endpointSet->Id = protoEndpointSet.endpoint_set_id();
    NYT::FromProto(&endpointSet->Endpoints, protoEndpointSet.endpoints());
}

////////////////////////////////////////////////////////////////////////////////

namespace NYP {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

// TODO(bidzilya): Anonymous namespace is not used intentionally due to YT-13603.
namespace NDetail {

TError ConvertResolveStatusToError(int status)
{
    switch (status) {
        case ::NYP::NServiceDiscovery::NApi::NOT_EXISTS:
            return TError(NServiceDiscovery::EErrorCode::EndpointSetDoesNotExist,
                "Endpoint set does not exist");
        case ::NYP::NServiceDiscovery::NApi::NOT_CHANGED:
        case ::NYP::NServiceDiscovery::NApi::OK:
        case ::NYP::NServiceDiscovery::NApi::EMPTY:
            return TError();
        default:
            return TError(NServiceDiscovery::EErrorCode::UnknownResolveStatus,
                "Unknown resolve status %v",
                status);
    }
}

struct TResolveEndpointsResponse
{
    ui64 Timestamp;
    TEndpointSet EndpointSet;
    int ResolveStatus;
};

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TResolveEndpointsResponse* response,
    const ::NYP::NServiceDiscovery::NApi::TRspResolveEndpoints& protoResponse)
{
    response->Timestamp = protoResponse.timestamp();
    FromProto(&response->EndpointSet, protoResponse.endpoint_set());
    response->ResolveStatus = protoResponse.resolve_status();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

namespace {

DECLARE_REFCOUNTED_CLASS(TServiceDiscoveryClient)

////////////////////////////////////////////////////////////////////////////////

class TServiceDiscoveryClient
    : public TRefCounted
{
public:
    explicit TServiceDiscoveryClient(TServiceDiscoveryConfigPtr config)
        : Config_(std::move(config))
        , ProviderChannel_(CreateProviderGrpcChannel())
    { }

    TFuture<NDetail::TResolveEndpointsResponse> ResolveEndpoints(
        const TString& cluster,
        const TString& endpointSetId) noexcept
    {
        auto requestId = ToString(TGuid::Create());

        YT_LOG_DEBUG("Resolving endpoint set (Cluster: %v, EndpointSetId: %v, Client: %v, RequestId: %v)",
            cluster,
            endpointSetId,
            Config_->Client,
            requestId);

        TServiceDiscoveryServiceProxy proxy(ProviderChannel_);

        auto req = proxy.ResolveEndpoints();
        req->set_cluster_name(cluster);
        req->set_endpoint_set_id(endpointSetId);
        req->set_client_name(Config_->Client);
        req->set_ruid(requestId);

        return req->Invoke().Apply(BIND(
            [cluster, endpointSetId, requestId] (const TErrorOr<TServiceDiscoveryServiceProxy::TRspResolveEndpointsPtr>& protoResponseOrError) {
                if (!protoResponseOrError.IsOK()) {
                    YT_LOG_DEBUG(protoResponseOrError,
                        "Error resolving endpoint set ("
                        "Cluster: %v, "
                        "EndpointSetId: %v, "
                        "RequestId: %v)",
                        cluster,
                        endpointSetId,
                        requestId);
                }
                const auto& protoResponse = protoResponseOrError.ValueOrThrow();
                auto response = NYT::FromProto<NDetail::TResolveEndpointsResponse>(*protoResponse);
                YT_LOG_DEBUG("Resolved endpoint set ("
                    "Cluster: %v, "
                    "EndpointSetId: %v, "
                    "RequestId: %v, "
                    "EndpointCount: %v, "
                    "Timestamp: %v, "
                    "ResolveStatus: %v, "
                    "ProviderHost: %v)",
                    cluster,
                    endpointSetId,
                    requestId,
                    response.EndpointSet.Endpoints.size(),
                    response.Timestamp,
                    response.ResolveStatus,
                    protoResponse->host());
                return response;
            }));
    }

private:
    const TServiceDiscoveryConfigPtr Config_;
    const IChannelPtr ProviderChannel_;

    IChannelPtr CreateProviderGrpcChannel() const
    {
        auto config = New<NGrpc::TChannelConfig>();
        config->Address = Config_->Fqdn + ":" + ToString(Config_->GrpcPort);
        config->Postprocess();
        return NRpc::CreateRetryingChannel(
            Config_,
            NGrpc::CreateGrpcChannel(std::move(config)));
    }
};

DEFINE_REFCOUNTED_TYPE(TServiceDiscoveryClient)

////////////////////////////////////////////////////////////////////////////////

using TClusterEndpointSetIdPair = std::pair<TString, TString>;

class TServiceDiscovery
    : public IServiceDiscovery
    , public TAsyncExpiringCache<TClusterEndpointSetIdPair, TEndpointSet>
{
public:
    explicit TServiceDiscovery(TServiceDiscoveryConfigPtr config)
        : TAsyncExpiringCache<TClusterEndpointSetIdPair, TEndpointSet>(config)
        , Config_(std::move(config))
        , Client_(New<TServiceDiscoveryClient>(Config_))
    { }

    TFuture<TEndpointSet> ResolveEndpoints(
        const TString& cluster,
        const TString& endpointSetId) override
    {
        return Get(TClusterEndpointSetIdPair{cluster, endpointSetId});
    }

protected:
    TFuture<TEndpointSet> DoGet(
        const TClusterEndpointSetIdPair& clusterEndpointSetIdPair,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return Client_->ResolveEndpoints(clusterEndpointSetIdPair.first, clusterEndpointSetIdPair.second).Apply(BIND(
            [=, this, this_ = MakeStrong(this)] (const TErrorOr<NDetail::TResolveEndpointsResponse>& rawResponseOrError) {
                auto responseOrError = UpdateAndGetMostActualSuccessfulResponse(
                    clusterEndpointSetIdPair,
                    rawResponseOrError);
                const auto& response = responseOrError.ValueOrThrow();
                if (auto resolveStatusError = NDetail::ConvertResolveStatusToError(response.ResolveStatus); !resolveStatusError.IsOK()) {
                    THROW_ERROR_EXCEPTION(
                        NServiceDiscovery::EErrorCode::EndpointResolveFailed,
                        "Error resolving endpoint set %Qv at YP cluster %Qv",
                        clusterEndpointSetIdPair.second,
                        clusterEndpointSetIdPair.first)
                        << resolveStatusError;
                }
                return response.EndpointSet;
            }));
    }

    void OnRemoved(const TClusterEndpointSetIdPair& key) noexcept override
    {
        auto guard = Guard(MostActualResponsesLock_);

        auto it = MostActualResponses_.find(key);
        if (it != MostActualResponses_.end()) {
            YT_LOG_DEBUG("Removing endpoint set from cache (Cluster: %v, EndpointSetId: %v, Timestamp: %v)",
                key.first,
                key.second,
                it->second.Timestamp);
            MostActualResponses_.erase(it);
        }
    }

private:
    const TServiceDiscoveryConfigPtr Config_;
    const TServiceDiscoveryClientPtr Client_;

    THashMap<TClusterEndpointSetIdPair, NDetail::TResolveEndpointsResponse> MostActualResponses_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, MostActualResponsesLock_);

    TErrorOr<NDetail::TResolveEndpointsResponse> UpdateAndGetMostActualSuccessfulResponse(
        const TClusterEndpointSetIdPair& key,
        const TErrorOr<NDetail::TResolveEndpointsResponse>& responseOrError)
    {
        auto guard = Guard(MostActualResponsesLock_);

        auto it = MostActualResponses_.find(key);
        if (responseOrError.IsOK()) {
            auto response = responseOrError.Value();
            if (it == MostActualResponses_.end()) {
                YT_LOG_DEBUG("Caching new endpoint set (Cluster: %v, EndpointSetId: %v, Timestamp: %v)",
                    key.first,
                    key.second,
                    response.Timestamp);
                MostActualResponses_.emplace(key, response);
            } else if (it->second.Timestamp < response.Timestamp) {
                YT_LOG_DEBUG("Caching more actual endpoint set (Cluster: %v, EndpointSetId: %v, OldTimestamp: %v, NewTimestamp: %v)",
                    key.first,
                    key.second,
                    it->second.Timestamp,
                    response.Timestamp);
                it->second = response;
            } else {
                if (it->second.Timestamp > response.Timestamp) {
                    YT_LOG_DEBUG("Resolved stale endpoint set; using the most actual one instead ("
                        "Cluster: %v, "
                        "EndpointSetId: %v, "
                        "StaleTimestamp: %v, "
                        "MostActualTimestamp: %v)",
                        key.first,
                        key.second,
                        response.Timestamp,
                        it->second.Timestamp);
                }
                response = it->second;
            }
            return response;
        } else {
            if (it == MostActualResponses_.end()) {
                return responseOrError;
            } else {
                YT_LOG_DEBUG(responseOrError,
                    "Error resolving endpoint set; using the most actual one instead ("
                    "Cluster: %v, "
                    "EndpointSetId: %v, "
                    "MostActualTimestamp: %v)",
                    key.first,
                    key.second,
                    it->second.Timestamp);
                return it->second;
            }
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IServiceDiscoveryPtr CreateServiceDiscovery(TServiceDiscoveryConfigPtr config)
{
    if (!config->Enable) {
        return nullptr;
    }
    return New<TServiceDiscovery>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP
} // namespace NYT::NServiceDiscovery
