#ifndef DISCOVERY_CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include discovery_client.h"
// For the sake of sane code completion.
#include "discovery_client.h"
#endif

namespace NYT::NOrm::NClient::NNative::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TDiscoveryServiceProxy>
TGetMastersResult ParseGetMastersResult(
    TErrorOr<typename TDiscoveryServiceProxy::TRspGetMastersPtr>&& rspOrError)
{
    auto& rsp = rspOrError.ValueOrThrow();

    TGetMastersResult result;

    result.MasterInfos.reserve(rsp->master_infos_size());
    for (auto& protoMasterInfo : *rsp->mutable_master_infos()) {
        result.MasterInfos.push_back(TMasterInfo{
            .Fqdn = std::move(*protoMasterInfo.mutable_fqdn()),
            .GrpcAddress = std::move(*protoMasterInfo.mutable_grpc_address()),
            .GrpcIP6Address = std::move(*protoMasterInfo.mutable_grpc_ip6_address()),
            .HttpAddress = std::move(*protoMasterInfo.mutable_http_address()),
            .RpcProxyAddress = std::move(*protoMasterInfo.mutable_rpc_proxy_address()),
            .InstanceTag = TMasterInstanceTag{protoMasterInfo.instance_tag()},
            .Alive = protoMasterInfo.alive(),
            .Leading = protoMasterInfo.leading(),
        });
    }

    result.ClusterTag = rsp->cluster_tag();

    return result;
}

template<class TDiscoveryServiceProxy>
struct TDiscoveryServiceTraits
{
    static TFuture<TGetMastersResult> GetMasters(
        const NRpc::IChannelPtr& channel,
        const NLogging::TLogger& Logger,
        const TDuration timeout)
    {
        TDiscoveryServiceProxy proxy(channel);
        auto req = proxy.GetMasters();
        req->SetTimeout(timeout);
        YT_LOG_DEBUG("Invoking request (Method: %v, RequestId: %v)",
            req->GetMethod(),
            req->GetRequestId());
        return req->Invoke().ApplyUnique(BIND(&ParseGetMastersResult<TDiscoveryServiceProxy>));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative::NDetail
