#ifndef DISCOVERY_SERVICE_INL_H_
#error "Direct inclusion of this file is not allowed, include discovery_service.h"
// For the sake of sane code completion.
#include "discovery_service.h"
#endif

#include "private.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/service_detail.h>
#include <yt/yt/orm/server/master/yt_connector.h>

namespace NYT::NOrm::NServer::NApi {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf DiscoveryServiceTag = "discovery-service";

////////////////////////////////////////////////////////////////////////////////

struct TClientMasterInfoAddressesConverter
{
    template <class TProtoMasterDiscoveryInfo>
    static void Convert(const NMaster::TMasterDiscoveryInfo& info, TProtoMasterDiscoveryInfo* protoInfo)
    {
        protoInfo->set_grpc_address(info.ClientGrpcAddress);
        protoInfo->set_grpc_ip6_address(info.ClientGrpcIP6Address);
        protoInfo->set_http_address(info.ClientHttpAddress);
        protoInfo->set_http_ip6_address(info.ClientHttpIP6Address);
    }
};

struct TSecureClientMasterInfoAddressesConverter
{
    template <class TProtoMasterDiscoveryInfo>
    static void Convert(const NMaster::TMasterDiscoveryInfo& info, TProtoMasterDiscoveryInfo* protoInfo)
    {
        protoInfo->set_grpc_address(info.SecureClientGrpcAddress ? info.SecureClientGrpcAddress : info.ClientGrpcAddress);
        protoInfo->set_grpc_ip6_address(info.SecureClientGrpcIP6Address ? info.SecureClientGrpcIP6Address : info.ClientGrpcIP6Address);
        protoInfo->set_http_address(info.SecureClientHttpAddress ? info.SecureClientHttpAddress : info.ClientHttpAddress);
        protoInfo->set_http_ip6_address(info.SecureClientHttpIP6Address ? info.SecureClientHttpIP6Address : info.ClientHttpIP6Address);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <
    class TDiscoveryServiceProtoModule,
    class TMasterInfoAddressesConverter>
class TDiscoveryService
    : public NMaster::TServiceBase
{
public:
    TDiscoveryService(
        NMaster::IBootstrap* bootstrap,
        const NRpc::TServiceDescriptor& descriptor,
        TString interfaceName)
        : NMaster::TServiceBase(
            bootstrap,
            descriptor,
            NApi::Logger(),
            /*authenticator*/ nullptr,
            /*defaultInvoker*/ bootstrap->GetWorkerPoolInvoker(TString(DiscoveryServiceTag), ""))
        , InterfaceName_(std::move(interfaceName))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetMasters));
    }

private:
    const TString InterfaceName_;

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TDiscoveryServiceProtoModule::TReqGetMasters,
        typename TDiscoveryServiceProtoModule::TRspGetMasters,
        GetMasters)
    {
        Y_UNUSED(request);

        context->SetRequestInfo("Interface: %v",
            InterfaceName_);

        const auto& ytConnector = Bootstrap_->GetYTConnector();
        auto infos = ytConnector->GetMasters();
        // Always send master infos to provide clear response
        // structure in case of missing discovery data.
        response->mutable_master_infos();
        for (const auto& info : infos) {
            auto* protoInfo = response->add_master_infos();
            protoInfo->set_fqdn(info.Fqdn);
            protoInfo->set_ip6_address(info.IP6Address);
            TMasterInfoAddressesConverter::Convert(info, protoInfo);
            protoInfo->set_rpc_proxy_address(info.RpcProxyAddress);
            protoInfo->set_rpc_proxy_ip6_address(info.RpcProxyIP6Address);
            protoInfo->set_instance_tag(info.InstanceTag.Underlying());
            protoInfo->set_alive(info.Alive);
            protoInfo->set_leading(info.Leading);
        }
        response->set_cluster_tag(ytConnector->GetClusterTag().Underlying());

        context->SetResponseInfo("Infos: %v",
            MakeFormattableView(response->master_infos(),
                [] (auto* builder, const auto& protoInfo) {
                    builder->AppendFormat("{"
                        "GrpcAddress: %v, "
                        "GrpcIP6Address: %v, "
                        "HttpAddress: %v, "
                        "HttpIP6Address: %v, "
                        "RpcProxyAddress: %v, "
                        "RpcProxyIP6Address: %v, "
                        "InstanceTag: %v, "
                        "Alive: %v, "
                        "Leading: %v}",
                        protoInfo.grpc_address(),
                        protoInfo.grpc_ip6_address(),
                        protoInfo.http_address(),
                        protoInfo.http_ip6_address(),
                        protoInfo.rpc_proxy_address(),
                        protoInfo.rpc_proxy_ip6_address(),
                        protoInfo.instance_tag(),
                        protoInfo.alive(),
                        protoInfo.leading());
                }));

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <
    class TDiscoveryServiceProtoModule,
    class TMasterInfoAddressesConverter>
NRpc::IServicePtr CreateDiscoveryService(
    NMaster::IBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor,
    TString interfaceName)
{
    return New<NDetail::TDiscoveryService<
        TDiscoveryServiceProtoModule,
        TMasterInfoAddressesConverter>>(
        bootstrap,
        descriptor,
        std::move(interfaceName));
}

////////////////////////////////////////////////////////////////////////////////

template <class TDiscoveryServiceProtoModule>
NRpc::IServicePtr CreateClientDiscoveryService(
    NMaster::IBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor)
{
    return CreateDiscoveryService<
        TDiscoveryServiceProtoModule,
        NDetail::TClientMasterInfoAddressesConverter>(
        bootstrap,
        descriptor,
        "Client");
}

template <class TDiscoveryServiceProtoModule>
NRpc::IServicePtr CreateSecureClientDiscoveryService(
    NMaster::IBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor)
{
    return CreateDiscoveryService<
        TDiscoveryServiceProtoModule,
        NDetail::TSecureClientMasterInfoAddressesConverter>(
        bootstrap,
        descriptor,
        "SecureClient");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NApi
