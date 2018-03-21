#include "discovery_service.h"
#include "public.h"
#include "private.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/service_detail.h>
#include <yp/server/master/yt_connector.h>

#include <yp/client/api/proto/discovery_service.pb.h>

namespace NYP {
namespace NServer {
namespace NApi {

using namespace NYT;
using namespace NYT::NRpc;

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryService
    : public NMaster::TServiceBase
{
public:
    TDiscoveryService(
        NMaster::TBootstrap* bootstrap,
        EMasterInterface interface)
        : TServiceBase(
            bootstrap,
            TServiceDescriptor("NYP.NClient.NApi.NProto.DiscoveryService"),
            NApi::Logger)
        , Interface_(interface)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetMasters));
    }

private:
    const EMasterInterface Interface_;


    DECLARE_RPC_SERVICE_METHOD(NClient::NApi::NProto, GetMasters)
    {
        context->SetRequestInfo("Interface: %v",
            Interface_);

        const auto& ytConnector = Bootstrap_->GetYTConnector();
        auto infos = ytConnector->GetMasters();
        for (const auto& info : infos) {
            auto* protoInfo = response->add_master_infos();
            protoInfo->set_fqdn(info.Fqdn);
            switch (Interface_) {
                case EMasterInterface::Client:
                    protoInfo->set_grpc_address(info.ClientGrpcAddress);
                    protoInfo->set_http_address(info.ClientHttpAddress);
                    break;
                case EMasterInterface::Agent:
                    protoInfo->set_grpc_address(info.AgentGrpcAddress);
                    break;
                default:
                    Y_UNREACHABLE();
            }
            protoInfo->set_instance_tag(info.InstanceTag);
            protoInfo->set_alive(info.Alive);
            protoInfo->set_leading(info.Leading);
        }
        response->set_cluster_tag(ytConnector->GetClusterTag());

        context->SetResponseInfo("InstanceCount: %v", infos.size());
        context->Reply();
    }
};

IServicePtr CreateDiscoveryService(
    NMaster::TBootstrap* bootstrap,
    EMasterInterface interface)
{
    return New<TDiscoveryService>(bootstrap, interface);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NServer
} // namespace NYP

