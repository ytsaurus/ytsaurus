#include "tablet_balancer_service.h"

#include "bootstrap.h"
#include "private.h"
#include "tablet_balancer.h"

#include <yt/yt/ytlib/tablet_balancer_client/proto/tablet_balancer_service.pb.h>

#include <yt/yt/ytlib/tablet_balancer_client/balancing_request.h>
#include <yt/yt/ytlib/tablet_balancer_client/tablet_balancer_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NTabletBalancer {

using namespace NRpc;
using namespace NLogging;
using namespace NTabletBalancerClient;

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerService
    : public TServiceBase
{
public:
    TTabletBalancerService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TTabletBalancerServiceProxy::GetDescriptor(),
            TabletBalancerLogger(),
            {.Authenticator = bootstrap->GetNativeAuthenticator()})
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RequestBalancing));
    }

private:
    const IBootstrap* Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NTabletBalancerClient::NProto, RequestBalancing)
    {
        auto balancingRequest = FromProto<TBalancingRequest>(*request);
        const auto& tabletBalancer = Bootstrap_->GetTabletBalancer();

        tabletBalancer->RequestBalancing(balancingRequest);
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTabletBalancerService(IBootstrap* bootstrap)
{
    return New<TTabletBalancerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
