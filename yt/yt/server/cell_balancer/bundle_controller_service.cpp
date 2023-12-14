#include "bundle_controller_service.h"

#include "private.h"
#include "bootstrap.h"

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/ytlib/bundle_controller/bundle_controller_service_proxy.h>

namespace NYT::NBundleController {

using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TBundleControllerService
    : public TServiceBase
{
public:
    explicit TBundleControllerService(NCellBalancer::IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TBundleControllerServiceProxy::GetDescriptor(),
            NCellBalancer::BundleControllerLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        Y_UNUSED(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBundleConfig));
    }

private:
    NCellBalancer::IBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NBundleController::NProto, GetBundleConfig)
    {
        context->SetRequestInfo("BundleName: %v",
            request->bundle_name());

        response->set_bundle_name(request->bundle_name());
        response->set_rpc_proxy_count(2);
        response->set_tablet_node_count(5);
        context->Reply();
    }
};

IServicePtr CreateBundleControllerService(NCellBalancer::IBootstrap* bootstrap)
{
    return New<TBundleControllerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleController
