#include "bundle_controller_service.h"

#include "private.h"
#include "bootstrap.h"
#include "cypress_bindings.h"

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/bundle_controller/bundle_controller_service_proxy.h>

#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NBundleController {

using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const NYPath::TYPath TabletCellBundlesPath("//sys/tablet_cell_bundles");

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

    inline static const TString  BundleAttributeTargetConfig = "bundle_controller_target_config";

    NCellBalancer::TBundleConfigPtr GetBundleConfig(const TString& bundleName)
    {
        auto path = Format("%v/%v/@%v", TabletCellBundlesPath, bundleName, BundleAttributeTargetConfig);
        auto yson = NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->GetNode(path))
            .ValueOrThrow();

        return NYTree::ConvertTo<NCellBalancer::TBundleConfigPtr>(yson);
    }

    DECLARE_RPC_SERVICE_METHOD(NBundleController::NProto, GetBundleConfig)
    {
        context->SetRequestInfo("BundleName: %v",
            request->bundle_name());

        TString bundleName = request->bundle_name();

        auto bundleConfig = GetBundleConfig(bundleName);

        response->set_bundle_name(bundleName);

        NBundleControllerClient::NProto::ToProto(response->mutable_cpu_limits(), bundleConfig->CpuLimits);
        NBundleControllerClient::NProto::ToProto(response->mutable_memory_limits(), bundleConfig->MemoryLimits);

        response->set_rpc_proxy_count(bundleConfig->RpcProxyCount);
        NBundleControllerClient::NProto::ToProto(response->mutable_rpc_proxy_resource_guarantee(), bundleConfig->RpcProxyResourceGuarantee);

        response->set_tablet_node_count(bundleConfig->TabletNodeCount);
        NBundleControllerClient::NProto::ToProto(response->mutable_tablet_node_resource_guarantee(), bundleConfig->TabletNodeResourceGuarantee);

        context->Reply();
    }
};

IServicePtr CreateBundleControllerService(NCellBalancer::IBootstrap* bootstrap)
{
    return New<TBundleControllerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleController
