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
static const NYPath::TYPath ZoneBundlesPath("//sys/bundle_controller/controller/zones");

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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetBundleConfig));
    }

private:
    NCellBalancer::IBootstrap* const Bootstrap_;

    inline static const TString BundleAttributeTargetConfig = "bundle_controller_target_config";
    inline static const TString BundleAttributeZone = "zone";

    NBundleControllerClient::TBundleTargetConfigPtr GetBundleConfig(const TString& bundleName)
    {
        auto path = Format("%v/%v/@%v", TabletCellBundlesPath, bundleName, BundleAttributeTargetConfig);
        auto yson = NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->GetNode(path))
            .ValueOrThrow();

        return NYTree::ConvertTo<NBundleControllerClient::TBundleTargetConfigPtr>(yson);
    }

    NBundleControllerClient::TBundleConfigConstraintsPtr GetBundleConstraints(const TString& bundleName)
    {
        auto zoneNamePath = Format("%v/%v/@%v", TabletCellBundlesPath, bundleName, BundleAttributeZone);
        auto zoneNameYson = NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->GetNode(zoneNamePath))
            .ValueOrThrow();
        TString zoneName = NYTree::ConvertTo<TString>(zoneNameYson);

        auto zoneInfoPath = Format("%v/%v/@", ZoneBundlesPath, zoneName);
        auto zoneInfoYson = NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->GetNode(zoneInfoPath))
            .ValueOrThrow();

        auto zoneInfo = NYTree::ConvertTo<NCellBalancer::TZoneInfoPtr>(zoneInfoYson);
        auto result = New<NBundleControllerClient::TBundleConfigConstraints>();

        for (auto& [type, instance] : zoneInfo->RpcProxySizes) {
            instance->ResourceGuarantee->Type = type;
            result->RpcProxySizes.push_back(instance);
        }

        for (auto& [type, instance] : zoneInfo->TabletNodeSizes) {
            instance->ResourceGuarantee->Type = type;
            result->TabletNodeSizes.push_back(instance);
        }

        return result;
    }

    void SetBundleConfig(const TString& bundleName, NBundleControllerClient::TBundleTargetConfigPtr& config)
    {
        auto path = Format("%v/%v/@%v", TabletCellBundlesPath, bundleName, BundleAttributeTargetConfig);
        NApi::TSetNodeOptions setOptions;
        NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->SetNode(path, NYson::ConvertToYsonString(config), setOptions))
            .ThrowOnError();
    }

    DECLARE_RPC_SERVICE_METHOD(NBundleController::NProto, GetBundleConfig)
    {
        context->SetRequestInfo("BundleName: %v",
            request->bundle_name());

        TString bundleName = request->bundle_name();

        auto bundleConfig = GetBundleConfig(bundleName);
        auto bundleConfigConstraints = GetBundleConstraints(bundleName);

        response->set_bundle_name(bundleName);

        NBundleControllerClient::NProto::ToProto(response->mutable_bundle_constraints(), bundleConfigConstraints);
        NBundleControllerClient::NProto::ToProto(response->mutable_bundle_config(), bundleConfig);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NBundleController::NProto, SetBundleConfig)
    {
        context->SetRequestInfo("BundleName: %v",
            request->bundle_name());

        TString bundleName = request->bundle_name();
        auto reqBundleConfig = request->mutable_bundle_config();

        auto bundleConfig = GetBundleConfig(bundleName);
        NBundleControllerClient::NProto::FromProto(bundleConfig, reqBundleConfig);

        SetBundleConfig(bundleName, bundleConfig);

        context->Reply();
    }
};

IServicePtr CreateBundleControllerService(NCellBalancer::IBootstrap* bootstrap)
{
    return New<TBundleControllerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleController
