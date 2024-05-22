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
            NCellBalancer::BundleControllerLogger(),
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
    inline static const TString BundleAttributeResourceQuota = "resource_quota";

    NBundleControllerClient::TBundleTargetConfigPtr GetBundleConfig(const TString& bundleName, std::optional<TDuration> timeout)
    {
        auto path = Format("%v/%v/@%v", TabletCellBundlesPath, NYPath::ToYPathLiteral(bundleName), BundleAttributeTargetConfig);

        NApi::TGetNodeOptions getOptions;
        getOptions.Timeout = timeout;

        auto yson = NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->GetNode(path, getOptions))
            .ValueOrThrow();

        return NYTree::ConvertTo<NBundleControllerClient::TBundleTargetConfigPtr>(yson);
    }

    NBundleControllerClient::TBundleConfigConstraintsPtr GetBundleConstraints(const TString& bundleName, std::optional<TDuration> timeout)
    {
        NApi::TGetNodeOptions getOptions;
        getOptions.Timeout = timeout;

        auto zoneNamePath = Format("%v/%v/@%v", TabletCellBundlesPath, NYPath::ToYPathLiteral(bundleName), BundleAttributeZone);
        auto zoneNameYson = NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->GetNode(zoneNamePath, getOptions))
            .ValueOrThrow();
        TString zoneName = NYTree::ConvertTo<TString>(zoneNameYson);

        auto zoneInfoPath = Format("%v/%v/@", ZoneBundlesPath, NYPath::ToYPathLiteral(zoneName));
        auto zoneInfoYson = NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->GetNode(zoneInfoPath, getOptions))
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

    NBundleControllerClient::TBundleResourceQuotaPtr GetResourceQuota(const TString& bundleName, std::optional<TDuration> timeout)
    {
        NApi::TGetNodeOptions getOptions;
        getOptions.Timeout = timeout;

        auto path = Format("%v/%v/@%v", TabletCellBundlesPath, NYPath::ToYPathLiteral(bundleName), BundleAttributeResourceQuota);
        auto yson = NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->GetNode(path, getOptions))
            .ValueOrThrow();

        auto cypressResourceQuota = NYTree::ConvertTo<NCellBalancer::TResourceQuotaPtr>(yson);
        auto result = New<NBundleControllerClient::TBundleResourceQuota>();
        result->Vcpu = cypressResourceQuota->Vcpu();
        result->Memory = cypressResourceQuota->Memory;
        return result;
    }

    void VerifyInstanceSize(
        const std::vector<NBundleControllerClient::TInstanceSizePtr>& availableSizes,
        const NBundleControllerClient::TInstanceResourcesPtr& instanceSize,
        const TString& instanceType)
    {
        if (instanceSize) {
            return;
        }

        bool found = std::any_of(availableSizes.begin(), availableSizes.end(), [&] (const auto& available) {
            return *instanceSize == *available->ResourceGuarantee;
        });

        if (!found) {
            THROW_ERROR_EXCEPTION("Invalid instance size")
                << TErrorAttribute("provided_instance_size", instanceSize)
                << TErrorAttribute("instance_type", instanceType);
        }
    }

    void ValidateInputConfig(const TString& bundleName, const NBundleControllerClient::TBundleTargetConfigPtr& bundleConfig, std::optional<TDuration> timeout) {
        auto resourceQuota = GetResourceQuota(bundleName, timeout);
        auto bundleConstraints = GetBundleConstraints(bundleName, timeout);

        VerifyInstanceSize(bundleConstraints->RpcProxySizes, bundleConfig->RpcProxyResourceGuarantee, "RpcProxy");
        VerifyInstanceSize(bundleConstraints->TabletNodeSizes, bundleConfig->TabletNodeResourceGuarantee, "TabNode");

        // Checking for total resource usage
        i64 currentMemory = bundleConfig->RpcProxyCount.value_or(0) * bundleConfig->RpcProxyResourceGuarantee->Memory + bundleConfig->TabletNodeCount.value_or(0) * bundleConfig->TabletNodeResourceGuarantee->Memory;
        i64 currentVcpu = bundleConfig->RpcProxyCount.value_or(0) * bundleConfig->RpcProxyResourceGuarantee->Vcpu + bundleConfig->TabletNodeCount.value_or(0) * bundleConfig->TabletNodeResourceGuarantee->Vcpu;

        if (currentMemory > resourceQuota->Memory) {
            THROW_ERROR_EXCEPTION("Cannot allocate new instance: quota memory exceed")
                << TErrorAttribute("current_memory", currentMemory)
                << TErrorAttribute("quota_memory", resourceQuota->Memory);
        }

        if (currentVcpu > resourceQuota->Vcpu) {
            THROW_ERROR_EXCEPTION("Cannot allocate new instance: quota vcpu exceed")
                << TErrorAttribute("current_vcpu", currentVcpu)
                << TErrorAttribute("quota_vcpu", resourceQuota->Vcpu);
        }

        // Checking memory categories oversubscription
        const auto& memoryLimits = bundleConfig->MemoryLimits;

        i64 sumMemoryLimits = memoryLimits->CompressedBlockCache.value_or(0) +
            memoryLimits->KeyFilterBlockCache.value_or(0) +
            memoryLimits->LookupRowCache.value_or(0) +
            memoryLimits->TabletDynamic.value_or(0) +
            memoryLimits->TabletStatic.value_or(0) +
            memoryLimits->UncompressedBlockCache.value_or(0) +
            memoryLimits->VersionedChunkMeta.value_or(0) +
            memoryLimits->Reserved.value_or(0);

        if (sumMemoryLimits > bundleConfig->TabletNodeResourceGuarantee->Memory) {
            THROW_ERROR_EXCEPTION("The sum of the memory limits exceeds the allowed values")
                << TErrorAttribute("current_memory", sumMemoryLimits)
                << TErrorAttribute("tablet_node_memory", bundleConfig->TabletNodeResourceGuarantee->Memory);
        }

        // Check for cpu oversubscription
        const auto& cpuLimits = bundleConfig->CpuLimits;
        int sumVcpuLimits = cpuLimits->LookupThreadPoolSize.value_or(0) +
            cpuLimits->QueryThreadPoolSize.value_or(0) +
            cpuLimits->WriteThreadPoolSize.value_or(0);

        if (sumVcpuLimits > bundleConfig->TabletNodeResourceGuarantee->Vcpu) {
            THROW_ERROR_EXCEPTION("The sum of the cpu limits thread pools exceeds the allowed values")
                << TErrorAttribute("current_vcpu", sumVcpuLimits)
                << TErrorAttribute("tablet_node_vcpu", bundleConfig->TabletNodeResourceGuarantee->Vcpu);
        }

        //TODO(capone212): multi-dc logic
    }

    void SetBundleConfig(const TString& bundleName, NBundleControllerClient::TBundleTargetConfigPtr& config, std::optional<TDuration> timeout)
    {
        auto path = Format("%v/%v/@%v", TabletCellBundlesPath, NYPath::ToYPathLiteral(bundleName), BundleAttributeTargetConfig);

        NApi::TSetNodeOptions setOptions;
        setOptions.Timeout = timeout;

        NConcurrency::WaitFor(Bootstrap_
            ->GetClient()
            ->SetNode(path, NYson::ConvertToYsonString(config), setOptions))
            .ThrowOnError();
    }

    DECLARE_RPC_SERVICE_METHOD(NBundleController::NProto, GetBundleConfig)
    {
        const auto& bundleName = request->bundle_name();
        context->SetRequestInfo("BundleName: %v",
            bundleName);

        auto timeout = context->GetTimeout();

        auto bundleConfig = GetBundleConfig(bundleName, timeout);
        auto bundleConfigConstraints = GetBundleConstraints(bundleName, timeout);
        auto resourceQuota = GetResourceQuota(bundleName, timeout);

        response->set_bundle_name(bundleName);

        NBundleControllerClient::NProto::ToProto(response->mutable_bundle_config(), bundleConfig);
        NBundleControllerClient::NProto::ToProto(response->mutable_bundle_constraints(), bundleConfigConstraints);
        NBundleControllerClient::NProto::ToProto(response->mutable_resource_quota(), resourceQuota);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NBundleController::NProto, SetBundleConfig)
    {
        const auto& bundleName = request->bundle_name();
        context->SetRequestInfo("BundleName: %v",
            bundleName);

        auto timeout = context->GetTimeout();

        const auto& patchConfig = request->bundle_config();

        auto bundleConfig = GetBundleConfig(bundleName, timeout);
        NBundleControllerClient::NProto::FromProto(bundleConfig, &patchConfig);

        ValidateInputConfig(bundleName, bundleConfig, timeout);
        SetBundleConfig(bundleName, bundleConfig, timeout);

        context->Reply();
    }
};

IServicePtr CreateBundleControllerService(NCellBalancer::IBootstrap* bootstrap)
{
    return New<TBundleControllerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleController
