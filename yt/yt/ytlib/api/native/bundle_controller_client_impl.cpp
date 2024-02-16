#include "client_impl.h"

#include <yt/yt/core/crypto/crypto.h>

#include <util/string/hex.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NCrypto;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NBundleControllerClient::TBundleConfigDescriptorPtr TClient::DoGetBundleConfig(
    const TString& bundleName,
    const NBundleControllerClient::TGetBundleConfigOptions& options)
{
    auto req = BundleControllerProxy_->GetBundleConfig();
    req->set_bundle_name(bundleName);

    req->SetTimeout(options.Timeout);

    WaitFor(req->Invoke())
        .ThrowOnError();

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    auto result = New<NBundleControllerClient::TBundleConfigDescriptor>();
    result->BundleName = rsp->bundle_name();

    auto bundleConfig = New<NBundleControllerClient::TBundleTargetConfig>();
    bundleConfig->CpuLimits = New<NBundleControllerClient::TCpuLimits>();
    bundleConfig->MemoryLimits = New<NBundleControllerClient::TMemoryLimits>();
    bundleConfig->RpcProxyResourceGuarantee = New<NBundleControllerClient::TInstanceResources>();
    bundleConfig->TabletNodeResourceGuarantee = New<NBundleControllerClient::TInstanceResources>();
    NBundleControllerClient::NProto::FromProto(bundleConfig, rsp->mutable_bundle_config());

    auto bundleConfigConstraints = New<NBundleControllerClient::TBundleConfigConstraints>();
    NBundleControllerClient::NProto::FromProto(bundleConfigConstraints, rsp->mutable_bundle_constraints());

    auto resourceQuota = New<NBundleControllerClient::TBundleResourceQuota>();
    NBundleControllerClient::NProto::FromProto(resourceQuota, rsp->mutable_resource_quota());

    result->Config = bundleConfig;
    result->ConfigConstraints = bundleConfigConstraints;
    result->ResourceQuota = resourceQuota;

    return result;
}

void TClient::DoSetBundleConfig(
    const TString& bundleName,
    const NBundleControllerClient::TBundleTargetConfigPtr& bundleConfig,
    const NBundleControllerClient::TSetBundleConfigOptions& options)
{
    auto req = BundleControllerProxy_->SetBundleConfig();

    req->set_bundle_name(bundleName);
    NBundleControllerClient::NProto::ToProto(req->mutable_bundle_config(), bundleConfig);

    req->SetTimeout(options.Timeout);

    WaitFor(req->Invoke())
        .ThrowOnError();

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
