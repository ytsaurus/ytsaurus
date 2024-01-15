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
    const NBundleControllerClient::TGetBundleConfigOptions& /*options*/)
{
    auto req = BundleControllerProxy_->GetBundleConfig();
    req->set_bundle_name(bundleName);

    WaitFor(req->Invoke())
        .ThrowOnError();

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    auto result = New<NBundleControllerClient::TBundleConfigDescriptor>();
    result->BundleName = rsp->bundle_name();

    auto conf = New<NBundleControllerClient::TBundleTargetConfig>();
    conf->CpuLimits = New<NBundleControllerClient::TCpuLimits>();
    conf->MemoryLimits = New<NBundleControllerClient::TMemoryLimits>();
    conf->RpcProxyResourceGuarantee = New<NBundleControllerClient::TInstanceResources>();
    conf->TabletNodeResourceGuarantee = New<NBundleControllerClient::TInstanceResources>();
    NBundleControllerClient::NProto::FromProto(conf, rsp->mutable_bundle_config());

    result->BundleConfig = conf;

    return result;
}

void TClient::DoSetBundleConfig(
    const TString& bundleName,
    const NBundleControllerClient::TBundleTargetConfigPtr& bundleConfig,
    const NBundleControllerClient::TSetBundleConfigOptions& /*options*/)
{
    auto req = BundleControllerProxy_->SetBundleConfig();

    req->set_bundle_name(bundleName);
    NBundleControllerClient::NProto::ToProto(req->mutable_bundle_config(), bundleConfig);

    WaitFor(req->Invoke())
        .ThrowOnError();

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
