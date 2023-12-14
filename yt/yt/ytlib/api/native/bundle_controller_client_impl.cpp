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

TBundleConfigDescriptorPtr TClient::DoGetBundleConfig(
    const TString& bundleName,
    const TGetBundleConfigOptions& /*options*/)
{
    auto req = BundleControllerProxy_->GetBundleConfig();
    req->set_bundle_name(bundleName);

    WaitFor(req->Invoke())
        .ThrowOnError();

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    auto result = New<TBundleConfigDescriptor>();
    result->BundleName = rsp->bundle_name();
    result->RpcProxyCount = rsp->rpc_proxy_count();
    result->TabletNodeCount = rsp->tablet_node_count();

    NCellBalancer::NProto::FromProto(result->CpuLimits, rsp->mutable_cpu_limits());
    NCellBalancer::NProto::FromProto(result->MemoryLimits, rsp->mutable_memory_limits());
    NCellBalancer::NProto::FromProto(result->RpcProxyResourceGuarantee, rsp->mutable_rpc_proxy_resource_guarantee());
    NCellBalancer::NProto::FromProto(result->TabletNodeResourceGuarantee, rsp->mutable_tablet_node_resource_guarantee());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
