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

TBundleConfigDescriptor TClient::DoGetBundleConfig(
    const TString& bundleName,
    const TGetBundleConfigOptions& /*options*/)
{
    auto req = BundleControllerProxy_->GetBundleConfig();
    req->set_bundle_name(bundleName);

    WaitFor(req->Invoke())
        .ThrowOnError();

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    return TBundleConfigDescriptor {
        .BundleName = rsp->bundle_name()
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
