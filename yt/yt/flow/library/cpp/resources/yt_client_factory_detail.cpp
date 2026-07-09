#include "yt_client_factory_detail.h"

#include <yt/yt/flow/library/cpp/common/authenticator.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/client/api/rpc_proxy/config.h>

#include <yt/yt/client/api/options.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/cache/rpc.h>

#include <library/cpp/yt/logging/backends/arcadia/backend.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NApi::NRpcProxy;
using namespace NClient::NCache;

////////////////////////////////////////////////////////////////////////////////

TYTClientFactory::TYTClientFactory(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : IYTClientFactory(std::move(context), std::move(dynamicContext))
    , ClientsCache_(CreateClientsCache(GetParameters(), GetContext()->PipelineAuthenticator->GetClientOptions()))
{ }

NApi::IClientPtr TYTClientFactory::GetClient(TStringBuf clusterUrl)
{
    return ClientsCache_->GetClient(clusterUrl);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
