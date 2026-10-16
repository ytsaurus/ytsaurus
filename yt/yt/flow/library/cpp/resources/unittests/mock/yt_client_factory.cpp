#include "yt_client_factory.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/client/cache/config.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

TResourceContextPtr MakeDummyContext()
{
    auto config = New<NClient::NCache::TClientsCacheConfig>();
    config->DefaultConnection = New<NApi::NRpcProxy::TConnectionConfig>();
    config->DefaultConnection->ClusterUrl = "";
    auto spec = New<TResourceSpec>();
    spec->Parameters = ConvertToNode(config)->AsMap();
    auto context = New<TResourceContext>();
    context->ResourceSpec = spec;
    return context;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TMockYTClientFactory::TMockYTClientFactory()
    : IYTClientFactory(MakeDummyContext(), New<TDynamicResourceContext>())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
