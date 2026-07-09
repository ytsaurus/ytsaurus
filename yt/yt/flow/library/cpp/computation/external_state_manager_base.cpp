#include "external_state_manager_base.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>
#include <yt/yt/flow/library/cpp/resources/yt_client_provider.h>

#include <yt/yt/client/cache/cache.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

NApi::IClientPtr ResolveExternalStateManagerClient(const TExternalStateManagerContextPtr& context)
{
    auto rawClient = context->ClientsCache->GetClient(*context->PipelinePath.GetCluster());
    return CreateRetryableClient(
        rawClient,
        context->SerializedInvoker,
        context->StatusProfiler,
        context->Logger);
}

NApi::IClientPtr ResolveExternalStateJoinerClient(
    const TExternalStateJoinerContextPtr& context,
    std::optional<std::string> cluster)
{
    const auto& spec = context->ExternalStateJoinerSpec;
    NApi::IClientPtr rawClient;
    if (spec && spec->ClientProviderResourceId) {
        auto resource = GetOrDefault(context->StaticResources, *spec->ClientProviderResourceId);
        if (!resource) {
            THROW_ERROR_EXCEPTION(
                "External state joiner references client provider resource %Qv which is not declared in the computation",
                *spec->ClientProviderResourceId);
        }
        auto provider = DynamicPointerCast<IYTClientProvider>(resource);
        if (!provider) {
            THROW_ERROR_EXCEPTION(
                "External state joiner resource %Qv does not implement IYTClientProvider",
                *spec->ClientProviderResourceId);
        }
        rawClient = provider->Get();
    } else if (spec && spec->ClientFactoryResourceId) {
        auto resource = GetOrDefault(context->StaticResources, *spec->ClientFactoryResourceId);
        if (!resource) {
            THROW_ERROR_EXCEPTION(
                "External state joiner references client factory resource %Qv which is not declared in the computation",
                *spec->ClientFactoryResourceId);
        }
        auto factory = DynamicPointerCast<IYTClientFactory>(resource);
        if (!factory) {
            THROW_ERROR_EXCEPTION(
                "External state joiner resource %Qv does not implement IYTClientFactory",
                *spec->ClientFactoryResourceId);
        }
        rawClient = factory->GetClient(cluster.value_or(*context->PipelinePath.GetCluster()));
    } else {
        rawClient = context->ClientsCache->GetClient(cluster.value_or(*context->PipelinePath.GetCluster()));
    }
    return CreateRetryableClient(
        rawClient,
        context->SerializedInvoker,
        context->StatusProfiler,
        context->Logger);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
