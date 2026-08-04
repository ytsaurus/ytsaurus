#include "root_clients_cache.h"

#include "private.h"

#include <yt/yt/client/cache/config.h>
#include <yt/yt/client/cache/rpc.h>

#include <yt/yt/core/net/address.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NFlow {

using namespace NClient::NCache;

////////////////////////////////////////////////////////////////////////////////

namespace {

constinit const auto& Logger = RootClientsCacheLogger;

NThreading::TAtomicObject<TRootClientsCacheFactory>& GetRootClientsCacheFactory()
{
    static NThreading::TAtomicObject<TRootClientsCacheFactory> factory;
    return factory;
}

std::string GetNormalClusterName(TStringBuf clusterName)
{
    return std::string(NNet::InferYTClusterFromClusterUrlRaw(clusterName).value_or(clusterName));
}

TClientsCacheConfigPtr ApplyProxyRole(const TRootClientsCacheOptions& options)
{
    if (!options.ProxyRole) {
        return options.ClientsCacheConfig;
    }

    auto config = CloneYsonStruct(options.ClientsCacheConfig);
    if (!config->DefaultConnection->ProxyRole) {
        config->DefaultConnection->ProxyRole = options.ProxyRole;
    }

    // GetConnectionConfig() resolves a cluster to its per-cluster entry whenever there is one and
    // never falls back to the default connection, so the role has to be set on that entry too.
    if (auto pipelineClusterUrl = options.PipelinePath.GetCluster()) {
        auto pipelineCluster = GetNormalClusterName(ExtractClusterAndProxyRole(*pipelineClusterUrl).first);
        for (const auto& [cluster, connection] : config->PerClusterConnection) {
            if (!connection->ProxyRole && GetNormalClusterName(cluster) == pipelineCluster) {
                connection->ProxyRole = options.ProxyRole;
            }
        }
    }

    return config;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void SetRootClientsCacheFactory(TRootClientsCacheFactory factory)
{
    GetRootClientsCacheFactory().Store(std::move(factory));
}

IClientsCachePtr CreateRootClientsCache(const TRootClientsCacheOptions& options)
{
    auto patchedOptions = options;
    patchedOptions.ClientsCacheConfig = ApplyProxyRole(options);

    auto factory = GetRootClientsCacheFactory().Load();
    if (!factory) {
        THROW_ERROR_EXCEPTION_IF(patchedOptions.Parameters,
            "Option \"clients_cache_factory\" is set, but no root clients cache factory is installed; "
            "the binary is not linked against an implementation calling SetRootClientsCacheFactory()");
        return CreateClientsCache(patchedOptions.ClientsCacheConfig, patchedOptions.ClientOptions);
    }

    YT_TLOG_INFO("Creating the root clients cache with a custom factory")
        .With("Pipeline", patchedOptions.PipelinePath)
        .With("HasParameters", static_cast<bool>(patchedOptions.Parameters));

    auto clientsCache = factory(patchedOptions);
    YT_VERIFY(clientsCache);
    return clientsCache;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
