#include "yql_ytflow_yt_clients_cache.h"

#include <library/cpp/yt/memory/new.h>

#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/cache/rpc.h>

#include <util/generic/hash.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <tuple>


namespace NYql::NYtflow::NPrivate {

class TYtClientsCache
    : public IYtClientsCache
{
public:
    TYtClientsCache(TConfigClusters::TPtr configClusters)
        : ConfigClusters(std::move(configClusters))
    {
    }

public:
    NYT::NApi::IClientPtr GetClient(
        const TString& cluster, const TString& token
    ) override {
        auto guard = Guard(Lock);

        auto key = std::tuple(cluster, token);
        if (auto iterator = Cache.find(key); iterator != Cache.end()) {
            return iterator->second;
        }

        auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
        connectionConfig->SetDefaults();
        connectionConfig->ProxyUrlAliasingRules = ConfigClusters->GetProxyUrlAliasingRules();

        NYT::NClient::NCache::SetClusterUrl(connectionConfig, cluster);

        auto client = NYT::NClient::NCache::CreateClient(
            std::move(connectionConfig),
            NYT::NApi::TClientOptions::FromToken(token));

        Cache.emplace(std::move(key), client);

        return client;
    }

private:
    TConfigClusters::TPtr ConfigClusters;

    // (cluster, token) -> client
    THashMap<std::tuple<TString, TString>, NYT::NApi::IClientPtr> Cache;
    TSpinLock Lock;
};

} // namespace NYql::NYtflow::NPrivate

namespace NYql::NYtflow {

DEFINE_REFCOUNTED_TYPE(IYtClientsCache);

IYtClientsCachePtr CreateYtClientsCache(TConfigClusters::TPtr configClusters)
{
    return NYT::New<NPrivate::TYtClientsCache>(std::move(configClusters));
}

} // namespace NYql::NYtflow
