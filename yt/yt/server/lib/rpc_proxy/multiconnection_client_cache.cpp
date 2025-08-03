#include "multiconnection_client_cache.h"

#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NRpcProxy {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TMulticonnectionCachedClient::TMulticonnectionCachedClient(
    const TMultiConnectionClientCacheKey& key,
    NNative::IClientPtr client)
    : TSyncCacheValueBase(key)
    , Client_(std::move(client))
{ }

const NNative::IClientPtr& TMulticonnectionCachedClient::GetClient() const
{
    return Client_;
}

////////////////////////////////////////////////////////////////////////////////

TMulticonnectionClientCache::TMulticonnectionClientCache(TSlruCacheConfigPtr config)
    : TSyncSlruCacheBase<TMultiConnectionClientCacheKey, TMulticonnectionCachedClient>(std::move(config))
{ }

NNative::IClientPtr TMulticonnectionClientCache::Get(
    const std::optional<std::string>& targetCluster,
    const NRpc::TAuthenticationIdentity& identity,
    const NNative::IConnectionPtr& connection,
    const NApi::TClientOptions& options)
{
    auto key = TMultiConnectionClientCacheKey{targetCluster, identity};
    auto cachedClient = Find(key);
    if (!cachedClient) {
        cachedClient = New<TMulticonnectionCachedClient>(key, connection->CreateNativeClient(options));
        TryInsert(cachedClient, &cachedClient);
    }
    return cachedClient->GetClient();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
