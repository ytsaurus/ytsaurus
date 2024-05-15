#include "client_cache.h"

#include "client.h"

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TClientCache::TClientCache(
    TSlruCacheConfigPtr config,
    IConnectionPtr connection)
    : NApi::TClientCache(
        std::move(config),
        std::move(connection))
{ }

IClientPtr TClientCache::Get(
    const NRpc::TAuthenticationIdentity& identity,
    const TClientOptions& options)
{
    auto client = NApi::TClientCache::Get(identity, options);
    return IClientPtr(dynamic_cast<IClient*>(client.Get()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
