#include "client_cache.h"

#include <yt/ytlib/api/native/client.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TClientCache::TClientCache(
    TSlruCacheConfigPtr config,
    IConnectionPtr connection)
    : NApi::TClientCache(
        std::move(config),
        std::move(connection))
{ }

IClientPtr TClientCache::GetClient(const TString& user, const std::optional<TString>& token)
{
    auto client = NApi::TClientCache::GetClient(user, token);
    return IClientPtr(static_cast<IClient*>(client.Get()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
