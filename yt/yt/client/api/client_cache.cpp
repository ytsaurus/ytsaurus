#include "client_cache.h"

#include "connection.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TCachedClient::TCachedClient(const TString& user, IClientPtr client)
    : TSyncCacheValueBase(user)
    , Client_(std::move(client))
{ }

const IClientPtr& TCachedClient::GetClient()
{
    return Client_;
}

////////////////////////////////////////////////////////////////////////////////

TClientCache::TClientCache(
    TSlruCacheConfigPtr config,
    IConnectionPtr connection)
    : TSyncSlruCacheBase<TString, TCachedClient>(std::move(config))
    , Connection_(std::move(connection))
{ }

IClientPtr TClientCache::GetClient(const TString& user, const std::optional<TString>& userToken)
{
    auto cachedClient = Find(user);
    if (!cachedClient) {
        TClientOptions options;
        options.PinnedUser = user;
        if (userToken) {
            options.Token = userToken;
        }

        cachedClient = New<TCachedClient>(user, Connection_->CreateClient(options));

        TryInsert(cachedClient, &cachedClient);
    }
    return cachedClient->GetClient();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
