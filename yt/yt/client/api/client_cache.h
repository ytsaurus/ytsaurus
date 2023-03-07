#pragma once

#include "private.h"

#include <yt/core/misc/sync_cache.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TCachedClient
    : public TSyncCacheValueBase<TString, TCachedClient>
{
public:
    TCachedClient(
        const TString& user,
        IClientPtr client);

    const IClientPtr& GetClient();

private:
    const IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

//! An SLRU-cache based class for keeping a cache of clients for different users.
/*
 *  For NApi::NNative::IClient equivalent see ytlib/api/native/client_cache.h.
 *
 *  Cache is completely thread-safe.
 */
class TClientCache
    : public TSyncSlruCacheBase<TString, TCachedClient>
{
public:
    TClientCache(
        TSlruCacheConfigPtr config,
        IConnectionPtr connection);

    IClientPtr GetClient(
        const TString& user,
        const std::optional<TString>& userToken = std::nullopt);

private:
    const IConnectionPtr Connection_;
};

DEFINE_REFCOUNTED_TYPE(TClientCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
