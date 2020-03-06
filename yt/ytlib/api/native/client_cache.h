#pragma once

#include "private.h"

#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/client_cache.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

//! An SLRU-cache based class for keeping a cache of clients for different users.
/*
 *  For NApi::IClient equivalent see client/api/client_cache.h.
 *
 *  Cache is completely thread-safe.
 */
class TClientCache
    : public NApi::TClientCache
{
public:
    TClientCache(
        TSlruCacheConfigPtr config,
        NApi::NNative::IConnectionPtr connection);

    IClientPtr GetClient(
        const TString& user,
        const std::optional<TString>& token = std::nullopt);
};

DEFINE_REFCOUNTED_TYPE(TClientCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
