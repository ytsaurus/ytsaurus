#include "client_cache.h"

#include "config.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/core/misc/async_expiring_cache.h>

#include <util/generic/hash.h>

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NApi::TClientOptions>
{
    size_t operator ()(const NYT::NApi::TClientOptions& options) const
    {
        return options.PinnedUser->hash();
    }
};

template <>
struct TEqualTo<NYT::NApi::TClientOptions>
{
    bool operator ()(const NYT::NApi::TClientOptions& l,
                     const NYT::NApi::TClientOptions& r) const
    {
        return l.PinnedUser == r.PinnedUser;
    }
};

namespace NYT {
namespace NClickHouse {

using namespace NYT::NApi;

////////////////////////////////////////////////////////////////////////////////

class TNativeClientCache
    : public INativeClientCache
    , public TAsyncExpiringCache<TClientOptions, NNative::IClientPtr>
{
private:
    const NNative::IConnectionPtr Connection;

public:
    TNativeClientCache(
        TNativeClientCacheConfigPtr config,
        NNative::IConnectionPtr connection)
        : TAsyncExpiringCache(std::move(config))
        , Connection(std::move(connection))
    {}

    NNative::IClientPtr CreateNativeClient(const TClientOptions& options) override
    {
        return Get(options).Get().ValueOrThrow();
    }

private:
    TFuture<NNative::IClientPtr> DoGet(const TClientOptions& options) override
    {
        return MakeFuture(Connection->CreateNativeClient(options));
    }
};

////////////////////////////////////////////////////////////////////////////////

INativeClientCachePtr CreateNativeClientCache(
    TNativeClientCacheConfigPtr config,
    NNative::IConnectionPtr connection)
{
    return New<TNativeClientCache>(
        std::move(config),
        std::move(connection));
}

}   // namespace NClickHouse
}   // namespace NYT
