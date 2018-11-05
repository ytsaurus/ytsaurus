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
namespace NClickHouseServer {
namespace NNative {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

class TNativeClientCache
    : public INativeClientCache
    , public TAsyncExpiringCache<TClientOptions, NApi::NNative::IClientPtr>
{
private:
    const NApi::NNative::IConnectionPtr Connection;

public:
    TNativeClientCache(
        TNativeClientCacheConfigPtr config,
        NApi::NNative::IConnectionPtr connection)
        : TAsyncExpiringCache(std::move(config))
        , Connection(std::move(connection))
    {}

    NApi::NNative::IClientPtr CreateNativeClient(const TClientOptions& options) override
    {
        return Get(options).Get().ValueOrThrow();
    }

private:
    TFuture<NApi::NNative::IClientPtr> DoGet(const TClientOptions& options) override
    {
        return MakeFuture(Connection->CreateNativeClient(options));
    }
};

////////////////////////////////////////////////////////////////////////////////

INativeClientCachePtr CreateNativeClientCache(
    TNativeClientCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection)
{
    return New<TNativeClientCache>(
        std::move(config),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
