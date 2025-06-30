#include "public.h"

#include <yt/yt/core/misc/sync_cache.h>
#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TMulticonnectionCachedClient
    : public TSyncCacheValueBase<TMultiConnectionClientCacheKey, TMulticonnectionCachedClient>
{
public:
    TMulticonnectionCachedClient(
        const TMultiConnectionClientCacheKey& key,
        NApi::NNative::IClientPtr client);

    const NApi::NNative::IClientPtr& GetClient() const;

private:
    const NApi::NNative::IClientPtr Client_;
};

class TMulticonnectionClientCache
    : public TSyncSlruCacheBase<TMultiConnectionClientCacheKey, TMulticonnectionCachedClient>
{
public:
    explicit TMulticonnectionClientCache(TSlruCacheConfigPtr config);

    NApi::NNative::IClientPtr Get(
        const std::optional<std::string>& targetCluster,
        const NRpc::TAuthenticationIdentity& identity,
        const NApi::NNative::IConnectionPtr& connection,
        const NApi::TClientOptions& options);
};

DEFINE_REFCOUNTED_TYPE(TMulticonnectionClientCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
