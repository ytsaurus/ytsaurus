#include "object_attribute_cache.h"

#include <yt/ytlib/cypress_client/object_attribute_fetcher.h>

namespace NYT::NObjectClient {

using namespace NApi;
using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TObjectAttributeCache::TObjectAttributeCache(
    TObjectAttributeCacheConfigPtr config,
    std::vector<TString> attributeNames,
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache(config, std::move(profiler))
    , AttributeNames_(std::move(attributeNames))
    , Config_(std::move(config))
    , Logger(logger.AddTag("ObjectAttributeCacheId: %v", TGuid::Create()))
    , Client_(std::move(client))
    , Invoker_(std::move(invoker))
{ }

TFuture<std::vector<TErrorOr<IAttributeDictionaryPtr>>> TObjectAttributeCache::GetFromClient(
    const std::vector<TYPath>& paths,
    const NNative::IClientPtr& client,
    const std::vector<TString>& attributeNames,
    const TMasterReadOptions& options)
{
    return NCypressClient::FetchAttributes(
        paths,
        attributeNames,
        client,
        options);
}

TFuture<IAttributeDictionaryPtr> TObjectAttributeCache::DoGet(
    const TYPath& path,
    bool isPeriodicUpdate) noexcept
{
    return DoGetMany({path}, isPeriodicUpdate)
        .Apply(BIND([path] (const std::vector<TErrorOr<IAttributeDictionaryPtr>>& response) {
            return response[0].ValueOrThrow();
        }));
}

TFuture<std::vector<TErrorOr<IAttributeDictionaryPtr>>> TObjectAttributeCache::DoGetMany(
    const std::vector<TYPath>& paths,
    bool /*isPeriodicUpdate*/) noexcept
{
    YT_LOG_DEBUG("Updating object attribute cache (PathCount: %v)", paths.size());
    return GetFromClient(
        paths,
        Client_,
        AttributeNames_,
        Config_->GetMasterReadOptions());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
