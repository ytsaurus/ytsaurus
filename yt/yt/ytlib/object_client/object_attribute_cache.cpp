#include "object_attribute_cache.h"
#include "private.h"

#include <yt/yt/ytlib/cypress_client/batch_attribute_fetcher.h>

namespace NYT::NObjectClient {

using namespace NApi;
using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TObjectAttributeCache::TObjectAttributeCache(
    TObjectAttributeCacheConfigPtr config,
    std::vector<TString> attributeNames,
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache(
        config,
        ObjectClientLogger.WithTag("Cache: ObjectAttribute"),
        std::move(profiler))
    , AttributeNames_(std::move(attributeNames))
    , Config_(std::move(config))
    , Logger(logger.WithTag("ObjectAttributeCacheId: %v", TGuid::Create()))
    , Client_(std::move(client))
    , Invoker_(std::move(invoker))
{ }

TFuture<std::vector<TErrorOr<IAttributeDictionaryPtr>>> TObjectAttributeCache::GetFromClient(
    const std::vector<TYPath>& paths,
    const NNative::IClientPtr& client,
    const IInvokerPtr& invoker,
    const std::vector<TString>& attributeNames,
    const NLogging::TLogger& logger,
    const TMasterReadOptions& options)
{
    auto fetcher = New<TBatchAttributeFetcher>(paths, attributeNames, client, invoker, logger, options);

    return fetcher->Fetch().Apply(BIND([fetcher = std::move(fetcher)] {
        return fetcher->Attributes();
    }));
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
        Invoker_,
        AttributeNames_,
        Logger,
        Config_->MasterReadOptions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
