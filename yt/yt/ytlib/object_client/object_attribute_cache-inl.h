#ifndef OBJECT_ATTRIBUTE_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include object_attribute_cache.h"
// For the sake of sane code completion.
#include "object_attribute_cache.h"
#endif

#include "public.h"
#include "config.h"

#include <yt/yt/ytlib/cypress_client/batch_attribute_fetcher.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

template <typename Key, typename Value>
TObjectAttributeCacheBase<Key, Value>::TObjectAttributeCacheBase(
    TObjectAttributeCacheConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    NProfiling::TProfiler profiler)
    : TAsyncExpiringCache<Key, Value>(
        config,
        ObjectClientLogger().WithTag("Cache: ObjectAttribute"),
        std::move(profiler))
    , Config_(std::move(config))
    , Logger(logger.WithTag("ObjectAttributeCacheId: %v", TGuid::Create()))
    , Client_(std::move(client))
    , Invoker_(std::move(invoker))
{ }

template <typename Key, typename Value>
TFuture<Value> TObjectAttributeCacheBase<Key, Value>::DoGet(
    const Key& key,
    bool isPeriodicUpdate) noexcept
{
    return DoGetMany({key}, isPeriodicUpdate)
        .Apply(BIND([] (const std::vector<TErrorOr<Value>>& response) {
            return response[0].ValueOrThrow();
        }));
}

template <typename Key, typename Value>
TFuture<std::vector<TErrorOr<Value>>> TObjectAttributeCacheBase<Key, Value>::DoGetMany(
    const std::vector<Key>& keys,
    bool /*isPeriodicUpdate*/) noexcept
{
    YT_LOG_DEBUG("Updating object attribute cache (ObjectCount: %v)", keys.size());

    return GetFromClient(keys, Client_);
}

template <typename Key, typename Value>
TFuture<std::vector<TErrorOr<Value>>> TObjectAttributeCacheBase<Key, Value>::GetFromClient(
    const std::vector<Key>& keys,
    const NApi::NNative::IClientPtr& client) noexcept
{
    YT_LOG_DEBUG("Fetching attributes via object attribute cache (ObjectCount: %v)", keys.size());

    std::vector<NYPath::TYPath> paths;
    paths.reserve(keys.size());
    for (const auto& key : keys) {
        paths.push_back(GetPath(key));
    }

    auto fetcher = New<NCypressClient::TBatchAttributeFetcher>(paths, GetAttributeNames(), client, Invoker_, Logger, *Config_->MasterReadOptions);

    return fetcher->Fetch().Apply(BIND([this, this_ = MakeStrong(this), fetcher = std::move(fetcher)] {
        std::vector<TErrorOr<Value>> result;
        result.reserve(fetcher->Attributes().size());
        for (const TErrorOr<NYTree::IAttributeDictionaryPtr>& objectAttributesOrError : fetcher->Attributes()) {
            if (objectAttributesOrError.IsOK()) {
                result.push_back(ParseValue(objectAttributesOrError.Value()));
            } else {
                result.push_back(TError(objectAttributesOrError));
            }
        }
        return result;
    }));
}

////////////////////////////////////////////////////////////////////////////////

template <class Key, class Value>
    requires std::derived_from<typename Value::TUnderlying, NYTree::TYsonStruct>
TObjectAttributeAsYsonStructCacheBase<Key, Value>::TObjectAttributeAsYsonStructCacheBase(
    TObjectAttributeCacheConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    NProfiling::TProfiler profiler)
    : TObjectAttributeCacheBase<Key, Value>(std::move(config), std::move(client), std::move(invoker), logger, std::move(profiler))
{
    const auto& attributeNameSet = New<typename Value::TUnderlying>()->GetRegisteredKeys();
    AttributeNames_ = std::vector<TString>(attributeNameSet.begin(), attributeNameSet.end());
}

template <class Key, class Value>
    requires std::derived_from<typename Value::TUnderlying, NYTree::TYsonStruct>
Value TObjectAttributeAsYsonStructCacheBase<Key, Value>::ParseValue(const NYTree::IAttributeDictionaryPtr& attributes) const
{
    return ConvertTo<Value>(attributes);
}

template <class Key, class Value>
    requires std::derived_from<typename Value::TUnderlying, NYTree::TYsonStruct>
const std::vector<TString>& TObjectAttributeAsYsonStructCacheBase<Key, Value>::GetAttributeNames() const
{
    return AttributeNames_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
