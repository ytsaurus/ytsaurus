#include "object_attribute_cache.h"

#include <yt/yt/ytlib/api/native/client.h>

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
    std::vector<std::string> attributeNames,
    TWeakPtr<NNative::IConnection> connection,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    NProfiling::TProfiler profiler)
    : TObjectAttributeCacheBase(
        config,
        std::move(connection),
        std::move(invoker),
        logger,
        std::move(profiler))
    , AttributeNames_(std::move(attributeNames))
    , RefreshRevisionStorage_(config->RefreshRevisionStorageSize)
{ }

NYPath::TYPath TObjectAttributeCache::GetPath(const NYPath::TYPath& key) const
{
    return key;
}

NYTree::IAttributeDictionaryPtr TObjectAttributeCache::ParseValue(const NYTree::IAttributeDictionaryPtr& attributes) const
{
    return attributes;
}

const std::vector<std::string>& TObjectAttributeCache::GetAttributeNames() const
{
    return AttributeNames_;
}

std::vector<NHydra::TRevision> TObjectAttributeCache::GetRefreshRevisions(const std::vector<NYPath::TYPath>& keys) const
{
    std::vector<NHydra::TRevision> refreshRevisions;
    refreshRevisions.reserve(keys.size());
    for (const auto& key : keys) {
        refreshRevisions.push_back(RefreshRevisionStorage_.Get(key));
    }
    return refreshRevisions;
}

void TObjectAttributeCache::InvalidateActiveAndSetRefreshRevision(const NYPath::TYPath& key, NHydra::TRevision revision)
{
    InvalidateActive(key);
    if (revision != NHydra::NullRevision) {
        RefreshRevisionStorage_.Add(key, revision);
    }
}

////////////////////////////////////////////////////////////////////////////////

TObjectAttributeCache::TRevisionStorage::TRevisionStorage(ui64 maxPathsSize)
    : MaxPathsSize_(maxPathsSize)
{ }

void TObjectAttributeCache::TRevisionStorage::Add(const NYPath::TYPath& path, NHydra::TRevision revision)
{
    Remove(path);
    if (Paths_.size() == MaxPathsSize_) {
        Remove(Paths_.begin()->second, true);
    }
    RevisionMap_[path] = revision;
    Paths_.insert({revision, path});
}

NHydra::TRevision TObjectAttributeCache::TRevisionStorage::Get(const NYPath::TYPath& path) const
{
    auto it = RevisionMap_.find(path);
    if (it != RevisionMap_.end()) {
        return it->second;
    }
    return DefaultRevision_;
}

NHydra::TRevision TObjectAttributeCache::TRevisionStorage::GetDefault() const
{
    return DefaultRevision_;
}

void TObjectAttributeCache::TRevisionStorage::Remove(const NYPath::TYPath& path, bool updateDefault)
{
    auto it = RevisionMap_.find(path);
    if (it != RevisionMap_.end()) {
        if (updateDefault) {
            DefaultRevision_ = std::max(DefaultRevision_, it->second);
        }
        Paths_.erase({it->second, path});
        RevisionMap_.erase(it);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
