#include "object_attribute_cache.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/options.h>

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
    , RefreshRevisionCache_(config->RefreshRevisionCacheCapacity)
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
        refreshRevisions.push_back(RefreshRevisionCache_.Get(key));
    }
    return refreshRevisions;
}

void TObjectAttributeCache::InvalidateActiveAndSetRefreshRevision(const NYPath::TYPath& key, NHydra::TRevision revision)
{
    InvalidateActive(key);
    if (revision != NHydra::NullRevision) {
        RefreshRevisionCache_.Add(key, revision);
    }
}

////////////////////////////////////////////////////////////////////////////////

TObjectAttributeCache::TRevisionCache::TRevisionCache(int maxPathsSize)
    : MaxPathsSize_(maxPathsSize)
{ }

void TObjectAttributeCache::TRevisionCache::Add(const NYPath::TYPath& path, NHydra::TRevision revision)
{
    auto guard = Guard(Lock_);
    Remove(path);
    if (std::ssize(Paths_) == MaxPathsSize_) {
        Remove(Paths_.begin()->second, true);
    }
    RevisionMap_[path] = revision;
    Paths_.insert({revision, path});
}

NHydra::TRevision TObjectAttributeCache::TRevisionCache::Get(const NYPath::TYPath& path) const
{
    auto guard = Guard(Lock_);
    auto it = RevisionMap_.find(path);
    if (it != RevisionMap_.end()) {
        return it->second;
    }
    return DefaultRevision_;
}

NHydra::TRevision TObjectAttributeCache::TRevisionCache::GetDefault() const
{
    return DefaultRevision_;
}

void TObjectAttributeCache::TRevisionCache::Remove(const NYPath::TYPath& path, bool updateDefault)
{
    YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
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
