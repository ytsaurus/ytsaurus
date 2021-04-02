#include "chunk_meta_cache.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 BaseChunkMetaWeight = 32;
static constexpr i64 BaseExtensionWeight = 16;

////////////////////////////////////////////////////////////////////////////////

TCachedChunkMeta::TCachedChunkMeta(
    TChunkId chunkId,
    TRefCountedChunkMetaPtr chunkMeta)
    : TAsyncCacheValueBase(chunkId)
    , MainMeta_(std::move(chunkMeta))
{
    for (auto& ext : *MainMeta_->mutable_extensions()->mutable_extensions()) {
        YT_VERIFY(Extensions_.emplace(ext.tag(), MakeFuture(std::move(*ext.mutable_data()))).second);
    }
    MainMeta_->mutable_extensions()->clear_extensions();
}

TFuture<TRefCountedChunkMetaPtr> TCachedChunkMeta::Fetch(
    std::optional<std::vector<int>> extensionTags,
    const TMetaFetchCallback& metaFetchCallback)
{
    // TODO(dakovalkov): support it.
    if (!extensionTags) {
        return metaFetchCallback(GetKey(), extensionTags);
    }

    // Precondition check.
    {
        std::sort(extensionTags->begin(), extensionTags->end());
        for (int index = 1; index < extensionTags->size(); ++index) {
            if (extensionTags.value()[index - 1] == extensionTags.value()[index]) {
                return MakeFuture<TRefCountedChunkMetaPtr>(
                    TError("Extension tags are not unique (Tags: %v)", *extensionTags));
            }
        }
    }

    // Fast path.
    {
        std::vector<TFuture<void>> tagFtures;
        tagFtures.reserve(extensionTags->size());
        bool containsMissingExtensions = false;

        auto guard = ReaderGuard(Lock_);
        
        for (int tag : *extensionTags) {
            auto it = Extensions_.find(tag);
            if (it != Extensions_.end() && (!it->second.IsSet() || it->second.Get().IsOK())) {
                tagFtures.emplace_back(it->second.AsVoid());
            } else {
                containsMissingExtensions = true;
                break;
            }
        }

        if (!containsMissingExtensions) {
            return AllSucceeded(tagFtures)
                .Apply(BIND(&TCachedChunkMeta::AssembleChunkMeta, MakeStrong(this), extensionTags));
        }
    }

    // Slow path.
    {
        std::vector<int> missingExtensionTags;
        std::vector<TFuture<void>> tagFutures;
        tagFutures.reserve(extensionTags->size());

        auto guard = WriterGuard(Lock_);

        for (int tag : *extensionTags) {
            auto it = Extensions_.find(tag);
            if (it == Extensions_.end()) {
                missingExtensionTags.push_back(tag);
                continue;
            }

            // Ignore errors since we do not cache them.
            if (it->second.IsSet() && !it->second.Get().IsOK()) {
                Extensions_.erase(it);
                missingExtensionTags.push_back(tag);
                continue;
            }

            tagFutures.emplace_back(it->second.AsVoid());
        }

        std::vector<TFuture<void>> newTagFutures;
        newTagFutures.reserve(missingExtensionTags.size());

        // Since we re-acquire the lock after the fast path, it's possible that there are no missing extensions now.
        if (!missingExtensionTags.empty()) {
            auto missingExtensionsFuture = metaFetchCallback(GetKey(), missingExtensionTags);

            // Represent extensions as a map to avoid a linear scan for every tag.
            auto missingExtensionMapFuture = missingExtensionsFuture.ApplyUnique(BIND([] (TRefCountedChunkMetaPtr&& fetchedChunkMeta) {
                THashMap<int, TString> extensionMap;
                for (auto& ext : *fetchedChunkMeta->mutable_extensions()->mutable_extensions()) {
                    YT_VERIFY(extensionMap.emplace(ext.tag(), std::move(*ext.mutable_data())).second);
                }
                return extensionMap;
            }));

            for (int tag : missingExtensionTags) {
                auto extensionFuture = missingExtensionMapFuture.Apply(BIND([tag, key = GetKey()] (const THashMap<int, TString>& extensionMap) {
                    auto it = extensionMap.find(tag);
                    if (it == extensionMap.end()) {
                        THROW_ERROR_EXCEPTION("Missing chunk meta extension (ChunkId: %v, Tag: %v)", key, tag);
                    }
                    // TODO(dakovalkov): We create an extension copy here.
                    // It's almost free as long as TString is ref-counted.
                    // If TString ever becomes std::string, we will need to find another way.
                    return it->second; 
                }));

                YT_VERIFY(Extensions_.emplace(tag, extensionFuture).second);
                newTagFutures.emplace_back(extensionFuture.AsVoid());
                tagFutures.emplace_back(extensionFuture.AsVoid());
            }
        }

        // AssembleChunkMeta and OnExtensionsReceived can acquire a Lock_.
        // If all futures have already been set, we will call these methods from this fiber
        // before WriterGuard is destoyed. To avoid deadlock, release the lock manually.
        guard.Release();

        if (!missingExtensionTags.empty()) {
            // To update weight and delete errors from Extensions_.
            AllSet(newTagFutures)
                .Subscribe(BIND(&TCachedChunkMeta::OnExtensionsReceived, MakeStrong(this), std::move(missingExtensionTags)));
        }

        return AllSucceeded(tagFutures)
            .Apply(BIND(&TCachedChunkMeta::AssembleChunkMeta, MakeStrong(this), std::move(extensionTags)));
    }
}

i64 TCachedChunkMeta::GetWeight() const
{
    auto guard = ReaderGuard(Lock_);

    i64 weight = BaseChunkMetaWeight + Extensions_.size() * BaseExtensionWeight;

    for (const auto& [_, extensionFuture] : Extensions_) {
        // Errors can weight a lot, but they can appear in Extensions_ only in a short interval
        // between setting the future and calling OnExtensionsReceived.
        // To avoid items expiration because of heavy errors, we do not count their weight into the total.
        if (extensionFuture.IsSet() && extensionFuture.Get().IsOK()) {
            weight += extensionFuture.Get().Value().Size();
        }
    }

    return weight;
}

TRefCountedChunkMetaPtr TCachedChunkMeta::AssembleChunkMeta(const std::optional<std::vector<int>>& extensionTags) const
{
    // Should not be called with std::nullopt (not supported yet).
    YT_VERIFY(extensionTags);

    auto guard = ReaderGuard(Lock_);

    auto meta = New<TRefCountedChunkMeta>(*MainMeta_);

    for (int tag : *extensionTags) {
        auto extensionFuture = GetOrCrash(Extensions_, tag);
        YT_VERIFY(extensionFuture.IsSet());
        YT_VERIFY(extensionFuture.Get().IsOK());
        auto* ext = meta->mutable_extensions()->add_extensions();
        ext->set_tag(tag);
        ext->set_data(extensionFuture.Get().Value());
    }

    return meta;
}

void TCachedChunkMeta::OnExtensionsReceived(
    const std::optional<std::vector<int>>& extensionTags,
    const TError& error)
{
    // Should not be called with std::nullopt (not supported yet).
    YT_VERIFY(extensionTags);

    if (!error.IsOK()) {
        auto guard = WriterGuard(Lock_);

        for (int tag : *extensionTags) {
            auto it = Extensions_.find(tag);
            // The error could be already deleted by a concurrent fetch of the same tag.
            if (it != Extensions_.end() && it->second.IsSet() && !it->second.Get().IsOK()) {
                Extensions_.erase(it);
            }
        }
    }

    UpdateWeight();
}

////////////////////////////////////////////////////////////////////////////////

TClientChunkMetaCache::TClientChunkMetaCache(
    TSlruCacheConfigPtr config,
    IInvokerPtr invoker)
    : TAsyncSlruCacheBase(std::move(config))
    , Invoker_(std::move(invoker))
{ }

TFuture<TRefCountedChunkMetaPtr> TClientChunkMetaCache::Fetch(
    TChunkId chunkId,
    const std::optional<std::vector<int>>& extensionTags,
    const TMetaFetchCallback& metaFetchCallback)
{
    // TODO(dakovalkov): Support it.
    if (!extensionTags) {
        return metaFetchCallback(chunkId, extensionTags);
    }

    return BIND(&TClientChunkMetaCache::DoFetch, MakeStrong(this), chunkId, extensionTags, metaFetchCallback)
        .AsyncVia(Invoker_)
        .Run();
}

i64 TClientChunkMetaCache::GetWeight(const TCachedChunkMetaPtr& chunkMeta) const
{
    return chunkMeta->GetWeight();
}

TRefCountedChunkMetaPtr TClientChunkMetaCache::DoFetch(
    TChunkId chunkId,
    const std::optional<std::vector<int>>& extensionTags,
    const TMetaFetchCallback& metaFetchCallback)
{
    auto cookie = BeginInsert(chunkId);

    if (cookie.IsActive()) {
        auto metaFuture = metaFetchCallback(chunkId, extensionTags);
        auto meta = WaitFor(metaFuture)
            .ValueOrThrow();
        cookie.EndInsert(New<TCachedChunkMeta>(chunkId, meta));
    }

    auto cachedMeta = WaitFor(cookie.GetValue())
        .ValueOrThrow();

    return WaitFor(cachedMeta->Fetch(extensionTags, metaFetchCallback))
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
