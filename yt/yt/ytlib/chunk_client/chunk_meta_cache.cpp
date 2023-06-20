#include "chunk_meta_cache.h"

#include "config.h"
#include "dispatcher.h"

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 BaseChunkMetaWeight = 32;
static constexpr i64 BaseExtensionWeight = 16;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCachedChunkMeta)

class TCachedChunkMeta
    : public ICachedChunkMeta
    , public TAsyncCacheValueBase<TChunkId, TCachedChunkMeta>
{
public:
    TCachedChunkMeta(
        TChunkId chunkId,
        TRefCountedChunkMetaPtr chunkMeta)
        : TAsyncCacheValueBase(chunkId)
        , MainMeta_(std::move(chunkMeta))
    {
        for (auto& ext : *MainMeta_->mutable_extensions()->mutable_extensions()) {
            YT_VERIFY(Extensions_.emplace(ext.tag(), MakeFuture(std::make_optional(std::move(*ext.mutable_data())))).second);
        }
        MainMeta_->mutable_extensions()->clear_extensions();
    }

    TFuture<TRefCountedChunkMetaPtr> Fetch(
        std::optional<std::vector<int>> extensionTags,
        const TMetaFetchCallback& metaFetchCallback) override
    {
        // TODO(dakovalkov): support it.
        if (!extensionTags) {
            return metaFetchCallback(extensionTags);
        }

        // Precondition check.
        {
            std::sort(extensionTags->begin(), extensionTags->end());
            for (int index = 1; index < std::ssize(*extensionTags); ++index) {
                if (extensionTags.value()[index - 1] == extensionTags.value()[index]) {
                    return MakeFuture<TRefCountedChunkMetaPtr>(
                        TError("Extension tags are not unique (Tags: %v)", *extensionTags));
                }
            }
        }

        // Fast path.
        {
            std::vector<TFuture<void>> tagFutures;
            tagFutures.reserve(extensionTags->size());
            bool containsMissingExtensions = false;

            auto guard = ReaderGuard(Lock_);

            for (int tag : *extensionTags) {
                auto it = Extensions_.find(tag);
                if (it != Extensions_.end() && (!it->second.IsSet() || it->second.Get().IsOK())) {
                    tagFutures.emplace_back(it->second.AsVoid());
                } else {
                    containsMissingExtensions = true;
                    break;
                }
            }

            if (!containsMissingExtensions) {
                return AllSucceeded(tagFutures)
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
                auto missingExtensionsFuture = metaFetchCallback(missingExtensionTags);

                // Represent extensions as a map to avoid a linear scan for every tag.
                auto missingExtensionMapFuture = missingExtensionsFuture.ApplyUnique(BIND([] (TRefCountedChunkMetaPtr&& fetchedChunkMeta) {
                    THashMap<int, TString> extensionMap;
                    for (auto& ext : *fetchedChunkMeta->mutable_extensions()->mutable_extensions()) {
                        YT_VERIFY(extensionMap.emplace(ext.tag(), std::move(*ext.mutable_data())).second);
                    }
                    return extensionMap;
                }));

                for (int tag : missingExtensionTags) {
                    auto extensionFuture = missingExtensionMapFuture.Apply(
                        BIND([tag, key = GetKey()] (const THashMap<int, TString>& extensionMap) -> std::optional<TString>
                        {
                            auto it = extensionMap.find(tag);
                            if (it == extensionMap.end()) {
                                return std::nullopt;
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

    i64 GetWeight() const override
    {
        auto guard = ReaderGuard(Lock_);

        i64 weight = BaseChunkMetaWeight + Extensions_.size() * BaseExtensionWeight;

        for (const auto& [_, extensionFuture] : Extensions_) {
            // Errors can weight a lot, but they can appear in Extensions_ only in a short interval
            // between setting the future and calling OnExtensionsReceived.
            // To avoid items expiration because of heavy errors, we do not count their weight into the total.
            if (extensionFuture.IsSet() && extensionFuture.Get().IsOK() && extensionFuture.Get().Value()) {
                weight += extensionFuture.Get().Value()->Size();
            }
        }

        return weight;
    }

private:
    mutable YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);

    TRefCountedChunkMetaPtr MainMeta_;

    using TExtensionState = TFuture<std::optional<TString>>;
    THashMap<int, TExtensionState> Extensions_;

    TRefCountedChunkMetaPtr AssembleChunkMeta(const std::optional<std::vector<int>>& extensionTags) const
    {
        // Should not be called with std::nullopt (not supported yet).
        YT_VERIFY(extensionTags);

        auto guard = ReaderGuard(Lock_);

        auto meta = New<TRefCountedChunkMeta>(*MainMeta_);

        for (int tag : *extensionTags) {
            auto extensionFuture = GetOrCrash(Extensions_, tag);
            YT_VERIFY(extensionFuture.IsSet());
            YT_VERIFY(extensionFuture.Get().IsOK());
            if (extensionFuture.Get().Value()) {
                auto* ext = meta->mutable_extensions()->add_extensions();
                ext->set_tag(tag);
                ext->set_data(*extensionFuture.Get().Value());
            }
        }

        return meta;
    }

    void OnExtensionsReceived(
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
};

DEFINE_REFCOUNTED_TYPE(TCachedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

ICachedChunkMetaPtr CreateCachedChunkMeta(
    TChunkId chunkId,
    TRefCountedChunkMetaPtr chunkMeta)
{
    return New<TCachedChunkMeta>(
        std::move(chunkId),
        std::move(chunkMeta));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClientChunkMetaCache)

class TClientChunkMetaCache
    : public IClientChunkMetaCache
    , public TAsyncSlruCacheBase<TChunkId, TCachedChunkMeta>
{
public:
    TClientChunkMetaCache(
        TClientChunkMetaCacheConfigPtr config,
        TProfiler profiler)
        : TAsyncSlruCacheBase(
            std::move(config),
            std::move(profiler))
        , Invoker_(TDispatcher::Get()->GetReaderInvoker())
    { }

    TFuture<TRefCountedChunkMetaPtr> Fetch(
        TChunkId chunkId,
        const std::optional<std::vector<int>>& extensionTags,
        const TMetaFetchCallback& metaFetchCallback) override
    {
        // Using either erasure chunk part id or journal chunk id as a key is forbidden.
        YT_VERIFY(IsBlobChunkId(chunkId));

        // TODO(dakovalkov): Support it.
        if (!extensionTags) {
            return metaFetchCallback(extensionTags);
        }

        return BIND(&TClientChunkMetaCache::DoFetch, MakeStrong(this), chunkId, extensionTags, metaFetchCallback)
            .AsyncVia(Invoker_)
            .Run();
    }

protected:
    i64 GetWeight(const TCachedChunkMetaPtr& value) const override
    {
        return value->GetWeight();
    }

private:
    const IInvokerPtr Invoker_;

    TRefCountedChunkMetaPtr DoFetch(
        TChunkId chunkId,
        const std::optional<std::vector<int>>& extensionTags,
        const TMetaFetchCallback& metaFetchCallback)
    {
        auto cookie = BeginInsert(chunkId);

        if (cookie.IsActive()) {
            auto metaFuture = metaFetchCallback(extensionTags);
            auto metaOrError = WaitFor(metaFuture);
            if (metaOrError.IsOK()) {
                cookie.EndInsert(New<TCachedChunkMeta>(chunkId, metaOrError.Value()));
            } else {
                auto error = TError(
                    NChunkClient::EErrorCode::ChunkMetaCacheFetchFailed,
                    "Error fetching meta for chunk %v",
                    chunkId)
                    << metaOrError;
                cookie.Cancel(error);
                THROW_ERROR(error);
            }
        }

        auto cachedMeta = WaitFor(cookie.GetValue())
            .ValueOrThrow();

        return WaitFor(cachedMeta->Fetch(extensionTags, metaFetchCallback))
            .ValueOrThrow();
    }
};

DEFINE_REFCOUNTED_TYPE(TClientChunkMetaCache)

////////////////////////////////////////////////////////////////////////////////

IClientChunkMetaCachePtr CreateClientChunkMetaCache(
    TClientChunkMetaCacheConfigPtr config,
    TProfiler profiler)
{
    return New<TClientChunkMetaCache>(
        std::move(config),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
