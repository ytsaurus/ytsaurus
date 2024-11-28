#include "deferred_chunk_meta.h"

#include "chunk_meta_extensions.h"

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void TDeferredChunkMeta::RegisterFinalizer(std::function<void(TDeferredChunkMeta*)> finalizer)
{
    YT_VERIFY(!Finalized_);
    Finalizers_.emplace_back(std::move(finalizer));
}

void TDeferredChunkMeta::SubscribeMetaFinalized(TCallback<void(const TRefCountedChunkMeta*)> callback)
{
    YT_VERIFY(!Finalized_);
    FinalizationSubscribers_.emplace_back(std::move(callback));
}

void TDeferredChunkMeta::Finalize()
{
    YT_VERIFY(!Finalized_);

    for (auto& finalizer : Finalizers_) {
        finalizer(this);
        finalizer = nullptr;
    }
    Finalizers_.clear();

    for (auto& finalizationSubscriber : FinalizationSubscribers_) {
        finalizationSubscriber(this);
        finalizationSubscriber.Reset();
    }
    FinalizationSubscribers_.clear();

    Finalized_ = true;
}

bool TDeferredChunkMeta::IsFinalized() const
{
    return Finalized_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
