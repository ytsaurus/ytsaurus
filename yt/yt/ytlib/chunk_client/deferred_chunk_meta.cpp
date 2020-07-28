#include "deferred_chunk_meta.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void TDeferredChunkMeta::PushCallback(std::function<void(TDeferredChunkMeta*)> callback)
{
    Callbacks_.emplace_back(std::move(callback));
}

void TDeferredChunkMeta::Finalize()
{
    YT_VERIFY(!Finalized_);

    for (auto& callback : Callbacks_) {
        callback(this);
        callback = nullptr;
    }
    Callbacks_.clear();

    Finalized_ = true;
}

bool TDeferredChunkMeta::IsFinalized() const
{
    return Finalized_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
