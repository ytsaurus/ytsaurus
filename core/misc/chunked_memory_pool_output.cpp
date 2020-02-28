#include "chunked_memory_pool_output.h"

#include "chunked_memory_pool.h"
#include "ref.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TChunkedMemoryPoolOutput::TChunkedMemoryPoolOutput(TChunkedMemoryPool* pool, size_t chunkSize)
    : Pool_(pool)
    , ChunkSize_(chunkSize)
{ }

size_t TChunkedMemoryPoolOutput::DoNext(void** ptr)
{
    if (Begin_ != nullptr) {
        Refs_.emplace_back(Begin_, End_);
    }
    // Use |AllocateAligned| to free memory efficiently.
    Begin_ = Pool_->AllocateAligned(ChunkSize_, /* align */ 1);
    End_ = Begin_ + ChunkSize_;
    *ptr = Begin_;
    return ChunkSize_;
}

void TChunkedMemoryPoolOutput::DoUndo(size_t size)
{
    YT_VERIFY(Begin_ + size <= End_);
    Pool_->Free(End_ - size, End_);
    End_ -= size;
}

std::vector<TRef> TChunkedMemoryPoolOutput::FinishAndGetRefs()
{
    if (Begin_ != nullptr) {
        Refs_.emplace_back(Begin_, End_);
    }
    return std::move(Refs_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

