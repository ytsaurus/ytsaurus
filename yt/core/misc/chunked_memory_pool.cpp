#include "stdafx.h"
#include "chunked_memory_pool.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const size_t TChunkedMemoryPool::DefaultChunkSize = 4096;
const double TChunkedMemoryPool::DefaultMaxSmallBlockSizeRatio = 0.25;

////////////////////////////////////////////////////////////////////////////////

TChunkedMemoryPool::TChunkedMemoryPool(
    size_t chunkSize,
    double maxSmallBlockSizeRatio,
    void* tagCookie)
    : ChunkSize_ ((chunkSize + 7) & ~7) // must be aligned
    , MaxSmallBlockSize_(static_cast<size_t>(ChunkSize_ * maxSmallBlockSizeRatio))
    , TagCookie_(tagCookie)
    , CurrentChunkIndex_(0)
    , Size_(0)
    , Capacity_(0)
{
    SetupPointers();
}

char* TChunkedMemoryPool::AllocateUnalignedSlow(size_t size)
{
    if (size > MaxSmallBlockSize_) {
        return AllocateLargeBlock(size).Begin();
    }

    if (CurrentChunkIndex_ + 1 >= Chunks_.size()) {
        AllocateChunk();
    } else {
        SwitchChunk();
    }

    return AllocateUnaligned(size);
}

void TChunkedMemoryPool::Clear()
{
    CurrentChunkIndex_ = 0;
    Size_ = 0;
    SetupPointers();

    for (const auto& block : LargeBlocks_) {
        Capacity_ -= block.Size();
    }
    LargeBlocks_.clear();
}

i64 TChunkedMemoryPool::GetSize() const
{
    return Size_;
}

i64 TChunkedMemoryPool::GetCapacity() const
{
    return Capacity_;
}

void TChunkedMemoryPool::AllocateChunk()
{
    auto chunk = TSharedRef::Allocate(ChunkSize_, false, TagCookie_);
    YCHECK((reinterpret_cast<intptr_t>(chunk.Begin()) & 7) == 0);
    Chunks_.push_back(chunk);
    Capacity_ += ChunkSize_;
    CurrentChunkIndex_ = static_cast<int>(Chunks_.size()) - 1;
    SetupPointers();
}

void TChunkedMemoryPool::SwitchChunk()
{
    ++CurrentChunkIndex_;
    SetupPointers();
}

void TChunkedMemoryPool::SetupPointers()
{
    if (CurrentChunkIndex_ >= Chunks_.size()) {
        CurrentPtr_ = nullptr;
        EndPtr_ = nullptr;
    } else {
        auto& chunk = Chunks_[CurrentChunkIndex_];
        CurrentPtr_ = chunk.Begin();
        EndPtr_ = chunk.End();    
    }
}

TSharedRef TChunkedMemoryPool::AllocateLargeBlock(size_t size)
{
    auto block = TSharedRef::Allocate(size, false, TagCookie_);
    YCHECK((reinterpret_cast<intptr_t>(block.Begin()) & 7) == 0);
    LargeBlocks_.push_back(block);
    Capacity_ += size;
    return block;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
