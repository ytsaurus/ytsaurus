#include "stdafx.h"
#include "chunked_memory_pool.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const size_t TChunkedMemoryPool::DefaultChunkSize = 4096;
const double TChunkedMemoryPool::DefaultMaxSmallBlockSizeRatio = 0.25;

////////////////////////////////////////////////////////////////////////////////

void TChunkedMemoryPool::Initialize(
    size_t chunkSize,
    double maxSmallBlockSizeRatio,
    void* tagCookie)
{
    ChunkSize_ = (chunkSize + 7) & ~7; // must be aligned
    MaxSmallBlockSize_ = static_cast<size_t>(ChunkSize_ * maxSmallBlockSizeRatio);
    TagCookie_ = tagCookie;
    CurrentChunkIndex_ = 0;
    CurrentOffset_ = 0;
    Size_ = 0;
    Capacity_ = 0;
}

char* TChunkedMemoryPool::AllocateUnaligned(size_t size)
{
    while (true) {
        if (CurrentChunkIndex_ == Chunks_.size()) {
            AllocateChunk();
        }

        auto& chunk = Chunks_[CurrentChunkIndex_];
        if (CurrentOffset_ + size < chunk.Size()) {
            auto* result = chunk.Begin() + CurrentOffset_;
            CurrentOffset_ += size;
            Size_ += size;
            return result;
        } 

        if (size > MaxSmallBlockSize_) {
            return AllocateLargeBlock(size).Begin();
        }

        CurrentOffset_ = 0;
        ++CurrentChunkIndex_;
    }
}

char* TChunkedMemoryPool::Allocate(size_t size)
{
    CurrentOffset_ = (CurrentOffset_ + 7) & ~7;
    return AllocateUnaligned(size);
}

void TChunkedMemoryPool::Clear()
{
    CurrentOffset_ = 0;
    CurrentChunkIndex_ = 0;

    Size_ = 0;

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
    Chunks_.push_back(AllocateLargeBlock(ChunkSize_));
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
