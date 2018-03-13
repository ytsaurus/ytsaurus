#include "chunked_memory_pool.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const i64 TChunkedMemoryPool::DefaultChunkSize = 4096;
const double TChunkedMemoryPool::DefaultMaxSmallBlockSizeRatio = 0.25;

////////////////////////////////////////////////////////////////////////////////

class TMemoryChunkProvider
    : public IMemoryChunkProvider
{
public:
    explicit TMemoryChunkProvider(i64 chunkSize)
        : ChunkSize_(chunkSize)
    { }

    virtual TSharedMutableRef Allocate(TRefCountedTypeCookie cookie) override
    {
        return TSharedMutableRef::Allocate(ChunkSize_, false, cookie);
    }

    virtual size_t GetChunkSize() const
    {
        return ChunkSize_;
    }

private:
    const size_t ChunkSize_;
};

IMemoryChunkProviderPtr CreateMemoryChunkProvider(i64 chunkSize)
{
    return New<TMemoryChunkProvider>(chunkSize);
}

////////////////////////////////////////////////////////////////////////////////

TChunkedMemoryPool::TChunkedMemoryPool(
    double maxSmallBlockSizeRatio,
    TRefCountedTypeCookie tagCookie,
    IMemoryChunkProviderPtr chunkProvider)
    : ChunkSize_(chunkProvider->GetChunkSize())
    , MaxSmallBlockSize_(static_cast<i64>(ChunkSize_ * maxSmallBlockSizeRatio))
    , TagCookie_(tagCookie)
    , ChunkProvider_(std::move(chunkProvider))
{
    SetupFreeZone();
}

void TChunkedMemoryPool::Purge()
{
    Chunks_.clear();
    LargeBlocks_.clear();
    Size_ = 0;
    Capacity_ = 0;
    CurrentChunkIndex_ = 0;
    FirstChunkBegin_ = FirstChunkEnd_ = nullptr;
    SetupFreeZone();
}

char* TChunkedMemoryPool::AllocateUnalignedSlow(i64 size)
{
    auto* large = AllocateSlowCore(size);
    if (large) {
        return large;
    }
    return AllocateUnaligned(size);
}

char* TChunkedMemoryPool::AllocateAlignedSlow(i64 size, int align)
{
    // NB: Do not rely on any particular alignment of chunks.
    auto* large = AllocateSlowCore(size + align);
    if (large) {
        return AlignUp(large, align);
    }
    return AllocateAligned(size, align);
}

char* TChunkedMemoryPool::AllocateSlowCore(i64 size)
{
    if (size > MaxSmallBlockSize_) {
        auto block = TSharedMutableRef::Allocate(size, false, TagCookie_);
        LargeBlocks_.push_back(block);
        Size_ += size;
        Capacity_ += size;
        return block.Begin();
    }

    if (CurrentChunkIndex_ + 1 >= Chunks_.size()) {
        auto chunk = ChunkProvider_->Allocate(TagCookie_);

        if (Chunks_.empty()) {
            FirstChunkBegin_ = chunk.Begin();
            FirstChunkEnd_ = chunk.End();
        }
        Chunks_.push_back(std::move(chunk));

        Capacity_ += ChunkSize_;
        CurrentChunkIndex_ = static_cast<int>(Chunks_.size()) - 1;
    } else {
        ++CurrentChunkIndex_;
    }

    SetupFreeZone();

    return nullptr;
}

void TChunkedMemoryPool::ClearSlow()
{
    CurrentChunkIndex_ = 0;
    Size_ = 0;
    SetupFreeZone();

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

void TChunkedMemoryPool::SetupFreeZone()
{
    if (CurrentChunkIndex_ >= Chunks_.size()) {
        FreeZoneBegin_ = nullptr;
        FreeZoneEnd_ = nullptr;
    } else {
        auto& chunk = Chunks_[CurrentChunkIndex_];
        FreeZoneBegin_ = chunk.Begin();
        FreeZoneEnd_ = chunk.End();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
