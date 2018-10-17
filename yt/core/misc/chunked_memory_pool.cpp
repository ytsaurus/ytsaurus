#include "chunked_memory_pool.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const size_t TChunkedMemoryPool::DefaultStartChunkSize = 4_KB;
const size_t TChunkedMemoryPool::RegularChunkSize = 36_KB - 512;

////////////////////////////////////////////////////////////////////////////////

TAllocationHolder::TAllocationHolder(TMutableRef ref, TRefCountedTypeCookie cookie)
    : Ref_(ref)
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    , Cookie_(cookie)
#endif
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    if (Cookie_ != NullRefCountedTypeCookie) {
        TRefCountedTrackerFacade::AllocateTagInstance(Cookie_);
        TRefCountedTrackerFacade::AllocateSpace(Cookie_, Ref_.Size());
    }
#endif
}

TAllocationHolder::~TAllocationHolder()
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    if (Cookie_ != NullRefCountedTypeCookie) {
        TRefCountedTrackerFacade::FreeTagInstance(Cookie_);
        TRefCountedTrackerFacade::FreeSpace(Cookie_, Ref_.Size());
    }
#endif
}

    ////////////////////////////////////////////////////////////////////////////////

class TMemoryChunkProvider
    : public IMemoryChunkProvider
{
public:
    virtual std::unique_ptr<TAllocationHolder> Allocate(size_t size, TRefCountedTypeCookie cookie) override
    {
        return std::unique_ptr<TAllocationHolder>(TAllocationHolder::Allocate<TAllocationHolder>(size, cookie));
    }
};

IMemoryChunkProviderPtr CreateMemoryChunkProvider()
{
    return New<TMemoryChunkProvider>();
}

////////////////////////////////////////////////////////////////////////////////

TChunkedMemoryPool::TChunkedMemoryPool(
    TRefCountedTypeCookie tagCookie,
    IMemoryChunkProviderPtr chunkProvider,
    size_t startChunkSize)
    : TagCookie_(tagCookie)
    , ChunkProvider_(std::move(chunkProvider))
    , NextSmallSize_(startChunkSize)
{
    FreeZoneBegin_ = nullptr;
    FreeZoneEnd_ = nullptr;
}

void TChunkedMemoryPool::Purge()
{
    Chunks_.clear();
    OtherBlocks_.clear();
    Size_ = 0;
    Capacity_ = 0;
    NextChunkIndex_ = 0;
    FreeZoneBegin_ = nullptr;
    FreeZoneEnd_ = nullptr;
}

char* TChunkedMemoryPool::AllocateUnalignedSlow(size_t size)
{
    auto* large = AllocateSlowCore(size);
    if (large) {
        return large;
    }
    return AllocateUnaligned(size);
}

char* TChunkedMemoryPool::AllocateAlignedSlow(size_t size, int align)
{
    // NB: Do not rely on any particular alignment of chunks.
    auto* large = AllocateSlowCore(size + align);
    if (large) {
        return AlignUp(large, align);
    }
    return AllocateAligned(size, align);
}

char* TChunkedMemoryPool::AllocateSlowCore(size_t size)
{
    if (size > RegularChunkSize) {
        auto block = ChunkProvider_->Allocate(size, TagCookie_);
        auto result = block->GetRef().Begin();
        OtherBlocks_.push_back(std::move(block));
        Size_ += size;
        Capacity_ += size;
        return result;
    }

    YCHECK(NextChunkIndex_ <= Chunks_.size());

    TMutableRef ref;

    if (NextSmallSize_ < RegularChunkSize) {
        auto block = ChunkProvider_->Allocate(std::max(NextSmallSize_, size), TagCookie_);
        ref = block->GetRef();
        Capacity_ += ref.Size();
        OtherBlocks_.push_back(std::move(block));
        NextSmallSize_ = 2 * ref.Size();
    } else if (NextChunkIndex_ == Chunks_.size()) {
        auto chunk = ChunkProvider_->Allocate(RegularChunkSize, TagCookie_);
        ref = chunk->GetRef();
        Capacity_ += ref.Size();
        Chunks_.push_back(std::move(chunk));
        ++NextChunkIndex_;
    } else {
        ref = Chunks_[NextChunkIndex_++]->GetRef();
    }

    FreeZoneBegin_ = ref.Begin();
    FreeZoneEnd_ = ref.End();

    return nullptr;
}

size_t TChunkedMemoryPool::GetSize() const
{
    return Size_;
}

size_t TChunkedMemoryPool::GetCapacity() const
{
    return Capacity_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
