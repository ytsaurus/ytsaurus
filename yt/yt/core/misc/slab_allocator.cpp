#include "slab_allocator.h"

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

TArenaPool::TArenaPool(
    size_t rank,
    size_t segmentSize,
    IMemoryUsageTrackerPtr memoryTracker)
    : ChunkSize_(NYTAlloc::SmallRankToSize[rank])
    , BatchSize_(segmentSize / ChunkSize_)
    , MemoryTracker_(std::move(memoryTracker))
{
    YT_VERIFY(ChunkSize_ % sizeof(TFreeListItem) == 0);
}

void* TArenaPool::Allocate()
{
    while (true) {
        void* obj = FreeList_.Extract();
        if (Y_LIKELY(obj)) {
            ++RefCount_;
            return obj;
        }
        if (!AllocateMore()) {
            return nullptr;
        }
    }
}

TArenaPool::~TArenaPool()
{
    FreeList_.ExtractAll();

    size_t segmentCount = 0;
    auto* segment = Segments_.ExtractAll();
    while (segment) {
        auto* next = segment->Next.load(std::memory_order_acquire);
        NYTAlloc::Free(segment);
        segment = next;
        ++segmentCount;
    }

    size_t totalSize = segmentCount * (sizeof(TFreeListItem) + ChunkSize_ * BatchSize_);
    if (MemoryTracker_) {
        MemoryTracker_->Release(totalSize);
    }

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTrackerFacade::FreeSpace(GetRefCountedTypeCookie<TArenaPool>(), totalSize);
#endif
}

void TArenaPool::Free(void* obj)
{
    FreeList_.Put(static_cast<TFreeListItem*>(obj));
    Unref();
}

size_t TArenaPool::Unref()
{
    auto count = --RefCount_;
    if (count > 0) {
        return count;
    }

    delete this;
    return 0;
}

bool TArenaPool::AllocateMore()
{
    // For large chunks it is better to allocate SegmentSize + sizeof(TFreeListItem) space
    // than aloocate SegmentSize and use BatchSize_ - 1.
    auto totalSize = sizeof(TFreeListItem) + ChunkSize_ * BatchSize_;

    if (MemoryTracker_ && !MemoryTracker_->TryAcquire(totalSize).IsOK()) {
        return false;
    }

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTrackerFacade::AllocateSpace(GetRefCountedTypeCookie<TArenaPool>(), totalSize);
#endif

    auto* ptr = NYTAlloc::Allocate(totalSize);

    // Save segments in list to free them in destructor.
    Segments_.Put(static_cast<TFreeListItem*>(ptr));

    // First TFreeListItem is for chain of segments.
    auto head = static_cast<TFreeListItem*>(ptr) + 1;
    auto current = head;
    auto step = ChunkSize_ / sizeof(TFreeListItem);

    // Build chain of chunks.
    auto chunkCount = BatchSize_;
    while (chunkCount-- > 1) {
        auto next = current + step;
        current->Next.store(next, std::memory_order_release);
        current = next;
    }
    current->Next.store(nullptr, std::memory_order_release);

    FreeList_.Put(head, current);

    return true;
}

/////////////////////////////////////////////////////////////////////////////

TSlabAllocator::TSlabAllocator(IMemoryUsageTrackerPtr memoryTracker)
    : MemoryTracker_(std::move(memoryTracker))
{
    for (size_t rank = 2; rank < NYTAlloc::SmallRankCount; ++rank) {
        // Rank is not used.
        if (rank == 3) {
            continue;
        }
        // There is no std::make_unique overload with custom deleter.
        SmallArenas_[rank].reset(new TArenaPool(rank, SegmentSize, MemoryTracker_));
    }
}

void* TSlabAllocator::Allocate(size_t size)
{
    size += sizeof(TArenaPool*);

    TArenaPool* arena = nullptr;
    void* ptr = nullptr;
    if (size < NYTAlloc::LargeAllocationSizeThreshold) {
        auto rank = NYTAlloc::SizeToSmallRank(size);
        arena = SmallArenas_[rank].get();
        ptr = arena->Allocate();
    } else {
        ptr = NYTAlloc::Allocate(size);
    }

    if (!ptr) {
        return nullptr;
    }

    // Mutes TSAN data race with write Next in TFreeList::Push.
    auto* header = static_cast<std::atomic<void*>*>(ptr);
    header->store(arena, std::memory_order_release);

    return header + 1;
}

void TSlabAllocator::Free(void* ptr)
{
    YT_VERIFY(ptr);
    auto* header = static_cast<TArenaPool**>(ptr) - 1;
    auto* arena = *header;
    if (arena) {
        arena->Free(header);
    } else {
        NYTAlloc::Free(header);
    }
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
