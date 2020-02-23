#include "slab_allocator.h"

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

TArenaPool::TArenaPool(
    size_t rank,
    size_t batchSize,
    IMemoryUsageTrackerPtr memoryTracker)
    : TDeleterBase(&Deallocate)
    , ChunkSize_(NYTAlloc::SmallRankToSize[rank])
    , BatchSize_(batchSize)
    , MemoryTracker_(std::move(memoryTracker))
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    , Cookie_(GetRefCountedTypeCookie<TArenaPool>())
#endif
{ }

void* TArenaPool::Allocate()
{
    while (true) {
        void* obj = FreeList_.Extract();
        if (Y_LIKELY(obj)) {
            ++RefCount_;
            return obj;
        }
        AllocateMore();
    }
}

TArenaPool::~TArenaPool()
{
    FreeList_.ExtractAll();

    if (MemoryTracker_) {
        size_t totalSize = SegmentsCount_.load() * (sizeof(TFreeListItem) + ChunkSize_ * BatchSize_);
        MemoryTracker_->Release(totalSize);
    }

    auto* segment = Segments_.ExtractAll();
    while (segment) {
        auto* next = segment->Next;
        NYTAlloc::Free(segment);
        segment = next;
    }
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

void TArenaPool::AllocateMore()
{
    auto totalSize = sizeof(TFreeListItem) + ChunkSize_ * BatchSize_;

    if (MemoryTracker_) {
        MemoryTracker_->TryAcquire(totalSize)
            .ThrowOnError();
    }

    auto* ptr = NYTAlloc::Allocate(totalSize);

    // Save segments in list to free them in destructor.
    Segments_.Put(static_cast<TFreeListItem*>(ptr));

    auto* objs = static_cast<char*>(ptr) + sizeof(TFreeListItem);

    ++SegmentsCount_;

    for (size_t index = 0; index < BatchSize_; ++index) {
        FreeList_.Put(reinterpret_cast<TFreeListItem*>(objs + ChunkSize_ * index));
    }
}

/////////////////////////////////////////////////////////////////////////////

TSlabAllocator::TSlabAllocator(IMemoryUsageTrackerPtr memoryTracker)
{
    for (size_t rank = 2; rank < NYTAlloc::SmallRankCount; ++rank) {
        // Rank is not used.
        if (rank == 3) {
            continue;
        }
        SmallArenas_[rank].reset(new TArenaPool(rank, SegmentSize / rank, memoryTracker));
    }
}

void* TSlabAllocator::Allocate(size_t size)
{
    void* ptr = nullptr;
    if (size < NYTAlloc::LargeSizeThreshold) {
        auto rank = NYTAlloc::SizeToSmallRank(size);
        ptr = SmallArenas_[rank]->Allocate();
    } else {
        ptr = NYTAlloc::Allocate(size);
    }

    return ptr;
}

TDeleterBase* TSlabAllocator::GetDeleter(size_t size)
{
    if (size < NYTAlloc::LargeSizeThreshold) {
        auto rank = NYTAlloc::SizeToSmallRank(size);
        return SmallArenas_[rank].get();
    } else {
        return &DefaultDeleter;
    }
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
