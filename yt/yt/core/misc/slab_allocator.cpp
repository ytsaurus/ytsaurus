#include "slab_allocator.h"

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

static constexpr size_t SegmentSize = 64_KB;
static_assert(SegmentSize >= NYTAlloc::LargeAllocationSizeThreshold, "Segment size violation");

constexpr size_t AcquireMemoryGranularity = 500_KB;
static_assert(AcquireMemoryGranularity % 2 == 0, "Must be divisible by 2");

class TSegment;

struct TFreeListItem
    : public TFreeListItemBase<TFreeListItem>
{
    TSegment* Segment;
};

class TSmallArena
    : public TRefTracked<TSmallArena>
{
public:
    TSmallArena(
        size_t rank,
        size_t segmentSize,
        IMemoryUsageTrackerPtr memoryTracker)
        : ObjectSize_(NYTAlloc::SmallRankToSize[rank])
        , ObjectCount_(segmentSize / ObjectSize_)
        , MemoryTracker_(std::move(memoryTracker))
    {
        YT_VERIFY(ObjectCount_ > 0);
    }

    TFreeListItem* Allocate();

    void ClearFreeList();

    size_t Unref();

private:
    const size_t ObjectSize_;
    const size_t ObjectCount_;
    const IMemoryUsageTrackerPtr MemoryTracker_;

    TFreeList<TFreeListItem> FreeList_;
    // One ref from allocator plus refs from segments.
    std::atomic<size_t> RefCount_ = 1;

    TFreeListItem* AllocateSlow();

    friend TSegment;
};

class TSegment
    : public TRefTracked<TSegment>
{
public:
    explicit TSegment(TSmallArena* arena)
        : Arena_(arena)
        , RefCount_(arena->ObjectCount_)
    {
        size_t totalSize = sizeof(TSegment) + Arena_->ObjectSize_ * Arena_->ObjectCount_;

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTrackerFacade::AllocateSpace(GetRefCountedTypeCookie<TSmallArena>(), totalSize);
#endif
    }

    ~TSegment()
    {
        size_t totalSize = sizeof(TSegment) + Arena_->ObjectSize_ * Arena_->ObjectCount_;

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTrackerFacade::FreeSpace(GetRefCountedTypeCookie<TSmallArena>(), totalSize);
#endif

        if (Arena_->MemoryTracker_) {
            Arena_->MemoryTracker_->Release(totalSize);
        }
    }

    void Ref()
    {
        ++RefCount_;
    }

    void Unref(size_t count = 1)
    {
        auto refCount = RefCount_.fetch_sub(count);
        YT_ASSERT(refCount >= count);

        if (refCount == count) {
            Destroy();
        }
    }

    bool Contains(void* obj) const
    {
        uintptr_t begin = reinterpret_cast<uintptr_t>(this + 1);
        uintptr_t address = reinterpret_cast<uintptr_t>(obj);
        return address >= begin && address < begin + Arena_->ObjectCount_ * Arena_->ObjectSize_;
    }

    void Destroy(bool unrefArena = true)
    {
        this->~TSegment();
        if (unrefArena) {
            Arena_->Unref();
        }
        NYTAlloc::Free(this);
    }

    void Free(void* obj)
    {
        YT_ASSERT(Contains(obj));

        auto item = static_cast<TFreeListItem*>(obj);
        item->Segment = this;

        if (!Arena_->FreeList_.PutIf(item, item, [&] (void* nextObj) {
            return Contains(nextObj);
        })) {
            // Segment is not active.
            Unref();
        }
    }

    std::pair<TFreeListItem*, TFreeListItem*> BuildFreeList()
    {
        auto ptr = reinterpret_cast<char*>(this + 1);
        auto head = reinterpret_cast<TFreeListItem*>(ptr);

        // Build chain of chunks.
        auto chunkCount = Arena_->ObjectCount_;
        auto objectSize = Arena_->ObjectSize_;

        YT_VERIFY(chunkCount > 0);
        YT_VERIFY(objectSize > 0);
        auto lastPtr = ptr + objectSize * (chunkCount - 1);
        YT_VERIFY(Contains(lastPtr));

        while (chunkCount-- > 1) {
            YT_VERIFY(Contains(ptr));

            auto* current = reinterpret_cast<TFreeListItem*>(ptr);
            ptr += objectSize;

            current->Next.store(reinterpret_cast<TFreeListItem*>(ptr), std::memory_order_release);
            current->Segment = this;
        }

        YT_VERIFY(ptr == lastPtr);

        auto* current = reinterpret_cast<TFreeListItem*>(ptr);
        current->Next.store(nullptr, std::memory_order_release);
        current->Segment = this;

        return {head, current};
    }

    bool IsReallocationNeeded() const
    {
        return RefCount_.load(std::memory_order_relaxed) * 2 < Arena_->ObjectCount_;
    }

private:
    TSmallArena* const Arena_;

    // Segment has ref if object is allocated or in free list.
    std::atomic<size_t> RefCount_;
};

TFreeListItem* TSmallArena::Allocate()
{
    auto* obj = FreeList_.Extract();
    if (Y_LIKELY(obj)) {
        // Fast path.
        return obj;
    }

    return AllocateSlow();
}

TFreeListItem* TSmallArena::AllocateSlow()
{
    // For large chunks it is better to allocate SegmentSize + sizeof(TFreeListItem) space
    // than allocate SegmentSize and use BatchSize_ - 1.
    auto totalSize = sizeof(TSegment) + ObjectSize_ * ObjectCount_;

    if (MemoryTracker_ && !MemoryTracker_->TryAcquire(totalSize).IsOK()) {
        return nullptr;
    }

    auto segment = new(NYTAlloc::Allocate(totalSize)) TSegment(this);

    auto [head, tail] = segment->BuildFreeList();
    auto* next = head->Next.load();

    while (true) {
        if (FreeList_.PutIf(next, tail, [] (void* nextObj) {
            // No elements in free list.
            return nextObj == nullptr;
        })) {
            ++RefCount_;
            return head;
        } else if (auto* object = FreeList_.Extract()) {
            // Remove allocated segment.
            segment->Destroy(false);
            return object;
        }
    }
}


/////////////////////////////////////////////////////////////////////////////

void TSmallArena::ClearFreeList()
{
    auto* object = FreeList_.ExtractAll();

    TSegment* segment = nullptr;
    size_t count = 0;

    while (object) {
        if (segment) {
            YT_VERIFY(segment == object->Segment);
        } else {
            segment = object->Segment;
        }

        object = object->Next.load(std::memory_order_acquire);
        ++count;
    }

    if (segment) {
        segment->Unref(count);
    }
}

size_t TSmallArena::Unref()
{
    auto count = --RefCount_;
    if (count == 0) {
        delete this;

    }
    return count;
}

/////////////////////////////////////////////////////////////////////////////

class TLargeArena
{
public:
    explicit TLargeArena(IMemoryUsageTrackerPtr memoryTracker)
        : MemoryTracker_(std::move(memoryTracker))
    { }

    void* Allocate(size_t size)
    {
        if (!TryAcquireMemory(size)) {
            return nullptr;
        }
        ++RefCount_;
        auto ptr = NYTAlloc::Allocate(size);
        auto allocatedSize = NYTAlloc::GetAllocationSize(ptr);
        YT_VERIFY(allocatedSize == size);
        return ptr;
    }

    void Free(void* ptr)
    {
        auto allocatedSize = NYTAlloc::GetAllocationSize(ptr);
        ReleaseMemory(allocatedSize);
        NYTAlloc::Free(ptr);
        Unref();
    }

    size_t Unref()
    {
        auto count = --RefCount_;
        if (count == 0) {
            delete this;
        }
        return count;
    }

    bool TryAcquireMemory(size_t size)
    {
        if (!MemoryTracker_) {
            return true;
        }

        auto overheadMemory = OverheadMemory_.load();
        do {
            if (overheadMemory < size) {
                auto targetAcquire = std::max(AcquireMemoryGranularity, size);
                auto result = MemoryTracker_->TryAcquire(targetAcquire);
                if (result.IsOK()) {
                    OverheadMemory_.fetch_add(targetAcquire - size);
                    return true;
                } else {
                    return false;
                }
            }
        } while (!OverheadMemory_.compare_exchange_weak(overheadMemory, overheadMemory - size));

        return true;
    }

    void ReleaseMemory(size_t size)
    {
        if (!MemoryTracker_) {
            return;
        }

        auto overheadMemory = OverheadMemory_.load();

        while (overheadMemory + size > AcquireMemoryGranularity) {
            if (OverheadMemory_.compare_exchange_weak(overheadMemory, AcquireMemoryGranularity / 2)) {
                MemoryTracker_->Release(overheadMemory + size - AcquireMemoryGranularity / 2);
                return;
            }
        }

        OverheadMemory_.fetch_add(size);
    }

private:
    const IMemoryUsageTrackerPtr MemoryTracker_;
    // One ref from allocator plus refs from allocated objects.
    std::atomic<size_t> RefCount_ = 1;
    std::atomic<size_t> OverheadMemory_ = 0;
};

/////////////////////////////////////////////////////////////////////////////

void TSlabAllocator::TSmallArenaDeleter::operator() (TSmallArena* arena)
{
    arena->ClearFreeList();
    arena->Unref();
}

TSlabAllocator::TSlabAllocator(IMemoryUsageTrackerPtr memoryTracker)
{
    for (size_t rank = 2; rank < NYTAlloc::SmallRankCount; ++rank) {
        // Rank is not used.
        if (rank == 3) {
            continue;
        }
        // There is no std::make_unique overload with custom deleter.
        SmallArenas_[rank].reset(new TSmallArena(rank, SegmentSize, memoryTracker));
    }

    LargeArena_.reset(new TLargeArena(memoryTracker));
}

namespace {

TLargeArena* TryGetLargeArenaFromTag(uintptr_t tag)
{
    return tag & 1ULL ? reinterpret_cast<TLargeArena*>(tag & ~1ULL) : nullptr;
}

TSegment* GetSegmentFromTag(uintptr_t tag)
{
    return reinterpret_cast<TSegment*>(tag);
}

uintptr_t MakeTagFromLargeArena(TLargeArena* arena)
{
    auto result = reinterpret_cast<uintptr_t>(arena);
    YT_ASSERT((result & 1ULL) == 0);
    return result | 1ULL;
}

uintptr_t MakeTagFromSegment(TSegment* segment)
{
    auto result = reinterpret_cast<uintptr_t>(segment);
    YT_ASSERT((result & 1ULL) == 0);
    return result & ~1ULL;
}

const uintptr_t* GetHeaderFromPtr(const void* ptr)
{
    return static_cast<const uintptr_t*>(ptr) - 1;
}

uintptr_t* GetHeaderFromPtr(void* ptr)
{
    return static_cast<uintptr_t*>(ptr) - 1;
}

} // namespace

void TSlabAllocator::TLargeArenaDeleter::operator() (TLargeArena* arena)
{
    arena->Unref();
}

void* TSlabAllocator::Allocate(size_t size)
{
    size += sizeof(uintptr_t);

    uintptr_t tag = 0;
    void* ptr = nullptr;
    if (size < NYTAlloc::LargeAllocationSizeThreshold) {
        auto rank = NYTAlloc::SizeToSmallRank(size);
        auto* object = SmallArenas_[rank]->Allocate();

        if (object) {
            tag = MakeTagFromSegment(object->Segment);
        }
        ptr = object;
    } else {
        ptr = LargeArena_->Allocate(size);
        tag = MakeTagFromLargeArena(LargeArena_.get());
    }

    if (!ptr) {
        return nullptr;
    }

    // Mutes TSAN data race with write Next in TFreeList::Push.
    auto* header = static_cast<std::atomic<uintptr_t>*>(ptr);
    header->store(tag, std::memory_order_release);

    return header + 1;
}

void TSlabAllocator::Free(void* ptr)
{
    YT_ASSERT(ptr);
    auto* header = GetHeaderFromPtr(ptr);
    auto tag = *header;

    if (auto* largeArena = TryGetLargeArenaFromTag(tag)) {
        largeArena->Free(header);
    } else {
        GetSegmentFromTag(tag)->Free(header);
    }
}

bool IsReallocationNeeded(const void* ptr)
{
    auto tag = *GetHeaderFromPtr(ptr);
    return !TryGetLargeArenaFromTag(tag) && GetSegmentFromTag(tag)->IsReallocationNeeded();
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT

