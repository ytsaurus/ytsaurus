#include "tracked_memory_chunk_provider.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TTrackedMemoryChunkProvider::THolder
    : public TAllocationHolder
{
    THolder(
        TMutableRef ref,
        TRefCountedTypeCookie cookie)
        : TAllocationHolder(ref, cookie)
    { }

    ~THolder()
    {
        if (!Owner) {
            return;
        }

        Owner->Allocated_ -= GetRef().Size();
        if (Owner->MemoryTracker_) {
            Owner->MemoryTracker_->Release(GetRef().Size());
        }
    }

    TIntrusivePtr<TTrackedMemoryChunkProvider> Owner;
};

////////////////////////////////////////////////////////////////////////////////

TTrackedMemoryChunkProvider::TTrackedMemoryChunkProvider(
    TString key,
    TMemoryProviderMapByTagPtr parent,
    size_t limit,
    IMemoryUsageTrackerPtr memoryTracker)
    : Key_(std::move(key))
    , Parent_(std::move(parent))
    , Limit_(limit)
    , MemoryTracker_(std::move(memoryTracker))
{ }

std::unique_ptr<TAllocationHolder> TTrackedMemoryChunkProvider::Allocate(size_t size, TRefCountedTypeCookie cookie)
{
    size_t allocated = Allocated_.load();
    do {
        if (allocated + size > Limit_) {
            THROW_ERROR_EXCEPTION("Not enough memory to serve allocation",
                size,
                allocated,
                Limit_)
                << TErrorAttribute("allocation_size", size)
                << TErrorAttribute("allocated", allocated)
                << TErrorAttribute("limit", Limit_);
        }
    } while (!Allocated_.compare_exchange_weak(allocated, allocated + size));

    std::unique_ptr<THolder> result(TAllocationHolder::Allocate<THolder>(size, cookie));
    auto allocatedSize = result->GetRef().Size();
    YT_VERIFY(allocatedSize != 0);

    auto delta = allocatedSize - size;
    allocated = Allocated_.fetch_add(delta) + delta;

    auto maxAllocated = MaxAllocated_.load();
    while (maxAllocated < allocated && !MaxAllocated_.compare_exchange_weak(maxAllocated, allocated));

    auto finally = Finally([&] {
        Allocated_ -= allocatedSize;
    });

    if (MemoryTracker_) {
        MemoryTracker_->TryAcquire(allocatedSize)
            .ThrowOnError();
    }

    finally.Release();
    result->Owner = this;

    return result;
}

size_t TTrackedMemoryChunkProvider::GetMaxAllocated()
{
    return MaxAllocated_;
}

TTrackedMemoryChunkProvider::~TTrackedMemoryChunkProvider()
{
    auto guard = Guard(Parent_->SpinLock_);
    Parent_->Map_.erase(Key_);
}

////////////////////////////////////////////////////////////////////////////////

TTrackedMemoryChunkProviderPtr TMemoryProviderMapByTag::GetProvider(
    const TString& tag,
    size_t limit,
    IMemoryUsageTrackerPtr memoryTracker)
{
    auto guard = Guard(SpinLock_);
    auto it = Map_.emplace(tag, nullptr).first;

    auto result = it->second.Lock();

    if (!result) {
        result = New<TTrackedMemoryChunkProvider>(tag, this, limit, std::move(memoryTracker));
        it->second = result;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
