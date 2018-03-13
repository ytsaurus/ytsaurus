#pragma once
#ifndef MEMORY_CHUNK_PROVIDER_INL_H_
#error "Direct inclusion of this file is not allowed, include memory_chunk_provider.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
TPooledMemoryChunkProvider<Size, PoolCategory>::TPooledMemoryChunkProvider(
    NNodeTrackerClient::EMemoryCategory mainCategory,
    NNodeTrackerClient::TNodeMemoryTracker* memoryTracker)
    : MainCategory_(mainCategory)
    , MemoryTracker_(memoryTracker)
{ }

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
TSharedMutableRef TPooledMemoryChunkProvider<Size, PoolCategory>::Allocate(TRefCountedTypeCookie cookie)
{
    auto holder = NewWithExtraSpace<TTrackedAllocationHolder<Size, PoolCategory>>(Size, cookie);

    holder->MemoryTracker = MemoryTracker_;
    if (holder->MemoryTracker) {
        holder->MemoryTrackerGuard = NNodeTrackerClient::TNodeMemoryTrackerGuard::Acquire(
            holder->MemoryTracker,
            MainCategory_,
            Size);
    }

    auto ref = holder->GetRef();
    return TSharedMutableRef(ref, std::move(holder));
}

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
size_t TPooledMemoryChunkProvider<Size, PoolCategory>::GetChunkSize() const
{
    return Size;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
