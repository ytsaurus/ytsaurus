#pragma once
#ifndef MEMORY_CHUNK_PROVIDER_INL_H_
#error "Direct inclusion of this file is not allowed, include memory_chunk_provider.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
TPeriodicDeleter<Size, PoolCategory>::TPeriodicDeleter()
{
    PeriodicQueue_ = New<NConcurrency::TActionQueue>("ObjectPoolPeriodicReleaser");

    PeriodicExecutor_ = New<NConcurrency::TPeriodicExecutor>(
        PeriodicQueue_->GetInvoker(),
        BIND([] () {
            auto& objectPool = ObjectPool<TTrackedAllocationHolder<Size, PoolCategory>>();
            objectPool.Release(objectPool.GetSize() / 16);
        }),
        TDuration::Seconds(1));
    PeriodicExecutor_->Start();
}

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
TPeriodicDeleter<Size, PoolCategory>& PeriodicDeleter()
{
    return *Singleton<TPeriodicDeleter<Size, PoolCategory>>();
}

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
TPooledMemoryChunkProvider<Size, PoolCategory>::TPooledMemoryChunkProvider(
    NNodeTrackerClient::EMemoryCategory mainCategory,
    NNodeTrackerClient::TNodeMemoryTracker* memoryTracker)
    : MainCategory_(mainCategory)
    , MemoryTracker_(memoryTracker)
{
    PeriodicDeleter<Size, PoolCategory>();
}

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
std::shared_ptr<TMutableRef> TPooledMemoryChunkProvider<Size, PoolCategory>::Allocate(TRefCountedTypeCookie cookie)
{
    auto result = ObjectPool<TTrackedAllocationHolder<Size, PoolCategory>>().Allocate();
    result->SetCookie(cookie);
    result->MemoryTracker = MemoryTracker_;
    if (result->MemoryTracker) {
        auto guardOrError = NNodeTrackerClient::TNodeMemoryTrackerGuard::TryAcquire(
            result->MemoryTracker,
            MainCategory_,
            Size);

        result->MemoryTrackerGuard = std::move(guardOrError.ValueOrThrow());
    }
    YCHECK(result->Size() != 0);

    return result;
}

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
size_t TPooledMemoryChunkProvider<Size, PoolCategory>::GetChunkSize() const
{
    return Size;
}

////////////////////////////////////////////////////////////////////////////////

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
TTrackedAllocationHolder<Size, PoolCategory>*
TPooledObjectTraits<TTrackedAllocationHolder<Size, PoolCategory>>::Allocate()
{
    return TAllocationHolder::Allocate<TTrackedAllocationHolder<Size, PoolCategory>>(Size);
}

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
void TPooledObjectTraits<TTrackedAllocationHolder<Size, PoolCategory>>::Clean(TTrackedAllocationHolder<Size, PoolCategory>* object)
{
    object->SetCookie(NullRefCountedTypeCookie);
    if (object->MemoryTracker) {
        object->MemoryTrackerGuard = NNodeTrackerClient::TNodeMemoryTrackerGuard::Acquire(
            object->MemoryTracker,
            PoolCategory,
            Size);
    }
}

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
size_t TPooledObjectTraits<TTrackedAllocationHolder<Size, PoolCategory>>::GetMaxPoolSize()
{
    return std::numeric_limits<size_t>::max();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
