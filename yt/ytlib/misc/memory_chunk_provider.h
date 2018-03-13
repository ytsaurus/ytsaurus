#pragma once

#include "memory_usage_tracker.h"

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/public.h>
#include <yt/core/misc/object_pool.h>
#include <yt/core/misc/chunked_memory_pool.h>

#include <util/generic/singleton.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
struct TTrackedAllocationHolder
    : public TAllocationHolder
{
    NNodeTrackerClient::TNodeMemoryTrackerGuard MemoryTrackerGuard;
    NNodeTrackerClient::TNodeMemoryTracker* MemoryTracker = nullptr;
};

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
class TPeriodicDeleter
{
public:
    TPeriodicDeleter();

private:
    NConcurrency::TActionQueuePtr PeriodicQueue_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    Y_DECLARE_SINGLETON_FRIEND();
};

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
TPeriodicDeleter<Size, PoolCategory>& PeriodicDeleter();

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
class TPooledMemoryChunkProvider
    : public IMemoryChunkProvider
{
public:
    TPooledMemoryChunkProvider(
        NNodeTrackerClient::EMemoryCategory mainCategory,
        NNodeTrackerClient::TNodeMemoryTracker* memoryTracker = nullptr);

    virtual std::shared_ptr<TMutableRef> Allocate(TRefCountedTypeCookie cookie);

    virtual size_t GetChunkSize() const;

private:
    NNodeTrackerClient::EMemoryCategory MainCategory_;
    NNodeTrackerClient::TNodeMemoryTracker* MemoryTracker_;

};

////////////////////////////////////////////////////////////////////////////////

using NNodeTrackerClient::EMemoryCategory;

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
struct TPooledObjectTraits<TTrackedAllocationHolder<Size, PoolCategory>>
    : public TPooledObjectTraitsBase<TTrackedAllocationHolder<Size, PoolCategory>>
{
    static TTrackedAllocationHolder<Size, PoolCategory>* Allocate();
    static void Clean(TTrackedAllocationHolder<Size, PoolCategory>* object);
    static size_t GetMaxPoolSize();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MEMORY_CHUNK_PROVIDER_INL_H_
#include "memory_chunk_provider-inl.h"
#undef MEMORY_CHUNK_PROVIDER_INL_H_
