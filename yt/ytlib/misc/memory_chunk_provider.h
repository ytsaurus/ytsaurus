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
    : public TIntrinsicRefCounted
    , public TWithExtraSpace<TTrackedAllocationHolder<Size, PoolCategory>>
{
    explicit TTrackedAllocationHolder(TRefCountedTypeCookie cookie)
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        : Cookie(cookie)
    {
        TRefCountedTrackerFacade::AllocateTagInstance(Cookie);
        TRefCountedTrackerFacade::AllocateSpace(Cookie, Size);
    }

    const TRefCountedTypeCookie Cookie;
#else
    { }
#endif

    TMutableRef GetRef()
    {
        return TMutableRef(this->GetExtraSpacePtr(), Size);
    }

    ~TTrackedAllocationHolder()
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTrackerFacade::FreeTagInstance(Cookie);
        TRefCountedTrackerFacade::FreeSpace(Cookie, Size);
#endif
    }

    NNodeTrackerClient::TNodeMemoryTrackerGuard MemoryTrackerGuard;
    NNodeTrackerClient::TNodeMemoryTracker* MemoryTracker = nullptr;
};

template <size_t Size, NNodeTrackerClient::EMemoryCategory PoolCategory>
class TPooledMemoryChunkProvider
    : public IMemoryChunkProvider
{
public:
    TPooledMemoryChunkProvider(
        NNodeTrackerClient::EMemoryCategory mainCategory,
        NNodeTrackerClient::TNodeMemoryTracker* memoryTracker = nullptr);

    virtual TSharedMutableRef Allocate(TRefCountedTypeCookie cookie);

    virtual size_t GetChunkSize() const;

private:
    NNodeTrackerClient::EMemoryCategory MainCategory_;
    NNodeTrackerClient::TNodeMemoryTracker* MemoryTracker_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MEMORY_CHUNK_PROVIDER_INL_H_
#include "memory_chunk_provider-inl.h"
#undef MEMORY_CHUNK_PROVIDER_INL_H_
