#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TTrackedMemoryChunkProvider
    : public IMemoryChunkProvider
{
private:

public:
    TTrackedMemoryChunkProvider(
        TString key,
        TMemoryProviderMapByTagPtr parent,
        size_t limit,
        IMemoryUsageTrackerPtr memoryTracker);

    std::unique_ptr<TAllocationHolder> Allocate(size_t size, TRefCountedTypeCookie cookie) override;

    size_t GetMaxAllocated();

    ~TTrackedMemoryChunkProvider();

private:
    struct THolder;

    const TString Key_;
    const TMemoryProviderMapByTagPtr Parent_;
    const size_t Limit_;
    const IMemoryUsageTrackerPtr MemoryTracker_;

    std::atomic<size_t> Allocated_ = {0};
    std::atomic<size_t> MaxAllocated_ = {0};
};

DEFINE_REFCOUNTED_TYPE(TTrackedMemoryChunkProvider)

////////////////////////////////////////////////////////////////////////////////

class TMemoryProviderMapByTag
    : public TRefCounted
{
public:
    TTrackedMemoryChunkProviderPtr GetProvider(
        const TString& tag,
        size_t limit,
        IMemoryUsageTrackerPtr memoryTracker);

    friend class TTrackedMemoryChunkProvider;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<TString, TWeakPtr<TTrackedMemoryChunkProvider>> Map_;
};

DEFINE_REFCOUNTED_TYPE(TMemoryProviderMapByTag)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
