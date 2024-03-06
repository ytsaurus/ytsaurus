#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderMemoryManagerOptions
{
    explicit TChunkReaderMemoryManagerOptions(
        i64 bufferSize,
        NProfiling::TTagList profilingTagList = {},
        bool enableDetailedLogging = false,
        ITypedNodeMemoryTrackerPtr memoryUsageTracker = nullptr);

    i64 BufferSize;

    NProfiling::TTagList ProfilingTagList;

    const bool EnableDetailedLogging;

    const ITypedNodeMemoryTrackerPtr MemoryUsageTracker;
};

////////////////////////////////////////////////////////////////////////////////

//! This interface is used by MultiReaderMemoryManager to track child memory managers.
struct IReaderMemoryManager
    : public virtual TRefCounted
{
    //! Minimum amount of memory required by reader to perform reads.
    virtual i64 GetRequiredMemorySize() const = 0;

    //! Desired amount of memory, enabling reader to do prefetch.
    virtual i64 GetDesiredMemorySize() const = 0;

    //! Amount of memory already reserved for this reader.
    virtual i64 GetReservedMemorySize() const = 0;

    //! Change reserved amount of memory reserved for this reader.
    virtual void SetReservedMemorySize(i64 size) = 0;

    //! Returns list of profiling tags for this memory manager.
    virtual const NProfiling::TTagList& GetProfilingTagList() const = 0;

    //! Adds information about corresponding chunk reader.
    virtual void AddChunkReaderInfo(TGuid chunkReaderId) = 0;

    //! Adds information about corresponding read session.
    virtual void AddReadSessionInfo(TGuid readSessionId) = 0;

    //! Returns unique reader id.
    virtual TGuid GetId() const = 0;

    //! Indicates that memory requirements of this manager will not increase anymore.
    virtual TFuture<void> Finalize() = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderMemoryManager)

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderMemoryManager
    : public IReaderMemoryManager
{
public:
    static TChunkReaderMemoryManagerHolderPtr CreateHolder(
        TChunkReaderMemoryManagerOptions options,
        TWeakPtr<IReaderMemoryManagerHost> hostMemoryManager = nullptr);

    i64 GetRequiredMemorySize() const override;

    i64 GetDesiredMemorySize() const override;

    i64 GetReservedMemorySize() const override;

    void SetReservedMemorySize(i64 size) override;

    const NProfiling::TTagList& GetProfilingTagList() const override;

    void AddChunkReaderInfo(TGuid chunkReaderId) override;

    void AddReadSessionInfo(TGuid readSessionId) override;

    TGuid GetId() const override;

    //! Called by fetcher when all blocks were fetched.
    TFuture<void> Finalize() override;

    //! Always succeeds, possibly with overcommit.
    TMemoryUsageGuardPtr Acquire(i64 size);

    //! Future is set, when enough free size is available.
    TFuture<TMemoryUsageGuardPtr> AsyncAcquire(i64 size);

    void TryUnregister();

    //! Returns amount of memory that is possible to acquire now.
    i64 GetFreeMemorySize() const;

    //! Set total size of blocks we are going to read. Called by block fetcher.
    void SetTotalSize(i64 size);

    //! Sets minimal required memory size by chunk reader.
    void SetRequiredMemorySize(i64 size);

    void SetPrefetchMemorySize(i64 size);

    i64 GetUsedMemorySize() const;

private:
    explicit TChunkReaderMemoryManager(
        TChunkReaderMemoryManagerOptions options,
        TWeakPtr<IReaderMemoryManagerHost> hostMemoryManager = nullptr);

    DECLARE_NEW_FRIEND()

    TMemoryUsageGuardPtr OnSemaphoreAcquired(NConcurrency::TAsyncSemaphoreGuard&& semaphoreGuard);

    void OnMemoryRequirementsUpdated();

    void DoUnregister();

    TChunkReaderMemoryManagerOptions Options_;

    std::atomic<i64> ReservedMemorySize_ = {0};
    std::atomic<i64> PrefetchMemorySize_ = {0};
    std::atomic<i64> RequiredMemorySize_ = {0};

    constexpr static i64 TotalMemorySizeUnknown = -1;
    std::atomic<i64> TotalMemorySize_ = {TotalMemorySizeUnknown};

    std::atomic<bool> Finalized_ = {false};
    std::atomic_flag Unregistered_ = ATOMIC_FLAG_INIT;

    NConcurrency::TAsyncSemaphorePtr AsyncSemaphore_;

    TWeakPtr<IReaderMemoryManagerHost> HostMemoryManager_;

    ITypedNodeMemoryTrackerPtr MemoryUsageTracker_;

    NProfiling::TTagList ProfilingTagList_;

    const TGuid Id_;

    const NLogging::TLogger Logger;

    TPromise<void> FinalizeEvent_ = NewPromise<void>();
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderMemoryManager)

////////////////////////////////////////////////////////////////////////////////

class TMemoryUsageGuard
    : public TRefCounted
{
public:
    TMemoryUsageGuard() = default;

    TMemoryUsageGuard(
        NConcurrency::TAsyncSemaphoreGuard guard,
        TWeakPtr<TChunkReaderMemoryManager> memoryManager,
        std::optional<TMemoryUsageTrackerGuard> memoryUsageTrackerGuard = {});

    TMemoryUsageGuard(const TMemoryUsageGuard& other) = delete;
    TMemoryUsageGuard(TMemoryUsageGuard&& other);
    TMemoryUsageGuard& operator=(const TMemoryUsageGuard& other) = delete;
    TMemoryUsageGuard& operator=(TMemoryUsageGuard&& other);

    ~TMemoryUsageGuard();

    void CaptureBlock(TSharedRef block);

    NConcurrency::TAsyncSemaphoreGuard* GetGuard();

    TWeakPtr<TChunkReaderMemoryManager> GetMemoryManager() const;

    TMemoryUsageGuardPtr TransferMemory(i64 slots);

private:
    NConcurrency::TAsyncSemaphoreGuard Guard_;
    TWeakPtr<TChunkReaderMemoryManager> MemoryManager_;
    std::optional<TMemoryUsageTrackerGuard> MemoryUsageTrackerGuard_;
    TSharedRef Block_;

    void MoveFrom(TMemoryUsageGuard&& other);

    void Release();
};

DEFINE_REFCOUNTED_TYPE(TMemoryUsageGuard)

////////////////////////////////////////////////////////////////////////////////

//! RAII holder for TChunkReaderMemoryManager.
//! Finalizes the memory manager on destruction.
class TChunkReaderMemoryManagerHolder
    : public TRefCounted
{
public:
    explicit TChunkReaderMemoryManagerHolder(TChunkReaderMemoryManagerPtr memoryManager);

    const TChunkReaderMemoryManagerPtr& Get() const;

    ~TChunkReaderMemoryManagerHolder();

private:
    const TChunkReaderMemoryManagerPtr MemoryManager_;
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderMemoryManagerHolder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
