#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/public.h>

#include <yt/core/concurrency/async_semaphore.h>

#include <yt/core/misc/ref.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderMemoryManagerOptions
{
    explicit TChunkReaderMemoryManagerOptions(
        i64 bufferSize,
        NProfiling::TTagIdList profilingTagList = {},
        bool enableDetailedLogging = false);

    i64 BufferSize;

    NProfiling::TTagIdList ProfilingTagList;

    const bool EnableDetailedLogging;
};

////////////////////////////////////////////////////////////////////////////////

//! This interface is used by MultiReaderMemoryManager to track children memory managers.
class IReaderMemoryManager
    : public virtual TRefCounted
{
public:
    //! Minimum amount of memory required by reader to perform reads.
    virtual i64 GetRequiredMemorySize() const = 0;

    //! Desired amount of memory, enabling reader to do prefetch.
    virtual i64 GetDesiredMemorySize() const = 0;

    //! Amount of memory already reserved for this reader.
    virtual i64 GetReservedMemorySize() const = 0;

    //! Change reserved amount of memory reserved for this reader.
    virtual void SetReservedMemorySize(i64 size) = 0;

    //! Returns list of profiling tags for this memory manager.
    virtual const NProfiling::TTagIdList& GetProfilingTagList() const = 0;

    //! Adds information about corresponding chunk reader.
    virtual void AddChunkReaderInfo(TGuid chunkReaderId) = 0;

    //! Adds information abount corresponding read session.
    virtual void AddReadSessionInfo(TGuid readSessionId) = 0;

    //! Returns unique reader id.
    virtual TGuid GetId() const = 0;

    //! Indicates that memory requirements of this manager will not increase anymore.
    virtual void Finalize() = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderMemoryManager)

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderMemoryManager
    : public IReaderMemoryManager
{
public:
    explicit TChunkReaderMemoryManager(
        TChunkReaderMemoryManagerOptions options,
        TWeakPtr<IReaderMemoryManagerHost> hostMemoryManager = nullptr);

    virtual i64 GetRequiredMemorySize() const override;

    virtual i64 GetDesiredMemorySize() const override;

    virtual i64 GetReservedMemorySize() const override;

    virtual void SetReservedMemorySize(i64 size) override;

    virtual const NProfiling::TTagIdList& GetProfilingTagList() const override;
 
    virtual void AddChunkReaderInfo(TGuid chunkReaderId) override;

    virtual void AddReadSessionInfo(TGuid readSessionId) override;

    virtual TGuid GetId() const override;

    //! Called by fetcher when all blocks were fetched.
    virtual void Finalize() override;

    //! Always succeeds, possibly with overcommit.
    TMemoryUsageGuardPtr Acquire(i64 size);

    //! Future is set, when enough free size is available.
    TFuture<TMemoryUsageGuardPtr> AsyncAquire(i64 size);

    void Release(i64 size);

    void TryUnregister();

    //! Returns amount of memory that is possible to acquire now.
    i64 GetFreeMemorySize() const;

    //! Set total size of blocks we are going to read. Called by block fetcher.
    void SetTotalSize(i64 size);

    //! Sets minimal required memory size by chunk reader.
    void SetRequiredMemorySize(i64 size);

    void SetPrefetchMemorySize(i64 size);

private:
    void OnSemaphoreAcquired(TPromise<TMemoryUsageGuardPtr> promise, NConcurrency::TAsyncSemaphoreGuard semaphoreGuard);

    void OnMemoryRequirementsUpdated();

    void DoUnregister();

    i64 GetUsedMemorySize() const;

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

    NProfiling::TTagIdList ProfilingTagList_;

    const TGuid Id_;

    const NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderMemoryManager)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryUsageGuard
    : public TIntrinsicRefCounted
{
    TMemoryUsageGuard() = default;

    TMemoryUsageGuard(
        NConcurrency::TAsyncSemaphoreGuard guard,
        TWeakPtr<TChunkReaderMemoryManager> memoryManager);

    ~TMemoryUsageGuard();

    NConcurrency::TAsyncSemaphoreGuard Guard;
    TWeakPtr<TChunkReaderMemoryManager> MemoryManager;
};

DEFINE_REFCOUNTED_TYPE(TMemoryUsageGuard)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryManagedData
    : public TIntrinsicRefCounted
{
    TMemoryManagedData() = default;

    TMemoryManagedData(TSharedRef data, TMemoryUsageGuardPtr memoryUsageGuard);

    TSharedRef Data;
    TMemoryUsageGuardPtr MemoryUsageGuard;
};

DEFINE_REFCOUNTED_TYPE(TMemoryManagedData)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
