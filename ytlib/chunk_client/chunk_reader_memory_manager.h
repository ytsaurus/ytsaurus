#pragma once

#include "public.h"

#include <yt/core/concurrency/async_semaphore.h>

#include <yt/core/misc/ref.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderMemoryManagerOptions
{
    explicit TChunkReaderMemoryManagerOptions(i64 bufferSize);

    i64 BufferSize;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderMemoryManager
    : public TRefCounted
{
public:
    explicit TChunkReaderMemoryManager(TChunkReaderMemoryManagerOptions options);

    //! Always succeeds, possibly with overcommit.
    TMemoryUsageGuardPtr Acquire(i64 size);

    //! Future is set, when enough free size is available.
    TFuture<TMemoryUsageGuardPtr> AsyncAquire(i64 size);

    void Release(i64 size);

    //! Returns amount of memory that is possible to acquire now.
    i64 GetAvailableSize() const;

    //! Set total size of blocks we are going to read. Called by block fetcher.
    //! Does nothing for now.
    void SetTotalSize(i64 size);

    //! Sets minimal required memory size by chunk reader.
    //! Does nothing for now.
    void SetRequiredMemorySize(i64 size);

    //! Called by fetcher when all blocks were fetched.
    //! Does nothing for now.
    void Finalize();

private:
    void OnSemaphoreAcquired(TPromise<TMemoryUsageGuardPtr> promise, NConcurrency::TAsyncSemaphoreGuard semaphoreGuard);

    TChunkReaderMemoryManagerOptions Options_;

    NConcurrency::TAsyncSemaphorePtr AsyncSemaphore_;
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderMemoryManager)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryUsageGuard
    : public TIntrinsicRefCounted
{
    TMemoryUsageGuard() = default;

    TMemoryUsageGuard(NConcurrency::TAsyncSemaphoreGuard guard);

    NConcurrency::TAsyncSemaphoreGuard Guard;
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
