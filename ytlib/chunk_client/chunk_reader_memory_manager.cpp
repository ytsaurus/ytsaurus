#include "chunk_reader_memory_manager.h"

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderMemoryManagerOptions::TChunkReaderMemoryManagerOptions(i64 bufferSize)
    : BufferSize(bufferSize)
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkReaderMemoryManager::TChunkReaderMemoryManager(TChunkReaderMemoryManagerOptions options)
    : Options_(std::move(options))
    , AsyncSemaphore_(New<TAsyncSemaphore>(Options_.BufferSize))
{ }

TMemoryUsageGuardPtr TChunkReaderMemoryManager::Acquire(i64 size)
{
    return New<TMemoryUsageGuard>(TAsyncSemaphoreGuard::Acquire(AsyncSemaphore_, size));
}

TFuture<TMemoryUsageGuardPtr> TChunkReaderMemoryManager::AsyncAquire(i64 size)
{
    auto memoryPromise = NewPromise<TMemoryUsageGuardPtr>();
    auto memoryFuture = memoryPromise.ToFuture();
    AsyncSemaphore_->AsyncAcquire(
        BIND(&TChunkReaderMemoryManager::OnSemaphoreAcquired, MakeWeak(this), std::move(memoryPromise)),
        GetSyncInvoker(),
        size);

    return memoryFuture.ToImmediatelyCancelable();
}

void TChunkReaderMemoryManager::Release(i64 size)
{
    AsyncSemaphore_->Release(size);
}

i64 TChunkReaderMemoryManager::GetAvailableSize() const
{
    return AsyncSemaphore_->GetFree();
}

void TChunkReaderMemoryManager::SetTotalSize(i64 /*size*/)
{ }

void TChunkReaderMemoryManager::SetRequiredMemorySize(i64 /*size*/)
{ }

void TChunkReaderMemoryManager::Finalize()
{ }

void TChunkReaderMemoryManager::OnSemaphoreAcquired(TPromise<TMemoryUsageGuardPtr> promise, TAsyncSemaphoreGuard semaphoreGuard)
{
    promise.Set(New<TMemoryUsageGuard>(std::move(semaphoreGuard)));
}

////////////////////////////////////////////////////////////////////////////////

TMemoryManagedData::TMemoryManagedData(TSharedRef data, TMemoryUsageGuardPtr memoryUsageGuard)
    : Data(std::move(data))
    , MemoryUsageGuard(std::move(memoryUsageGuard))
{ }

////////////////////////////////////////////////////////////////////////////////

TMemoryUsageGuard::TMemoryUsageGuard(NConcurrency::TAsyncSemaphoreGuard guard)
    : Guard(std::move(guard))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
