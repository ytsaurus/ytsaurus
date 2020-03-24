#include "chunk_reader_memory_manager.h"

#include "parallel_reader_memory_manager.h"

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderMemoryManagerOptions::TChunkReaderMemoryManagerOptions(
    i64 bufferSize,
    NProfiling::TTagIdList profilingTagList)
    : BufferSize(bufferSize)
    , ProfilingTagList(std::move(profilingTagList))
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkReaderMemoryManager::TChunkReaderMemoryManager(
    TChunkReaderMemoryManagerOptions options,
    TWeakPtr<IReaderMemoryManagerHost> hostMemoryManager)
    : Options_(std::move(options))
    , ReservedMemorySize_(Options_.BufferSize)
    , PrefetchMemorySize_(Options_.BufferSize)
    , AsyncSemaphore_(New<TAsyncSemaphore>(Options_.BufferSize))
    , HostMemoryManager_(std::move(hostMemoryManager))
    , ProfilingTagList_(std::move(options.ProfilingTagList))
{ }

i64 TChunkReaderMemoryManager::GetRequiredMemorySize() const
{
    if (Finalized_) {
        return GetUsedMemorySize();
    }

    auto requiredMemorySize = static_cast<i64>(RequiredMemorySize_);
    if (TotalMemorySize_ != TotalMemorySizeUnknown) {
        requiredMemorySize = std::min<i64>(requiredMemorySize, TotalMemorySize_);
    }

    return requiredMemorySize;
}

i64 TChunkReaderMemoryManager::GetDesiredMemorySize() const
{
    if (Finalized_) {
        return GetUsedMemorySize();
    }

    auto desiredMemorySize = RequiredMemorySize_ + PrefetchMemorySize_;
    if (TotalMemorySize_ != TotalMemorySizeUnknown) {
        desiredMemorySize = std::min<i64>(desiredMemorySize, TotalMemorySize_);
    }

    return desiredMemorySize;
}

i64 TChunkReaderMemoryManager::GetReservedMemorySize() const
{
    return ReservedMemorySize_;
}

void TChunkReaderMemoryManager::SetReservedMemorySize(i64 size)
{
    ReservedMemorySize_ = size;
    AsyncSemaphore_->SetTotal(ReservedMemorySize_);
}

const NProfiling::TTagIdList& TChunkReaderMemoryManager::GetProfilingTagList() const
{
    return ProfilingTagList_;
}

TMemoryUsageGuardPtr TChunkReaderMemoryManager::Acquire(i64 size)
{
    return New<TMemoryUsageGuard>(TAsyncSemaphoreGuard::Acquire(AsyncSemaphore_, size), MakeWeak(this));
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
    TryUnregister();
}

void TChunkReaderMemoryManager::TryUnregister()
{
    if (Finalized_) {
        if (AsyncSemaphore_->IsFree()) {
            DoUnregister();
        } else {
            OnMemoryRequirementsUpdated();
        }
    }
}

i64 TChunkReaderMemoryManager::GetAvailableSize() const
{
    return AsyncSemaphore_->GetFree();
}

void TChunkReaderMemoryManager::SetTotalSize(i64 size)
{
    TotalMemorySize_ = size;
    OnMemoryRequirementsUpdated();
}

void TChunkReaderMemoryManager::SetRequiredMemorySize(i64 size)
{
    // Required memory size should never decrease to prevent many memory rebalancings.
    // NB: maxmimum update should be performed atomically.
    while (true) {
        auto oldValue = RequiredMemorySize_.load(std::memory_order_relaxed);
        if (oldValue >= size) {
            break;
        }
        if (RequiredMemorySize_.compare_exchange_weak(oldValue, size)) {
            OnMemoryRequirementsUpdated();
            break;
        }
    }
}

void TChunkReaderMemoryManager::SetPrefetchMemorySize(i64 size)
{
    PrefetchMemorySize_ = size;
    OnMemoryRequirementsUpdated();
}

void TChunkReaderMemoryManager::Finalize()
{
    Finalized_ = true;
    TryUnregister();
}

void TChunkReaderMemoryManager::OnSemaphoreAcquired(TPromise<TMemoryUsageGuardPtr> promise, TAsyncSemaphoreGuard semaphoreGuard)
{
    promise.Set(New<TMemoryUsageGuard>(std::move(semaphoreGuard), MakeWeak(this)));
}

void TChunkReaderMemoryManager::OnMemoryRequirementsUpdated()
{
    auto hostMemoryManager = HostMemoryManager_.Lock();
    if (hostMemoryManager) {
        hostMemoryManager->UpdateMemoryRequirements(MakeStrong(this));
    }
}

void TChunkReaderMemoryManager::DoUnregister()
{
    if (!Unregistered_.test_and_set()) {
        auto hostMemoryManager = HostMemoryManager_.Lock();
        if (hostMemoryManager) {
            hostMemoryManager->Unregister(MakeStrong(this));
        }
    }
}

i64 TChunkReaderMemoryManager::GetUsedMemorySize() const
{
    return AsyncSemaphore_->GetUsed();
}

////////////////////////////////////////////////////////////////////////////////

TMemoryManagedData::TMemoryManagedData(TSharedRef data, TMemoryUsageGuardPtr memoryUsageGuard)
    : Data(std::move(data))
    , MemoryUsageGuard(std::move(memoryUsageGuard))
{ }

////////////////////////////////////////////////////////////////////////////////

TMemoryUsageGuard::TMemoryUsageGuard(
    NConcurrency::TAsyncSemaphoreGuard guard,
    TWeakPtr<TChunkReaderMemoryManager> memoryManager)
    : Guard(std::move(guard))
    , MemoryManager(std::move(memoryManager))
{ }

TMemoryUsageGuard::~TMemoryUsageGuard()
{
    Guard.Release();
    auto memoryManager = MemoryManager.Lock();
    if (memoryManager) {
        memoryManager->TryUnregister();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
