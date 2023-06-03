#include "chunk_reader_memory_manager.h"
#include "parallel_reader_memory_manager.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/guid.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderMemoryManagerOptions::TChunkReaderMemoryManagerOptions(
    i64 bufferSize,
    NProfiling::TTagList profilingTagList,
    bool enableDetailedLogging)
    : BufferSize(bufferSize)
    , ProfilingTagList(std::move(profilingTagList))
    , EnableDetailedLogging(enableDetailedLogging)
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
    , ProfilingTagList_(std::move(Options_.ProfilingTagList))
    , Id_(TGuid::Create())
    , Logger(ReaderMemoryManagerLogger.WithTag("Id: %v", Id_))
{
    TGuid parentId;
    if (auto parent = HostMemoryManager_.Lock()) {
        parentId = parent->GetId();
    }
    YT_LOG_DEBUG("Chunk reader memory manager created (ReservedMemorySize: %v, PrefetchMemorySize: %v, ParentId: %v)",
        GetReservedMemorySize(),
        PrefetchMemorySize_.load(),
        parentId);
}

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
    YT_LOG_DEBUG_UNLESS(GetReservedMemorySize() == size, "Updating reserved memory size (OldReservedMemorySize: %v, NewReservedMemorySize: %v)",
        GetReservedMemorySize(),
        size);

    ReservedMemorySize_ = size;
    AsyncSemaphore_->SetTotal(size);
}

const NProfiling::TTagList& TChunkReaderMemoryManager::GetProfilingTagList() const
{
    return ProfilingTagList_;
}

void TChunkReaderMemoryManager::AddChunkReaderInfo(TGuid chunkReaderId)
{
    YT_LOG_DEBUG("Chunk reader info added (ChunkReaderId: %v)", chunkReaderId);
}

void TChunkReaderMemoryManager::AddReadSessionInfo(TGuid readSessionId)
{
    YT_LOG_DEBUG("Read session info added (ReadSessionId: %v)", readSessionId);
}

TGuid TChunkReaderMemoryManager::GetId() const
{
    return Id_;
}

TMemoryUsageGuardPtr TChunkReaderMemoryManager::Acquire(i64 size)
{
    YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Force acquiring memory (MemorySize: %v, FreeMemorySize: %v)",
        size,
        GetFreeMemorySize());

    return New<TMemoryUsageGuard>(TAsyncSemaphoreGuard::Acquire(AsyncSemaphore_, size), MakeWeak(this));
}

TFuture<TMemoryUsageGuardPtr> TChunkReaderMemoryManager::AsyncAcquire(i64 size)
{
    YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Acquiring memory (MemorySize: %v, FreeMemorySize: %v)",
        size,
        GetFreeMemorySize());

    auto memoryPromise = NewPromise<TMemoryUsageGuardPtr>();
    auto memoryFuture = memoryPromise.ToFuture();
    AsyncSemaphore_->AsyncAcquire(
        BIND(
            &TChunkReaderMemoryManager::OnSemaphoreAcquired,
            MakeWeak(this),
            Passed(std::move(memoryPromise))),
        GetSyncInvoker(),
        size);

    return memoryFuture.ToImmediatelyCancelable();
}

void TChunkReaderMemoryManager::Release(i64 size)
{
    YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Releasing memory (MemorySize: %v, FreeMemorySize: %v)",
        size,
        GetFreeMemorySize());

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

i64 TChunkReaderMemoryManager::GetFreeMemorySize() const
{
    return AsyncSemaphore_->GetFree();
}

void TChunkReaderMemoryManager::SetTotalSize(i64 size)
{
    auto oldTotalMemorySize = TotalMemorySize_.load();
    YT_LOG_DEBUG_UNLESS(oldTotalMemorySize == size, "Updating total memory size (OldTotalSize: %v, NewTotalSize: %v)",
        oldTotalMemorySize,
        size);

    TotalMemorySize_ = size;
    OnMemoryRequirementsUpdated();
}

void TChunkReaderMemoryManager::SetRequiredMemorySize(i64 size)
{
    // Required memory size should never decrease to prevent many memory rebalancings.
    // NB: maximum update should be performed atomically.
    while (true) {
        auto oldValue = RequiredMemorySize_.load(std::memory_order::relaxed);
        if (oldValue >= size) {
            break;
        }
        if (RequiredMemorySize_.compare_exchange_weak(oldValue, size)) {
            YT_LOG_DEBUG("Updating required memory size (OldRequiredMemorySize: %v, NewRequiredMemorySize: %v)",
                oldValue,
                size);
            OnMemoryRequirementsUpdated();
            break;
        }
    }
}

void TChunkReaderMemoryManager::SetPrefetchMemorySize(i64 size)
{
    auto oldPrefetchMemorySize = PrefetchMemorySize_.load();
    YT_LOG_DEBUG_UNLESS(oldPrefetchMemorySize == size, "Updating prefetch memory size (OldPrefetchMemorySize: %v, NewPrefetchMemorySize: %v)",
        oldPrefetchMemorySize,
        size);

    PrefetchMemorySize_ = size;
    OnMemoryRequirementsUpdated();
}

void TChunkReaderMemoryManager::Finalize()
{
    YT_LOG_DEBUG("Finalizing chunk reader memory manager (AlreadyFinalized: %v)",
        Finalized_.load());

    Finalized_ = true;
    TryUnregister();
}

void TChunkReaderMemoryManager::OnSemaphoreAcquired(TPromise<TMemoryUsageGuardPtr> promise, TAsyncSemaphoreGuard semaphoreGuard)
{
    YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Semaphore acquired (MemorySize: %v)", semaphoreGuard.GetSlots());

    promise.Set(New<TMemoryUsageGuard>(std::move(semaphoreGuard), MakeWeak(this)));
}

void TChunkReaderMemoryManager::OnMemoryRequirementsUpdated()
{
    YT_LOG_DEBUG("Memory requirements updated (ReservedMemorySize: %v, UsedMemorySize: %v, TotalMemorySize: %v, "
        "RequiredMemorySize: %v, PrefetchMemorySize: %v)",
        GetReservedMemorySize(),
        GetUsedMemorySize(),
        TotalMemorySize_.load(),
        GetRequiredMemorySize(),
        PrefetchMemorySize_.load());

    if (auto hostMemoryManager = HostMemoryManager_.Lock()) {
        hostMemoryManager->UpdateMemoryRequirements(MakeStrong(this));
    }
}

void TChunkReaderMemoryManager::DoUnregister()
{
    if (!Unregistered_.test_and_set()) {
        YT_LOG_DEBUG("Unregistering chunk reader memory manager");
        if (auto hostMemoryManager = HostMemoryManager_.Lock()) {
            hostMemoryManager->Unregister(MakeStrong(this));
        }
    }
}

i64 TChunkReaderMemoryManager::GetUsedMemorySize() const
{
    return AsyncSemaphore_->GetUsed();
}

////////////////////////////////////////////////////////////////////////////////

TMemoryUsageGuard::TMemoryUsageGuard(
    NConcurrency::TAsyncSemaphoreGuard guard,
    TWeakPtr<TChunkReaderMemoryManager> memoryManager)
    : Guard_(std::move(guard))
    , MemoryManager_(std::move(memoryManager))
{ }

TMemoryUsageGuard::~TMemoryUsageGuard()
{
    Guard_.Release();
    if (auto memoryManager = MemoryManager_.Lock()) {

        memoryManager->TryUnregister();
    }
}

TAsyncSemaphoreGuard* TMemoryUsageGuard::GetGuard()
{
    return &Guard_;
}

TWeakPtr<TChunkReaderMemoryManager> TMemoryUsageGuard::GetMemoryManager() const
{
    return MemoryManager_;
}

void TMemoryUsageGuard::CaptureBlock(TSharedRef block)
{
    YT_VERIFY(!Block_);
    Block_ = std::move(block);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
