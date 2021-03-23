#include "chunk_detail.h"
#include "private.h"
#include "location.h"
#include "session_manager.h"
#include "chunk_meta_manager.h"
#include "chunk_registry.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NDataNode {

using namespace NClusterNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkBase::TChunkBase(
    TBootstrap* bootstrap,
    TLocationPtr location,
    TChunkId id)
    : Bootstrap_(bootstrap)
    , Location_(location)
    , Id_(id)
{ }

TChunkBase::~TChunkBase()
{
    const auto& chunkMetaManager = Bootstrap_->GetChunkMetaManager();
    chunkMetaManager->RemoveCachedMeta(Id_);
    chunkMetaManager->RemoveCachedBlocksExt(Id_);
}

TChunkId TChunkBase::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

const TLocationPtr& TChunkBase::GetLocation() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Location_;
}

TString TChunkBase::GetFileName() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Location_->GetChunkPath(Id_);
}

int TChunkBase::GetVersion() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Version_.load();
}

int TChunkBase::IncrementVersion()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ++Version_;
}

TFuture<void> TChunkBase::AcquireReadLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    int lockCount;
    TFuture<void> result;
    {
        auto guard = ReaderGuard(LifetimeLock_);
        if (RemoveScheduled_.load()) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunk,
                "Cannot read chunk %v since it is scheduled for removal",
                Id_);
        }
        ReaderSweepLatch_ += 2;
        lockCount = ++ReadLockCounter_;
        result = PrepareReader(guard);
    }

    YT_LOG_TRACE("Chunk read lock acquired (ChunkId: %v, LockCount: %v)",
        Id_,
        lockCount);

    return result;
}

void TChunkBase::ReleaseReadLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    bool removeNow = false;
    bool scheduleReaderSweep = false;
    int lockCount;
    {
        auto guard = ReaderGuard(LifetimeLock_);
        lockCount = --ReadLockCounter_;
        YT_VERIFY(lockCount >= 0);
        if (lockCount == 0) {
            if (UpdateLockCounter_ == 0 && RemoveScheduled_.load()) {
                removeNow = !Removing_.exchange(true);
            }
            scheduleReaderSweep = (ReaderSweepLatch_.exchange(1) & 1) == 0;
        }
    }

    YT_LOG_TRACE("Chunk read lock released (ChunkId: %v, LockCount: %v)",
        Id_,
        lockCount);

    if (scheduleReaderSweep) {
        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        chunkRegistry->ScheduleChunkReaderSweep(this);
    }

    if (removeNow) {
        StartAsyncRemove();
    }
}

void TChunkBase::AcquireUpdateLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        auto guard = WriterGuard(LifetimeLock_);
        if (RemoveScheduled_.load()) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunk,
                "Cannot acquire update lock for chunk %v since it is scheduled for removal",
                Id_);
        }
        if (UpdateLockCounter_ > 0) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::ConcurrentChunkUpdate,
                "Cannot acquire update lock for chunk %v since it is already locked by another update",
                Id_);
        }
        YT_VERIFY(++UpdateLockCounter_ == 1);
    }

    YT_LOG_DEBUG("Chunk update lock acquired (ChunkId: %v)",
        Id_);
}

void TChunkBase::ReleaseUpdateLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    bool removeNow = false;
    {
        auto guard = WriterGuard(LifetimeLock_);
        YT_VERIFY(--UpdateLockCounter_ == 0);
        if (ReadLockCounter_.load() == 0 && RemoveScheduled_.load()) {
            removeNow = !Removing_.exchange(true);;
        }
    }

    YT_LOG_DEBUG("Chunk update lock released (ChunkId: %v)",
        Id_);

    if (removeNow) {
        StartAsyncRemove();
    }
}

TFuture<void> TChunkBase::ScheduleRemove()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Chunk remove scheduled (ChunkId: %v)",
        Id_);

    bool removeNow = false;
    {
        auto guard = WriterGuard(LifetimeLock_);
        if (RemoveScheduled_.load()) {
            return RemovedFuture_;
        }

        RemovedPromise_ = NewPromise<void>();
        // NB: Ignore client attempts to cancel the removal process.
        RemovedFuture_ = RemovedPromise_.ToFuture().ToUncancelable();
        RemoveScheduled_.store(true);

        if (ReadLockCounter_.load() == 0 && UpdateLockCounter_ == 0) {
            removeNow = !Removing_.exchange(true);
        }
    }

    if (removeNow) {
        StartAsyncRemove();
    }

    return RemovedFuture_;
}

bool TChunkBase::IsRemoveScheduled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RemoveScheduled_.load();
}

void TChunkBase::TrySweepReader()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(LifetimeLock_);

    auto readerSweepLatch = ReaderSweepLatch_.load();
    YT_VERIFY((readerSweepLatch & 1) != 0);

    if (ReadLockCounter_.load() > 0) {
        // Sweep will be re-scheduled when the last reader leases the lock.
        ReaderSweepLatch_.store(readerSweepLatch & ~1);
        return;
    }

    if (readerSweepLatch != 1) {
        guard.Release();
        // Re-schedule the sweep right away.
        const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
        chunkRegistry->ScheduleChunkReaderSweep(this);
        return;
    }

    ReleaseReader(guard);
}

void TChunkBase::StartAsyncRemove()
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        auto guard = WriterGuard(LifetimeLock_);
        ReleaseReader(guard);
    }

    RemovedPromise_.SetFrom(AsyncRemove());
}

TFuture<void> TChunkBase::PrepareReader(TReaderGuard& /* readerGuard */)
{
    return {};
}

void TChunkBase::ReleaseReader(TWriterGuard& /* writerGuard */)
{ }

TRefCountedChunkMetaPtr TChunkBase::FilterMeta(
    TRefCountedChunkMetaPtr meta,
    const std::optional<std::vector<int>>& extensionTags)
{
    return extensionTags
        ? New<TRefCountedChunkMeta>(FilterChunkMetaByExtensionTags(*meta, extensionTags))
        : std::move(meta);
}

void TChunkBase::StartReadSession(
    const TReadSessionBasePtr& session,
    const TChunkReadOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    session->Options = options;
    session->ChunkReadGuard = TChunkReadGuard::Acquire(this);
}

void TChunkBase::ProfileReadBlockSetLatency(const TReadSessionBasePtr& session)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto& performanceCounters = Location_->GetPerformanceCounters();
    performanceCounters.BlobBlockReadLatencies[session->Options.WorkloadDescriptor.Category]
        .Record(session->SessionTimer.GetElapsedTime());
}

void TChunkBase::ProfileReadMetaLatency(const TReadSessionBasePtr& session)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto& performanceCounters = Location_->GetPerformanceCounters();
    performanceCounters.BlobChunkMetaReadLatencies[session->Options.WorkloadDescriptor.Category]
        .Record(session->SessionTimer.GetElapsedTime());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
