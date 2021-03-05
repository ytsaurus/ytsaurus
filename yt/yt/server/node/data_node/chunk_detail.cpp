#include "chunk_detail.h"
#include "private.h"
#include "location.h"
#include "session_manager.h"
#include "chunk_meta_manager.h"

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

void TChunkBase::AcquireReadLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    int lockCount;
    {
        auto guard = Guard(SpinLock_);
        if (RemovedFuture_) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunk,
                "Cannot read chunk %v since it is scheduled for removal",
                Id_);
        }
        lockCount = ++ReadLockCounter_;
    }

    YT_LOG_TRACE("Chunk read lock acquired (ChunkId: %v, LockCount: %v)",
        Id_,
        lockCount);
}

void TChunkBase::ReleaseReadLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    bool removeNow = false;
    int lockCount;
    {
        auto guard = Guard(SpinLock_);
        lockCount = --ReadLockCounter_;
        YT_VERIFY(lockCount >= 0);
        if (ReadLockCounter_ == 0 && UpdateLockCounter_ == 0 && !Removing_ && RemovedFuture_) {
            removeNow = Removing_ = true;
        }
    }

    YT_LOG_TRACE("Chunk read lock released (ChunkId: %v, LockCount: %v)",
        Id_,
        lockCount);

    if (removeNow) {
        StartAsyncRemove();
    }
}

void TChunkBase::AcquireUpdateLock()
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        auto guard = Guard(SpinLock_);
        if (RemovedFuture_) {
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
        auto guard = Guard(SpinLock_);
        YT_VERIFY(--UpdateLockCounter_ == 0);
        if (ReadLockCounter_ == 0 && !Removing_ && RemovedFuture_) {
            removeNow = Removing_ = true;
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
        auto guard = Guard(SpinLock_);
        if (RemovedFuture_) {
            return RemovedFuture_;
        }

        RemovedPromise_ = NewPromise<void>();
        // NB: Ignore client attempts to cancel the removal process.
        RemovedFuture_ = RemovedPromise_.ToFuture().ToUncancelable();

        if (ReadLockCounter_ == 0 && UpdateLockCounter_ == 0 && !Removing_) {
            removeNow = Removing_ = true;
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

    auto guard = Guard(SpinLock_);
    return RemovedFuture_.operator bool();
}

void TChunkBase::StartAsyncRemove()
{
    VERIFY_THREAD_AFFINITY_ANY();

    RemovedPromise_.SetFrom(AsyncRemove());
}

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
    const TBlockReadOptions& options)
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
