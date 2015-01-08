#include "stdafx.h"
#include "chunk_detail.h"
#include "location.h"
#include "session_manager.h"
#include "private.h"

#include <core/misc/fs.h>

#include <server/cell_node/bootstrap.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

namespace NYT {
namespace NDataNode {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkFilesHolder::TChunkFilesHolder(
    TLocationPtr location,
    const TChunkId& id)
    : Location_(location)
    , Id_(id)
{ }

void TChunkFilesHolder::Remove()
{
    try {
        LOG_DEBUG("Started removing chunk files (ChunkId: %v)", Id_);

        auto partNames = Location_->GetChunkPartNames(Id_);
        auto directory = NFS::GetDirectoryName(Location_->GetChunkPath(Id_));

        for (const auto& name : partNames) {
            auto fileName = NFS::CombinePaths(directory, name);
            NFS::Remove(fileName);
        }

        LOG_DEBUG("Finished removing chunk files (ChunkId: %v)", Id_);
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error removing chunk %v",
            Id_)
            << ex;
        LOG_ERROR(error);
        Location_->Disable(error);
    }
}

void TChunkFilesHolder::MoveToTrash()
{
    try {
        LOG_DEBUG("Started moving chunk files to trash (ChunkId: %v)", Id_);

        auto partNames = Location_->GetChunkPartNames(Id_);
        auto directory = NFS::GetDirectoryName(Location_->GetChunkPath(Id_));
        auto trashDirectory = NFS::GetDirectoryName(Location_->GetTrashChunkPath(Id_));

        for (const auto& name : partNames) {
            auto srcFileName = NFS::CombinePaths(directory, name);
            auto dstFileName = NFS::CombinePaths(trashDirectory, name);
            NFS::Replace(srcFileName, dstFileName);
            NFS::Touch(dstFileName);
        }

        LOG_DEBUG("Finished moving chunk files to trash (ChunkId: %v)", Id_);

        Location_->RegisterTrashChunk(Id_);
    } catch (const std::exception& ex) {
        auto error = TError(
            NChunkClient::EErrorCode::IOError,
            "Error moving chunk %v to trash",
            Id_)
             << ex;
        LOG_ERROR(error);
        Location_->Disable(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkBase::TChunkBase(
    TBootstrap* bootstrap,
    TLocationPtr location,
    const TChunkId& id)
    : Bootstrap_(bootstrap)
    , Location_(location)
    , Id_(id)
    , FilesHolder_(New<TChunkFilesHolder>(Location_, Id_))
{ }

const TChunkId& TChunkBase::GetId() const
{
    return Id_;
}

TLocationPtr TChunkBase::GetLocation() const
{
    return Location_;
}

Stroka TChunkBase::GetFileName() const
{
    return Location_->GetChunkPath(Id_);
}

int TChunkBase::GetVersion() const
{
    return Version_;
}

void TChunkBase::IncrementVersion()
{
    ++Version_;
}

bool TChunkBase::TryAcquireReadLock()
{
    int lockCount;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (RemovedPromise_) {
            LOG_DEBUG("Chunk read lock cannot be acquired since removal is already pending (ChunkId: %v)",
                Id_);
            return false;
        }

        lockCount = ++ReadLockCounter_;
    }

    LOG_DEBUG("Chunk read lock acquired (ChunkId: %v, LockCount: %v)",
        Id_,
        lockCount);

    return true;
}

void TChunkBase::ReleaseReadLock()
{
    bool removing = false;
    int lockCount;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        lockCount = --ReadLockCounter_;
        YCHECK(lockCount >= 0);
        if (ReadLockCounter_ == 0 && !Removing_ && RemovedPromise_) {
            removing = Removing_ = true;
        }
    }

    LOG_DEBUG("Chunk read lock released (ChunkId: %v, LockCount: %v)",
        Id_,
        lockCount);

    if (removing) {
        StartAsyncRemove();
    }
}

bool TChunkBase::IsReadLockAcquired() const
{
    return ReadLockCounter_ > 0;
}

TFuture<void> TChunkBase::ScheduleRemove()
{
    LOG_INFO("Chunk remove scheduled (ChunkId: %v)",
        Id_);

    bool removing = false;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (RemovedPromise_) {
            return RemovedPromise_;
        }

        RemovedPromise_ = NewPromise<void>();
        if (ReadLockCounter_ == 0 && !Removing_) {
            removing = Removing_ = true;
        }
    }

    if (removing) {
        StartAsyncRemove();
    }

    return RemovedPromise_;
}

bool TChunkBase::IsRemoveScheduled() const
{
    TGuard<TSpinLock> guard(SpinLock_);
    return static_cast<bool>(RemovedPromise_);
}

void TChunkBase::StartAsyncRemove()
{
    RemovedPromise_.SetFrom(AsyncRemove());
}

TRefCountedChunkMetaPtr TChunkBase::FilterCachedMeta(const std::vector<int>* tags) const
{
    YCHECK(Meta_);
    return tags
        ? New<TRefCountedChunkMeta>(FilterChunkMetaByExtensionTags(*Meta_, *tags))
        : Meta_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
