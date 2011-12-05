#include "stdafx.h"
#include "snapshot_creator.h"
#include "meta_state_manager_rpc.h"

#include "../misc/serialize.h"
#include "../actions/action_util.h"

#include <util/system/fs.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

class TSnapshotCreator::TSession
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TSnapshotCreator::TPtr creator,
        TMetaVersion version)
        : Creator(creator)
        , Version(version)
        , Awaiter(New<TParallelAwaiter>(Creator->ServiceInvoker))
        , Checksums(Creator->CellManager->GetPeerCount())
    { }

    void Run()
    {
        LOG_INFO("Creating a distributed snapshot (Version: %s)",
            ~Version.ToString());

        const TConfig& config = Creator->Config;
        for (TPeerId followerId = 0; followerId < Creator->CellManager->GetPeerCount(); ++followerId) {
            if (followerId == Creator->CellManager->GetSelfId()) continue;
            LOG_DEBUG("Requesting follower to create a snapshot (FollowerId: %d)",
                followerId);

            auto proxy = Creator->CellManager->GetMasterProxy<TProxy>(followerId);
            proxy->SetTimeout(config.Timeout);
            auto request = proxy->AdvanceSegment();
            request->SetSegmentId(Version.SegmentId);
            request->SetRecordCount(Version.RecordCount);
            request->SetEpoch(Creator->Epoch.ToProto());
            request->SetCreateSnapshot(true);

            Awaiter->Await(
                request->Invoke(),
                FromMethod(
                    &TSession::OnRemote,
                    TPtr(this),
                    followerId));
        }

        Awaiter->Await(
            Creator->CreateLocal(Version),
            FromMethod(&TSession::OnLocal, TPtr(this)));

        Awaiter->Complete(FromMethod(&TSession::OnComplete, TPtr(this)));
    }

private:
    void OnComplete()
    {
        for (TPeerId id1 = 0; id1 < Checksums.ysize(); ++id1) {
            for (TPeerId id2 = id1 + 1; id2 < Checksums.ysize(); ++id2) {
                const auto& checksum1 = Checksums[id1];
                const auto& checksum2 = Checksums[id2];
                if (checksum1.Second() && checksum2.Second() && 
                    checksum1.First() != checksum2.First())
                {
                    // TODO: consider killing followers
                    LOG_FATAL(
                        "Snapshot checksum mismatch: "
                        "peer %d reported %" PRIx64 ", "
                        "peer %d reported %" PRIx64,
                        id1, checksum1.First(),
                        id2, checksum2.First());
                }
            }
        }

        LOG_INFO("Distributed snapshot is created");
    }

    void OnLocal(TLocalResult result)
    {
        YASSERT(result.ResultCode == EResultCode::OK);

        Checksums[Creator->CellManager->GetSelfId()] = MakePair(result.Checksum, true);
    }

    void OnRemote(TProxy::TRspAdvanceSegment::TPtr response, TPeerId followerId)
    {
        if (!response->IsOK()) {
            LOG_WARNING("Error creating a snapshot at follower (FollowerId: %d, Version: %s, Error: %s)",
                followerId,
                ~Version.ToString(),
                ~response->GetError().ToString());
            return;
        }

        auto checksum = response->GetChecksum();
        LOG_INFO("Remote snapshot is created (FollowerId: %d, Checksum: %" PRIx64 ")",
            followerId,
            checksum);

        Checksums[followerId] = MakePair(checksum, true);
    }

    TSnapshotCreator::TPtr Creator;
    TMetaVersion Version;
    TParallelAwaiter::TPtr Awaiter;
    yvector< TPair<TChecksum, bool> > Checksums;
};

////////////////////////////////////////////////////////////////////////////////

TSnapshotCreator::TSnapshotCreator(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMetaState::TPtr metaState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TEpoch epoch,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , CellManager(cellManager)
    , MetaState(metaState)
    , SnapshotStore(snapshotStore)
    , ChangeLogCache(changeLogCache)
    , Epoch(epoch)
    , ServiceInvoker(serviceInvoker)
    , LocalProgress(ToFuture(TVoid()))
{
    YASSERT(~cellManager != NULL);
    YASSERT(~metaState != NULL);
    YASSERT(~changeLogCache != NULL);
    YASSERT(~snapshotStore != NULL);
    YASSERT(~serviceInvoker != NULL);

    StateInvoker = metaState->GetStateInvoker();
}

TSnapshotCreator::EResultCode TSnapshotCreator::CreateDistributed()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (IsInProgress()) {
        return EResultCode::AlreadyInProgress;
    }

    auto version = MetaState->GetVersion();
    New<TSession>(TPtr(this), version)->Run();
    return EResultCode::OK;
}

TSnapshotCreator::TAsyncLocalResult::TPtr TSnapshotCreator::CreateLocal(
    TMetaVersion version)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (IsInProgress()) {
        LOG_ERROR("Could not create local snapshot, snapshot creation is already in progress (Version: %s)",
            ~version.ToString());
        return New<TAsyncLocalResult>(TLocalResult(EResultCode::AlreadyInProgress));
    }

    LocalProgress = New< TFuture<TVoid> >();

    LOG_INFO("Creating a local snapshot (Version: %s)", ~version.ToString());

    // TODO: handle IO errors
    if (MetaState->GetVersion() != version) {
        LOG_WARNING("Invalid version, snapshot creation canceled (expected: %s, received: %s)",
            ~version.ToString(),
            ~MetaState->GetVersion().ToString());
        return New<TAsyncLocalResult>(TLocalResult(EResultCode::InvalidVersion));
    }

    // Prepare writer.
    i32 snapshotId = version.SegmentId + 1;
    auto writer = SnapshotStore->GetWriter(snapshotId);
    writer->Open(version.RecordCount);
    
    auto* stream = &writer->GetStream();

    // Start an async snapshot creation process.
    auto saveResult = MetaState->Save(stream);

    // Switch to a new changelog.
    MetaState->RotateChangeLog();

    // The writer reference is being held by the closure action.
    return saveResult->Apply(FromMethod(
        &TSnapshotCreator::OnSave,
        TPtr(this),
        snapshotId,
        writer));
}

TSnapshotCreator::TLocalResult TSnapshotCreator::OnSave(
    TVoid /* fake */,
    i32 segmentId,
    TSnapshotWriter::TPtr writer)
{
    writer->Close();

    SnapshotStore->UpdateMaxSnapshotId(segmentId);

    LOG_INFO("Local snapshot is created (SegmentId: %d, Checksum: %" PRIx64 ")",
        segmentId,
        writer->GetChecksum());

    LocalProgress->Set(TVoid());

    return TLocalResult(EResultCode::OK, writer->GetChecksum());
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
