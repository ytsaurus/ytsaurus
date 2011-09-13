#include "snapshot_creator.h"
#include "meta_state_manager_rpc.h"

#include "../misc/serialize.h"
#include "../actions/action_util.h"

#include <util/system/fs.h>

namespace NYT {

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
        LOG_INFO("Creating a distributed snapshot for state %s",
            ~Version.ToString());

        const TConfig& config = Creator->Config;
        for (TPeerId peerId = 0; peerId < Creator->CellManager->GetPeerCount(); ++peerId) {
            if (peerId == Creator->CellManager->GetSelfId()) continue;
            LOG_DEBUG("Requesting peer %d to create a snapshot",
                peerId);

            auto proxy = Creator->CellManager->GetMasterProxy<TProxy>(peerId);
            auto request = proxy->AdvanceSegment();
            request->SetSegmentId(Version.SegmentId);
            request->SetRecordCount(Version.RecordCount);
            request->SetEpoch(Creator->Epoch.ToProto());

            Awaiter->Await(request->Invoke(config.Timeout), FromMethod(
                &TSession::OnRemote,
                TPtr(this),
                peerId));
        }

        auto asyncResult = Creator->DoCreateLocal(Version);

        Awaiter->Await(
            asyncResult,
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
        Checksums[Creator->CellManager->GetSelfId()] = MakePair(result.Checksum, true);
    }

    void OnRemote(TProxy::TRspAdvanceSegment::TPtr response, TPeerId peerId)
    {
        if (!response->IsOK()) {
            LOG_WARNING("Error %s requesting peer %d to create a snapshot at state %s",
                ~response->GetErrorCode().ToString(),
                peerId,
                ~Version.ToString());
            return;
        }

        TChecksum checksum = response->GetChecksum();
        LOG_INFO("Remote snapshot is created (PeerId: %d, Checksum: %" PRIx64 ")",
            peerId,
            checksum);

        Checksums[peerId] = MakePair(checksum, true);
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
    , StateInvoker(metaState->GetInvoker())
{ }

void TSnapshotCreator::CreateDistributed(TMetaVersion version)
{
    New<TSession>(this, version)->Run();
}

TSnapshotCreator::TAsyncLocalResult::TPtr TSnapshotCreator::CreateLocal(
    TMetaVersion version)
{
    LOG_INFO("Creating a local snapshot for state (%d, %d)",
               version.SegmentId, version.RecordCount);
    return 
        FromMethod(
            &TSnapshotCreator::DoCreateLocal,
            TPtr(this),
            version)
        ->AsyncVia(StateInvoker)
        ->Do();
}

TSnapshotCreator::TAsyncLocalResult::TPtr TSnapshotCreator::DoCreateLocal(
    TMetaVersion version)
{
    // TODO: handle IO errors
    if (MetaState->GetVersion() != version) {
        LOG_WARNING("Invalid version, snapshot creation canceled: expected %s, found %s",
            ~version.ToString(),
            ~MetaState->GetVersion().ToString());
        return New<TAsyncLocalResult>(TLocalResult(EResultCode::InvalidVersion));
    }

    // Prepare writer.
    i32 snapshotId = version.SegmentId + 1;
    TSnapshotWriter::TPtr writer = SnapshotStore->GetWriter(snapshotId);
    writer->Open(version.RecordCount);
    
    TOutputStream* stream = &writer->GetStream();

    // Start an async snapshot creation process.
    TAsyncResult<TVoid>::TPtr saveResult = MetaState->Save(stream);

    // Switch to a new changelog.
    MetaState->RotateChangeLog();

    // The writer reference is being held by the closure action.
    return saveResult->Apply(FromMethod(
        &TSnapshotCreator::OnSave,
        snapshotId,
        writer));
}

TSnapshotCreator::TLocalResult TSnapshotCreator::OnSave(
    TVoid /* fake */,
    i32 segmentId,
    TSnapshotWriter::TPtr writer)
{
    writer->Close();

    LOG_INFO("Local snapshot is created (SegmentId: %d, Checksum: %" PRIx64 ")",
        segmentId,
        writer->GetChecksum());

    return TLocalResult(EResultCode::OK, writer->GetChecksum());
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
