#include "snapshot_creator.h"
#include "master_state_manager_rpc.h"

#include "../misc/serialize.h"
#include "../actions/action_util.h"

#include <util/system/fs.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MasterLogger;

////////////////////////////////////////////////////////////////////////////////

class TSnapshotCreator::TSession
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TSnapshotCreator::TPtr creator,
        TMasterStateId stateId)
        : Creator(creator)
        , StateId(stateId)
        , Awaiter(new TParallelAwaiter(Creator->ServiceInvoker))
        , Checksums(Creator->CellManager->GetMasterCount())
    { }

    void CreateDistributed()
    {
        LOG_INFO("Creating a distributed snapshot for state %s",
            ~StateId.ToString());

        const TConfig& config = Creator->Config;
        for (TMasterId masterId = 0; masterId < Creator->CellManager->GetMasterCount(); ++masterId) {
            if (masterId == Creator->CellManager->GetSelfId()) continue;
            LOG_DEBUG("Requesting master %d to create a snapshot",
                masterId);

            TAutoPtr<TProxy> proxy = Creator->CellManager->GetMasterProxy<TProxy>(masterId);
            TProxy::TReqCreateSnapshot::TPtr request = proxy->CreateSnapshot();
            request->SetSegmentId(StateId.SegmentId);
            request->SetChangeCount(StateId.ChangeCount);
            request->SetEpoch(Creator->Epoch.ToProto());

            Awaiter->Await(request->Invoke(config.Timeout), FromMethod(
                &TSession::OnRemote,
                TPtr(this),
                masterId));
        }

        TAsyncLocalResult::TPtr asyncResult = Creator->DoCreateLocal(StateId);

        Awaiter->Await(
            asyncResult,
            FromMethod(&TSession::OnLocal, TPtr(this)));

        Awaiter->Complete(FromMethod(&TSession::OnComplete, TPtr(this)));
    }

private:
    void OnComplete()
    {
        for (TMasterId id1 = 0; id1 < Checksums.ysize(); ++id1) {
            for (TMasterId id2 = id1 + 1; id2 < Checksums.ysize(); ++id2) {
                TPair<TChecksum, bool> checksum1 = Checksums[id1];
                TPair<TChecksum, bool> checksum2 = Checksums[id2];
                if (checksum1.Second() && checksum2.Second() && 
                    checksum1.First() != checksum2.First())
                {
                    LOG_FATAL(
                        "Snapshot checksum mismatch: "
                        "master %d reported %" PRIx64 ", "
                        "master %d reported %" PRIx64,
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

    void OnRemote(TProxy::TRspCreateSnapshot::TPtr response, TMasterId masterId)
    {
        if (!response->IsOK()) {
            LOG_WARNING("Error %s requesting master %d to create a snapshot at state %s",
                ~response->GetErrorCode().ToString(),
                masterId,
                ~StateId.ToString());
            return;
        }

        TChecksum checksum = response->GetChecksum();
        LOG_INFO("Remote snapshot is created (MasterId: %d, Checksum: %" PRIx64 ")",
            masterId,
            checksum);

        Checksums[masterId] = MakePair(checksum, true);
    }

    TSnapshotCreator::TPtr Creator;
    TMasterStateId StateId;
    TParallelAwaiter::TPtr Awaiter;
    yvector< TPair<TChecksum, bool> > Checksums;
};

////////////////////////////////////////////////////////////////////////////////

TSnapshotCreator::TSnapshotCreator(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    TDecoratedMasterState::TPtr masterState,
    TChangeLogCache::TPtr changeLogCache,
    TSnapshotStore::TPtr snapshotStore,
    TMasterEpoch epoch,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , CellManager(cellManager)
    , MasterState(masterState)
    , SnapshotStore(snapshotStore)
    , ChangeLogCache(changeLogCache)
    , Epoch(epoch)
    , ServiceInvoker(serviceInvoker)
    , StateInvoker(masterState->GetInvoker())
{ }

void TSnapshotCreator::CreateDistributed(TMasterStateId stateId)
{
    TSession::TPtr session = new TSession(TPtr(this), stateId);
    session->CreateDistributed();
}

TSnapshotCreator::TAsyncLocalResult::TPtr TSnapshotCreator::CreateLocal(
    TMasterStateId stateId)
{
    LOG_INFO("Creating a local snapshot for state (%d, %d)",
               stateId.SegmentId, stateId.ChangeCount);
    return 
        FromMethod(
            &TSnapshotCreator::DoCreateLocal,
            TPtr(this),
            stateId)
        ->AsyncVia(StateInvoker)
        ->Do();
}

TSnapshotCreator::TAsyncLocalResult::TPtr TSnapshotCreator::DoCreateLocal(
    TMasterStateId stateId)
{
    // TODO: handle IO errors
    if (MasterState->GetStateId() != stateId) {
        LOG_WARNING("Invalid state id, snapshot creation canceled: expected %s, found %s",
            ~stateId.ToString(),
            ~MasterState->GetStateId().ToString());
        return new TAsyncLocalResult(TLocalResult(EResultCode::InvalidStateId));
    }

    // Prepare writer.
    i32 snapshotId = stateId.SegmentId + 1;
    TSnapshotWriter::TPtr writer = SnapshotStore->GetWriter(snapshotId);
    writer->Open(stateId.ChangeCount);
    TOutputStream& output = writer->GetStream();

    // Start an async snapshot creation process.
    TAsyncResult<TVoid>::TPtr saveResult = MasterState->Save(output);

    // Switch to a new changelog.
    MasterState->RotateChangeLog();

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
