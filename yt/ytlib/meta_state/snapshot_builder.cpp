#include "stdafx.h"
#include "snapshot_builder.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/actions/action_util.h>

#include <util/system/fs.h>

#if defined(_unix_)
 /* for fork() */
#include <sys/types.h>
#include <unistd.h>
 /* for wait*() */
#include <sys/wait.h>
#endif

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder::TSession
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TSnapshotBuilder::TPtr creator,
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

        auto& config = Creator->Config;
        for (TPeerId followerId = 0; followerId < Creator->CellManager->GetPeerCount(); ++followerId) {
            if (followerId == Creator->CellManager->GetSelfId()) continue;
            LOG_DEBUG("Requesting follower %d to create a snapshot", followerId);

            auto proxy = Creator->CellManager->GetMasterProxy<TProxy>(followerId);
            proxy->SetTimeout(config->RemoteTimeout);
            auto request = proxy->AdvanceSegment();
            request->set_segment_id(Version.SegmentId);
            request->set_record_count(Version.RecordCount);
            request->set_epoch(Creator->Epoch.ToProto());
            request->set_create_snapshot(true);

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
                if (checksum1 && checksum2 && checksum1 != checksum2) {
                    // TODO: consider killing followers
                    LOG_FATAL(
                        "Snapshot checksum mismatch: "
                        "peer %d reported %" PRIx64 ", "
                        "peer %d reported %" PRIx64,
                        id1, *checksum1,
                        id2, *checksum2);
                }
            }
        }

        LOG_INFO("Distributed snapshot is created");
    }

    void OnLocal(TLocalResult result)
    {
        if (result.ResultCode != EResultCode::OK) {
            LOG_FATAL("Failed to create snapshot locally (Result: %s)",
                ~result.ResultCode.ToString());
        }
        Checksums[Creator->CellManager->GetSelfId()] = MakeNullable(result.Checksum);
    }

    void OnRemote(TProxy::TRspAdvanceSegment::TPtr response, TPeerId followerId)
    {
        if (!response->IsOK()) {
            LOG_WARNING("Error creating a snapshot at follower %d (Version: %s)\n%s",
                followerId,
                ~Version.ToString(),
                ~response->GetError().ToString());
            return;
        }

        auto checksum = response->checksum();
        LOG_INFO("Remote snapshot is created at follower %d (Checksum: %" PRIx64 ")",
            followerId,
            checksum);

        Checksums[followerId] = checksum;
    }

    TSnapshotBuilder::TPtr Creator;
    TMetaVersion Version;
    TParallelAwaiter::TPtr Awaiter;
    yvector< TNullable<TChecksum> > Checksums;
};

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilder::TSnapshotBuilder(
    TConfig* config,
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
    , LocalResult(MakeFuture(TLocalResult()))
    , CurrentSnapshotId(-1)
{
    YASSERT(cellManager);
    YASSERT(metaState);
    YASSERT(changeLogCache);
    YASSERT(snapshotStore);
    YASSERT(serviceInvoker);

    StateInvoker = metaState->GetStateInvoker();
}

TSnapshotBuilder::EResultCode TSnapshotBuilder::CreateDistributed()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto version = MetaState->GetVersion();
    New<TSession>(TPtr(this), version)->Run();
    return EResultCode::OK;
}

TSnapshotBuilder::TAsyncLocalResult::TPtr TSnapshotBuilder::CreateLocal(
    TMetaVersion version)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (IsInProgress()) {
        LOG_ERROR("Unable to create a local snapshot, snapshot creation is already in progress (Version: %s)",
            ~version.ToString());
        return MakeFuture(TLocalResult(EResultCode::AlreadyInProgress));
    }
    LocalResult = New<TAsyncLocalResult>();

    LOG_INFO("Creating a local snapshot (Version: %s)", ~version.ToString());

    // TODO: handle IO errors
    if (MetaState->GetVersion() != version) {
        LOG_WARNING("Invalid version, snapshot creation canceled: expected %s, received %s",
            ~version.ToString(),
            ~MetaState->GetVersion().ToString());
        return MakeFuture(TLocalResult(EResultCode::InvalidVersion));
    }

    CurrentSnapshotId = version.SegmentId + 1;

#if defined(_unix_)
    LOG_TRACE("Going to fork...");
    ChildPid = fork();
    if (ChildPid == -1) {
        LOG_ERROR("Could not fork while creating local snapshot %d", CurrentSnapshotId);
        LocalResult->Set(TLocalResult(EResultCode::ForkError));
    } else if (ChildPid == 0) {
        DoCreateLocal(version);
        _exit(0);
    } else {
        LOG_TRACE("Forked successfully. Starting watchdog thread...");
        WatchdogThread.Reset(new TThread(WatchdogThreadFunc, (void*) this));
        WatchdogThread->Start();
    }
#else
    auto checksum = DoCreateLocal(version);
    OnLocalCreated(checksum);
#endif
        
    return LocalResult;
}

TChecksum TSnapshotBuilder::DoCreateLocal(TMetaVersion version)
{
    auto writer = SnapshotStore->GetWriter(CurrentSnapshotId);
    writer->Open(version.RecordCount);
    auto* stream = &writer->GetStream();
    MetaState->Save(stream);
    MetaState->RotateChangeLog();
    writer->Close();
    return writer->GetChecksum();
}

void TSnapshotBuilder::OnLocalCreated(const TChecksum& checksum)
{
    SnapshotStore->UpdateMaxSnapshotId(CurrentSnapshotId);

    LOG_INFO("Local snapshot %d is created (Checksum: %" PRIx64 ")",
        CurrentSnapshotId,
        checksum);

    LocalResult->Set(TLocalResult(EResultCode::OK, checksum));
}

#if defined(_unix_)
void* TSnapshotBuilder::WatchdogThreadFunc(void* param)
{
    auto* snapshotBuilder = static_cast<TSnapshotBuilder*>(param);
    auto deadline = snapshotBuilder->Config->LocalTimeout.ToDeadLine();
    auto childPid = snapshotBuilder->ChildPid;
    auto segmentId = snapshotBuilder->CurrentSnapshotId;
    auto localResult = snapshotBuilder->LocalResult;
    int status;
    LOG_DEBUG("Waiting for child process (ChildPID: %d)",
        childPid);
    while (waitpid(childPid, &status, WNOHANG) == 0) {
        if (TInstant::Now() <= deadline) {
            sleep(1);
        } else {
            LOG_ERROR("Local snapshot creating timed out, killing child process (SegmentId: %d)",
                segmentId);
            kill(childPid, 9);
            localResult->Set(TLocalResult(EResultCode::TimeoutExceeded));
            return NULL;
        }
    }
    if (!WIFEXITED(status)) {
        LOG_ERROR("Local snapshot creating child process has not terminated correctly (SegmentId: %d)",
                segmentId);
        localResult->Set(TLocalResult(EResultCode::ForkError));
        return NULL;
    }
    auto exitStatus = WEXITSTATUS(status);
    LOG_INFO("Local snapshot creating child process exited with exit status %d (SegmentId: %d)",
        exitStatus,
        segmentId);

    auto result = snapshotBuilder->SnapshotStore->GetReader(segmentId);
    if (!result.IsOK()) {
        LOG_ERROR("Cannot open snapshot (SnapshotId: %d)\n%s",
            segmentId,
            ~result.ToString());
        localResult->Set(TLocalResult(EResultCode::ForkError));
        return NULL;
    }
    auto reader = result.Value();
    reader->Open();
    auto checksum = reader->GetChecksum();
    reader->Close();
    snapshotBuilder->OnSave(checksum);
    return NULL;
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
