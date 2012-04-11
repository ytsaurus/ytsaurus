#include "stdafx.h"
#include "snapshot_builder.h"
#include "common.h"
#include "config.h"
#include "meta_state_manager_proxy.h"
#include "meta_version.h"
#include "decorated_meta_state.h"
#include "snapshot_store.h"
#include "snapshot.h"
#include "change_log_cache.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/actions/bind.h>
#include <ytlib/profiling/profiler.h>

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

using namespace NElection;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MetaState");
static NProfiling::TProfiler Profiler("/meta_state");

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder::TSession
    : public TRefCounted
{
public:
    TSession(
        TSnapshotBuilderPtr owner,
        const TMetaVersion& version,
        bool createSnapshot)
        : Owner(owner)
        , Version(version)
        , CreateSnapshot(createSnapshot)
    { }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner->StateThread);

        if (CreateSnapshot) {
            LOG_INFO("Creating a snapshot at version %s", ~Version.ToString());

            Checksums.resize(Owner->CellManager->GetPeerCount());
            Awaiter = New<TParallelAwaiter>(
                ~Owner->EpochControlInvoker,
                &Profiler,
                "snapshot_build_time");
        } else {
            LOG_INFO("Rotating changelog at version %s", ~Version.ToString());

            Awaiter = New<TParallelAwaiter>(~Owner->EpochControlInvoker);
        }

        Owner->EpochControlInvoker->Invoke(BIND(
            &TSession::DoSendRequests,
            MakeStrong(this)));
        
        if (CreateSnapshot) {
            Awaiter->Await(
                Owner->CreateLocalSnapshot(Version),
                BIND(&TSession::OnLocalSnapshotCreated, MakeStrong(this)));

            // The awaiter must be completed from the control thread.
            Owner->EpochControlInvoker->Invoke(BIND(
                &TSession::DoCompleteSession,
                MakeStrong(this)));
        } else {
            Owner->DecoratedState->RotateChangeLog();
            // No need to complete the awaiter.
        }
    }

private:
    void DoSendRequests()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        for (TPeerId id = 0; id < Owner->CellManager->GetPeerCount(); ++id) {
            if (id == Owner->CellManager->GetSelfId()) continue;

            if (CreateSnapshot) {
                LOG_DEBUG("Requesting follower %d to create a snapshot", id);
            } else {
                LOG_DEBUG("Requesting follower %d to rotate the changelog", id);
            }

            auto proxy = Owner->CellManager->GetMasterProxy<TProxy>(id);
            auto request = proxy
                ->AdvanceSegment()
                ->SetTimeout(Owner->Config->RemoteTimeout);
            request->set_segment_id(Version.SegmentId);
            request->set_record_count(Version.RecordCount);
            *request->mutable_epoch() = Owner->Epoch.ToProto();
            request->set_create_snapshot(CreateSnapshot);

            auto responseHandler =
                CreateSnapshot
                ? BIND(&TSession::OnSnapshotCreated, MakeStrong(this), id)
                : BIND(&TSession::OnChangeLogRotated, MakeStrong(this), id);
            Awaiter->Await(
                request->Invoke(),
                EscapeYPath(Owner->CellManager->GetPeerAddress(id)),
                responseHandler);
        }

        Owner->DecoratedState->SetPingVersion(TMetaVersion(Version.SegmentId + 1, 0));
    }

    void DoCompleteSession()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        Awaiter->Complete(BIND(&TSession::OnComplete, MakeStrong(this)));
    }

    void OnComplete()
    {
        int successCount = 0;
        for (TPeerId id1 = 0; id1 < Checksums.ysize(); ++id1) {
            const auto& checksum1 = Checksums[id1];
            if (checksum1) {
                ++successCount;
            }
            for (TPeerId id2 = id1 + 1; id2 < Checksums.ysize(); ++id2) {
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

        LOG_INFO("Distributed snapshot creation finished (SuccessCount: %d)", successCount);
    }

    void OnLocalSnapshotCreated(TLocalResult result)
    {
        if (result.ResultCode != EResultCode::OK) {
            LOG_ERROR("Failed to create a local snapshot\n%s", ~result.ResultCode.ToString());
            return;
        }

        Checksums[Owner->CellManager->GetSelfId()] = result.Checksum;
    }

    void OnChangeLogRotated(TPeerId id, TProxy::TRspAdvanceSegment::TPtr response)
    {
        if (!response->IsOK()) {
            LOG_WARNING("Error rotating the changelog at follower %d\n%s",
                id,
                ~response->GetError().ToString());
        }
    }

    void OnSnapshotCreated(TPeerId id, TProxy::TRspAdvanceSegment::TPtr response)
    {
        if (!response->IsOK()) {
            LOG_WARNING("Error creating the snapshot at follower %d\n%s",
                id,
                ~response->GetError().ToString());
            return;
        }

        auto checksum = response->checksum();
        LOG_INFO("Remote snapshot is created at follower %d (Checksum: %" PRIx64 ")",
            id,
            checksum);

        Checksums[id] = checksum;
    }

    TSnapshotBuilderPtr Owner;
    TMetaVersion Version;
    bool CreateSnapshot;

    TParallelAwaiter::TPtr Awaiter;
    yvector< TNullable<TChecksum> > Checksums;
};

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilder::TSnapshotBuilder(
    TSnapshotBuilderConfig *config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    TChangeLogCachePtr changeLogCache,
    TSnapshotStorePtr snapshotStore,
    TEpoch epoch,
    IInvoker::TPtr epochControlInvoker,
    IInvoker::TPtr epochStateInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedState(decoratedState)
    , SnapshotStore(snapshotStore)
    , ChangeLogCache(changeLogCache)
    , Epoch(epoch)
    , EpochControlInvoker(epochControlInvoker)
    , EpochStateInvoker(epochStateInvoker)
    , LocalResult(MakeFuture(TLocalResult()))
#if defined(_unix_)
    , WatchdogQueue(New<TActionQueue>("SnapshotWDog"))
#endif
{
    YASSERT(cellManager);
    YASSERT(decoratedState);
    YASSERT(changeLogCache);
    YASSERT(snapshotStore);
    YASSERT(epochControlInvoker);
    YASSERT(epochStateInvoker);

    VERIFY_INVOKER_AFFINITY(EpochControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(EpochStateInvoker, StateThread);
}

void TSnapshotBuilder::CreateDistributedSnapshot()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto version = DecoratedState->GetVersion();
    New<TSession>(MakeStrong(this), version, true)->Run();
}

void TSnapshotBuilder::RotateChangeLog()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto version = DecoratedState->GetVersion();
    New<TSession>(MakeStrong(this), version, false)->Run();
}

TSnapshotBuilder::TAsyncLocalResult::TPtr TSnapshotBuilder::CreateLocalSnapshot(const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (IsInProgress()) {
        LOG_ERROR("Unable to create a local snapshot at version %s, snapshot creation is already in progress",
            ~version.ToString());
        return MakeFuture(TLocalResult(EResultCode::AlreadyInProgress));
    }
    LocalResult = New<TAsyncLocalResult>();

    LOG_INFO("Creating a local snapshot at version %s", ~version.ToString());

    if (DecoratedState->GetVersion() != version) {
        LOG_WARNING("Invalid version, snapshot creation canceled: expected %s, received %s",
            ~version.ToString(),
            ~DecoratedState->GetVersion().ToString());
        return MakeFuture(TLocalResult(EResultCode::InvalidVersion));
    }

    i32 snapshotId = version.SegmentId + 1;

#if defined(_unix_)
    LOG_INFO("Going to fork");
    auto forkTimer = Profiler.TimingStart("fork_time");
    pid_t childPid = fork();
    if (childPid == -1) {
        LOG_ERROR("Could not fork while creating local snapshot %d", snapshotId);
        LocalResult->Set(TLocalResult(EResultCode::ForkError));
    } else if (childPid == 0) {
        DoCreateLocalSnapshot(version);
        _exit(0);
    } else {
        Profiler.TimingStop(forkTimer);
        LOG_INFO("Forked successfully, starting watchdog");
        WatchdogQueue->GetInvoker()->Invoke(BIND(
            &TSnapshotBuilder::WatchdogFork,
            MakeWeak(this),
            snapshotId,
            childPid));
    }
#else
    auto checksum = DoCreateLocalSnapshot(version);
    OnLocalSnapshotCreated(snapshotId, checksum);
#endif
        
    DecoratedState->RotateChangeLog();
    return LocalResult;
}

TChecksum TSnapshotBuilder::DoCreateLocalSnapshot(TMetaVersion version)
{
    auto writer = SnapshotStore->GetWriter(version.SegmentId + 1);
    writer->Open(version.RecordCount);
    auto* stream = &writer->GetStream();
    DecoratedState->Save(stream);
    writer->Close();
    return writer->GetChecksum();
}

void TSnapshotBuilder::OnLocalSnapshotCreated(i32 snapshotId, const TChecksum& checksum)
{
    SnapshotStore->OnSnapshotAdded(snapshotId);

    LOG_INFO("Local snapshot %d is created (Checksum: %" PRIx64 ")",
        snapshotId,
        checksum);

    LocalResult->Set(TLocalResult(EResultCode::OK, checksum));
}

#if defined(_unix_)
void TSnapshotBuilder::WatchdogFork(
    TWeakPtr<TSnapshotBuilder> weakSnapshotBuilder,
    i32 snapshotId, 
    pid_t childPid)
{
    TInstant deadline;
    TAsyncLocalResult::TPtr localResult;
    {
        auto snapshotBuilder = weakSnapshotBuilder.Lock();
        if (!snapshotBuilder) {
            LOG_INFO("Snapshot builder has been deleted, exiting watchdog (SnapshotId: %d)", snapshotId);
            return;
        }
        deadline = snapshotBuilder->Config->LocalTimeout.ToDeadLine();
        localResult = snapshotBuilder->LocalResult;
    }

    LOG_DEBUG("Waiting for child process (ChildPid: %d)", childPid);
    int status;
    while (waitpid(childPid, &status, WNOHANG) == 0) {
        if (!weakSnapshotBuilder.IsExpired() && TInstant::Now() <= deadline) {
            sleep(1);
        } else {
            if (!weakSnapshotBuilder.IsExpired()) {
                LOG_INFO("Snapshot builder has been deleted, killing child process (ChildPid: %d, SnapshotId: %d)",
                    childPid,
                    snapshotId);
            } else {
                LOG_ERROR("Local snapshot creating timed out, killing child process (ChildPid: %d, SnapshotId: %d)",
                    childPid,
                    snapshotId);
            }
            auto killResult = kill(childPid, 9);
            if (killResult != 0) {
                LOG_ERROR("Could not kill child process (ChildPid: %d, ErrorCode: %d)",
                    childPid,
                    killResult);
            }
            localResult->Set(TLocalResult(EResultCode::TimeoutExceeded));
            return;
        }
    }

    if (!WIFEXITED(status)) {
        LOG_ERROR("Snapshot child process has terminated abnormally with status %d (SnapshotId: %d)",
            status,
            snapshotId);
        localResult->Set(TLocalResult(EResultCode::ForkError));
        return;
    }

    auto exitStatus = WEXITSTATUS(status);
    LOG_INFO("Snapshot child process terminated with exit status %d (SnapshotId: %d)",
        exitStatus,
        snapshotId);

    auto snapshotBuilder = weakSnapshotBuilder.Lock();
    if (!snapshotBuilder) {
        LOG_INFO("Snapshot builder has been deleted, exiting watchdog (SnapshotId: %d)",
            snapshotId);
        return;
    }

    auto result = snapshotBuilder->SnapshotStore->GetReader(snapshotId);
    if (!result.IsOK()) {
        LOG_ERROR("Cannot open snapshot %d\n%s",
            snapshotId,
            ~result.ToString());
        localResult->Set(TLocalResult(EResultCode::ForkError));
        return;
    }

    auto reader = result.Value();
    reader->Open();
    auto checksum = reader->GetChecksum();
    snapshotBuilder->OnLocalSnapshotCreated(snapshotId, checksum);
}
#endif


void TSnapshotBuilder::WaitUntilFinished()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    LocalResult->Get();
}

bool TSnapshotBuilder::IsInProgress() const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return !LocalResult->IsSet();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
