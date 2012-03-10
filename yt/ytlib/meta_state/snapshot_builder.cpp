#include "stdafx.h"
#include "snapshot_builder.h"
#include "meta_state_manager_proxy.h"
#include "meta_version.h"
#include "decorated_meta_state.h"
#include "snapshot_store.h"
#include "snapshot.h"
#include "change_log_cache.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/actions/action_util.h>
#include <ytlib/actions/action_queue.h>
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

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MetaState");
static NProfiling::TProfiler Profiler("meta_state");

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder::TSession
    : public TRefCounted
{
public:
    TSession(
        TSnapshotBuilderPtr owner,
        TMetaVersion version)
        : Owner(owner)
        , Version(version)
        , Awaiter(New<TParallelAwaiter>(
			~Owner->ServiceInvoker,
			&Profiler,
			"snapshot_build_time"))
        , Checksums(Owner->CellManager->GetPeerCount())
    { }

    void Run()
    {
        LOG_INFO("Creating a distributed snapshot at version: %s",
            ~Version.ToString());

        auto& config = Owner->Config;
		auto cellManager = Owner->CellManager;
        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == Owner->CellManager->GetSelfId()) continue;

            LOG_DEBUG("Requesting follower %d to create a snapshot", id);

            auto proxy = Owner->CellManager->GetMasterProxy<TProxy>(id);
            auto request = proxy->AdvanceSegment()->SetTimeout(config->RemoteTimeout);
            request->set_segment_id(Version.SegmentId);
            request->set_record_count(Version.RecordCount);
            request->set_epoch(Owner->Epoch.ToProto());
            request->set_create_snapshot(true);

            Awaiter->Await(
                request->Invoke(),
				cellManager->GetPeerAddress(id),
                FromMethod(&TSession::OnRemote, MakeStrong(this), id));
        }
        
        Awaiter->Await(
            Owner->CreateLocal(Version),
            FromMethod(&TSession::OnLocal, MakeStrong(this)));

        Awaiter->Complete(FromMethod(&TSession::OnComplete, MakeStrong(this)));
    }

private:
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

        LOG_INFO("Distributed snapshot is created (SuccessCount: %d)", successCount);
    }

    void OnLocal(TLocalResult result)
    {
        if (result.ResultCode != EResultCode::OK) {
            LOG_ERROR("Failed to create a local snapshot\n%s", ~result.ResultCode.ToString());
            return;
        }

        Checksums[Owner->CellManager->GetSelfId()] = MakeNullable(result.Checksum);
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

    TSnapshotBuilderPtr Owner;
    TMetaVersion Version;
    TParallelAwaiter::TPtr Awaiter;
    yvector< TNullable<TChecksum> > Checksums;
};

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilder::TSnapshotBuilder(
    TConfig* config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr metaState,
    TChangeLogCachePtr changeLogCache,
    TSnapshotStorePtr snapshotStore,
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
#if defined(_unix_)
    , WatchdogQueue(New<TActionQueue>("SnapshotWDog"))
#endif
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
    New<TSession>(MakeStrong(this), version)->Run();
    return EResultCode::OK;
}

TSnapshotBuilder::TAsyncLocalResult::TPtr TSnapshotBuilder::CreateLocal(
    TMetaVersion version)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (IsInProgress()) {
        LOG_ERROR("Unable to create a local snapshot at version %s, snapshot creation is already in progress",
            ~version.ToString());
        return MakeFuture(TLocalResult(EResultCode::AlreadyInProgress));
    }
    LocalResult = New<TAsyncLocalResult>();

    LOG_INFO("Creating a local snapshot at version: %s", ~version.ToString());

    if (MetaState->GetVersion() != version) {
        LOG_WARNING("Invalid version, snapshot creation canceled: expected %s, received %s",
            ~version.ToString(),
            ~MetaState->GetVersion().ToString());
        return MakeFuture(TLocalResult(EResultCode::InvalidVersion));
    }

    i32 segmentId = version.SegmentId + 1;

#if defined(_unix_)
    LOG_INFO("Going to fork");
    auto forkTimer = Profiler.TimingStart("fork_time");
    pid_t childPid = fork();
    if (childPid == -1) {
        LOG_ERROR("Could not fork while creating local snapshot %d", segmentId);
        LocalResult->Set(TLocalResult(EResultCode::ForkError));
    } else if (childPid == 0) {
        DoCreateLocal(version);
        _exit(0);
    } else {
        Profiler.TimingStop("fork_time");
        LOG_INFO("Forked successfully, starting watchdog");
        WatchdogQueue->GetInvoker()->Invoke(FromMethod(
            &TSnapshotBuilder::WatchdogFork,
            TWeakPtr<TSnapshotBuilder>(this),
            segmentId,
            childPid));
    }
#else
    auto checksum = DoCreateLocal(version);
    OnLocalCreated(segmentId, checksum);
#endif
        
    MetaState->RotateChangeLog();
    return LocalResult;
}

TChecksum TSnapshotBuilder::DoCreateLocal(TMetaVersion version)
{
    auto writer = SnapshotStore->GetWriter(version.SegmentId + 1);
    writer->Open(version.RecordCount);
    auto* stream = &writer->GetStream();
    MetaState->Save(stream);
    writer->Close();
    return writer->GetChecksum();
}

void TSnapshotBuilder::OnLocalCreated(i32 segmentId, const TChecksum& checksum)
{
    SnapshotStore->UpdateMaxSnapshotId(segmentId);

    LOG_INFO("Local snapshot %d is created (Checksum: %" PRIx64 ")",
        segmentId,
        checksum);

    LocalResult->Set(TLocalResult(EResultCode::OK, checksum));
}

#if defined(_unix_)
void TSnapshotBuilder::WatchdogFork(
    TWeakPtr<TSnapshotBuilder> weakSnapshotBuilder,
    i32 segmentId, 
    pid_t childPid)
{
    TInstant deadline;
    TAsyncLocalResult::TPtr localResult;
    {
        auto snapshotBuilder = weakSnapshotBuilder.Lock();
        if (!snapshotBuilder) {
            LOG_INFO("Snapshot builder has been deleted, exiting watchdog (SegmentId: %d)",
                segmentId);
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
                LOG_INFO("Snapshot builder has been deleted, killing child process (ChildPid: %d, SegmentId: %d)",
                    childPid,
                    segmentId);
            } else {
                LOG_ERROR("Local snapshot creating timed out, killing child process (ChildPid: %d, SegmentId: %d)",
                    childPid,
                    segmentId);
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
        LOG_ERROR("Snapshot child process has terminated abnormally with status %d (SegmentId: %d)",
            status,
            segmentId);
        localResult->Set(TLocalResult(EResultCode::ForkError));
        return;
    }

    auto exitStatus = WEXITSTATUS(status);
    LOG_INFO("Snapshot child process terminated with exit status %d (SegmentId: %d)",
        exitStatus,
        segmentId);

    auto snapshotBuilder = weakSnapshotBuilder.Lock();
    if (!snapshotBuilder) {
        LOG_INFO("Snapshot builder has been deleted, exiting watchdog (SegmentId: %d)",
            segmentId);
        return;
    }

    auto result = snapshotBuilder->SnapshotStore->GetReader(segmentId);
    if (!result.IsOK()) {
        LOG_ERROR("Cannot open snapshot %d\n%s",
            segmentId,
            ~result.ToString());
        localResult->Set(TLocalResult(EResultCode::ForkError));
        return;
    }

    auto reader = result.Value();
    reader->Open();
    auto checksum = reader->GetChecksum();
    snapshotBuilder->OnLocalCreated(segmentId, checksum);
}
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
