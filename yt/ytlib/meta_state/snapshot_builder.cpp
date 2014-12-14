#include "stdafx.h"
#include "snapshot_builder.h"
#include "private.h"
#include "config.h"
#include "meta_state_manager_proxy.h"
#include "meta_version.h"
#include "decorated_meta_state.h"
#include "snapshot_store.h"
#include "snapshot.h"
#include "change_log_cache.h"

#include <core/concurrency/parallel_awaiter.h>

#include <core/ytree/ypath_client.h>

#include <core/misc/proc.h>
#include <core/misc/serialize.h>

#include <ytlib/election/cell_manager.h>

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
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = MetaStateLogger;
static auto& Profiler = MetaStateProfiler;

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilder::TSession
    : public TRefCounted
{
public:
    TSession(
        TSnapshotBuilderPtr owner,
        const TMetaVersion& version,
        const TEpochId& epoch,
        bool createSnapshot)
        : Owner(owner)
        , Version(version)
        , CreateSnapshot(createSnapshot)
        , Promise(NewPromise<TResultOrError>())
    { }

    TFuture<TResultOrError> Run()
    {
        VERIFY_THREAD_AFFINITY(Owner->StateThread);

        if (CreateSnapshot) {
            LOG_INFO("Creating snapshot at version %s", ~Version.ToString());

            Checksums.resize(Owner->CellManager->GetPeerCount());
            Awaiter = New<TParallelAwaiter>(
                Owner->ControlInvoker,
                &Profiler,
                "/snapshot_build_time");
        } else {
            LOG_INFO("Rotating changelog at version %s", ~Version.ToString());

            Awaiter = New<TParallelAwaiter>(Owner->ControlInvoker);
        }

        Owner->ControlInvoker->Invoke(BIND(
            &TSession::DoSendRequests,
            MakeStrong(this)));

        if (CreateSnapshot) {
            Awaiter->Await(
                Owner->BuildSnapshotLocal(Version),
                BIND(&TSession::OnLocalSnapshotCreated, MakeStrong(this)));

            // The awaiter must be completed from the control thread.
            Owner->ControlInvoker->Invoke(BIND(
                &TSession::DoCompleteSession,
                MakeStrong(this)));
        } else {
            Owner->DecoratedState->RotateChangeLog(Owner->EpochId);
            // No need to complete the awaiter.
        }

        return Promise;
    }

private:
    void DoSendRequests()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        for (TPeerId id = 0; id < Owner->CellManager->GetPeerCount(); ++id) {
            if (id == Owner->CellManager->GetSelfId()) continue;

            if (CreateSnapshot) {
                LOG_DEBUG("Requesting follower %d to create snapshot", id);
            } else {
                LOG_DEBUG("Requesting follower %d to rotate changelog", id);
            }

            TProxy proxy(Owner->CellManager->GetMasterChannel(id));
            proxy.SetDefaultTimeout(Owner->Config->RemoteTimeout);

            auto request = proxy.AdvanceSegment();
            request->set_segment_id(Version.SegmentId);
            request->set_record_count(Version.RecordCount);
            ToProto(request->mutable_epoch_id(), Owner->EpochId);
            request->set_create_snapshot(CreateSnapshot);

            auto responseHandler =
                CreateSnapshot
                ? BIND(&TSession::OnSnapshotCreated, MakeStrong(this), id)
                : BIND(&TSession::OnChangeLogRotated, MakeStrong(this), id);

            Awaiter->Await(
                request->Invoke(),
                Owner->CellManager->GetPeerTags(id),
                responseHandler);
        }

        Owner->DecoratedState->SetPingVersion(TMetaVersion(Version.SegmentId + 1, 0));
    }

    void DoCompleteSession()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        Awaiter->Complete(
            BIND(&TSession::OnComplete, MakeStrong(this)),
            Owner->CellManager->GetAllPeersTags());
    }

    void OnComplete()
    {
        int successCount = 0;
        for (TPeerId id1 = 0; id1 < Checksums.size(); ++id1) {
            const auto& checksum1 = Checksums[id1];
            if (checksum1) {
                ++successCount;
            }
            for (TPeerId id2 = id1 + 1; id2 < Checksums.size(); ++id2) {
                const auto& checksum2 = Checksums[id2];
                if (checksum1 && checksum2 && checksum1 != checksum2) {
                    // TODO: consider killing followers
                    LOG_FATAL(
                        "Snapshot checksum mismatch: "
                        "peer %d reported %" PRIx64 ", "
                        "peer %d reported %" PRIx64 "(SnapshotId: %d)",
                        id1, *checksum1,
                        id2, *checksum2,
                        Version.SegmentId + 1);
                }
            }
        }

        LOG_INFO("Distributed snapshot creation finished, %d peers succeeded",
            successCount);
    }

    void OnLocalSnapshotCreated(TErrorOr<TResult> result)
    {
        Promise.Set(result);

        if (!result.IsOK()) {
            LOG_WARNING(result, "Error creating local snapshot");
            return;
        }

        const auto& value = result.Value();
        Checksums[Owner->CellManager->GetSelfId()] = value.Checksum;
    }

    void OnChangeLogRotated(TPeerId id, TProxy::TRspAdvanceSegmentPtr response)
    {
        if (!response->IsOK()) {
            LOG_WARNING(*response, "Error rotating changelog at follower %d",
                id);
            return;
        }

        LOG_INFO("Remote changelog rotated (FollowerId: %d)",
            id);
    }

    void OnSnapshotCreated(TPeerId id, TProxy::TRspAdvanceSegmentPtr response)
    {
        if (!response->IsOK()) {
            LOG_WARNING(*response, "Error creating snapshot at follower %d",
                id);
            return;
        }

        auto checksum = response->checksum();
        Checksums[id] = checksum;

        LOG_INFO("Remote snapshot created (FollowerId: %d, Checksum: %" PRIx64 ")",
            id,
            checksum);
    }

    TSnapshotBuilderPtr Owner;
    TMetaVersion Version;
    TEpochId Epoch;
    bool CreateSnapshot;
    TPromise<TResultOrError> Promise;

    TParallelAwaiterPtr Awaiter;
    std::vector<TNullable<TChecksum>> Checksums;

};

////////////////////////////////////////////////////////////////////////////////

TSnapshotBuilder::TSnapshotBuilder(
    TSnapshotBuilderConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    TSnapshotStorePtr snapshotStore,
    const TEpochId& epochId,
    IInvokerPtr controlInvoker,
    IInvokerPtr stateInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedState(decoratedState)
    , SnapshotStore(snapshotStore)
    , EpochId(epochId)
    , ControlInvoker(controlInvoker)
    , StateInvoker(stateInvoker)
    , Canceled(false)
#if defined(_unix_)
    , WatchdogQueue(New<TActionQueue>("SnapshotWDog"))
#endif
{
    YCHECK(cellManager);
    YCHECK(decoratedState);
    YCHECK(snapshotStore);
    YCHECK(controlInvoker);
    YCHECK(stateInvoker);
    VERIFY_INVOKER_AFFINITY(ControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(StateInvoker, StateThread);
}

TFuture<TSnapshotBuilder::TResultOrError> TSnapshotBuilder::BuildSnapshotDistributed()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto version = DecoratedState->GetVersion();
    return New<TSession>(MakeStrong(this), version, EpochId, true)->Run();
}

void TSnapshotBuilder::RotateChangeLog()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto version = DecoratedState->GetVersion();
    New<TSession>(MakeStrong(this), version, EpochId, false)->Run();
}

TFuture<TSnapshotBuilder::TResultOrError> TSnapshotBuilder::BuildSnapshotLocal(const TMetaVersion& version)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    if (DecoratedState->GetVersion() != version) {
        return MakeFuture(TResultOrError(TError(
            "Invalid version, snapshot creation canceled: expected %s, received %s",
            ~version.ToString(),
            ~DecoratedState->GetVersion().ToString())));
    }

    if (IsInProgress()) {
        DecoratedState->RotateChangeLog(EpochId);
        return MakeFuture(TResultOrError(TError(
            "Unable to create local snapshot at version %s: another snapshot is already in progress",
            ~version.ToString())));
    }

    LocalPromise = NewPromise< TErrorOr<TResult> >();

    LOG_INFO("Creating local snapshot at version %s", ~version.ToString());

    i32 snapshotId = version.SegmentId + 1;

#if defined(_unix_)
    LOG_INFO("Going to fork");
    auto forkTimer = Profiler.TimingStart("/fork_time");
    pid_t childPid = fork();
    if (childPid == -1) {
        LocalPromise.Set(TError(
            "Error creating local snapshot %d: fork failed",
            snapshotId)
            << TError::FromSystem());
    } else if (childPid == 0) {
        CloseAllDescriptors();
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
    auto result = DoCreateLocalSnapshot(version);
    OnLocalSnapshotCreated(snapshotId, result);
#endif

    DecoratedState->RotateChangeLog(EpochId);
    return LocalPromise;
}

TSnapshotBuilder::TResult TSnapshotBuilder::DoCreateLocalSnapshot(const TMetaVersion& version)
{
    int snapshotId = version.SegmentId + 1;

    auto writer = SnapshotStore->GetWriter(snapshotId);
    writer->Open(version.RecordCount, EpochId);
    DecoratedState->Save(writer->GetStream());
    writer->Close();

    TResult result;
    result.SnapshotId = snapshotId;
    result.Checksum = writer->GetChecksum();
    return result;
}

void TSnapshotBuilder::OnLocalSnapshotCreated(i32 snapshotId, const TResult& result)
{
    SnapshotStore->OnSnapshotAdded(snapshotId);

    LOG_INFO("Local snapshot created (Checksum: %" PRIx64 ")",
        result.Checksum);

    LocalPromise.Set(result);
}

#if defined(_unix_)

void TSnapshotBuilder::WatchdogFork(
    TWeakPtr<TSnapshotBuilder> weakSnapshotBuilder,
    i32 snapshotId,
    pid_t childPid)
{
    TInstant deadline;
    TPromise<TResultOrError> localPromise;
    {
        auto snapshotBuilder = weakSnapshotBuilder.Lock();
        if (!snapshotBuilder) {
            LOG_INFO("Snapshot builder has been deleted, exiting watchdog (SnapshotId: %d)", snapshotId);
            return;
        }
        deadline = snapshotBuilder->Config->LocalTimeout.ToDeadLine();
        localPromise = snapshotBuilder->LocalPromise;
    }

    LOG_DEBUG("Waiting for child process %d", childPid);
    int status;
    while (waitpid(childPid, &status, WNOHANG) == 0) {
        auto snapshotBuilder = weakSnapshotBuilder.Lock();
        bool kill = false;
        if (!snapshotBuilder || snapshotBuilder->Canceled) {
            LOG_INFO("Snapshot canceled, killing child process %d",
                childPid);
            localPromise.Set(TError("Snapshot canceled"));
            kill = true;
        } else if (TInstant::Now() > deadline) {
            LOG_ERROR("Local snapshot creating timed out, killing child process %d",
                childPid);
            localPromise.Set(TError("Snapshot timed out"));
            kill = true;
        }
 
        if (kill) {
            auto killResult = ::kill(childPid, 9);
            if (killResult != 0) {
                LOG_ERROR(TError("Could not kill child process %d",
                    childPid)
                    << TError::FromSystem());
            }
            return;
        }
 
        sleep(1);
    }

    if (!WIFEXITED(status)) {
        localPromise.Set(TError(
            "Snapshot child process exited with status %d",
            status));
        return;
    }

    auto exitStatus = WEXITSTATUS(status);
    LOG_INFO("Snapshot child process exited with status %d (SnapshotId: %d)",
        exitStatus,
        snapshotId);

    auto snapshotBuilder = weakSnapshotBuilder.Lock();
    if (!snapshotBuilder)
        return;

    auto readerResult = snapshotBuilder->SnapshotStore->GetReader(snapshotId);
    if (!readerResult.IsOK()) {
        localPromise.Set(TError(readerResult));
        return;
    }

    auto reader = readerResult.Value();
    try {
        reader->Open();
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Error extracting snapshot checksum");
    }

    TResult builderResult;
    builderResult.SnapshotId = snapshotId;
    builderResult.Checksum = reader->GetChecksum();

    snapshotBuilder->OnLocalSnapshotCreated(snapshotId, builderResult);
}

#endif

void TSnapshotBuilder::WaitUntilFinished()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Canceled = true;
    if (LocalPromise) {
        LocalPromise.Get();
    }
}

bool TSnapshotBuilder::IsInProgress() const
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return LocalPromise && !LocalPromise.IsSet();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
