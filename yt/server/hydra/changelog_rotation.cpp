#include "stdafx.h"
#include "changelog_rotation.h"
#include "config.h"
#include "decorated_automaton.h"
#include "mutation_committer.h"
#include "snapshot.h"
#include "changelog.h"
#include "snapshot_discovery.h"

#include <core/concurrency/parallel_awaiter.h>

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TChangelogRotation::TSession
    : public TRefCounted
{
public:
    TSession(
        TChangelogRotationPtr owner,
        bool buildSnapshot)
        : Owner(owner)
        , BuildSnapshot(buildSnapshot)
        , LocalRotationFlag(false)
        , RemoteRotationCount(0)
        , SnapshotPromise(NewPromise<TErrorOr<TSnapshotInfo>>())
        , ChangelogPromise(NewPromise<TError>())
        , Logger(Owner->Logger)
    { }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner->AutomatonThread);

        Version = Owner->DecoratedAutomaton->GetLoggedVersion();
        Owner->LeaderCommitter->Flush();
        Owner->LeaderCommitter->SuspendLogging();

        LOG_INFO("Starting distributed changelog rotation at version %s", ~ToString(Version));

        Owner->LeaderCommitter->GetQuorumFlushResult()
            .Subscribe(BIND(&TSession::OnQuorumFlushed, MakeStrong(this))
                .Via(Owner->EpochAutomatonInvoker));
    }

    TFuture<TErrorOr<TSnapshotInfo>> GetSnapshotResult()
    {
        return SnapshotPromise;
    }

    TFuture<TError> GetChangelogResult()
    {
        return ChangelogPromise;
    }

private:
    TChangelogRotationPtr Owner;
    bool BuildSnapshot;
    
    bool LocalRotationFlag;
    int RemoteRotationCount;

    TVersion Version;
    TPromise<TErrorOr<TSnapshotInfo>> SnapshotPromise;
    TPromise<TError> ChangelogPromise;
    TParallelAwaiterPtr SnapshotAwaiter;
    TParallelAwaiterPtr ChangelogAwaiter;
    std::vector<TNullable<TChecksum>> SnapshotChecksums;

    NLog::TTaggedLogger& Logger;


    void OnQuorumFlushed()
    {
        VERIFY_THREAD_AFFINITY(Owner->AutomatonThread);
        YCHECK(Owner->DecoratedAutomaton->GetLoggedVersion() == Version);

        if (BuildSnapshot) {
            RequestSnapshotCreation();
        }

        RequestChangelogRotation();

        Owner->DecoratedAutomaton->CommitMutations(TVersion(Version.SegmentId + 1, 0));
    }


    void RequestSnapshotCreation()
    {
        LOG_INFO("Sending snapshot creation requests");

        SnapshotChecksums.resize(Owner->CellManager->GetPeerCount());

        auto awaiter = SnapshotAwaiter = New<TParallelAwaiter>(Owner->EpochControlInvoker);
        auto this_ = MakeStrong(this);

        for (auto peerId = 0; peerId < Owner->CellManager->GetPeerCount(); ++peerId) {
            if (peerId == Owner->CellManager->GetSelfId())
                continue;

            auto channel = Owner->CellManager->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_DEBUG("Requesting follower %d to build a snapshot", peerId);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Owner->Config->SnapshotTimeout);

            auto req = proxy.BuildSnapshotLocal();
            ToProto(req->mutable_epoch_id(), Owner->EpochId);
            req->set_revision(Version.ToRevision());

            awaiter->Await(
                req->Invoke(),
                BIND(&TSession::OnRemoteSnapshotBuilt, this_, peerId));
        }

        awaiter->Await(
            Owner->DecoratedAutomaton->BuildSnapshot(),
            BIND(&TSession::OnLocalSnapshotBuilt, this_));

        awaiter->Complete(
            BIND(&TSession::OnSnapshotsComplete, this_));
    }

    void OnRemoteSnapshotBuilt(TPeerId id, THydraServiceProxy::TRspBuildSnapshotLocalPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (!rsp->IsOK()) {
            LOG_WARNING(*rsp, "Error building snapshot at follower %d",
                id);
            return;
        }

        LOG_INFO("Remote snapshot built by follower %d",
            id);

        SnapshotChecksums[id] = rsp->checksum();
    }

    void OnLocalSnapshotBuilt(TErrorOr<TSnapshotInfo> infoOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        SnapshotPromise.Set(infoOrError);

        if (!infoOrError.IsOK()) {
            LOG_WARNING(infoOrError, "Error building local snapshot");
            return;
        }

        LOG_INFO("Local snapshot built");

        const auto& info = infoOrError.GetValue();
        SnapshotChecksums[Owner->CellManager->GetSelfId()] = info.Checksum;
    }

    void OnSnapshotsComplete()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        int successCount = 0;
        for (TPeerId id1 = 0; id1 < SnapshotChecksums.size(); ++id1) {
            auto& checksum1 = SnapshotChecksums[id1];
            if (checksum1) {
                ++successCount;
            }
            for (TPeerId id2 = id1 + 1; id2 < SnapshotChecksums.size(); ++id2) {
                const auto& checksum2 = SnapshotChecksums[id2];
                if (checksum1 && checksum2 && checksum1 != checksum2) {
                    // TODO(babenko): consider killing followers
                    LOG_FATAL(
                        "Snapshot %d checksum mismatch: "
                        "peer %d reported %" PRIx64 ", "
                        "peer %d reported %" PRIx64,
                        Version.SegmentId + 1,
                        id1, *checksum1,
                        id2, *checksum2);
                }
            }
        }
        
        LOG_INFO("Distributed snapshot creation finished, %d peers succeeded",
            successCount);
    }


    void RequestChangelogRotation()
    {
        LOG_INFO("Sending changelog rotation requests");

        auto awaiter = ChangelogAwaiter = New<TParallelAwaiter>(Owner->EpochControlInvoker);
        auto this_ = MakeStrong(this);

        for (auto peerId = 0; peerId < Owner->CellManager->GetPeerCount(); ++peerId) {
            if (peerId == Owner->CellManager->GetSelfId())
                continue;

            auto channel = Owner->CellManager->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_DEBUG("Requesting follower %d to rotate the changelog", peerId);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Owner->Config->RpcTimeout);

            auto req = proxy.RotateChangelog();
            ToProto(req->mutable_epoch_id(), Owner->EpochId);
            req->set_revision(Version.ToRevision());

            awaiter->Await(
                req->Invoke(),
                BIND(&TSession::OnRemoteChangelogRotated, this_, peerId));
        }

        awaiter->Await(
            Owner->DecoratedAutomaton->RotateChangelog(),
            BIND(&TSession::OnLocalChangelogRotated, this_));

        awaiter->Complete(
            BIND(&TSession::OnChangelogsComplete, this_));
    }

    void OnRemoteChangelogRotated(TPeerId id, THydraServiceProxy::TRspRotateChangelogPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (!rsp->IsOK()) {
            LOG_WARNING(*rsp, "Error rotating changelog at follower %d",
                id);
            return;
        }

        LOG_INFO("Remote changelog rotated by follower %d",
            id);

        ++RemoteRotationCount;
        CheckRotationQuorum();
    }

    void OnLocalChangelogRotated()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        LOG_INFO("Local changelog rotated");

        YCHECK(!LocalRotationFlag);
        LocalRotationFlag = true;
        CheckRotationQuorum();
    }

    void CheckRotationQuorum()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        // NB: It is vital to wait for the local rotation to complete.
        // Otherwise we risk assigning out-of-order versions.
        if (!LocalRotationFlag || RemoteRotationCount < Owner->CellManager->GetQuorumCount() - 1)
            return;

        LOG_INFO("Distributed changelog rotation complete");

        ChangelogAwaiter->Cancel();

        Owner->EpochAutomatonInvoker->Invoke(
            BIND(&TLeaderCommitter::ResumeLogging, Owner->LeaderCommitter));

        ChangelogPromise.Set(TError());
    }

    void OnChangelogsComplete()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        TError error("Distributed changelog rotation failed");
        LOG_WARNING(error);
        ChangelogPromise.Set(error);
    }

};

////////////////////////////////////////////////////////////////////////////////

TChangelogRotation::TChangelogRotation(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TLeaderCommitterPtr leaderCommitter,
    ISnapshotStorePtr snapshotStore,
    const TEpochId& epochId,
    IInvokerPtr epochControlInvoker,
    IInvokerPtr epochAutomatonInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedAutomaton(decoratedAutomaton)
    , LeaderCommitter(leaderCommitter)
    , SnapshotStore(snapshotStore)
    , EpochId(epochId)
    , EpochControlInvoker(epochControlInvoker)
    , EpochAutomatonInvoker(epochAutomatonInvoker)
    , Logger(HydraLogger)
{
    YCHECK(Config);
    YCHECK(CellManager);
    YCHECK(DecoratedAutomaton);
    YCHECK(LeaderCommitter);
    YCHECK(SnapshotStore);
    YCHECK(EpochControlInvoker);
    YCHECK(EpochAutomatonInvoker);
    VERIFY_INVOKER_AFFINITY(EpochControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(EpochAutomatonInvoker, AutomatonThread);

    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(CellManager->GetCellGuid())));
}

TFuture<TError> TChangelogRotation::RotateChangelog()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto session = New<TSession>(this, false);
    session->Run();
    return session->GetChangelogResult();
}

TFuture<TErrorOr<TSnapshotInfo>> TChangelogRotation::BuildSnapshot()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto session = New<TSession>(this, true);
    session->Run();
    return session->GetSnapshotResult();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
