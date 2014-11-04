#include "stdafx.h"
#include "checkpointer.h"
#include "config.h"
#include "decorated_automaton.h"
#include "mutation_committer.h"
#include "snapshot.h"
#include "changelog.h"
#include "snapshot_discovery.h"

#include <core/concurrency/parallel_awaiter.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/version.h>
#include <ytlib/hydra/hydra_service_proxy.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TCheckpointer::TSession
    : public TRefCounted
{
public:
    TSession(
        TCheckpointerPtr owner,
        bool buildSnapshot)
        : Owner_(owner)
        , BuildSnapshot_(buildSnapshot)
        , Logger(Owner_->Logger)
    { }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        Owner_->RotatingChangelogs_ = true;
        Owner_->BuildingSnapshot_ = BuildSnapshot_;

        Version_ = Owner_->DecoratedAutomaton_->GetLoggedVersion();
        Owner_->LeaderCommitter_->Flush();
        Owner_->LeaderCommitter_->SuspendLogging();

        LOG_INFO("Starting distributed changelog rotation (Version: %v)",
            Version_);

        Owner_->LeaderCommitter_->GetQuorumFlushResult()
            .Subscribe(BIND(&TSession::OnQuorumFlushed, MakeStrong(this))
                .Via(Owner_->EpochContext_->EpochUserAutomatonInvoker));
    }

    TFuture<TErrorOr<TRemoteSnapshotParams>> GetSnapshotResult()
    {
        return SnapshotPromise_;
    }

    TFuture<TError> GetChangelogResult()
    {
        return ChangelogPromise_;
    }

private:
    TCheckpointerPtr Owner_;
    bool BuildSnapshot_;
    
    bool LocalRotationSuccessFlag = false;
    int RemoteRotationSuccessCount_ = 0;

    TVersion Version_;
    TPromise<TErrorOr<TRemoteSnapshotParams>> SnapshotPromise_ = NewPromise<TErrorOr<TRemoteSnapshotParams>>();
    TPromise<TError> ChangelogPromise_ = NewPromise<TError>();
    TParallelAwaiterPtr SnapshotAwaiter_;
    TParallelAwaiterPtr ChangelogAwaiter_;
    std::vector<TNullable<TChecksum>> SnapshotChecksums_;

    NLog::TLogger& Logger;


    void OnQuorumFlushed(TError error)
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);
        YCHECK(Owner_->DecoratedAutomaton_->GetLoggedVersion() == Version_);

        if (!error.IsOK())
            return;

        if (BuildSnapshot_) {
            RequestSnapshotCreation();
        }

        RequestChangelogRotation();
    }


    void RequestSnapshotCreation()
    {
        LOG_INFO("Sending snapshot creation requests");

        SnapshotChecksums_.resize(Owner_->CellManager_->GetPeerCount());

        auto awaiter = SnapshotAwaiter_ = New<TParallelAwaiter>(Owner_->EpochContext_->EpochControlInvoker);
        auto this_ = MakeStrong(this);

        for (auto peerId = 0; peerId < Owner_->CellManager_->GetPeerCount(); ++peerId) {
            if (peerId == Owner_->CellManager_->GetSelfPeerId())
                continue;

            auto channel = Owner_->CellManager_->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_DEBUG("Requesting follower %v to build a snapshot", peerId);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Owner_->Config_->SnapshotBuildTimeout);

            auto req = proxy.BuildSnapshot();
            ToProto(req->mutable_epoch_id(), Owner_->EpochContext_->EpochId);
            req->set_revision(Version_.ToRevision());

            awaiter->Await(
                req->Invoke(),
                BIND(&TSession::OnRemoteSnapshotBuilt, this_, peerId));
        }

        awaiter->Await(
            Owner_->DecoratedAutomaton_->BuildSnapshot(),
            BIND(&TSession::OnLocalSnapshotBuilt, this_));

        awaiter->Complete(
            BIND(&TSession::OnSnapshotsComplete, this_));
    }

    void OnRemoteSnapshotBuilt(TPeerId id, THydraServiceProxy::TRspBuildSnapshotPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!rsp->IsOK()) {
            LOG_WARNING(*rsp, "Error building snapshot at follower %v",
                id);
            return;
        }

        LOG_INFO("Remote snapshot built by follower %v",
            id);

        SnapshotChecksums_[id] = rsp->checksum();
    }

    void OnLocalSnapshotBuilt(TErrorOr<TRemoteSnapshotParams> paramsOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        SnapshotPromise_.Set(paramsOrError);

        if (!paramsOrError.IsOK()) {
            LOG_WARNING(paramsOrError, "Error building local snapshot");
            return;
        }

        LOG_INFO("Local snapshot built");

        const auto& params = paramsOrError.Value();
        SnapshotChecksums_[Owner_->CellManager_->GetSelfPeerId()] = params.Checksum;
    }

    void OnSnapshotsComplete()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        int successCount = 0;
        for (TPeerId id1 = 0; id1 < SnapshotChecksums_.size(); ++id1) {
            auto& checksum1 = SnapshotChecksums_[id1];
            if (checksum1) {
                ++successCount;
            }
            for (TPeerId id2 = id1 + 1; id2 < SnapshotChecksums_.size(); ++id2) {
                const auto& checksum2 = SnapshotChecksums_[id2];
                if (checksum1 && checksum2 && checksum1 != checksum2) {
                    // TODO(babenko): consider killing followers
                    LOG_FATAL(
                        "Snapshot %v checksum mismatch: "
                        "peer %v reported %v, "
                        "peer %v reported %v",
                        Version_.SegmentId + 1,
                        id1, *checksum1,
                        id2, *checksum2);
                }
            }
        }
        
        LOG_INFO("Distributed snapshot creation finished, %v peers succeeded",
            successCount);

        auto owner = Owner_;
        Owner_->EpochContext_->EpochUserAutomatonInvoker->Invoke(BIND([owner] () {
            owner->BuildingSnapshot_ = false;
        }));
    }


    void RequestChangelogRotation()
    {
        auto awaiter = ChangelogAwaiter_ = New<TParallelAwaiter>(Owner_->EpochContext_->EpochControlInvoker);
        auto this_ = MakeStrong(this);

        for (auto peerId = 0; peerId < Owner_->CellManager_->GetPeerCount(); ++peerId) {
            if (peerId == Owner_->CellManager_->GetSelfPeerId())
                continue;

            auto channel = Owner_->CellManager_->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_DEBUG("Requesting follower %v to rotate the changelog", peerId);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Owner_->Config_->ControlRpcTimeout);

            auto req = proxy.RotateChangelog();
            ToProto(req->mutable_epoch_id(), Owner_->EpochContext_->EpochId);
            req->set_revision(Version_.ToRevision());

            awaiter->Await(
                req->Invoke(),
                BIND(&TSession::OnRemoteChangelogRotated, this_, peerId));
        }

        awaiter->Await(
            Owner_->DecoratedAutomaton_->RotateChangelog(Owner_->EpochContext_),
            BIND(&TSession::OnLocalChangelogRotated, this_));

        awaiter->Complete(
            BIND(&TSession::OnRotationFailed, this_));
    }

    void OnRemoteChangelogRotated(TPeerId id, THydraServiceProxy::TRspRotateChangelogPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!rsp->IsOK()) {
            LOG_WARNING(*rsp, "Error rotating changelog at follower %v",
                id);
            return;
        }

        LOG_INFO("Remote changelog rotated by follower %v",
            id);

        ++RemoteRotationSuccessCount_;
        CheckRotationQuorum();
    }

    void OnLocalChangelogRotated(TError error)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!error.IsOK()) {
            SetFailed(TError("Error rotating local changelog") << error);
            return;
        }

        LOG_INFO("Local changelog rotated");

        YCHECK(!LocalRotationSuccessFlag);
        LocalRotationSuccessFlag = true;
        CheckRotationQuorum();
    }

    void CheckRotationQuorum()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        // NB: It is vital to wait for the local rotation to complete.
        // Otherwise we risk assigning out-of-order versions.
        if (!LocalRotationSuccessFlag || RemoteRotationSuccessCount_ < Owner_->CellManager_->GetQuorumCount() - 1)
            return;

        Owner_->EpochContext_->EpochUserAutomatonInvoker->Invoke(
            BIND(&TSession::OnRotationSucceded, MakeStrong(this)));

        SetSucceded();
    }

    void OnRotationSucceded()
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        Owner_->RotatingChangelogs_ = false;
        Owner_->DecoratedAutomaton_->RotateAutomatonVersion(Version_.SegmentId + 1);
        Owner_->LeaderCommitter_->ResumeLogging();
    }

    void OnRotationFailed()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        // NB: Otherwise an error is already reported.
        YCHECK(LocalRotationSuccessFlag);
        SetFailed(TError("Not enough successful changelog rotation replies: %v out of %v",
            RemoteRotationSuccessCount_ + 1,
            Owner_->CellManager_->GetPeerCount()));
    }


    void SetSucceded()
    {
        LOG_INFO("Distributed changelog rotation complete");
        ChangelogAwaiter_->Cancel();
        ChangelogPromise_.Set(TError());
    }

    void SetFailed(const TError& error)
    {
        ChangelogAwaiter_->Cancel();
        ChangelogPromise_.Set(error);
        Owner_->LeaderFailed_.Fire();
    }

};

////////////////////////////////////////////////////////////////////////////////

TCheckpointer::TCheckpointer(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TLeaderCommitterPtr leaderCommitter,
    ISnapshotStorePtr snapshotStore,
    TEpochContext* epochContext)
    : Config_(config)
    , CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , LeaderCommitter_(leaderCommitter)
    , SnapshotStore_(snapshotStore)
    , EpochContext_(epochContext)
    , Logger(HydraLogger)
{
    YCHECK(Config_);
    YCHECK(CellManager_);
    YCHECK(DecoratedAutomaton_);
    YCHECK(LeaderCommitter_);
    YCHECK(SnapshotStore_);
    YCHECK(EpochContext_);
    VERIFY_INVOKER_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(EpochContext_->EpochUserAutomatonInvoker, AutomatonThread);

    Logger.AddTag("CellId: %v", CellManager_->GetCellId());
}

TFuture<TError> TCheckpointer::RotateChangelog()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(CanRotateChangelogs());

    auto session = New<TSession>(this, false);
    session->Run();
    return session->GetChangelogResult();
}

TFuture<TErrorOr<TRemoteSnapshotParams>> TCheckpointer::BuildSnapshot()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(CanBuildSnapshot());

    auto session = New<TSession>(this, true);
    session->Run();
    return session->GetSnapshotResult();
}

bool TCheckpointer::CanBuildSnapshot() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return !BuildingSnapshot_ && !RotatingChangelogs_;
}

bool TCheckpointer::CanRotateChangelogs() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return !RotatingChangelogs_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
