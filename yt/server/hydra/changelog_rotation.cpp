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
        : Owner_(owner)
        , BuildSnapshot_(buildSnapshot)
        , SnapshotPromise_(NewPromise<TErrorOr<TRemoteSnapshotParams>>())
        , ChangelogPromise_(NewPromise<TError>())
        , Logger(Owner_->Logger)
    { }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        Version_ = Owner_->DecoratedAutomaton_->GetLoggedVersion();
        Owner_->LeaderCommitter_->Flush();
        Owner_->LeaderCommitter_->SuspendLogging();

        LOG_INFO("Starting distributed changelog rotation at version %s", ~ToString(Version_));

        Owner_->LeaderCommitter_->GetQuorumFlushResult()
            .Subscribe(BIND(&TSession::OnQuorumFlushed, MakeStrong(this))
                .Via(Owner_->EpochContext_->EpochSystemAutomatonInvoker));
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
    TChangelogRotationPtr Owner_;
    bool BuildSnapshot_;
    
    bool LocalRotationFlag_ = false;
    int RemoteRotationCount_ = 0;

    TVersion Version_;
    TPromise<TErrorOr<TRemoteSnapshotParams>> SnapshotPromise_;
    TPromise<TError> ChangelogPromise_;
    TParallelAwaiterPtr SnapshotAwaiter_;
    TParallelAwaiterPtr ChangelogAwaiter_;
    std::vector<TNullable<TChecksum>> SnapshotChecksums_;

    NLog::TTaggedLogger& Logger;


    void OnQuorumFlushed(TError error)
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);
        YCHECK(Owner_->DecoratedAutomaton_->GetLoggedVersion() == Version_);

        if (!error.IsOK())
            return;

        if (BuildSnapshot_) {
            RequestSnapshotCreation();
        }

        Owner_->DecoratedAutomaton_->CommitMutations(TVersion(Version_.SegmentId + 1, 0));

        RequestChangelogRotation();
    }


    void RequestSnapshotCreation()
    {
        LOG_INFO("Sending snapshot creation requests");

        SnapshotChecksums_.resize(Owner_->CellManager_->GetPeerCount());

        auto awaiter = SnapshotAwaiter_ = New<TParallelAwaiter>(Owner_->EpochContext_->EpochControlInvoker);
        auto this_ = MakeStrong(this);

        if (Owner_->Config_->BuildSnapshotsAtFollowers) {
            for (auto peerId = 0; peerId < Owner_->CellManager_->GetPeerCount(); ++peerId) {
                if (peerId == Owner_->CellManager_->GetSelfId())
                    continue;

                auto channel = Owner_->CellManager_->GetPeerChannel(peerId);
                if (!channel)
                    continue;

                LOG_DEBUG("Requesting follower %d to build a snapshot", peerId);

                THydraServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(Owner_->Config_->SnapshotTimeout);

                auto req = proxy.BuildSnapshot();
                ToProto(req->mutable_epoch_id(), Owner_->EpochContext_->EpochId);
                req->set_revision(Version_.ToRevision());

                awaiter->Await(
                    req->Invoke(),
                    BIND(&TSession::OnRemoteSnapshotBuilt, this_, peerId));
            }
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
            LOG_WARNING(*rsp, "Error building snapshot at follower %d",
                id);
            return;
        }

        LOG_INFO("Remote snapshot built by follower %d",
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
        SnapshotChecksums_[Owner_->CellManager_->GetSelfId()] = params.Checksum;
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
                        "Snapshot %d checksum mismatch: "
                        "peer %d reported %" PRIx64 ", "
                        "peer %d reported %" PRIx64,
                        Version_.SegmentId + 1,
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
        auto awaiter = ChangelogAwaiter_ = New<TParallelAwaiter>(Owner_->EpochContext_->EpochControlInvoker);
        auto this_ = MakeStrong(this);

        for (auto peerId = 0; peerId < Owner_->CellManager_->GetPeerCount(); ++peerId) {
            if (peerId == Owner_->CellManager_->GetSelfId())
                continue;

            auto channel = Owner_->CellManager_->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_DEBUG("Requesting follower %d to rotate the changelog", peerId);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Owner_->Config_->RpcTimeout);

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
            BIND(&TSession::OnChangelogsComplete, this_));
    }

    void OnRemoteChangelogRotated(TPeerId id, THydraServiceProxy::TRspRotateChangelogPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!rsp->IsOK()) {
            LOG_WARNING(*rsp, "Error rotating changelog at follower %d",
                id);
            return;
        }

        LOG_INFO("Remote changelog rotated by follower %d",
            id);

        ++RemoteRotationCount_;
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

        YCHECK(!LocalRotationFlag_);
        LocalRotationFlag_ = true;
        CheckRotationQuorum();
    }

    void CheckRotationQuorum()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        // NB: It is vital to wait for the local rotation to complete.
        // Otherwise we risk assigning out-of-order versions.
        if (!LocalRotationFlag_ || RemoteRotationCount_ < Owner_->CellManager_->GetQuorumCount() - 1)
            return;

        Owner_->EpochContext_->EpochSystemAutomatonInvoker->Invoke(
            BIND(&TLeaderCommitter::ResumeLogging, Owner_->LeaderCommitter_));

        SetSucceded();
    }

    void OnChangelogsComplete()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        SetFailed(TError("Not enough successful replies"));
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
    }

};

////////////////////////////////////////////////////////////////////////////////

TChangelogRotation::TChangelogRotation(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TLeaderCommitterPtr leaderCommitter,
    ISnapshotStorePtr snapshotStore,
    TEpochContextPtr epochContext)
    : Config_(config)
    , CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , LeaderCommitter_(leaderCommitter)
    , SnapshotStore_(snapshotStore)
    , EpochContext_(epochContext)
    , SnapshotsInProgress_(0)
    , Logger(HydraLogger)
{
    YCHECK(Config_);
    YCHECK(CellManager_);
    YCHECK(DecoratedAutomaton_);
    YCHECK(LeaderCommitter_);
    YCHECK(SnapshotStore_);
    YCHECK(EpochContext_);
    VERIFY_INVOKER_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(EpochContext_->EpochSystemAutomatonInvoker, AutomatonThread);

    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(CellManager_->GetCellGuid())));
}

TFuture<TError> TChangelogRotation::RotateChangelog()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto session = New<TSession>(this, false);
    session->Run();
    return session->GetChangelogResult();
}

TFuture<TErrorOr<TRemoteSnapshotParams>> TChangelogRotation::BuildSnapshot()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto session = New<TSession>(this, true);
    session->Run();
    auto result = session->GetSnapshotResult();

    ++SnapshotsInProgress_;
    auto this_ = MakeStrong(this);
    result.Finally().Subscribe(BIND([this, this_] () {
        --SnapshotsInProgress_;
    }));

    return result;
}

bool TChangelogRotation::IsSnapshotInProgress() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SnapshotsInProgress_.load() > 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
