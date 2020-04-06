#include "checkpointer.h"
#include "changelog.h"
#include "config.h"
#include "decorated_automaton.h"
#include "mutation_committer.h"
#include "snapshot.h"
#include "snapshot_discovery.h"

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/hydra/hydra_service_proxy.h>
#include <yt/client/hydra/version.h>

namespace NYT::NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TCheckpointer::TSession
    : public TRefCounted
{
public:
    TSession(
        TCheckpointer* owner,
        bool buildSnapshot,
        bool setReadOnly,
        TDuration buildSnapshotDelay = TDuration::Zero())
        : Owner_(owner)
        , BuildSnapshot_(buildSnapshot)
        , SetReadOnly_(setReadOnly)
        , Logger(Owner_->Logger)
    { }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        Owner_->RotatingChangelogs_ = true;
        if (BuildSnapshot_) {
            Owner_->BuildingSnapshot_ = true;
        }

        Version_ = Owner_->DecoratedAutomaton_->GetLoggedVersion();
        Owner_->EpochContext_->LeaderCommitter->Flush();
        Owner_->EpochContext_->LeaderCommitter->SuspendLogging();

        YT_LOG_INFO("Starting distributed changelog rotation (Version: %v)",
            Version_);

        Owner_->EpochContext_->LeaderCommitter->GetQuorumFlushResult()
            .Subscribe(BIND(&TSession::OnQuorumFlushed, MakeStrong(this))
                .Via(Owner_->EpochContext_->EpochUserAutomatonInvoker));
    }

    int GetSnapshotId() const
    {
        // See decorated_automaton.cpp, TDecoratedAutomaton::TSnapshotBuilderBase::TSnapshotBuilderBase.
        return Version_.SegmentId + 1;
    }

    TFuture<TRemoteSnapshotParams> GetSnapshotResult()
    {
        return SnapshotPromise_;
    }

    TFuture<void> GetChangelogResult()
    {
        return ChangelogPromise_;
    }

private:
    // NB: TSession cannot outlive its owner.
    TCheckpointer* const Owner_;
    const bool BuildSnapshot_;
    const bool SetReadOnly_;
    const NLogging::TLogger Logger;

    bool LocalRotationSuccessFlag_ = false;
    int RemoteRotationSuccessCount_ = 0;

    TVersion Version_;
    TPromise<TRemoteSnapshotParams> SnapshotPromise_ = NewPromise<TRemoteSnapshotParams>();
    TPromise<void> ChangelogPromise_ = NewPromise<void>();
    std::vector<std::optional<TChecksum>> SnapshotChecksums_;

    void OnQuorumFlushed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);
        YT_VERIFY(Owner_->DecoratedAutomaton_->GetLoggedVersion() == Version_);

        if (!error.IsOK())
            return;

        if (BuildSnapshot_) {
            RequestSnapshotCreation();
        }

        RequestChangelogRotation();
    }


    void RequestSnapshotCreation()
    {
        YT_LOG_INFO("Sending snapshot creation requests (SetReadOnly: %v)",
            SetReadOnly_);

        SnapshotChecksums_.resize(Owner_->CellManager_->GetTotalPeerCount());

        std::vector<TFuture<void>> asyncResults;
        if (Owner_->Options_.WriteSnapshotsAtFollowers) {
            for (auto peerId = 0; peerId < Owner_->CellManager_->GetTotalPeerCount(); ++peerId) {
                if (peerId == Owner_->CellManager_->GetSelfPeerId())
                    continue;

                auto channel = Owner_->CellManager_->GetPeerChannel(peerId);
                if (!channel)
                    continue;

                YT_LOG_DEBUG("Requesting follower to build a snapshot (PeerId: %v)",
                    peerId);

                THydraServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(Owner_->Config_->SnapshotBuildTimeout);

                auto req = proxy.BuildSnapshot();
                ToProto(req->mutable_epoch_id(), Owner_->EpochContext_->EpochId);
                req->set_revision(Version_.ToRevision());
                req->set_set_read_only(SetReadOnly_);

                asyncResults.push_back(req->Invoke().Apply(
                    BIND(&TSession::OnRemoteSnapshotBuilt, MakeStrong(this), peerId)
                        .AsyncVia(Owner_->EpochContext_->EpochControlInvoker)));
            }
        }
        asyncResults.push_back(
            Owner_->DecoratedAutomaton_->BuildSnapshot().Apply(
                BIND(&TSession::OnLocalSnapshotBuilt, MakeStrong(this))
                    .AsyncVia(Owner_->EpochContext_->EpochControlInvoker)));

        Combine(asyncResults).Subscribe(
            BIND(&TSession::OnSnapshotsComplete, MakeStrong(this))
                .Via(Owner_->EpochContext_->EpochControlInvoker));
    }

    void OnRemoteSnapshotBuilt(TPeerId id, const THydraServiceProxy::TErrorOrRspBuildSnapshotPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error building snapshot at follower (PeerId: %v)",
                id);
            return;
        }

        YT_LOG_INFO("Remote snapshot built by follower (PeerId: %v)",
            id);

        const auto& rsp = rspOrError.Value();
        SnapshotChecksums_[id] = rsp->checksum();
    }

    void OnLocalSnapshotBuilt(const TErrorOr<TRemoteSnapshotParams>& paramsOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        SnapshotPromise_.Set(paramsOrError);

        if (!paramsOrError.IsOK()) {
            YT_LOG_WARNING(paramsOrError, "Error building local snapshot");
            return;
        }

        YT_LOG_INFO("Local snapshot built");

        const auto& params = paramsOrError.Value();
        SnapshotChecksums_[Owner_->CellManager_->GetSelfPeerId()] = params.Checksum;
    }

    void OnSnapshotsComplete(const TError&)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        int successCount = 0;
        bool checksumMismatch = false;
        std::optional<TChecksum> canonicalChecksum;
        for (TPeerId id = 0; id < SnapshotChecksums_.size(); ++id) {
            auto checksum = SnapshotChecksums_[id];
            if (checksum) {
                ++successCount;
                if (canonicalChecksum) {
                    checksumMismatch |= (*canonicalChecksum != *checksum);
                } else {
                    canonicalChecksum = checksum;
                }
            }
        }

        YT_LOG_INFO("Distributed snapshot creation finished (SuccessCount: %v)",
            successCount);

        if (checksumMismatch) {
            for (TPeerId id = 0; id < SnapshotChecksums_.size(); ++id) {
                auto checksum = SnapshotChecksums_[id];
                if (checksum) {
                    YT_LOG_ERROR("Snapshot checksum mismatch (SnapshotId: %v, PeerId: %v, Checksum: %llx)",
                        GetSnapshotId(),
                        id,
                        *checksum);
                }
            }
        }

        auto owner = Owner_;
        Owner_->EpochContext_->EpochUserAutomatonInvoker->Invoke(BIND([owner] () {
            owner->BuildingSnapshot_ = false;
        }));
    }


    void RequestChangelogRotation()
    {
        std::vector<TFuture<void>> asyncResults;
        for (auto peerId = 0; peerId < Owner_->CellManager_->GetTotalPeerCount(); ++peerId) {
            if (peerId == Owner_->CellManager_->GetSelfPeerId()) {
                continue;
            }
                
            auto channel = Owner_->CellManager_->GetPeerChannel(peerId);
            if (!channel) {
                continue;
            }

            YT_LOG_DEBUG("Requesting follower to rotate the changelog (PeerId: %v)", peerId);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Owner_->Config_->ControlRpcTimeout);

            auto req = proxy.RotateChangelog();
            ToProto(req->mutable_epoch_id(), Owner_->EpochContext_->EpochId);
            req->set_revision(Version_.ToRevision());

            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TSession::OnRemoteChangelogRotated, MakeStrong(this), peerId)
                    .AsyncVia(Owner_->EpochContext_->EpochControlInvoker)));
        }

        asyncResults.push_back(
            Owner_->DecoratedAutomaton_->RotateChangelog().Apply(
                BIND(&TSession::OnLocalChangelogRotated, MakeStrong(this))
                    .AsyncVia(Owner_->EpochContext_->EpochControlInvoker)));

        Combine(asyncResults).Subscribe(
            BIND(&TSession::OnRotationFailed, MakeStrong(this))
                .Via(Owner_->EpochContext_->EpochControlInvoker));
    }

    void OnRemoteChangelogRotated(TPeerId id, const THydraServiceProxy::TErrorOrRspRotateChangelogPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error rotating changelog at follower (PeerId: %v)", id);
            return;
        }

        YT_LOG_INFO("Remote changelog rotated by follower (PeerId: %v)", id);

        ++RemoteRotationSuccessCount_;
        CheckRotationQuorum();
    }

    void OnLocalChangelogRotated(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (ChangelogPromise_.IsSet())
            return;

        if (!error.IsOK()) {
            ChangelogPromise_.Set(TError("Error rotating local changelog") << error);
            return;
        }

        YT_LOG_INFO("Local changelog rotated");

        YT_VERIFY(!LocalRotationSuccessFlag_);
        LocalRotationSuccessFlag_ = true;
        CheckRotationQuorum();
    }

    void CheckRotationQuorum()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (ChangelogPromise_.IsSet())
            return;

        // NB: It is vital to wait for the local rotation to complete.
        // Otherwise we risk assigning out-of-order versions.
        if (!LocalRotationSuccessFlag_ || RemoteRotationSuccessCount_ < Owner_->CellManager_->GetQuorumPeerCount() - 1)
            return;

        Owner_->EpochContext_->EpochUserAutomatonInvoker->Invoke(
            BIND(&TSession::OnRotationSucceeded, MakeStrong(this)));

        Owner_->DecoratedAutomaton_->SetLastLeadingSegmentId(Version_.SegmentId + 1);

        ChangelogPromise_.Set();
    }

    void OnRotationSucceeded()
    {
        VERIFY_THREAD_AFFINITY(Owner_->AutomatonThread);

        Owner_->RotatingChangelogs_ = false;
        Owner_->EpochContext_->LeaderCommitter->ResumeLogging();
    }

    void OnRotationFailed(const TError&)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (ChangelogPromise_.IsSet())
            return;

        ChangelogPromise_.Set(TError("Not enough successful changelog rotation replies: %v out of %v",
            RemoteRotationSuccessCount_ + 1,
            Owner_->CellManager_->GetTotalPeerCount()));
    }
};

////////////////////////////////////////////////////////////////////////////////

TCheckpointer::TCheckpointer(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TLeaderCommitterPtr leaderCommitter,
    ISnapshotStorePtr snapshotStore,
    TEpochContext* epochContext,
    NLogging::TLogger logger)
    : Config_(config)
    , Options_(options)
    , DecoratedAutomaton_(decoratedAutomaton)
    , EpochContext_(epochContext)
    , Logger(std::move(logger))
    , CellManager_(EpochContext_->CellManager)
{
    YT_VERIFY(Config_);
    YT_VERIFY(DecoratedAutomaton_);
    YT_VERIFY(EpochContext_);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochUserAutomatonInvoker, AutomatonThread);
}

TCheckpointer::TRotateChangelogResult TCheckpointer::RotateChangelog()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(CanRotateChangelogs());

    auto session = New<TSession>(this, false, false);
    session->Run();
    return session->GetChangelogResult();
}

TCheckpointer::TBuildSnapshotResult TCheckpointer::BuildSnapshot(bool setReadOnly)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(CanBuildSnapshot());

    auto session = New<TSession>(this, true, setReadOnly, Config_->BuildSnapshotDelay);
    session->Run();

    return TBuildSnapshotResult{session->GetChangelogResult(), session->GetSnapshotResult(), session->GetSnapshotId()};
}

bool TCheckpointer::CanBuildSnapshot() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return
        !BuildingSnapshot_ &&
        !RotatingChangelogs_ &&
        DecoratedAutomaton_->GetLoggedVersion().RecordId > 0;
}

bool TCheckpointer::CanRotateChangelogs() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return !RotatingChangelogs_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
