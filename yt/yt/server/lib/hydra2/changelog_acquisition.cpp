#include "changelog_acquisition.h"
#include "decorated_automaton.h"
#include "mutation_committer.h"

#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>
#include <yt/yt/server/lib/hydra_common/snapshot_discovery.h>

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>
#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/client/hydra/version.h>

namespace NYT::NHydra2 {

using namespace NElection;
using namespace NConcurrency;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

class TAcquireChangelogSession
    : public TRefCounted
{
public:
    TAcquireChangelogSession(
        TDistributedHydraManagerConfigPtr config,
        TEpochContextPtr epochContext,
        i64 changelogId,
        std::optional<TPeerPriority> priority)
        : Config_(config)
        , EpochContext_(epochContext)
        , ChangelogId_(changelogId)
        , Priority_(priority)
    { }

    TFuture<void> Run()
    {
        YT_LOG_INFO("Starting acquiring changelog (ChangelogId: %v, Priority: %v)",
            ChangelogId_,
            Priority_);

        DoAcquireChangelog();

        return ChangelogPromise_;
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const TEpochContextPtr EpochContext_;
    const i64 ChangelogId_;
    const std::optional<TPeerPriority> Priority_;
    const NLogging::TLogger Logger = HydraLogger;

    int SuccessCount_ = 0;

    TPromise<void> ChangelogPromise_ = NewPromise<void>();

    void DoAcquireChangelog()
    {
        std::vector<TFuture<void>> futures;
        for (auto peerId = 0; peerId < EpochContext_->CellManager->GetTotalPeerCount(); ++peerId) {
            if (peerId == EpochContext_->CellManager->GetSelfPeerId()) {
                continue;
            }

            // copypaste
            auto channel = EpochContext_->CellManager->GetPeerChannel(peerId);
            if (!channel) {
                continue;
            }

            YT_LOG_INFO("Acquiring changelog from follower (PeerId: %v, ChangelogId: %v, Term: %v)",
                peerId,
                ChangelogId_,
                EpochContext_->Term);

            TInternalHydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.AcquireChangelog();

            req->set_changelog_id(ChangelogId_);
            if (Priority_) {
                ToProto(req->mutable_priority(), *Priority_);
            }
            req->set_term(EpochContext_->Term);

            futures.push_back(req->Invoke().Apply(
                BIND(&TAcquireChangelogSession::OnRemoteChangelogAcquired, MakeStrong(this), peerId)
                    .AsyncVia(EpochContext_->EpochControlInvoker)));
        }

        futures.push_back(
            AcquireLocalChangelog(ChangelogId_, EpochContext_->Term).Apply(
                BIND(&TAcquireChangelogSession::OnLocalChangelogAcquired, MakeStrong(this))
                    .AsyncVia(EpochContext_->EpochControlInvoker)));

        AllSucceeded(futures).Subscribe(
            BIND(&TAcquireChangelogSession::OnFailed, MakeStrong(this))
                .Via(EpochContext_->EpochControlInvoker));
    }

    TFuture<void> AcquireLocalChangelog(int changelogId, int term)
    {
        const auto& changelogStore = EpochContext_->ChangelogStore;
        auto currentChangelogId = WaitFor(changelogStore->GetLatestChangelogId())
            .ValueOrThrow();

        if (currentChangelogId >= changelogId) {
            return MakeFuture(TError(
                "Cannot acquire local changelog %v, because changelog %v exists",
                    changelogId,
                    currentChangelogId));
        }

        NHydra::NProto::TChangelogMeta meta;
        // Questionable.
        meta.set_term(term);
        for (int i = currentChangelogId + 1; i < changelogId; ++i) {
            changelogStore->CreateChangelog(i, meta);
        }
        return changelogStore->CreateChangelog(changelogId, meta).AsVoid();
    }

    void OnRemoteChangelogAcquired(TPeerId id, const TInternalHydraServiceProxy::TErrorOrRspAcquireChangelogPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_LOG_INFO(rspOrError, "Error aqcuiring changelog at follower (PeerId: %v)", id);
            return;
        }

        auto voting = EpochContext_->CellManager->GetPeerConfig(id).Voting;
        YT_LOG_INFO("Remote changelog acquired by follower (PeerId: %v, Voting: %v)",
            id,
            voting);

        if (voting) {
            ++SuccessCount_;
            CheckQuorum();
        }
    }

    void OnLocalChangelogAcquired(const TError& error)
    {
        if (!error.IsOK()) {
            ChangelogPromise_.TrySet(TError("Error rotating local changelog") << error);
            return;
        }

        YT_LOG_INFO("Local changelog rotated");

        ++SuccessCount_;
        CheckQuorum();
    }

    void CheckQuorum()
    {
        if (ChangelogPromise_.IsSet()) {
            return;
        }

        // NB: It is vital to wait for the local rotation to complete.
        // Otherwise we risk assigning out-of-order versions.

        if (SuccessCount_ < EpochContext_->CellManager->GetQuorumPeerCount()) {
            return;
        }

        ChangelogPromise_.TrySet();
    }

    void OnFailed(const TError& /* error */)
    {
        ChangelogPromise_.TrySet(TError("Not enough successful replies: %v out of %v",
            SuccessCount_,
            EpochContext_->CellManager->GetTotalPeerCount()));
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<void> RunChangelogAcquisition(
    TDistributedHydraManagerConfigPtr config,
    TEpochContextPtr epochContext,
    int changelogId,
    std::optional<TPeerPriority> priority)
{
    auto session = New<TAcquireChangelogSession>(config, epochContext, changelogId, priority);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
