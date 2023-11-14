#include "changelog_acquisition.h"
#include "decorated_automaton.h"
#include "mutation_committer.h"
#include "changelog.h"
#include "config.h"
#include "snapshot.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>
#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/client/hydra/version.h>

namespace NYT::NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TAcquireChangelogSession
    : public TRefCounted
{
public:
    TAcquireChangelogSession(
        TDistributedHydraManagerConfigPtr config,
        TEpochContextPtr epochContext,
        int changelogId,
        std::optional<TPeerPriority> priority,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , EpochContext_(std::move(epochContext))
        , ChangelogId_(changelogId)
        , Priority_(priority)
        , Logger(std::move(logger))
    { }

    TFuture<IChangelogPtr> Run()
    {
        YT_LOG_INFO("Starting acquiring changelog (ChangelogId: %v, Priority: %v)",
            ChangelogId_,
            Priority_);

        DoAcquireChangelog();

        return NewChangelogPromise_;
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const TEpochContextPtr EpochContext_;
    const int ChangelogId_;
    const std::optional<TPeerPriority> Priority_;
    const NLogging::TLogger Logger;

    int SuccessCount_ = 0;
    bool LocalSucceeded_ = false;

    const TPromise<IChangelogPtr> NewChangelogPromise_ = NewPromise<IChangelogPtr>();

    IChangelogPtr NewChangelog_;

    void DoAcquireChangelog()
    {
        std::vector<TFuture<void>> futures;
        for (auto peerId = 0; peerId < EpochContext_->CellManager->GetTotalPeerCount(); ++peerId) {
            if (peerId == EpochContext_->CellManager->GetSelfPeerId()) {
                continue;
            }

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
            AcquireLocalChangelog(ChangelogId_).Apply(
                BIND(&TAcquireChangelogSession::OnLocalChangelogAcquired, MakeStrong(this))
                    .AsyncVia(EpochContext_->EpochControlInvoker)));

        AllSucceeded(futures).Subscribe(
            BIND(&TAcquireChangelogSession::OnFailed, MakeStrong(this))
                .Via(EpochContext_->EpochControlInvoker));
    }

    TFuture<void> CreateAndCloseChangelog(int changelogId)
    {
        const auto& changelogStore = EpochContext_->ChangelogStore;
        return changelogStore->CreateChangelog(changelogId, /*meta*/ {})
            .Apply(BIND([] (const IChangelogPtr& changelog) {
                return changelog->Close();
            }));
    }

    TFuture<void> AcquireLocalChangelog(int changelogId)
    {
        const auto& changelogStore = EpochContext_->ChangelogStore;
        int currentChangelogId = changelogStore->GetLatestChangelogIdOrThrow();
        if (currentChangelogId >= changelogId) {
            return MakeFuture(TError(
                "Cannot acquire local changelog %v because changelog %v exists",
                changelogId,
                currentChangelogId));
        }

        auto startChangelogId = std::max(currentChangelogId + 1, changelogId - Config_->MaxChangelogsToCreateDuringAcquisition);
        std::vector<TFuture<void>> futures;
        for (int id = startChangelogId; id < changelogId; ++id) {
            futures.push_back(CreateAndCloseChangelog(id));
        }

        futures.push_back(changelogStore->CreateChangelog(changelogId, /*meta*/ {}).Apply(
            BIND([&, this, this_ = MakeStrong(this)] (const IChangelogPtr& changelog) {
                NewChangelog_ = changelog;
            })));
        return AllSucceeded(std::move(futures));
    }

    void OnRemoteChangelogAcquired(int id, const TInternalHydraServiceProxy::TErrorOrRspAcquireChangelogPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_LOG_INFO(rspOrError, "Error acquiring changelog at follower (PeerId: %v, ChangelogId: %v)",
                id,
                ChangelogId_);
            return;
        }

        auto voting = EpochContext_->CellManager->GetPeerConfig(id)->Voting;
        YT_LOG_INFO("Remote changelog acquired by follower (PeerId: %v, Voting: %v, ChangelogId: %v)",
            id,
            voting,
            ChangelogId_);

        if (voting) {
            ++SuccessCount_;
            CheckQuorum();
        }
    }

    void OnLocalChangelogAcquired(const TError& error)
    {
        if (!error.IsOK()) {
            NewChangelogPromise_.TrySet(TError("Error acquiring local changelog") << error);
            return;
        }

        YT_LOG_INFO("Local changelog acquired (ChangelogId: %v)", ChangelogId_);

        ++SuccessCount_;
        LocalSucceeded_ = true;
        CheckQuorum();
    }

    void CheckQuorum()
    {
        if (NewChangelogPromise_.IsSet()) {
            return;
        }

        if (SuccessCount_ < EpochContext_->CellManager->GetQuorumPeerCount()) {
            return;
        }

        if (!LocalSucceeded_) {
            return;
        }

        NewChangelogPromise_.TrySet(NewChangelog_);
    }

    void OnFailed(const TError& /*error*/)
    {
        NewChangelogPromise_.TrySet(TError("Not enough successful replies: %v out of %v",
            SuccessCount_,
            EpochContext_->CellManager->GetTotalPeerCount())
            << TErrorAttribute("LocalChangelogAcquisitionSuccess", LocalSucceeded_));
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<IChangelogPtr> RunChangelogAcquisition(
    TDistributedHydraManagerConfigPtr config,
    TEpochContextPtr epochContext,
    int changelogId,
    std::optional<TPeerPriority> priority,
    NLogging::TLogger logger)
{
    auto session = New<TAcquireChangelogSession>(
        std::move(config),
        std::move(epochContext),
        changelogId,
        priority,
        std::move(logger));
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
