#include "changelog_discovery.h"
#include "private.h"
#include "hydra_service_proxy.h"
#include "config.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDiscoverChangelogSession
    : public TRefCounted
{
public:
    TDiscoverChangelogSession(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        int changelogId,
        int minRecordCount)
        : Config_(std::move(config))
        , CellManager_(std::move(cellManager))
        , ChangelogId_(changelogId)
        , MinRecordCount_(minRecordCount)
        , Logger(HydraLogger()
            .WithTag("ChangelogId", ChangelogId_)
            .WithTag("CellId", CellManager_->GetCellId()))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(CellManager_);
    }

    TFuture<TChangelogInfo> Run()
    {
        YT_UNUSED_FUTURE(BIND(&TDiscoverChangelogSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run());
        return Promise_;
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const int ChangelogId_;
    const int MinRecordCount_;

    const NLogging::TLogger Logger;

    const TPromise<TChangelogInfo> Promise_ = NewPromise<TChangelogInfo>();

    void DoRun()
    {
        YT_TLOG_INFO("Running changelog discovery");

        std::vector<TFuture<void>> asyncResults;
        for (auto peerId = 0; peerId < CellManager_->GetTotalPeerCount(); ++peerId) {
            auto channel = CellManager_->GetPeerChannel(peerId);
            if (!channel)
                continue;

            YT_TLOG_DEBUG("Requesting changelog info")
                .With("PeerId", peerId)
                .With("ChangelogId", ChangelogId_);

            TInternalHydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.LookupChangelog();
            req->set_changelog_id(ChangelogId_);
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TDiscoverChangelogSession::OnResponse, MakeStrong(this), peerId)
                    .AsyncVia(GetCurrentInvoker())));
        }

        AllSucceeded(asyncResults).Subscribe(
            BIND(&TDiscoverChangelogSession::OnComplete, MakeStrong(this))
                .Via(GetCurrentInvoker()));
    }

    void OnResponse(
        int peerId,
        const TInternalHydraServiceProxy::TErrorOrRspLookupChangelogPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_TLOG_WARNING("Error requesting changelog info")
                .With("PeerId", peerId)
                .With(rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();
        int recordCount = rsp->record_count();
        YT_TLOG_INFO("Changelog info received")
            .With("PeerId", peerId)
            .With("RecordCount", recordCount);

        if (recordCount < MinRecordCount_) {
            return;
        }

        TChangelogInfo result;
        result.ChangelogId = ChangelogId_;
        result.PeerId = peerId;
        result.RecordCount = recordCount;

        if (Promise_.TrySet(result)) {
            YT_TLOG_INFO("Changelog discovery succeeded")
                .With("PeerId", peerId)
                .With("RecordCount", recordCount);
        }
    }

    void OnComplete(const TError&)
    {
        Promise_.TrySet(TError("Unable to find a download source for changelog %v with %v records",
            ChangelogId_,
            MinRecordCount_));
    }
};

TFuture<TChangelogInfo> DiscoverChangelog(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int changelogId,
    int minRecordCount)
{
    auto session = New<TDiscoverChangelogSession>(
        std::move(config),
        std::move(cellManager),
        changelogId,
        minRecordCount);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TComputeQuorumLatestChangelogIdSession
    : public TRefCounted
{
public:
    TComputeQuorumLatestChangelogIdSession(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        int localChangelogId,
        int localTerm)
        : Config_(config)
        , CellManager_(cellManager)
    {
        YT_VERIFY(Config_);
        YT_VERIFY(CellManager_);

        YT_VERIFY(CellManager_->GetSelfConfig()->Voting);
        RegisterSuccess(CellManager_->GetSelfPeerId(), localChangelogId, localTerm);
    }

    TFuture<std::pair<int, int>> Run()
    {
        YT_UNUSED_FUTURE(BIND(&TComputeQuorumLatestChangelogIdSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run());
        return Promise_;
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const TPromise<std::pair<int, int>> Promise_ = NewPromise<std::pair<int, int>>();

    const NLogging::TLogger Logger = HydraLogger();

    int ChangelogId_ = 0;
    int Term_ = 0;
    int SuccessWeight_ = 0;

    std::vector<TError> InnerErrors_;


    void RegisterSuccess(int peerId, int changelogId, int term)
    {
        SuccessWeight_ += CellManager_->GetPeerWeight(peerId);
        ChangelogId_ = std::max(ChangelogId_, changelogId);
        Term_ = std::max(Term_, term);
    }

    void RegisterFailure(const TError& error)
    {
        InnerErrors_.push_back(error);
    }

    void DoRun()
    {
        YT_TLOG_INFO("Computing latest quorum changelog id");

        std::vector<TFuture<void>> asyncResults;
        asyncResults.reserve(CellManager_->GetTotalPeerCount());
        for (auto peerId = 0; peerId < CellManager_->GetTotalPeerCount(); ++peerId) {
            if (peerId == CellManager_->GetSelfPeerId()) {
                continue;
            }

            const auto& config = CellManager_->GetPeerConfig(peerId);
            if (!config->Voting) {
                continue;
            }

            auto channel = CellManager_->GetPeerChannel(peerId);
            if (!channel) {
                continue;
            }

            YT_TLOG_DEBUG("Requesting changelog info")
                .With("PeerId", peerId);

            TInternalHydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.GetLatestChangelogId();
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TComputeQuorumLatestChangelogIdSession::OnResponse, MakeStrong(this), peerId)));
        }

        AllSucceeded(asyncResults).Subscribe(
            BIND(&TComputeQuorumLatestChangelogIdSession::OnComplete, MakeStrong(this)));
    }

    void OnResponse(
        int peerId,
        const TInternalHydraServiceProxy::TErrorOrRspGetLatestChangelogIdPtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            const auto& rsp = rspOrError.Value();
            int changelogId = rsp->changelog_id();
            int term = rsp->term();
            RegisterSuccess(peerId, changelogId, term);

            YT_TLOG_DEBUG("Changelog id received")
                .With("PeerId", peerId)
                .With("ChangelogId", changelogId)
                .With("Term", term);
        } else {
            RegisterFailure(rspOrError);

            YT_TLOG_WARNING("Error requesting changelog id")
                .With("PeerId", peerId)
                .With(rspOrError);
        }
    }

    void OnComplete(const TError&)
    {
        auto quorumWeight = CellManager_->GetQuorumWeight();
        if (SuccessWeight_ < quorumWeight) {
            Promise_.TrySet(TError("Not enough answers to compute quorum changelog id: weight %v out of %v",
                SuccessWeight_,
                quorumWeight));
            return;
        }

        YT_TLOG_INFO("Computed quorum latest changelog id")
            .With("ChangelogId", ChangelogId_)
            .With("Term", Term_);

        Promise_.Set({ChangelogId_, Term_});
    }
};

TFuture<std::pair<int, int>> ComputeQuorumLatestChangelogId(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int localChangelogId,
    int localTerm)
{
    auto session = New<TComputeQuorumLatestChangelogIdSession>(
        std::move(config),
        std::move(cellManager),
        localChangelogId,
        localTerm);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
