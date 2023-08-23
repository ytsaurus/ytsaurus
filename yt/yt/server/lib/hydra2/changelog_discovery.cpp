#include "changelog_discovery.h"
#include "private.h"
#include "hydra_service_proxy.h"

#include <yt/yt/server/lib/hydra_common/config.h>

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NHydra2 {

using namespace NElection;
using namespace NConcurrency;
using namespace NHydra;

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
        , Logger(HydraLogger.WithTag("ChangelogId: %v, CellId: %v",
            ChangelogId_,
            CellManager_->GetCellId()))
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
        YT_LOG_INFO("Running changelog discovery");

        std::vector<TFuture<void>> asyncResults;
        for (auto peerId = 0; peerId < CellManager_->GetTotalPeerCount(); ++peerId) {
            auto channel = CellManager_->GetPeerChannel(peerId);
            if (!channel)
                continue;

            YT_LOG_DEBUG("Requesting changelog info (PeerId: %v)",
                peerId,
                ChangelogId_);

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
        TPeerId peerId,
        const TInternalHydraServiceProxy::TErrorOrRspLookupChangelogPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error requesting changelog info (PeerId: %v)",
                peerId);
            return;
        }

        const auto& rsp = rspOrError.Value();
        int recordCount = rsp->record_count();
        YT_LOG_INFO("Changelog info received (PeerId: %v, RecordCount: %v)",
            peerId,
            recordCount);

        if (recordCount < MinRecordCount_) {
            return;
        }

        TChangelogInfo result;
        result.ChangelogId = ChangelogId_;
        result.PeerId = peerId;
        result.RecordCount = recordCount;

        if (Promise_.TrySet(result)) {
            YT_LOG_INFO("Changelog discovery succeeded (PeerId: %v, RecordCount: %v)",
                peerId,
                recordCount);
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

class TComputeQuorumInfoSession
    : public TRefCounted
{
public:
    TComputeQuorumInfoSession(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        int changelogId,
        int localRecordCount)
        : Config_(config)
        , CellManager_(cellManager)
        , ChangelogId_(changelogId)
        , Logger(HydraLogger.WithTag("ChangelogId: %v, CellId: %v",
            ChangelogId_,
            CellManager_->GetCellId()))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(CellManager_);

        RegisterSuccess(localRecordCount);
    }

    TFuture<TChangelogQuorumInfo> Run()
    {
        YT_UNUSED_FUTURE(BIND(&TComputeQuorumInfoSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run());
        return Promise_;
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const int ChangelogId_;

    const NLogging::TLogger Logger;

    std::vector<int> RecordCountsLo_;
    std::vector<int> RecordCountsHi_;
    // TODO(aleksandra-zh): Consider actually using this.
    std::vector<TError> InnerErrors_;
    const TPromise<TChangelogQuorumInfo> Promise_ = NewPromise<TChangelogQuorumInfo>();


    void RegisterSuccess(int recordCount)
    {
        RecordCountsLo_.push_back(recordCount);
        RecordCountsHi_.push_back(recordCount);
    }

    void RegisterFailure(const TError& error)
    {
        InnerErrors_.push_back(error);
        RecordCountsLo_.push_back(std::numeric_limits<int>::min());
        RecordCountsHi_.push_back(std::numeric_limits<int>::max());
    }

    void DoRun()
    {
        YT_LOG_INFO("Computing changelog quorum record count");

        std::vector<TFuture<void>> asyncResults;
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

            YT_LOG_DEBUG("Requesting changelog info (PeerId: %v)",
                peerId);

            TInternalHydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.LookupChangelog();
            req->set_changelog_id(ChangelogId_);
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TComputeQuorumInfoSession::OnResponse, MakeStrong(this), peerId)));
        }

        AllSucceeded(asyncResults).Subscribe(
            BIND(&TComputeQuorumInfoSession::OnComplete, MakeStrong(this)));
    }

    void OnResponse(
        TPeerId peerId,
        const TInternalHydraServiceProxy::TErrorOrRspLookupChangelogPtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            const auto& rsp = rspOrError.Value();
            int recordCount = rsp->record_count();
            RegisterSuccess(recordCount);

            YT_LOG_DEBUG("Changelog info received (PeerId: %v, RecordCount: %v)",
                peerId,
                recordCount);
        } else {
            RegisterFailure(rspOrError);

            YT_LOG_WARNING(rspOrError, "Error requesting changelog info (PeerId: %v)",
                peerId);
        }
    }

    void OnComplete(const TError&)
    {
        std::sort(RecordCountsLo_.begin(), RecordCountsLo_.end());
        std::sort(RecordCountsHi_.begin(), RecordCountsHi_.end());

        int quorum = CellManager_->GetQuorumPeerCount();
        TChangelogQuorumInfo result{
            RecordCountsLo_[quorum - 1],
            RecordCountsHi_[quorum - 1]
        };

        YT_LOG_INFO("Changelog quorum info count computed successfully (RecordCountLo: %v, RecordCountHi: %v)",
            result.RecordCountLo,
            result.RecordCountHi);

        Promise_.Set(result);
    }
};

TFuture<TChangelogQuorumInfo> ComputeChangelogQuorumInfo(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int changelogId,
    int localRecordCount)
{
    auto session = New<TComputeQuorumInfoSession>(
        std::move(config),
        std::move(cellManager),
        changelogId,
        localRecordCount);
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
        RegisterSuccess(localChangelogId, localTerm);
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

    const NLogging::TLogger Logger = HydraLogger;

    int ChangelogId_ = 0;
    int Term_ = 0;
    int SuccessCount_ = 0;

    std::vector<TError> InnerErrors_;


    void RegisterSuccess(int changelogId, int term)
    {
        ++SuccessCount_;
        ChangelogId_ = std::max(ChangelogId_, changelogId);
        Term_ = std::max(Term_, term);
    }

    void RegisterFailure(const TError& error)
    {
        InnerErrors_.push_back(error);
    }

    void DoRun()
    {
        YT_LOG_INFO("Computing latest quorum changelog id");

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

            YT_LOG_DEBUG("Requesting changelog info (PeerId: %v)",
                peerId);

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
        TPeerId peerId,
        const TInternalHydraServiceProxy::TErrorOrRspGetLatestChangelogIdPtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            const auto& rsp = rspOrError.Value();
            int changelogId = rsp->changelog_id();
            int term = rsp->term();
            RegisterSuccess(changelogId, term);

            YT_LOG_DEBUG("Changelog id received (PeerId: %v, ChangelogId: %v, Term: %v)",
                peerId,
                changelogId,
                term);
        } else {
            RegisterFailure(rspOrError);

            YT_LOG_WARNING(rspOrError, "Error requesting changelog id (PeerId: %v)",
                peerId);
        }
    }

    void OnComplete(const TError&)
    {
        int quorum = CellManager_->GetQuorumPeerCount();
        if (SuccessCount_ < quorum) {
            Promise_.TrySet(TError("Not enough answers to compute quorum changelog id: %v out of %v",
                SuccessCount_,
                quorum));
            return;
        }

        YT_LOG_INFO("Computed quorum latest changelog id (ChangelogId: %v, Term: %v)",
            ChangelogId_,
            Term_);

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

} // namespace NYT::NHydra2
