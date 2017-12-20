#include "changelog_discovery.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/core/rpc/dispatcher.h>

namespace NYT {
namespace NHydra {

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
        , Logger(NLogging::TLogger(HydraLogger)
            .AddTag("ChangelogId: %v, CellId: %v",
                ChangelogId_,
                CellManager_->GetCellId()))
    {
        YCHECK(Config_);
        YCHECK(CellManager_);
    }

    TFuture<TChangelogInfo> Run()
    {
        BIND(&TDiscoverChangelogSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
        return Promise_;
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const int ChangelogId_;
    const int MinRecordCount_;

    const NLogging::TLogger Logger;

    TPromise<TChangelogInfo> Promise_ = NewPromise<TChangelogInfo>();

    void DoRun()
    {
        LOG_INFO("Running changelog discovery");

        std::vector<TFuture<void>> asyncResults;
        for (auto peerId = 0; peerId < CellManager_->GetTotalPeerCount(); ++peerId) {
            auto channel = CellManager_->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_DEBUG("Requesting changelog info (PeerId: %v)",
                peerId,
                ChangelogId_);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.LookupChangelog();
            req->set_changelog_id(ChangelogId_);
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TDiscoverChangelogSession::OnResponse, MakeStrong(this), peerId)
                    .AsyncVia(GetCurrentInvoker())));
        }

        Combine(asyncResults).Subscribe(
            BIND(&TDiscoverChangelogSession::OnComplete, MakeStrong(this))
                .AsyncVia(GetCurrentInvoker()));
    }

    void OnResponse(
        TPeerId peerId,
        const THydraServiceProxy::TErrorOrRspLookupChangelogPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error requesting changelog info (PeerId: %v)",
                peerId);
            return;
        }

        const auto& rsp = rspOrError.Value();
        int recordCount = rsp->record_count();
        LOG_INFO("Changelog info received (PeerId: %v, RecordCount: %v)",
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
            LOG_INFO("Changelog discovery succeeded (PeerId: %v, RecordCount: %v)",
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
        int changelogId)
        : Config_(config)
        , CellManager_(cellManager)
        , ChangelogId_(changelogId)
        , Logger(NLogging::TLogger(HydraLogger)
            .AddTag("ChangelogId: %v, CellId: %v",
                ChangelogId_,
                CellManager_->GetCellId()))
    {
        YCHECK(Config_);
        YCHECK(CellManager_);
    }

    TFuture<TChangelogQuorumInfo> Run()
    {
        BIND(&TComputeQuorumInfoSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
        return Promise_;
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const int ChangelogId_;

    const NLogging::TLogger Logger;

    std::vector<int> RecordCountsLo_;
    std::vector<int> RecordCountsHi_;
    std::vector<TError> InnerErrors_;
    TPromise<TChangelogQuorumInfo> Promise_ = NewPromise<TChangelogQuorumInfo>();


    void DoRun()
    {
        LOG_INFO("Computing changelog quorum record count");

        std::vector<TFuture<void>> asyncResults;
        for (auto peerId = 0; peerId < CellManager_->GetTotalPeerCount(); ++peerId) {
            auto channel = CellManager_->GetPeerChannel(peerId);
            if (!channel) {
                continue;
            }

            LOG_DEBUG("Requesting changelog info (PeerId: %v)",
                peerId);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.LookupChangelog();
            req->set_changelog_id(ChangelogId_);
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TComputeQuorumInfoSession::OnResponse, MakeStrong(this), peerId)));
        }

        Combine(asyncResults).Subscribe(
            BIND(&TComputeQuorumInfoSession::OnComplete, MakeStrong(this)));
    }

    void OnResponse(
        TPeerId peerId,
        const THydraServiceProxy::TErrorOrRspLookupChangelogPtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            const auto& rsp = rspOrError.Value();
            int recordCount = rsp->record_count();
            RecordCountsLo_.push_back(recordCount);
            RecordCountsHi_.push_back(recordCount);

            LOG_DEBUG("Changelog info received (PeerId: %v, RecordCount: %v)",
                peerId,
                recordCount);
        } else {
            InnerErrors_.push_back(rspOrError);
            RecordCountsLo_.push_back(std::numeric_limits<int>::min());
            RecordCountsHi_.push_back(std::numeric_limits<int>::max());

            LOG_WARNING(rspOrError, "Error requesting changelog info (PeerId: %v)",
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

        LOG_INFO("Changelog quorum info count computed successfully (RecordCountLo: %v, RecordCountHi: %v)",
            result.RecordCountLo,
            result.RecordCountHi);

        Promise_.Set(result);
    }
};

TFuture<TChangelogQuorumInfo> ComputeChangelogQuorumInfo(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int changelogId)
{
    auto session = New<TComputeQuorumInfoSession>(
        std::move(config),
        std::move(cellManager),
        changelogId);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
