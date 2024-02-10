#include "snapshot_discovery.h"
#include "private.h"
#include "hydra_service_proxy.h"
#include "config.h"

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NHydra {

using namespace NElection;

///////////////////////////////////////////////////////////////////////////////

class TDiscoverSnapshotSession
    : public TRefCounted
{
public:
    TDiscoverSnapshotSession(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        int maxSnapshotId,
        bool exactId)
        : Config_(std::move(config))
        , CellManager_(std::move(cellManager))
        , MaxSnapshotId_(maxSnapshotId)
        , ExactId_(exactId)
        , Logger(HydraLogger.WithTag("CellId: %v, SelfPeerId: %v",
            CellManager_->GetCellId(),
            CellManager_->GetSelfPeerId()))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(CellManager_);
    }

    TFuture<TRemoteSnapshotParams> Run()
    {
        return BIND(&TDiscoverSnapshotSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

    TFuture<TRemoteSnapshotParams> DoRun()
    {
        if (ExactId_) {
            YT_LOG_INFO("Running snapshot discovery (SnapshotId: %v)",
                MaxSnapshotId_);
        } else {
            YT_LOG_INFO("Running snapshot discovery (MaxSnapshotId: %v)",
                MaxSnapshotId_);
        }

        std::vector<TFuture<void>> asyncResults;
        asyncResults.reserve(CellManager_->GetTotalPeerCount());
        for (auto peerId = 0; peerId < CellManager_->GetTotalPeerCount(); ++peerId) {
            auto channel = CellManager_->GetPeerChannel(peerId);
            if (!channel) {
                continue;
            }

            YT_LOG_DEBUG("Requesting snapshot info (PeerId: %v)",
                peerId);

            TInternalHydraServiceProxy proxy(std::move(channel));
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.LookupSnapshot();
            req->set_max_snapshot_id(MaxSnapshotId_);
            req->set_exact_id(ExactId_);
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TDiscoverSnapshotSession::OnResponse, MakeStrong(this), peerId)
                    .AsyncVia(GetCurrentInvoker())));
        }

        return AllSucceeded(asyncResults).Apply(
            BIND(&TDiscoverSnapshotSession::OnComplete, MakeStrong(this))
                .AsyncVia(GetCurrentInvoker()));
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const int MaxSnapshotId_;
    const bool ExactId_;

    const NLogging::TLogger Logger;

    TRemoteSnapshotParams Params_;


    void OnResponse(
        int peerId,
        const TInternalHydraServiceProxy::TErrorOrRspLookupSnapshotPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error requesting snapshot info (PeerId: %v)",
                peerId);
            return;
        }

        const auto& rsp = rspOrError.Value();
        int snapshotId = rsp->snapshot_id();

        YT_LOG_DEBUG("Snapshot info received (PeerId: %v, SnapshotId: %v)",
            peerId,
            snapshotId);

        if (rsp->snapshot_id() > Params_.SnapshotId) {
            Params_.PeerId = peerId;
            Params_.SnapshotId = snapshotId;
            Params_.CompressedLength = rsp->compressed_length();
            Params_.UncompressedLength = rsp->uncompressed_length();
            Params_.Checksum = rsp->checksum();
            Params_.Meta = rsp->meta();
        }
    }

    TRemoteSnapshotParams OnComplete(const TError&)
    {
        if (ExactId_ && Params_.SnapshotId == InvalidSegmentId) {
            THROW_ERROR_EXCEPTION("Unable to find a download source for snapshot %v",
                MaxSnapshotId_);
        }

        if (Params_.SnapshotId == InvalidSegmentId) {
            YT_LOG_INFO("Snapshot discovery finished; no feasible snapshot is found");
        } else {
            YT_LOG_INFO("Snapshot discovery succeeded (PeerId: %v, SnapshotId: %v)",
                Params_.PeerId,
                Params_.SnapshotId);
        }

        return Params_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<TRemoteSnapshotParams> DiscoverLatestSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int maxSnapshotId)
{
    auto session = New<TDiscoverSnapshotSession>(
        std::move(config),
        std::move(cellManager),
        maxSnapshotId,
        /*exactId*/ false);
    return session->Run();
}

TFuture<TRemoteSnapshotParams> DiscoverSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int snapshotId)
{
    auto session = New<TDiscoverSnapshotSession>(
        std::move(config),
        std::move(cellManager),
        snapshotId,
        /*exactId*/ true);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra


