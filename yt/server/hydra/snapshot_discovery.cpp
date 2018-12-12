#include "snapshot_discovery.h"
#include "private.h"
#include "config.h"

#include <yt/server/hydra/snapshot_service_proxy.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/core/rpc/dispatcher.h>

namespace NYT::NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

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
        , Logger(NLogging::TLogger(HydraLogger)
            .AddTag("CellId: %v", CellManager_->GetCellId()))
    {
        YCHECK(Config_);
        YCHECK(CellManager_);
    }

    TFuture<TRemoteSnapshotParams> Run()
    {
        BIND(&TDiscoverSnapshotSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
        return Promise_;
    }

    void DoRun()
    {
        if (ExactId_) {
            YT_LOG_INFO("Running snapshot discovery (SnapshotId: %v)",
                MaxSnapshotId_);
        } else {
            YT_LOG_INFO("Running snapshot discovery (MaxSnapshotId: %v)",
                MaxSnapshotId_);
        }

        std::vector<TFuture<void>> asyncResults;
        for (auto peerId = 0; peerId < CellManager_->GetTotalPeerCount(); ++peerId) {
            auto channel = CellManager_->GetPeerChannel(peerId);
            if (!channel) {
                continue;
            }

            YT_LOG_DEBUG("Requesting snapshot info (PeerId: %v)",
                peerId);

            TSnapshotServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.LookupSnapshot();
            req->set_max_snapshot_id(MaxSnapshotId_);
            req->set_exact_id(ExactId_);
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TDiscoverSnapshotSession::OnResponse, MakeStrong(this), peerId)
                    .AsyncVia(GetCurrentInvoker())));
        }

        Combine(asyncResults).Subscribe(
            BIND(&TDiscoverSnapshotSession::OnComplete, MakeStrong(this))
                .Via(GetCurrentInvoker()));
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const int MaxSnapshotId_;
    const bool ExactId_;

    const NLogging::TLogger Logger;

    TPromise<TRemoteSnapshotParams> Promise_ = NewPromise<TRemoteSnapshotParams>();
    TRemoteSnapshotParams Params_;


    void OnResponse(
        TPeerId peerId,
        const TSnapshotServiceProxy::TErrorOrRspLookupSnapshotPtr& rspOrError)
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

    void OnComplete(const TError&)
    {
        if (ExactId_ && Params_.SnapshotId == InvalidSegmentId) {
            auto error = TError("Unable to find a download source for snapshot %v",
                MaxSnapshotId_);
            Promise_.Set(error);
            return;
        }

        if (Params_.SnapshotId == InvalidSegmentId) {
            YT_LOG_INFO("Snapshot discovery finished; no feasible snapshot is found");
        } else {
            YT_LOG_INFO("Snapshot discovery succeeded (PeerId: %v, SnapshotId: %v)",
                Params_.PeerId,
                Params_.SnapshotId);
        }

        Promise_.Set(Params_);
    }
};

TFuture<TRemoteSnapshotParams> DiscoverLatestSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int maxSnapshotId)
{
    auto session = New<TDiscoverSnapshotSession>(
        config,
        cellManager,
        maxSnapshotId,
        false);
    return session->Run();
}

TFuture<TRemoteSnapshotParams> DiscoverSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int snapshotId)
{
    auto session = New<TDiscoverSnapshotSession>(
        config,
        cellManager,
        snapshotId,
        true);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
