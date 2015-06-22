#include "stdafx.h"
#include "snapshot_discovery.h"
#include "private.h"
#include "config.h"

#include <core/concurrency/thread_affinity.h>

#include <ytlib/election/cell_manager.h>

#include <server/hydra/snapshot_service_proxy.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSnapshotDiscovery
    : public TRefCounted
{
public:
    TSnapshotDiscovery(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        int maxSnapshotId,
        bool exactId)
        : Config_(config)
        , CellManager_(cellManager)
        , MaxSnapshotId_(maxSnapshotId)
        , ExactId_(exactId)
    {
        YCHECK(Config_);
        YCHECK(CellManager_);

        Logger = HydraLogger;
        Logger.AddTag("CellId: %v", CellManager_->GetCellId());
    }

    TFuture<TRemoteSnapshotParams> Run()
    {
        if (ExactId_) {
            LOG_INFO("Running snapshot discovery (SnapshotId: %v)",
                MaxSnapshotId_);
        } else {
            LOG_INFO("Running latest snapshot discovery (MaxSnapshotId: %v)",
                MaxSnapshotId_);
        }

        std::vector<TFuture<void>> asyncResults;
        for (auto peerId = 0; peerId < CellManager_->GetPeerCount(); ++peerId) {
            auto channel = CellManager_->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_INFO("Requesting snapshot info (PeerId: %v)",
                peerId);

            TSnapshotServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.LookupSnapshot();
            req->set_max_snapshot_id(MaxSnapshotId_);
            req->set_exact_id(ExactId_);
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TSnapshotDiscovery::OnResponse, MakeStrong(this), peerId)));
        }

        Combine(asyncResults).Subscribe(
            BIND(&TSnapshotDiscovery::OnComplete, MakeStrong(this)));

        return Promise_;
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const int MaxSnapshotId_;
    const bool ExactId_;

    TPromise<TRemoteSnapshotParams> Promise_ = NewPromise<TRemoteSnapshotParams>();

    TSpinLock SpinLock_;
    TRemoteSnapshotParams Params_;

    NLogging::TLogger Logger;


    void OnResponse(
        TPeerId peerId,
        const TSnapshotServiceProxy::TErrorOrRspLookupSnapshotPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error requesting snapshot info (PeerId: %v)",
                peerId);
            return;
        }

        const auto& rsp = rspOrError.Value();
        int snapshotId = rsp->snapshot_id();

        LOG_INFO("Snapshot info received (PeerId: %v, SnapshotId: %v)",
            peerId,
            snapshotId);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (rsp->snapshot_id() > Params_.SnapshotId) {
                Params_.PeerId = peerId;
                Params_.SnapshotId = snapshotId;
                Params_.CompressedLength = rsp->compressed_length();
                Params_.UncompressedLength = rsp->uncompressed_length();
                Params_.Checksum = rsp->checksum();
                Params_.Meta = rsp->meta();
            }
        }
    }

    void OnComplete(const TError&)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (Params_.SnapshotId == InvalidSegmentId) {
            LOG_INFO("Snapshot discovery failed, no suitable peer found");
            auto error = ExactId_
                ? TError("Unable to find a download source for snapshot %v", MaxSnapshotId_)
                : TError("Unable to find a download source for snapshots up to %v", MaxSnapshotId_);
            Promise_.Set(error);
        } else {
            LOG_INFO("Snapshot discovery succeeded (PeerId: %v, SnapshotId: %v)",
                Params_.PeerId,
                Params_.SnapshotId);
            Promise_.Set(Params_);
        }
    }

};

TFuture<TRemoteSnapshotParams> DiscoverLatestSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int maxSnapshotId)
{
    auto discovery = New<TSnapshotDiscovery>(
        config,
        cellManager,
        maxSnapshotId,
        false);
    return discovery->Run();
}

TFuture<TRemoteSnapshotParams> DiscoverSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int snapshotId)
{
    auto discovery = New<TSnapshotDiscovery>(
        config,
        cellManager,
        snapshotId,
        true);
    return discovery->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
