#include "stdafx.h"
#include "snapshot_discovery.h"
#include "private.h"
#include "config.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/actions/invoker_util.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSnapshotInfo::TSnapshotInfo()
    : SnapshotId(NonexistingSegmentId)
    , PeerId(InvalidPeerId)
    , Length(-1)
{ }

////////////////////////////////////////////////////////////////////////////////

class TSnapshotDiscovery
    : public TRefCounted
{
public:
    TSnapshotDiscovery(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager)
        : Config(config)
        , CellManager(cellManager)
        , Promise(NewPromise<TSnapshotInfo>())
        , Logger(HydraLogger)
    {
        YCHECK(Config);
        YCHECK(CellManager);

        Logger.AddTag(Sprintf("CellGuid: %s",
            ~ToString(CellManager->GetCellGuid())));
    }

    TFuture<TSnapshotInfo> Run(int maxSnapshotId, bool exactId)
    {
        auto awaiter = New<TParallelAwaiter>(GetSyncInvoker());

        if (exactId) {
            LOG_INFO("Looking for snapshot %d", maxSnapshotId);
        } else {
            LOG_INFO("Looking for the latest snapshot up to %d", maxSnapshotId);
        }

        for (auto peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
            auto channel = CellManager->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_INFO("Requesting snapshot info from peer %d", peerId);

            THydraServiceProxy proxy(CellManager->GetPeerChannel(peerId));
            proxy.SetDefaultTimeout(Config->RpcTimeout);

            auto req = proxy.LookupSnapshot();
            req->set_max_snapshot_id(maxSnapshotId);
            req->set_exact_id(exactId);
            awaiter->Await(
                req->Invoke(),
                BIND(&TSnapshotDiscovery::OnResponse, MakeStrong(this), peerId));
        }
        LOG_INFO("Snapshot lookup requests sent");

        awaiter->Complete(
            BIND(&TSnapshotDiscovery::OnComplete, MakeStrong(this)));

        return Promise;
    }

private:
    TDistributedHydraManagerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;

    TPromise<TSnapshotInfo> Promise;

    TSpinLock SpinLock;
    TSnapshotInfo SnapshotInfo;

    NLog::TTaggedLogger Logger;


    void OnResponse(
        TPeerId peerId,
        THydraServiceProxy::TRspLookupSnapshotPtr rsp)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!rsp->IsOK()) {
            LOG_WARNING(rsp->GetError(), "Error looking up snapshots at peer %d",
                peerId);
            return;
        }

        LOG_INFO("Snapshot found on peer %d (SnapshotId: %d, Length: %" PRId64 ")",
            peerId,
            rsp->snapshot_id(),
            rsp->length());

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (rsp->snapshot_id() > SnapshotInfo.SnapshotId) {
                SnapshotInfo.PeerId = peerId;
                SnapshotInfo.SnapshotId = rsp->snapshot_id();
                SnapshotInfo.Length = rsp->length();
                SnapshotInfo.Checksum = rsp->checksum();
            }
        }
    }

    void OnComplete()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (SnapshotInfo.SnapshotId == NonexistingSegmentId) {
            LOG_INFO("Snapshot lookup failed, no suitable snapshot found");
        } else {
            LOG_INFO("Snapshot lookup succeeded (PeerId: %d, SnapshotId: %d)",
                SnapshotInfo.PeerId,
                SnapshotInfo.SnapshotId);
        }

        Promise.Set(SnapshotInfo);
    }

};

TFuture<TSnapshotInfo> DiscoverLatestSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int maxSnapshotId)
{
    auto discovery = New<TSnapshotDiscovery>(config, cellManager);
    return discovery->Run(maxSnapshotId, false);
}

TFuture<TSnapshotInfo> DiscoverSnapshot(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int snapshotId)
{
    auto discovery = New<TSnapshotDiscovery>(config, cellManager);
    return discovery->Run(snapshotId, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
