#include "stdafx.h"
#include "changelog_discovery.h"
#include "private.h"
#include "config.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/actions/invoker_util.h>

#include <core/logging/log.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/hydra/hydra_service_proxy.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TChangelogInfo::TChangelogInfo()
    : PeerId(InvalidPeerId)
    , ChangelogId(NonexistingSegmentId)
    , RecordCount(-1)
{ }

////////////////////////////////////////////////////////////////////////////////

class TChangelogDiscovery
    : public TRefCounted
{
public:
    TChangelogDiscovery(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        int changelogId,
        int minRecordCount)
        : Config(config)
        , CellManager(cellManager)
        , MinRecordCount(minRecordCount)
        , PromiseLock(false)
        , Awaiter(New<TParallelAwaiter>(GetSyncInvoker()))
        , Promise(NewPromise<TChangelogInfo>())
        , Logger(HydraLogger)
    {
        YCHECK(Config);
        YCHECK(CellManager);

        Logger.AddTag("CellId: %v", CellManager->GetCellId());

        ChangelogInfo.ChangelogId = changelogId;
    }

    TFuture<TChangelogInfo> Run()
    {
        auto awaiter = Awaiter;
        auto promise = Promise;

        LOG_INFO("Looking for a replica of changelog %v with at least %v records",
            ChangelogInfo.ChangelogId,
            MinRecordCount);

        for (auto peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
            auto channel = CellManager->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_INFO("Requesting changelog info from peer %v", peerId);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config->ControlRpcTimeout);

            auto req = proxy.LookupChangelog();
            req->set_changelog_id(ChangelogInfo.ChangelogId);
            awaiter->Await(
                req->Invoke(),
                BIND(&TChangelogDiscovery::OnResponse, MakeStrong(this), peerId));
        }
        LOG_INFO("Changelog lookup requests sent");

        awaiter->Complete(
            BIND(&TChangelogDiscovery::OnComplete, MakeStrong(this)));

        return promise;
    }

private:
    TDistributedHydraManagerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    int MinRecordCount;

    TAtomic PromiseLock;
    TParallelAwaiterPtr Awaiter;
    TPromise<TChangelogInfo> Promise;
    TChangelogInfo ChangelogInfo;

    NLog::TLogger Logger;


    bool AcquireLock()
    {
        return AtomicCas(&PromiseLock, true, false);
    }

    void SetPromise()
    {
        Promise.Set(ChangelogInfo);
        Awaiter->Cancel();
    }

    void OnResponse(
        TPeerId peerId,
        const THydraServiceProxy::TErrorOrRspLookupChangelogPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error looking up changelog at peer %v",
                peerId);
            return;
        }

        const auto& rsp = rspOrError.Value();
        LOG_INFO("Found changelog %v on peer %v with %v record(s)",
            ChangelogInfo.ChangelogId,
            peerId,
            rsp->record_count());

        if (rsp->record_count() < MinRecordCount)
            return;

        if (!AcquireLock())
            return;

        ChangelogInfo.PeerId = peerId;
        ChangelogInfo.RecordCount = rsp->record_count();
        SetPromise();
    }

    void OnComplete()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (ChangelogInfo.ChangelogId == NonexistingSegmentId) {
            LOG_INFO("Changelog lookup failed, no suitable replica found");
        } else {
            LOG_INFO("Changelog lookup succeeded, found replica on peer %v (RecordCount: %v)",
                ChangelogInfo.PeerId,
                ChangelogInfo.RecordCount);
        }

        if (!AcquireLock())
            return;

        SetPromise();
    }

};

TFuture<TChangelogInfo> DiscoverChangelog(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    int changelogId,
    int minRecordCount)
{
    auto discovery = New<TChangelogDiscovery>(
        config,
        cellManager,
        changelogId,
        minRecordCount);
    return discovery->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
