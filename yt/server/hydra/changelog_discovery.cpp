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

        Logger.AddTag("CellGuid: %v", CellManager->GetCellGuid());

        ChangelogInfo.ChangelogId = changelogId;
    }

    TFuture<TChangelogInfo> Run()
    {
        auto awaiter = Awaiter;
        auto promise = Promise;

        LOG_INFO("Looking for a replica of changelog %d with at least %d records",
            ChangelogInfo.ChangelogId,
            MinRecordCount);

        for (auto peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
            auto channel = CellManager->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_INFO("Requesting changelog info from peer %d", peerId);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config->RpcTimeout);

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
        THydraServiceProxy::TRspLookupChangelogPtr rsp)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!rsp->IsOK()) {
            LOG_WARNING(rsp->GetError(), "Error looking up changelog at peer %d",
                peerId);
            return;
        }

        LOG_INFO("Found changelog %d on peer %d with %d record(s)",
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
            LOG_INFO("Changelog lookup succeeded, found replica on peer %d (RecordCount: %d)",
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
