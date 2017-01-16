#include "changelog_discovery.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

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
        : Config_(config)
        , CellManager_(cellManager)
        , ChangelogId_(changelogId)
        , MinRecordCount_(minRecordCount)
    {
        YCHECK(Config_);
        YCHECK(CellManager_);

        Logger = HydraLogger;
        Logger.AddTag("CellId: %v",
            ChangelogId_,
            CellManager_->GetCellId());
    }

    TFuture<TChangelogInfo> Run()
    {
        LOG_INFO("Running changelog discovery (ChangelogId: %v)",
            ChangelogId_);

        std::vector<TFuture<void>> asyncResults;
        for (auto peerId = 0; peerId < CellManager_->GetTotalPeerCount(); ++peerId) {
            auto channel = CellManager_->GetPeerChannel(peerId);
            if (!channel)
                continue;

            LOG_INFO("Requesting changelog info (PeerId: %v, ChangelogId: %v)",
                peerId,
                ChangelogId_);

            THydraServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

            auto req = proxy.LookupChangelog();
            req->set_changelog_id(ChangelogId_);
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TChangelogDiscovery::OnResponse, MakeStrong(this), peerId)));
        }

        Combine(asyncResults).Subscribe(
            BIND(&TChangelogDiscovery::OnComplete, MakeStrong(this)));

        return Promise_;
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const NElection::TCellManagerPtr CellManager_;
    const int ChangelogId_;
    const int MinRecordCount_;

    TPromise<TChangelogInfo> Promise_ = NewPromise<TChangelogInfo>();

    NLogging::TLogger Logger;


    void OnResponse(
        TPeerId peerId,
        const THydraServiceProxy::TErrorOrRspLookupChangelogPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error requesting changelog info (PeerId: %v, ChangelogId: %v)",
                peerId,
                ChangelogId_);
            return;
        }

        const auto& rsp = rspOrError.Value();
        int recordCount = rsp->record_count();
        LOG_INFO("Changelog info received (PeerId: %v, ChangelogId: %v, RecordCount: %v)",
            peerId,
            ChangelogId_,
            recordCount);

        if (recordCount < MinRecordCount_)
            return;

        TChangelogInfo result;
        result.ChangelogId = ChangelogId_;
        result.PeerId = peerId;
        result.RecordCount = recordCount;

        if (Promise_.TrySet(result)) {
            LOG_INFO("Changelog discovery succeeded (PeerId: %v, ChangelogId: %v, RecordCount: %v)",
                peerId,
                ChangelogId_,
                recordCount);
        }
    }

    void OnComplete(const TError&)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto error = TError("Unable to find a download source for changelog %v with %v records",
            ChangelogId_,
            MinRecordCount_);
        if (Promise_.TrySet(error)) {
            LOG_INFO("Changelog discovery failed, no suitable peer found (ChangelogId: %v, MinRecordCount: %v)",
                ChangelogId_,
                MinRecordCount_);
        }
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
