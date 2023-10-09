#include "cell_hydra_janitor.h"

#include "tamed_cell_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/incumbent_server/incumbent_detail.h>
#include <yt/yt/server/master/incumbent_server/incumbent_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/cellar_agent/helpers.h>

#include <yt/yt/server/lib/hydra/hydra_janitor_helpers.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/cellar_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NCellServer {

using namespace NConcurrency;
using namespace NCellarAgent;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NHydra;
using namespace NIncumbentClient;
using namespace NIncumbentServer;
using namespace NCellMaster;
using namespace NCellarClient;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellHydraJanitor
    : public ICellHydraJanitor
    , public TShardedIncumbentBase
{
public:
    explicit TCellHydraJanitor(NCellMaster::TBootstrap* bootstrap)
        : TShardedIncumbentBase(
            bootstrap->GetIncumbentManager(),
            EIncumbentType::CellJanitor)
        , Bootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        hydraManager->SubscribeStartLeading(BIND(&TCellHydraJanitor::OnStartEpoch, MakeWeak(this)));
        hydraManager->SubscribeStopLeading(BIND(&TCellHydraJanitor::OnStopEpoch, MakeWeak(this)));
        hydraManager->SubscribeStartFollowing(BIND(&TCellHydraJanitor::OnStartEpoch, MakeWeak(this)));
        hydraManager->SubscribeStopFollowing(BIND(&TCellHydraJanitor::OnStopEpoch, MakeWeak(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(
            BIND(&TCellHydraJanitor::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            const auto& incumbentManager = Bootstrap_->GetIncumbentManager();
            incumbentManager->RegisterIncumbent(this);
        }
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    TPeriodicExecutorPtr PeriodicExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void OnStartEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(!PeriodicExecutor_);
        PeriodicExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletCellJanitor),
            BIND(&TCellHydraJanitor::OnCleanup, MakeWeak(this)));
        PeriodicExecutor_->Start();
    }

    void OnStopEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (PeriodicExecutor_) {
            PeriodicExecutor_->Stop();
            PeriodicExecutor_.Reset();
        }
    }


    const NTabletServer::TDynamicTabletManagerConfigPtr& GetDynamicConfig()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& config = GetDynamicConfig();
        PeriodicExecutor_->SetPeriod(config->TabletCellsCleanupPeriod);
    }


    std::optional<std::vector<THydraFileInfo>> ListHydraFiles(const TYPath& path)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto rootService = objectManager->GetRootService();

        auto listReq = TYPathProxy::List(TYPath(path));
        ToProto(listReq->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "compressed_data_size"
        });

        auto listRsp = WaitFor(ExecuteVerb(rootService, listReq))
            .ValueOrThrow();
        auto list = ConvertTo<IListNodePtr>(TYsonString(listRsp->value()));
        auto children = list->GetChildren();

        std::vector<THydraFileInfo> result;
        result.reserve(children.size());
        for (const auto& child : children) {
            auto key = ConvertTo<TString>(child);
            int id;
            if (!TryFromString<int>(key, id)) {
                YT_LOG_WARNING("Janitor has found a broken Hydra file (Path: %v, Key: %v)",
                    path,
                    key);
                return std::nullopt;
            }

            const auto& attributes = child->Attributes();
            result.push_back({id, attributes.Get<i64>("compressed_data_size")});
        }

        return result;
    }

    void RemoveHydraFiles(const TYPath& path, int thresholdId, int* budget)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto rootService = objectManager->GetRootService();

        auto listReq = TYPathProxy::List(path);
        auto listRsp = WaitFor(ExecuteVerb(rootService, listReq))
            .ValueOrThrow();
        auto list = ConvertTo<IListNodePtr>(TYsonString(listRsp->value()));
        auto children = list->GetChildren();

        for (const auto& child : children) {
            if (*budget <= 0) {
                break;
            }

            auto key = ConvertTo<TString>(child);
            int id;
            if (!TryFromString<int>(key, id)) {
                continue;
            }

            if (id >= thresholdId) {
                continue;
            }

            auto filePath = path + "/" + ToYPathLiteral(key);
            YT_LOG_DEBUG("Janitor is removing Hydra file (Path: %v)", filePath);

            --(*budget);

            auto removeReq = TYPathProxy::Remove(filePath);
            ExecuteVerb(rootService, removeReq)
                .Subscribe(BIND([=] (const TYPathProxy::TErrorOrRspRemovePtr& removeRspOrError) {
                    if (removeRspOrError.IsOK()) {
                        YT_LOG_DEBUG("Janitor has successfully removed Hydra file (Path: %v)", filePath);
                    } else {
                        YT_LOG_WARNING(removeRspOrError, "Janitor has failed to remove Hydra file (Path: %v)", filePath);
                    }
                }));
        }
    }

    void CleanSnapshotsAndChangelogs(
        TCellId /*cellId*/,
        TString path,
        int* snapshotBudget,
        int* changelogBudget)
    {
        auto snapshotPath = path + "/snapshots";
        auto changelogPath = path + "/changelogs";

        auto snapshots = ListHydraFiles(snapshotPath);
        auto changelogs = ListHydraFiles(changelogPath);

        if (!snapshots || !changelogs) {
            return;
        }

        auto thresholdId = ComputeJanitorThresholdId(
            *snapshots,
            *changelogs,
            GetDynamicConfig());

        RemoveHydraFiles(snapshotPath, thresholdId, snapshotBudget);
        RemoveHydraFiles(changelogPath, thresholdId, changelogBudget);
    }

    void OnCleanup()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (GetActiveShardCount() == 0) {
            return;
        }

        YT_LOG_DEBUG("Cell janitor cleanup started (ActiveShardIndices: %v)",
            ListActiveShardIndices());

        int snapshotBudget = GetDynamicConfig()->MaxSnapshotCountToRemovePerCheck;
        int changelogBudget = GetDynamicConfig()->MaxChangelogCountToRemovePerCheck;

        const auto& tamedCellManager = Bootstrap_->GetTamedCellManager();
        auto cellIds = ListActiveCellIds();
        for (auto cellId : cellIds) {
            auto* cell = tamedCellManager->FindCell(cellId);
            if (!IsObjectAlive(cell)) {
                continue;
            }

            try {
                if (cell->CellBundle()->GetOptions()->IndependentPeers) {
                    for (int peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
                        if (!cell->IsAlienPeer(peerId)) {
                            auto path = Format("%v/%v/%v", GetCellCypressPrefix(cellId), cellId, peerId);
                            CleanSnapshotsAndChangelogs(cellId, path, &snapshotBudget, &changelogBudget);
                        }
                    }
                } else {
                    auto path = Format("%v/%v", GetCellCypressPrefix(cellId), cellId);
                    CleanSnapshotsAndChangelogs(cellId, path, &snapshotBudget, &changelogBudget);
                }
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Cell janitor cleanup failed (CellId: %v)", cellId);
            }
        }

        YT_LOG_DEBUG("Cell janitor cleanup completed (CellCount: %v, RemainingSnapshotBudget: %v, RemainingChangelogBudget: %v)",
            cellIds.size(),
            snapshotBudget,
            changelogBudget);
    }

    std::vector<TCellId> ListActiveCellIds()
    {
        std::vector<TCellId> result;
        const auto& tamedCellManager = Bootstrap_->GetTamedCellManager();
        result.reserve(tamedCellManager->Cells().size());
        for (auto [cellId, cell] : tamedCellManager->Cells()) {
            if (IsShardActive(cell->GetShardIndex())) {
                result.push_back(cellId);
            }
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellHydraJanitorPtr CreateCellHydraJanitor(TBootstrap* bootstrap)
{
    return New<TCellHydraJanitor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
