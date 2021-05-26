#include "cell_hydra_janitor.h"
#include "tamed_cell_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/hydra/hydra_janitor_helpers.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NCellServer {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NHydra;
using namespace NCellMaster;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellHydraJanitor::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Initialize()
    {
        Bootstrap_->GetConfigManager()->SubscribeConfigChanged(
            BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
        Bootstrap_->GetHydraFacade()->GetHydraManager()->SubscribeLeaderActive(
            BIND(&TImpl::OnLeaderActive, MakeWeak(this)));
        Bootstrap_->GetHydraFacade()->GetHydraManager()->SubscribeStopLeading(
            BIND(&TImpl::OnStopLeading, MakeWeak(this)));
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    TPeriodicExecutorPtr PeriodicExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void OnLeaderActive()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsPrimaryMaster()) {
            return;
        }

        PeriodicExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletCellJanitor),
            BIND(&TImpl::OnCleanup, MakeWeak(this)));
        PeriodicExecutor_->Start();

        OnDynamicConfigChanged();
    }

    void OnStopLeading()
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

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& config = GetDynamicConfig();

        if (PeriodicExecutor_) {
            PeriodicExecutor_->SetPeriod(config->TabletCellsCleanupPeriod);
        }
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
                .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TYPathProxy::TErrorOrRspRemovePtr& removeRspOrError) {
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

        int snapshotBudget = GetDynamicConfig()->MaxSnapshotCountToRemovePerCheck;
        int changelogBudget = GetDynamicConfig()->MaxChangelogCountToRemovePerCheck;

        const auto& tamedCellManager = Bootstrap_->GetTamedCellManager();
        auto cellIds = GetKeys(tamedCellManager->Cells());
        for (auto cellId : cellIds) {
            auto* cell = tamedCellManager->FindCell(cellId);

            if (!IsObjectAlive(cell)) {
                continue;
            }

            try {
                if (cell->GetCellBundle()->GetOptions()->IndependentPeers) {
                    for (int peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
                        if (!cell->IsAlienPeer(peerId)) {
                            auto path = Format("//sys/tablet_cells/%v/%v", ToYPathLiteral(ToString(cellId)), peerId);
                            CleanSnapshotsAndChangelogs(cellId, path, &snapshotBudget, &changelogBudget);
                        }
                    }
                } else {
                    auto path = Format("//sys/tablet_cells/%v", ToYPathLiteral(ToString(cellId)));
                    CleanSnapshotsAndChangelogs(cellId, path, &snapshotBudget, &changelogBudget);
                }
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Janitor cleanup failed (CellId: %v)", cellId);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TCellHydraJanitor::TCellHydraJanitor(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TCellHydraJanitor::~TCellHydraJanitor()
{ }

void TCellHydraJanitor::Initialize()
{
    Impl_->Initialize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
