#include "cell_hydra_janitor.h"

#include "tamed_cell_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/incumbent_server/incumbent_detail.h>
#include <yt/yt/server/master/incumbent_server/incumbent_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/cellar_agent/helpers.h>

#include <yt/yt/server/lib/hydra/hydra_janitor_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/cellar_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/ypath/helpers.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NCellServer {

using namespace NConcurrency;
using namespace NCellarAgent;
using namespace NCypressClient;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NHydra;
using namespace NIncumbentClient;
using namespace NIncumbentServer;
using namespace NCellMaster;
using namespace NCellarClient;
using namespace NObjectClient;
using namespace NTabletServer;
using namespace NApi;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CellServerLogger;

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
        , DynamicConfig_(New<TDynamicTabletManagerConfig>())
    { }

    void Initialize() override
    {
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        hydraManager->SubscribeStartLeading(BIND_NO_PROPAGATE(&TCellHydraJanitor::OnStartEpoch, MakeWeak(this)));
        hydraManager->SubscribeStopLeading(BIND_NO_PROPAGATE(&TCellHydraJanitor::OnStopEpoch, MakeWeak(this)));
        hydraManager->SubscribeStartFollowing(BIND_NO_PROPAGATE(&TCellHydraJanitor::OnStartEpoch, MakeWeak(this)));
        hydraManager->SubscribeStopFollowing(BIND_NO_PROPAGATE(&TCellHydraJanitor::OnStopEpoch, MakeWeak(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(
            BIND_NO_PROPAGATE(&TCellHydraJanitor::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            const auto& incumbentManager = Bootstrap_->GetIncumbentManager();
            incumbentManager->RegisterIncumbent(this);
        }
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    TAtomicIntrusivePtr<TDynamicTabletManagerConfig> DynamicConfig_;

    TPeriodicExecutorPtr PeriodicExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    struct TPeerInfo
    {
        TCellId CellId;
        std::optional<int> PeerId;
    };

    struct TListResult
    {
        IListNodePtr Primary;
        IListNodePtr Secondary;
    };

    struct TPeerCleanupInfo
    {
        TPeerInfo Peer;
        TListResult Snapshots;
        TListResult Changelogs;
        int ThresholdId = 0;
    };

    void OnStartEpoch()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(!PeriodicExecutor_);
        PeriodicExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletCellJanitor),
            BIND(&TCellHydraJanitor::OnCleanup, MakeWeak(this)));
        PeriodicExecutor_->Start();
    }

    void OnStopEpoch()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (PeriodicExecutor_) {
            YT_UNUSED_FUTURE(PeriodicExecutor_->Stop());
            PeriodicExecutor_.Reset();
        }
    }

    TDynamicTabletManagerConfigPtr GetDynamicConfig()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return DynamicConfig_.Acquire();
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& newConfig = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
        DynamicConfig_.Store(newConfig);

        PeriodicExecutor_->SetPeriod(newConfig->TabletCellsCleanupPeriod);
    }

    // COMPAT(danilalexeev)
    // Returns primary and secondary persistence storage paths for a given peer.
    std::pair<TString, TString> GetPeerPersistencePaths(TPeerInfo peer)
    {
        if (peer.PeerId) {
            return std::pair(
                Format("%v/%v", GetCellHydraPersistencePath(peer.CellId), *peer.PeerId),
                Format("%v/%v", GetCellPath(peer.CellId), *peer.PeerId));
        } else {
            return std::pair(
                GetCellHydraPersistencePath(peer.CellId),
                GetCellPath(peer.CellId));
        }
    }

    std::vector<TPeerCleanupInfo> GetCleanupInfoForPeers(
        const std::vector<TPeerInfo>& peers,
        TEnumIndexedArray<ECellarType, bool> checkSecondaryStorage)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto proxy = CreateObjectServiceReadProxy(
            Bootstrap_->GetRootClient(),
            EMasterChannelKind::Follower);

        // Batch request contains four requests for each cell:
        // - primary path -> snapshots
        // - secondary path -> snapshots
        // - primary path -> changelogs
        // - secondary path -> changelogs
        auto batchReq = proxy.ExecuteBatch();

        auto addListRequest = [&] (const TYPath& path) {
            auto req = TYPathProxy::List(path);
            ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                "compressed_data_size"
            });
            batchReq->AddRequest(req);
        };

        for (auto peer : peers) {
            auto [primaryPath, secondaryPath] = GetPeerPersistencePaths(peer);
            for (const char* fileType : {"snapshots", "changelogs"}) {
                addListRequest(YPathJoin(primaryPath, fileType));
                addListRequest(YPathJoin(secondaryPath, fileType));
            }
        }

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();

        auto getMergedHydraFiles = [&] (
            const TListResult& listResult,
            TPeerInfo peer) -> std::optional<std::vector<THydraFileInfo>>
        {
            std::vector<THydraFileInfo> result;
            auto tryParseHydraFiles = [&] (const IListNodePtr& list, const TYPath& path) {
                if (!list) {
                    return true;
                }
                auto children = list->GetChildren();
                result.reserve(result.size() + children.size());
                for (const auto& child : children) {
                    auto key = ConvertTo<std::string>(child);
                    int id = 0;
                    if (!TryFromString<int>(key, id)) {
                        YT_LOG_WARNING("Janitor has found a broken Hydra file (Path: %v, Key: %v)",
                            path,
                            key);
                        return false;
                    }

                    const auto& attributes = child->Attributes();
                    result.push_back({id, attributes.Get<i64>("compressed_data_size")});
                }
                return true;
            };

            auto [primaryPath, secondaryPath] = GetPeerPersistencePaths(peer);
            if (!tryParseHydraFiles(listResult.Primary, primaryPath) ||
                !tryParseHydraFiles(listResult.Secondary, secondaryPath))
            {
                return std::nullopt;
            }

            return result;
        };

        auto getListResult = [&] (
            int primarySubrequestIndex,
            int secondarySubrequestIndex,
            TPeerInfo peer) -> std::optional<TListResult>
        {
            auto getListResponse = [&] (int subrequestIndex) -> IListNodePtr {
                auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspList>(subrequestIndex);
                if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return nullptr;
                }
                return ConvertTo<IListNodePtr>(TYsonString(rspOrError.ValueOrThrow()->value()));
            };

            auto primaryList = getListResponse(primarySubrequestIndex);
            auto secondaryList = checkSecondaryStorage[GetCellarTypeFromCellId(peer.CellId)]
                ? getListResponse(secondarySubrequestIndex)
                : nullptr;

            if (!primaryList && !secondaryList) {
                YT_LOG_WARNING("Missing both storages for cell (CellId: %v)",
                    peer.CellId);
                return std::nullopt;
            }

            return TListResult{std::move(primaryList), std::move(secondaryList)};
        };

        std::vector<TPeerCleanupInfo> result;
        for (auto [index, peer] : Enumerate(peers)) {
            auto snapshots = getListResult(
                /*primarySubrequestIndex*/ 4 * index + 0,
                /*secondarySubrequestIndex*/ 4 * index + 1,
                peer);
            auto changelogs = getListResult(
                /*primarySubrequestIndex*/ 4 * index + 2,
                /*secondarySubrequestIndex*/ 4 * index + 3,
                peer);
            if (!snapshots || !changelogs) {
                continue;
            }

            auto snapshotFiles = getMergedHydraFiles(*snapshots, peer);
            auto changelogFiles = getMergedHydraFiles(*changelogs, peer);
            if (!snapshotFiles || !changelogFiles) {
                continue;
            }

            auto thresholdId = ComputeJanitorThresholdId(
                *snapshotFiles,
                *changelogFiles,
                GetDynamicConfig());

            result.push_back(TPeerCleanupInfo{
                .Peer = peer,
                .Snapshots = std::move(*snapshots),
                .Changelogs = std::move(*changelogs),
                .ThresholdId = thresholdId,
            });
        }

        return result;
    }

    // COMPAT(danilalexeev): Purge `secondaryPath`.
    void CleanPeersPersistence(
        const std::vector<TPeerInfo>& peers,
        TEnumIndexedArray<ECellarType, bool> checkSecondaryStorage,
        int* snapshotBudget,
        int* changelogBudget)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto cleanupInfo = GetCleanupInfoForPeers(peers, checkSecondaryStorage);

        auto proxy = CreateObjectServiceWriteProxy(
            Bootstrap_->GetRootClient());

        auto batchReq = proxy.ExecuteBatch();

        auto removeHydraPersistence = [&] (
            const IListNodePtr& list,
            const TYPath& path,
            int thresholdId,
            int* budget)
        {
            if (!list) {
                return;
            }

            auto children = list->GetChildren();
            for (const auto& child : children) {
                if (*budget <= 0) {
                    break;
                }

                auto key = ConvertTo<TString>(child);
                int id = 0;
                if (!TryFromString<int>(key, id)) {
                    YT_LOG_WARNING("Janitor has found a broken Hydra file (Path: %v, Key: %v)",
                        path,
                        key);
                    continue;
                }

                if (id >= thresholdId) {
                    continue;
                }

                auto filePath = path + "/" + ToYPathLiteral(key);
                YT_LOG_DEBUG("Janitor is removing Hydra file (Path: %v)", filePath);

                --(*budget);

                auto removeReq = TYPathProxy::Remove(filePath);
                removeReq->Tag() = filePath;
                batchReq->AddRequest(removeReq);
            }
        };

        for (const auto& info : cleanupInfo) {
            auto [primaryPath, secondaryPath] = GetPeerPersistencePaths(info.Peer);
            removeHydraPersistence(info.Snapshots.Primary, primaryPath + "/snapshots", info.ThresholdId, snapshotBudget);
            removeHydraPersistence(info.Changelogs.Primary, primaryPath + "/changelogs", info.ThresholdId, changelogBudget);
            removeHydraPersistence(info.Snapshots.Secondary, secondaryPath + + "/snapshots", info.ThresholdId, snapshotBudget);
            removeHydraPersistence(info.Changelogs.Secondary, secondaryPath + "/changelogs", info.ThresholdId, changelogBudget);
        }

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();

        for (const auto& [tag, rspOrError] : batchRsp->GetTaggedResponses<TYPathProxy::TRspRemove>()) {
            auto filePath = std::any_cast<TString>(tag);
            if (rspOrError.IsOK()) {
                YT_LOG_DEBUG("Janitor has successfully removed Hydra file (Path: %v)", filePath);
            } else {
                YT_LOG_WARNING(rspOrError, "Janitor has failed to remove Hydra file (Path: %v)", filePath);
            }
        }
    }

    void OnCleanup()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        if (GetActiveShardCount() == 0) {
            return;
        }

        YT_LOG_DEBUG("Cell janitor cleanup started (ActiveShardIndices: %v)",
            ListActiveShardIndices());

        int snapshotBudget = GetDynamicConfig()->MaxSnapshotCountToRemovePerCheck;
        int changelogBudget = GetDynamicConfig()->MaxChangelogCountToRemovePerCheck;

        // COMPAT(danilalexeev)
        TEnumIndexedArray<ECellarType, bool> isLegacyCellMap;
        try {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            auto tabletCellMapNode = cypressManager->ResolvePathToNodeProxy(TabletCellCypressPrefix);
            auto chaosCellMapNode = cypressManager->ResolvePathToNodeProxy(ChaosCellCypressPrefix);
            isLegacyCellMap[ECellarType::Tablet] = tabletCellMapNode->GetTrunkNode()->GetType() == EObjectType::TabletCellMap;
            isLegacyCellMap[ECellarType::Chaos] = chaosCellMapNode->GetTrunkNode()->GetType() == EObjectType::ChaosCellMap;
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_WARNING(ex,
                    "Cell Cypress map node is missing");
                return;
            }
            throw;
        }

        const auto& tamedCellManager = Bootstrap_->GetTamedCellManager();
        std::vector<TPeerInfo> peersToCleanup;
        auto cellIds = ListActiveCellIds();
        for (auto cellId : cellIds) {
            auto* cell = tamedCellManager->FindCell(cellId);
            if (!IsObjectAlive(cell)) {
                continue;
            }

            if (cell->CellBundle()->GetOptions()->IndependentPeers) {
                for (int peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
                    if (cell->IsAlienPeer(peerId)) {
                        continue;
                    }
                    peersToCleanup.push_back(TPeerInfo{
                        .CellId = cellId,
                        .PeerId = peerId,
                    });
                }
            } else {
                peersToCleanup.push_back(TPeerInfo{
                    .CellId = cellId,
                });
            }
        }

        auto result = WaitFor(BIND(&TCellHydraJanitor::CleanPeersPersistence, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
            .Run(peersToCleanup, isLegacyCellMap, &snapshotBudget, &changelogBudget));

        if (!result.IsOK()) {
            YT_LOG_WARNING(result, "Cell janitor cleanup failed");
            return;
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
