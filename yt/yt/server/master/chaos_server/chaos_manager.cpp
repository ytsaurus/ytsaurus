#include "chaos_manager.h"

#include "alien_cell.h"
#include "alien_cell_synchronizer.h"
#include "alien_cluster_registry.h"
#include "config.h"
#include "chaos_cell.h"
#include "chaos_cell_type_handler.h"
#include "chaos_cell_bundle.h"
#include "chaos_cell_bundle_type_handler.h"
#include "chaos_replicated_table_node.h"
#include "chaos_replicated_table_node_type_handler.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/chaos_server/proto/chaos_manager.pb.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/server/lib/hive/helpers.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/ytlib/object_client/public.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NCellServer;
using namespace NTransactionServer;
using namespace NCypressServer;
using namespace NCellarClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NChaosClient;
using namespace NTableClient;
using namespace NCypressClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

const auto static& Logger = ChaosServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChaosManager
    : public IChaosManager
    , public TMasterAutomatonPart
{
public:
    explicit TChaosManager(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::ChaosManager)
        , AlienClusterRegistry_(New<TAlienClusterRegistry>())
        , AlienCellSynchronizer_(CreateAlienCellSynchronizer(bootstrap))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);

        RegisterLoader(
            "ChaosManager.Keys",
            BIND(&TChaosManager::LoadKeys, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "ChaosManager.Keys",
            BIND(&TChaosManager::SaveKeys, Unretained(this)));

        RegisterMethod(BIND(&TChaosManager::HydraUpdateAlienCellPeers, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TChaosManager::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateChaosCellBundleTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateChaosCellTypeHandler(Bootstrap_));

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->RegisterHandler(CreateChaosReplicatedTableTypeHandler(Bootstrap_));

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->SubscribeCellCreated(BIND(&TChaosManager::OnCellCreated, MakeWeak(this)));
        cellManager->SubscribeCellDecommissionStarted(BIND(&TChaosManager::OnCellDecommissionStarted, MakeWeak(this)));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TChaosManager::HydraPrepareCreateReplicationCard, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TChaosManager::HydraCommitCreateReplicationCard, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TChaosManager::HydraAbortCreateReplicationCard, MakeStrong(this))));
    }

    const TAlienClusterRegistryPtr& GetAlienClusterRegistry() const override
    {
        return AlienClusterRegistry_;
    }

    TChaosCell* FindChaosCellById(TChaosCellId cellId) const override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cell = cellManager->FindCell(cellId);
        return IsObjectAlive(cell) && cell->GetType() == EObjectType::ChaosCell ? cell->As<TChaosCell>() : nullptr;
    }

    TChaosCell* GetChaosCellByIdOrThrow(TChaosCellId cellId) const override
    {
        auto* cell = FindChaosCellById(cellId);
        if (!IsObjectAlive(cell)) {
            THROW_ERROR_EXCEPTION("No chaos cell with id %v is known", cellId);
        }
        return cell;
    }

    TChaosCellBundle* FindChaosCellBundle(TChaosCellBundleId id) override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* bundle = cellManager->FindCellBundle(id);
        if (!bundle) {
            return nullptr;
        }
        return bundle->GetType() == EObjectType::ChaosCellBundle
            ? bundle->As<TChaosCellBundle>()
            : nullptr;
    }

    TChaosCellBundle* GetChaosCellBundleOrThrow(TChaosCellBundleId id) override
    {
        auto* bundle = FindChaosCellBundle(id);
        if (!bundle) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such chaos cell bundle %v",
                id);
        }
        return bundle;
    }

    TChaosCellBundle* GetChaosCellBundleByNameOrThrow(const TString& name, bool activeLifeStageOnly) const override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cellBundle = cellManager->GetCellBundleByNameOrThrow(name, ECellarType::Chaos, true);

        if (activeLifeStageOnly) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(cellBundle);
        }

        return cellBundle->As<TChaosCellBundle>();
    }

    virtual TChaosCell* FindChaosCellByTag(TCellTag cellTag) const override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cell = cellManager->FindCellByCellTag(cellTag);
        return IsObjectAlive(cell) && cell->GetType() == EObjectType::ChaosCell ? cell->As<TChaosCell>() : nullptr;
    }

    virtual TChaosCell* GetChaosCellByTagOrThrow(TCellTag cellTag) const override
    {
        auto* cell = FindChaosCellByTag(cellTag);
        if (!IsObjectAlive(cell)) {
            THROW_ERROR_EXCEPTION("No chaos cell with tag %v is known", cellTag);
        }
        return cell;
    }

    void SetChaosCellBundle(TChaosReplicatedTableNode* node, TChaosCellBundle* cellBundle) override
    {
        YT_VERIFY(node->IsTrunk());
        YT_VERIFY(cellBundle);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(cellBundle, NYTree::EPermission::Use);

        node->ChaosCellBundle().Assign(cellBundle);
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const TAlienClusterRegistryPtr AlienClusterRegistry_;
    const IAlienCellSynchronizerPtr AlienCellSynchronizer_;


    void OnCellCreated(TCellBase* cellBase)
    {
        if (cellBase->GetType() != EObjectType::ChaosCell){
            return;
        }

        auto* cell = cellBase->As<TChaosCell>();
        const auto& chaosOptions = cell->GetCellBundle()->As<TChaosCellBundle>()->GetChaosOptions();
        THashSet<int> alienClusterIndexes;

        for (int peerId = 0; peerId < std::ssize(chaosOptions->Peers); ++peerId) {
            if (cell->IsAlienPeer(peerId)) {
                auto alienClusterIndex = AlienClusterRegistry_->GetOrRegisterAlienClusterIndex(*chaosOptions->Peers[peerId]->AlienCluster);
                alienClusterIndexes.insert(alienClusterIndex);
            }
        }
        for (auto alienClusterIndex : alienClusterIndexes) {
            cell->SetAlienConfigVersion(alienClusterIndex, 0);
        }
    }

    void OnCellDecommissionStarted(TCellBase* cellBase)
    {
        if (cellBase->GetType() != EObjectType::ChaosCell){
            return;
        }
        if (!cellBase->IsDecommissionStarted()) {
            return;
        }
        cellBase->GossipStatus().Local().Decommissioned = true;
    }

    void HydraUpdateAlienCellPeers(NProto::TReqUpdateAlienCellPeers* request)
    {
        auto constellations = FromProto<std::vector<TAlienCellConstellation>>(request->constellations());
        auto fullSync = request->full_sync();

        for (const auto& [alienClusterIndex, alienCells, lostAlienCellIds] : constellations) {
            for (const auto& alienCell : alienCells) {
                auto* cell = FindChaosCellById(alienCell.CellId);
                if (!IsObjectAlive(cell)) {
                    continue;
                }
                if (!fullSync && cell->GetAlienConfigVersion(alienClusterIndex) >= alienCell.ConfigVersion) {
                    continue;
                }

                for (const auto& alienPeer : alienCell.AlienPeers) {
                    if (alienPeer.PeerId < 0 || alienPeer.PeerId >= std::ssize(cell->Peers())) {
                        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Trying to update alien peer with invalid peer id (ChaosCellId: %v, PeerId: %v, AlienCluster: %v)",
                            cell->GetId(),
                            alienPeer.PeerId,
                            AlienClusterRegistry_->GetAlienClusterName(alienClusterIndex));
                        continue;
                    }

                    if (!cell->IsAlienPeer(alienPeer.PeerId)) {
                        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Trying to update local peer as alien, ignored (ChaosCellId: %v, PeerId: %v, AlienCluster: %v)",
                            cell->GetId(),
                            alienPeer.PeerId,
                            AlienClusterRegistry_->GetAlienClusterName(alienClusterIndex));
                        continue;
                    }

                    cell->UpdateAlienPeer(alienPeer.PeerId, alienPeer.NodeDescriptor);

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Updated alien peer config (ChaosCellId: %v, "
                        "AlienCluster: %v, AlienConfigVersion: %v, PeerAddress: %v)",
                        cell->GetId(),
                        AlienClusterRegistry_->GetAlienClusterName(alienClusterIndex),
                        alienCell.ConfigVersion,
                        alienPeer.NodeDescriptor.GetDefaultAddress());
                }

                cell->SetAlienConfigVersion(alienClusterIndex, alienCell.ConfigVersion);
            }

            if (fullSync) {
                for (auto cellId : lostAlienCellIds) {
                    auto* cell = FindChaosCellById(cellId);
                    if (!IsObjectAlive(cell) || cell->GetAlienConfigVersion(alienClusterIndex) == 0) {
                        continue;
                    }

                    const auto& options = cell->GetChaosCellBundle()->GetChaosOptions();
                    for (int peerId = 0; peerId < std::ssize(options->Peers); ++peerId) { 
                        const auto& alienCluster = options->Peers[peerId]->AlienCluster;
                        if (alienCluster && alienCluster == AlienClusterRegistry_->GetAlienClusterName(alienClusterIndex)) {
                            cell->UpdateAlienPeer(peerId, {});
                        }
                    }

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Updated alien peer config for lost peers (ChaosCellId: %v, AlienCluster: %v)",
                        cellId,
                        AlienClusterRegistry_->GetAlienClusterName(alienClusterIndex));

                    cell->SetAlienConfigVersion(alienClusterIndex, 0);
                }
            }
        }
    }

    void HydraPrepareCreateReplicationCard(
        TTransaction* /*transaction*/,
        NChaosClient::NProto::TReqCreateReplicationCard* /*request*/,
        bool /*persistent*/)
    { }

    void HydraCommitCreateReplicationCard(
        TTransaction* /*transaction*/,
        NChaosClient::NProto::TReqCreateReplicationCard* request)
    {
        auto replicationCardId = FromProto<TReplicationCardId>(request->hint_id());
        auto tableId = FromProto<TTableId>(request->table_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkTable = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId));
        if (trunkTable->GetType() != EObjectType::ChaosReplicatedTable) {
            THROW_ERROR_EXCEPTION("Chaos replicated table %v has invalid type: expected %Qlv, actual %Qlv",
                tableId,
                EObjectType::ChaosReplicatedTable,
                trunkTable->GetType());
            return;
        }

        auto updateNode = [&] (TTransaction* transaction) {
            if (auto* node = cypressManager->FindNode(trunkTable, transaction)) {
                auto* tableNode = node->As<TChaosReplicatedTableNode>();
                tableNode->SetReplicationCardId(replicationCardId);
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Replication card assigned to chaos replicated table (TableId: %v, ReplicationCardId: %v)",
                    TVersionedNodeId(tableId, GetObjectId(transaction)),
                    replicationCardId);
            }
        };

        updateNode(nullptr);

        const auto& lockingState = trunkTable->LockingState();
        for (auto* lock : lockingState.AcquiredLocks) {
            updateNode(lock->GetTransaction());
        }
    }

    void HydraAbortCreateReplicationCard(
        TTransaction* /*transaction*/,
        NChaosClient::NProto::TReqCreateReplicationCard* /*request*/)
    { }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        Save(context, *AlienClusterRegistry_);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        Load(context, *AlienClusterRegistry_);
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        AlienClusterRegistry_->Clear();
    }


    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        OnDynamicConfigChanged();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            AlienCellSynchronizer_->Start();
        }
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            AlienCellSynchronizer_->Stop();
        }
    }


    const TDynamicChaosManagerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->ChaosManager;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        const auto& config = GetDynamicConfig();
        AlienCellSynchronizer_->Reconfigure(config->AlienCellSynchronizer);
    }
};

////////////////////////////////////////////////////////////////////////////////

IChaosManagerPtr CreateChaosManager(NCellMaster::TBootstrap* bootstrap)
{
    return New<TChaosManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
