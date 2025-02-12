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

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/client/chaos_client/helpers.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NCellServer;
using namespace NTransactionServer;
using namespace NCypressServer;
using namespace NCellarClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NChaosClient;
using namespace NTableClient;
using namespace NCypressClient;

using NYT::FromProto;
using NYT::ToProto;

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
        YT_ASSERT_INVOKER_THREAD_AFFINITY(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);

        RegisterLoader(
            "ChaosManager.Keys",
            BIND(&TChaosManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "ChaosManager.Values",
            BIND(&TChaosManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "ChaosManager.Keys",
            BIND(&TChaosManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ChaosManager.Values",
            BIND(&TChaosManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraUpdateAlienCellPeers, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TChaosManager::HydraReplicateAlienClusterRegistry, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TChaosManager::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateChaosCellBundleTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateChaosCellTypeHandler(Bootstrap_));

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->RegisterHandler(CreateChaosReplicatedTableTypeHandler(Bootstrap_));

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->SubscribeCellCreated(BIND_NO_PROPAGATE(&TChaosManager::OnCellCreated, MakeWeak(this)));
        cellManager->SubscribeCellDecommissionStarted(BIND_NO_PROPAGATE(&TChaosManager::OnCellDecommissionStarted, MakeWeak(this)));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers<NChaosClient::NProto::TReqCreateReplicationCard>({
            .Prepare = BIND_NO_PROPAGATE(&TChaosManager::HydraPrepareCreateReplicationCard, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TChaosManager::HydraCommitCreateReplicationCard, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TChaosManager::HydraAbortCreateReplicationCard, Unretained(this)),
        });
    }

    void ReplicateAlienClusterRegistryToSecondaryMaster(TCellTag cellTag) const override
    {
        NProto::TReqReplicateAlienClusterRegistry req;
        ToProto(req.mutable_clusters(), AlienClusterRegistry_->GetIndexToName());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(req, cellTag);
    }

    const TAlienClusterRegistryPtr& GetAlienClusterRegistry() const override
    {
        return AlienClusterRegistry_;
    }

    TChaosCell* GetBundleMetadataCell(const TChaosCellBundle* cellBundle) const override
    {
        YT_ASSERT(cellBundle->MetadataCells().size() <= 2);

        for (auto metadataCell : cellBundle->MetadataCells()) {
            if (IsMetadataCellInEnabledCluster(metadataCell)) {
                return metadataCell;
            }
        }

        return nullptr;
    }

    void SetBundleMetadataCells(TChaosCellBundle* cellBundle, const std::vector<TChaosCellId>& metadataCellIds) const override
    {
        if (metadataCellIds.size() > 2) {
            THROW_ERROR_EXCEPTION("Expected 2 or less metadata cells ids, got %v",
                metadataCellIds.size());
        }
        if (metadataCellIds.size() == 2 &&
            GetSiblingChaosCellTag(CellTagFromId(metadataCellIds[0])) != CellTagFromId(metadataCellIds[1]))
        {
            THROW_ERROR_EXCEPTION("Metadata cells should be siblings");
        }

        std::vector<TChaosCellRawPtr> metadataCells;
        for (auto cellId : metadataCellIds) {
            auto* cell = GetChaosCellByIdOrThrow(cellId);
            if (cell->CellBundle() != cellBundle) {
                THROW_ERROR_EXCEPTION("Cell %v belongs to a different bundle %Qv",
                    cellId,
                    cell->CellBundle()->GetName());
            }

            metadataCells.push_back(cell);
        }

        cellBundle->MetadataCells() = std::move(metadataCells);
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

    TChaosCellBundle* GetChaosCellBundleOrThrow(TChaosCellBundleId id, bool activeLifeStageOnly) override
    {
        auto* bundle = FindChaosCellBundle(id);
        if (!bundle) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such chaos cell bundle %v",
                id);
        }

        if (activeLifeStageOnly) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(bundle);
        }

        return bundle;
    }

    TChaosCellBundle* GetChaosCellBundleByNameOrThrow(const std::string& name, bool activeLifeStageOnly) const override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cellBundle = cellManager->GetCellBundleByNameOrThrow(name, ECellarType::Chaos, true);

        if (activeLifeStageOnly) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(cellBundle);
        }

        return cellBundle->As<TChaosCellBundle>();
    }

    TChaosCell* FindChaosCellByTag(TCellTag cellTag) const override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cell = cellManager->FindCellByCellTag(cellTag);
        return IsObjectAlive(cell) && cell->GetType() == EObjectType::ChaosCell ? cell->As<TChaosCell>() : nullptr;
    }

    TChaosCell* GetChaosCellByTagOrThrow(TCellTag cellTag) const override
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
    THashSet<std::string> EnabledMetadataClusters_;

    //! Contains native trunk nodes for which IsQueue() is true.
    THashSet<TChaosReplicatedTableNodeRawPtr> Queues_;
    //! Contains native trunk nodes for which IsQueueConsumer() is true.
    THashSet<TChaosReplicatedTableNodeRawPtr> Consumers_;
    //! Contains native trunk nodes for which IsQueueProducer() is true.
    THashSet<TChaosReplicatedTableNodeRawPtr> Producers_;

    const THashSet<TChaosReplicatedTableNodeRawPtr>& GetQueues() const override
    {
        VerifyPersistentStateRead();

        return Queues_;
    }

    const THashSet<TChaosReplicatedTableNodeRawPtr>& GetQueueConsumers() const override
    {
        VerifyPersistentStateRead();

        return Consumers_;
    }

    const THashSet<TChaosReplicatedTableNodeRawPtr>& GetQueueProducers() const override
    {
        VerifyPersistentStateRead();

        return Producers_;
    }

    void RegisterQueue(TChaosReplicatedTableNode* node) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Queues_.insert(node).second) {
            YT_LOG_ALERT("Attempting to register a queue twice (Node: %v, Path: %v)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    void UnregisterQueue(TChaosReplicatedTableNode* node) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Queues_.erase(node)) {
            YT_LOG_ALERT("Attempting to unregister an unknown queue (Node: %v, Path: %v)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    void RegisterQueueConsumer(TChaosReplicatedTableNode* node) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Consumers_.insert(node).second) {
            YT_LOG_ALERT("Attempting to register a consumer twice (Node: %v, Path: %v)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    void UnregisterQueueConsumer(TChaosReplicatedTableNode* node) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Consumers_.erase(node)) {
            YT_LOG_ALERT("Attempting to unregister an unknown consumer (Node: %v, Path: %v)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    void RegisterQueueProducer(TChaosReplicatedTableNode* node) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Producers_.insert(node).second) {
            YT_LOG_ALERT("Attempting to register a producer twice (Node: %v, Path: %v)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    void UnregisterQueueProducer(TChaosReplicatedTableNode* node) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Producers_.erase(node)) {
            YT_LOG_ALERT("Attempting to unregister an unknown producer (Node: %v, Path: %v)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    bool IsMetadataCellInEnabledCluster(const TChaosCell* chaosCell) const
    {
        YT_VERIFY(chaosCell);

        if (chaosCell->GetDescriptor().Peers.empty()) {
            return false;
        }

        const auto& alienCluster = chaosCell->GetDescriptor().Peers[0].GetAlienCluster();

        if (!alienCluster) {
            return true;
        }

        return EnabledMetadataClusters_.contains(alienCluster.value());
    }

    void OnCellCreated(TCellBase* cellBase)
    {
        if (cellBase->GetType() != EObjectType::ChaosCell){
            return;
        }

        auto* cell = cellBase->As<TChaosCell>();
        const auto& chaosOptions = cell->GetChaosOptions();
        YT_VERIFY(chaosOptions);

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

        if (fullSync) {
            EnabledMetadataClusters_.clear();
        }

        for (const auto& [alienClusterIndex, alienCells, lostAlienCellIds, enableMetadataCells] : constellations) {
            const auto& clusterName = AlienClusterRegistry_->GetAlienClusterName(alienClusterIndex);
            if (enableMetadataCells) {
                EnabledMetadataClusters_.insert(clusterName);
            } else {
                EnabledMetadataClusters_.erase(clusterName);
            }

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
                        YT_LOG_ALERT("Trying to update alien peer with invalid peer id (ChaosCellId: %v, PeerId: %v, AlienCluster: %v)",
                            cell->GetId(),
                            alienPeer.PeerId,
                            clusterName);
                        continue;
                    }

                    if (!cell->IsAlienPeer(alienPeer.PeerId)) {
                        YT_LOG_ALERT("Trying to update local peer as alien, ignored (ChaosCellId: %v, PeerId: %v, AlienCluster: %v)",
                            cell->GetId(),
                            alienPeer.PeerId,
                            clusterName);
                        continue;
                    }

                    cell->UpdateAlienPeer(alienPeer.PeerId, alienPeer.NodeDescriptor);

                    YT_LOG_DEBUG("Updated alien peer config (ChaosCellId: %v, "
                        "AlienCluster: %v, AlienConfigVersion: %v, PeerAddress: %v)",
                        cell->GetId(),
                        clusterName,
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

                    const auto& options = cell->GetChaosOptions();
                    for (int peerId = 0; peerId < std::ssize(options->Peers); ++peerId) {
                        const auto& alienCluster = options->Peers[peerId]->AlienCluster;
                        if (alienCluster && alienCluster == clusterName) {
                            cell->UpdateAlienPeer(peerId, {});
                        }
                    }

                    YT_LOG_DEBUG("Updated alien peer config for lost peers (ChaosCellId: %v, AlienCluster: %v)",
                        cellId,
                        clusterName);

                    cell->SetAlienConfigVersion(alienClusterIndex, 0);
                }
            }
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(*request);
        }
    }

    void HydraReplicateAlienClusterRegistry(NProto::TReqReplicateAlienClusterRegistry* request)
    {
        auto indexToName = FromProto<std::vector<std::string>>(request->clusters());
        AlienClusterRegistry_->Reset(std::move(indexToName));

        YT_LOG_DEBUG("Alien cluster registry is reset (ActualClusters: %v)",
            AlienClusterRegistry_->GetIndexToName());
    }

    void HydraPrepareCreateReplicationCard(
        TTransaction* /*transaction*/,
        NChaosClient::NProto::TReqCreateReplicationCard* /*request*/,
        const NTransactionSupervisor::TTransactionPrepareOptions& /*options*/)
    { }

    void HydraCommitCreateReplicationCard(
        TTransaction* /*transaction*/,
        NChaosClient::NProto::TReqCreateReplicationCard* request,
        const NTransactionSupervisor::TTransactionCommitOptions& /*options*/)
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
                YT_LOG_DEBUG("Replication card assigned to chaos replicated table (TableId: %v, ReplicationCardId: %v)",
                    TVersionedNodeId(tableId, GetObjectId(transaction)),
                    replicationCardId);
            }
        };

        updateNode(nullptr);

        const auto& lockingState = trunkTable->LockingState();
        for (auto lock : lockingState.AcquiredLocks) {
            updateNode(lock->GetTransaction());
        }
    }

    void HydraAbortCreateReplicationCard(
        TTransaction* /*transaction*/,
        NChaosClient::NProto::TReqCreateReplicationCard* /*request*/,
        const NTransactionSupervisor::TTransactionAbortOptions& /*options*/)
    { }


    void SaveKeys(NCellMaster::TSaveContext& /*context*/) const
    { }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        Save(context, *AlienClusterRegistry_);
        Save(context, EnabledMetadataClusters_);

        Save(context, Queues_);
        Save(context, Consumers_);
        Save(context, Producers_);
    }

    void LoadKeys(NCellMaster::TLoadContext& /*context*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        Load(context, *AlienClusterRegistry_);

        // COMPAT(ponasenko-rs)
        Load(context, EnabledMetadataClusters_);

        Load(context, Queues_);
        Load(context, Consumers_);

        // COMPAT(apachee)
        // DropLegacyClusterNodeMap is the start of 24.2 reigns.
        if ((context.GetVersion() >= EMasterReign::QueueProducers_24_1 && context.GetVersion() < EMasterReign::DropLegacyClusterNodeMap) ||
            context.GetVersion() >= EMasterReign::QueueProducers)
        {
            Load(context, Producers_);
        }
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        const auto& config = GetDynamicConfig();
        AlienCellSynchronizer_->Reconfigure(config->AlienCellSynchronizer);
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        AlienClusterRegistry_->Clear();

        Queues_.clear();
        Consumers_.clear();
        Producers_.clear();
    }


    void OnLeaderActive() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            AlienCellSynchronizer_->Start();
        }
    }

    void OnStopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
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
