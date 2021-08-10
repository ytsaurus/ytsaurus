#include "chaos_manager.h"

#include "alien_cell.h"
#include "alien_cell_synchronizer.h"
#include "alien_cluster_registry.h"
#include "config.h"
#include "chaos_cell_bundle_type_handler.h"
#include "chaos_cell_type_handler.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/chaos_server/proto/chaos_manager.pb.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/ytlib/object_client/public.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NCellServer;
using namespace NHydra;
using namespace NObjectClient;

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
    }

    virtual void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TChaosManager::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateChaosCellBundleTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateChaosCellTypeHandler(Bootstrap_));

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->SubscribeCellCreated(BIND(&TChaosManager::OnCellCreated, MakeWeak(this)));
        cellManager->SubscribeCellDecommissionStarted(BIND(&TChaosManager::OnCellDecommissionStarted, MakeWeak(this)));

        RegisterMethod(BIND(&TChaosManager::HydraUpdateAlienCellPeers, Unretained(this)));
    }

    virtual const TAlienClusterRegistryPtr& GetAlienClusterRegistry() const override
    {
        return AlienClusterRegistry_;
    }

    virtual TChaosCell* FindChaosCell(TChaosCellId cellId) const override
    {
        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        auto* cell = cellManager->FindCell(cellId);
        if (!cell || cell->GetType() != EObjectType::ChaosCell) {
            return nullptr;
        }
        return cell->As<TChaosCell>();
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

        for (const auto& [alienClusterIndex, alienCells] : constellations) {
            for (const auto& alienCell : alienCells) {
                auto* cell = FindChaosCell(alienCell.CellId);
                if (!IsObjectAlive(cell) || cell->GetAlienConfigVersion(alienClusterIndex) >= alienCell.ConfigVersion) {
                    continue;
                }

                for (const auto& alienPeer : alienCell.AlienPeers) {
                    if (!cell->IsAlienPeer(alienPeer.PeerId)) {
                        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Trying to update local peer as alien, ignored (ChaosCellId: %v, PeerId: %v, AlienCluster: %v)",
                            cell->GetId(),
                            alienPeer.PeerId,
                            AlienClusterRegistry_->GetAlienClusterName(alienClusterIndex));
                        continue;
                    }

                    if (alienPeer.PeerId < 0 || alienPeer.PeerId >= std::ssize(cell->Peers())) {
                        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Trying to update alien peer with invalid peer id (ChaosCellId: %v, PeerId: %v, AlienCluster: %v)",
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
        }
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        Save(context, *AlienClusterRegistry_);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        Load(context, *AlienClusterRegistry_);
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        AlienClusterRegistry_->Clear();
    }

    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        OnDynamicConfigChanged();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            AlienCellSynchronizer_->Start();
        }
    }

    virtual void OnStopLeading() override
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
