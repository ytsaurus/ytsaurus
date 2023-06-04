#include "chaos_service.h"

#include "alien_cell.h"
#include "chaos_cell.h"
#include "chaos_cell_bundle.h"
#include "chaos_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/chaos_server/config.h>

#include <yt/yt/ytlib/chaos_client/chaos_master_service_proxy.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NCellarClient;
using namespace NChaosClient;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TChaosService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TChaosMasterServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::ChaosService,
            ChaosServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SyncAlienCells)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChaosService))
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetCellDescriptorsByCellBundle)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChaosService))
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetCellDescriptors)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChaosService))
            .SetHeavy(true));
        // COMPAT(savrus)
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FindCellDescriptorsByCellTags)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChaosService))
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, SyncAlienCells)
    {
        auto requestDescriptors = FromProto<std::vector<TAlienCellDescriptorLite>>(request->cell_descriptors());
        auto fullSync = request->full_sync();

        context->SetRequestInfo("CellCount: %v, FullSync: %v",
            requestDescriptors.size(),
            fullSync);

        const auto& chaosManager = Bootstrap_->GetChaosManager();
        std::vector<TAlienCellDescriptor> responseDescriptors;

        for (auto [cellId, configVersion] : requestDescriptors) {
            auto cell = chaosManager->FindChaosCellById(cellId);
            if (!IsObjectAlive(cell)) {
                continue;
            }
            if (!fullSync && cell->GetConfigVersion() <= configVersion) {
                continue;
            }

            TAlienCellDescriptor descriptor;
            descriptor.CellId = cellId;
            descriptor.ConfigVersion = cell->GetConfigVersion();

            for (TPeerId peerId = 0; peerId < std::ssize(cell->Peers()); ++peerId) {
                if (!cell->IsAlienPeer(peerId)) {
                    descriptor.AlienPeers.push_back({
                        .PeerId = peerId,
                        .NodeDescriptor = cell->Peers()[peerId].Descriptor
                    });
                }
            }

            responseDescriptors.push_back(std::move(descriptor));
        }

        ToProto(response->mutable_cell_descriptors(), responseDescriptors);

        const auto& chaosConfig = Bootstrap_->GetConfigManager()->GetConfig()->ChaosManager;
        response->set_enable_metadata_cells(chaosConfig->EnableMetadataCells);

        context->SetResponseInfo("CellCount: %v EnableMetadataCells: %v",
            response->cell_descriptors_size(),
            response->enable_metadata_cells());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetCellDescriptorsByCellBundle)
    {
        const auto& cellBundleName = request->cell_bundle();

        context->SetRequestInfo("CellBundle: %v",
            cellBundleName);

        const auto& chaosManager = Bootstrap_->GetChaosManager();
        auto* cellBundle = chaosManager->GetChaosCellBundleByNameOrThrow(cellBundleName, /*activeLifeStageOnly*/ true);
        for (const auto* cell : cellBundle->Cells()) {
            ToProto(response->add_cell_descriptors(), cell->GetDescriptor());
        }

        context->SetResponseInfo("CellCount: %v",
            response->cell_descriptors_size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetCellDescriptors)
    {
        context->SetRequestInfo();

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        for (const auto* cell : cellManager->Cells(ECellarType::Chaos)) {
            ToProto(response->add_cell_descriptors(), cell->GetDescriptor());
        }

        context->SetResponseInfo("CellCount: %v",
            response->cell_descriptors_size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, FindCellDescriptorsByCellTags)
    {
        auto cellTags = FromProto<std::vector<TCellTag>>(request->cell_tags());

        context->SetRequestInfo("CellTags: %v",
            cellTags);

        const auto& chaosManager = Bootstrap_->GetChaosManager();
        for (auto cellTag : cellTags) {
            if (auto* cell = chaosManager->FindChaosCellByTag(cellTag)) {
                ToProto(response->add_cell_descriptors(), cell->GetDescriptor());
            } else {
                ToProto(response->add_cell_descriptors(), TCellDescriptor());
            }
        }

        context->Reply();
    }
};

IServicePtr CreateMasterChaosService(TBootstrap* bootstrap)
{
    return New<TChaosService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
