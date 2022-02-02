#include "chaos_service.h"

#include "alien_cell.h"
#include "chaos_cell.h"
#include "chaos_cell_bundle.h"
#include "chaos_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/ytlib/chaos_client/chaos_master_service_proxy.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;
using namespace NChaosClient;
using namespace NObjectClient;
using namespace NHiveClient;
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetCellDescriptorsByCellTags)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChaosService))
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, SyncAlienCells)
    {
        context->SetRequestInfo("CellCount: %v",
            request->cell_descriptors_size());

        const auto& chaosManager = Bootstrap_->GetChaosManager();
        auto requestDescriptors = FromProto<std::vector<TAlienCellDescriptorLite>>(request->cell_descriptors());
        std::vector<TAlienCellDescriptor> responseDescriptors;

        for (const auto& requestDescriptor : requestDescriptors) {
            auto cellId = requestDescriptor.CellId;
            int configVersion = requestDescriptor.ConfigVersion;

            auto cell = chaosManager->FindChaosCellById(cellId);
            if (!IsObjectAlive(cell) || cell->GetConfigVersion() <= configVersion) {
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

        context->SetResponseInfo("CellCount: %v",
            response->cell_descriptors_size());

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

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, GetCellDescriptorsByCellTags)
    {
        auto cellTags = FromProto<std::vector<TCellTag>>(request->cell_tags());

        context->SetRequestInfo("CellTags: %v",
            cellTags);

        const auto& chaosManager = Bootstrap_->GetChaosManager();
        for (auto cellTag : cellTags) {
            auto* cell = chaosManager->GetChaosCellByTagOrThrow(cellTag);
            ToProto(response->add_cell_descriptors(), cell->GetDescriptor());
        }

        context->Reply();
    }
};

IServicePtr CreateMasterChaosService(TBootstrap* boostrap)
{
    return New<TChaosService>(boostrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
