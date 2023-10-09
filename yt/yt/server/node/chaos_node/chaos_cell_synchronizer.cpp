#include "chaos_cell_synchronizer.h"

#include "bootstrap.h"
#include "chaos_manager.h"
#include "chaos_slot.h"
#include "private.h"
#include "public.h"

#include <yt/yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/chaos_client/chaos_master_service_proxy.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NChaosNode {

using namespace NApi;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NHiveClient;
using namespace NChaosNode::NProto;
using namespace NChaosClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChaosNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TChaosCellSynchronizer
    : public IChaosCellSynchronizer
{
public:
    TChaosCellSynchronizer(
        TChaosCellSynchronizerConfigPtr config,
        IChaosSlotPtr slot,
        IBootstrap* bootstrap)
        : Slot_(slot)
        , Bootstrap_(bootstrap)
        , SynchronizeExecutor_(New<TPeriodicExecutor>(
            slot->GetAutomatonInvoker(),
            BIND(&TChaosCellSynchronizer::SafeSynchronize, MakeWeak(this)),
            config->SyncPeriod))
    { }

    void Start() override
    {
        SynchronizeExecutor_->Start();
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(SynchronizeExecutor_->Stop());
    }

private:
    const IChaosSlotPtr Slot_;
    IBootstrap* const Bootstrap_;

    NConcurrency::TPeriodicExecutorPtr SynchronizeExecutor_;

    void SafeSynchronize()
    {
        try {
            Synchronize();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error while chaos cell directory synchronization");
        }
    }

    void Synchronize()
    {
        YT_LOG_DEBUG("Chaos cell synchronizer requesting cells from master");

        const auto& connection = Bootstrap_->GetConnection();
        const auto& cellDirectory = connection->GetCellDirectory();

        auto channel = connection->GetMasterChannelOrThrow(EMasterChannelKind::Follower, PrimaryMasterCellTagSentinel);
        auto proxy = TChaosMasterServiceProxy(std::move(channel));

        auto req = proxy.GetCellDescriptorsByCellBundle();
        req->set_cell_bundle(Slot_->GetCellBundleName());

        auto result = WaitFor(req->Invoke())
            .ValueOrThrow();

        auto descriptors = FromProto<std::vector<TCellDescriptor>>(result->cell_descriptors());

        THashSet<TCellId> chaosCellIds;
        for (const auto& descriptor : descriptors) {
            chaosCellIds.insert(descriptor.CellId);
            cellDirectory->ReconfigureCell(descriptor);
        }

        YT_LOG_DEBUG("Chaos cell synchronizer received cells (ChaosCellIds: %v)",
            chaosCellIds);

        std::vector<TCellId> oldCoordinators;
        const auto& chaosManager = Slot_->GetChaosManager();
        for (const auto cellId : chaosManager->CoordinatorCellIds()) {
            if (auto it = chaosCellIds.find(cellId)) {
                chaosCellIds.erase(it);
            } else {
                oldCoordinators.push_back(cellId);
            }
        }

        std::vector<TCellId> newCoordinators(chaosCellIds.begin(), chaosCellIds.end());

        YT_LOG_DEBUG("Chaos cell synchronizer will update coordinator cells (NewChaosCellIds: %v, OldChaosCellIds: %v)",
            newCoordinators,
            oldCoordinators);

        TReqUpdateCoordinatorCells request;
        ToProto(request.mutable_add_coordinator_cell_ids(), newCoordinators);
        ToProto(request.mutable_remove_coordinator_cell_ids(), oldCoordinators);

        auto hydraManager = Slot_->GetHydraManager();
        auto mutation = CreateMutation(hydraManager, request);
        YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
    }
};

IChaosCellSynchronizerPtr CreateChaosCellSynchronizer(
    TChaosCellSynchronizerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap)
{
    return New<TChaosCellSynchronizer>(std::move(config), std::move(slot), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
