#include "config.h"
#include "chaos_cell_bundle_type_handler.h"
#include "chaos_cell_type_handler.h"
#include "chaos_manager.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/misc/small_vector.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NChaosServer {

using namespace NCellMaster;

using namespace NHiveClient;
using namespace NHiveServer;
using namespace NHydra;
using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerServer;
using namespace NCellServer;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TChaosManager
    : public IChaosManager
    , public TMasterAutomatonPart
{
public:
    explicit TChaosManager(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::ChaosManager)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);
    }

    virtual void Initialize() override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateChaosCellBundleTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateChaosCellTypeHandler(Bootstrap_));

        const auto& cellManager = Bootstrap_->GetTamedCellManager();
        cellManager->SubscribeCellDecommissionStarted(BIND(&TChaosManager::OnTabletCellDecommissionStarted, MakeWeak(this)));
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void OnTabletCellDecommissionStarted(TCellBase* cellBase)
    {
        if (cellBase->GetType() != EObjectType::ChaosCell){
            return;
        }
        if (!cellBase->IsDecommissionStarted()) {
            return;
        }
        cellBase->GossipStatus().Local().Decommissioned = true;
    }
};

////////////////////////////////////////////////////////////////////////////////

IChaosManagerPtr CreateChaosManager(NCellMaster::TBootstrap* bootstrap)
{
    return New<TChaosManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
