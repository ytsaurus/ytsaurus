#include "master_hydra_service.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "world_initializer.h"

namespace NYT::NCellMaster {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TMasterHydraServiceBase::TMasterHydraServiceBase(
    TBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor,
    EAutomatonThreadQueue defaultQueue,
    const NLogging::TLogger& logger)
    : THydraServiceBase(
        bootstrap->GetHydraFacade()->GetGuardedAutomatonInvoker(defaultQueue),
        descriptor,
        logger,
        bootstrap->GetCellId())
    , Bootstrap_(bootstrap)
{
    YCHECK(Bootstrap_);
}

IInvokerPtr TMasterHydraServiceBase::GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue)
{
    return Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker(queue);
}

IHydraManagerPtr TMasterHydraServiceBase::GetHydraManager()
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager();
}

void TMasterHydraServiceBase::ValidateClusterInitialized()
{
    const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
    worldInitializer->ValidateInitialized();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
