#include "clock_hydra_service.h"
#include "bootstrap.h"
#include "hydra_facade.h"

namespace NYT::NClusterClock {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TClockHydraServiceBase::TClockHydraServiceBase(
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
    YT_VERIFY(Bootstrap_);
}

IInvokerPtr TClockHydraServiceBase::GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue)
{
    return Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker(queue);
}

IHydraManagerPtr TClockHydraServiceBase::GetHydraManager()
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
