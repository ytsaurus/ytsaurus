#include "stdafx.h"
#include "hydra_service.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "bootstrap.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

THydraServiceBase::THydraServiceBase(
    TBootstrap* bootstrap,
    const Stroka& serviceName,
    const NLog::TLogger& logger)
    : NHydra::THydraServiceBase(
        bootstrap->GetHydraFacade()->GetHydraManager(),
        bootstrap->GetHydraFacade()->GetGuardedAutomatonInvoker(),
        serviceName,
        logger)
    , Bootstrap(bootstrap)
{
    YCHECK(Bootstrap);
}

void THydraServiceBase::BeforeInvoke()
{
    Bootstrap->GetWorldInitializer()->ValidateInitialized();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
