#include "stdafx.h"
#include "hydra_service.h"
#include "meta_state_facade.h"
#include "bootstrap.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

THydraServiceBase::THydraServiceBase(
    TBootstrap* bootstrap,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : NHydra::THydraServiceBase(
        bootstrap->GetMetaStateFacade()->GetManager(),
        bootstrap->GetMetaStateFacade()->GetGuardedInvoker(),
        serviceName,
        loggingCategory)
    , Bootstrap(bootstrap)
{
    YCHECK(Bootstrap);
}

TClosure THydraServiceBase::PrepareHandler(TClosure handler)
{
    auto* bootstrap = Bootstrap;
    return TServiceBase::PrepareHandler(BIND([=] () {
        bootstrap->GetMetaStateFacade()->ValidateInitialized();
        handler.Run();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
