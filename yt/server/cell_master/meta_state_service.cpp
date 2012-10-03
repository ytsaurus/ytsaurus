#include "stdafx.h"
#include "meta_state_service.h"
#include "bootstrap.h"
#include "meta_state_facade.h"

namespace NYT {
namespace NCellMaster {

using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TMetaStateServiceBase::TMetaStateServiceBase(
    TBootstrap* bootstrap,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : NRpc::TServiceBase(
        bootstrap->GetMetaStateFacade()->GetGuardedInvoker(),
        serviceName,
        loggingCategory)
    , Bootstrap(bootstrap)
{
    YCHECK(bootstrap);
}

void TMetaStateServiceBase::ValidateLeaderStatus()
{
    Bootstrap->GetMetaStateFacade()->ValidateLeaderStatus();
}

void TMetaStateServiceBase::ValidateInitialized()
{
    Bootstrap->GetMetaStateFacade()->ValidateInitialized();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
