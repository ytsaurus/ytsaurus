#include "stdafx.h"
#include "meta_state_service.h"
#include "bootstrap.h"
#include "meta_state_facade.h"

namespace NYT {
namespace NCellMaster {

using namespace NMetaState;
using namespace NRpc;

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

void TMetaStateServiceBase::InvokerHandler(
    IServiceContextPtr context,
    IInvokerPtr invoker,
    TClosure handler)
{
    auto* bootstrap = Bootstrap;
    auto wrappedHandler = BIND([=] () {
        bootstrap->GetMetaStateFacade()->ValidateInitialized();
        handler.Run();
    });

    TServiceBase::InvokerHandler(context, invoker, wrappedHandler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
