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

    auto metaStateManager = Bootstrap->GetMetaStateFacade()->GetManager();
    metaStateManager->SubscribeStopLeading(BIND(&TMetaStateServiceBase::OnStopEpoch, MakeWeak(this)));
    metaStateManager->SubscribeStopFollowing(BIND(&TMetaStateServiceBase::OnStopEpoch, MakeWeak(this)));
}

void TMetaStateServiceBase::ValidateLeaderStatus()
{
    Bootstrap->GetMetaStateFacade()->ValidateLeaderStatus();
}

TClosure TMetaStateServiceBase::PrepareHandler(
    IServiceContextPtr context,
    TClosure handler)
{
    auto* bootstrap = Bootstrap;
    return TServiceBase::PrepareHandler(
        context,
        BIND([=] () {
            bootstrap->GetMetaStateFacade()->ValidateInitialized();
            handler.Run();
        }));
}

void TMetaStateServiceBase::OnStopEpoch()
{
    CancelActiveRequests(TError(
        NRpc::EErrorCode::Unavailable,
        "Master is restarting"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
