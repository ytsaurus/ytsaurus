#include "stdafx.h"
#include "hydra_service.h"
#include "meta_state_facade.h"
#include "bootstrap.h"

namespace NYT {
namespace NCellMaster {

using namespace NHydra;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

THydraServiceBase::THydraServiceBase(
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

    auto hydraManager = Bootstrap->GetMetaStateFacade()->GetManager();
    hydraManager->SubscribeStopLeading(BIND(&THydraServiceBase::OnStopEpoch, MakeWeak(this)));
    hydraManager->SubscribeStopFollowing(BIND(&THydraServiceBase::OnStopEpoch, MakeWeak(this)));
}

void THydraServiceBase::ValidateActiveLeader()
{
    Bootstrap->GetMetaStateFacade()->ValidateActiveLeader();
}

TClosure THydraServiceBase::PrepareHandler(TClosure handler)
{
    auto* bootstrap = Bootstrap;
    return TServiceBase::PrepareHandler(BIND([=] () {
        bootstrap->GetMetaStateFacade()->ValidateInitialized();
        handler.Run();
    }));
}

void THydraServiceBase::OnStopEpoch()
{
    CancelActiveRequests(TError(
        NRpc::EErrorCode::Unavailable,
        "Master is restarting"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
