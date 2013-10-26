#include "stdafx.h"
#include "hydra_service.h"
#include "hydra_manager.h"

#include <server/election/election_manager.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

THydraServiceBase::THydraServiceBase(
    IHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker,
    const TServiceId& serviceId,
    const Stroka& loggingCategory)
    : TServiceBase(
        hydraManager->CreateGuardedAutomatonInvoker(automatonInvoker),
        serviceId,
        loggingCategory)
    , ServiceHydraManager(hydraManager)
    , AutomatonInvoker(automatonInvoker)
{
    ServiceHydraManager->SubscribeLeaderActive(BIND(&THydraServiceBase::OnLeaderActive, Unretained(this)));
    ServiceHydraManager->SubscribeStopLeading(BIND(&THydraServiceBase::OnStopLeading, Unretained(this)));
    ServiceHydraManager->SubscribeStopFollowing(BIND(&THydraServiceBase::OnStopFollowing, Unretained(this)));
}

void THydraServiceBase::OnLeaderActive()
{
    EpochAutomatonInvoker = ServiceHydraManager
        ->GetEpochContext()
        ->CancelableContext
        ->CreateInvoker(AutomatonInvoker);
}

void THydraServiceBase::OnStopLeading()
{
    EpochAutomatonInvoker.Reset();
    OnStopEpoch();
}

void THydraServiceBase::OnStopFollowing()
{
    OnStopEpoch();
}

void THydraServiceBase::OnStopEpoch()
{
    CancelActiveRequests(TError(
        NRpc::EErrorCode::Unavailable,
        "Service is restarting"));
}

void THydraServiceBase::ValidateActiveLeader()
{
    if (!ServiceHydraManager->IsActiveLeader()) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Not an active leader");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
