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
    const NLog::TLogger& logger,
    int protocolVersion)
    : TServiceBase(
        hydraManager->CreateGuardedAutomatonInvoker(automatonInvoker),
        serviceId,
        logger,
        protocolVersion)
    , AutomatonInvoker_(automatonInvoker)
    , ServiceHydraManager_(hydraManager)
{
    ServiceHydraManager_->SubscribeLeaderActive(BIND(&THydraServiceBase::OnLeaderActive, MakeWeak(this)));
    ServiceHydraManager_->SubscribeStopLeading(BIND(&THydraServiceBase::OnStopLeading, MakeWeak(this)));
}

void THydraServiceBase::OnLeaderActive()
{
    EpochAutomatonInvoker_ = ServiceHydraManager_
        ->GetAutomatonEpochContext()
        ->CancelableContext
        ->CreateInvoker(AutomatonInvoker_);
}

void THydraServiceBase::OnStopLeading()
{
    EpochAutomatonInvoker_.Reset();
}

void THydraServiceBase::ValidateActiveLeader()
{
    if (!ServiceHydraManager_->IsActiveLeader()) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Not an active leader");
    }
}

bool THydraServiceBase::IsUp() const
{
    return ServiceHydraManager_->IsActiveLeader();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
