#include "stdafx.h"
#include "hydra_facade.h"
#include "master_hydra_service.h"
#include "world_initializer.h"
#include "bootstrap.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

TMasterHydraServiceBase::TMasterHydraServiceBase(
    TBootstrap* bootstrap,
    const Stroka& serviceName,
    const NLogging::TLogger& logger,
    int protocolVersion)
    : NHydra::THydraServiceBase(
        bootstrap->GetHydraFacade()->GetHydraManager(),
        bootstrap->GetHydraFacade()->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::RpcService),
        NRpc::TServiceId(serviceName, bootstrap->GetCellId()),
        logger,
        protocolVersion)
    , Bootstrap_(bootstrap)
{
    YCHECK(Bootstrap_);
}

IInvokerPtr TMasterHydraServiceBase::GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue)
{
    return Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker(queue);
}

void TMasterHydraServiceBase::BeforeInvoke()
{
    auto worldInitializer = Bootstrap_->GetWorldInitializer();
    if (!worldInitializer->CheckInitialized()) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Cluster is not initialized");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
