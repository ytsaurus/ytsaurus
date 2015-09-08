#include "stdafx.h"
#include "master_hydra_service.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "bootstrap.h"

namespace NYT {
namespace NCellMaster {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TMasterHydraServiceBase::TMasterHydraServiceBase(
    TBootstrap* bootstrap,
    const Stroka& serviceName,
    const NLogging::TLogger& logger,
    int protocolVersion)
    : THydraServiceBase(
        bootstrap->GetHydraFacade()->GetGuardedAutomatonInvoker(),
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

IHydraManagerPtr TMasterHydraServiceBase::GetHydraManager()
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
