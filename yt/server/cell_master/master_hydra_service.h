#pragma once

#include "public.h"

#include <server/hydra/hydra_service.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMasterHydraServiceBase
    : public NHydra::THydraServiceBase
{
protected:
    TBootstrap* const Bootstrap_;

    TMasterHydraServiceBase(
        TBootstrap* bootstrap,
        const Stroka& serviceName,
        const NLogging::TLogger& logger,
        int protocolVersion = NRpc::TProxyBase::DefaultProtocolVersion);


    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue);

private:
    virtual void BeforeInvoke() override;
    virtual NHydra::IHydraManagerPtr GetHydraManager() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
