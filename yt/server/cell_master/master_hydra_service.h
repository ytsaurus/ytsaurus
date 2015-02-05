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
        const NLog::TLogger& logger,
        int protocolVersion = NRpc::TProxyBase::DefaultProtocolVersion);

private:
    virtual void BeforeInvoke() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
