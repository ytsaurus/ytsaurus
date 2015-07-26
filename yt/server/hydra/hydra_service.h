#pragma once

#include "public.h"

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class THydraServiceBase
    : public NRpc::TServiceBase
{
protected:
    THydraServiceBase(
        IInvokerPtr invoker,
        const NRpc::TServiceId& serviceId,
        const NLogging::TLogger& logger,
        int protocolVersion = NRpc::TProxyBase::DefaultProtocolVersion);

    void ValidatePeer(EPeerKind kind);

    virtual IHydraManagerPtr GetHydraManager() = 0;
    
private:
    virtual bool IsUp(TCtxDiscoverPtr context) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
