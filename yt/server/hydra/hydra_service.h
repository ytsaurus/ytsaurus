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
        IHydraManagerPtr hydraManager,
        IInvokerPtr automatonInvoker,
        const NRpc::TServiceId& serviceId,
        const NLogging::TLogger& logger,
        int protocolVersion = NRpc::TProxyBase::DefaultProtocolVersion);

    void ValidatePeer(EPeerKind kind);
    void SyncWithUpstream();

private:
    // Avoid name clash when inheriting from both THydraServiceBase and TCompositeAutomatonPart.
    const IHydraManagerPtr ServiceHydraManager_;

    virtual bool IsUp(TCtxDiscoverPtr context) const override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
