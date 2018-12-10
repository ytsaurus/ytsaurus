#pragma once

#include "public.h"

#include <yt/core/rpc/service_detail.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class THydraServiceBase
    : public NRpc::TServiceBase
{
protected:
    THydraServiceBase(
        IInvokerPtr invoker,
        const NRpc::TServiceDescriptor& descriptor,
        const NLogging::TLogger& logger,
        const NRpc::TRealmId& realmId);

    void ValidatePeer(EPeerKind kind);
    void SyncWithUpstream();

    virtual IHydraManagerPtr GetHydraManager() = 0;
    
private:
    virtual bool IsUp(const TCtxDiscoverPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
