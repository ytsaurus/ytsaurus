#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/hydra_service.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMasterHydraServiceBase
    : public NHydra::THydraServiceBase
{
protected:
    TBootstrap* const Bootstrap_;

    TMasterHydraServiceBase(
        TBootstrap* bootstrap,
        const NRpc::TServiceDescriptor& descriptor,
        EAutomatonThreadQueue defaultQueue,
        NLogging::TLogger logger,
        NRpc::TServiceOptions options = {});

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue);
    void ValidateClusterInitialized();
};

////////////////////////////////////////////////////////////////////////////////

NHydra::IUpstreamSynchronizerPtr CreateMulticellUpstreamSynchronizer(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
