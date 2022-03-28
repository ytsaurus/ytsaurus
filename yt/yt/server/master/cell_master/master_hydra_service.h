#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/hydra_service.h>

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
        const NLogging::TLogger& logger);


    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue);
    void ValidateClusterInitialized();
};

////////////////////////////////////////////////////////////////////////////////

NHydra::IUpstreamSynchronizerPtr CreateMulticellUpstreamSynchronizer(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
