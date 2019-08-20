#pragma once

#include "public.h"

#include <yt/server/lib/hydra/hydra_service.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

class TClockHydraServiceBase
    : public NHydra::THydraServiceBase
{
protected:
    TBootstrap* const Bootstrap_;

    TClockHydraServiceBase(
        TBootstrap* bootstrap,
        const NRpc::TServiceDescriptor& descriptor,
        EAutomatonThreadQueue defaultQueue,
        const NLogging::TLogger& logger);


    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue);

private:
    virtual NHydra::IHydraManagerPtr GetHydraManager() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
