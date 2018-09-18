#pragma once

#include "public.h"

#include <yt/server/hydra/hydra_service.h>

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
        const NRpc::TServiceDescriptor& descriptor,
        EAutomatonThreadQueue defaultQueue,
        const NLogging::TLogger& logger);


    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue);
    void ValidateClusterInitialized();

private:
    virtual NHydra::IHydraManagerPtr GetHydraManager() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
