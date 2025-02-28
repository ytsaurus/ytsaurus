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

    struct TRpcHeavyDefaultInvoker
    { };

    using TDefaultInvokerKind = std::variant<EAutomatonThreadQueue, TRpcHeavyDefaultInvoker>;

    TMasterHydraServiceBase(
        TBootstrap* bootstrap,
        const NRpc::TServiceDescriptor& descriptor,
        TDefaultInvokerKind defaultInvokerKind,
        NLogging::TLogger logger,
        NRpc::TServiceOptions options = {});

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue);
    void ValidateClusterInitialized();

private:
    using TValidateClusterInititalizedFunction = void(TBootstrap*);
    TValidateClusterInititalizedFunction* const ValidateClusterInitialized_;

    static TValidateClusterInititalizedFunction* SelectClusterInitializationValidator(
        TDefaultInvokerKind invokerKind);
    static IInvokerPtr SelectDefaultInvoker(
        TDefaultInvokerKind invokerKind,
        TBootstrap* bootsrap);
};

////////////////////////////////////////////////////////////////////////////////

NHydra::IUpstreamSynchronizerPtr CreateMulticellUpstreamSynchronizer(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
