#include "master_hydra_service.h"

#include "bootstrap.h"
#include "hydra_facade.h"
#include "multicell_manager.h"
#include "world_initializer.h"
#include "world_initializer_cache.h"

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NHydra;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TMasterHydraServiceBase::TMasterHydraServiceBase(
    TBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor,
    TDefaultInvokerKind defaultInvokerKind,
    NLogging::TLogger logger,
    NRpc::TServiceOptions options)
    : THydraServiceBase(
        bootstrap->GetHydraFacade()->GetHydraManager(),
        SelectDefaultInvoker(defaultInvokerKind, bootstrap),
        descriptor,
        std::move(logger),
        CreateMulticellUpstreamSynchronizer(bootstrap),
        [&] {
            options.RealmId = bootstrap->GetMulticellManager()->GetCellId(),
            options.Authenticator = bootstrap->GetNativeAuthenticator();
            return options;
        }())
    , Bootstrap_(bootstrap)
    , ValidateClusterInitialized_(SelectClusterInitializationValidator(defaultInvokerKind))
{
    YT_VERIFY(Bootstrap_);
}

IInvokerPtr TMasterHydraServiceBase::GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue)
{
    return Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker(queue);
}

void TMasterHydraServiceBase::ValidateClusterInitialized()
{
    ValidateClusterInitialized_(Bootstrap_);
}

IInvokerPtr TMasterHydraServiceBase::SelectDefaultInvoker(
    TDefaultInvokerKind invokerKind,
    TBootstrap* bootstrap)
{
    return Visit(invokerKind,
        [&] (EAutomatonThreadQueue threadQueue) {
            return bootstrap->GetHydraFacade()->GetGuardedAutomatonInvoker(threadQueue);
        },
        [&] (TRpcHeavyDefaultInvoker /*rpcHeavy*/) {
            return TDispatcher::Get()->GetHeavyInvoker();
        });
}

auto TMasterHydraServiceBase::SelectClusterInitializationValidator(
    TDefaultInvokerKind invokerKind) -> TValidateClusterInititalizedFunction*
{
    static const auto validateViaWorldInitializer = [] (TBootstrap* bootstrap) {
        const auto& worldInitializer = bootstrap->GetWorldInitializer();
        worldInitializer->ValidateInitialized();
    };

    static const auto validateViaWorldInitializerCache = [] (TBootstrap* bootstrap) {
        const auto& worldInitializerCache = bootstrap->GetWorldInitializerCache();
        WaitForFast(worldInitializerCache->ValidateWorldInitialized())
            .ThrowOnError();
    };

    return Visit(invokerKind,
        [&] (EAutomatonThreadQueue /*automatonThreadQueue*/) -> TValidateClusterInititalizedFunction* {
            return validateViaWorldInitializer;
        },
        [&] (TRpcHeavyDefaultInvoker /*rpcHeavy*/) -> TValidateClusterInititalizedFunction* {
            return validateViaWorldInitializerCache;
        });
}

////////////////////////////////////////////////////////////////////////////////

class TMulticellUpstreamSynchronizer
    : public IUpstreamSynchronizer
{
public:
    explicit TMulticellUpstreamSynchronizer(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    TFuture<void> SyncWithUpstream() override
    {
        return Bootstrap_->GetMulticellManager()->SyncWithUpstream();
    }

private:
    TBootstrap* const Bootstrap_;
};

IUpstreamSynchronizerPtr CreateMulticellUpstreamSynchronizer(TBootstrap* bootstrap)
{
    return New<TMulticellUpstreamSynchronizer>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
