#include "master_hydra_service.h"

#include "bootstrap.h"
#include "hydra_facade.h"
#include "multicell_manager.h"
#include "world_initializer.h"

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NHydra;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TMasterHydraServiceBase::TMasterHydraServiceBase(
    TBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor,
    std::variant<EAutomatonThreadQueue, IInvokerPtr> defaultInvoker,
    NLogging::TLogger logger,
    NRpc::TServiceOptions options)
    : THydraServiceBase(
        bootstrap->GetHydraFacade()->GetHydraManager(),
        Visit(defaultInvoker,
            [&] (EAutomatonThreadQueue threadQueue) {
                return bootstrap->GetHydraFacade()->GetGuardedAutomatonInvoker(threadQueue);
            },
            [] (IInvokerPtr invoker) {
                return invoker;
            }),
        descriptor,
        std::move(logger),
        CreateMulticellUpstreamSynchronizer(bootstrap),
        [&] {
            options.RealmId = bootstrap->GetMulticellManager()->GetCellId(),
            options.Authenticator = bootstrap->GetNativeAuthenticator();
            return options;
        }())
    , Bootstrap_(bootstrap)
{
    YT_VERIFY(Bootstrap_);
}

IInvokerPtr TMasterHydraServiceBase::GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue)
{
    return Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker(queue);
}

void TMasterHydraServiceBase::ValidateClusterInitialized()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    Bootstrap_->GetWorldInitializer()->ValidateInitialized();
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
