#include "master_hydra_service.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "multicell_manager.h"

namespace NYT::NCellMaster {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TMasterHydraServiceBase::TMasterHydraServiceBase(
    TBootstrap* bootstrap,
    const NRpc::TServiceDescriptor& descriptor,
    EAutomatonThreadQueue defaultQueue,
    NLogging::TLogger logger,
    NRpc::TServiceOptions options)
    : THydraServiceBase(
        bootstrap->GetHydraFacade()->GetHydraManager(),
        bootstrap->GetHydraFacade()->GetGuardedAutomatonInvoker(defaultQueue),
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
    const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
    worldInitializer->ValidateInitialized();
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
