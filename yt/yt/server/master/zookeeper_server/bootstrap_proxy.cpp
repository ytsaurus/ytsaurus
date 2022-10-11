#include "bootstrap_proxy.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/lib/zookeeper_master/bootstrap_proxy.h>

namespace NYT::NZookeeperServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NZookeeperMaster;

////////////////////////////////////////////////////////////////////////////////

class TZookeeperBootstrapProxy
    : public IBootstrapProxy
{
public:
    explicit TZookeeperBootstrapProxy(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void VerifyPersistentStateRead() override
    {
        Bootstrap_->VerifyPersistentStateRead();
    }

    TCompositeAutomatonPtr GetAutomaton() const override
    {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        return hydraFacade->GetAutomaton();
    }

    IInvokerPtr GetAutomatonInvoker() const override
    {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        return hydraFacade->GetAutomatonInvoker(EAutomatonThreadQueue::Zookeeper);
    }

    NHydra::IHydraManagerPtr GetHydraManager() const override
    {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        return hydraFacade->GetHydraManager();
    }

private:
    TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrapProxy> CreateZookeeperBootstrapProxy(TBootstrap* bootstrap)
{
    return std::make_unique<TZookeeperBootstrapProxy>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
