#include "bootstrap.h"

#include "bootstrap_proxy.h"
#include "zookeeper_manager.h"

namespace NYT::NZookeeperMaster {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(IBootstrapProxy* bootstrapProxy)
        : BootstrapProxy_(bootstrapProxy)
    { }

    void Initialize() override
    {
        ZookeeperManager_ = CreateZookeeperManager(this);
    }

    const IZookeeperManagerPtr& GetZookeeperManager() const override
    {
        return ZookeeperManager_;
    }

    void VerifyPersistentStateRead() override
    {
        BootstrapProxy_->VerifyPersistentStateRead();
    }

    TCompositeAutomatonPtr GetAutomaton() const override
    {
        return BootstrapProxy_->GetAutomaton();
    }

    IInvokerPtr GetAutomatonInvoker() const override
    {
        return BootstrapProxy_->GetAutomatonInvoker();
    }

    NHydra::IHydraManagerPtr GetHydraManager() const override
    {
        return BootstrapProxy_->GetHydraManager();
    }

private:
    IBootstrapProxy* const BootstrapProxy_;

    IZookeeperManagerPtr ZookeeperManager_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(IBootstrapProxy* bootstrapProxy)
{
    return std::make_unique<TBootstrap>(bootstrapProxy);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperMaster
