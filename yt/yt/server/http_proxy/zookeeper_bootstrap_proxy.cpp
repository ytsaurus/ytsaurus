#include "zookeeper_bootstrap_proxy.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/lib/zookeeper_proxy/bootstrap_proxy.h>

namespace NYT::NHttpProxy {

using namespace NZookeeperProxy;

////////////////////////////////////////////////////////////////////////////////

class TZookeeperBootstrapProxy
    : public IBootstrapProxy
{
public:
    explicit TZookeeperBootstrapProxy(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    const TZookeeperProxyConfigPtr& GetConfig() const override
    {
        const auto& config = Bootstrap_->GetConfig();
        return config->ZookeeperProxy;
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

} // namespace NYT::NHttpProxy
