#include "bootstrap.h"

#include "private.h"

namespace NYT::NZookeeperProxy {

static const auto& Logger = ZookeeperProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    explicit TBootstrap(IBootstrapProxy* bootstrapProxy)
        : BootstrapProxy_(bootstrapProxy)
    {
        Y_UNUSED(BootstrapProxy_);
    }

    void Run() override
    {
        YT_LOG_INFO("Running Zookeeper proxy");
    }

private:
    IBootstrapProxy* const BootstrapProxy_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(IBootstrapProxy* bootstrapProxy)
{
    return std::make_unique<TBootstrap>(bootstrapProxy);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy
