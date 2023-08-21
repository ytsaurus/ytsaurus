#include "bootstrap.h"

#include "bootstrap_proxy.h"
#include "config.h"
#include "private.h"
#include "server.h"

#include <yt/yt/core/concurrency/thread_pool_poller.h>

namespace NYT::NZookeeperProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ZookeeperProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    explicit TBootstrap(IBootstrapProxy* bootstrapProxy)
        : BootstrapProxy_(bootstrapProxy)
        , Config_(BootstrapProxy_->GetConfig())
    {
        Y_UNUSED(BootstrapProxy_);

        // TODO: Parametrize.
        Poller_ = CreateThreadPoolPoller(4, "Poller");
        Acceptor_ = CreateThreadPoolPoller(1, "Acceptor");

        Server_ = CreateServer(
            Config_->Server,
            Poller_,
            Acceptor_);
    }

    void Run() override
    {
        YT_LOG_INFO("Running Zookeeper proxy");

        Server_->Start();
    }

private:
    IBootstrapProxy* const BootstrapProxy_;

    const TZookeeperProxyConfigPtr Config_;

    IPollerPtr Poller_;
    IPollerPtr Acceptor_;

    IServerPtr Server_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(IBootstrapProxy* bootstrapProxy)
{
    return std::make_unique<TBootstrap>(bootstrapProxy);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy
