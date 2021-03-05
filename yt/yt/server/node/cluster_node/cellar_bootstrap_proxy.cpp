#include "bootstrap.h"

#include <yt/yt/server/lib/cellar_agent/bootstrap_proxy.h>

#include <yt/yt/server/node/tablet_node/security_manager.h>

namespace NYT::NClusterNode {

using namespace NApi::NNative;
using namespace NCellarAgent;
using namespace NElection;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

class TCellarBootstrapProxy
    : public ICellarBootstrapProxy
{
public:
    explicit TCellarBootstrapProxy(
        TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual TCellId GetCellId() const override
    {
        return Bootstrap_->GetCellId();
    }
    
    virtual IClientPtr GetMasterClient() const override
    {
        return Bootstrap_->GetMasterClient();
    }

    virtual TNetworkPreferenceList GetLocalNetworks() const override
    {
        return Bootstrap_->GetLocalNetworks();
    }

    virtual IInvokerPtr GetControlInvoker() const override
    {
        return Bootstrap_->GetControlInvoker();
    }
    virtual IInvokerPtr GetTransactionTrackerInvoker() const override
    {
        return Bootstrap_->GetTransactionTrackerInvoker();
    }

    virtual IServerPtr GetRpcServer() const override
    {
        return Bootstrap_->GetRpcServer();
    }

    virtual IResourceLimitsManagerPtr GetResourceLimitsManager() const override
    {
        return Bootstrap_->GetSecurityManager();
    }

private:
    TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

ICellarBootstrapProxyPtr CreateCellarBootstrapProxy(TBootstrap* bootstrap)
{
    return New<TCellarBootstrapProxy>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
