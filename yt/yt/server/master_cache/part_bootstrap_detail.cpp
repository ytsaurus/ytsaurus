#include "part_bootstrap_detail.h"

namespace NYT::NMasterCache {

using namespace NApi::NNative;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPartBootstrapBase::TPartBootstrapBase(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

const TMasterCacheBootstrapConfigPtr& TPartBootstrapBase::GetConfig() const
{
    return Bootstrap_->GetConfig();
}

const IConnectionPtr& TPartBootstrapBase::GetConnection() const
{
    return Bootstrap_->GetConnection();
}

const NApi::IClientPtr& TPartBootstrapBase::GetRootClient() const
{
    return Bootstrap_->GetRootClient();
}

const IMapNodePtr& TPartBootstrapBase::GetOrchidRoot() const
{
    return Bootstrap_->GetOrchidRoot();
}

const NRpc::IServerPtr& TPartBootstrapBase::GetRpcServer() const
{
    return Bootstrap_->GetRpcServer();
}

const IInvokerPtr& TPartBootstrapBase::GetControlInvoker() const
{
    return Bootstrap_->GetControlInvoker();
}

const NRpc::IAuthenticatorPtr& TPartBootstrapBase::GetNativeAuthenticator() const
{
    return Bootstrap_->GetNativeAuthenticator();
}

const TDynamicConfigManagerPtr& TPartBootstrapBase::GetDynamicConfigManger() const
{
    return Bootstrap_->GetDynamicConfigManger();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
