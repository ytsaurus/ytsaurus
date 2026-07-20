#include "runtime_init_context.h"

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TRuntimeInitContext::TRuntimeInitContext(
    IJobInitContextPtr underlying,
    TJobStateManagerPtr stateManager,
    NYTree::IMapNodePtr parametersNode,
    THashMap<TResourceId, IResourcePtr> staticResources)
    : Underlying_(std::move(underlying))
    , StateManager_(std::move(stateManager))
    , ParametersNode_(parametersNode ? std::move(parametersNode) : NYTree::GetEphemeralNodeFactory()->CreateMap())
    , StaticResources_(std::move(staticResources))
{ }

TFuture<IMutableStateKeyProviderPtr> TRuntimeInitContext::CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> ctor) const
{
    return Underlying_->CreateMutableStateKeyProvider(std::move(ctor));
}

TFuture<IJoinedStateKeyProviderPtr> TRuntimeInitContext::CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> ctor) const
{
    return Underlying_->CreateJoinedStateKeyProvider(std::move(ctor));
}

IInitContextPtr TRuntimeInitContext::AsPartition() const
{
    return Underlying_->AsPartition();
}

IInitContextPtr TRuntimeInitContext::AsKey(TKey key) const
{
    return Underlying_->AsKey(std::move(key));
}

IRuntimeInitContextPtr TRuntimeInitContext::WithPrefix(TStringBuf prefix) const
{
    return New<TRuntimeInitContext>(Underlying_->WithPrefix(prefix), StateManager_, ParametersNode_, StaticResources_);
}

const std::string& TRuntimeInitContext::GetPrefix() const
{
    return Underlying_->GetPrefix();
}

NYTree::IMapNodePtr TRuntimeInitContext::GetParametersNode() const
{
    return ParametersNode_;
}

IResourcePtr TRuntimeInitContext::GetStaticResource(const TResourceId& resourceId) const
{
    auto iter = StaticResources_.find(resourceId);
    if (iter == StaticResources_.end()) {
        THROW_ERROR_EXCEPTION("Static resource is not found")
            << TErrorAttribute("resource_id", resourceId);
    }
    return iter->second;
}

IExternalStateManagerPtr TRuntimeInitContext::GetExternalStateManagerOrThrow(const std::string& name) const
{
    return StateManager_->GetExternalStateManagerOrThrow(name);
}

IExternalStateJoinerPtr TRuntimeInitContext::GetExternalStateJoinerOrThrow(const std::string& name) const
{
    return StateManager_->GetExternalStateJoinerOrThrow(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
