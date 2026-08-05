#include "runtime_init_context.h"

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

TCompanionRuntimeInitContext::TCompanionRuntimeInitContext(
    TCompanionStateStorePtr stateStore,
    NYTree::IMapNodePtr parametersNode,
    std::string prefix)
    : StateStore_(std::move(stateStore))
    , ParametersNode_(parametersNode
            ? std::move(parametersNode)
            : NYTree::GetEphemeralNodeFactory()->CreateMap())
    , Prefix_(std::move(prefix))
{ }

TFuture<IMutableStateKeyProviderPtr> TCompanionRuntimeInitContext::CreateMutableStateKeyProvider(
    std::function<IStateHolderPtr()> ctor) const
{
    return MakeFuture(StateStore_->RegisterInternalState(Prefix_, std::move(ctor)));
}

TFuture<IJoinedStateKeyProviderPtr> TCompanionRuntimeInitContext::CreateJoinedStateKeyProvider(
    std::function<IStateHolderPtr()> /*ctor*/) const
{
    THROW_ERROR_EXCEPTION("Internal state joiners are not available in a companion process");
}

IInitContextPtr TCompanionRuntimeInitContext::AsPartition() const
{
    THROW_ERROR_EXCEPTION("Partition init context is not available in a companion process");
}

IInitContextPtr TCompanionRuntimeInitContext::AsKey(TKey /*key*/) const
{
    THROW_ERROR_EXCEPTION("Key init context is not available in a companion process");
}

IRuntimeInitContextPtr TCompanionRuntimeInitContext::WithPrefix(TStringBuf prefix) const
{
    return New<TCompanionRuntimeInitContext>(
        StateStore_,
        ParametersNode_,
        ExtendStateNamePrefix(Prefix_, prefix));
}

const std::string& TCompanionRuntimeInitContext::GetPrefix() const
{
    return Prefix_;
}

NYTree::IMapNodePtr TCompanionRuntimeInitContext::GetParametersNode() const
{
    return ParametersNode_;
}

IResourcePtr TCompanionRuntimeInitContext::GetStaticResource(const TResourceId& resourceId) const
{
    THROW_ERROR_EXCEPTION("Static resource %Qv is not available in a companion process",
        resourceId);
}

NProfiling::TProfiler TCompanionRuntimeInitContext::GetProfiler() const
{
    return {};
}

IExternalStateManagerPtr TCompanionRuntimeInitContext::GetExternalStateManagerOrThrow(
    const std::string& name) const
{
    return StateStore_->GetExternalStateManager(name);
}

IExternalStateJoinerPtr TCompanionRuntimeInitContext::GetExternalStateJoinerOrThrow(
    const std::string& name) const
{
    return StateStore_->GetExternalStateJoiner(name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
