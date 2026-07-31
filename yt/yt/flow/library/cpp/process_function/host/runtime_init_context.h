#pragma once

#include <yt/yt/flow/library/cpp/computation/job_state/state_manager.h>

#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Production IRuntimeInitContext: a thin facade over the worker's IJobInitContext.
//! State-provider creation and prefixing are forwarded to the underlying init context;
//! external state managers/joiners are resolved through the TJobStateManager directly
//! (the underlying init context hides those behind protected members).
class TRuntimeInitContext
    : public IRuntimeInitContext
{
public:
    TRuntimeInitContext(
        IJobInitContextPtr underlying,
        TJobStateManagerPtr stateManager,
        NYTree::IMapNodePtr parametersNode = {},
        THashMap<TResourceId, IResourcePtr> staticResources = {});

    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> ctor) const override;
    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> ctor) const override;

    IInitContextPtr AsPartition() const override;
    IInitContextPtr AsKey(TKey key) const override;

    IRuntimeInitContextPtr WithPrefix(TStringBuf prefix) const override;
    const std::string& GetPrefix() const override;

    NYTree::IMapNodePtr GetParametersNode() const override;

    IResourcePtr GetStaticResource(const TResourceId& resourceId) const override;

protected:
    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& name) const override;
    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const override;

private:
    const IJobInitContextPtr Underlying_;
    const TJobStateManagerPtr StateManager_;
    const NYTree::IMapNodePtr ParametersNode_;
    const THashMap<TResourceId, IResourcePtr> StaticResources_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
