#pragma once

#include "public.h"

#include "server_context.h"
#include "state_store.h"

#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Companion implementation of #IRuntimeInitContext.
class TCompanionRuntimeInitContext
    : public IRuntimeInitContext
{
public:
    TCompanionRuntimeInitContext(
        TCompanionStateStorePtr stateStore,
        NYTree::IMapNodePtr parametersNode,
        NYTree::TYsonStructPtr parametersObject = {},
        THashMap<TResourceId, IResourcePtr> resources = {},
        std::string prefix = {},
        NProfiling::TProfiler profiler = {},
        TCompanionServerContextPtr serverContext = {},
        TComputationId computationId = {});

    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(
        std::function<IStateHolderPtr()> ctor) const override;
    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(
        std::function<IStateHolderPtr()> ctor) const override;

    IInitContextPtr AsPartition() const override;
    IInitContextPtr AsKey(TKey key) const override;

    IRuntimeInitContextPtr WithPrefix(TStringBuf prefix) const override;
    const std::string& GetPrefix() const override;

    NYTree::IMapNodePtr GetParametersNode() const override;
    NYTree::TYsonStructPtr GetParametersObject() const override;

    IResourcePtr GetStaticResource(const TResourceId& resourceId) const override;

    //! Profiler for the hosted computation.
    NProfiling::TProfiler GetProfiler() const override;

    //! The companion process' shared HTTP clients from #TCompanionServerContext, running
    //! on its HTTP poller; throw when the context was built without one.
    NHttp::IClientPtr GetHttpClient() const override;
    NHttp::IClientPtr GetHttpsClient() const override;

    //! The id of the computation this companion hosts, as carried by the request.
    TComputationId GetComputationId() const override;

    //! Throws because the wire protocol does not identify a partition.
    TPartitionId GetPartitionId() const override;

protected:
    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& name) const override;
    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const override;

private:
    const TCompanionStateStorePtr StateStore_;
    const NYTree::IMapNodePtr ParametersNode_;
    //! Parsed static parameters; null only in direct tests.
    const NYTree::TYsonStructPtr ParametersObject_;
    //! Companion resources keyed by required-resource alias.
    const THashMap<TResourceId, IResourcePtr> Resources_;
    const std::string Prefix_;
    const NProfiling::TProfiler Profiler_;
    //! Process-wide facilities of the hosting companion; null only when a test
    //! constructs the context directly.
    const TCompanionServerContextPtr ServerContext_;
    const TComputationId ComputationId_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionRuntimeInitContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
