#pragma once

#include "public.h"

#include "state_store.h"

#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Companion-side IRuntimeInitContext: state key clients are bound to the
//! per-job #TCompanionStateStore; static resources resolve against the
//! companion-hosted instances acquired for the job; internal-state joiners
//! are not available out of process.
class TCompanionRuntimeInitContext
    : public IRuntimeInitContext
{
public:
    TCompanionRuntimeInitContext(
        TCompanionStateStorePtr stateStore,
        NYTree::IMapNodePtr parametersNode,
        THashMap<TResourceId, IResourcePtr> resources = {},
        std::string prefix = {});

    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(
        std::function<IStateHolderPtr()> ctor) const override;
    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(
        std::function<IStateHolderPtr()> ctor) const override;

    IInitContextPtr AsPartition() const override;
    IInitContextPtr AsKey(TKey key) const override;

    IRuntimeInitContextPtr WithPrefix(TStringBuf prefix) const override;
    const std::string& GetPrefix() const override;

    NYTree::IMapNodePtr GetParametersNode() const override;

    IResourcePtr GetStaticResource(const TResourceId& resourceId) const override;

    //! Null profiler: the computation profiler does not cross the process boundary.
    NProfiling::TProfiler GetProfiler() const override;

    //! Throws: the hosting partition is not identified on the wire, and a null id would
    //! silently collapse every partition into one value.
    TPartitionId GetPartitionId() const override;

protected:
    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& name) const override;
    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const override;

private:
    const TCompanionStateStorePtr StateStore_;
    const NYTree::IMapNodePtr ParametersNode_;
    //! Companion-hosted resources acquired for the job, keyed by their
    //! required-resource alias; immutable for the context's lifetime.
    const THashMap<TResourceId, IResourcePtr> Resources_;
    const std::string Prefix_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionRuntimeInitContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
