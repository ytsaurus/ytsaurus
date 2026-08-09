#pragma once

#include "companion_computation_base.h"
#include "companion_model.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/job_state/state_manager.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/transform_ordered_source_computation.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

struct TTransformOrderedSourceCompanionParameters
    : public TTransformOrderedSourceComputation::TParameters
{
    std::optional<THashSet<std::string>> InternalStates;

    REGISTER_YSON_STRUCT(TTransformOrderedSourceCompanionParameters);

    static void Register(TRegistrar registrar);
};

struct TTransformOrderedSourceCompanionDynamicParameters
    : public TTransformOrderedSourceComputation::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TTransformOrderedSourceCompanionDynamicParameters);

    static void Register(TRegistrar registrar);
};

class TTransformOrderedSourceCompanionComputation
    : public TCompanionComputationBaseAdapter<TTransformOrderedSourceComputation>
{
public:
    TTransformOrderedSourceCompanionComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    YT_FLOW_EXTEND_PARAMETERS(TTransformOrderedSourceCompanionParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TTransformOrderedSourceCompanionDynamicParameters);

    void DoInit(IJobInitContextPtr initContext) final;

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) final;

private:
    THashMap<std::string, TMutableStateKeyClient<TCompanionState>> InternalStateClients_;
    THashMap<std::string, TJoinedStateKeyClient<TSimpleExternalState>> ExternalStateJoiners_;
};

DEFINE_REFCOUNTED_TYPE(TTransformOrderedSourceCompanionComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
