#pragma once

#include "companion_computation_base.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/computation/computation_base.h>
#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/public.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/computation/job_state/state_manager.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

struct TCompanionParameters
    : public TTransformComputation::TParameters
{
    std::optional<THashSet<std::string>> InternalStates;

    REGISTER_YSON_STRUCT(TCompanionParameters);

    static void Register(TRegistrar registrar);
};

struct TCompanionDynamicParameters
    : public TTransformComputation::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TCompanionDynamicParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TCompanionState
    : public NYTree::TYsonStruct
{
    std::optional<std::string> Payload;

    REGISTER_YSON_STRUCT(TCompanionState);

    static void Register(TRegistrar registrar);
};

using TCompanionStatePtr = TIntrusivePtr<TCompanionState>;

////////////////////////////////////////////////////////////////////////////////

class TTransformCompanionComputation
    : public TCompanionComputationBaseAdapter<TTransformComputation>
{
public:
    TTransformCompanionComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    YT_FLOW_EXTEND_PARAMETERS(TCompanionParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TCompanionDynamicParameters);

    void DoInit(IJobInitContextPtr initContext) final;

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) final;

private:
    THashMap<std::string, TMutableStateKeyClient<TCompanionState>> InternalStateClients_;
    THashMap<std::string, TMutableStateKeyClient<TSimpleExternalState>> ExternalStateClients_;
    THashMap<std::string, TJoinedStateKeyClient<TSimpleExternalState>> ExternalStateJoiners_;
};

DEFINE_REFCOUNTED_TYPE(TTransformCompanionComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
