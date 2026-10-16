#pragma once

#include "companion_computation_base.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

class TSwiftMapCompanionComputation
    : public TCompanionComputationBaseAdapter<TSwiftMapComputation>
{
public:
    TSwiftMapCompanionComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override;

    void DoInit(IJobInitContextPtr initContext) override;
};

DEFINE_REFCOUNTED_TYPE(TSwiftMapCompanionComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
