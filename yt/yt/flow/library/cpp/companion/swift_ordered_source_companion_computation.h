#pragma once

#include "companion_computation_base.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

class TSwiftOrderedSourceCompanionComputation
    : public TCompanionComputationBaseAdapter<TSwiftOrderedSourceComputation>
{
public:
    TSwiftOrderedSourceCompanionComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    void DoInit(IJobInitContextPtr initContext) override;

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override;
};

DEFINE_REFCOUNTED_TYPE(TSwiftOrderedSourceCompanionComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
