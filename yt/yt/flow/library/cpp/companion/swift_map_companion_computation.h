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

    //! Adds the epoch watermarks of the input streams to |request|.
    static void AddWatermarks(
        const TCompanionProcessRequestPtr& request,
        const TComputationSpec& spec,
        const TWatermarkStatePtr& watermarkState);

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override;

    void DoInit(IJobInitContextPtr initContext) override;
};

DEFINE_REFCOUNTED_TYPE(TSwiftMapCompanionComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
