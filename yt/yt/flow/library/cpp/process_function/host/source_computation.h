#pragma once

#include "computation.h"

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Source computation that runs the process function named by the spec's `processing_function`
//! field over the whole epoch's source messages.
class TProcessFunctionSourceComputation
    : public TProcessFunctionComputationBase<TSwiftOrderedSourceComputation>
{
public:
    static constexpr bool RequiresProcessingFunction = true;

    TProcessFunctionSourceComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
