#pragma once

#include "computation_runtime_context.h"
#include "runtime_init_context.h"

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/process_function.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Source computation that runs the process function named by the spec's `processing_function`
//! field over the whole epoch's source messages.
class TProcessFunctionSourceComputation
    : public TSwiftOrderedSourceComputation
{
public:
    static constexpr bool RequiresProcessingFunction = true;

    TProcessFunctionSourceComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    void DoInit(IJobInitContextPtr initContext) override;
    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override;

private:
    const IProcessFunctionBasePtr Function_;
    //! Function_ normalized to the whole-epoch batch form the worker drives.
    const IBatchProcessFunctionPtr Batch_;
    const TComputationRuntimeContextPtr RuntimeContext_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
