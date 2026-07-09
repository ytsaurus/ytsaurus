#include "source_computation.h"

#include "computation.h"

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TProcessFunctionSourceComputation::TProcessFunctionSourceComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TSwiftOrderedSourceComputation(std::move(context), std::move(dynamicContext))
    , Function_(CreateProcessFunction(GetSpec()))
    , Batch_(WrapAsBatch(Function_))
    , RuntimeContext_(New<TComputationRuntimeContext>(
        GetSpec(),
        GetContext()->StreamSpecStorage,
        GetKeySchema(),
        GetContext()->ConverterCache,
        GetThrottlerFactory()))
{ }

void TProcessFunctionSourceComputation::DoInit(IJobInitContextPtr initContext)
{
    auto runtimeInitContext = New<TRuntimeInitContext>(
        std::move(initContext),
        StateManager_,
        GetSpec()->ProcessingFunctionParameters);
    Function_->Init(runtimeInitContext);
}

void TProcessFunctionSourceComputation::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    RuntimeContext_->RefreshEpochState(GetWatermarkState(), GetDynamicSpec()->ProcessingFunctionParameters);
    Batch_->Process(input, output, RuntimeContext_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
