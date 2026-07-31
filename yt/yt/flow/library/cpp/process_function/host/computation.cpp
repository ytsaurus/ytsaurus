#include "computation.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

IProcessFunctionBasePtr CreateProcessFunction(const TComputationSpecPtr& spec)
{
    // Spec validation (TRegistry::ValidatePipelineSpecParseability) already guaranteed the field
    // is set and names a registered function for an adapter computation.
    YT_VERIFY(spec->ProcessingFunction);
    return TRegistry::Get()->CreateProcessFunction(*spec->ProcessingFunction);
}

ISyncProcessFunction* ViewProcessFunctionAsSync(const TComputationSpecPtr& spec, const IProcessFunctionBasePtr& function)
{
    YT_VERIFY(spec->ProcessingFunction);
    return TRegistry::Get()->ViewProcessFunctionAsSync(*spec->ProcessingFunction, function);
}

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
TProcessFunctionComputationBase<TBase>::TProcessFunctionComputationBase(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TBase(std::move(context), std::move(dynamicContext))
    , Function_(CreateProcessFunction(this->GetSpec()))
    , Batch_(WrapAsBatch(Function_))
    , RuntimeContext_(New<TComputationRuntimeContext>(
        this->GetSpec(),
        this->GetContext()->StreamSpecStorage,
        this->GetKeySchema(),
        this->GetContext()->ConverterCache,
        this->GetThrottlerFactory()))
{ }

template <class TBase>
void TProcessFunctionComputationBase<TBase>::DoInit(IJobInitContextPtr initContext)
{
    auto runtimeInitContext = New<TRuntimeInitContext>(
        std::move(initContext),
        this->StateManager_,
        this->GetSpec()->ProcessingFunctionParameters,
        this->GetContext()->StaticResources);
    Function_->Init(runtimeInitContext);
}

template <class TBase>
void TProcessFunctionComputationBase<TBase>::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    RuntimeContext_->RefreshEpochState(this->GetWatermarkState(), this->GetDynamicSpec()->ProcessingFunctionParameters);
    Batch_->Process(input, output, RuntimeContext_);
}

////////////////////////////////////////////////////////////////////////////////

template class TProcessFunctionComputationBase<TTransformComputation>;
template class TProcessFunctionComputationBase<TSwiftMapComputation>;

////////////////////////////////////////////////////////////////////////////////

void TProcessFunctionComputation::DoSync(IRetryableTransactionPtr transaction)
{
    if (SyncFunction_) {
        SyncFunction_->Sync(transaction, RuntimeContext_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
