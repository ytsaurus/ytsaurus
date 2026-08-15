#include "computation.h"

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

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
        this->GetPartitionId(),
        this->GetSpec()->ProcessingFunctionParameters,
        this->GetContext()->StaticResources,
        this->GetContext()->Profiler);
    Function_->Init(runtimeInitContext);
}

template <class TBase>
void TProcessFunctionComputationBase<TBase>::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    RefreshRuntimeContext();
    Batch_->Process(input, output, RuntimeContext_);
}

template <class TBase>
void TProcessFunctionComputationBase<TBase>::DoSyncIfPresent(IRetryableTransactionPtr transaction)
{
    if (SyncFunction_) {
        RefreshRuntimeContext();
        SyncFunction_->Sync(transaction, RuntimeContext_);
    }
}

template <class TBase>
void TProcessFunctionComputationBase<TBase>::RefreshRuntimeContext()
{
    RuntimeContext_->RefreshEpochState(
        this->GetWatermarkState(),
        this->GetDynamicSpec()->ProcessingFunctionParameters,
        this->GetEpochUniqueSeqNo());
}

////////////////////////////////////////////////////////////////////////////////

template class TProcessFunctionComputationBase<TTransformComputation>;
template class TProcessFunctionComputationBase<TSwiftMapComputation>;
template class TProcessFunctionComputationBase<TTransformOrderedSourceComputation>;
template class TProcessFunctionComputationBase<TSwiftOrderedSourceComputation>;

////////////////////////////////////////////////////////////////////////////////

void TProcessFunctionComputation::DoSync(IRetryableTransactionPtr transaction)
{
    DoSyncIfPresent(std::move(transaction));
}

void TProcessFunctionTransformOrderedSourceComputation::DoSync(IRetryableTransactionPtr transaction)
{
    DoSyncIfPresent(std::move(transaction));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
