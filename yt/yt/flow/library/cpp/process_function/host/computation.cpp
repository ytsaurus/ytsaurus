#include "computation.h"

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

IProcessFunctionBasePtr CreateProcessFunction(
    const TComputationSpecPtr& spec,
    const TProcessFunctionContextPtr& context)
{
    YT_VERIFY(spec->ProcessingFunction);
    return TRegistry::Get()->CreateProcessFunction(*spec->ProcessingFunction, context);
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
    YT_VERIFY(!Function_);

    auto context = New<TProcessFunctionContext>();
    context->InitContext = New<TRuntimeInitContext>(
        std::move(initContext),
        this->StateManager_,
        this->GetPartitionId(),
        this->GetSpec()->ProcessingFunctionParameters,
        TRegistry::Get()->ParseProcessFunctionParameters(this->GetSpec()),
        this->GetContext()->StaticResources,
        this->GetContext()->Profiler,
        this->GetContext()->HttpClient,
        this->GetContext()->HttpsClient);
    context->ClientsCache = this->GetContext()->ClientsCache;
    context->Invoker = this->GetContext()->SerializedInvoker;
    context->RetryableClient =
        this->GetRetryableClient()->WithErrorComponent("/process_function/default");
    context->Logger =
        this->Logger.WithTag("ProcessingFunction", *this->GetSpec()->ProcessingFunction);
    context->StatusProfiler = this->GetContext()->StatusProfiler->WithPrefix("/process_function");

    Function_ = CreateProcessFunction(this->GetSpec(), context);
    Batch_ = WrapAsBatch(Function_);
    SyncFunction_ = ViewProcessFunctionAsSync(this->GetSpec(), Function_);
    Function_->Init(context->InitContext);
}

template <class TBase>
void TProcessFunctionComputationBase<TBase>::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    YT_VERIFY(Batch_);
    RefreshRuntimeContext();
    Batch_->Process(input, output, RuntimeContext_);
}

template <class TBase>
void TProcessFunctionComputationBase<TBase>::DoSyncIfPresent(IRetryableTransactionPtr transaction)
{
    YT_VERIFY(Function_);
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
