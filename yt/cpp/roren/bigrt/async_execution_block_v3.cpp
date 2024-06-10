#include "async_execution_block_v3.h"

namespace NRoren::NPrivate {

static NYT::TError FiberCanceledError("Fiber was canceled by invoker. Thread pool is dead?");

////////////////////////////////////////////////////////////////////////////////

std::optional<NBigRT::TStatelessShardProcessor::TRowWithMeta> TAsyncExecutionBlockV3Input::PopData(int fiberIndex)
{
    Y_UNUSED(fiberIndex);
    return Data_.TryPop();
}

void TAsyncExecutionBlockV3Input::PushData(NBigRT::TStatelessShardProcessor::TRowWithMeta value)
{
    Data_.Push(std::move(value));
}

NYT::TFuture<void> TAsyncExecutionBlockV3Input::GetDataReady()
{
    return Data_.GetReady();
}

std::optional<std::pair<TString, std::vector<TTimer>>> TAsyncExecutionBlockV3Input::PopTimer(int fiberIndex)
{
    Y_UNUSED(fiberIndex);
    return Timers_.TryPop();
}

void TAsyncExecutionBlockV3Input::PushTimer(std::pair<TString, std::vector<TTimer>> value)
{
    Timers_.Push(std::move(value));
}

NYT::TFuture<void> TAsyncExecutionBlockV3Input::GetTimerReady()
{
    return Timers_.GetReady();
}

////////////////////////////////////////////////////////////////////////////////

TAsyncExecutionBlockV3::TAsyncExecutionBlockV3(int fiberIndex, NYT::IInvokerPtr invoker, TSyncExecutionBlockV3Ptr executionBlock, NYT::TCancelableContextPtr cancelableContext)
    : FiberIndex_(fiberIndex)
    , Invoker_(std::move(invoker))
    , ExecutionBlock_(std::move(executionBlock))
    , CancelableContext_(std::move(cancelableContext))
{
}

TAsyncExecutionBlockV3::~TAsyncExecutionBlockV3()
{
    if (NeedFinish_) {
        NeedFinish_.ToFuture().Cancel(NYT::TError("Terminate"));
        IsFiberTerminated_.Get();
    }
}

NYT::TFuture<void> TAsyncExecutionBlockV3::AsyncStartBundle(IBigRtExecutionContextPtr context, IAsyncExecutionBlockV3InputPtr input, std::vector<IRowWithMetaOutputPtr> outputs)
{
    Input_ = std::move(input);
    IsFiberTerminated_ = NYT::NewPromise<void>();
    NeedFinish_ = NYT::NewPromise<void>();
    if (CancelableContext_) {
        CancelableContext_->PropagateTo(NeedFinish_);
    }

    auto startBundleFuture = BIND([this, context=std::move(context), outputs=std::move(outputs)] () mutable {
        this->ExecutionBlock_->StartBundle(std::move(context), std::move(outputs));
    }).AsyncViaGuarded(Invoker_, FiberCanceledError).Run();
    startBundleFuture.Subscribe(BIND([this] (const NYT::TErrorOr<void>& error) {
        if (error.IsOK()) {
            Invoker_->Invoke(BIND([this] () {
                this->Loop();
            }));
        }
    }));
    return startBundleFuture;
}

void TAsyncExecutionBlockV3::AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta data)
{
    Input_->PushData(std::move(data));
}

void TAsyncExecutionBlockV3::AsyncOnTimer(const TString& callbackId, std::vector<TTimer> readyTimers)
{
    Input_->PushTimer({callbackId, std::move(readyTimers)});
}

NYT::TFuture<void> TAsyncExecutionBlockV3::AsyncFinishBundle()
{
    NeedFinish_.Set();
    return IsFiberTerminated_;
}

void TAsyncExecutionBlockV3::Loop() noexcept
try {
    // Working Loop. Run via fiber.
    const TDuration delay = TDuration::MilliSeconds(500);
    NYT::TFuture<void> yieldTimerFuture = NYT::NConcurrency::TDelayedExecutor::MakeDelayed(delay);
    for (;;) {
        auto dataFuture = Input_->GetDataReady();
        auto timerFuture = Input_->GetTimerReady();
        auto futureResult = NYT::NConcurrency::WaitForFast(
            NYT::AnySet<void>({dataFuture, timerFuture, NeedFinish_.ToFuture(), yieldTimerFuture}, {false, false})
        );
        if (NeedFinish_.IsCanceled()) {
            IsFiberTerminated_.Set();
            return;
        }
        Y_ABORT_IF(!futureResult.IsOK());  // Useful for debug
        if (yieldTimerFuture.IsSet()) {
            NYT::NConcurrency::Yield();
            yieldTimerFuture = NYT::NConcurrency::TDelayedExecutor::MakeDelayed(delay);
            continue;
        } else if (dataFuture.IsSet()) {
            while(auto value = Input_->PopData(FiberIndex_)) {
                ExecutionBlock_->Do(std::move(*value));
            }
            continue;
        } else if (timerFuture.IsSet()) {
            while (auto value = Input_->PopTimer(FiberIndex_)) {
                ExecutionBlock_->OnTimer(value->first, std::move(value->second));
            }
            continue;
        }
        ExecutionBlock_->FinishBundle();
        IsFiberTerminated_.Set();
        return;
    }
} catch (const std::exception& e) {
    IsFiberTerminated_.Set(NYT::TError(e));
} catch (...) {
    Y_ABORT();
}

const THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr>& TAsyncExecutionBlockV3::GetTimersCallbacks() const noexcept
{
    return ExecutionBlock_->GetTimersCallbacks();
}

const THashMap<TString, TCreateBaseStateManagerFunction>& TAsyncExecutionBlockV3::GetStateManagerFunctions() const noexcept
{
    return ExecutionBlock_->GetStateManagerFunctions();
}

TRowVtable TAsyncExecutionBlockV3::GetInputVtable() const noexcept
{
    return ExecutionBlock_->GetInputVtable();
}

std::vector<TDynamicTypeTag> TAsyncExecutionBlockV3::GetOutputTags() const noexcept
{
    return ExecutionBlock_->GetOutputTags();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
