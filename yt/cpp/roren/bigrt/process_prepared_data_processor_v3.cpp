#include "process_prepared_data_processor_v3.h"

#include "supplier.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

TProcessPreparedDataProcessorV3::TProcessPreparedDataProcessorV3(NYT::IInvokerPtr invoker, int fibers, const TString& inputTag, TParsedPipelineV3& parsedPipeline, NYT::TCancelableContextPtr cancelableContext)
    : Invoker_(NYT::New<TAccountCpuTimeInvoker>(std::move(invoker)))
{
    ParallelExecutionBlock_ = parsedPipeline.GetProcessPreparedDataExecutionBlock(Invoker_, fibers, inputTag, std::move(cancelableContext));
}

TDuration TProcessPreparedDataProcessorV3::GetCpuTime() const
{
    //TODO:
    return Invoker_->GetCpuTime();
}

void TProcessPreparedDataProcessorV3::StartEpoch(const TBigRtExecutionContextArgs& contextArgs)
{
    ParallelExecutionBlock_->StartEpoch(contextArgs);
}

NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> TProcessPreparedDataProcessorV3::FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseFinishFuture)
{
    return ParallelExecutionBlock_->FinishEpoch(std::move(baseFinishFuture));
}

void TProcessPreparedDataProcessorV3::ProcessPreparedData(const TString& dataSource, std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data)
{
    if (dataSource == NPrivate::SYSTEM_TIMERS_SUPPLIER_NAME) {
        ProcessPreparedTimers(dataSource, std::move(data));
    } else {
        ProcessPreparedInputData(dataSource, std::move(data));
    }
}

void TProcessPreparedDataProcessorV3::ProcessPreparedTimers(const TString& dataSource, std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data)
{
    Y_ABORT_IF(dataSource != NPrivate::SYSTEM_TIMERS_SUPPLIER_NAME);

    THashMap<TString, std::vector<TTimer>> timersMap;
    for (auto& preparedMessage : data) {
        for (auto& rowWithMeta : preparedMessage) {
            auto& timer = rowWithMeta.GetValueAs<NPrivate::TTimerProto>();
            timersMap[timer.GetKey().GetCallbackId()].emplace_back(std::move(timer));
        }
    }
    data.clear();
    for (auto& [callbackId, readyTimers] : timersMap) {
        ParallelExecutionBlock_->AsyncOnTimer(callbackId, std::move(readyTimers));
    }
    NYT::NConcurrency::WaitForFast(ParallelExecutionBlock_->AsyncStartBundle()).ThrowOnError();
    NYT::NConcurrency::WaitForFast(ParallelExecutionBlock_->AsyncFinishBundle()).ThrowOnError();
}

void TProcessPreparedDataProcessorV3::ProcessPreparedInputData(const TString& dataSource, std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data)
{
    Y_UNUSED(dataSource);
    ParallelExecutionBlock_->AsyncBatchDo(std::move(data));
    NYT::NConcurrency::WaitForFast(ParallelExecutionBlock_->AsyncStartBundle()).ThrowOnError();
    NYT::NConcurrency::WaitForFast(ParallelExecutionBlock_->AsyncFinishBundle()).ThrowOnError();
}

THashMap<TString, TCreateBaseStateManagerFunction> TProcessPreparedDataProcessorV3::GetStateManagerFunctions() const noexcept
{
    return ParallelExecutionBlock_->GetStateManagerFunctions();
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
