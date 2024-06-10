#include "prepare_data_processor_v3.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TParallelExecutionBlockBufferOutput
    : public IParallelExecutionBlockV3Input
{
public:
    void AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data) override;
    std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> Get();
private:
    std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> Result_;
};  //class TParallelExecutionBlockBufferOutput

using TParallelExecutionBlockBufferOutputPtr = TIntrusivePtr<TParallelExecutionBlockBufferOutput>;

////////////////////////////////////////////////////////////////////////////////

void TParallelExecutionBlockBufferOutput::AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data)
{
    Y_ABORT_IF(Result_.size());
    Result_ = std::move(data);
}

std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> TParallelExecutionBlockBufferOutput::Get()
{
    return std::move(Result_);
}

////////////////////////////////////////////////////////////////////////////////

TPrepareDataProcessorV3::TPrepareDataProcessorV3(NYT::IInvokerPtr invoker, int fibers, const TString& inputTag, TParsedPipelineV3& parsedPipeline, NYT::TCancelableContextPtr cancelableContext)
    : Invoker_(NYT::New<TAccountCpuTimeInvoker>(std::move(invoker)))
{
    ParallelExecutionBlock_ = parsedPipeline.GetPrepareDataExecutionBlock(Invoker_, fibers, inputTag, std::move(cancelableContext));
}

TPrepareDataProcessorV3::TPreparedData TPrepareDataProcessorV3::Process(const TBigRtExecutionContextArgs& contextArgs, NBigRT::TMessageBatch messageBatch)
{
    TPreparedData result;
    result.DataSources = messageBatch.DataSources;
    result.RequireAtomicCommit = messageBatch.RequireAtomicCommit;
    TParallelExecutionBlockBufferOutputPtr bufferOutput = MakeIntrusive<TParallelExecutionBlockBufferOutput>();
    ParallelExecutionBlock_->SetOutputs({{bufferOutput}});
    ParallelExecutionBlock_->StartEpoch(contextArgs);
    ParallelExecutionBlock_->AsyncMessageBatchDo(std::move(messageBatch));
    NYT::NConcurrency::WaitForFast(ParallelExecutionBlock_->AsyncStartBundle()).ThrowOnError();
    NYT::NConcurrency::WaitForFast(ParallelExecutionBlock_->AsyncFinishBundle()).ThrowOnError();
    #if 0
    NYT::NConcurrency::WaitForFast(ParallelExecutionBlock_->FinishEpoch(
        NYT::MakeFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult>({}))
    ).ThrowOnError(); //TODO: check: result must be empty
    #endif
    result.Data = bufferOutput->Get();
    return result;
}

TDuration TPrepareDataProcessorV3::GetCpuTime() const
{
    return Invoker_->GetCpuTime();
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
