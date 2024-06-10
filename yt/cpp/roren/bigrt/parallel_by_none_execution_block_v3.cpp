#include "parallel_execution_block_v3.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TParallelByNoneExecutionBlockV3 //Non-parallel
    : public TParallelExecutionBlockV3
{
public:
    TParallelByNoneExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks);
    void AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta rowWithMeta) override;
    void AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data) override;

    NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseResultFuture) override;

private:
    std::vector<IRowWithMetaOutputPtr> MakeOutputs(ssize_t executionBlockIndex) override;
};

////////////////////////////////////////////////////////////////////////////////

TParallelByNoneExecutionBlockV3::TParallelByNoneExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks)
    : TParallelExecutionBlockV3(std::move(invoker), std::move(asyncExecutionBlocks))
{
    Y_ABORT_IF(AsyncExecutionBlocks_.size() != 1);
    for (ssize_t i = 0; i < std::ssize(Inputs_); ++i) {
        Inputs_[i] = MakeIntrusive<TAsyncExecutionBlockV3Input>();
        Outputs_[i] = MakeOutputs(i);
    }
}

std::vector<IRowWithMetaOutputPtr> TParallelByNoneExecutionBlockV3::MakeOutputs(ssize_t executionBlockIndex)
{
    std::vector<IRowWithMetaOutputPtr> outputs;
    for (const auto& outputTag : AsyncExecutionBlocks_[executionBlockIndex]->GetOutputTags()) {
        outputs.push_back(MakeIntrusive<TParallelExecutionBlockV3ProxyOutput>(*this, outputTag.GetRowVtable(), 0));
    }
    return outputs;
}

void TParallelByNoneExecutionBlockV3::AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta rowWithMeta)
{
    Inputs_[0]->PushData(std::move(rowWithMeta));
}

void TParallelByNoneExecutionBlockV3::AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data)
{
    while (!data.empty()) {
        for (auto& rowWithMeta : data.front()) {
            AsyncDo(std::move(rowWithMeta));
        }
        data.pop_front();
   }
}

NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> TParallelByNoneExecutionBlockV3::FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseResultFuture)
{
    GatherWrites();
    return TParallelExecutionBlockV3::FinishEpoch(std::move(baseResultFuture));
}

////////////////////////////////////////////////////////////////////////////////

IParallelExecutionBlockV3Ptr MakeParallelByNoneExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks)
{
    return MakeIntrusive<TParallelByNoneExecutionBlockV3>(std::move(invoker), std::move(asyncExecutionBlocks));
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
