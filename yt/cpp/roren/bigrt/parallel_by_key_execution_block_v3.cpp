#include "parallel_execution_block_v3.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TParallelByKeyExecutionBlockV3
    : public TParallelExecutionBlockV3
{
public:
    TParallelByKeyExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks);
    void AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta data) override;
    void AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data) override;
    //void AsyncOnTimer(const TString& callbackId, std::vector<TTimer> readyTimers) override;

    [[nodiscard]] NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseFinishFuture) override;

private:
    //using TOutputContainer = std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV>;
    std::vector<IRowWithMetaOutputPtr> MakeOutputs(ssize_t executionBlockIndex) override;
};  // class TParallelByKeyExecutionBlockV3

////////////////////////////////////////////////////////////////////////////////

TParallelByKeyExecutionBlockV3::TParallelByKeyExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks)
    : TParallelExecutionBlockV3(std::move(invoker), std::move(asyncExecutionBlocks))
{
    for (ssize_t i = 0; i < std::ssize(Inputs_); ++i) {
        Inputs_[i] = MakeIntrusive<TAsyncExecutionBlockV3Input>();
        Outputs_[i] = MakeOutputs(i);
    }
}

std::vector<IRowWithMetaOutputPtr> TParallelByKeyExecutionBlockV3::MakeOutputs(ssize_t executionBlockIndex)
{
    std::vector<IRowWithMetaOutputPtr> outputs;
    for (const auto& outputTag : AsyncExecutionBlocks_[executionBlockIndex]->GetOutputTags()) {
        outputs.push_back(MakeIntrusive<TParallelExecutionBlockV3ProxyOutput>(*this, outputTag.GetRowVtable(), 0));
    }
    return outputs;
}

void TParallelByKeyExecutionBlockV3::AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data)
{
    while (!data.empty()) {
        for (auto& rowWithMeta : data.front()) {
            AsyncDo(std::move(rowWithMeta));
        }
        data.pop_front();
    }
}

void TParallelByKeyExecutionBlockV3::AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta rowWithMeta)
{
    const int i = rowWithMeta.KeyHash % AsyncExecutionBlocks_.size();
    Inputs_[i]->PushData(std::move(rowWithMeta));
}

NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> TParallelByKeyExecutionBlockV3::FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseResultFuture)
{
    GatherWrites();
    return TParallelExecutionBlockV3::FinishEpoch(std::move(baseResultFuture));
}

////////////////////////////////////////////////////////////////////////////////

IParallelExecutionBlockV3Ptr MakeParallelByKeyExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks)
{
    return MakeIntrusive<TParallelByKeyExecutionBlockV3>(std::move(invoker), std::move(asyncExecutionBlocks));
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
