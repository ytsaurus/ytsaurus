#include "parallel_execution_block_v3.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TParallelByPartExecutionBlockV3
    : public TParallelExecutionBlockV3
{
public:
    TParallelByPartExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks);
    void AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data) override;

    [[nodiscard]] NYT::TFuture<void> AsyncFinishBundle() override;

private:
    std::vector<IRowWithMetaOutputPtr> MakeOutputs(ssize_t executionBlockIndex) override;

private:
    std::vector<TParallelExecutionBlockV3BufferedOutput::TContainer> Results_;
};  // class TParallelByPartExecutionBlockV3

////////////////////////////////////////////////////////////////////////////////

TParallelByPartExecutionBlockV3::TParallelByPartExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks)
    : TParallelExecutionBlockV3(std::move(invoker), std::move(asyncExecutionBlocks))
{
    Results_.resize(AsyncExecutionBlocks_.size());
    for (size_t i = 0; i < Inputs_.size(); ++i) {
        Inputs_[i] = MakeIntrusive<TAsyncExecutionBlockV3Input>();
        Outputs_[i] = MakeOutputs(i);
    }
}

std::vector<IRowWithMetaOutputPtr> TParallelByPartExecutionBlockV3::MakeOutputs(ssize_t executionBlockIndex)
{
    std::vector<IRowWithMetaOutputPtr> outputs;
    for (const auto& outputTag : AsyncExecutionBlocks_[executionBlockIndex]->GetOutputTags()) {
        outputs.push_back(MakeIntrusive<TParallelExecutionBlockV3BufferedOutput>(outputTag.GetRowVtable(), Results_[executionBlockIndex]));
    }
    return outputs;
}

void TParallelByPartExecutionBlockV3::AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data)
{
    size_t count = 0;
    for (const auto& rows : data) {
        count += rows.size();
    }

    const size_t perFiber = (count + AsyncExecutionBlocks_.size() - 1) / AsyncExecutionBlocks_.size();  // ceil(a/b)
    size_t i = 0;
    for (auto& rows : data) {
        for (auto& row : rows) {
            const size_t fiberIndex = i / perFiber;
            Inputs_[fiberIndex]->PushData(std::move(row));
            ++i;
        }
    }
    Y_ABORT_IF(i != count);
}

NYT::TFuture<void> TParallelByPartExecutionBlockV3::AsyncFinishBundle()
{
    return TParallelExecutionBlockV3::AsyncFinishBundle().Apply<void>(BIND([this] () {
        GatherWrites();
        Y_ABORT_IF(this->ForeignOutputs_.size() != 1);  //TODO: support for multiple outputs
        for (const auto& output: this->ForeignOutputs_) {
            Y_ABORT_IF(output.size() != 1);  //TODO: support for multiple outputs
            std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> result;
            for (auto& resultPart : Results_) {
                for (auto& row : resultPart) {
                    result.push_back(std::move(row));
                }
            }
            output[0]->AsyncBatchDo(std::move(result));
        }
        return NYT::VoidFuture;
    }));
}

////////////////////////////////////////////////////////////////////////////////

IParallelExecutionBlockV3Ptr MakeParallelByPartExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks)
{
    return MakeIntrusive<TParallelByPartExecutionBlockV3>(std::move(invoker), std::move(asyncExecutionBlocks));
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
