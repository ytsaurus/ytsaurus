#include "parallel_execution_block_v3.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TPrepareDataExecutionBlockInput
    : public IAsyncExecutionBlockV3Input
{
public:
    TPrepareDataExecutionBlockInput();
    void Init(NBigRT::TMessageBatch messageBatch) noexcept;
    [[nodiscard]] const std::vector<int>& GetFibersOrder() const;

    [[nodiscard]] std::optional<NBigRT::TStatelessShardProcessor::TRowWithMeta> PopData(int fiberIndex) override;
    void PushData(NBigRT::TStatelessShardProcessor::TRowWithMeta value) override;
    [[nodiscard]] NYT::TFuture<void> GetDataReady() override;

    [[nodiscard]] std::optional<std::pair<TString, std::vector<TTimer>>> PopTimer(int fiberIndex) override;
    void PushTimer(std::pair<TString, std::vector<TTimer>> value) override;
    [[nodiscard]] NYT::TFuture<void> GetTimerReady() override;

private:
    std::optional<NBigRT::TMessageBatch::TMessage> PopMessage(int fiberIndex);

private:
    NYT::TPromise<void> UnsetVoidPromise_;
    NYT::TFuture<void> UnsetVoidFuture_;
    NBigRT::TMessageBatch MessageBatch_;
    TSpinLock Lock_;
    size_t MessageIndex_ = 0;
    std::vector<int> FibersOrder_;
};  // class TPrepareDataExecutionBlockInput

using TPrepareDataExecutionBlockInputPtr = TIntrusivePtr<TPrepareDataExecutionBlockInput>;

////////////////////////////////////////////////////////////////////////////////

TPrepareDataExecutionBlockInput::TPrepareDataExecutionBlockInput()
    : UnsetVoidPromise_(NYT::NewPromise<void>())
    , UnsetVoidFuture_(UnsetVoidPromise_.ToFuture())
{
}

void TPrepareDataExecutionBlockInput::Init(NBigRT::TMessageBatch messageBatch) noexcept
{
    MessageBatch_ = std::move(messageBatch);
    MessageIndex_ = 0;
    FibersOrder_.clear();
}

std::optional<NBigRT::TMessageBatch::TMessage> TPrepareDataExecutionBlockInput::PopMessage(int fiberIndex)
{
    auto guard = Guard(Lock_);
    if (MessageIndex_ < MessageBatch_.Messages.size()) {
        FibersOrder_.push_back(fiberIndex);
        auto& message = MessageBatch_.Messages[MessageIndex_];
        MessageIndex_++;
        return std::optional<NBigRT::TMessageBatch::TMessage>(std::move(message));
    }
    return std::nullopt;
}

std::optional<NBigRT::TStatelessShardProcessor::TRowWithMeta> TPrepareDataExecutionBlockInput::PopData(int fiberIndex)
{
    auto message = PopMessage(fiberIndex);
    if (!message) {
        return std::nullopt;
    }
    message->Unpack();
    NBigRT::TStatelessShardProcessor::TRowMeta meta(*message);
    const size_t size = message->UnpackedData().size();
    return NBigRT::TStatelessShardProcessor::TRowWithMeta(std::move(message->UnpackedData()), size, std::move(meta));
}

void TPrepareDataExecutionBlockInput::PushData(NBigRT::TStatelessShardProcessor::TRowWithMeta value)
{
    Y_UNUSED(value);
    Y_ABORT();
}

NYT::TFuture<void> TPrepareDataExecutionBlockInput::GetDataReady()
{
    auto guard = Guard(Lock_);
    if (MessageIndex_ < MessageBatch_.Messages.size()) {
        return NYT::VoidFuture;
    }
    return UnsetVoidFuture_;
}

std::optional<std::pair<TString, std::vector<TTimer>>> TPrepareDataExecutionBlockInput::PopTimer(int fiberIndex)
{
    Y_UNUSED(fiberIndex);
    Y_ABORT();
}

void TPrepareDataExecutionBlockInput::PushTimer(std::pair<TString, std::vector<TTimer>> value)
{
    Y_UNUSED(value);
    Y_ABORT();
}

NYT::TFuture<void> TPrepareDataExecutionBlockInput::GetTimerReady()
{
    return UnsetVoidFuture_;
}

const std::vector<int>& TPrepareDataExecutionBlockInput::GetFibersOrder() const
{
    return FibersOrder_;
}

////////////////////////////////////////////////////////////////////////////////

class TParallelByGreedyExecutionBlockV3
    : public TParallelExecutionBlockV3
{
public:
    TParallelByGreedyExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks);

    void AsyncMessageBatchDo(NBigRT::TMessageBatch messageBatch) override;

    [[nodiscard]] NYT::TFuture<void> AsyncFinishBundle() override;

private:
    std::vector<IRowWithMetaOutputPtr> MakeOutputs(ssize_t executionBlockIndex) override;

private:
    TPrepareDataExecutionBlockInputPtr SharedInput_;
    std::vector<TParallelExecutionBlockV3BufferedOutput::TContainer> Results_;
};  // class TParallelByGreedyExecutionBlockV3

////////////////////////////////////////////////////////////////////////////////

TParallelByGreedyExecutionBlockV3::TParallelByGreedyExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks)
    : TParallelExecutionBlockV3(std::move(invoker), std::move(asyncExecutionBlocks))
    , SharedInput_(MakeIntrusive<TPrepareDataExecutionBlockInput>())
{
    Results_.resize(AsyncExecutionBlocks_.size());
    for (size_t i = 0; i < AsyncExecutionBlocks_.size(); ++i) {
        Inputs_[i] = SharedInput_;
        Outputs_[i] = MakeOutputs(i);
    }
}

std::vector<IRowWithMetaOutputPtr> TParallelByGreedyExecutionBlockV3::MakeOutputs(ssize_t executionBlockIndex)
{
    std::vector<IRowWithMetaOutputPtr> outputs;
    for (const auto& outputTag : AsyncExecutionBlocks_[executionBlockIndex]->GetOutputTags()) {
        outputs.push_back(MakeIntrusive<TParallelExecutionBlockV3BufferedOutput>(outputTag.GetRowVtable(), Results_[executionBlockIndex]));
    }
    return outputs;
}

void TParallelByGreedyExecutionBlockV3::AsyncMessageBatchDo(NBigRT::TMessageBatch messageBatch)
{
    SharedInput_->Init(std::move(messageBatch));
}

NYT::TFuture<void> TParallelByGreedyExecutionBlockV3::AsyncFinishBundle()
{
    return TParallelExecutionBlockV3::AsyncFinishBundle().Apply<void>(BIND([this] () {
        std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> outputResult;
        for (const int i : SharedInput_->GetFibersOrder()) {
            outputResult.emplace_back(std::move(Results_[i].front()));
            Results_[i].pop_front();
        }
        const size_t outputsCount = ForeignOutputs_.size();
        Y_ABORT_IF(outputsCount != 1); //TODO: supoprt for many outputs
        //const size_t lastOutputIndex = outputsCount -1;
        for (size_t i = 0; i < outputsCount; ++i) {
            Y_ABORT_IF(ForeignOutputs_[i].size() != 1);  //TODO: support for many outputs
            for (const auto& nextExecutionBlock : ForeignOutputs_[i]) {
                nextExecutionBlock->AsyncBatchDo(std::move(outputResult));
            }
            //} else {
            //    ForeignOutputs_[i]->AsyncBatchDo(outputResult);
            //}
        }
        GatherWrites();
        return NYT::VoidFuture;
    }));
}

////////////////////////////////////////////////////////////////////////////////

IParallelExecutionBlockV3Ptr MakeParallelByGreedyExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks)
{
    return MakeIntrusive<TParallelByGreedyExecutionBlockV3>(std::move(invoker), std::move(asyncExecutionBlocks));
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
