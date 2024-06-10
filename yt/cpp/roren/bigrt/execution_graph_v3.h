#pragma once

#include "parse_graph_v3.h"
#include "parallel_execution_block_v3.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TExecutionGraphV3
    : public IParallelExecutionBlockV3
{
public:
    struct TGraphNode {
        IParallelExecutionBlockV3Ptr ExecutionBlock;
        std::vector<std::vector<TGraphNode>> Outputs;
    };

    TExecutionGraphV3(NYT::IInvokerPtr invoker, int fibers, NYT::TCancelableContextPtr cancelableContext, const TParsedGraphV3::TRawExecutionBlock& rawExecutionBlock);

    void StartEpoch(const TBigRtExecutionContextArgs& contextArgs) override;
    [[nodiscard]] NYT::TFuture<void> AsyncStartBundle() override;
    void AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data) override;
    void AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta rowWithMeta) override;
    void AsyncOnTimer(const TString& callbackId, std::vector<TTimer> readyTimers) override;
    [[nodiscard]] NYT::TFuture<void> AsyncFinishBundle() override;
    [[nodiscard]] NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseFinishFuture) override;
    [[nodiscard]] THashMap<TString, TCreateBaseStateManagerFunction> GetStateManagerFunctions() const noexcept override;

private:
    void SetOutputs(std::vector<std::vector<IParallelExecutionBlockV3InputPtr>> foreignOutputs) override;

private:
    IParallelExecutionBlockV3Ptr ExecutionBlock_;
    TGraphNode Root_;
    std::vector<const TGraphNode*> FlatGraph_;
};  // class TExecutionGraphV3

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate

