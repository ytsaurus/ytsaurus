#include "execution_graph_v3.h"

#include <ranges>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

std::vector<const TExecutionGraphV3::TGraphNode*> GetFlatGraph(const TExecutionGraphV3::TGraphNode* root)
{
    TSet<const TExecutionGraphV3::TGraphNode*> processedGraphNodes;
    std::vector<const TExecutionGraphV3::TGraphNode*> orderedGraphNodes;
    std::deque<const TExecutionGraphV3::TGraphNode*> queue;
    queue.push_back(root);
    while (!queue.empty()) {
        const TExecutionGraphV3::TGraphNode* cur = queue.front();
        queue.pop_front();
        if (processedGraphNodes.contains(cur)) {
            continue;
        } else {
            processedGraphNodes.insert(cur);
            orderedGraphNodes.push_back(cur);
        }
        for (const auto& output : cur->Outputs) {
            for (const auto& nextGraphNode : output) {
                queue.push_back(&nextGraphNode);
            }
        }
    }
    return orderedGraphNodes;
}

////////////////////////////////////////////////////////////////////////////////

IParallelExecutionBlockV3Ptr MakeExecutionBlock(NYT::IInvokerPtr invoker, int fibers, NYT::TCancelableContextPtr cancelableContext, const TParsedGraphV3::TRawExecutionBlock& rawExecutionBlock)
{
    auto factory = TExecutionBlockFactoryV3(rawExecutionBlock, std::move(cancelableContext));
    return factory.BuildParallelExecutionBlock(invoker, fibers);
}

//TODO: remove recursion
TExecutionGraphV3::TGraphNode BuildGraph(NYT::IInvokerPtr invoker, int fibers, NYT::TCancelableContextPtr cancelableContext, const TParsedGraphV3::TRawExecutionBlock& rawExecutionBlock)
{
    TExecutionGraphV3::TGraphNode current{
        .ExecutionBlock = MakeExecutionBlock(invoker, fibers, cancelableContext, rawExecutionBlock)
    };
    std::vector<std::vector<IParallelExecutionBlockV3InputPtr>> foreignOutputs;
    for (size_t i = 0; i < rawExecutionBlock.Outputs.size(); ++i) {
        current.Outputs.push_back({});
        foreignOutputs.push_back({});
        for (const auto* nextRawExecutionBlock : rawExecutionBlock.Outputs[i]) {
            current.Outputs[i].push_back(BuildGraph(invoker, fibers, cancelableContext, *nextRawExecutionBlock));
            foreignOutputs.back().push_back(current.Outputs[i].back().ExecutionBlock);
        }
    }
    current.ExecutionBlock->SetOutputs(std::move(foreignOutputs));
    return current;
}

////////////////////////////////////////////////////////////////////////////////

TExecutionGraphV3::TExecutionGraphV3(NYT::IInvokerPtr invoker, int fibers, NYT::TCancelableContextPtr cancelableContext, const TParsedGraphV3::TRawExecutionBlock& rawExecutionBlock)
{
    Root_ = BuildGraph(invoker, fibers, cancelableContext, rawExecutionBlock);
    FlatGraph_ = GetFlatGraph(&Root_);
}

void TExecutionGraphV3::SetOutputs(std::vector<std::vector<IParallelExecutionBlockV3InputPtr>> outputs)
{
    Y_UNUSED(outputs);
    Y_ABORT("Not implemented");
}
void TExecutionGraphV3::StartEpoch(const TBigRtExecutionContextArgs& contextArgs)
{
    for (const auto* graphNode : std::ranges::reverse_view{FlatGraph_}) {
        graphNode->ExecutionBlock->StartEpoch(contextArgs);
    }
}

[[nodiscard]] NYT::TFuture<void> TExecutionGraphV3::AsyncStartBundle()
{
    NYT::TFuture<void> last = NYT::VoidFuture;
    for (const auto* graphNode : std::ranges::reverse_view{FlatGraph_}) {
        last = last.Apply(BIND([graphNode]() {
            return graphNode->ExecutionBlock->AsyncStartBundle();
        }));
    }
    return last;
}

void TExecutionGraphV3::AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data)
{
    Root_.ExecutionBlock->AsyncBatchDo(std::move(data));
}

void TExecutionGraphV3::AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta rowWithMeta)
{
    Root_.ExecutionBlock->AsyncDo(std::move(rowWithMeta));
}

void TExecutionGraphV3::AsyncOnTimer(const TString& callbackId, std::vector<TTimer> readyTimers)
{
    Y_UNUSED(callbackId, readyTimers);
    Y_ABORT("Not implemented");
    //ExecutionBlock_->AsyncOnTimer(callbackId, std::move(readyTimers)); //TODO:
}

[[nodiscard]] NYT::TFuture<void> TExecutionGraphV3::AsyncFinishBundle()
{
    NYT::TFuture<void> last = NYT::VoidFuture;
    for (const auto* graphNode : FlatGraph_) {
        last = last.Apply(BIND([graphNode]() {
            return graphNode->ExecutionBlock->AsyncFinishBundle();
        }));
    }
    return last;
}

[[nodiscard]] NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> TExecutionGraphV3::FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseFinishFuture)
{
    NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> last = std::move(baseFinishFuture);
    for (const auto* graphNode : FlatGraph_) {
        last = graphNode->ExecutionBlock->FinishEpoch(last);
    }
    return last;
}

[[nodiscard]] THashMap<TString, TCreateBaseStateManagerFunction> TExecutionGraphV3::GetStateManagerFunctions() const noexcept
{
    THashMap<TString, TCreateBaseStateManagerFunction> result;
    for (const auto* graphNode : FlatGraph_) {
        auto stateManagerFunctions = graphNode->ExecutionBlock->GetStateManagerFunctions();
        result.insert(stateManagerFunctions.begin(), stateManagerFunctions.end());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate

