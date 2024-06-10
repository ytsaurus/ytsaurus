#pragma once

#include "parse_graph_v3.h"
#include "parallel_execution_block_v3.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TProcessPreparedDataProcessorV3
{
public:
    using TPreparedData = NBigRT::TStatelessShardProcessor::TPreparedData;

    TProcessPreparedDataProcessorV3(NYT::IInvokerPtr invoker, int fibers, const TString& inputTag, TParsedPipelineV3& parsedPipeline, NYT::TCancelableContextPtr cancelableContext);
    void StartEpoch(const TBigRtExecutionContextArgs& contextArgs);
    void ProcessPreparedData(const TString& dataSource, std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data);
    NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseFinishFuture);

    THashMap<TString, TCreateBaseStateManagerFunction> GetStateManagerFunctions() const noexcept;

    TDuration GetCpuTime() const;

private:
    void ProcessPreparedTimers(const TString& dataSource, std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data);
    void ProcessPreparedInputData(const TString& dataSource, std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data);

private:
    TAccountCpuTimeInvokerPtr Invoker_;
    IParallelExecutionBlockV3Ptr ParallelExecutionBlock_;  //TODO: replace with ParallelExecutionGraph
};  // class TProcessPreparedDataProcessorV3

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
