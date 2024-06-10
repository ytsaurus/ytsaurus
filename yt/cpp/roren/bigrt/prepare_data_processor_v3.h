#pragma once

#include "parse_graph_v3.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TPrepareDataProcessorV3
{
public:
    using TPreparedData = NBigRT::TStatelessShardProcessor::TPreparedData;

    TPrepareDataProcessorV3(NYT::IInvokerPtr invoker, int fibers, const TString& inputTag, TParsedPipelineV3& parsedPipeline, NYT::TCancelableContextPtr cancelableContext);
    TPreparedData Process(const TBigRtExecutionContextArgs& contextArgs, NBigRT::TMessageBatch messageBatch);
    TDuration GetCpuTime() const;

private:
    TAccountCpuTimeInvokerPtr Invoker_;
    IParallelExecutionBlockV3Ptr ParallelExecutionBlock_;
};  // class TPrepareDataProcessorV3

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
