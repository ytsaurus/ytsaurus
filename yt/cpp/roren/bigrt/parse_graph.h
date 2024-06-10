#pragma once

#include "fwd.h"

#include "stateful_timer_impl/stateful_timer_par_do.h"
#include "execution_block.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/tvm/service/public.h>

#include <vector>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

struct TParseResult
{
    IExecutionBlockPtr ExecutionBlock;
    std::vector<TRegisterWriterFunction> RegisterWriterFunctionList;
    std::vector<std::pair<TString, TCreateBaseStateManagerFunction>> CreateStateManagerFunctionList;
    TString InputTag;
};

using TRegisterExecutionBlockTimers = std::function<void (const NPrivate::IExecutionBlockPtr executionBlock)>;

std::vector<TParseResult> ParseBigRtPipeline(const TPipeline& pipeline, const TRegisterExecutionBlockTimers registerTimerExecutionBlock, TVCpuMetricsPtr vcpuMetrics = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
