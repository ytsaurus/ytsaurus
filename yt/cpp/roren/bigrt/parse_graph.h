#pragma once

#include "fwd.h"

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

TParseResult ParseBigRtPipeline(const TPipeline& pipeline);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
