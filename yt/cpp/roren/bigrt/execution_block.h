#pragma once

#include "fwd.h"

#include "concurrency_transforms.h"

#include <yt/cpp/roren/interface/roren.h>

#include <library/cpp/yt/memory/ref.h>
#include <yt/yt/core/actions/public.h>

#include <util/generic/ptr.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using TExecutionBlockConfig = std::variant<TSerializedBlockConfig, TConcurrentBlockConfig>;

////////////////////////////////////////////////////////////////////////////////

// Block of an execution graph that runs with particular concurrency mode.
class IExecutionBlock
    : public TThrRefBase
{
public:
    virtual ~IExecutionBlock() = default;

    virtual void BindToInvoker(NYT::IInvokerPtr invoker) = 0;

    virtual IExecutionBlockPtr Clone() = 0;

    // This API is supposed to be used as follows.
    //
    // 1. Call Start(...), returned future will be set once all processing is done (after Finish() is called)
    //    if error occurs while processing (e.g. user code inside ParDo thorws error) future will be set prematurely with error.
    // 2. Multiple calls to AsyncDo()
    // 3. Call Finish()
    // 4. Once future returned from Start() is set we are ready to next round.
    virtual NYT::TFuture<void> Start(IExecutionContextPtr executionContext) = 0;
    virtual void AsyncDo(NYT::TSharedRef data) = 0;
    virtual void AsyncFinish() = 0;

    virtual TBlockPipeConfig GetInputPipeConfiguration() const = 0;
    virtual TRowVtable GetInputRowVtable() const = 0;

    TString GetDebugDescription() const;
    virtual void WriteDebugDescription(IOutputStream* out, const TString& indent = "") const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IExecutionBlockPtr CreateOutputExecutionBlock(
    TBlockPipeConfig config,
    IRawOutputPtr output,
    TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

IExecutionBlockPtr CreateSerializedExecutionBlock(
        TBlockPipeConfig config,
        IRawParDoPtr parDo,
        std::vector<IExecutionBlockPtr> outputs);

IExecutionBlockPtr CreateSerializedExecutionBlock(
        TBlockPipeConfig config,
        IRawParDoPtr parDo,
        std::vector<IRawOutputPtr> outputs);

////////////////////////////////////////////////////////////////////////////////

IExecutionBlockPtr CreateConcurrentExecutionBlock(
        TBlockPipeConfig config,
        IRawParDoPtr parDo,
        std::vector<IExecutionBlockPtr> outputs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
