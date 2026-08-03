#pragma once

#include "public.h"

#include "computation_base.h"
#include "watermark_generator.h"

#include <yt/yt/flow/library/cpp/common/source.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TOrderedSourceComputationBase
    : public TUniversalComputationBase
{
public:
    TOrderedSourceComputationBase(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    TComputationOrchidStatePtr GetOrchidState() override;

    virtual void DoInit(IJobInitContextPtr initContext);
    virtual void DoInit(); // For backward compatibility - remove it later.
    virtual void DoProcess(IInputContextPtr input, IOutputCollectorPtr output);
    virtual void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output);
    virtual void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output);
    virtual void DoSync(IRetryableTransactionPtr transaction);

protected:
    void DoPrepare(const IComputationRunContextPtr& context) final;

    static void ValidateOrderedSourceSpec(const TComputationSpec& spec, TStringBuf className);

    TSystemTimestamp GetReadDelayThreshold();

    IOrderedSourcePtr OrderedSource_;
    IWatermarkGeneratorPtr WatermarkGenerator_;
    IMessageFilterPtr Filter_;
    NProfiling::TCounter SkippedByExpressionCounter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
