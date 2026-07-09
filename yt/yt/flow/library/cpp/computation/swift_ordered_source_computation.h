#pragma once

#include "public.h"

#include "computation_base.h"
#include "watermark_generator.h"

#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/source.h>
#include <yt/yt/flow/library/cpp/common/state.h>

#include <yt/yt/flow/library/cpp/misc/ordered_memory.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf ActiveSourceStateName = "$active_source";
inline constexpr TStringBuf WatermarkStateName = "$watermark";
inline constexpr TStringBuf TimestampMemoryStateName = "$timestamp_memory";

////////////////////////////////////////////////////////////////////////////////

struct TSwiftOrderedSourceTimestampMemory
    : public TOrderedMemory<TMessageId, TSystemTimestamp>
{
    REGISTER_YSON_STRUCT(TSwiftOrderedSourceTimestampMemory);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSwiftOrderedSourceTimestampMemory);

////////////////////////////////////////////////////////////////////////////////

class TSwiftOrderedSourceComputation
    : public TUniversalComputationBase
{
private:
    static void ValidateSpec(const TComputationSpec& spec);

    struct TExtendedParameters
        : public virtual TUniversalComputationBase::TParameters
    {
        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar registrar);
    };

    struct TExtendedDynamicParameters
        : public virtual TUniversalComputationBase::TDynamicParameters
    {
        TDuration MaxReadWindow;

        REGISTER_YSON_STRUCT(TExtendedDynamicParameters);

        static void Register(TRegistrar registrar);
    };

public:
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TExtendedDynamicParameters);

    YT_FLOW_EXTEND_SPEC_VALIDATION(ValidateSpec);

    TSwiftOrderedSourceComputation(
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

private:
    IOrderedSourcePtr OrderedSource_;
    std::optional<TKey> SourceKey_;

    struct TProcessedBatch
    {
        TMessageId FirstMessageId;
        TSourceMessageBatchCookie BatchCookie;
        std::vector<TMessage> CandidateOutputMessages;
        std::vector<bool> IsOutput;
        TSystemTimestamp Timestamp;
        TWatermarkGeneratorCookie WatermarkGeneratorCookie;
    };

    std::deque<TProcessedBatch> DelayedMessages_;

    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> GetExtraInputLimits() override;

private:
    IWatermarkGeneratorPtr WatermarkGenerator_;

    IMessageFilterPtr Filter_;
    NProfiling::TCounter SkippedByExpressionCounter_;

    void DoExecute(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard) override;

    TSystemTimestamp GetTriggerTimestamp(const std::vector<TMessage>& batch);

    void ProcessSourceBatches(std::vector<ISource::TMessageBatch>&& sourceMessageBatches);
    bool CheckDelayedMessages(
        IComputationRunContextPtr context,
        NTracing::TTraceContextPtr epochTraceContext,
        TSystemTimestamp now,
        TIntrusivePtr<TOrderedMemory<TMessageId, TSystemTimestamp>> timestampMemory,
        TDynamicComputationSpecPtr dynamicSpec);

    void ProcessDistributedMessages(const IComputationRunContextPtr& context, std::deque<TOutputMessageConstPtr>&& messages) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
