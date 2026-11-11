#pragma once

#include "public.h"

#include "ordered_source_computation_base.h"
#include "read_delay.h"

#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/source_read_delay/estimator.h>

#include <yt/yt/flow/library/cpp/misc/ordered_memory.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf TimestampMemoryStateName = "$timestamp_memory";

////////////////////////////////////////////////////////////////////////////////

struct TSwiftOrderedSourceTimestampMemory
    : public TOrderedMemory<TMessageId, TSystemTimestamp>
{
    TSourceReadDelayEstimator ReadDelayEstimator;

    REGISTER_YSON_STRUCT(TSwiftOrderedSourceTimestampMemory);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSwiftOrderedSourceTimestampMemory);

////////////////////////////////////////////////////////////////////////////////

class TSwiftOrderedSourceComputation
    : public TOrderedSourceComputationBase
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

    using TOrderedSourceComputationBase::TOrderedSourceComputationBase;

    TComputationStatusPtr GetStatus() override;

private:
    TMutableStateClient<TSwiftOrderedSourceTimestampMemory> TimestampMemory_;

    bool HasReadDelays() const;
    TInflightStreamTraverseDataPtr BuildReadDelayInflight(const TInflightStreamTraverseDataPtr& inflight, TInstant now);

    struct TPublishResult
    {
        bool EmptyEpoch = false;
    };

    TReadDelayQueue ReadDelayQueue_;
    TAtomicIntrusivePtr<TReadDelayStatus> ReadDelayStatus_;
    IStatusErrorStatePtr ReadDelayEstimateError_;

    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> GetExtraInputLimits() override;

    void DoExecute(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard) override;

    std::vector<TReadDelayInput> ProcessSourceBatches(std::vector<ISource::TMessageBatch>&& sourceMessageBatches);
    TPublishResult CheckDelayedMessages(
        IComputationRunContextPtr context,
        NTracing::TTraceContextPtr epochTraceContext,
        TSystemTimestamp now,
        TIntrusivePtr<TOrderedMemory<TMessageId, TSystemTimestamp>> timestampMemory,
        TDynamicComputationSpecPtr dynamicSpec);

    void ProcessDistributedMessages(const IComputationRunContextPtr& context, std::deque<TOutputMessageConstPtr>&& messages) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
