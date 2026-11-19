#pragma once

#include "computation.h"

#include <yt/yt/flow/library/cpp/computation/message_filter.h>
#include <yt/yt/flow/library/cpp/computation/ordered_source_computation_base.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/misc/ordered_memory.h>

#include <map>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf StableBatchSourceStateName = "$stable_batch_source";

//! `skip_if_expression` per read group, keyed as the publication times.
using TSkipIfMemory = TOrderedMemory<TMessageId, std::optional<std::string>>;

struct TSwiftStableBatchSourceState
    : public TOrderedMemory<TMessageId, TSystemTimestamp> // Publication time per read group.
{
    TIntrusivePtr<TSkipIfMemory> SkipIfMemory;

    REGISTER_YSON_STRUCT(TSwiftStableBatchSourceState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSwiftStableBatchSourceState);

////////////////////////////////////////////////////////////////////////////////

struct TSkipIfExpressionSelection
{
    std::optional<std::string> Expression;
    //! True if a first-sight group differs from the previous remembered expression.
    bool Changed = false;
};

//! Selects the `skip_if_expression` of the read group from |firstMessageId| up to the first message id
//! |lastBatchFirstMessageId| of its last batch. A first-sight group gets |dynamicExpression|, which is
//! remembered; a replayed group gets the expression of its first read.
TSkipIfExpressionSelection SelectSkipIfExpression(
    TSkipIfMemory* memory,
    const TMessageId& firstMessageId,
    const TMessageId& lastBatchFirstMessageId,
    const std::optional<std::string>& dynamicExpression);

//! The checks of #TSwiftStableBatchSourceComputation beyond those of every ordered source computation.
void ValidateSwiftStableBatchSourceSpecSpecifics(const TComputationSpec& spec);

////////////////////////////////////////////////////////////////////////////////

//! Ordered source computation that hosts a process function over replay-stable read groups:
//! every read is processed and completed as a whole, and a replay re-reads exactly its groups.
// Not `final` only because #New derives TRefCountedWrapper from it.
class TSwiftStableBatchSourceComputation
    : public TProcessFunctionComputationBase<TOrderedSourceComputationBase>
{
private:
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

    static void ValidateSpec(const TComputationSpec& spec);

public:
    static constexpr bool RequiresProcessingFunction = true;
    static constexpr auto InputAcknowledgementPolicy = EInputAcknowledgementPolicy::AfterDependentOutputs;

    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TExtendedDynamicParameters);

    YT_FLOW_EXTEND_SPEC_VALIDATION(ValidateSpec);

    TSwiftStableBatchSourceComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

private:
    TMutableStateClient<TSwiftStableBatchSourceState> State_;
    std::map<std::optional<std::string>, IMessageFilterPtr> Filters_;

    NProfiling::TCounter ReplayedGroupsCounter_;
    NProfiling::TGauge SkipIfMemorySizeGauge_;

    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> GetExtraInputLimits() override;

    void DoExecute(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard) override;

    //! Processes and publishes a non-empty read group.
    void ProcessReadGroup(
        const IComputationRunContextPtr& context,
        const NTracing::TTraceContextPtr& epochTraceContext,
        std::vector<ISource::TMessageBatch>* batches,
        TFuture<ITimeProvider::TGlobalUniqueSeqNo> futureNow);

    const IMessageFilterPtr& GetFilter(const std::optional<std::string>& expression);

    void ProcessDistributedMessages(const IComputationRunContextPtr& context, std::deque<TOutputMessageConstPtr>&& messages) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
