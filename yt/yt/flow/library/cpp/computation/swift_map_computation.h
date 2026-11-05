#pragma once

#include "public.h"

#include "computation_base.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TSwiftMapComputation
    : public TUniversalComputationBase
{
private:
    static void ValidateSpec(const TComputationSpec& spec);

    struct TExtendedParameters
        : public virtual TUniversalComputationBase::TParameters
    {
        //! Allows merging several input messages into one output message (batching).
        //! WARNING: a merged output gets a fresh MessageId (from UniqueSeqNo) instead of inheriting it from a
        //! parent, so per-key MessageId order is not preserved and the same parent may be reflected in several
        //! merged outputs across replays. Downstream must tolerate at-least-once delivery and must not rely on
        //! MessageId ordering within a key. With batching disabled every output has a single parent and behaves
        //! exactly-once.
        bool AllowBatchingWithRelaxedGuarantees = false;

        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar registrar);
    };

    struct TExtendedDynamicParameters
        : public virtual TUniversalComputationBase::TDynamicParameters
    {
        REGISTER_YSON_STRUCT(TExtendedDynamicParameters);

        static void Register(TRegistrar registrar);
    };

public:
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TExtendedDynamicParameters);

    YT_FLOW_EXTEND_SPEC_VALIDATION(ValidateSpec);

    TSwiftMapComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    virtual void DoInit(IJobInitContextPtr initContext);
    virtual void DoInit(); // For backward compatibility - remove it later.
    virtual void DoProcess(IInputContextPtr input, IOutputCollectorPtr output);
    virtual void DoProcessKey(IInputContextPtr input, IOutputCollectorPtr output);
    virtual void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output);
    virtual void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output);
    virtual void DoProcessTimer(const TInputTimerConstPtr& timer, IOutputCollectorPtr output);
    virtual void DoProcessTimer(const TTimer& timer, IOutputCollectorPtr output);
    virtual void DoProcessVisit(const TInputVisitConstPtr& visit, IOutputCollectorPtr output);
    virtual void DoProcessVisit(const TVisit& visit, IOutputCollectorPtr output);

private:
    void DoPrepare(const IComputationRunContextPtr& context) final;
    void DoExecute(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard) override;
    TSystemTimestamp GetInputSystemWatermark();

    void ProcessDistributedMessages(const IComputationRunContextPtr& context, std::deque<TOutputMessageConstPtr>&& messages) override;

    // Accumulates input message IDs that have been fully distributed (all outputs delivered).
    // Flushed via context->MarkPersisted() before Commit() and in ProcessDistributedMessages().
    std::vector<TMessageId> PersistedInputMessageIds_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
