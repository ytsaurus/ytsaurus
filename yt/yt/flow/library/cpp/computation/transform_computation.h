#pragma once

#include "public.h"

#include "computation_base.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TTransformComputation
    : public TUniversalComputationBase
{
private:
    static void ValidateSpec(const TComputationSpec& spec);

    struct TExtendedParameters
        : public virtual TUniversalComputationBase::TParameters
    {
        EProcessingMode ProcessingMode{};

        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar registrar);
    };

    struct TDynamicExtendedParameters
        : public virtual TUniversalComputationBase::TDynamicParameters
    {
        REGISTER_YSON_STRUCT(TDynamicExtendedParameters);

        static void Register(TRegistrar registrar);
    };

public:
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicExtendedParameters);

    YT_FLOW_EXTEND_SPEC_VALIDATION(ValidateSpec);

public:
    TTransformComputation(
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
    virtual void DoSync(IRetryableTransactionPtr transaction);

protected:
    void DoPrepare(const IComputationRunContextPtr& context) override;

private:
    void DoExecute(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard) override;

    void ProcessDistributedMessages(const IComputationRunContextPtr& context, std::deque<TOutputMessageConstPtr>&& messages) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
