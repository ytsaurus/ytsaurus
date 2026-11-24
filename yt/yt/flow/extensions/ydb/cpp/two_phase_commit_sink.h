#pragma once

#include "public.h"

#include "sink.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TYdbTwoPhaseCommitSinkController
    : public TYdbSinkController
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbSinkControllerParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbSinkControllerParameters);

    TYdbTwoPhaseCommitSinkController(
        TSinkControllerContextPtr context,
        TDynamicSinkControllerContextPtr dynamicContext);

    void Sync() final;

private:
    const IStatusErrorStatePtr OrderingWarningState_;

    void UpdateOrderingWarning();
};

DEFINE_REFCOUNTED_TYPE(TYdbTwoPhaseCommitSinkController);

////////////////////////////////////////////////////////////////////////////////

class TYdbTwoPhaseCommitSink
    : public TSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbTwoPhaseCommitSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbTwoPhaseCommitSinkParameters);
    using TSinkController = TYdbTwoPhaseCommitSinkController;

    TYdbTwoPhaseCommitSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) final;
    void PreSyncFlush() final;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) final;
    void Commit() final;
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() const final;

private:
    const TYdbTwoPhaseCommitEnginePtr Engine_;
};

DEFINE_REFCOUNTED_TYPE(TYdbTwoPhaseCommitSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
