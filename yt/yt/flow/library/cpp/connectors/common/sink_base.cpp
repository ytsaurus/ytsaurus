#include "sink_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TSinkBase::TSinkBase(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : Logger(context->Logger)
    , Context_(context)
    , Parameters_(DynamicPointerCast<ISink::TParameters>(TRegistry::Get()->ParseSinkParameters(context->SinkSpec)))
    , DynamicContext_(dynamicContext)
    , DynamicParameters_(DynamicPointerCast<ISink::TDynamicParameters>(TRegistry::Get()->ParseDynamicSinkParameters(context->SinkSpec, dynamicContext->DynamicSinkSpec)))
    , EventLagObserver_(
        context->Profiler.WithPrefix("/event_lag"),
        {context->SinkSpec->InputStreamIds.begin(), context->SinkSpec->InputStreamIds.end()})
{
    SubscribeReconfigured(BIND([this] (const TDynamicSinkContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = DynamicPointerCast<ISink::TDynamicParameters>(TRegistry::Get()->ParseDynamicSinkParameters(Context_->SinkSpec, dynamicContext->DynamicSinkSpec));
    }));
}

TSinkContextPtr TSinkBase::GetContext() const
{
    return Context_;
}

TDynamicSinkContextPtr TSinkBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TSinkSpecPtr TSinkBase::GetSpec() const
{
    return Context_->SinkSpec;
}

TDynamicSinkSpecPtr TSinkBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicSinkSpec;
}

ISink::TParametersPtr TSinkBase::GetParametersBase() const
{
    return Parameters_;
}

ISink::TDynamicParametersPtr TSinkBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

void TSinkBase::ObserveEventLag(const TStreamId& streamId, TSystemTimestamp eventTimestamp)
{
    EventLagObserver_.Observe(streamId, eventTimestamp);
    EventLagObserver_.Flush(TInstant::Now());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
