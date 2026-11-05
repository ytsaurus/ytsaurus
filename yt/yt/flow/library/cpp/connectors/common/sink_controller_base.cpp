#include "sink_controller_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TSinkControllerBase::TSinkControllerBase(
    TSinkControllerContextPtr context,
    TDynamicSinkControllerContextPtr dynamicContext)
    : Logger(context->Logger)
    , Context_(context)
    , Parameters_(DynamicPointerCast<ISink::TParameters>(TRegistry::Get()->ParseSinkParameters(context->SinkSpec)))
    , DynamicContext_(dynamicContext)
    , DynamicParameters_(DynamicPointerCast<ISink::TDynamicParameters>(TRegistry::Get()->ParseDynamicSinkParameters(context->SinkSpec, dynamicContext->DynamicSinkSpec)))
{
    SubscribeReconfigured(BIND([this] (const TDynamicSinkControllerContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = DynamicPointerCast<ISink::TDynamicParameters>(TRegistry::Get()->ParseDynamicSinkParameters(Context_->SinkSpec, dynamicContext->DynamicSinkSpec));
    }));
}

TSinkControllerContextPtr TSinkControllerBase::GetContext() const
{
    return Context_;
}

TDynamicSinkControllerContextPtr TSinkControllerBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TSinkSpecPtr TSinkControllerBase::GetSpec() const
{
    return Context_->SinkSpec;
}

TDynamicSinkSpecPtr TSinkControllerBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicSinkSpec;
}

void TSinkControllerBase::Init(IInitContextPtr /*initContext*/)
{ }

void TSinkControllerBase::Sync()
{ }

void TSinkControllerBase::Commit()
{ }

void TSinkControllerBase::UpdateWatermarkState(TWatermarkStatePtr watermarkState)
{
    WatermarkState_ = std::move(watermarkState);
}

TWatermarkStatePtr TSinkControllerBase::GetWatermarkState()
{
    return WatermarkState_.Acquire();
}

ISink::TParametersPtr TSinkControllerBase::GetParametersBase() const
{
    return Parameters_;
}

ISink::TDynamicParametersPtr TSinkControllerBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
