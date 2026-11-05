#include "source_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSourceBase::TSourceBase(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : Logger(context->Logger)
    , Context_(context)
    , Parameters_(DynamicPointerCast<ISource::TParameters>(TRegistry::Get()->ParseSourceParameters(context->SourceSpec)))
    , DynamicContext_(dynamicContext)
    , DynamicParameters_(DynamicPointerCast<ISource::TDynamicParameters>(TRegistry::Get()->ParseDynamicSourceParameters(context->SourceSpec, dynamicContext->DynamicSourceSpec)))
    , DynamicPartitionSpec_(DynamicPointerCast<ISource::TDynamicPartitionSpec>(TRegistry::Get()->ParseDynamicSourcePartitionSpec(context->SourceSpec, dynamicContext->DynamicPartitionSpec)))
{
    SubscribeReconfigured(
        BIND([this] (const TDynamicSourceContextPtr& dynamicContext) {
            // It is atomics. So it is thread-safe.
            DynamicContext_ = dynamicContext;
            DynamicParameters_ = DynamicPointerCast<ISource::TDynamicParameters>(TRegistry::Get()->ParseDynamicSourceParameters(Context_->SourceSpec, dynamicContext->DynamicSourceSpec));
            DynamicPartitionSpec_ = DynamicPointerCast<ISource::TDynamicPartitionSpec>(TRegistry::Get()->ParseDynamicSourcePartitionSpec(Context_->SourceSpec, dynamicContext->DynamicPartitionSpec));
        }));
}

TSourceContextPtr TSourceBase::GetContext() const
{
    return Context_;
}

TDynamicSourceContextPtr TSourceBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TSourceSpecPtr TSourceBase::GetSpec() const
{
    return Context_->SourceSpec;
}

TDynamicSourceSpecPtr TSourceBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicSourceSpec;
}

IMapNodePtr TSourceBase::GetPartitionStatus()
{
    return GetEphemeralNodeFactory()->CreateMap();
}

bool TSourceBase::IsDraining() const
{
    return GetDynamicContext()->DynamicSourceSpec->Draining;
}

ISource::TParametersPtr TSourceBase::GetParametersBase() const
{
    return Parameters_;
}

ISource::TDynamicParametersPtr TSourceBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

ISource::TDynamicPartitionSpecPtr TSourceBase::GetDynamicPartitionSpecBase() const
{
    auto ptr = DynamicPartitionSpec_.Acquire();
    YT_TLOG_FATAL_UNLESS(ptr, "Dynamic partition spec is available only after first Reconfigure call");
    return ptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
