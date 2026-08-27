#include "file_source_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TFileSourceBase::TFileSourceBase(
    TFileSourceContextPtr context,
    TDynamicFileSourceContextPtr dynamicContext)
    : Context_(std::move(context))
    , Parameters_(TRegistry::Get()->ParseFileSourceParameters(Context_->SourceSpec))
    , DynamicContext_(dynamicContext)
    , DynamicParameters_(TRegistry::Get()->ParseDynamicFileSourceParameters(
        Context_->SourceSpec,
        dynamicContext->DynamicFileSourceSpec))
{
    SubscribeReconfigured(BIND([this] (const TDynamicFileSourceContextPtr& newDynamicContext) {
        DynamicContext_ = newDynamicContext;
        DynamicParameters_ = TRegistry::Get()->ParseDynamicFileSourceParameters(
            Context_->SourceSpec,
            newDynamicContext->DynamicFileSourceSpec);
    }));
}

TFileSourceContextPtr TFileSourceBase::GetContext() const
{
    return Context_;
}

TDynamicFileSourceContextPtr TFileSourceBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TFileSourceSpecPtr TFileSourceBase::GetSpec() const
{
    return Context_->SourceSpec;
}

TDynamicFileSourceSpecPtr TFileSourceBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicFileSourceSpec;
}

NYTree::TYsonStructPtr TFileSourceBase::GetParametersBase() const
{
    return Parameters_;
}

NYTree::TYsonStructPtr TFileSourceBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
