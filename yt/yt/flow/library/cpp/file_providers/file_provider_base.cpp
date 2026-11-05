#include "file_provider_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TFileProviderBase::TFileProviderBase(
    TFileProviderContextPtr context,
    TDynamicFileProviderContextPtr dynamicContext)
    : Context_(std::move(context))
    , Parameters_(TRegistry::Get()->ParseFileProviderParameters(Context_->ProviderSpec))
    , DynamicContext_(dynamicContext)
    , DynamicParameters_(TRegistry::Get()->ParseDynamicFileProviderParameters(
        Context_->ProviderSpec,
        dynamicContext->DynamicFileProviderSpec))
{
    SubscribeReconfigured(BIND([this] (const TDynamicFileProviderContextPtr& newDynamicContext) {
        DynamicContext_ = newDynamicContext;
        DynamicParameters_ = TRegistry::Get()->ParseDynamicFileProviderParameters(
            Context_->ProviderSpec,
            newDynamicContext->DynamicFileProviderSpec);
    }));
}

TFileProviderContextPtr TFileProviderBase::GetContext() const
{
    return Context_;
}

TDynamicFileProviderContextPtr TFileProviderBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TFileProviderSpecPtr TFileProviderBase::GetSpec() const
{
    return Context_->ProviderSpec;
}

TDynamicFileProviderSpecPtr TFileProviderBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicFileProviderSpec;
}

NYTree::TYsonStructPtr TFileProviderBase::GetParametersBase() const
{
    return Parameters_;
}

NYTree::TYsonStructPtr TFileProviderBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
