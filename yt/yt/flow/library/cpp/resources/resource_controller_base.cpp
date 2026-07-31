#include "resource_controller_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TResourceControllerBase::TResourceControllerBase(
    TResourceControllerContextPtr context,
    TDynamicResourceControllerContextPtr dynamicContext)
    : Context_(std::move(context))
    , DynamicContext_(std::move(dynamicContext))
    , Parameters_(TRegistry::Get()->ParseResourceParameters(Context_->ResourceSpec))
    , DynamicParameters_(TRegistry::Get()->ParseResourceDynamicParameters(Context_->ResourceSpec, DynamicContext_.Acquire()->DynamicResourceSpec))
    , Logger(Context_->Logger)
{
    SubscribeReconfigured(BIND([this] (const TDynamicResourceControllerContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = TRegistry::Get()->ParseResourceDynamicParameters(Context_->ResourceSpec, dynamicContext->DynamicResourceSpec);
    }));
}

void TResourceControllerBase::Init(IInitContextPtr /*initContext*/)
{ }

TResourceControllerContextPtr TResourceControllerBase::GetContext() const
{
    return Context_;
}

TDynamicResourceControllerContextPtr TResourceControllerBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TResourceSpecPtr TResourceControllerBase::GetSpec() const
{
    return Context_->ResourceSpec;
}

TDynamicResourceSpecPtr TResourceControllerBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicResourceSpec;
}

NYTree::TYsonStructPtr TResourceControllerBase::GetParametersBase() const
{
    return Parameters_;
}

NYTree::TYsonStructPtr TResourceControllerBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
