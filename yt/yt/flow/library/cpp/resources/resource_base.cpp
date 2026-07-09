#include "resource_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TResourceBase::TResourceBase(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : Context_(context)
    , DynamicContext_(dynamicContext)
    , Parameters_(TRegistry::Get()->ParseResourceParameters(context->ResourceSpec))
    , DynamicParameters_(TRegistry::Get()->ParseResourceDynamicParameters(context->ResourceSpec, dynamicContext->DynamicResourceSpec))
    , Logger(Context_->Logger)
{
    SubscribeReconfigured(BIND([this] (const TDynamicResourceContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = TRegistry::Get()->ParseResourceDynamicParameters(Context_->ResourceSpec, dynamicContext->DynamicResourceSpec);
    }));
}

TResourceContextPtr TResourceBase::GetContext() const
{
    return Context_;
}

TDynamicResourceContextPtr TResourceBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TResourceSpecPtr TResourceBase::GetSpec() const
{
    return Context_->ResourceSpec;
}

TDynamicResourceSpecPtr TResourceBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicResourceSpec;
}

NYTree::TYsonStructPtr TResourceBase::GetParametersBase() const
{
    return Parameters_;
}

TFuture<void> TResourceBase::Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/)
{
    return OKFuture;
}

void TResourceBase::Reconfigure(const TDynamicResourceContextPtr& dynamicContext)
{
    TReconfigurable<TDynamicResourceContext>::Reconfigure(dynamicContext);
}

NYTree::TYsonStructPtr TResourceBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

void TResourceBase::FeedStatus(i64 morePushedToQueue, i64 moreFetchedFromQueue)
{
    auto resourceManager = Context_->ResourceManager.Lock();
    if (!resourceManager) {
        YT_LOG_WARNING("Resource manager is not available, skipping FeedStatus");
        return;
    }
    resourceManager->FeedStatus(Context_->ResourceId, morePushedToQueue, moreFetchedFromQueue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
