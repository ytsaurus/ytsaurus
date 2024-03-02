#include "public.h"
#include "pool_resources.h"
#include "scheduler_pool_proxy.h"
#include "scheduler_pool_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/object_server/type_handler.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

namespace NYT::NSchedulerPoolServer {

using namespace NObjectServer;
using namespace NScheduler;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSchedulerPoolProxy::TProxyBasePtr TSchedulerPoolProxy::ResolveNameOrThrow(const TString& name)
{
    auto poolTreeName = GetMaybePoolTreeName(GetThisImpl());
    if (!poolTreeName) {
        THROW_ERROR_EXCEPTION("Failed to resolve pool tree name");
    }

    const auto& schedulerPoolManager = Bootstrap_->GetSchedulerPoolManager();
    return GetProxy(schedulerPoolManager->FindPoolTreeOrSchedulerPoolOrThrow(*poolTreeName, name));
}

std::optional<TString> TSchedulerPoolProxy::GetMaybePoolTreeName(const TSchedulerPool* schedulerPool)
{
    return Bootstrap_->GetSchedulerPoolManager()->GetMaybePoolTreeName(schedulerPool);
}

std::unique_ptr<NObjectServer::TNonversionedMapObjectFactoryBase<TSchedulerPool>> TSchedulerPoolProxy::CreateObjectFactory() const
{
    return std::make_unique<TSchedulerPoolFactory>(Bootstrap_);
}

void TSchedulerPoolProxy::DoRemoveSelf(bool recursive, bool force)
{
    if (GetThisImpl()->IsRoot()) {
        ValidateRemoval();
        RemoveChildren();
        Bootstrap_->GetObjectManager()->RemoveObject(GetThisImpl());
    } else {
        TNonversionedMapObjectProxyBase::DoRemoveSelf(recursive, force);
    }
}

void TSchedulerPoolProxy::ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors)
{
    TNonversionedMapObjectProxyBase::ListSystemAttributes(descriptors);
    const auto& schedulerPoolManager = Bootstrap_->GetSchedulerPoolManager();
    const auto& poolAttributes = schedulerPoolManager->GetKnownPoolAttributes();
    auto schedulerPool = GetThisImpl();

    for (auto poolAttributeKey : poolAttributes) {
        auto isPresent = !schedulerPool->IsRoot() && schedulerPool->SpecifiedAttributes().contains(poolAttributeKey);
        descriptors->push_back(TAttributeDescriptor(poolAttributeKey)
            .SetWritable(true)
            .SetRemovable(true)
            .SetPresent(isPresent));
    }
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Config)
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(schedulerPool->IsRoot()));
}

bool TSchedulerPoolProxy::GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer)
{
    auto* schedulerPool = GetThisImpl();
    key = TSchedulerPool::RemapDeprecatedKey(key);

    if (schedulerPool->IsRoot()) {
        if (key == EInternedAttributeKey::Config) {
            consumer->OnRaw(schedulerPool->GetMaybePoolTree()->SpecifiedConfig());
            return true;
        }
    } else if (IsKnownPoolAttribute(key)) {
        auto it = schedulerPool->SpecifiedAttributes().find(key);
        if (it == schedulerPool->SpecifiedAttributes().end()) {
            return false;
        }
        consumer->OnRaw(it->second);
        return true;
    }

    return TNonversionedMapObjectProxyBase::GetBuiltinAttribute(key, consumer);
}

bool TSchedulerPoolProxy::SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force)
{
    auto* schedulerPool = GetThisImpl();
    key = TSchedulerPool::RemapDeprecatedKey(key);

    if (schedulerPool->IsRoot()) {
        if (key == EInternedAttributeKey::Config) {
            auto* schedulerPoolTree = schedulerPool->GetMaybePoolTree();
            auto treeConfig = New<NScheduler::TFairShareStrategyTreeConfig>();
            treeConfig->Load(ConvertToNode(value));
            schedulerPool->ValidateStrongGuaranteesRecursively(treeConfig);
            schedulerPoolTree->UpdateSpecifiedConfig(value);
            return true;
        }
    } else if (IsKnownPoolAttribute(key)) {
        ValidateNoAliasClash(schedulerPool->FullConfig(), schedulerPool->SpecifiedAttributes(), key);
        GuardedUpdateBuiltinPoolAttribute(key, [&value] (const TPoolConfigPtr& config, const TString& uninternedKey) {
            config->LoadParameter(uninternedKey, ConvertToNode(value));
        });

        schedulerPool->SpecifiedAttributes()[key] = value;
        return true;
    }
    return TNonversionedMapObjectProxyBase::SetBuiltinAttribute(key, value, force);
}

bool TSchedulerPoolProxy::RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key)
{
    auto schedulerPool = GetThisImpl();
    if (schedulerPool->IsRoot()) {
        if (key == EInternedAttributeKey::Config) {
            auto defaultPoolTreeConfig = New<TFairShareStrategyTreeConfig>();
            schedulerPool->ValidateStrongGuaranteesRecursively(defaultPoolTreeConfig);
            schedulerPool->GetMaybePoolTree()->UpdateSpecifiedConfig(ConvertToYsonString(EmptyAttributes()));
            return true;
        }
    } else if (IsKnownPoolAttribute(key)) {
        auto it = schedulerPool->SpecifiedAttributes().find(key);
        if (it == schedulerPool->SpecifiedAttributes().end()) {
            return false;
        }

        GuardedUpdateBuiltinPoolAttribute(key, [] (const TPoolConfigPtr& config, const TString& uninternedKey) {
            config->ResetParameter(uninternedKey);
        });
        schedulerPool->SpecifiedAttributes().erase(it);
        return true;
    }
    return TNonversionedMapObjectProxyBase::RemoveBuiltinAttribute(key);
}

void TSchedulerPoolProxy::GuardedUpdateBuiltinPoolAttribute(
    NYT::NYTree::TInternedAttributeKey key,
    const std::function<void(const TPoolConfigPtr&, const TString&)>& update)
{
    if (!Bootstrap_->GetSchedulerPoolManager()->IsUserManagedAttribute(key)) {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Administer);
    }

    GetThisImpl()->GuardedUpdatePoolAttribute(key, update);
}

bool TSchedulerPoolProxy::IsKnownPoolAttribute(NYTree::TInternedAttributeKey key)
{
    return Bootstrap_->GetSchedulerPoolManager()->GetKnownPoolAttributes().contains(key);
}

bool TSchedulerPoolProxy::IsKnownPoolTreeAttribute(NYTree::TInternedAttributeKey key)
{
    return Bootstrap_->GetSchedulerPoolManager()->GetKnownPoolTreeAttributes().contains(key);
}

void TSchedulerPoolProxy::ValidateChildNameAvailability(const TString& newChildName)
{
    TNonversionedMapObjectProxyBase::ValidateChildNameAvailability(newChildName);

    auto poolTreeName = GetMaybePoolTreeName(GetThisImpl());
    if (!poolTreeName) {
        THROW_ERROR_EXCEPTION("Failed to resolve pool tree name");
    }

    if (Bootstrap_->GetSchedulerPoolManager()->FindSchedulerPoolByName(*poolTreeName, newChildName)) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::AlreadyExists,
            "Pool tree %Qv already contains pool with name %Qv",
            poolTreeName,
            newChildName);
    }
}

void TSchedulerPoolProxy::ValidateAfterAttachChild(const TString& key, const TProxyBasePtr& childProxy)
{
    TNonversionedMapObjectProxyBase::ValidateAfterAttachChild(key, childProxy);

    GetThisImpl()->ValidateChildrenCompatibility();
}

void TSchedulerPoolProxy::ValidateNoAliasClash(
    const TYsonStructPtr& config,
    const TSpecifiedAttributesMap& specifiedAttributes,
    TInternedAttributeKey key)
{
    const auto& uninternedKey = key.Unintern();
    for (const auto& alias : config->GetAllParameterAliases(uninternedKey)) {
        if (alias != uninternedKey && specifiedAttributes.contains(TInternedAttributeKey::Lookup(alias))) {
            THROW_ERROR_EXCEPTION("Attempt to set the same attribute with different alias")
                    << TErrorAttribute("previous_alias", alias)
                    << TErrorAttribute("current_alias", key);
        }
    }
}

EPermission TSchedulerPoolProxy::GetCustomAttributeModifyPermission()
{
    return NYTree::EPermission::Administer;
}

bool TSchedulerPoolProxy::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(TransferPoolResources);
    return TBase::DoInvoke(context);
}

DEFINE_YPATH_SERVICE_METHOD(TSchedulerPoolProxy, TransferPoolResources)
{
    Y_UNUSED(response);

    DeclareMutating();

    const auto& schedulerPoolManager = Bootstrap_->GetSchedulerPoolManager();

    auto* impl = GetThisImpl();
    auto* poolTreeImpl = impl->GetMaybePoolTree();
    if (!poolTreeImpl) {
        THROW_ERROR_EXCEPTION("Transferring pool resources must be targeted at pool tree")
            << TErrorAttribute("current_scheduler_pool_name", impl->GetName());
    }

    if (request->src_pool() == request->dst_pool()) {
        THROW_ERROR_EXCEPTION("Source and destination pools must differ")
            << TErrorAttribute("provided_src_and_dst_pool_name", request->src_pool());
    }

    auto* srcPool = request->src_pool() == RootPoolName
        ? impl
        : schedulerPoolManager->FindSchedulerPoolByName(poolTreeImpl->GetTreeName(), request->src_pool());
    if (!srcPool) {
        THROW_ERROR_EXCEPTION("Source pool does not exist")
            << TErrorAttribute("pool_name", request->src_pool())
            << TErrorAttribute("pool_tree", poolTreeImpl->GetTreeName());
    }
    auto* dstPool = request->dst_pool() == RootPoolName
        ? impl
        : schedulerPoolManager->FindSchedulerPoolByName(poolTreeImpl->GetTreeName(), request->dst_pool());
    if (!dstPool) {
        THROW_ERROR_EXCEPTION("Destination pool does not exist")
            << TErrorAttribute("pool_name", request->dst_pool())
            << TErrorAttribute("pool_tree", poolTreeImpl->GetTreeName());
    }

    auto resourceDelta = ConvertTo<TPoolResourcesPtr>(TYsonString(request->resource_delta()));
    if (!resourceDelta->IsNonNegative()) {
        THROW_ERROR_EXCEPTION("All provided resources must be non-negative")
            << TErrorAttribute("resource_delta", resourceDelta);
    }

    context->SetRequestInfo("SrcPool: %v, DstPool: %v, PoolTree: %v",
        srcPool->GetName(),
        dstPool->GetName(),
        poolTreeImpl->GetTreeName());

    schedulerPoolManager->TransferPoolResources(srcPool, dstPool, resourceDelta);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerPoolFactory::TSchedulerPoolFactory(NCellMaster::TBootstrap* bootstrap)
    : TNonversionedMapObjectFactoryBase<TSchedulerPool>(bootstrap)
{ }

TSchedulerPool* TSchedulerPoolFactory::DoCreateObject(IAttributeDictionary* /*attributes*/)
{
    return Bootstrap_->GetSchedulerPoolManager()->CreateSchedulerPool();
}

TIntrusivePtr<TNonversionedMapObjectProxyBase<TSchedulerPool>> TSchedulerPoolFactory::GetSchedulerPoolProxy(TObject* schedulerPool) const
{
    // TODO(renadeen): Unite with account?
    const auto& handler = Bootstrap_->GetObjectManager()->GetHandler(EObjectType::SchedulerPool);
    auto proxy = handler->GetProxy(schedulerPool, nullptr);
    auto* schedulerPoolPtr = dynamic_cast<TSchedulerPoolProxy*>(proxy.Get());
    YT_VERIFY(schedulerPoolPtr);
    return schedulerPoolPtr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
