#include "public.h"
#include "scheduler_pool_proxy.h"
#include "scheduler_pool_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/object_server/type_handler.h>

#include <yt/server/lib/scheduler/helpers.h>

namespace NYT::NSchedulerPoolServer {

using namespace NObjectServer;
using namespace NScheduler;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSchedulerPoolProxy::TSchedulerPoolProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TSchedulerPool* schedulerPool)
    : TNonversionedMapObjectProxyBase<TSchedulerPool>(bootstrap, metadata, schedulerPool)
{ }

TSchedulerPoolProxy::TProxyBasePtr TSchedulerPoolProxy::ResolveNameOrThrow(const TString& name)
{
    auto poolTreeName = GetPoolTreeName(GetThisImpl());
    const auto& schedulerPoolManager = Bootstrap_->GetSchedulerPoolManager();
    return GetProxy(schedulerPoolManager->FindPoolTreeOrSchedulerPoolOrThrow(poolTreeName, name));
}

TString TSchedulerPoolProxy::GetPoolTreeName(const TSchedulerPool* schedulerPool)
{
    return Bootstrap_->GetSchedulerPoolManager()->GetPoolTreeName(schedulerPool);
}

std::unique_ptr<NObjectServer::TNonversionedMapObjectFactoryBase<TSchedulerPool>> TSchedulerPoolProxy::CreateObjectFactory() const
{
    return std::make_unique<TSchedulerPoolFactory>(Bootstrap_);
}

void TSchedulerPoolProxy::DoRemoveSelf()
{
    if (GetThisImpl()->IsRoot()) {
        ValidateRemoval();
        RemoveChildren();
        Bootstrap_->GetObjectManager()->RemoveObject(GetThisImpl());
    } else {
        TNonversionedMapObjectProxyBase::DoRemoveSelf();
    }
}

void TSchedulerPoolProxy::ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors)
{
    TNonversionedMapObjectProxyBase::ListSystemAttributes(descriptors);
    const auto& schedulerPoolManager = Bootstrap_->GetSchedulerPoolManager();
    auto allAttributes = schedulerPoolManager->GetKnownPoolAttributes();
    const auto& poolTreeAttributes = schedulerPoolManager->GetKnownPoolTreeAttributes();
    allAttributes.insert(poolTreeAttributes.begin(), poolTreeAttributes.end());
    auto specifiedAttributes = GetThisImpl()->SpecifiedAttributes();

    for (auto internedAttributeKey : allAttributes) {
        descriptors->push_back(TAttributeDescriptor(internedAttributeKey)
            .SetWritable(true)
            .SetRemovable(true)
            .SetPresent(specifiedAttributes.contains(internedAttributeKey)));
    }
}

bool TSchedulerPoolProxy::GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer)
{
    auto* schedulerPool = GetThisImpl();

    if (schedulerPool->IsRoot() && IsKnownPoolTreeAttribute(key)) {
        auto* schedulerPoolTree = schedulerPool->GetMaybePoolTree();
        auto it = schedulerPoolTree->SpecifiedAttributes().find(key);
        if (it == schedulerPoolTree->SpecifiedAttributes().end()) {
            return false;
        }
        consumer->OnRaw(it->second);
        return true;
    }

    if (!schedulerPool->IsRoot() && IsKnownPoolAttribute(key)) {
        auto it = schedulerPool->SpecifiedAttributes().find(key);
        if (it == schedulerPool->SpecifiedAttributes().end()) {
            return false;
        }
        consumer->OnRaw(it->second);
        return true;
    }

    return TNonversionedMapObjectProxyBase::GetBuiltinAttribute(key, consumer);
}

bool TSchedulerPoolProxy::SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value)
{
    auto* schedulerPool = GetThisImpl();

    if (IsKnownPoolTreeAttribute(key)) {
        if (schedulerPool->IsRoot()) {
            auto* schedulerPoolTree = schedulerPool->GetMaybePoolTree();
            const auto& config = schedulerPoolTree->FullConfig();
            ValidateNoAliasClash(config, schedulerPoolTree->SpecifiedAttributes(), key);

            // NB: the order matters; first statement can throw exception.
            config->LoadParameter(key.Unintern(), ConvertToNode(value), EMergeStrategy::Overwrite);
            schedulerPoolTree->SpecifiedAttributes()[key] = value;
            return true;
        } else if (!IsKnownPoolAttribute(key)){
            THROW_ERROR_EXCEPTION("Cannot set known pool tree attribute %Qv on pool %Qv", key.Unintern(), schedulerPool->GetName())
                << TErrorAttribute("attribute_name", key.Unintern())
                << TErrorAttribute("pool_name", schedulerPool->GetName());
        }
    }

    if (IsKnownPoolAttribute(key)) {
        if (!schedulerPool->IsRoot()) {
            ValidateNoAliasClash(schedulerPool->FullConfig(), schedulerPool->SpecifiedAttributes(), key);
            GuardedUpdateBuiltinPoolAttribute(key, [&value] (const TPoolConfigPtr& config, const TString& uninternedKey) {
                config->LoadParameter(uninternedKey, ConvertToNode(value), EMergeStrategy::Overwrite);
            });

            schedulerPool->SpecifiedAttributes()[key] = value;
            return true;
        } else if (!IsKnownPoolTreeAttribute(key)){
            THROW_ERROR_EXCEPTION("Cannot set known pool attribute %Qv on pool tree %Qv", key.Unintern(), schedulerPool->GetMaybePoolTree()->GetTreeName())
                << TErrorAttribute("attribute_name", key.Unintern())
                << TErrorAttribute("pool_tree_name", schedulerPool->GetMaybePoolTree()->GetTreeName());
        }
    }
    return TNonversionedMapObjectProxyBase::SetBuiltinAttribute(key, value);
}

bool TSchedulerPoolProxy::RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key)
{
    auto schedulerPool = GetThisImpl();
    if (schedulerPool->IsRoot() && IsKnownPoolTreeAttribute(key)) {
        auto* schedulerPoolTree = schedulerPool->GetMaybePoolTree();
        auto it = schedulerPoolTree->SpecifiedAttributes().find(key);
        if (it == schedulerPoolTree->SpecifiedAttributes().end()) {
            return false;
        }

        schedulerPoolTree->FullConfig()->ResetParameter(key.Unintern());
        schedulerPoolTree->SpecifiedAttributes().erase(it);
        return true;
    }

    if (!schedulerPool->IsRoot() && IsKnownPoolAttribute(key)) {
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

    auto poolTreeName = GetPoolTreeName(GetThisImpl());
    if (Bootstrap_->GetSchedulerPoolManager()->FindSchedulerPoolByName(poolTreeName, newChildName)) {
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
    const TYsonSerializablePtr& config,
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

////////////////////////////////////////////////////////////////////////////////

TSchedulerPoolFactory::TSchedulerPoolFactory(NCellMaster::TBootstrap* bootstrap)
    : TNonversionedMapObjectFactoryBase<TSchedulerPool>(bootstrap)
{ }

TSchedulerPool* TSchedulerPoolFactory::DoCreateObject(IAttributeDictionary* attributes)
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
