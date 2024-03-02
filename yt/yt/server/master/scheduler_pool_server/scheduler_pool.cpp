#include "private.h"

#include "pool_resources.h"
#include "scheduler_pool.h"
#include "scheduler_pool_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSchedulerPoolServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NYTree;
using namespace NYson;

using NScheduler::TPoolConfigPtr;
using NScheduler::TFairShareStrategyTreeConfigPtr;
using NVectorHdrf::ESchedulingMode;
using NVectorHdrf::EJobResourceType;
using NVectorHdrf::TJobResources;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger& Logger = SchedulerPoolServerLogger;

////////////////////////////////////////////////////////////////////////////////

INodePtr ConvertToNode(const THashMap<TInternedAttributeKey, TYsonString>& attributes)
{
    return BuildYsonNodeFluently()
        .DoMapFor(attributes, [] (TFluentMap fluent, const auto& pair) {
            fluent.Item(pair.first.Unintern()).Value(pair.second);
        });
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerPool::TSchedulerPool(TObjectId id, bool isRoot)
    : TBase(id, isRoot)
    , FullConfig_(New<NScheduler::TPoolConfig>())
{ }

TString TSchedulerPool::GetLowercaseObjectName() const
{
    return IsRoot()
        ? MaybePoolTree_->GetLowercaseObjectName()
        : Format("scheduler pool %Qv", GetName());
}

TString TSchedulerPool::GetCapitalizedObjectName() const
{
    return IsRoot()
        ? MaybePoolTree_->GetCapitalizedObjectName()
        : Format("Scheduler pool %Qv", GetName());
}

void TSchedulerPool::FullValidate()
{
    FullConfig_->Validate(GetName());
    ValidateChildrenCompatibility();
    ValidateStrongGuarantees(GetPoolTreeConfig());
}

void TSchedulerPool::ValidateChildrenCompatibility()
{
    if (IsRoot()) {
        return;
    }

    // TODO(renadeen): move children validation to pool config?
    FullConfig()->StrongGuaranteeResources->ForEachResource([this] (auto NVectorHdrf::TJobResourcesConfig::* resourceDataMember, EJobResourceType resourceType) {
        using TResource = typename std::remove_reference_t<decltype(std::declval<NVectorHdrf::TJobResourcesConfig>().*resourceDataMember)>::value_type;

        ValidateChildrenGuaranteeSum<TResource>("Strong guarantee", resourceType, [&] (const TPoolConfigPtr& config) -> std::optional<TResource> {
            return config->StrongGuaranteeResources.Get()->*resourceDataMember;
        });
        ValidateChildrenGuaranteeSum<TResource>("Burst guarantee", resourceType, [&] (const TPoolConfigPtr& config) -> std::optional<TResource> {
            return config->IntegralGuarantees->BurstGuaranteeResources.Get()->*resourceDataMember;
        });
        ValidateChildrenGuaranteeSum<TResource>("Resource flow", resourceType, [&] (const TPoolConfigPtr& config) -> std::optional<TResource> {
            return config->IntegralGuarantees->ResourceFlow.Get()->*resourceDataMember;
        });
    });

    if (FullConfig_->IntegralGuarantees->GuaranteeType != NScheduler::EIntegralGuaranteeType::None) {
        for (const auto& [_, child] : KeyToChild_) {
            auto childGuarantees = child->FullConfig()->IntegralGuarantees;
            if (childGuarantees->BurstGuaranteeResources->IsNonTrivial() ||
                childGuarantees->ResourceFlow->IsNonTrivial())
            {
                THROW_ERROR_EXCEPTION("Integral pool %Qv cannot have children with integral resources", GetName())
                    << TErrorAttribute("pool_name", GetName())
                    << TErrorAttribute("pool_guarantee_type", FullConfig()->IntegralGuarantees->GuaranteeType)
                    << TErrorAttribute("child_pool_name", child->GetName())
                    << TErrorAttribute("child_burst_guarantee_resources", childGuarantees->BurstGuaranteeResources)
                    << TErrorAttribute("child_resource_flow", childGuarantees->ResourceFlow);
            }
        }
    }

    if (!KeyToChild().empty() && FullConfig_->Mode == ESchedulingMode::Fifo) {
        THROW_ERROR_EXCEPTION("Pool cannot have subpools since it is in FIFO mode")
            << TErrorAttribute("pool_name", GetName());
    }
}

void TSchedulerPool::ValidateStrongGuarantees(const TFairShareStrategyTreeConfigPtr& poolTreeConfig) const
{
    DoValidateStrongGuarantees(poolTreeConfig, /*recursive*/ false);
}

void TSchedulerPool::ValidateStrongGuaranteesRecursively(const TFairShareStrategyTreeConfigPtr& poolTreeConfig) const
{
    DoValidateStrongGuarantees(poolTreeConfig, /*recursive*/ true);
}

void TSchedulerPool::DoValidateStrongGuarantees(const TFairShareStrategyTreeConfigPtr& poolTreeConfig, bool recursive) const
{
    bool hasMainResourceGuarantee = false;
    bool hasAnyResourceGuarantee = false;
    FullConfig()->StrongGuaranteeResources->ForEachResource([&] (auto NVectorHdrf::TJobResourcesConfig::* resourceDataMember, EJobResourceType resourceType) {
        bool hasResource = (FullConfig()->StrongGuaranteeResources.Get()->*resourceDataMember).has_value();
        hasAnyResourceGuarantee = hasAnyResourceGuarantee || hasResource;
        if (resourceType == poolTreeConfig->MainResource) {
            hasMainResourceGuarantee = hasResource;
        }
    });

    if (hasAnyResourceGuarantee && !hasMainResourceGuarantee) {
        THROW_ERROR_EXCEPTION("Main resource guarantee must be specified in order to set guarantees for any other resource")
            << TErrorAttribute("pool_name", GetName())
            << TErrorAttribute("main_resource", poolTreeConfig->MainResource)
            << TErrorAttribute("guarantee_config", FullConfig()->StrongGuaranteeResources);
    }

    if (recursive) {
        for (const auto& [_, child] : KeyToChild_) {
            child->DoValidateStrongGuarantees(poolTreeConfig, recursive);
        }
    }
}

TFairShareStrategyTreeConfigPtr TSchedulerPool::GetPoolTreeConfig() const
{
    const TSchedulerPool* schedulerPool = this;
    while (auto* parent = schedulerPool->GetParent()) {
        schedulerPool = parent;
    }

    if (!schedulerPool->IsRoot()) {
        // NB: Unlikely to happen.
        THROW_ERROR_EXCEPTION("Failed to get pool tree config because the pool is detached from the hierarchy")
            << TErrorAttribute("pool_name", GetName());
    }

    return schedulerPool->GetMaybePoolTree()->GetDeserializedConfigOrThrow();
}

void TSchedulerPool::Save(NCellMaster::TSaveContext& context) const
{
    TBase::Save(context);

    using NYT::Save;

    Save(context, SpecifiedAttributes_);
    Save(context, MaybePoolTree_);
}

void TSchedulerPool::Load(NCellMaster::TLoadContext& context)
{
    TBase::Load(context);

    using NYT::Load;

    const auto& schedulerPoolManager = context.GetBootstrap()->GetSchedulerPoolManager();
    const auto& knownPoolAttributes = schedulerPoolManager->GetKnownPoolAttributes();

    if (ToUnderlying(context.GetVersion()) != NCellMaster::GetCurrentReign() && !IsRoot_) {
        // Some attributes could become unknown.
        // We will move them from known attributes map to unknown attributes map.
        THashMap<TString, TYsonString> oldSpecifiedAttributes;
        Load(context, oldSpecifiedAttributes);

        for (auto& [uninternedKey, value]: oldSpecifiedAttributes) {
            auto internedKey = TInternedAttributeKey::Lookup(uninternedKey);
            if (knownPoolAttributes.contains(internedKey)) {
                EmplaceOrCrash(SpecifiedAttributes_, internedKey, std::move(value));
            } else {
                GetMutableAttributes()->Set(uninternedKey, value);
                YT_LOG_INFO("Moving pool attribute from specified attributes map to common attributes map "
                    "(ObjectId: %v, AttributeName: %v, AttributeValue: %v)",
                    Id_,
                    uninternedKey,
                    ConvertToYsonString(value, EYsonFormat::Text));
            }
        }
    } else {
        Load(context, SpecifiedAttributes_);
    }

    Load(context, MaybePoolTree_);

    FullConfig_->Load(ConvertToNode(SpecifiedAttributes_));

    if (ToUnderlying(context.GetVersion()) != NCellMaster::GetCurrentReign() && !IsRoot_ && Attributes_) {
        // Move attributes from unknown attributes map to known attributes map.
        TCompactVector<TString, 3> keysToRemove;
        for (const auto& [key, value] : Attributes_->Attributes()) {
            auto internedKey = TInternedAttributeKey::Lookup(key);
            if (internedKey == InvalidInternedAttribute) {
                continue;
            }
            if (knownPoolAttributes.contains(internedKey)) {
                if (SpecifiedAttributes_.contains(internedKey)) {
                    YT_LOG_ALERT("Found pool attribute that is stored in both SpecifiedAttributes map and common attributes map "
                        "(ObjectId: %v, AttributeName: %v, CommonAttributeValue: %v, SpecifiedAttributeValue: %v)",
                        Id_,
                        key,
                        ConvertToYsonString(value, EYsonFormat::Text),
                        ConvertToYsonString(SpecifiedAttributes_[internedKey]), EYsonFormat::Text);
                } else {
                    try {
                        YT_LOG_INFO("Moving pool attribute from common attributes map to SpecifiedAttributes map "
                            "(ObjectId: %v, AttributeName: %v, AttributeValue: %v)",
                            Id_,
                            key,
                            ConvertToYsonString(value, EYsonFormat::Text));
                        FullConfig_->LoadParameter(key, NYTree::ConvertToNode(value));
                        EmplaceOrCrash(SpecifiedAttributes_, internedKey, std::move(value));
                        keysToRemove.push_back(key);
                    } catch (const std::exception& e) {
                        YT_LOG_ALERT(e, "Cannot parse value of pool attribute; the value will be dropped "
                            "(ObjectId: %v, AttributeName: %v, AttributeValue: %v)",
                            Id_,
                            key,
                            ConvertToYsonString(value, EYsonFormat::Text));
                    }
                }
            }
        }

        for (const auto& key : keysToRemove) {
            YT_VERIFY(Attributes_->Remove(key));
        }
    }
}

void TSchedulerPool::GuardedUpdatePoolAttribute(
    TInternedAttributeKey key,
    const std::function<void(const TPoolConfigPtr&, const TString&)>& update)
{
    const auto& uninternedKey = key.Unintern();
    update(FullConfig_, uninternedKey);

    try {
        FullValidate();
        GetParent()->ValidateChildrenCompatibility();
    } catch (const std::exception&) {
        auto restoringValueIt = SpecifiedAttributes_.find(key);
        if (restoringValueIt != SpecifiedAttributes_.end()) {
            // TODO(renadeen): avoid building INode
            FullConfig_->LoadParameter(uninternedKey, NYTree::ConvertToNode(restoringValueIt->second));
        } else {
            FullConfig_->ResetParameter(uninternedKey);
        }
        throw;
    }
}

TInternedAttributeKey TSchedulerPool::RemapDeprecatedKey(TInternedAttributeKey key)
{
    return key == EInternedAttributeKey::MinShareResources
        ? EInternedAttributeKey::StrongGuaranteeResources
        : key;
}

TPoolResourcesPtr TSchedulerPool::GetResources() const
{
    auto result = New<TPoolResources>();
    result->StrongGuaranteeResources = FullConfig_->StrongGuaranteeResources->Clone();
    result->ResourceFlow = FullConfig_->IntegralGuarantees->ResourceFlow->Clone();
    result->BurstGuaranteeResources = FullConfig_->IntegralGuarantees->BurstGuaranteeResources->Clone();
    result->MaxOperationCount = FullConfig_->MaxOperationCount;
    result->MaxRunningOperationCount = FullConfig_->MaxRunningOperationCount;
    return result;
}

void TSchedulerPool::SetResourcesInConfig(TPoolResourcesPtr resources)
{
    FullConfig_->StrongGuaranteeResources = std::move(resources->StrongGuaranteeResources);
    FullConfig_->IntegralGuarantees->BurstGuaranteeResources = std::move(resources->BurstGuaranteeResources);
    FullConfig_->IntegralGuarantees->ResourceFlow = std::move(resources->ResourceFlow);
    FullConfig_->MaxOperationCount = std::move(resources->MaxOperationCount);
    FullConfig_->MaxRunningOperationCount = std::move(resources->MaxRunningOperationCount);
}

void TSchedulerPool::AddResourcesToConfig(const TPoolResourcesPtr& poolResources)
{
    *(FullConfig_->StrongGuaranteeResources) += *poolResources->StrongGuaranteeResources;
    *(FullConfig_->IntegralGuarantees->ResourceFlow) += *poolResources->ResourceFlow;
    *(FullConfig_->IntegralGuarantees->BurstGuaranteeResources) += *poolResources->BurstGuaranteeResources;
    if (poolResources->MaxOperationCount) {
        if (FullConfig_->MaxOperationCount) {
            *FullConfig_->MaxOperationCount += *poolResources->MaxOperationCount;
        } else {
            FullConfig_->MaxOperationCount = poolResources->MaxOperationCount;
        }
    }
    if (poolResources->MaxRunningOperationCount) {
        if (FullConfig_->MaxRunningOperationCount) {
            *FullConfig_->MaxRunningOperationCount += *poolResources->MaxRunningOperationCount;
        } else {
            FullConfig_->MaxRunningOperationCount = poolResources->MaxRunningOperationCount;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerPoolTree::TSchedulerPoolTree(NCypressClient::TObjectId id)
    : TBase(id)
    , SpecifiedConfig_(ConvertToYsonString(EmptyAttributes()))
{ }

TString TSchedulerPoolTree::GetLowercaseObjectName() const
{
    return Format("scheduler pool tree %Qv", TreeName_);
}

TString TSchedulerPoolTree::GetCapitalizedObjectName() const
{
    return Format("Scheduler pool tree %Qv", TreeName_);
}

TString TSchedulerPoolTree::GetObjectPath() const
{
    return Format("//sys/pool_trees/%v", TreeName_);
}

void TSchedulerPoolTree::Save(NCellMaster::TSaveContext& context) const
{
    TBase::Save(context);

    using NYT::Save;
    Save(context, TreeName_);
    Save(context, RootPool_);
    Save(context, SpecifiedConfig_);
}

void TSchedulerPoolTree::Load(NCellMaster::TLoadContext& context)
{
    TBase::Load(context);

    using NYT::Load;
    Load(context, TreeName_);
    Load(context, RootPool_);
    Load(context, SpecifiedConfig_);
}

void TSchedulerPoolTree::UpdateSpecifiedConfig(TYsonString newConfig)
{
    SpecifiedConfig_ = std::move(newConfig);
    MemoizedDeserializedPoolTreeConfig_.Reset();
}

TFairShareStrategyTreeConfigPtr TSchedulerPoolTree::GetDeserializedConfigOrThrow() const
{
    if (!MemoizedDeserializedPoolTreeConfig_) {
        try {
            MemoizedDeserializedPoolTreeConfig_ = ConvertTo<TFairShareStrategyTreeConfigPtr>(SpecifiedConfig_);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Invalid pool tree config")
                    << TErrorAttribute("pool_tree", GetTreeName())
                    << ex;
        }
    }

    return MemoizedDeserializedPoolTreeConfig_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
