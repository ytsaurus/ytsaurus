#include "scheduler_pool.h"

#include "scheduler_pool_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>

namespace NYT::NSchedulerPoolServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NScheduler;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

INodePtr ConvertToNode(const THashMap<TInternedAttributeKey, TYsonString>& attributes)
{
    return BuildYsonNodeFluently()
        .DoMapFor(attributes, [] (TFluentMap fluent, const auto& pair) {
            fluent.Item(GetUninternedAttributeKey(pair.first)).Value(pair.second);
        });
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerPool::TSchedulerPool(TObjectId id, bool isRoot)
    : TBase(id, isRoot)
    , FullConfig_(New<NScheduler::TPoolConfig>())
{ }

TString TSchedulerPool::GetObjectName() const
{
    return IsRoot()
        ? MaybePoolTree_->GetObjectName()
        : Format("Scheduler pool %Qv", GetName());
}

void TSchedulerPool::ValidateAll()
{
    FullConfig_->Validate();
    ValidateChildrenCompatibility();
    GetParent()->ValidateChildrenCompatibility();
}

void TSchedulerPool::ValidateChildrenCompatibility()
{
    if (IsRoot()) {
        return;
    }

    // TODO(renadeen): move children validation to pool config?
    FullConfig()->MinShareResources->ForEachResource([this] (auto TResourceLimitsConfig::* resourceDataMember, const TString& name) {
        using TResource = typename std::remove_reference_t<decltype(std::declval<TResourceLimitsConfig>().*resourceDataMember)>::value_type;
        auto getResource = [&] (TSchedulerPool* object) -> TResource {
            return (object->FullConfig()->MinShareResources.Get()->*resourceDataMember).value_or(0);
        };

        auto parentResource = getResource(this);
        TResource childrenResourceSum = 0;
        for (const auto& [_, child] : KeyToChild_) {
            childrenResourceSum += getResource(child);
        }

        if (parentResource < childrenResourceSum) {
            THROW_ERROR_EXCEPTION("Guarantee of resource for pool %Qv is less than the sum of children guarantees",  GetName())
                << TErrorAttribute("resource_name", name)
                << TErrorAttribute("pool_name", GetName())
                << TErrorAttribute("parent_resource_value", parentResource)
                << TErrorAttribute("children_resource_sum", childrenResourceSum);
        }
    });

    if (!KeyToChild().empty() && FullConfig()->Mode == NScheduler::ESchedulingMode::Fifo) {
        THROW_ERROR_EXCEPTION("Pool %Qv cannot have subpools since it is in FIFO mode")
            << TErrorAttribute("pool_name", GetName());
    }
}

void TSchedulerPool::Save(NCellMaster::TSaveContext& context) const
{
    TBase::Save(context);

    using NYT::Save;

    TSizeSerializer::Save(context, SpecifiedAttributes_.size());
    for (const auto& [k, v] : SpecifiedAttributes_) {
        Save(context, GetUninternedAttributeKey(k));
        Save(context, v);
    }

    Save(context, MaybePoolTree_);
}

void TSchedulerPool::Load(NCellMaster::TLoadContext& context)
{
    TBase::Load(context);

    using NYT::Load;

    // COMPAT(shakurov)
    if (context.GetVersion() < EMasterReign::SpecifiedAttributeFix) {
        Load(context, SpecifiedAttributes_);
    } else {
        auto specifiedAttributeSize = TSizeSerializer::Load(context);
        SpecifiedAttributes_.reserve(specifiedAttributeSize);
        for (size_t i = 0; i < specifiedAttributeSize; ++i) {
            auto k = Load<TString>(context);
            auto v = Load<TYsonString>(context);
            YT_VERIFY(SpecifiedAttributes_.emplace(GetInternedAttributeKey(k), v).second);
        }
    }

    Load(context, MaybePoolTree_);

    FullConfig_->Load(ConvertToNode(SpecifiedAttributes_));
}

void TSchedulerPool::GuardedUpdatePoolAttribute(
    TInternedAttributeKey key,
    const std::function<void(const TPoolConfigPtr&, const TString&)>& update)
{
    const auto& stringKey = GetUninternedAttributeKey(key);

    update(FullConfig_, stringKey);
    try {
        ValidateAll();
    } catch (const std::exception&) {
        auto restoringValueIt = SpecifiedAttributes_.find(key);
        if (restoringValueIt != SpecifiedAttributes_.end()) {
            // TODO(renadeen): avoid building INode
            FullConfig_->LoadParameter(stringKey, NYTree::ConvertToNode(restoringValueIt->second), EMergeStrategy::Overwrite);
        } else {
            FullConfig_->ResetParameter(stringKey);
        }
        throw;
    }
}
////////////////////////////////////////////////////////////////////////////////

TSchedulerPoolTree::TSchedulerPoolTree(NCypressClient::TObjectId id)
    : TBase(id)
    , FullConfig_(New<TFairShareStrategyTreeConfig>())
{ }

TString TSchedulerPoolTree::GetObjectName() const
{
    return Format("Scheduler pool tree %Qv", TreeName_);
}

void TSchedulerPoolTree::Save(NCellMaster::TSaveContext& context) const
{
    TBase::Save(context);

    using NYT::Save;
    Save(context, TreeName_);
    Save(context, RootPool_);

    TSizeSerializer::Save(context, SpecifiedAttributes_.size());
    for (const auto& [k, v] : SpecifiedAttributes_) {
        Save(context, GetUninternedAttributeKey(k));
        Save(context, v);
    }
}

void TSchedulerPoolTree::Load(NCellMaster::TLoadContext& context)
{
    TBase::Load(context);

    using NYT::Load;
    Load(context, TreeName_);
    Load(context, RootPool_);

    // COMPAT(shakurov)
    if (context.GetVersion() < EMasterReign::SpecifiedAttributeFix) {
        Load(context, SpecifiedAttributes_);
    } else {
        auto specifiedAttributeSize = TSizeSerializer::Load(context);
        SpecifiedAttributes_.reserve(specifiedAttributeSize);
        for (size_t i = 0; i < specifiedAttributeSize; ++i) {
            auto k = Load<TString>(context);
            auto v = Load<TYsonString>(context);
            YT_VERIFY(SpecifiedAttributes_.emplace(GetInternedAttributeKey(k), v).second);
        }
    }

    FullConfig_->Load(ConvertToNode(SpecifiedAttributes_));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
