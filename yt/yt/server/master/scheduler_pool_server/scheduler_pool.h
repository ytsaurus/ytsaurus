#pragma once

#include "public.h"

#include <yt/server/master/object_server/map_object.h>
#include <yt/server/master/object_server/map_object_proxy.h>
#include <yt/server/master/object_server/object.h>

#include <yt/server/lib/scheduler/config.h>

#include <yt/ytlib/scheduler/config.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

using TSpecifiedAttributesMap = THashMap<NYTree::TInternedAttributeKey, NYson::TYsonString>;

class TSchedulerPool
    : public NObjectServer::TNonversionedMapObjectBase<TSchedulerPool>
{
public:
    explicit TSchedulerPool(NCypressClient::TObjectId id, bool isRoot = false);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    // NB: exception is raised upon validation fails. Caller is obliged to restore correct state.
    void ValidateAll();
    void ValidateChildrenCompatibility();
    void ValidateStrongGuarantees(const NScheduler::TFairShareStrategyTreeConfigPtr& poolTreeConfig) const;
    void ValidateStrongGuaranteesRecursively(const NScheduler::TFairShareStrategyTreeConfigPtr& poolTreeConfig) const;

    template<class TResource>
    void ValidateChildrenGuaranteeSum(
        const char* guaranteeName,
        NScheduler::EJobResourceType resourceType,
        std::function<std::optional<TResource>(const NScheduler::TPoolConfigPtr&)> getResource);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // It is the map of attributes specified by user. It is stored to snapshot.
    // We want this object to behave like map node in terms of `get` attributes
    // i. e. we don't want to return attributes not specified by user.
    DEFINE_BYREF_RW_PROPERTY(TSpecifiedAttributesMap, SpecifiedAttributes);

    // It is config that we use to validate attributes specified by user.
    // Not specified attributes have default values.
    // We don't want to store it to snapshot
    // because change of parameter's default value would take no effect on stored config.
    DEFINE_BYREF_RW_PROPERTY(NScheduler::TPoolConfigPtr, FullConfig);

    // Pointer from root pool object to pool tree object.
    DEFINE_BYVAL_RW_PROPERTY(TSchedulerPoolTree*, MaybePoolTree, nullptr);

    void GuardedUpdatePoolAttribute(
        NYTree::TInternedAttributeKey key,
        const std::function<void(const NScheduler::TPoolConfigPtr&, const TString&)>& update);

private:
    using TBase = NObjectServer::TNonversionedMapObjectBase<TSchedulerPool>;

    NScheduler::TFairShareStrategyTreeConfigPtr GetPoolTreeConfig() const;
    void DoValidateStrongGuarantees(const NScheduler::TFairShareStrategyTreeConfigPtr& poolTreeConfig, bool recursive) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolTree
    : public NObjectServer::TNonversionedObjectBase
{
public:
    explicit TSchedulerPoolTree(NCypressClient::TObjectId id);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    DEFINE_BYVAL_RW_PROPERTY(TString, TreeName);
    DEFINE_BYVAL_RW_PROPERTY(TSchedulerPool*, RootPool, nullptr);

    DEFINE_BYREF_RO_PROPERTY(NYson::TYsonString, SpecifiedConfig);

    void UpdateSpecifiedConfig(NYson::TYsonString newConfig);

    // Throws an error if we fail to deserialize the specified config.
    NScheduler::TFairShareStrategyTreeConfigPtr GetDeserializedConfigOrThrow() const;

private:
    using TBase = NObjectServer::TNonversionedObjectBase;

    mutable NScheduler::TFairShareStrategyTreeConfigPtr MemoizedDeserializedPoolTreeConfig_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer

#define SCHEDULER_POOL_INL_H_
#include "scheduler_pool-inl.h"
#undef SCHEDULER_POOL_INL_H_
