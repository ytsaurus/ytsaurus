#pragma once

#include "public.h"

#include "scheduler_pool.h"

#include <yt/yt/server/master/object_server/map_object.h>

#include <yt/yt/ytlib/scheduler/proto/pool_ypath.pb.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolFactory
    : public NObjectServer::TNonversionedMapObjectFactoryBase<TSchedulerPool>
{
public:
    explicit TSchedulerPoolFactory(NCellMaster::TBootstrap* bootstrap);

    TSchedulerPool* DoCreateObject(NYTree::IAttributeDictionary* attributes) override;

private:
    TIntrusivePtr<NObjectServer::TNonversionedMapObjectProxyBase<TSchedulerPool>> GetSchedulerPoolProxy(NObjectServer::TObject* schedulerPool) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolProxy
    : public NObjectServer::TNonversionedMapObjectProxyBase<TSchedulerPool>
{
public:
    using TNonversionedMapObjectProxyBase::TNonversionedMapObjectProxyBase;

protected:
    using TProxyBasePtr = TIntrusivePtr<NObjectServer::TNonversionedMapObjectProxyBase<TSchedulerPool>>;

    void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
    bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;
    NYTree::EPermission GetCustomAttributeModifyPermission() override;

    // TODO(renadeen): move to SchedulerPool?
    bool IsKnownPoolAttribute(NYTree::TInternedAttributeKey key);
    bool IsKnownPoolTreeAttribute(NYTree::TInternedAttributeKey key);

    TProxyBasePtr ResolveNameOrThrow(const TString& name) override;

    std::optional<TString> GetMaybePoolTreeName(const TSchedulerPool* schedulerPool);

    void ValidateChildNameAvailability(const TString& newChildName) override;
    void ValidateAfterAttachChild(const TString& key, const TProxyBasePtr& childProxy) override;

    void DoRemoveSelf(bool recursive, bool force) override;

    std::unique_ptr<NObjectServer::TNonversionedMapObjectFactoryBase<TSchedulerPool>> CreateObjectFactory() const override;

    bool DoInvoke(const NYTree::IYPathServiceContextPtr& context) override;

    DECLARE_YPATH_SERVICE_METHOD(NScheduler::NProto, TransferPoolResources);

private:
    using TBase = TNonversionedMapObjectProxyBase<TSchedulerPool>;

    void GuardedUpdateBuiltinPoolAttribute(
        NYTree::TInternedAttributeKey key,
        const std::function<void(const NScheduler::TPoolConfigPtr&, const TString&)>& update);

    void ValidateNoAliasClash(
        const NYTree::TYsonStructPtr& config,
        const TSpecifiedAttributesMap& specifiedAttributes,
        NYTree::TInternedAttributeKey key);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
