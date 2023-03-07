#pragma once

#include "public.h"

#include "scheduler_pool.h"

#include <yt/server/master/object_server/map_object.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolFactory
    : public NObjectServer::TNonversionedMapObjectFactoryBase<TSchedulerPool>
{
public:
    explicit TSchedulerPoolFactory(NCellMaster::TBootstrap* bootstrap);

    virtual TSchedulerPool* DoCreateObject(NYTree::IAttributeDictionary* attributes) override;

private:
    TIntrusivePtr<NObjectServer::TNonversionedMapObjectProxyBase<TSchedulerPool>> GetSchedulerPoolProxy(NObjectServer::TObject* schedulerPool) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolProxy
    : public NObjectServer::TNonversionedMapObjectProxyBase<TSchedulerPool>
{
public:
    TSchedulerPoolProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        TSchedulerPool* schedulerPool);

protected:
    using TProxyBasePtr = TIntrusivePtr<NObjectServer::TNonversionedMapObjectProxyBase<TSchedulerPool>>;

    virtual void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    // TODO(renadeen): move to SchedulerPool?
    bool IsKnownPoolAttribute(NYTree::TInternedAttributeKey key);
    bool IsKnownPoolTreeAttribute(NYTree::TInternedAttributeKey key);

    virtual TProxyBasePtr ResolveNameOrThrow(const TString& name) override;

    TString GetPoolTreeName(const TSchedulerPool* schedulerPool);

    virtual void ValidateChildNameAvailability(const TString& newChildName) override;
    virtual void ValidateAfterAttachChild(const TString& key, const TProxyBasePtr& childProxy) override;

    virtual void DoRemoveSelf() override;

    virtual std::unique_ptr<NObjectServer::TNonversionedMapObjectFactoryBase<TSchedulerPool>> CreateObjectFactory() const override;

private:
    void GuardedUpdateBuiltinPoolAttribute(
        NYTree::TInternedAttributeKey key,
        const std::function<void(const NScheduler::TPoolConfigPtr&, const TString&)>& update);

    void ValidateNoAliasClash(
        const NYTree::TYsonSerializablePtr& config,
        const TSpecifiedAttributesMap& specifiedAttributes,
        NYTree::TInternedAttributeKey key);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
