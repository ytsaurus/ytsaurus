#pragma once

#include "public.h"

#include "scheduler_pool.h"

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolManager
    : public TRefCounted
{
public:
    explicit TSchedulerPoolManager(NCellMaster::TBootstrap* bootstrap);

    ~TSchedulerPoolManager();

    void Initialize();

    TSchedulerPool* CreateSchedulerPool();

    TSchedulerPool* FindSchedulerPoolByName(const TString& treeName, const TString& name) const;
    TSchedulerPoolTree* FindPoolTreeObjectByName(const TString& treeName) const;
    TSchedulerPool* FindPoolTreeOrSchedulerPoolOrThrow(const TString& treeName, const TString& name) const;

    const THashMap<TString, TSchedulerPoolTree*>& GetPoolTrees() const;

    const THashSet<NYTree::TInternedAttributeKey>& GetKnownPoolAttributes();
    const THashSet<NYTree::TInternedAttributeKey>& GetKnownPoolTreeAttributes();
    bool IsUserManagedAttribute(NYTree::TInternedAttributeKey key);

    // Pool tree name is obtained from root object.
    // It has complexity linear in the object's depth since we have to traverse all parents all the way to the root.
    TString GetPoolTreeName(const TSchedulerPool* schedulerPool) noexcept;

private:
    class TImpl;
    class TSchedulerPoolTypeHandler;
    class TSchedulerPoolTreeTypeHandler;

    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerPoolManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
