#include "scheduler_pool_manager.h"

#include "scheduler_pool.h"
#include "scheduler_pool_proxy.h"

#include <yt/server/lib/scheduler/helpers.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/lib/hydra/composite_automaton.h>

#include <yt/server/master/object_server/map_object_type_handler.h>

namespace NYT::NSchedulerPoolServer {

using namespace NObjectServer;
using namespace NCellMaster;
using namespace NHydra;
using namespace NScheduler;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolManager::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::SchedulerPoolManager)
    {
        RegisterLoader(
            "SchedulerPoolManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "SchedulerPoolManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "SchedulerPoolManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "SchedulerPoolManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
    }

    void Initialize();

    TSchedulerPoolTree* CreatePoolTree(TString treeName)
    {
        NScheduler::CheckPoolName(treeName)
            .ThrowOnError();

        if (FindPoolTreeObjectByName(treeName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Pool tree %Qv already exists",
                treeName);
        }

        auto* rootPool = CreateSchedulerPoolObject(/* isRoot */ true);

        auto* poolTree = CreateSchedulerPoolTreeObject();
        poolTree->SetTreeName(treeName);
        poolTree->SetRootPool(rootPool);

        rootPool->SetMaybePoolTree(poolTree);

        RegisterPoolTreeObject(std::move(treeName), poolTree);

        return poolTree;
    }

    TSchedulerPool* CreateSchedulerPoolObject(bool isRoot)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::SchedulerPool, NullObjectId);

        auto* schedulerPool = SchedulerPoolMap_.Insert(id, std::make_unique<TSchedulerPool>(id, isRoot));

        // Make the fake reference.
        YT_VERIFY(schedulerPool->RefObject() == 1);

        return schedulerPool;
    }

    TSchedulerPoolTree* CreateSchedulerPoolTreeObject()
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::SchedulerPoolTree, NullObjectId);

        auto* poolTree = SchedulerPoolTreeMap_.Insert(id, std::make_unique<TSchedulerPoolTree>(id));

        // Make the fake reference.
        YT_VERIFY(poolTree->RefObject() == 1);

        return poolTree;
    }

    void RegisterPoolTreeObject(TString treeName, TSchedulerPoolTree* schedulerPool)
    {
        YT_VERIFY(PoolTreeToPoolsMap_.emplace(treeName, THashMap<TString, TSchedulerPool*>()).second);
        YT_VERIFY(PoolTrees_.emplace(std::move(treeName), schedulerPool).second);
    }

    void UnregisterPoolTreeObject(const TString& treeName)
    {
        YT_VERIFY(PoolTreeToPoolsMap_.erase(treeName) == 1);
        YT_VERIFY(PoolTrees_.erase(treeName) == 1);
    }

    TSchedulerPool* FindSchedulerPoolByName(const TString& treeName, const TString& name) const
    {
        auto poolsMapIt = PoolTreeToPoolsMap_.find(treeName);
        if (poolsMapIt == PoolTreeToPoolsMap_.end()) {
            return nullptr;
        }
        const auto& poolsMap = poolsMapIt->second;
        auto it = poolsMap.find(name);
        return it != poolsMap.end() ? it->second : nullptr;
    }

    TSchedulerPoolTree* FindPoolTreeObjectByName(const TString& treeName) const
    {
        auto it = PoolTrees_.find(treeName);
        return it != PoolTrees_.end() ? it->second : nullptr;
    }

    TSchedulerPool* FindSchedulerPoolOrRootPoolOrThrow(const TString& treeName, const TString& name) const
    {
        auto* poolTree = FindPoolTreeObjectByName(treeName);
        if (!poolTree) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "Pool tree %Qv does not exist",
                treeName);
        }
        if (name == RootPoolName) {
            return poolTree->GetRootPool();
        }
        auto* schedulerPool = FindSchedulerPoolByName(treeName, name);
        if (!schedulerPool) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "Pool %Qv does not exist",
                name);
        }
        return schedulerPool;
    }

    void RegisterPoolName(const TString& name, TSchedulerPool* schedulerPool)
    {
        auto poolTreeName = GetPoolTreeName(schedulerPool);
        auto it = PoolTreeToPoolsMap_.find(poolTreeName);
        YT_VERIFY(it != PoolTreeToPoolsMap_.end());
        YT_VERIFY(it->second.emplace(name, schedulerPool).second);
    }

    void UnregisterPoolName(const TString& name, TSchedulerPool* schedulerPool)
    {
        auto poolTreeName = GetPoolTreeName(schedulerPool);
        auto it = PoolTreeToPoolsMap_.find(poolTreeName);
        YT_VERIFY(it != PoolTreeToPoolsMap_.end());
        YT_VERIFY(it->second.erase(name) == 1);
    }

    const THashMap<TString, TSchedulerPoolTree*>& GetPoolTrees() const
    {
        return PoolTrees_;
    }

    const THashSet<TInternedAttributeKey>& GetKnownPoolAttributes()
    {
        return GetKnownAttributes<TPoolConfig>(KnownPoolAttributes_);
    }

    const THashSet<TInternedAttributeKey>& GetKnownPoolTreeAttributes()
    {
        return GetKnownAttributes<TFairShareStrategyTreeConfig>(KnownPoolTreeAttributes_);
    }

    template<class TConfig>
    const THashSet<TInternedAttributeKey>& GetKnownAttributes(THashSet<TInternedAttributeKey>& knownAttributesCache)
    {
        if (knownAttributesCache.empty()) {
            auto registeredKeyStrings = New<TConfig>()->GetRegisteredKeys();
            knownAttributesCache.reserve(registeredKeyStrings.size());
            for (const auto& registeredKey : registeredKeyStrings) {
                auto internedAttribute = TInternedAttributeKey::Lookup(registeredKey);
                YT_VERIFY(internedAttribute != InvalidInternedAttribute);
                knownAttributesCache.insert(TInternedAttributeKey::Lookup(registeredKey));
            }
        }

        return knownAttributesCache;
    }

    bool IsUserManagedAttribute(TInternedAttributeKey key)
    {
        switch (key) {
            case EInternedAttributeKey::Weight:
            case EInternedAttributeKey::MaxOperationCount:
            case EInternedAttributeKey::MaxRunningOperationCount:
            case EInternedAttributeKey::MinShareResources:
            case EInternedAttributeKey::ForbidImmediateOperations:
            case EInternedAttributeKey::Mode:
            case EInternedAttributeKey::FifoSortParameters:
            case EInternedAttributeKey::ResourceLimits:
            case EInternedAttributeKey::CreateEphemeralSubpools:
            case EInternedAttributeKey::EphemeralSubpoolConfig:
                return true;
            default:
                return false;
        }
    }

    TString GetPoolTreeName(const TSchedulerPool* schedulerPool) noexcept
    {
        while (auto* parent = schedulerPool->GetParent()) {
            schedulerPool = parent;
        }
        YT_VERIFY(schedulerPool->IsRoot());
        return schedulerPool->GetMaybePoolTree()->GetTreeName();
    }

private:
    friend class TSchedulerPoolTypeHandler;
    friend class TSchedulerPoolTreeTypeHandler;

    TEntityMap<TSchedulerPool> SchedulerPoolMap_;
    TEntityMap<TSchedulerPoolTree> SchedulerPoolTreeMap_;

    THashMap<TString, THashMap<TString, TSchedulerPool*>> PoolTreeToPoolsMap_;
    THashMap<TString, TSchedulerPoolTree*> PoolTrees_;

    THashSet<TInternedAttributeKey> KnownPoolAttributes_;
    THashSet<TInternedAttributeKey> KnownPoolTreeAttributes_;

    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        for (const auto& [_, schedulerPoolTree] : SchedulerPoolTreeMap_) {
            if (!IsObjectAlive(schedulerPoolTree)) {
                continue;
            }
            YT_VERIFY(PoolTrees_.emplace(schedulerPoolTree->GetTreeName(), schedulerPoolTree).second);
            auto [it, inserted] = PoolTreeToPoolsMap_.emplace(schedulerPoolTree->GetTreeName(), THashMap<TString, TSchedulerPool*>());
            YT_VERIFY(inserted);

            BuildPoolNameMapRecursively(schedulerPoolTree->GetRootPool(), &it->second);
        }
    }

    void BuildPoolNameMapRecursively(TSchedulerPool* schedulerPool, THashMap<TString, TSchedulerPool*>* map)
    {
        for (const auto& [_, child] : schedulerPool->KeyToChild()) {
            YT_VERIFY(map->emplace(child->GetName(), child).second);
            BuildPoolNameMapRecursively(child, map);
        }
    }

    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        PoolTrees_.clear();
        PoolTreeToPoolsMap_.clear();
        SchedulerPoolMap_.Clear();
        SchedulerPoolTreeMap_.Clear();
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        SchedulerPoolTreeMap_.SaveKeys(context);
        SchedulerPoolMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        SchedulerPoolTreeMap_.SaveValues(context);
        SchedulerPoolMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        SchedulerPoolTreeMap_.LoadKeys(context);
        SchedulerPoolMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        SchedulerPoolTreeMap_.LoadValues(context);
        SchedulerPoolMap_.LoadValues(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolManager::TSchedulerPoolTypeHandler
    : public NObjectServer::TNonversionedMapObjectTypeHandlerBase<TSchedulerPool>
{
public:
    explicit TSchedulerPoolTypeHandler(TSchedulerPoolManager::TImpl* owner)
        : TBase(owner->Bootstrap_, &owner->SchedulerPoolMap_)
        , Owner_(owner)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::SchedulerPool;
    }

    virtual TObject* CreateObject(TObjectId hintId, IAttributeDictionary* attributes) override
    {
        const auto name = attributes->GetAndRemove<TString>("name");
        const auto poolTree = attributes->GetAndRemove<TString>("pool_tree");
        const auto parentName = attributes->GetAndRemove<TString>("parent_name", RootPoolName);
        auto* parentObject = Owner_->FindSchedulerPoolOrRootPoolOrThrow(poolTree, parentName);

        return CreateObjectImpl(name, parentObject, attributes);
    }

    virtual void RegisterName(const TString& name, TSchedulerPool* schedulerPool) noexcept override
    {
        Owner_->RegisterPoolName(name, schedulerPool);
    }

    virtual void UnregisterName(const TString& name, TSchedulerPool* schedulerPool) noexcept override
    {
        Owner_->UnregisterPoolName(name, schedulerPool);
    }

    virtual void ValidateObjectName(const TString& name) override
    {
        ValidatePoolName(name);
    }

    virtual TString GetRootPath(const TSchedulerPool* rootPool) const override
    {
        YT_VERIFY(rootPool && rootPool->IsRoot());
        YT_VERIFY(rootPool->GetMaybePoolTree());
        return Format("%v/%v", PoolTreesRootCypressPath, rootPool->GetMaybePoolTree()->GetTreeName());
    }

protected:
    virtual std::optional<int> GetDepthLimit() const override
    {
        return 30;
    }

    virtual TProxyPtr GetMapObjectProxy(TSchedulerPool* object) override
    {
        return New<TSchedulerPoolProxy>(Owner_->Bootstrap_, &Metadata_, object);
    }

    virtual NObjectServer::TObject* FindObjectByAttributes(const NYTree::IAttributeDictionary* attributes) override
    {
        auto poolTree = attributes->Get<TString>("pool_tree");
        auto parentName = attributes->Get<TString>("parent_name", RootPoolName);
        auto* parentObject = Owner_->FindSchedulerPoolOrRootPoolOrThrow(poolTree, parentName);

        auto name = attributes->Get<TString>("name");
        return parentObject->FindChild(name);
    }

    virtual NObjectServer::TObject* DoGetParent(TSchedulerPool* object) override
    {
        if (!object->IsRoot()) {
            return TBase::DoGetParent(object);
        }
        const auto& poolTreeHandler = Bootstrap_->GetObjectManager()->GetHandler(EObjectType::SchedulerPoolTree);
        return poolTreeHandler->GetParent(object->GetMaybePoolTree());
    }

    virtual void DoZombifyObject(TSchedulerPool* object) override
    {
        if (object->IsRoot()) {
            if (IsObjectAlive(object->GetMaybePoolTree())) {
                Bootstrap_->GetObjectManager()->RemoveObject(object->GetMaybePoolTree());
            }
            object->SetMaybePoolTree(nullptr);
        }

        TBase::DoZombifyObject(object);
    }

private:
    TSchedulerPoolManager::TImpl* Owner_;

    using TBase = TNonversionedMapObjectTypeHandlerBase<TSchedulerPool>;
};

class TSchedulerPoolManager::TSchedulerPoolTreeTypeHandler
    : public TObjectTypeHandlerWithMapBase<TSchedulerPoolTree>
{
public:
    explicit TSchedulerPoolTreeTypeHandler(TSchedulerPoolManager::TImpl* owner)
        : TBase(owner->Bootstrap_, &owner->SchedulerPoolTreeMap_)
        , Owner_(owner)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::SchedulerPoolTree;
    }

    virtual TObject* CreateObject(TObjectId hintId, IAttributeDictionary* attributes) override
    {
        const auto name = attributes->GetAndRemove<TString>("name");
        ValidatePoolTreeCreationPermission();
        return Owner_->CreatePoolTree(name);
    }

protected:
    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    virtual IObjectProxyPtr DoGetProxy(TSchedulerPoolTree* object, NTransactionServer::TTransaction* transaction) override
    {
        return New<TSchedulerPoolProxy>(Owner_->Bootstrap_, &Metadata_, object->GetRootPool());
    }

    virtual NObjectServer::TObject* DoGetParent(TSchedulerPoolTree* object) override
    {
        return Bootstrap_->GetCypressManager()->ResolvePathToTrunkNode(PoolTreesRootCypressPath);
    }

    virtual void DoZombifyObject(TSchedulerPoolTree* object) override
    {
        Owner_->UnregisterPoolTreeObject(object->GetTreeName());
        if (IsObjectAlive(object->GetRootPool())) {
            Bootstrap_->GetObjectManager()->RemoveObject(object->GetRootPool());
        }
        object->SetRootPool(nullptr);

        TBase::DoZombifyObject(object);
    }

    NObjectServer::TObject* FindObjectByAttributes(const NYTree::IAttributeDictionary* attributes) override
    {
        auto name = attributes->Get<TString>("name");
        return Owner_->FindPoolTreeObjectByName(name);
    }

private:
    TSchedulerPoolManager::TImpl* Owner_;

    void ValidatePoolTreeCreationPermission()
    {
        auto* poolTreesRoot = Bootstrap_->GetCypressManager()->ResolvePathToTrunkNode(PoolTreesRootCypressPath);
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        securityManager->ValidatePermission(poolTreesRoot, user, EPermission::Write);
    }

    using TBase = TObjectTypeHandlerWithMapBase<TSchedulerPoolTree>;
};

void TSchedulerPoolManager::TImpl::Initialize()
{
    Bootstrap_->GetObjectManager()->RegisterHandler(New<TSchedulerPoolTypeHandler>(this));
    Bootstrap_->GetObjectManager()->RegisterHandler(New<TSchedulerPoolTreeTypeHandler>(this));
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerPoolManager::TSchedulerPoolManager(TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TSchedulerPoolManager::~TSchedulerPoolManager()
{ }

void TSchedulerPoolManager::Initialize()
{
    Impl_->Initialize();
}

const THashMap<TString, TSchedulerPoolTree*>& TSchedulerPoolManager::GetPoolTrees() const
{
    return Impl_->GetPoolTrees();
}

TSchedulerPool* TSchedulerPoolManager::FindSchedulerPoolByName(const TString& treeName, const TString& name) const
{
    return Impl_->FindSchedulerPoolByName(treeName, name);
}

TSchedulerPoolTree* TSchedulerPoolManager::FindPoolTreeObjectByName(const TString& treeName) const
{
    return Impl_->FindPoolTreeObjectByName(treeName);
}

TSchedulerPool* TSchedulerPoolManager::FindPoolTreeOrSchedulerPoolOrThrow(const TString& treeName, const TString& name) const
{
    return Impl_->FindSchedulerPoolOrRootPoolOrThrow(treeName, name);
}

TString TSchedulerPoolManager::GetPoolTreeName(const TSchedulerPool* schedulerPool) noexcept
{
    return Impl_->GetPoolTreeName(schedulerPool);
}

TSchedulerPool* TSchedulerPoolManager::CreateSchedulerPool()
{
    return Impl_->CreateSchedulerPoolObject(/* isRoot */ false);
}

const THashSet<TInternedAttributeKey>& TSchedulerPoolManager::GetKnownPoolAttributes()
{
    return Impl_->GetKnownPoolAttributes();
}

const THashSet<TInternedAttributeKey>& TSchedulerPoolManager::GetKnownPoolTreeAttributes()
{
    return Impl_->GetKnownPoolTreeAttributes();
}

bool TSchedulerPoolManager::IsUserManagedAttribute(NYTree::TInternedAttributeKey key)
{
    return Impl_->IsUserManagedAttribute(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
