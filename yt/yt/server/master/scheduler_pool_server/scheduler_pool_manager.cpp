#include "scheduler_pool_manager.h"

#include "pool_resources.h"
#include "private.h"
#include "config.h"
#include "scheduler_pool.h"
#include "scheduler_pool_proxy.h"

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/server/master/object_server/map_object.h>
#include <yt/yt/server/master/object_server/map_object_type_handler.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

namespace NYT::NSchedulerPoolServer {

using namespace NObjectServer;
using namespace NCellMaster;
using namespace NHydra;
using namespace NScheduler;
using namespace NSecurityClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger& Logger = SchedulerPoolServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolManager
    : public ISchedulerPoolManager
    , public TMasterAutomatonPart
{
public:
    explicit TSchedulerPoolManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::SchedulerPoolManager)
    {
        RegisterLoader(
            "SchedulerPoolManager.Keys",
            BIND(&TSchedulerPoolManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "SchedulerPoolManager.Values",
            BIND(&TSchedulerPoolManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "SchedulerPoolManager.Keys",
            BIND(&TSchedulerPoolManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "SchedulerPoolManager.Values",
            BIND(&TSchedulerPoolManager::SaveValues, Unretained(this)));
    }

    void Initialize() override;

    TSchedulerPoolTree* CreatePoolTree(TString treeName)
    {
        ValidatePoolName(treeName, PoolNameRegexForAdministrators());

        if (FindPoolTreeObjectByName(treeName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Pool tree %Qv already exists",
                treeName);
        }

        auto* rootPool = CreateSchedulerPoolObject(/*isRoot*/ true);

        auto* poolTree = CreateSchedulerPoolTreeObject();
        poolTree->SetTreeName(treeName);
        poolTree->SetRootPool(rootPool);

        rootPool->SetMaybePoolTree(poolTree);

        RegisterPoolTreeObject(std::move(treeName), poolTree);

        return poolTree;
    }

    TSchedulerPool* CreateSchedulerPool() override
    {
        return CreateSchedulerPoolObject(/*isRoot*/ false);
    }

    TSchedulerPool* CreateSchedulerPoolObject(bool isRoot)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::SchedulerPool);

        auto* schedulerPool = SchedulerPoolMap_.Insert(id, TPoolAllocator::New<TSchedulerPool>(id, isRoot));

        // Make the fake reference.
        YT_VERIFY(schedulerPool->RefObject() == 1);

        return schedulerPool;
    }

    TSchedulerPoolTree* CreateSchedulerPoolTreeObject()
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::SchedulerPoolTree);

        auto* poolTree = SchedulerPoolTreeMap_.Insert(id, TPoolAllocator::New<TSchedulerPoolTree>(id));

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

    TSchedulerPool* FindSchedulerPoolByName(const TString& treeName, const TString& name) const override
    {
        auto poolsMapIt = PoolTreeToPoolsMap_.find(treeName);
        if (poolsMapIt == PoolTreeToPoolsMap_.end()) {
            return nullptr;
        }
        const auto& poolsMap = poolsMapIt->second;
        auto it = poolsMap.find(name);
        return it != poolsMap.end() ? it->second : nullptr;
    }

    TSchedulerPoolTree* FindPoolTreeObjectByName(const TString& treeName) const override
    {
        auto it = PoolTrees_.find(treeName);
        return it != PoolTrees_.end() ? it->second : nullptr;
    }

    TSchedulerPool* FindPoolTreeOrSchedulerPoolOrThrow(const TString& treeName, const TString& name) const override
    {
        // TODO(renadeen, gritukan): Is it intended?
        return FindSchedulerPoolOrRootPoolOrThrow(treeName, name);
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
        auto maybePoolTreeName = GetMaybePoolTreeName(schedulerPool);
        YT_VERIFY(maybePoolTreeName);
        auto it = PoolTreeToPoolsMap_.find(*maybePoolTreeName);
        YT_VERIFY(it != PoolTreeToPoolsMap_.end());
        YT_VERIFY(it->second.emplace(name, schedulerPool).second);
    }

    void UnregisterPoolName(const TString& name, TSchedulerPool* schedulerPool)
    {
        auto maybePoolTreeName = GetMaybePoolTreeName(schedulerPool);
        YT_VERIFY(maybePoolTreeName);
        auto it = PoolTreeToPoolsMap_.find(*maybePoolTreeName);
        YT_VERIFY(it != PoolTreeToPoolsMap_.end());
        YT_VERIFY(it->second.erase(name) == 1);
    }

    const THashMap<TString, TSchedulerPoolTree*>& GetPoolTrees() const override
    {
        return PoolTrees_;
    }

    const THashSet<TInternedAttributeKey>& GetKnownPoolAttributes() override
    {
        return GetKnownAttributes<TPoolConfig>(KnownPoolAttributes_);
    }

    const THashSet<TInternedAttributeKey>& GetKnownPoolTreeAttributes() override
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

    bool IsUserManagedAttribute(TInternedAttributeKey key) override
    {
        switch (key) {
            case EInternedAttributeKey::Weight:
            case EInternedAttributeKey::MaxOperationCount:
            case EInternedAttributeKey::MaxRunningOperationCount:
            case EInternedAttributeKey::MinShareResources:
            case EInternedAttributeKey::StrongGuaranteeResources:
            case EInternedAttributeKey::ForbidImmediateOperations:
            case EInternedAttributeKey::Mode:
            case EInternedAttributeKey::FifoSortParameters:
            case EInternedAttributeKey::ResourceLimits:
            case EInternedAttributeKey::CreateEphemeralSubpools:
            case EInternedAttributeKey::EphemeralSubpoolConfig:
            case EInternedAttributeKey::Abc:
            case EInternedAttributeKey::IntegralGuarantees:
                return true;
            default:
                return false;
        }
    }

    std::optional<TString> GetMaybePoolTreeName(const TSchedulerPool* schedulerPool) noexcept override
    {
        while (auto* parent = schedulerPool->GetParent()) {
            schedulerPool = parent;
        }

        if (!schedulerPool->IsRoot()) {
            return {};
        }

        return schedulerPool->GetMaybePoolTree()->GetTreeName();
    }

    const TDynamicSchedulerPoolManagerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->SchedulerPoolManager;
    }

    void TransferPoolResources(
        TSchedulerPool* srcPool,
        TSchedulerPool* dstPool,
        const TPoolResourcesPtr& resourceDelta) override
    {
        YT_VERIFY(srcPool);
        YT_VERIFY(dstPool);

        YT_LOG_DEBUG(
            "Transferring pool resources (SrcPool: %v, DstPool: %v, ResourceDelta: %v)",
            srcPool->GetName(),
            dstPool->GetName(),
            ConvertToYsonString(resourceDelta, EYsonFormat::Text));

        TCommandTransaction transaction;
        try {
            auto* lcaPool = FindMapObjectLCA(srcPool, dstPool);
            if (!lcaPool) {
                YT_LOG_ALERT("Pools do not have common ancestor (SrcPool: %v, DstPool: %v)",
                    srcPool->GetName(),
                    dstPool->GetName());
                THROW_ERROR_EXCEPTION("Pools do not have common ancestor")
                    << TErrorAttribute("src_pool", srcPool->GetName())
                    << TErrorAttribute("dst_pool", dstPool->GetName());
            }

            const auto& securityManager = Bootstrap_->GetSecurityManager();
            for (auto* pool = srcPool; pool != lcaPool; pool = pool->GetParent()) {
                securityManager->ValidatePermission(pool, EPermission::Write);
            }
            for (auto* pool = dstPool; pool != lcaPool; pool = pool->GetParent()) {
                securityManager->ValidatePermission(pool, EPermission::Write);
            }

            for (auto* pool = srcPool; pool != lcaPool; pool = pool->GetParent()) {
                transaction.AddResources(pool, -*resourceDelta);
                pool->FullValidate();
            }

            TCompactVector<TSchedulerPool*, 4> reverseDstAncestry;
            for (auto* pool = dstPool; pool != lcaPool; pool = pool->GetParent()) {
                reverseDstAncestry.push_back(pool);
            }
            std::reverse(reverseDstAncestry.begin(), reverseDstAncestry.end());
            for (auto* pool: reverseDstAncestry) {
                transaction.AddResources(pool, resourceDelta);
                pool->FullValidate();
            }

            if (lcaPool == srcPool || lcaPool == dstPool) {
                lcaPool->ValidateChildrenCompatibility();
            }
        } catch (const std::exception& ex) {
            transaction.Abort();
            THROW_ERROR_EXCEPTION("Failed to transfer resources from pool %Qv to pool %Qv",
                srcPool->GetName(),
                dstPool->GetName())
                << ex;
        }
        transaction.Commit();
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        const auto& config = GetDynamicConfig();
        UpdatePoolNameRegexes(config->PoolNameRegexForAdministrators, config->PoolNameRegexForUsers);
    }

    void UpdatePoolNameRegexes(
        const TString& poolNameRegexForAdministrators,
        const TString& poolNameRegexForUsers)
    {
        if (!PoolNameRegexForAdministrators_ ||
            PoolNameRegexForAdministrators_->pattern() != poolNameRegexForAdministrators)
        {
            PoolNameRegexForAdministrators_.emplace(poolNameRegexForAdministrators);
        }

        if (!PoolNameRegexForUsers_ ||
            PoolNameRegexForUsers_->pattern() != poolNameRegexForUsers)
        {
            PoolNameRegexForUsers_.emplace(poolNameRegexForUsers);
        }
    }

    void ValidateSchedulerPoolName(const TString& name)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* schema = Bootstrap_->GetObjectManager()->FindSchema(EObjectType::SchedulerPool);
        auto* user = securityManager->GetAuthenticatedUser();
        const re2::RE2& validationRegex = securityManager->CheckPermission(schema, user, EPermission::Administer).Action == ESecurityAction::Deny
            ? PoolNameRegexForUsers()
            : PoolNameRegexForAdministrators();

        if (!validationRegex.ok()) {
            THROW_ERROR_EXCEPTION("Pool name validation regular expression is malformed")
                << TErrorAttribute("regular_expression", validationRegex.pattern())
                << TErrorAttribute("inner_error", validationRegex.error());
        }

        ValidatePoolName(name, validationRegex);
    }

private:
    class TCommandTransaction
    {
    public:
        void AddResources(TSchedulerPool* pool, const TPoolResourcesPtr& delta)
        {
            PoolResourcesBackup_.emplace_back(pool, pool->GetResources());
            pool->AddResourcesToConfig(delta);
        }

        void Commit()
        {
            for (const auto& [pool, oldResources]: PoolResourcesBackup_) {
                ApplyConfigChanges(pool, oldResources);
            }
        }

        void Abort() noexcept
        {
            std::reverse(PoolResourcesBackup_.begin(), PoolResourcesBackup_.end());
            for (auto& [pool, oldResources]: PoolResourcesBackup_) {
                pool->SetResourcesInConfig(std::move(oldResources));
            }
        }

    private:
        TCompactVector<std::pair<TSchedulerPool*, TPoolResourcesPtr>, 4> PoolResourcesBackup_;

        void ApplyConfigChanges(TSchedulerPool* pool, const TPoolResourcesPtr& oldResources)
        {
            TCompactVector<TString, 3> logMessages;
            auto& attributes = pool->SpecifiedAttributes();
            const auto& poolConfig = pool->FullConfig();
            const auto& actualStrongGuaranteeResources = poolConfig->StrongGuaranteeResources;
            if (!oldResources->StrongGuaranteeResources->IsEqualTo(*actualStrongGuaranteeResources)) {
                attributes[EInternedAttributeKey::StrongGuaranteeResources] = ConvertToYsonString(actualStrongGuaranteeResources);
                logMessages.push_back(Format(
                    "StrongGuaranteeResources: %v",
                    ConvertToYsonString(actualStrongGuaranteeResources, EYsonFormat::Text)));
            }
            const auto& actualIntegralGuarantees = poolConfig->IntegralGuarantees;
            const auto& oldBurstGuaranteeResources = oldResources->BurstGuaranteeResources;
            const auto& oldResourceFlow = oldResources->ResourceFlow;
            const auto& actualBurstGuaranteeResources = actualIntegralGuarantees->BurstGuaranteeResources;
            const auto& actualResourceFlow = actualIntegralGuarantees->ResourceFlow;
            if (!oldBurstGuaranteeResources->IsEqualTo(*actualBurstGuaranteeResources)
                || !oldResourceFlow->IsEqualTo(*actualResourceFlow))
            {
                // TODO(renadeen): extract resources from integral guarantees and rewrite this code.
                auto it = attributes.find(EInternedAttributeKey::IntegralGuarantees);
                auto updatedIntegralGuarantees = it != attributes.end()
                    ? ConvertTo<IMapNodePtr>(it->second)
                    : GetEphemeralNodeFactory()->CreateMap();

                if (!oldBurstGuaranteeResources->IsEqualTo(*actualBurstGuaranteeResources)) {
                    updatedIntegralGuarantees->RemoveChild("burst_guarantee_resources");
                    updatedIntegralGuarantees->AddChild("burst_guarantee_resources", ConvertToNode(actualBurstGuaranteeResources));
                }
                if (!oldResourceFlow->IsEqualTo(*actualResourceFlow)) {
                    updatedIntegralGuarantees->RemoveChild("resource_flow");
                    updatedIntegralGuarantees->AddChild("resource_flow", ConvertToNode(actualResourceFlow));
                }
                attributes[EInternedAttributeKey::IntegralGuarantees] = ConvertToYsonString(updatedIntegralGuarantees);
                logMessages.push_back(Format(
                    "IntegralGuarantees: %v",
                    ConvertToYsonString(actualIntegralGuarantees, EYsonFormat::Text)));
            }

            if (oldResources->MaxOperationCount != poolConfig->MaxOperationCount) {
                attributes[EInternedAttributeKey::MaxOperationCount] = ConvertToYsonString(poolConfig->MaxOperationCount);
                logMessages.push_back(Format("MaxOperationCount: %v", poolConfig->MaxOperationCount));
            }
            if (oldResources->MaxRunningOperationCount != poolConfig->MaxRunningOperationCount) {
                attributes[EInternedAttributeKey::MaxRunningOperationCount] = ConvertToYsonString(poolConfig->MaxRunningOperationCount);
                logMessages.push_back(Format("MaxRunningOperationCount: %v", poolConfig->MaxRunningOperationCount));
            }
            YT_LOG_DEBUG("Updated pool resources (PoolName: %v, %v)", pool->GetName(), JoinToString(logMessages));
        }
    };

    friend class TSchedulerPoolTypeHandler;
    friend class TSchedulerPoolTreeTypeHandler;

    TEntityMap<TSchedulerPool> SchedulerPoolMap_;
    TEntityMap<TSchedulerPoolTree> SchedulerPoolTreeMap_;

    THashMap<TString, THashMap<TString, TSchedulerPool*>> PoolTreeToPoolsMap_;
    THashMap<TString, TSchedulerPoolTree*> PoolTrees_;

    THashSet<TInternedAttributeKey> KnownPoolAttributes_;
    THashSet<TInternedAttributeKey> KnownPoolTreeAttributes_;

    // NB: re2::RE2 does not have default constructor.
    std::optional<re2::RE2> PoolNameRegexForAdministrators_;
    std::optional<re2::RE2> PoolNameRegexForUsers_;

    const re2::RE2& PoolNameRegexForAdministrators() const
    {
        YT_VERIFY(PoolNameRegexForAdministrators_);
        YT_VERIFY(PoolNameRegexForAdministrators_->ok());
        return *PoolNameRegexForAdministrators_;
    }

    const re2::RE2& PoolNameRegexForUsers() const
    {
        YT_VERIFY(PoolNameRegexForUsers_);
        YT_VERIFY(PoolNameRegexForUsers_->ok());
        return *PoolNameRegexForUsers_;
    }

    void SetZeroState() override
    {
        TMasterAutomatonPart::SetZeroState();

        const auto& config = GetDynamicConfig();
        UpdatePoolNameRegexes(config->PoolNameRegexForAdministrators, config->PoolNameRegexForUsers);
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        const auto& config = GetDynamicConfig();
        UpdatePoolNameRegexes(config->PoolNameRegexForAdministrators, config->PoolNameRegexForUsers);

        for (const auto& [_, schedulerPoolTree] : SchedulerPoolTreeMap_) {
            if (!IsObjectAlive(schedulerPoolTree)) {
                continue;
            }
            YT_VERIFY(PoolTrees_.emplace(schedulerPoolTree->GetTreeName(), schedulerPoolTree).second);
            auto [it, inserted] = PoolTreeToPoolsMap_.emplace(schedulerPoolTree->GetTreeName(), THashMap<TString, TSchedulerPool*>());
            YT_VERIFY(inserted);

            BuildPoolNameMapRecursively(schedulerPoolTree->GetRootPool(), &it->second);
        }

        for (const auto& [_, schedulerPoolTree] : PoolTrees_) {
            RecomputeSubtreeSize(schedulerPoolTree->GetRootPool(), /*validateMatch*/ true);
        }
    }

    void BuildPoolNameMapRecursively(TSchedulerPool* schedulerPool, THashMap<TString, TSchedulerPool*>* map)
    {
        for (const auto& [_, child] : schedulerPool->KeyToChild()) {
            YT_VERIFY(map->emplace(child->GetName(), child).second);
            BuildPoolNameMapRecursively(child, map);
        }
    }

    void Clear() override
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

class TSchedulerPoolTypeHandler
    : public NObjectServer::TNonversionedMapObjectTypeHandlerBase<TSchedulerPool>
{
public:
    explicit TSchedulerPoolTypeHandler(TSchedulerPoolManager* owner)
        : TBase(owner->Bootstrap_, &owner->SchedulerPoolMap_)
        , Owner_(owner)
    { }

    EObjectType GetType() const override
    {
        return EObjectType::SchedulerPool;
    }

    TObject* CreateObject(TObjectId /*hintId*/, IAttributeDictionary* attributes) override
    {
        const auto name = attributes->GetAndRemove<TString>("name");
        const auto poolTree = attributes->GetAndRemove<TString>("pool_tree");
        const auto parentName = attributes->GetAndRemove<TString>("parent_name", RootPoolName);
        auto* parentObject = Owner_->FindSchedulerPoolOrRootPoolOrThrow(poolTree, parentName);

        return CreateObjectImpl(name, parentObject, attributes);
    }

    void RegisterName(const TString& name, TSchedulerPool* schedulerPool) noexcept override
    {
        Owner_->RegisterPoolName(name, schedulerPool);
    }

    void UnregisterName(const TString& name, TSchedulerPool* schedulerPool) noexcept override
    {
        Owner_->UnregisterPoolName(name, schedulerPool);
    }

    void ValidateObjectName(const TString& name) override
    {
        Owner_->ValidateSchedulerPoolName(name);
    }

    TString GetRootPath(const TSchedulerPool* rootPool) const override
    {
        YT_VERIFY(rootPool && rootPool->IsRoot());
        YT_VERIFY(rootPool->GetMaybePoolTree());
        return Format("%v/%v", PoolTreesRootCypressPath, rootPool->GetMaybePoolTree()->GetTreeName());
    }

protected:
    std::optional<int> GetDepthLimit() const override
    {
        return 30;
    }

    std::optional<int> GetSubtreeSizeLimit() const override
    {
        return Owner_->GetDynamicConfig()->MaxSchedulerPoolSubtreeSize;
    }

    TProxyPtr GetMapObjectProxy(TSchedulerPool* object) override
    {
        return New<TSchedulerPoolProxy>(Owner_->Bootstrap_, &Metadata_, object);
    }

    std::optional<NObjectServer::TObject*> FindObjectByAttributes(
        const NYTree::IAttributeDictionary* attributes) override
    {
        auto poolTree = attributes->Get<TString>("pool_tree");
        auto parentName = attributes->Get<TString>("parent_name", RootPoolName);
        auto* parentObject = Owner_->FindSchedulerPoolOrRootPoolOrThrow(poolTree, parentName);

        auto name = attributes->Get<TString>("name");
        return parentObject->FindChild(name);
    }

    NObjectServer::TObject* DoGetParent(TSchedulerPool* object) override
    {
        if (!object->IsRoot()) {
            return TBase::DoGetParent(object);
        }
        const auto& poolTreeHandler = Bootstrap_->GetObjectManager()->GetHandler(EObjectType::SchedulerPoolTree);
        return poolTreeHandler->GetParent(object->GetMaybePoolTree());
    }

    void DoZombifyObject(TSchedulerPool* object) override
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
    TSchedulerPoolManager* Owner_;

    using TBase = TNonversionedMapObjectTypeHandlerBase<TSchedulerPool>;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolTreeTypeHandler
    : public TObjectTypeHandlerWithMapBase<TSchedulerPoolTree>
{
public:
    explicit TSchedulerPoolTreeTypeHandler(TSchedulerPoolManager* owner)
        : TBase(owner->Bootstrap_, &owner->SchedulerPoolTreeMap_)
        , Owner_(owner)
    { }

    EObjectType GetType() const override
    {
        return EObjectType::SchedulerPoolTree;
    }

    TObject* CreateObject(TObjectId /*hintId*/, IAttributeDictionary* attributes) override
    {
        const auto name = attributes->GetAndRemove<TString>("name");
        ValidatePoolTreeCreationPermission();
        return Owner_->CreatePoolTree(name);
    }

protected:
    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    IObjectProxyPtr DoGetProxy(TSchedulerPoolTree* object, NTransactionServer::TTransaction* /*transaction*/) override
    {
        return New<TSchedulerPoolProxy>(Owner_->Bootstrap_, &Metadata_, object->GetRootPool());
    }

    NObjectServer::TObject* DoGetParent(TSchedulerPoolTree* /*object*/) override
    {
        return Bootstrap_->GetCypressManager()->ResolvePathToTrunkNode(PoolTreesRootCypressPath);
    }

    void DoZombifyObject(TSchedulerPoolTree* object) override
    {
        Owner_->UnregisterPoolTreeObject(object->GetTreeName());
        if (IsObjectAlive(object->GetRootPool())) {
            Bootstrap_->GetObjectManager()->RemoveObject(object->GetRootPool());
        }
        object->SetRootPool(nullptr);

        TBase::DoZombifyObject(object);
    }

    std::optional<NObjectServer::TObject*> FindObjectByAttributes(
        const NYTree::IAttributeDictionary* attributes) override
    {
        auto name = attributes->Get<TString>("name");
        return Owner_->FindPoolTreeObjectByName(name);
    }

private:
    TSchedulerPoolManager* Owner_;

    void ValidatePoolTreeCreationPermission()
    {
        auto* poolTreesRoot = Bootstrap_->GetCypressManager()->ResolvePathToTrunkNode(PoolTreesRootCypressPath);
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(poolTreesRoot, EPermission::Write);
    }

    using TBase = TObjectTypeHandlerWithMapBase<TSchedulerPoolTree>;
};

////////////////////////////////////////////////////////////////////////////////

void TSchedulerPoolManager::Initialize()
{
    Bootstrap_->GetObjectManager()->RegisterHandler(New<TSchedulerPoolTypeHandler>(this));
    Bootstrap_->GetObjectManager()->RegisterHandler(New<TSchedulerPoolTreeTypeHandler>(this));

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(BIND(&TSchedulerPoolManager::OnDynamicConfigChanged, MakeWeak(this)));
}

////////////////////////////////////////////////////////////////////////////////

ISchedulerPoolManagerPtr CreateSchedulerPoolManager(TBootstrap* bootstrap)
{
    return New<TSchedulerPoolManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
