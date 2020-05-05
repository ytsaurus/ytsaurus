#include "security_manager.h"
#include "private.h"
#include "account.h"
#include "account_proxy.h"
#include "acl.h"
#include "config.h"
#include "group.h"
#include "group_proxy.h"
#include "network_project.h"
#include "network_project_proxy.h"
#include "request_tracker.h"
#include "user.h"
#include "user_proxy.h"
#include "security_tags.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/serialize.h>
#include <yt/server/master/cell_master/config.h>

#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/chunk_requisition.h>
#include <yt/server/master/chunk_server/medium.h>

#include <yt/server/master/cypress_server/node.h>
#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/tablet_server/tablet.h>

#include <yt/server/lib/hydra/composite_automaton.h>
#include <yt/server/lib/hydra/entity_map.h>

#include <yt/server/master/object_server/map_object_type_handler.h>
#include <yt/server/master/object_server/type_handler_detail.h>

#include <yt/server/master/table_server/table_node.h>

#include <yt/server/master/transaction_server/transaction.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/lib/hive/hive_manager.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/security_client/group_ypath_proxy.h>

#include <yt/client/security_client/helpers.h>

#include <yt/core/concurrency/fls.h>

#include <yt/library/erasure/codec.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/intern_registry.h>

#include <yt/core/logging/fluent_log.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/ypath/token.h>

namespace NYT::NSecurityServer {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NCypressServer;
using namespace NSecurityClient;
using namespace NTableServer;
using namespace NObjectServer;
using namespace NHiveServer;
using namespace NProfiling;
using namespace NLogging;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;
static const auto& Profiler = SecurityServerProfiler;

static const auto ProfilingPeriod = TDuration::MilliSeconds(10000);

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard::TAuthenticatedUserGuard(TSecurityManagerPtr securityManager, TUser* user)
{
    if (user) {
        securityManager->SetAuthenticatedUser(user);
        SecurityManager_ = std::move(securityManager);
    }
}

TAuthenticatedUserGuard::~TAuthenticatedUserGuard()
{
    if (SecurityManager_) {
        SecurityManager_->ResetAuthenticatedUser();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TAccountTypeHandler
    : public TNonversionedMapObjectTypeHandlerBase<TAccount>
{
public:
    explicit TAccountTypeHandler(TImpl* owner);

    virtual ETypeFlags GetFlags() const override
    {
        return
            TBase::GetFlags() |
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::TwoPhaseCreation |
            ETypeFlags::TwoPhaseRemoval;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Account;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override;

    virtual std::unique_ptr<TObject> InstantiateObject(TObjectId id) override;

    virtual TObject* FindObjectByAttributes(const NYTree::IAttributeDictionary* attributes) override;

    virtual void RegisterName(const TString& name, TAccount* account) noexcept override;
    virtual void UnregisterName(const TString& name, TAccount* account) noexcept override;

    virtual TString GetRootPath(const TAccount* rootAccount) const override;

protected:
    virtual TCellTagList DoGetReplicationCellTags(const TAccount* /*account*/) override
    {
        return TObjectTypeHandlerBase<TAccount>::AllSecondaryCellTags();
    }

private:
    using TBase = TNonversionedMapObjectTypeHandlerBase<TAccount>;

    TImpl* const Owner_;

    virtual std::optional<int> GetDepthLimit() const override
    {
        return AccountTreeDepthLimit;
    }

    virtual TProxyPtr GetMapObjectProxy(TAccount* account) override;

    virtual void DoZombifyObject(TAccount* account) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TUserTypeHandler
    : public TObjectTypeHandlerWithMapBase<TUser>
{
public:
    explicit TUserTypeHandler(TImpl* owner);

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    virtual TCellTagList DoGetReplicationCellTags(const TUser* /*object*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::User;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TAccessControlDescriptor* DoFindAcd(TUser* user) override
    {
        return &user->Acd();
    }

    virtual IObjectProxyPtr DoGetProxy(TUser* user, TTransaction* transaction) override;
    virtual void DoZombifyObject(TUser* user) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TGroupTypeHandler
    : public TObjectTypeHandlerWithMapBase<TGroup>
{
public:
    explicit TGroupTypeHandler(TImpl* owner);

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Group;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TGroup* /*group*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual TAccessControlDescriptor* DoFindAcd(TGroup* group) override
    {
        return &group->Acd();
    }

    virtual IObjectProxyPtr DoGetProxy(TGroup* group, TTransaction* transaction) override;
    virtual void DoZombifyObject(TGroup* group) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TNetworkProjectTypeHandler
    : public TObjectTypeHandlerWithMapBase<TNetworkProject>
{
public:
    explicit TNetworkProjectTypeHandler(TImpl* owner);

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::NetworkProject;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override;

    virtual TAccessControlDescriptor* DoFindAcd(TNetworkProject* networkProject) override
    {
        return &networkProject->Acd();
    }

private:
    TImpl* const Owner_;

    virtual IObjectProxyPtr DoGetProxy(TNetworkProject* networkProject, TTransaction* transaction) override;
    virtual void DoZombifyObject(TNetworkProject* networkProject) override;
};

////////////////////////////////////////////////////////////////////////////////
class TSecurityManager::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::SecurityManager)
        , RequestTracker_(New<TRequestTracker>(bootstrap))
    {
        RegisterLoader(
            "SecurityManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "SecurityManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "SecurityManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "SecurityManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        auto cellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();

        RootAccountId_ = MakeWellKnownId(EObjectType::Account, cellTag, 0xfffffffffffffffb);
        SysAccountId_ = MakeWellKnownId(EObjectType::Account, cellTag, 0xffffffffffffffff);
        TmpAccountId_ = MakeWellKnownId(EObjectType::Account, cellTag, 0xfffffffffffffffe);
        IntermediateAccountId_ = MakeWellKnownId(EObjectType::Account, cellTag, 0xfffffffffffffffd);
        ChunkWiseAccountingMigrationAccountId_ = MakeWellKnownId(EObjectType::Account, cellTag, 0xfffffffffffffffc);

        RootUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xffffffffffffffff);
        GuestUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffe);
        JobUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffd);
        SchedulerUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffc);
        ReplicatorUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffb);
        OwnerUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffa);
        FileCacheUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xffffffffffffffef);
        OperationsCleanerUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xffffffffffffffee);
        OperationsClientUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xffffffffffffffed);
        TabletCellChangeloggerUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xffffffffffffffec);
        TabletCellSnapshotterUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xffffffffffffffeb);
        TableMountInformerUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xffffffffffffffea);

        EveryoneGroupId_ = MakeWellKnownId(EObjectType::Group, cellTag, 0xffffffffffffffff);
        UsersGroupId_ = MakeWellKnownId(EObjectType::Group, cellTag, 0xfffffffffffffffe);
        SuperusersGroupId_ = MakeWellKnownId(EObjectType::Group, cellTag, 0xfffffffffffffffd);

        RegisterMethod(BIND(&TImpl::HydraSetAccountStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRecomputeMembershipClosure, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateAccountMasterMemoryUsage, Unretained(this)));
    }

    void Initialize()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TAccountTypeHandler>(this));
        objectManager->RegisterHandler(New<TUserTypeHandler>(this));
        objectManager->RegisterHandler(New<TGroupTypeHandler>(this));
        objectManager->RegisterHandler(New<TNetworkProjectTypeHandler>(this));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND(&TImpl::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND(&TImpl::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Account, TAccount);
    DECLARE_ENTITY_MAP_ACCESSORS(User, TUser);
    DECLARE_ENTITY_MAP_ACCESSORS(Group, TGroup);
    DECLARE_ENTITY_MAP_ACCESSORS(NetworkProject, TNetworkProject);


    TAccount* CreateAccount(TObjectId hintId = NullObjectId)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Account, hintId);
        return DoCreateAccount(id);
    }

    void DestroyAccount(TAccount* account)
    { }

    TAccount* GetAccountOrThrow(TAccountId id)
    {
        auto* account = FindAccount(id);
        if (!IsObjectAlive(account)) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::NoSuchAccount,
                "No such account %v",
                id);
        }
        return account;
    }

    TAccount* FindAccountByName(const TString& name)
    {
        // Access buggy parentless accounts by id.
        if (name.StartsWith(NObjectClient::ObjectIdPathPrefix)) {
            TStringBuf idString(name, NObjectClient::ObjectIdPathPrefix.size());
            auto id = TObjectId::FromString(idString);
            auto* account = AccountMap_.Find(id);
            if (!IsObjectAlive(account) || account->GetName() != name) {
                return nullptr;
            }
            return account;
        }

        auto it = AccountNameMap_.find(name);
        auto* account = it == AccountNameMap_.end() ? nullptr : it->second;
        return IsObjectAlive(account) ? account : nullptr;
    }

    TAccount* GetAccountByNameOrThrow(const TString& name)
    {
        auto* account = FindAccountByName(name);
        if (!account) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::NoSuchAccount,
                "No such account %Qv",
                name);
        }
        return account;
    }

    void RegisterAccountName(const TString& name, TAccount* account) noexcept
    {
        YT_VERIFY(account);
        YT_VERIFY(AccountNameMap_.emplace(name, account).second);
    }

    void UnregisterAccountName(const TString& name) noexcept
    {
        YT_VERIFY(AccountNameMap_.erase(name) == 1);
    }

    TAccount* GetRootAccount()
    {
        return GetBuiltin(RootAccount_);
    }

    TAccount* GetSysAccount()
    {
        return GetBuiltin(SysAccount_);
    }

    TAccount* GetTmpAccount()
    {
        return GetBuiltin(TmpAccount_);
    }

    TAccount* GetIntermediateAccount()
    {
        return GetBuiltin(IntermediateAccount_);
    }

    TAccount* GetChunkWiseAccountingMigrationAccount()
    {
        return GetBuiltin(ChunkWiseAccountingMigrationAccount_);
    }

    TViolatedResourceLimits GetAccountRecursiveViolatedResourceLimits(const TAccount* account) const
    {
        return AccumulateOverMapObjectSubtree(
            account,
            TViolatedResourceLimits(),
            [] (const TAccount* account, TViolatedResourceLimits* violatedLimits) {
                if (account->IsNodeCountLimitViolated()) {
                    ++violatedLimits->NodeCount;
                }
                if (account->IsChunkCountLimitViolated()) {
                    ++violatedLimits->ChunkCount;
                }
                if (account->IsTabletCountLimitViolated()) {
                    ++violatedLimits->TabletCount;
                }
                if (account->IsTabletStaticMemoryLimitViolated()) {
                    ++violatedLimits->TabletStaticMemory;
                }

                for (const auto& [mediumIndex, usage] : account->ClusterStatistics().ResourceUsage.DiskSpace()) {
                    if (account->IsDiskSpaceLimitViolated(mediumIndex)) {
                        violatedLimits->AddToMediumDiskSpace(mediumIndex, 1);
                    }
                }
            });
    }

    bool IsAccountOvercommited(TAccount* account, TClusterResources newResources)
    {
        auto totalChildrenLimits = account->ComputeTotalChildrenLimits();
        const auto& dynamicConfig = GetDynamicConfig();
        if (!dynamicConfig->EnableMasterMemoryUsageAccountOvercommitValidation) {
            newResources.MasterMemory = 0;
            totalChildrenLimits.MasterMemory = 0;
        }
        return newResources.IsAtLeastOneResourceLessThan(totalChildrenLimits);
    }

    void ValidateResourceLimits(TAccount* account, const TClusterResources& resourceLimits)
    {
        if (!Bootstrap_->GetMulticellManager()->IsPrimaryMaster()) {
            return;
        }

        if (resourceLimits.IsAtLeastOneResourceLessThan(TClusterResources())) {
            THROW_ERROR_EXCEPTION("Failed to change resource limits for account %Qv: limits cannot be negative",
                account->GetName());
        }

        const auto& currentResourceLimits = account->ClusterResourceLimits();
        if (resourceLimits.IsAtLeastOneResourceLessThan(currentResourceLimits)) {
            for (const auto& [key, child] : SortHashMapByKeys(account->KeyToChild())) {
                if (resourceLimits.IsAtLeastOneResourceLessThan(child->ClusterResourceLimits())) {
                    THROW_ERROR_EXCEPTION("Failed to change resource limits for account %Qv: "
                        "the limit cannot be below that of its child account %Qv",
                        account->GetName(),
                        child->GetName());
                }
            }

            if (!account->GetAllowChildrenLimitOvercommit() && IsAccountOvercommited(account, resourceLimits)) {
                THROW_ERROR_EXCEPTION("Failed to change resource limits for account %Qv: "
                    "the limit cannot be below the sum of its children limits",
                    account->GetName());
            }
        }

        if (currentResourceLimits.IsAtLeastOneResourceLessThan(resourceLimits)) {
            if (auto* parent = account->GetParent()) {
                const auto& parentResourceLimits = parent->ClusterResourceLimits();
                if (parentResourceLimits.IsAtLeastOneResourceLessThan(resourceLimits)) {
                    THROW_ERROR_EXCEPTION("Failed to change resource limits for account %Qv: "
                        "the limit cannot be above that of its parent",
                        account->GetName());
                }

                if (!parent->GetAllowChildrenLimitOvercommit() &&
                    IsAccountOvercommited(parent, parent->ClusterResourceLimits() + currentResourceLimits - resourceLimits))
                {
                    THROW_ERROR_EXCEPTION("Failed to change resource limits for account %Qv: "
                        "the change would overcommit its parent",
                        account->GetName());
                }
            }
        }
    }

    void TrySetResourceLimits(TAccount* account, const TClusterResources& resourceLimits)
    {
        ValidateResourceLimits(account, resourceLimits);
        account->ClusterResourceLimits() = resourceLimits;
    }

    void UpdateResourceUsage(const TChunk* chunk, const TChunkRequisition& requisition, i64 delta)
    {
        YT_VERIFY(chunk->IsNative());

        auto doCharge = [] (TClusterResources* usage, int mediumIndex, i64 chunkCount, i64 diskSpace) {
            usage->AddToMediumDiskSpace(mediumIndex, diskSpace);
            usage->ChunkCount += chunkCount;
        };

        ComputeChunkResourceDelta(
            chunk,
            requisition,
            delta,
            [&] (TAccount* account, int mediumIndex, i64 chunkCount, i64 diskSpace, i64 masterMemory, bool committed) {
                account->SetMasterMemoryUsage(account->GetMasterMemoryUsage() + masterMemory);
                doCharge(&account->ClusterStatistics().ResourceUsage, mediumIndex, chunkCount, diskSpace);
                doCharge(&account->LocalStatistics().ResourceUsage, mediumIndex, chunkCount, diskSpace);
                if (committed) {
                    doCharge(&account->ClusterStatistics().CommittedResourceUsage, mediumIndex, chunkCount, diskSpace);
                    doCharge(&account->LocalStatistics().CommittedResourceUsage, mediumIndex, chunkCount, diskSpace);
                }
            });
    }

    void UpdateTransactionResourceUsage(
        const TChunk* chunk,
        const TChunkRequisition& requisition,
        i64 delta)
    {
        YT_ASSERT(chunk->IsStaged());
        YT_ASSERT(chunk->IsDiskSizeFinal());

        auto* stagingTransaction = chunk->GetStagingTransaction();
        auto* stagingAccount = chunk->GetStagingAccount();

        auto chargeTransaction = [&] (TAccount* account, int mediumIndex, i64 chunkCount, i64 diskSpace, i64 /*masterMemoryUsage*/, bool /*committed*/) {
            // If a chunk has been created before the migration but is being confirmed after it,
            // charge it to the staging account anyway: it's ok, because transaction resource usage accounting
            // isn't really delta-based, and it's nicer from the user's point of view.
            if (Y_UNLIKELY(account == ChunkWiseAccountingMigrationAccount_)) {
                account = stagingAccount;
            }

            auto* transactionUsage = GetTransactionAccountUsage(stagingTransaction, account);
            transactionUsage->AddToMediumDiskSpace(mediumIndex, diskSpace);
            transactionUsage->ChunkCount += chunkCount;
        };

        ComputeChunkResourceDelta(chunk, requisition, delta, chargeTransaction);
    }

    void ResetMasterMemoryUsage(TCypressNode* node)
    {
        ChargeMasterMemoryUsage(node, 0);
    }

    void UpdateMasterMemoryUsage(TCypressNode* node)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& typeHandler = cypressManager->GetHandler(node);
        ChargeMasterMemoryUsage(node, node->GetMasterMemoryUsage() + typeHandler->GetStaticMasterMemoryUsage());
    }

    void ChargeMasterMemoryUsage(TCypressNode* node, i64 currentMasterMemoryUsage)
    {
        auto* account = node->GetAccount();
        if (!account) {
            return;
        }

        auto delta = currentMasterMemoryUsage - node->GetChargedMasterMemoryUsage();
        YT_LOG_TRACE_UNLESS(IsRecovery(), "Updating master memory usage (Account: %v, MasterMemoryUsage: %v, Delta: %v)",
            account->GetName(),
            account->GetMasterMemoryUsage(),
            delta);
        account->SetMasterMemoryUsage(account->GetMasterMemoryUsage() + delta);
        if (account->GetMasterMemoryUsage() < 0) {
            YT_LOG_ALERT_UNLESS(IsRecovery(), "Master memory usage is negative (MasterMemoryUsage: %v, Account: %v)",
                account->GetMasterMemoryUsage(),
                account->GetName());
        }
        node->SetChargedMasterMemoryUsage(currentMasterMemoryUsage);
    }

    void ResetTransactionAccountResourceUsage(TTransaction* transaction)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (const auto& [account, usage] : transaction->AccountResourceUsage()) {
            objectManager->UnrefObject(account);
        }
        transaction->AccountResourceUsage().clear();
    }

    void RecomputeTransactionResourceUsage(TTransaction* transaction)
    {
        ResetTransactionAccountResourceUsage(transaction);

        auto addNodeResourceUsage = [&] (const TCypressNode* node) {
            if (node->IsExternal()) {
                return;
            }
            auto* account = node->GetAccount();
            auto* transactionUsage = GetTransactionAccountUsage(transaction, account);
            *transactionUsage += node->GetDeltaResourceUsage();
        };

        for (auto* node : transaction->BranchedNodes()) {
            addNodeResourceUsage(node);
        }
        for (auto* node : transaction->StagedNodes()) {
            addNodeResourceUsage(node);
        }
    }

    void SetAccount(
        TCypressNode* node,
        TAccount* newAccount,
        TTransaction* transaction) noexcept
    {
        YT_VERIFY(node);
        YT_VERIFY(newAccount);
        YT_VERIFY(node->IsTrunk() == !transaction);

        auto* oldAccount = node->GetAccount();
        YT_VERIFY(!oldAccount || !transaction);

        if (oldAccount == newAccount) {
            return;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        if (oldAccount) {
            if (auto* shard = node->GetShard()) {
                cypressManager->UpdateShardNodeCount(shard, oldAccount, -1);
            }
            UpdateAccountNodeCountUsage(node, oldAccount, nullptr, -1);
            ResetMasterMemoryUsage(node);
            objectManager->UnrefObject(oldAccount);
        }

        if (auto* shard = node->GetShard()) {
            cypressManager->UpdateShardNodeCount(shard, newAccount, +1);
        }
        UpdateAccountNodeCountUsage(node, newAccount, transaction, +1);
        node->SetAccount(newAccount);
        UpdateMasterMemoryUsage(node);
        objectManager->RefObject(newAccount);
        UpdateAccountTabletResourceUsage(node, oldAccount, true, newAccount, !transaction);
    }

    void ResetAccount(TCypressNode* node)
    {
        auto* account = node->GetAccount();
        if (!account) {
            return;
        }

        ResetMasterMemoryUsage(node);
        node->SetAccount(nullptr);

        UpdateAccountNodeCountUsage(node, account, node->GetTransaction(), -1);
        UpdateAccountTabletResourceUsage(node, account, !node->GetTransaction(), nullptr, false);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(account);
    }

    void UpdateAccountNodeCountUsage(TCypressNode* node, TAccount* account, TTransaction* transaction, i64 delta)
    {
        if (node->IsExternal()) {
            return;
        }

        auto resources = TClusterResources()
            .SetNodeCount(node->GetDeltaResourceUsage().NodeCount) * delta;

        ChargeAccountAncestry(
            account,
            [&] (TAccount* account) {
                account->ClusterStatistics().ResourceUsage += resources;
                account->LocalStatistics().ResourceUsage += resources;
            });

        if (transaction) {
            auto* transactionUsage = GetTransactionAccountUsage(transaction, account);
            *transactionUsage += resources;
        } else {
            ChargeAccountAncestry(
                account,
                [&] (TAccount* account) {
                    account->ClusterStatistics().CommittedResourceUsage += resources;
                    account->LocalStatistics().CommittedResourceUsage += resources;
                });
        }
    }

    void UpdateAccountTabletResourceUsage(TCypressNode* node, TAccount* oldAccount, bool oldCommitted, TAccount* newAccount, bool newCommitted)
    {
        if (node->IsExternal()) {
            return;
        }

        auto resources = node->GetDeltaResourceUsage()
            .SetNodeCount(0)
            .SetChunkCount(0);
        resources.ClearDiskSpace();

        UpdateTabletResourceUsage(node, oldAccount, -resources, oldCommitted);
        UpdateTabletResourceUsage(node, newAccount, resources, newCommitted);
    }

    void UpdateTabletResourceUsage(TCypressNode* node, const TClusterResources& resourceUsageDelta)
    {
        UpdateTabletResourceUsage(node, node->GetAccount(), resourceUsageDelta, node->IsTrunk());
    }

    void UpdateTabletResourceUsage(TCypressNode* node, TAccount* account, const TClusterResources& resourceUsageDelta, bool committed)
    {
        if (!account) {
            return;
        }

        YT_ASSERT(resourceUsageDelta.NodeCount == 0);
        YT_ASSERT(resourceUsageDelta.ChunkCount == 0);
        for (auto [mediumIndex, diskUsage] : resourceUsageDelta.DiskSpace()) {
            YT_ASSERT(diskUsage == 0);
        }

        ChargeAccountAncestry(
            account,
            [&] (TAccount* account) {
                account->ClusterStatistics().ResourceUsage += resourceUsageDelta;
                account->LocalStatistics().ResourceUsage += resourceUsageDelta;
                if (committed) {
                    account->ClusterStatistics().CommittedResourceUsage += resourceUsageDelta;
                    account->LocalStatistics().CommittedResourceUsage += resourceUsageDelta;
                }
            });
    }

    void DestroySubject(TSubject* subject)
    {
        for (auto* group : subject->MemberOf()) {
            YT_VERIFY(group->Members().erase(subject) == 1);
        }
        subject->MemberOf().clear();
        subject->RecursiveMemberOf().clear();

        for (auto [object, counter] : subject->LinkedObjects()) {
            auto* acd = GetAcd(object);
            acd->OnSubjectDestroyed(subject, GuestUser_);
        }
        subject->LinkedObjects().clear();
    }

    TUser* CreateUser(const TString& name, TObjectId hintId)
    {
        ValidateSubjectName(name);

        if (FindUserByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "User %Qv already exists",
                name);
        }

        if (FindGroupByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Group %Qv already exists",
                name);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::User, hintId);
        auto* user = DoCreateUser(id, name);
        if (user) {
            YT_LOG_DEBUG("User created (User: %v)", name);
            LogStructuredEventFluently(Logger, ELogLevel::Info)
                .Item("event").Value(EAccessControlEvent::UserCreated)
                .Item("name").Value(user->GetName());
        }
        return user;
    }

    void DestroyUser(TUser* user)
    {
        YT_VERIFY(UserNameMap_.erase(user->GetName()) == 1);
        DestroySubject(user);

        LogStructuredEventFluently(Logger, ELogLevel::Info)
            .Item("event").Value(EAccessControlEvent::UserDestroyed)
            .Item("name").Value(user->GetName());
    }

    TUser* FindUserByName(const TString& name)
    {
        auto it = UserNameMap_.find(name);
        return it == UserNameMap_.end() ? nullptr : it->second;
    }

    TUser* GetUserByNameOrThrow(const TString& name)
    {
        auto* user = FindUserByName(name);
        if (!IsObjectAlive(user)) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthenticationError,
                "No such user %Qv; create user by requesting any IDM role on this cluster",
                name);
        }
        return user;
    }

    TUser* GetUserOrThrow(TUserId id)
    {
        auto* user = FindUser(id);
        if (!IsObjectAlive(user)) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthenticationError,
                "No such user %v",
                id);
        }
        return user;
    }


    TUser* GetRootUser()
    {
        return GetBuiltin(RootUser_);
    }

    TUser* GetGuestUser()
    {
        return GetBuiltin(GuestUser_);
    }

    TUser* GetOwnerUser()
    {
        return GetBuiltin(OwnerUser_);
    }


    TGroup* CreateGroup(const TString& name, TObjectId hintId)
    {
        ValidateSubjectName(name);

        if (FindGroupByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Group %Qv already exists",
                name);
        }

        if (FindUserByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "User %Qv already exists",
                name);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Group, hintId);
        auto* group = DoCreateGroup(id, name);
        if (group) {
            YT_LOG_DEBUG("Group created (Group: %v)", name);
            LogStructuredEventFluently(Logger, ELogLevel::Info)
                .Item("event").Value(EAccessControlEvent::GroupCreated)
                .Item("name").Value(name);
        }
        return group;
    }

    void DestroyGroup(TGroup* group)
    {
        YT_VERIFY(GroupNameMap_.erase(group->GetName()) == 1);

        for (auto* subject : group->Members()) {
            YT_VERIFY(subject->MemberOf().erase(group) == 1);
        }
        group->Members().clear();

        for  (auto [userId, user] : UserMap_) {
            user->RecursiveMemberOf().erase(group);
        }

        DestroySubject(group);

        MaybeRecomputeMembershipClosure();

        LogStructuredEventFluently(Logger, ELogLevel::Info)
            .Item("event").Value(EAccessControlEvent::GroupDestroyed)
            .Item("name").Value(group->GetName());
    }

    TGroup* FindGroupByName(const TString& name)
    {
        auto it = GroupNameMap_.find(name);
        return it == GroupNameMap_.end() ? nullptr : it->second;
    }


    TGroup* GetEveryoneGroup()
    {
        return GetBuiltin(EveryoneGroup_);
    }

    TGroup* GetUsersGroup()
    {
        return GetBuiltin(UsersGroup_);
    }

    TGroup* GetSuperusersGroup()
    {
        return GetBuiltin(SuperusersGroup_);
    }


    TSubject* FindSubject(TSubjectId id)
    {
        auto* user = FindUser(id);
        if (IsObjectAlive(user)) {
            return user;
        }
        auto* group = FindGroup(id);
        if (IsObjectAlive(group)) {
            return group;
        }
        return nullptr;
    }

    TSubject* GetSubjectOrThrow(TSubjectId id)
    {
        auto* subject = FindSubject(id);
        if (!IsObjectAlive(subject)) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::NoSuchSubject,
                "No such subject %v",
                id);
        }
        return subject;
    }

    TNetworkProject* CreateNetworkProject(const TString& name, TObjectId hintId)
    {
        if (FindNetworkProjectByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Network project %Qv already exists",
                name);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::NetworkProject, hintId);
        auto* networkProject = DoCreateNetworkProject(id, name);
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Network project created (NetworkProject: %v)", name);
        LogStructuredEventFluently(Logger, ELogLevel::Info)
            .Item("event").Value(EAccessControlEvent::NetworkProjectCreated)
            .Item("name").Value(networkProject->GetName());
        return networkProject;
    }

    void DestroyNetworkProject(TNetworkProject* networkProject)
    {
        YT_VERIFY(NetworkProjectNameMap_.erase(networkProject->GetName()) == 1);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Network project destroyed (NetworkProject: %v)",
            networkProject->GetName());
        LogStructuredEventFluently(Logger, ELogLevel::Info)
            .Item("event").Value(EAccessControlEvent::NetworkProjectDestroyed)
            .Item("name").Value(networkProject->GetName());
    }

    TSubject* FindSubjectByName(const TString& name)
    {
        auto* user = FindUserByName(name);
        if (IsObjectAlive(user)) {
            return user;
        }
        auto* group = FindGroupByName(name);
        if (IsObjectAlive(group)) {
            return group;
        }
        return nullptr;
    }

    TSubject* GetSubjectByNameOrThrow(const TString& name)
    {
        auto* subject = FindSubjectByName(name);
        if (!IsObjectAlive(subject)) {
            THROW_ERROR_EXCEPTION("No such subject %Qv", name);
        }
        return subject;
    }

    TNetworkProject* FindNetworkProjectByName(const TString& name)
    {
        auto it = NetworkProjectNameMap_.find(name);
        return it == NetworkProjectNameMap_.end() ? nullptr : it->second;
    }

    void RenameNetworkProject(NYT::NSecurityServer::TNetworkProject* networkProject, const TString& newName)
    {
        if (FindNetworkProjectByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Network project %Qv already exists",
                newName);
        }

        YT_VERIFY(NetworkProjectNameMap_.erase(networkProject->GetName()) == 1);
        YT_VERIFY(NetworkProjectNameMap_.emplace(newName, networkProject).second);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Network project renamed (NetworkProject: %v, OldName: %v, NewName: %v",
            networkProject->GetId(),
            networkProject->GetName(),
            newName);

        networkProject->SetName(newName);
    }

    void AddMember(TGroup* group, TSubject* member, bool ignoreExisting)
    {
        ValidateMembershipUpdate(group, member);

        if (group->Members().find(member) != group->Members().end()) {
            if (ignoreExisting) {
                return;
            }
            THROW_ERROR_EXCEPTION("Member %Qv is already present in group %Qv",
                member->GetName(),
                group->GetName());
        }

        if (member->GetType() == EObjectType::Group) {
            auto* memberGroup = member->AsGroup();
            if (group == memberGroup || group->RecursiveMemberOf().find(memberGroup) != group->RecursiveMemberOf().end()) {
                THROW_ERROR_EXCEPTION("Adding group %Qv to group %Qv would produce a cycle",
                    memberGroup->GetName(),
                    group->GetName());
            }
        }

        DoAddMember(group, member);
        MaybeRecomputeMembershipClosure();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Group member added (Group: %v, Member: %v)",
            group->GetName(),
            member->GetName());

        LogStructuredEventFluently(Logger, ELogLevel::Info)
            .Item("event").Value(EAccessControlEvent::MemberAdded)
            .Item("group_name").Value(group->GetName())
            .Item("member_type").Value(member->GetType())
            .Item("member_name").Value(member->GetName());
    }

    void RemoveMember(TGroup* group, TSubject* member, bool force)
    {
        ValidateMembershipUpdate(group, member);

        if (group->Members().find(member) == group->Members().end()) {
            if (force) {
                return;
            }
            THROW_ERROR_EXCEPTION("Member %Qv is not present in group %Qv",
                member->GetName(),
                group->GetName());
        }

        DoRemoveMember(group, member);
        MaybeRecomputeMembershipClosure();

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Group member removed (Group: %v, Member: %v)",
            group->GetName(),
            member->GetName());

        LogStructuredEventFluently(Logger, ELogLevel::Info)
            .Item("event").Value(EAccessControlEvent::MemberRemoved)
            .Item("group_name").Value(group->GetName())
            .Item("member_type").Value(member->GetType())
            .Item("member_name").Value(member->GetName());
    }

    void RenameSubject(TSubject* subject, const TString& newName)
    {
        ValidateSubjectName(newName);

        if (FindSubjectByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Subject %Qv already exists",
                newName);
        }

        switch (subject->GetType()) {
            case EObjectType::User:
                YT_VERIFY(UserNameMap_.erase(subject->GetName()) == 1);
                YT_VERIFY(UserNameMap_.emplace(newName, subject->AsUser()).second);
                break;

            case EObjectType::Group:
                YT_VERIFY(GroupNameMap_.erase(subject->GetName()) == 1);
                YT_VERIFY(GroupNameMap_.emplace(newName, subject->AsGroup()).second);
                break;

            default:
                YT_ABORT();
        }

        LogStructuredEventFluently(Logger, ELogLevel::Info)
            .Item("event").Value(EAccessControlEvent::SubjectRenamed)
            .Item("subject_type").Value(subject->GetType())
            .Item("old_name").Value(subject->GetName())
            .Item("new_name").Value(newName);

        subject->SetName(newName);
    }


    TAccessControlDescriptor* FindAcd(TObject* object)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& handler = objectManager->GetHandler(object);
        return handler->FindAcd(object);
    }

    TAccessControlDescriptor* GetAcd(TObject* object)
    {
        auto* acd = FindAcd(object);
        YT_VERIFY(acd);
        return acd;
    }

    std::optional<TString> GetEffectiveAnnotation(TCypressNode* node)
    {
        while (node && !node->GetAnnotation()) {
            node = node->GetParent();
        }
        return node? node->GetAnnotation() : std::nullopt;
    }

    TAccessControlList GetEffectiveAcl(NObjectServer::TObject* object)
    {
        TAccessControlList result;
        const auto& objectManager = Bootstrap_->GetObjectManager();
        int depth = 0;
        while (object) {
            const auto& handler = objectManager->GetHandler(object);
            auto* acd = handler->FindAcd(object);
            if (acd) {
                for (auto entry : acd->Acl().Entries) {
                    auto inheritedMode = GetInheritedInheritanceMode(entry.InheritanceMode, depth);
                    if (inheritedMode) {
                        entry.InheritanceMode = *inheritedMode;
                        result.Entries.push_back(entry);
                    }
                }
                if (!acd->GetInherit()) {
                    break;
                }
            }

            object = handler->GetParent(object);
            ++depth;
        }

        return result;
    }


    void SetAuthenticatedUser(TUser* user)
    {
        *AuthenticatedUser_ = user;
    }

    void SetAuthenticatedUserByNameOrThrow(const TString& userName)
    {
        SetAuthenticatedUser(GetUserByNameOrThrow(userName));
    }

    void ResetAuthenticatedUser()
    {
        *AuthenticatedUser_ = nullptr;
    }

    TUser* GetAuthenticatedUser()
    {
        TUser* result = nullptr;

        if (AuthenticatedUser_.IsInitialized()) {
            result = *AuthenticatedUser_;
        }

        return result ? result : RootUser_;
    }

    std::optional<TString> GetAuthenticatedUserName()
    {
        if (auto* user = GetAuthenticatedUser()) {
            return user->GetName();
        }
        return std::nullopt;
    }


    bool IsSafeMode()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->EnableSafeMode;
    }

    TPermissionCheckResponse CheckPermission(
        TObject* object,
        TUser* user,
        EPermission permission,
        const TPermissionCheckOptions& options = {})
    {
        if (IsVersionedType(object->GetType()) && object->IsForeign()) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Checking permission for a versioned foreign object (ObjectId: %v)",
                object->GetId());
        }

        TPermissionChecker checker(
            this,
            user,
            permission,
            options);

        if (!checker.ShouldProceed()) {
            return checker.GetResponse();
        }

        // Slow lane: check ACLs through the object hierarchy.
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto* rootObject = cypressManager->GetRootNode();
        auto* currentObject = object;
        TSubject* owner = nullptr;
        int depth = 0;
        while (currentObject) {
            const auto& handler = objectManager->GetHandler(currentObject);
            auto* acd = handler->FindAcd(currentObject);

            // Check the current ACL, if any.
            if (acd) {
                if (!owner && currentObject == object) {
                    owner = acd->GetOwner();
                }

                for (const auto& ace: acd->Acl().Entries) {
                    checker.ProcessAce(ace, owner, currentObject, depth);
                    if (!checker.ShouldProceed()) {
                        break;
                    }
                }

                // Proceed to the parent object unless the current ACL explicitly forbids inheritance.
                if (!acd->GetInherit()) {
                    break;
                }
            }

            auto* parentObject = handler->GetParent(currentObject);

            // XXX(shakurov): YT-10896: remove this workaround.
            if (!HasMutationContext() && IsVersionedType(object->GetType())) {
                // Check if current object is orphaned.
                if (!parentObject && currentObject != rootObject) {
                    checker.ProcessAce(
                        TAccessControlEntry(
                            ESecurityAction::Allow,
                            GetEveryoneGroup(),
                            EPermissionSet(EPermission::Read)),
                        owner,
                        currentObject,
                        depth);
                }
            }

            currentObject = parentObject;
            ++depth;
        }

        return checker.GetResponse();
    }

    TPermissionCheckResponse CheckPermission(
        TUser* user,
        EPermission permission,
        const TAccessControlList& acl,
        const TPermissionCheckOptions& options = {})
    {
        TPermissionChecker checker(
            this,
            user,
            permission,
            options);

        if (!checker.ShouldProceed()) {
            return checker.GetResponse();
        }

        for (const auto& ace : acl.Entries) {
            checker.ProcessAce(ace, nullptr, nullptr, 0);
            if (!checker.ShouldProceed()) {
                break;
            }
        }

        return checker.GetResponse();
    }

    void ValidatePermission(
        TObject* object,
        TUser* user,
        EPermission permission,
        const TPermissionCheckOptions& options = {})
    {
        if (IsHiveMutation()) {
            return;
        }

        YT_VERIFY(!options.Columns);

        auto response = CheckPermission(object, user, permission, options);
        if (response.Action == ESecurityAction::Allow) {
            return;
        }

        TPermissionCheckTarget target;
        target.Object = object;
        LogAndThrowAuthorizationError(
            target,
            user,
            permission,
            response);
    }

    void ValidatePermission(
        TObject* object,
        EPermission permission,
        const TPermissionCheckOptions& options = {})
    {
        ValidatePermission(
            object,
            GetAuthenticatedUser(),
            permission,
            options);
    }

    void LogAndThrowAuthorizationError(
        const TPermissionCheckTarget& target,
        TUser* user,
        EPermission permission,
        const TPermissionCheckResult& result)
    {
        if (result.Action != ESecurityAction::Deny) {
            return;
        }

        auto objectName = GetPermissionCheckTargetName(target);

        TError error;

        if (IsSafeMode()) {
            error = TError(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Access denied for user %Qv: cluster is in safe mode; "
                "check for announces at https://infra.yandex-team.ru before reporting any issues",
                user->GetName());
        } else {
            auto event = LogStructuredEventFluently(Logger, ELogLevel::Info)
                .Item("event").Value(EAccessControlEvent::AccessDenied)
                .Item("user").Value(user->GetName())
                .Item("permission").Value(permission)
                .Item("object_name").Value(objectName);

            if (target.Column) {
                event = event
                    .Item("object_column").Value(*target.Column);
            }

            if (result.Object && result.Subject) {
                const auto& objectManager = Bootstrap_->GetObjectManager();
                auto deniedBy = objectManager->GetHandler(result.Object)->GetName(result.Object);

                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission for %v is denied for %Qv by ACE at %v",
                    user->GetName(),
                    permission,
                    objectName,
                    result.Subject->GetName(),
                    deniedBy)
                    << TErrorAttribute("denied_by", result.Object->GetId())
                    << TErrorAttribute("denied_for", result.Subject->GetId());

                event
                    .Item("reason").Value(EAccessDeniedReason::DeniedByAce)
                    .Item("denied_for").Value(result.Subject->GetName())
                    .Item("denied_by").Value(deniedBy);
            } else {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission for %v is not allowed by any matching ACE",
                    user->GetName(),
                    permission,
                    objectName);

                event
                    .Item("reason").Value(EAccessDeniedReason::NoAllowingAce);
            }
        }

        error.Attributes().Set("permission", permission);
        error.Attributes().Set("user", user->GetName());
        error.Attributes().Set("object_id", target.Object->GetId());
        if (target.Column) {
            error.Attributes().Set("object_column", target.Column);
        }
        THROW_ERROR(error);
    }

    void ValidateResourceUsageIncrease(
        TAccount* account,
        const TClusterResources& delta,
        bool allowRootAccount)
    {
        if (IsHiveMutation()) {
            return;
        }

        account->ValidateActiveLifeStage();

        if (!allowRootAccount && account == GetRootAccount()) {
            THROW_ERROR_EXCEPTION("Root account cannot be used");
        }

        auto* initialAccount = account;

        auto throwOverdraftError = [&] (
            const char* resourceType,
            TAccount* overdraftedAccount,
            const auto& usage,
            const auto& limit,
            const TMedium* medium = nullptr)
        {
            auto errorMessage = Format("%v %Qv is over %v limit%v%v",
                overdraftedAccount == initialAccount
                    ? "Account"
                    : overdraftedAccount == initialAccount->GetParent()
                        ? "Parent account"
                        : "Ancestor account",
                overdraftedAccount->GetName(),
                resourceType,
                medium
                    ? Format(" in medium %Qv", medium->GetName())
                    : TString(),
                overdraftedAccount != initialAccount
                    ? Format(" (while validating account %Qv)", initialAccount->GetName())
                    : TString());

            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AccountLimitExceeded, errorMessage)
                << TErrorAttribute("usage", usage)
                << TErrorAttribute("limit", limit);
        };

        const auto& dynamicConfig = GetDynamicConfig();

        for (; account; account = account->GetParent()) {
            const auto& usage = account->ClusterStatistics().ResourceUsage;
            const auto& committedUsage = account->ClusterStatistics().CommittedResourceUsage;
            const auto& limits = account->ClusterResourceLimits();

            for (auto [index, deltaSpace] : delta.DiskSpace()) {
                auto usageSpace = usage.DiskSpace().lookup(index);
                auto limitsSpace = limits.DiskSpace().lookup(index);

                if (usageSpace + deltaSpace > limitsSpace) {
                    const auto& chunkManager = Bootstrap_->GetChunkManager();
                    const auto* medium = chunkManager->GetMediumByIndex(index);

                    throwOverdraftError("disk space", account, usageSpace, limitsSpace, medium);
                }
            }
            // Branched nodes are usually "paid for" by the originating node's
            // account, which is wrong, but can't be easily avoided. To mitigate the
            // issue, only committed node count is checked here. All this does is
            // effectively ignores non-trunk nodes, which constitute the majority of
            // problematic nodes.
            if (delta.NodeCount > 0 && committedUsage.NodeCount + delta.NodeCount > limits.NodeCount) {
                throwOverdraftError("Cypress node count", account, committedUsage.NodeCount, limits.NodeCount);
            }
            if (delta.ChunkCount > 0 && usage.ChunkCount + delta.ChunkCount > limits.ChunkCount) {
                throwOverdraftError("chunk count", account, usage.ChunkCount, limits.ChunkCount);
            }
            if (delta.TabletCount > 0 && usage.TabletCount + delta.TabletCount > limits.TabletCount) {
                throwOverdraftError("tablet count", account, usage.TabletCount, limits.TabletCount);
            }
            if (delta.TabletStaticMemory > 0 && usage.TabletStaticMemory + delta.TabletStaticMemory > limits.TabletStaticMemory) {
                throwOverdraftError("tablet static memory", account, usage.TabletStaticMemory, limits.TabletStaticMemory);
            }
            if (dynamicConfig->EnableMasterMemoryUsageValidation && delta.MasterMemory > 0 &&
                usage.MasterMemory + delta.MasterMemory > limits.MasterMemory)
            {
                throwOverdraftError("master memory", account, usage.MasterMemory, limits.MasterMemory);
            }
        }

    }

    void ValidateAttachChildAccount(TAccount* parentAccount, TAccount* childAccount)
    {
        const auto& childUsage = childAccount->ClusterStatistics().ResourceUsage;
        const auto& childLimits = childAccount->ClusterResourceLimits();

        const auto& parentLimits = parentAccount->ClusterResourceLimits();
        if (parentLimits.IsAtLeastOneResourceLessThan(childLimits)) {
            THROW_ERROR_EXCEPTION("Failed to change account %Qv parent to %Qv: "
                "child resource limit cannot be above that of its parent",
                childAccount->GetName(),
                parentAccount->GetName());
        }

        if (!parentAccount->GetAllowChildrenLimitOvercommit() && IsAccountOvercommited(parentAccount, parentLimits - childLimits)) {
            THROW_ERROR_EXCEPTION("Failed to change account %Qv parent to %Qv: "
                "the sum of children limits cannot be above parent limits",
                childAccount->GetName(),
                parentAccount->GetName());
        }

        ValidateResourceUsageIncrease(parentAccount, childUsage, true /*allowRootAccount*/);
    }

    void SetAccountAllowChildrenLimitOvercommit(
        TAccount* account,
        bool overcommitAllowed)
    {
        if (!overcommitAllowed && account->GetAllowChildrenLimitOvercommit() &&
            IsAccountOvercommited(account, account->ClusterResourceLimits()))
        {
            THROW_ERROR_EXCEPTION(
                "Failed to disable children limit overcommit for account %Qv because it is currently overcommitted",
                account->GetName());
        }

        account->SetAllowChildrenLimitOvercommit(overcommitAllowed);
    }

    // This is just a glorified for-each, but it's worth giving it a semantic name.
    template <class T>
    void ChargeAccountAncestry(TAccount* account, T&& chargeIndividualAccount)
    {
        for (; account; account = account->GetParent()) {
            chargeIndividualAccount(account);
        }
    }


    void SetUserBanned(TUser* user, bool banned)
    {
        if (banned && user == RootUser_) {
            THROW_ERROR_EXCEPTION("User %Qv cannot be banned",
                user->GetName());
        }

        if (user->GetBanned() != banned) {
            user->SetBanned(banned);
            if (banned) {
                YT_LOG_INFO_UNLESS(IsRecovery(), "User is banned (User: %v)", user->GetName());
            } else {
                YT_LOG_INFO_UNLESS(IsRecovery(), "User is no longer banned (User: %v)", user->GetName());
            }
        }
    }

    TError CheckUserAccess(TUser* user)
    {
        if (user->GetBanned()) {
            return TError(
                NSecurityClient::EErrorCode::UserBanned,
                "User %Qv is banned",
                user->GetName());
        }

        if (user == GetOwnerUser()) {
            return TError(
                NSecurityClient::EErrorCode::AuthenticationError,
                "Cannot authenticate as %Qv",
                user->GetName());
        }

        return {};
    }


    void ChargeUser(TUser* user, const TUserWorkload& workload)
    {
        if (!IsObjectAlive(user)) {
            return;
        }
        RequestTracker_->ChargeUser(user, workload);
        UserCharged_.Fire(user, workload);
    }

    TFuture<void> ThrottleUser(TUser* user, int requestCount, EUserWorkloadType workloadType)
    {
        return RequestTracker_->ThrottleUserRequest(user, requestCount, workloadType);
    }

    void SetUserRequestRateLimit(TUser* user, int limit, EUserWorkloadType type)
    {
        RequestTracker_->SetUserRequestRateLimit(user, limit, type);
    }

    void SetUserRequestLimits(TUser* user, TUserRequestLimitsConfigPtr config)
    {
        RequestTracker_->SetUserRequestLimits(user, std::move(config));
    }

    void SetUserRequestQueueSizeLimit(TUser* user, int limit)
    {
        RequestTracker_->SetUserRequestQueueSizeLimit(user, limit);
    }


    bool TryIncreaseRequestQueueSize(TUser* user)
    {
        return RequestTracker_->TryIncreaseRequestQueueSize(user);
    }

    void DecreaseRequestQueueSize(TUser* user)
    {
        RequestTracker_->DecreaseRequestQueueSize(user);
    }


    const TSecurityTagsRegistryPtr& GetSecurityTagsRegistry() const
    {
        return SecurityTagsRegistry_;
    }

    DEFINE_SIGNAL(void(TUser*, const TUserWorkload&), UserCharged);

private:
    friend class TAccountTypeHandler;
    friend class TUserTypeHandler;
    friend class TGroupTypeHandler;
    friend class TNetworkProjectTypeHandler;

    const TRequestTrackerPtr RequestTracker_;

    const TSecurityTagsRegistryPtr SecurityTagsRegistry_ = New<TSecurityTagsRegistry>();

    TPeriodicExecutorPtr AccountStatisticsGossipExecutor_;
    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr MembershipClosureRecomputeExecutor_;
    TPeriodicExecutorPtr AccountMasterMemoryUsageUpdateExecutor_;

    NHydra::TEntityMap<TAccount> AccountMap_;
    THashMap<TString, TAccount*> AccountNameMap_;

    TAccountId RootAccountId_;
    TAccount* RootAccount_ = nullptr;

    TAccountId SysAccountId_;
    TAccount* SysAccount_ = nullptr;

    TAccountId TmpAccountId_;
    TAccount* TmpAccount_ = nullptr;

    TAccountId IntermediateAccountId_;
    TAccount* IntermediateAccount_ = nullptr;

    TAccountId ChunkWiseAccountingMigrationAccountId_;
    TAccount* ChunkWiseAccountingMigrationAccount_ = nullptr;

    NHydra::TEntityMap<TUser> UserMap_;
    THashMap<TString, TUser*> UserNameMap_;
    THashMap<TString, TTagId> UserNameToProfilingTagId_;

    TUserId RootUserId_;
    TUser* RootUser_ = nullptr;

    TUserId GuestUserId_;
    TUser* GuestUser_ = nullptr;

    TUserId JobUserId_;
    TUser* JobUser_ = nullptr;

    TUserId SchedulerUserId_;
    TUser* SchedulerUser_ = nullptr;

    TUserId ReplicatorUserId_;
    TUser* ReplicatorUser_ = nullptr;

    TUserId OwnerUserId_;
    TUser* OwnerUser_ = nullptr;

    TUserId FileCacheUserId_;
    TUser* FileCacheUser_ = nullptr;

    TUserId OperationsCleanerUserId_;
    TUser* OperationsCleanerUser_ = nullptr;

    TUserId OperationsClientUserId_;
    TUser* OperationsClientUser_ = nullptr;

    TUserId TabletCellChangeloggerUserId_;
    TUser* TabletCellChangeloggerUser_ = nullptr;

    TUserId TabletCellSnapshotterUserId_;
    TUser* TabletCellSnapshotterUser_ = nullptr;

    TUserId TableMountInformerUserId_;
    TUser* TableMountInformerUser_ = nullptr;

    NHydra::TEntityMap<TGroup> GroupMap_;
    THashMap<TString, TGroup*> GroupNameMap_;

    TGroupId EveryoneGroupId_;
    TGroup* EveryoneGroup_ = nullptr;

    TGroupId UsersGroupId_;
    TGroup* UsersGroup_ = nullptr;

    TGroupId SuperusersGroupId_;
    TGroup* SuperusersGroup_ = nullptr;

    TFls<TUser*> AuthenticatedUser_;

    NHydra::TEntityMap<TNetworkProject> NetworkProjectMap_;
    THashMap<TString, TNetworkProject*> NetworkProjectNameMap_;

    // COMPAT(shakurov)
    bool RecomputeAccountResourceUsage_ = false;
    bool ValidateAccountResourceUsage_ = false;

    bool MustRecomputeMembershipClosure_ = false;

    // COMPAT(kiselyovp)
    bool MustInitializeAccountHierarchy_ = false;

    // COMPAT(aleksandra-zh)
    bool MustInitializeMasterMemoryLimits_ = false;

    bool NeedAdjustRootAccountLimits_ = false;

    static i64 GetDiskSpaceToCharge(i64 diskSpace, NErasure::ECodec erasureCodec, TReplicationPolicy policy)
    {
        auto isErasure = erasureCodec != NErasure::ECodec::None;
        auto replicationFactor = isErasure ? 1 : policy.GetReplicationFactor();
        auto result = diskSpace *  replicationFactor;

        if (policy.GetDataPartsOnly() && isErasure) {
            auto* codec = NErasure::GetCodec(erasureCodec);
            auto dataPartCount = codec->GetDataPartCount();
            auto totalPartCount = codec->GetTotalPartCount();

            // Should only charge for data parts.
            result = result * dataPartCount / totalPartCount;
        }

        return result;
    }

    TClusterResources* GetTransactionAccountUsage(TTransaction* transaction, TAccount* account)
    {
        auto it = transaction->AccountResourceUsage().find(account);
        if (it == transaction->AccountResourceUsage().end()) {
            it = transaction->AccountResourceUsage().emplace(account, TClusterResources()).first;
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->RefObject(account);
        }
        return &it->second;
    }

    template <class T>
    void ComputeChunkResourceDelta(const TChunk* chunk, const TChunkRequisition& requisition, i64 delta, T&& doCharge)
    {
        auto chunkDiskSpace = chunk->ChunkInfo().disk_space();
        auto erasureCodec = chunk->GetErasureCodec();

        const TAccount* lastAccount = nullptr;
        auto lastMediumIndex = InvalidMediumIndex;
        i64 lastDiskSpace = 0;
        auto masterMemoryUsage = delta * chunk->GetMasterMemoryUsage();

        for (const auto& entry : requisition) {
            auto* account = entry.Account;
            if (!IsObjectAlive(account)) {
                continue;
            }

            auto mediumIndex = entry.MediumIndex;
            YT_ASSERT(mediumIndex != NChunkClient::InvalidMediumIndex);

            auto policy = entry.ReplicationPolicy;
            auto diskSpace = delta * GetDiskSpaceToCharge(chunkDiskSpace, erasureCodec, policy);
            auto chunkCount = delta * ((account == lastAccount) ? 0 : 1); // charge once per account

            if (account == lastAccount && mediumIndex == lastMediumIndex) {
                // TChunkRequisition keeps entries sorted, which means an
                // uncommitted entry for account A and medium M, if any,
                // immediately follows a committed entry for A and M (if any).
                YT_VERIFY(!entry.Committed);

                // Avoid overcharging: if, for example, a chunk has 3 'committed' and
                // 5 'uncommitted' replicas (for the same account and medium), the account
                // has already been charged for 3 and should now be charged for 2 only.
                if (delta > 0) {
                    diskSpace = std::max(i64(0), diskSpace - lastDiskSpace);
                } else {
                    diskSpace = std::min(i64(0), diskSpace - lastDiskSpace);
                }
            }

            ChargeAccountAncestry(
                account,
                [&] (TAccount* account) {
                    doCharge(account, mediumIndex, chunkCount, diskSpace, masterMemoryUsage, entry.Committed);
                });

            lastAccount = account;
            lastMediumIndex = mediumIndex;
            lastDiskSpace = diskSpace;
        }
    }


    TAccount* DoCreateAccount(TAccountId id)
    {
        auto accountHolder = std::make_unique<TAccount>(id, id == RootAccountId_);

        auto* account = AccountMap_.Insert(id, std::move(accountHolder));

        InitializeAccountStatistics(account);

        // Make the fake reference.
        YT_VERIFY(account->RefObject() == 1);

        return account;
    }

    TGroup* GetBuiltinGroupForUser(TUser* user)
    {
        // "guest" is a member of "everyone" group.
        // "root", "job", "scheduler", "replicator", "file_cache", "operations_cleaner", "operations_client",
        // "tablet_cell_changelogger", "tablet_cell_snapshotter" and "table_mount_informer" are members of "superusers" group.
        // others are members of "users" group.
        const auto& id = user->GetId();
        if (id == GuestUserId_) {
            return EveryoneGroup_;
        } else if (
            id == RootUserId_ ||
            id == JobUserId_ ||
            id == SchedulerUserId_ ||
            id == ReplicatorUserId_ ||
            id == FileCacheUserId_ ||
            id == OperationsCleanerUserId_ ||
            id == OperationsClientUserId_ ||
            id == TabletCellChangeloggerUserId_ ||
            id == TabletCellSnapshotterUserId_ ||
            id == TableMountInformerUserId_)
        {
            return SuperusersGroup_;
        } else {
            return UsersGroup_;
        }
    }

    TUser* DoCreateUser(TUserId id, const TString& name)
    {
        auto userHolder = std::make_unique<TUser>(id);
        userHolder->SetName(name);

        auto* user = UserMap_.Insert(id, std::move(userHolder));
        YT_VERIFY(UserNameMap_.emplace(user->GetName(), user).second);

        YT_VERIFY(user->RefObject() == 1);
        DoAddMember(GetBuiltinGroupForUser(user), user);
        MaybeRecomputeMembershipClosure();

        if (!IsRecovery()) {
            RequestTracker_->ReconfigureUserRequestRateThrottler(user);
        }

        return user;
    }

    TGroup* DoCreateGroup(TGroupId id, const TString& name)
    {
        auto groupHolder = std::make_unique<TGroup>(id);
        groupHolder->SetName(name);

        auto* group = GroupMap_.Insert(id, std::move(groupHolder));
        YT_VERIFY(GroupNameMap_.emplace(group->GetName(), group).second);

        // Make the fake reference.
        YT_VERIFY(group->RefObject() == 1);

        return group;
    }

    TNetworkProject* DoCreateNetworkProject(TNetworkProjectId id, const TString& name)
    {
        auto networkProjectHolder = std::make_unique<TNetworkProject>(id);
        networkProjectHolder->SetName(name);

        auto* networkProject = NetworkProjectMap_.Insert(id, std::move(networkProjectHolder));
        YT_VERIFY(NetworkProjectNameMap_.emplace(networkProject->GetName(), networkProject).second);

        // Make the fake reference.
        YT_VERIFY(networkProject->RefObject() == 1);

        return networkProject;
    }

    void PropagateRecursiveMemberOf(TSubject* subject, TGroup* ancestorGroup)
    {
        bool added = subject->RecursiveMemberOf().insert(ancestorGroup).second;
        if (added && subject->GetType() == EObjectType::Group) {
            auto* subjectGroup = subject->AsGroup();
            for (auto* member : subjectGroup->Members()) {
                PropagateRecursiveMemberOf(member, ancestorGroup);
            }
        }
    }

    void MaybeRecomputeMembershipClosure()
    {
        const auto& dynamicConfig = GetDynamicConfig();
        if (dynamicConfig->EnableDelayedMembershipClosureRecomputation) {
            if (!MustRecomputeMembershipClosure_) {
                MustRecomputeMembershipClosure_ = true;
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Will recompute membership closure");
            }
        } else {
            DoRecomputeMembershipClosure();
        }
    }

    void DoRecomputeMembershipClosure()
    {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Started recomputing membership closure");

        for (auto [userId, user] : UserMap_) {
            user->RecursiveMemberOf().clear();
        }

        for (auto [groupId, group] : GroupMap_) {
            group->RecursiveMemberOf().clear();
        }

        for (auto [groupId, group] : GroupMap_) {
            for (auto* member : group->Members()) {
                PropagateRecursiveMemberOf(member, group);
            }
        }

        MustRecomputeMembershipClosure_ = false;

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Finished recomputing membership closure");
    }

    void OnRecomputeMembershipClosure()
    {
        NProto::TReqRecomputeMembershipClosure request;
        CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(Logger);
    }


    void DoAddMember(TGroup* group, TSubject* member)
    {
        YT_VERIFY(group->Members().insert(member).second);
        YT_VERIFY(member->MemberOf().insert(group).second);
    }

    void DoRemoveMember(TGroup* group, TSubject* member)
    {
        YT_VERIFY(group->Members().erase(member) == 1);
        YT_VERIFY(member->MemberOf().erase(group) == 1);
    }


    void ValidateMembershipUpdate(TGroup* group, TSubject* member)
    {
        if (group == EveryoneGroup_ || group == UsersGroup_) {
            THROW_ERROR_EXCEPTION("Cannot modify group");
        }

        ValidatePermission(group, EPermission::Write);
    }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        AccountMap_.SaveKeys(context);
        UserMap_.SaveKeys(context);
        GroupMap_.SaveKeys(context);
        NetworkProjectMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        AccountMap_.SaveValues(context);
        UserMap_.SaveValues(context);
        GroupMap_.SaveValues(context);
        NetworkProjectMap_.SaveValues(context);
        Save(context, MustRecomputeMembershipClosure_);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        AccountMap_.LoadKeys(context);
        UserMap_.LoadKeys(context);
        GroupMap_.LoadKeys(context);

        // COMPAT(gritukan)
        if (context.GetVersion() >= EMasterReign::NetworkProject) {
            NetworkProjectMap_.LoadKeys(context);
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        AccountMap_.LoadValues(context);
        UserMap_.LoadValues(context);
        GroupMap_.LoadValues(context);

        // COMPAT(gritukan)
        if (context.GetVersion() >= EMasterReign::NetworkProject) {
            NetworkProjectMap_.LoadValues(context);
        }

        MustRecomputeMembershipClosure_ = Load<bool>(context);

        // COMPAT(savrus) COMPAT(shakurov)
        ValidateAccountResourceUsage_ = true;
        RecomputeAccountResourceUsage_ = false;

        // COMPAT(kiselyovp)
        MustInitializeAccountHierarchy_ = context.GetVersion() < EMasterReign::HierarchicalAccounts;

        // COMPAT(aleksandra-zh)
        MustInitializeMasterMemoryLimits_ = context.GetVersion() < EMasterReign::InitializeAccountMasterMemoryUsage;

        // COMPAT(aleksandra-zh)
        NeedAdjustRootAccountLimits_ = context.GetVersion() < EMasterReign::FixRootAccountLimits;
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        AccountNameMap_.clear();
        for (auto [accountId, account] : AccountMap_) {
            // Initialize statistics for this cell.
            // NB: This also provides the necessary data migration for pre-0.18 versions.
            InitializeAccountStatistics(account);

            // COMPAT(kiselyovp)
            if (MustInitializeAccountHierarchy_) {
                continue;
            }
            if (!IsObjectAlive(account)) {
                continue;
            }
            if (!account->GetParent() && account->GetId() != RootAccountId_) {
                YT_LOG_ALERT("Unattended account found in snapshot (Id: %v)",
                    account->GetId());
            } else {
                // Reconstruct account name map.
                RegisterAccountName(account->GetName(), account);
            }
        }

        UserNameMap_.clear();
        for (auto [userId, user] : UserMap_) {
            if (!IsObjectAlive(user)) {
                continue;
            }

            // Reconstruct user name map.
            YT_VERIFY(UserNameMap_.emplace(user->GetName(), user).second);
        }

        GroupNameMap_.clear();
        for (auto [groupId, group] : GroupMap_) {
            if (!IsObjectAlive(group)) {
                continue;
            }
            // Reconstruct group name map.
            YT_VERIFY(GroupNameMap_.emplace(group->GetName(), group).second);
        }

        NetworkProjectNameMap_.clear();
        for (auto [networkProjectId, networkProject] : NetworkProjectMap_) {
            if (!IsObjectAlive(networkProject)) {
                continue;
            }

            // Reconstruct network project name map.
            YT_VERIFY(NetworkProjectNameMap_.emplace(networkProject->GetName(), networkProject).second);
        }

        InitBuiltins();

        // COMPAT(kiselyovp)
        if (MustInitializeAccountHierarchy_) {
            if (ChunkWiseAccountingMigrationAccount_->ClusterResourceLimits().DiskSpace().lookup(NChunkServer::DefaultStoreMediumIndex) >
                std::numeric_limits<i64>::max() / 4)
            {
                auto newResourceLimits = ChunkWiseAccountingMigrationAccount_->ClusterResourceLimits();
                newResourceLimits.SetMediumDiskSpace(NChunkServer::DefaultStoreMediumIndex, std::numeric_limits<i64>::max() / 4);
                TrySetResourceLimits(ChunkWiseAccountingMigrationAccount_, newResourceLimits);
            }

            for (auto [accountId, account] : AccountMap_) {
                YT_VERIFY(!account->GetParent());
                if (!IsObjectAlive(account) || account == RootAccount_) {
                    continue;
                }

                auto name = account->GetLegacyName();
                account->SetLegacyName("");
                RootAccount_->AttachChild(name, account);
                const auto& objectManager = Bootstrap_->GetObjectManager();
                objectManager->RefObject(RootAccount_);
                RegisterAccountName(name, account);
            }
        }

        // Leads to overcommit in hierarchical accounts!
        if (MustInitializeMasterMemoryLimits_) {
            for (auto [accountId, account] : AccountMap_) {
                if (!IsObjectAlive(account)) {
                    continue;
                }

                auto resourceLimits = account->ClusterResourceLimits();
                resourceLimits.MasterMemory = 100_GB;

                TrySetResourceLimits(account, resourceLimits);
            }
        }

        if (NeedAdjustRootAccountLimits_) {
            RootAccount_->ClusterResourceLimits() = TClusterResources::Infinite();
        }

        RecomputeAccountMasterMemoryUsage();
    }

    // COMPAT(shakurov): currently unused but may become useful
    void RecomputeAccountResourceUsage()
    {
        if (!ValidateAccountResourceUsage_ && !RecomputeAccountResourceUsage_) {
            return;
        }

        struct TStat
        {
            TClusterResources NodeUsage;
            TClusterResources NodeCommittedUsage;
        };

        THashMap<TAccount*, TStat> statMap;

        const auto& cypressManager = Bootstrap_->GetCypressManager();

        // Recompute everything except chunk count and disk space.
        for (auto [nodeId, node] : cypressManager->Nodes()) {
            // NB: zombie nodes are still accounted.
            if (node->IsDestroyed()) {
                continue;
            }

            if (node->IsExternal()) {
                continue;
            }

            auto* account = node->GetAccount();
            auto usage = node->GetDeltaResourceUsage();
            usage.ChunkCount = 0;
            usage.ClearDiskSpace();

            auto& stat = statMap[account];
            stat.NodeUsage += usage;
            if (node->IsTrunk()) {
                stat.NodeCommittedUsage += usage;
            }
        }

        auto chargeStatMap = [&] (TAccount* account, int mediumIndex, i64 chunkCount, i64 diskSpace, i64 /*masterMemoryUsage*/, bool committed) {
            auto& stat = statMap[account];
            stat.NodeUsage.AddToMediumDiskSpace(mediumIndex, diskSpace);
            stat.NodeUsage.ChunkCount += chunkCount;
            if (committed) {
                stat.NodeCommittedUsage.AddToMediumDiskSpace(mediumIndex, diskSpace);
                stat.NodeCommittedUsage.ChunkCount += chunkCount;
            }
        };

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();

        for (auto [chunkId, chunk] : chunkManager->Chunks()) {
            // NB: zombie chunks are still accounted.
            if (chunk->IsDestroyed()) {
                continue;
            }

            if (chunk->IsForeign()) {
                continue;
            }

            if (chunk->IsDiskSizeFinal()) {
                auto requisition = chunk->GetAggregatedRequisition(requisitionRegistry);
                ComputeChunkResourceDelta(chunk, requisition, +1, chargeStatMap);
            }  // Else this'll be done later when the chunk is confirmed/sealed.
        }

        auto resourceUsageMatch = [&] (
            TAccount* account,
            const TClusterResources& accountUsage,
            const TClusterResources& expectedUsage)
        {
            if (accountUsage == expectedUsage) {
                return true;
            }
            if (account != SysAccount_) {
                return false;
            }

            // Root node requires special handling (unless resource usage have previously been recomputed).
            auto accountUsageCopy = accountUsage;
            ++accountUsageCopy.NodeCount;
            return accountUsageCopy == expectedUsage;
        };

        for (auto [accountId, account] : Accounts()) {
            if (!IsObjectAlive(account)) {
                continue;
            }

            // NB: statMap may contain no entry for an account if it has no nodes or chunks.
            const auto& stat = statMap[account];
            auto& actualUsage = account->LocalStatistics().ResourceUsage;
            auto& actualCommittedUsage = account->LocalStatistics().CommittedResourceUsage;
            const auto& expectedUsage = stat.NodeUsage;
            const auto& expectedCommittedUsage = stat.NodeCommittedUsage;
            if (ValidateAccountResourceUsage_) {
                if (!resourceUsageMatch(account, actualUsage, expectedUsage)) {
                    YT_LOG_ERROR("%v account usage mismatch, snapshot usage: %v, recomputed usage: %v",
                        account->GetName(),
                        actualUsage,
                        expectedUsage);
                }
                if (!resourceUsageMatch(account, actualCommittedUsage, expectedCommittedUsage)) {
                    YT_LOG_ERROR("%v account committed usage mismatch, snapshot usage: %v, recomputed usage: %v",
                        account->GetName(),
                        actualCommittedUsage,
                        expectedCommittedUsage);
                }
            }
            if (RecomputeAccountResourceUsage_) {;
                actualUsage = expectedUsage;
                actualCommittedUsage = expectedCommittedUsage;

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (multicellManager->IsPrimaryMaster()) {
                    account->RecomputeClusterStatistics();
                }
            }
        }
    }

    void RecomputeAccountMasterMemoryUsage()
    {
        YT_LOG_INFO("Started recomputing account master memory usage");

        for (auto [id, account] : AccountMap_) {
            if (!IsObjectAlive(account)) {
                continue;
            }
            account->SetMasterMemoryUsage(0);
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (auto [nodeId, node] : cypressManager->Nodes()) {
            if (node->IsDestroyed()) {
                continue;
            }

            if (IsTableType(node->GetType()) && node->IsTrunk()) {
                auto* table = node->As<TTableNode>();
                table->RecomputeTabletMasterMemoryUsage();
            }
            UpdateMasterMemoryUsage(node);
        }

        auto chargeAccount = [&] (TAccount* account, int /*mediumIndex*/, i64 /*chunkCount*/, i64 /*diskSpace*/, i64 masterMemoryUsage, bool /*committed*/) {
            account->SetMasterMemoryUsage(account->GetMasterMemoryUsage() + masterMemoryUsage);
        };

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();

        for (auto [chunkId, chunk] : chunkManager->Chunks()) {
            // NB: zombie chunks are still accounted.
            if (chunk->IsDestroyed()) {
                continue;
            }

            if (chunk->IsForeign()) {
                continue;
            }

            if (!chunk->IsDiskSizeFinal()) {
                continue;
            }

            auto requisition = chunk->GetAggregatedRequisition(requisitionRegistry);
            ComputeChunkResourceDelta(chunk, requisition, +1, chargeAccount);
        }

        YT_LOG_INFO("Finished recomputing account master memory usage");
    }

    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        AccountMap_.Clear();
        AccountNameMap_.clear();

        UserMap_.Clear();
        UserNameMap_.clear();

        GroupMap_.Clear();
        GroupNameMap_.clear();

        NetworkProjectMap_.Clear();
        NetworkProjectNameMap_.clear();

        RootUser_ = nullptr;
        GuestUser_ = nullptr;
        JobUser_ = nullptr;
        SchedulerUser_ = nullptr;
        OperationsCleanerUser_ = nullptr;
        OperationsClientUser_ = nullptr;
        TabletCellChangeloggerUser_ = nullptr;
        TabletCellSnapshotterUser_ = nullptr;
        TableMountInformerUser_ = nullptr;
        ReplicatorUser_ = nullptr;
        OwnerUser_ = nullptr;
        FileCacheUser_ = nullptr;
        EveryoneGroup_ = nullptr;
        UsersGroup_ = nullptr;
        SuperusersGroup_ = nullptr;

        RootAccount_ = nullptr;
        SysAccount_ = nullptr;
        TmpAccount_ = nullptr;
        IntermediateAccount_ = nullptr;
        ChunkWiseAccountingMigrationAccount_ = nullptr;

        MustRecomputeMembershipClosure_ = false;

        ResetAuthenticatedUser();
    }

    virtual void SetZeroState() override
    {
        TMasterAutomatonPart::SetZeroState();

        InitBuiltins();
        InitDefaultSchemaAcds();
    }

    void InitDefaultSchemaAcds()
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto type : objectManager->GetRegisteredTypes()) {
            if (HasSchema(type)) {
                auto* schema = objectManager->GetSchema(type);
                auto* acd = GetAcd(schema);
                // TODO(renadeen): giving read, write, remove for object schema to all users looks like a bad idea.
                if (!IsVersionedType(type)) {
                    acd->AddEntry(TAccessControlEntry(
                        ESecurityAction::Allow,
                        GetUsersGroup(),
                        EPermission::Remove));
                    acd->AddEntry(TAccessControlEntry(
                        ESecurityAction::Allow,
                        GetUsersGroup(),
                        EPermission::Write));
                    acd->AddEntry(TAccessControlEntry(
                        ESecurityAction::Allow,
                        GetEveryoneGroup(),
                        EPermission::Read));
                }
                if (IsUserType(type)) {
                    acd->AddEntry(TAccessControlEntry(
                        ESecurityAction::Allow,
                        GetUsersGroup(),
                        EPermission::Create));
                }
            }
        }
    }

    template <class T>
    T* GetBuiltin(T*& builtin)
    {
        if (!builtin) {
            InitBuiltins();
        }
        YT_VERIFY(builtin);
        return builtin;
    }

    void InitBuiltins()
    {
        // Groups

        // users
        EnsureBuiltinGroupInitialized(UsersGroup_, UsersGroupId_, UsersGroupName);

        // everyone
        if (EnsureBuiltinGroupInitialized(EveryoneGroup_, EveryoneGroupId_, EveryoneGroupName)) {
            DoAddMember(EveryoneGroup_, UsersGroup_);
        }

        // superusers
        if (EnsureBuiltinGroupInitialized(SuperusersGroup_, SuperusersGroupId_, SuperusersGroupName)) {
            DoAddMember(UsersGroup_, SuperusersGroup_);
        }

        DoRecomputeMembershipClosure();

        // Users

        // root
        if (EnsureBuiltinUserInitialized(RootUser_, RootUserId_, RootUserName)) {
            RootUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            RootUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            RootUser_->SetRequestQueueSizeLimit(1000000);
        }

        // guest
        EnsureBuiltinUserInitialized(GuestUser_, GuestUserId_, GuestUserName);

        if (EnsureBuiltinUserInitialized(JobUser_, JobUserId_, JobUserName)) {
            // job
            JobUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            JobUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            JobUser_->SetRequestQueueSizeLimit(1000000);
        }

        // scheduler
        if (EnsureBuiltinUserInitialized(SchedulerUser_, SchedulerUserId_, SchedulerUserName)) {
            SchedulerUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            SchedulerUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            SchedulerUser_->SetRequestQueueSizeLimit(1000000);
        }

        // replicator
        if (EnsureBuiltinUserInitialized(ReplicatorUser_, ReplicatorUserId_, ReplicatorUserName)) {
            ReplicatorUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            ReplicatorUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            ReplicatorUser_->SetRequestQueueSizeLimit(1000000);
        }

        // owner
        EnsureBuiltinUserInitialized(OwnerUser_, OwnerUserId_, OwnerUserName);

        // file cache
        if (EnsureBuiltinUserInitialized(FileCacheUser_, FileCacheUserId_, FileCacheUserName)) {
            FileCacheUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            FileCacheUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            FileCacheUser_->SetRequestQueueSizeLimit(1000000);
        }

        // operations cleaner
        if (EnsureBuiltinUserInitialized(OperationsCleanerUser_, OperationsCleanerUserId_, OperationsCleanerUserName)) {
            OperationsCleanerUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            OperationsCleanerUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            OperationsCleanerUser_->SetRequestQueueSizeLimit(1000000);
        }

        // operations client
        if (EnsureBuiltinUserInitialized(OperationsClientUser_, OperationsClientUserId_, OperationsClientUserName)) {
            OperationsClientUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            OperationsClientUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            OperationsClientUser_->SetRequestQueueSizeLimit(1000000);
        }

        // tablet cell changelogger
        if (EnsureBuiltinUserInitialized(TabletCellChangeloggerUser_, TabletCellChangeloggerUserId_, TabletCellChangeloggerUserName)) {
            TabletCellChangeloggerUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            TabletCellChangeloggerUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            TabletCellChangeloggerUser_->SetRequestQueueSizeLimit(1000000);
        }

        // tablet cell snapshotter
        if (EnsureBuiltinUserInitialized(TabletCellSnapshotterUser_, TabletCellSnapshotterUserId_, TabletCellSnapshotterUserName)) {
            TabletCellSnapshotterUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            TabletCellSnapshotterUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            TabletCellSnapshotterUser_->SetRequestQueueSizeLimit(1000000);
        }

        // table mount informer
        if (EnsureBuiltinUserInitialized(TableMountInformerUser_, TableMountInformerUserId_, TableMountInformerUserName)) {
            TableMountInformerUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Read);
            TableMountInformerUser_->SetRequestRateLimit(1000000, EUserWorkloadType::Write);
            TableMountInformerUser_->SetRequestQueueSizeLimit(1000000);
        }

        // Accounts
        // root, infinite resources, not meant to be used
        if (EnsureBuiltinAccountInitialized(RootAccount_, RootAccountId_, RootAccountName)) {
            RootAccount_->ClusterResourceLimits() = TClusterResources::Infinite();
        }
        RootAccount_->SetAllowChildrenLimitOvercommit(true);

        auto defaultResources = TClusterResources()
            .SetNodeCount(100000)
            .SetChunkCount(1000000000)
            .SetMediumDiskSpace(NChunkServer::DefaultStoreMediumIndex, 1_TB)
            .SetMasterMemory(100_GB);

        // sys, 1 TB disk space, 100 000 nodes, 1 000 000 000 chunks, 100 000 tablets, 10TB tablet static memory, 100_GB master memory allowed for: root
        if (EnsureBuiltinAccountInitialized(SysAccount_, SysAccountId_, SysAccountName)) {
            auto resourceLimits = defaultResources;
            resourceLimits.TabletCount = 100000;
            resourceLimits.TabletStaticMemory = 10_TB;
            TrySetResourceLimits(SysAccount_, resourceLimits);

            SysAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                RootUser_,
                EPermission::Use));
        }

        // tmp, 1 TB disk space, 100 000 nodes, 1 000 000 000 chunks, 100_GB master memory, allowed for: users
        if (EnsureBuiltinAccountInitialized(TmpAccount_, TmpAccountId_, TmpAccountName)) {
            TrySetResourceLimits(TmpAccount_, defaultResources);
            TmpAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                UsersGroup_,
                EPermission::Use));
        }

        // intermediate, 1 TB disk space, 100 000 nodes, 1 000 000 000 chunks, 100_GB master memory allowed for: users
        if (EnsureBuiltinAccountInitialized(IntermediateAccount_, IntermediateAccountId_, IntermediateAccountName)) {
            TrySetResourceLimits(IntermediateAccount_, defaultResources);
            IntermediateAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                UsersGroup_,
                EPermission::Use));
        }

        // chunk_wise_accounting_migration, maximum disk space, maximum nodes, maximum chunks, 100_GB master memory allowed for: root
        if (EnsureBuiltinAccountInitialized(ChunkWiseAccountingMigrationAccount_, ChunkWiseAccountingMigrationAccountId_, ChunkWiseAccountingMigrationAccountName)) {
            auto resourceLimits = TClusterResources()
                .SetNodeCount(std::numeric_limits<int>::max())
                .SetChunkCount(std::numeric_limits<int>::max())
                .SetMasterMemory(100_GB)
                .SetMediumDiskSpace(NChunkServer::DefaultStoreMediumIndex, std::numeric_limits<i64>::max() / 4);
            TrySetResourceLimits(ChunkWiseAccountingMigrationAccount_, resourceLimits);
            ChunkWiseAccountingMigrationAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                RootUser_,
                EPermission::Use));
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
        requisitionRegistry->EnsureBuiltinRequisitionsInitialized(
            GetChunkWiseAccountingMigrationAccount(),
            Bootstrap_->GetObjectManager());
    }

    bool EnsureBuiltinGroupInitialized(TGroup*& group, TGroupId id, const TString& name)
    {
        if (group) {
            return false;
        }
        group = FindGroup(id);
        if (group) {
            return false;
        }
        group = DoCreateGroup(id, name);
        return true;
    }

    bool EnsureBuiltinUserInitialized(TUser*& user, TUserId id, const TString& name)
    {
        if (user) {
            return false;
        }
        user = FindUser(id);
        if (user) {
            return false;
        }
        user = DoCreateUser(id, name);
        return true;
    }

    bool EnsureBuiltinAccountInitialized(TAccount*& account, TAccountId id, const TString& name)
    {
        if (account) {
            return false;
        }
        account = FindAccount(id);
        if (account) {
            return false;
        }

        account = DoCreateAccount(id);

        if (id != RootAccountId_) {
            YT_VERIFY(RootAccount_);
            RootAccount_->AttachChild(name, account);
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->RefObject(RootAccount_);
        }
        RegisterAccountName(name, account);

        return true;
    }


    virtual void OnRecoveryComplete() override
    {
        TMasterAutomatonPart::OnRecoveryComplete();

        RequestTracker_->Start();
    }

    virtual void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        AccountStatisticsGossipExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::SecurityGossip),
            BIND(&TImpl::OnAccountStatisticsGossip, MakeWeak(this)));
        AccountStatisticsGossipExecutor_->Start();

        MembershipClosureRecomputeExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::OnRecomputeMembershipClosure, MakeWeak(this)));
        MembershipClosureRecomputeExecutor_->Start();

        AccountMasterMemoryUsageUpdateExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::SecurityManager),
            BIND(&TImpl::CommitAccountMasterMemoryUsage, MakeWeak(this)));
        AccountMasterMemoryUsageUpdateExecutor_->Start();

        OnDynamicConfigChanged();
    }

    virtual void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        RequestTracker_->Stop();

        if (AccountStatisticsGossipExecutor_) {
            AccountStatisticsGossipExecutor_->Stop();
            AccountStatisticsGossipExecutor_.Reset();
        }

        if (MembershipClosureRecomputeExecutor_) {
            MembershipClosureRecomputeExecutor_->Stop();
            MembershipClosureRecomputeExecutor_.Reset();
        }

        if (AccountMasterMemoryUsageUpdateExecutor_) {
            AccountMasterMemoryUsageUpdateExecutor_->Stop();
            AccountMasterMemoryUsageUpdateExecutor_.Reset();
        }
    }

    virtual void OnStopFollowing() override
    {
        TMasterAutomatonPart::OnStopFollowing();

        RequestTracker_->Stop();
    }


    void CommitAccountMasterMemoryUsage()
    {
        NProto::TReqUpdateAccountMasterMemoryUsage request;
        for (auto [id, account] : AccountMap_) {
            if (!IsObjectAlive(account)) {
                continue;
            }

            const auto& resources = account->LocalStatistics().ResourceUsage;

            auto newMemoryUsage = account->GetMasterMemoryUsage();
            if (newMemoryUsage == resources.MasterMemory) {
                continue;
            }

            auto* entry = request.add_entries();
            ToProto(entry->mutable_account_id(), account->GetId());
            entry->set_master_memory_usage(newMemoryUsage);
        }
        CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(Logger);
    }


    void InitializeAccountStatistics(TAccount* account)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellTag = multicellManager->GetCellTag();
        const auto& secondaryCellTags = multicellManager->GetSecondaryCellTags();

        auto& multicellStatistics = account->MulticellStatistics();
        if (multicellStatistics.find(cellTag) == multicellStatistics.end()) {
            multicellStatistics[cellTag] = account->ClusterStatistics();
        }

        for (auto secondaryCellTag : secondaryCellTags) {
            multicellStatistics[secondaryCellTag];
        }

        account->SetLocalStatisticsPtr(&multicellStatistics[cellTag]);
        account->SetMasterMemoryUsage(multicellStatistics[cellTag].ResourceUsage.MasterMemory);
    }

    void OnAccountStatisticsGossip()
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

        YT_LOG_INFO("Sending account statistics gossip message");

        NProto::TReqSetAccountStatistics request;
        request.set_cell_tag(multicellManager->GetCellTag());
        for (auto [accountId, account] : AccountMap_) {
            if (!IsObjectAlive(account)) {
                continue;
            }

            auto* entry = request.add_entries();
            ToProto(entry->mutable_account_id(), account->GetId());
            ToProto(
                entry->mutable_statistics(),
                multicellManager->IsPrimaryMaster() ? account->ClusterStatistics() : account->LocalStatistics());
        }

        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToMaster(request, PrimaryMasterCellTag, false);
        }
    }

    void HydraSetAccountStatistics(NProto::TReqSetAccountStatistics* request)
    {
        auto cellTag = request->cell_tag();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster() || cellTag == multicellManager->GetPrimaryCellTag());

        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            YT_LOG_ERROR_UNLESS(IsRecovery(), "Received account statistics gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        YT_LOG_INFO_UNLESS(IsRecovery(), "Received account statistics gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto accountId = FromProto<TAccountId>(entry.account_id());
            auto* account = FindAccount(accountId);
            if (!IsObjectAlive(account)) {
                continue;
            }

            auto newStatistics = FromProto<TAccountStatistics>(entry.statistics());
            if (multicellManager->IsPrimaryMaster()) {
                *account->GetCellStatistics(cellTag) = newStatistics;
                account->RecomputeClusterStatistics();
            } else {
                account->ClusterStatistics() = newStatistics;
            }
        }
    }

    void HydraRecomputeMembershipClosure(NProto::TReqRecomputeMembershipClosure* /*request*/)
    {
        if (MustRecomputeMembershipClosure_) {
            DoRecomputeMembershipClosure();
        }
    }

    void HydraUpdateAccountMasterMemoryUsage(NProto::TReqUpdateAccountMasterMemoryUsage* request)
    {
        for (const auto& entry : request->entries()) {
            auto accountId = FromProto<TAccountId>(entry.account_id());
            auto* account = FindAccount(accountId);

            if (!IsObjectAlive(account)) {
                continue;
            }

            auto masterMemoryUsage = entry.master_memory_usage();

            account->ClusterStatistics().ResourceUsage.MasterMemory = masterMemoryUsage;
            account->LocalStatistics().ResourceUsage.MasterMemory = masterMemoryUsage;

            account->ClusterStatistics().CommittedResourceUsage.MasterMemory = masterMemoryUsage;
            account->LocalStatistics().CommittedResourceUsage.MasterMemory = masterMemoryUsage;
        }
    }

    void OnReplicateKeysToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto accounts = GetValuesSortedByKey(AccountMap_);
        for (auto* account : accounts) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(account, cellTag);
        }

        auto users = GetValuesSortedByKey(UserMap_);
        for (auto* user : users) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(user, cellTag);
        }

        auto groups = GetValuesSortedByKey(GroupMap_);
        for (auto* group : groups) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(group, cellTag);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto accounts = GetValuesSortedByKey(AccountMap_);
        for (auto* account : accounts) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(account, cellTag);
        }

        auto users = GetValuesSortedByKey(UserMap_);
        for (auto* user : users) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(user, cellTag);
        }

        auto groups = GetValuesSortedByKey(GroupMap_);
        for (auto* group : groups) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(group, cellTag);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto replicateMembership = [&] (TSubject* subject) {
            for (auto* group : subject->MemberOf()) {
                auto req = TGroupYPathProxy::AddMember(FromObjectId(group->GetId()));
                req->set_name(subject->GetName());
                req->set_ignore_existing(true);
                multicellManager->PostToMaster(req, cellTag);
            }
        };

        for (auto* user : users) {
            replicateMembership(user);
        }

        for (auto* group : groups) {
            replicateMembership(group);
        }
    }

    static void ValidateSubjectName(const TString& name)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Subject name cannot be empty");
        }
    }

    class TPermissionChecker
    {
    public:
        TPermissionChecker(
            TImpl* impl,
            TUser* user,
            EPermission permission,
            const TPermissionCheckOptions& options)
            : Impl_(impl)
            , User_(user)
            , Permission_(permission)
            , Options_(options)
        {
            auto fastAction = FastCheckPermission();
            if (fastAction != ESecurityAction::Undefined) {
                Response_ = MakeFastCheckPermissionResponse(fastAction, options);
                Proceed_ = false;
                return;
            }

            Response_.Action = ESecurityAction::Undefined;
            if (Options_.Columns) {
                for (const auto& column : *Options_.Columns) {
                    // NB: Multiple occurrences are possible.
                    Columns_.insert(column);
                }
            }
            Proceed_ = true;
        }

        bool ShouldProceed() const
        {
            return Proceed_;
        }

        void ProcessAce(
            const TAccessControlEntry& ace,
            TSubject* owner,
            TObject* object,
            int depth)
        {
            if (!Proceed_) {
                return;
            }

            if (ace.Columns) {
                for (const auto& column : *ace.Columns) {
                    auto it = Columns_.find(column);
                    if (it == Columns_.end()) {
                        continue;
                    }
                    // NB: Multiple occurrences are possible.
                    ColumnToResult_.emplace(*it, TPermissionCheckResult());
                }
            }

            if (!CheckInheritanceMode(ace.InheritanceMode, depth)) {
                return;
            }

            if (!CheckPermissionMatch(ace.Permissions, Permission_)) {
                return;
            }

            for (auto* subject : ace.Subjects) {
                auto* adjustedSubject = subject == Impl_->GetOwnerUser() && owner
                    ? owner
                    : subject;
                if (!adjustedSubject) {
                    continue;
                }

                if (!CheckSubjectMatch(adjustedSubject, User_)) {
                    continue;
                }

                if (ace.Columns) {
                    for (const auto& column : *ace.Columns) {
                        auto it = ColumnToResult_.find(column);
                        if (it == ColumnToResult_.end()) {
                            continue;
                        }
                        ProcessMatchingAce(
                            &it->second,
                            ace,
                            adjustedSubject,
                            object);
                    }
                } else {
                    ProcessMatchingAce(
                        &Response_,
                        ace,
                        adjustedSubject,
                        object);
                    if (Response_.Action == ESecurityAction::Deny) {
                        SetDeny(adjustedSubject, object);
                        break;
                    }
                }

                if (!Proceed_) {
                    break;
                }
            }
        }

        TPermissionCheckResponse GetResponse()
        {
            if (Response_.Action == ESecurityAction::Undefined) {
                SetDeny(nullptr, nullptr);
            }

            if (Response_.Action == ESecurityAction::Allow && Options_.Columns) {
                Response_.Columns = std::vector<TPermissionCheckResult>(Options_.Columns->size());
                for (size_t index = 0; index < Options_.Columns->size(); ++index) {
                    const auto& column = (*Options_.Columns)[index];
                    auto& result = (*Response_.Columns)[index];
                    auto it = ColumnToResult_.find(column);
                    if (it == ColumnToResult_.end()) {
                        result = static_cast<const TPermissionCheckResult>(Response_);
                    } else {
                        result = it->second;
                        if (result.Action == ESecurityAction::Undefined) {
                            result.Action = ESecurityAction::Deny;
                        }
                    }
                }
            }

            return std::move(Response_);
        }

    private:
        TImpl* const Impl_;
        TUser* const User_;
        const EPermission Permission_;
        const TPermissionCheckOptions& Options_;

        THashSet<TStringBuf> Columns_;
        THashMap<TStringBuf, TPermissionCheckResult> ColumnToResult_;

        bool Proceed_;
        TPermissionCheckResponse Response_;

        ESecurityAction FastCheckPermission()
        {
            // "replicator", though being superuser, can only read in safe mode.
            if (User_ == Impl_->ReplicatorUser_ &&
                Permission_ != EPermission::Read &&
                Impl_->IsSafeMode())
            {
                return ESecurityAction::Deny;
            }

            // "root" and "superusers" need no authorization.
            if (IsUserRootOrSuperuser(User_)) {
                return ESecurityAction::Allow;
            }

            // Banned users are denied any permission.
            if (User_->GetBanned()) {
                return ESecurityAction::Deny;
            }

            // Non-reads are forbidden in safe mode.
            if (Permission_ != EPermission::Read &&
                Impl_->Bootstrap_->GetConfigManager()->GetConfig()->EnableSafeMode)
            {
                return ESecurityAction::Deny;
            }

            return ESecurityAction::Undefined;
        }

        bool IsUserRootOrSuperuser(const TUser* user)
        {
            // NB: This is also useful for migration when "superusers" is initially created.
            if (user == Impl_->RootUser_) {
                return true;
            }

            if (user->RecursiveMemberOf().find(Impl_->SuperusersGroup_) != user->RecursiveMemberOf().end()) {
                return true;
            }

            return false;
        }

        static bool CheckSubjectMatch(TSubject* subject, TUser* user)
        {
            switch (subject->GetType()) {
                case EObjectType::User:
                    return subject == user;

                case EObjectType::Group: {
                    auto* subjectGroup = subject->AsGroup();
                    return user->RecursiveMemberOf().find(subjectGroup) != user->RecursiveMemberOf().end();
                }

                default:
                    YT_ABORT();
            }
        }

        static bool CheckInheritanceMode(EAceInheritanceMode mode, int depth)
        {
            return GetInheritedInheritanceMode(mode, depth).has_value();
        }

        static bool CheckPermissionMatch(EPermissionSet permissions, EPermission requestedPermission)
        {
            return (permissions & requestedPermission) != NonePermissions;
        }

        static TPermissionCheckResponse MakeFastCheckPermissionResponse(ESecurityAction action, const TPermissionCheckOptions& options)
        {
            TPermissionCheckResponse response;
            response.Action = action;
            if (options.Columns) {
                response.Columns = std::vector<TPermissionCheckResult>(options.Columns->size());
                for (size_t index = 0; index < options.Columns->size(); ++index) {
                    (*response.Columns)[index].Action = action;
                }
            }
            return response;
        }

        void ProcessMatchingAce(
            TPermissionCheckResult* result,
            const TAccessControlEntry& ace,
            TSubject* subject,
            TObject* object)
        {
            if (result->Action == ESecurityAction::Deny) {
                return;
            }

            result->Action = ace.Action;
            result->Object = object;
            result->Subject = subject;
        }

        static void SetDeny(TPermissionCheckResult* result, TSubject* subject, TObject* object)
        {
            result->Action = ESecurityAction::Deny;
            result->Subject = subject;
            result->Object = object;
        }

        void SetDeny(TSubject* subject, TObject* object)
        {
            SetDeny(&Response_, subject, object);
            if (Response_.Columns) {
                for (auto& result : *Response_.Columns) {
                    SetDeny(&result, subject, object);
                }
            }
            Proceed_ = false;
        }
    };

    static std::optional<EAceInheritanceMode> GetInheritedInheritanceMode(EAceInheritanceMode mode, int depth)
    {
        auto nothing = std::optional<EAceInheritanceMode>();
        switch (mode) {
            case EAceInheritanceMode::ObjectAndDescendants:
                return EAceInheritanceMode::ObjectAndDescendants;
            case EAceInheritanceMode::ObjectOnly:
                return (depth == 0 ? EAceInheritanceMode::ObjectOnly : nothing);
            case EAceInheritanceMode::DescendantsOnly:
                return (depth > 0 ? EAceInheritanceMode::ObjectAndDescendants : nothing);
            case EAceInheritanceMode::ImmediateDescendantsOnly:
                return (depth == 1 ? EAceInheritanceMode::ObjectOnly : nothing);
            default:
                YT_ABORT();
        }
    }

    TString GetPermissionCheckTargetName(const TPermissionCheckTarget& target)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto name = objectManager->GetHandler(target.Object)->GetName(target.Object);
        if (target.Column) {
            return Format("column %Qv of %v",
                *target.Column,
                name);
        } else {
            return name;
        }
    }


    const TDynamicSecurityManagerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->SecurityManager;
    }

    void OnDynamicConfigChanged()
    {
        if (AccountStatisticsGossipExecutor_) {
            AccountStatisticsGossipExecutor_->SetPeriod(GetDynamicConfig()->AccountStatisticsGossipPeriod);
        }

        if (MembershipClosureRecomputeExecutor_) {
            MembershipClosureRecomputeExecutor_->SetPeriod(GetDynamicConfig()->MembershipClosureRecomputePeriod);
        }

        if (AccountMasterMemoryUsageUpdateExecutor_) {
            AccountMasterMemoryUsageUpdateExecutor_->SetPeriod(GetDynamicConfig()->AccountMasterMemoryUsageUpdatePeriod);
        }
    }


    TTagId GetProfilingTagForUser(TUser* user)
    {
        if (auto it = UserNameToProfilingTagId_.find(user->GetName())) {
            return it->second;
        }

        auto tagId = TProfileManager::Get()->RegisterTag("user", user->GetName());
        YT_VERIFY(UserNameToProfilingTagId_.emplace(user->GetName(), tagId).second);
        return tagId;
    }

    void OnProfiling()
    {
        for (auto [userId, user] : UserMap_) {
            if (!IsObjectAlive(user)) {
                continue;
            }
            if (!user->GetNeedsProfiling()) {
                continue;
            }

            TTagIdList tagIds{
                GetProfilingTagForUser(user)
            };

            Profiler.Enqueue("/user_read_time", NProfiling::DurationToValue(user->Statistics()[EUserWorkloadType::Read].RequestTime), EMetricType::Counter, tagIds);
            Profiler.Enqueue("/user_write_time", NProfiling::DurationToValue(user->Statistics()[EUserWorkloadType::Write].RequestTime), EMetricType::Counter, tagIds);
            Profiler.Enqueue("/user_read_request_count", user->Statistics()[EUserWorkloadType::Read].RequestCount, EMetricType::Counter, tagIds);
            Profiler.Enqueue("/user_write_request_count", user->Statistics()[EUserWorkloadType::Write].RequestCount, EMetricType::Counter, tagIds);
            Profiler.Enqueue("/user_request_count", user->Statistics()[EUserWorkloadType::Read].RequestCount + user->Statistics()[EUserWorkloadType::Write].RequestCount, EMetricType::Counter, tagIds);
            Profiler.Enqueue("/user_request_queue_size", user->GetRequestQueueSize(), EMetricType::Gauge, tagIds);
            user->SetNeedsProfiling(false);
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, Account, TAccount, AccountMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, User, TUser, UserMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, Group, TGroup, GroupMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, NetworkProject, TNetworkProject, NetworkProjectMap_)

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TAccountTypeHandler::TAccountTypeHandler(TImpl* owner)
    : TNonversionedMapObjectTypeHandlerBase<TAccount>(owner->Bootstrap_, &owner->AccountMap_)
    , Owner_(owner)
{ }

TObject* TSecurityManager::TAccountTypeHandler::CreateObject(
    TObjectId hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<TString>("name");
    auto parentName = attributes->GetAndRemove<TString>("parent_name", NSecurityClient::RootAccountName);
    auto* parent = Owner_->GetAccountByNameOrThrow(parentName);
    attributes->Set("hint_id", hintId);

    auto* account = CreateObjectImpl(name, parent, attributes);
    return account;
}

std::unique_ptr<TObject> TSecurityManager::TAccountTypeHandler::InstantiateObject(TObjectId id)
{
    return std::make_unique<TAccount>(id, id == Owner_->RootAccountId_);
}

TObject* TSecurityManager::TAccountTypeHandler::FindObjectByAttributes(
    const NYTree::IAttributeDictionary* attributes)
{
    auto name = attributes->Get<TString>("name");
    auto parentName = attributes->Get<TString>("parent_name", RootAccountName);
    auto* parent = Owner_->GetAccountByNameOrThrow(parentName);

    return parent->FindChild(name);
}

TIntrusivePtr<TNonversionedMapObjectProxyBase<TAccount>> TSecurityManager::TAccountTypeHandler::GetMapObjectProxy(
    TAccount* account)
{
    return CreateAccountProxy(Owner_->Bootstrap_, &Metadata_, account);
}

void TSecurityManager::TAccountTypeHandler::DoZombifyObject(TAccount* account)
{
    TNonversionedMapObjectTypeHandlerBase<TAccount>::DoZombifyObject(account);
    Owner_->DestroyAccount(account);
}

void TSecurityManager::TAccountTypeHandler::RegisterName(const TString& name, TAccount* account) noexcept
{
    Owner_->RegisterAccountName(name, account);
}

void TSecurityManager::TAccountTypeHandler::UnregisterName(const TString& name, TAccount* /* account */) noexcept
{
    Owner_->UnregisterAccountName(name);
}

TString TSecurityManager::TAccountTypeHandler::GetRootPath(const TAccount* rootAccount) const
{
    YT_VERIFY(rootAccount == Owner_->GetRootAccount());
    return RootAccountCypressPath;
}

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TUserTypeHandler::TUserTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->UserMap_)
    , Owner_(owner)
{ }

TObject* TSecurityManager::TUserTypeHandler::CreateObject(
    TObjectId hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<TString>("name");

    return Owner_->CreateUser(name, hintId);
}

IObjectProxyPtr TSecurityManager::TUserTypeHandler::DoGetProxy(
    TUser* user,
    TTransaction* /*transaction*/)
{
    return CreateUserProxy(Owner_->Bootstrap_, &Metadata_, user);
}

void TSecurityManager::TUserTypeHandler::DoZombifyObject(TUser* user)
{
    TObjectTypeHandlerWithMapBase::DoZombifyObject(user);
    Owner_->DestroyUser(user);
}

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TGroupTypeHandler::TGroupTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->GroupMap_)
    , Owner_(owner)
{ }

TObject* TSecurityManager::TGroupTypeHandler::CreateObject(
    TObjectId hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<TString>("name");

    return Owner_->CreateGroup(name, hintId);
}

IObjectProxyPtr TSecurityManager::TGroupTypeHandler::DoGetProxy(
    TGroup* group,
    TTransaction* /*transaction*/)
{
    return CreateGroupProxy(Owner_->Bootstrap_, &Metadata_, group);
}

void TSecurityManager::TGroupTypeHandler::DoZombifyObject(TGroup* group)
{
    TObjectTypeHandlerWithMapBase::DoZombifyObject(group);
    Owner_->DestroyGroup(group);
}

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TNetworkProjectTypeHandler::TNetworkProjectTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->NetworkProjectMap_)
    , Owner_(owner)
{ }

TObject* TSecurityManager::TNetworkProjectTypeHandler::CreateObject(
    TObjectId hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<TString>("name");

    return Owner_->CreateNetworkProject(name, hintId);
}

IObjectProxyPtr TSecurityManager::TNetworkProjectTypeHandler::DoGetProxy(
    TNetworkProject* networkProject,
    TTransaction* /*transaction*/)
{
    return CreateNetworkProjectProxy(Owner_->Bootstrap_, &Metadata_, networkProject);
}

void TSecurityManager::TNetworkProjectTypeHandler::DoZombifyObject(TNetworkProject* networkProject)
{
    TObjectTypeHandlerWithMapBase::DoZombifyObject(networkProject);
    Owner_->DestroyNetworkProject(networkProject);
}

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TSecurityManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TSecurityManager::~TSecurityManager() = default;

void TSecurityManager::Initialize()
{
    return Impl_->Initialize();
}

TAccount* TSecurityManager::CreateAccount(TObjectId hintId)
{
    return Impl_->CreateAccount(hintId);
}

TAccount* TSecurityManager::GetAccountOrThrow(TAccountId id)
{
    return Impl_->GetAccountOrThrow(id);
}

TAccount* TSecurityManager::FindAccountByName(const TString& name)
{
    return Impl_->FindAccountByName(name);
}

TAccount* TSecurityManager::GetAccountByNameOrThrow(const TString& name)
{
    return Impl_->GetAccountByNameOrThrow(name);
}

TAccount* TSecurityManager::GetRootAccount()
{
    return Impl_->GetRootAccount();
}

TAccount* TSecurityManager::GetSysAccount()
{
    return Impl_->GetSysAccount();
}

TAccount* TSecurityManager::GetTmpAccount()
{
    return Impl_->GetTmpAccount();
}

TAccount* TSecurityManager::GetIntermediateAccount()
{
    return Impl_->GetIntermediateAccount();
}

TAccount* TSecurityManager::GetChunkWiseAccountingMigrationAccount()
{
    return Impl_->GetChunkWiseAccountingMigrationAccount();
}

TClusterResources TSecurityManager::GetAccountRecursiveViolatedResourceLimits(const TAccount* account) const
{
    return Impl_->GetAccountRecursiveViolatedResourceLimits(account);
}

void TSecurityManager::TrySetResourceLimits(TAccount* account, const TClusterResources& resourceLimits)
{
    Impl_->TrySetResourceLimits(account, resourceLimits);
}

void TSecurityManager::UpdateResourceUsage(const TChunk* chunk, const TChunkRequisition& requisition, i64 delta)
{
    Impl_->UpdateResourceUsage(chunk, requisition, delta);
}

void TSecurityManager::UpdateTabletResourceUsage(TCypressNode* node, const TClusterResources& resourceUsageDelta)
{
    Impl_->UpdateTabletResourceUsage(node, resourceUsageDelta);
}

void TSecurityManager::UpdateTransactionResourceUsage(const TChunk* chunk, const TChunkRequisition& requisition, i64 delta)
{
    Impl_->UpdateTransactionResourceUsage(chunk, requisition, delta);
}

void TSecurityManager::UpdateMasterMemoryUsage(NCypressServer::TCypressNode* node)
{
    Impl_->UpdateMasterMemoryUsage(node);
}

void TSecurityManager::ResetTransactionAccountResourceUsage(TTransaction* transaction)
{
    Impl_->ResetTransactionAccountResourceUsage(transaction);
}

void TSecurityManager::RecomputeTransactionAccountResourceUsage(TTransaction* transaction)
{
    Impl_->RecomputeTransactionResourceUsage(transaction);
}

void TSecurityManager::SetAccount(
    TCypressNode* node,
    TAccount* newAccount,
    TTransaction* transaction) noexcept
{
    Impl_->SetAccount(node, newAccount, transaction);
}

void TSecurityManager::ResetAccount(TCypressNode* node)
{
    Impl_->ResetAccount(node);
}

TUser* TSecurityManager::FindUserByName(const TString& name)
{
    return Impl_->FindUserByName(name);
}

TUser* TSecurityManager::GetUserByNameOrThrow(const TString& name)
{
    return Impl_->GetUserByNameOrThrow(name);
}

TUser* TSecurityManager::GetUserOrThrow(TUserId id)
{
    return Impl_->GetUserOrThrow(id);
}

TUser* TSecurityManager::GetRootUser()
{
    return Impl_->GetRootUser();
}

TUser* TSecurityManager::GetGuestUser()
{
    return Impl_->GetGuestUser();
}

TUser* TSecurityManager::GetOwnerUser()
{
    return Impl_->GetOwnerUser();
}

TGroup* TSecurityManager::FindGroupByName(const TString& name)
{
    return Impl_->FindGroupByName(name);
}

TGroup* TSecurityManager::GetEveryoneGroup()
{
    return Impl_->GetEveryoneGroup();
}

TGroup* TSecurityManager::GetUsersGroup()
{
    return Impl_->GetUsersGroup();
}

TGroup* TSecurityManager::GetSuperusersGroup()
{
    return Impl_->GetSuperusersGroup();
}

TSubject* TSecurityManager::FindSubject(TSubjectId id)
{
    return Impl_->FindSubject(id);
}

TSubject* TSecurityManager::GetSubjectOrThrow(TSubjectId id)
{
    return Impl_->GetSubjectOrThrow(id);
}

TSubject* TSecurityManager::FindSubjectByName(const TString& name)
{
    return Impl_->FindSubjectByName(name);
}

TSubject* TSecurityManager::GetSubjectByNameOrThrow(const TString& name)
{
    return Impl_->GetSubjectByNameOrThrow(name);
}

TNetworkProject* TSecurityManager::FindNetworkProjectByName(const TString& name)
{
    return Impl_->FindNetworkProjectByName(name);
}

void TSecurityManager::RenameNetworkProject(NYT::NSecurityServer::TNetworkProject* networkProject, const TString& newName)
{
    Impl_->RenameNetworkProject(networkProject, newName);
}

void TSecurityManager::AddMember(TGroup* group, TSubject* member, bool ignoreExisting)
{
    Impl_->AddMember(group, member, ignoreExisting);
}

void TSecurityManager::RemoveMember(TGroup* group, TSubject* member, bool ignoreMissing)
{
    Impl_->RemoveMember(group, member, ignoreMissing);
}

void TSecurityManager::RenameSubject(TSubject* subject, const TString& newName)
{
    Impl_->RenameSubject(subject, newName);
}

TAccessControlDescriptor* TSecurityManager::FindAcd(TObject* object)
{
    return Impl_->FindAcd(object);
}

TAccessControlDescriptor* TSecurityManager::GetAcd(TObject* object)
{
    return Impl_->GetAcd(object);
}

TAccessControlList TSecurityManager::GetEffectiveAcl(TObject* object)
{
    return Impl_->GetEffectiveAcl(object);
}

std::optional<TString> TSecurityManager::GetEffectiveAnnotation(TCypressNode* node)
{
    return Impl_->GetEffectiveAnnotation(node);
}

void TSecurityManager::SetAuthenticatedUser(TUser* user)
{
    Impl_->SetAuthenticatedUser(user);
}

void TSecurityManager::SetAuthenticatedUserByNameOrThrow(const TString& userName)
{
    Impl_->SetAuthenticatedUserByNameOrThrow(userName);
}

void TSecurityManager::ResetAuthenticatedUser()
{
    Impl_->ResetAuthenticatedUser();
}

TUser* TSecurityManager::GetAuthenticatedUser()
{
    return Impl_->GetAuthenticatedUser();
}

std::optional<TString> TSecurityManager::GetAuthenticatedUserName()
{
    return Impl_->GetAuthenticatedUserName();
}

bool TSecurityManager::IsSafeMode()
{
    return Impl_->IsSafeMode();
}

TPermissionCheckResponse TSecurityManager::CheckPermission(
    TObject* object,
    TUser* user,
    EPermission permission,
    const TPermissionCheckOptions& options)
{
    return Impl_->CheckPermission(
        object,
        user,
        permission,
        options);
}

TPermissionCheckResponse TSecurityManager::CheckPermission(
    TUser* user,
    EPermission permission,
    const TAccessControlList& acl,
    const TPermissionCheckOptions& options)
{
    return Impl_->CheckPermission(
        user,
        permission,
        acl,
        options);
}

void TSecurityManager::ValidatePermission(
    TObject* object,
    TUser* user,
    EPermission permission,
    const TPermissionCheckOptions& options)
{
    Impl_->ValidatePermission(
        object,
        user,
        permission,
        options);
}

void TSecurityManager::ValidatePermission(
    TObject* object,
    EPermission permission,
    const TPermissionCheckOptions& options)
{
    Impl_->ValidatePermission(
        object,
        permission,
        options);
}

void TSecurityManager::LogAndThrowAuthorizationError(
    const TPermissionCheckTarget& target,
    TUser* user,
    EPermission permission,
    const TPermissionCheckResult& result)
{
    Impl_->LogAndThrowAuthorizationError(
        target,
        user,
        permission,
        result);
}

void TSecurityManager::ValidateResourceUsageIncrease(
    TAccount* account,
    const TClusterResources& delta,
    bool allowRootAccount)
{
    Impl_->ValidateResourceUsageIncrease(
        account,
        delta,
        allowRootAccount);
}

void TSecurityManager::ValidateAttachChildAccount(TAccount* parentAccount, TAccount* childAccount)
{
    Impl_->ValidateAttachChildAccount(parentAccount, childAccount);
}

void TSecurityManager::SetAccountAllowChildrenLimitOvercommit(
    TAccount* account,
    bool overcommitAllowed)
{
    Impl_->SetAccountAllowChildrenLimitOvercommit(account, overcommitAllowed);
}

void TSecurityManager::SetUserBanned(TUser* user, bool banned)
{
    Impl_->SetUserBanned(user, banned);
}

TError TSecurityManager::CheckUserAccess(TUser* user)
{
    return Impl_->CheckUserAccess(user);
}

void TSecurityManager::ChargeUser(TUser* user, const TUserWorkload& workload)
{
    return Impl_->ChargeUser(user, workload);
}

TFuture<void> TSecurityManager::ThrottleUser(TUser* user, int requestCount, EUserWorkloadType workloadType)
{
    return Impl_->ThrottleUser(user, requestCount, workloadType);
}

void TSecurityManager::SetUserRequestRateLimit(TUser* user, int limit, EUserWorkloadType type)
{
    Impl_->SetUserRequestRateLimit(user, limit, type);
}

void TSecurityManager::SetUserRequestLimits(TUser* user, TUserRequestLimitsConfigPtr config)
{
    Impl_->SetUserRequestLimits(user, std::move(config));
}

void TSecurityManager::SetUserRequestQueueSizeLimit(TUser* user, int limit)
{
    Impl_->SetUserRequestQueueSizeLimit(user, limit);
}

bool TSecurityManager::TryIncreaseRequestQueueSize(TUser* user)
{
    return Impl_->TryIncreaseRequestQueueSize(user);
}

void TSecurityManager::DecreaseRequestQueueSize(TUser* user)
{
    Impl_->DecreaseRequestQueueSize(user);
}

const TSecurityTagsRegistryPtr& TSecurityManager::GetSecurityTagsRegistry() const
{
    return Impl_->GetSecurityTagsRegistry();
}

DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, Account, TAccount, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, User, TUser, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, Group, TGroup, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, NetworkProject, TNetworkProject, *Impl_)

DELEGATE_SIGNAL(TSecurityManager, void(TUser*, const TUserWorkload&), UserCharged, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
