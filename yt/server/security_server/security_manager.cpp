#include "stdafx.h"
#include "security_manager.h"
#include "private.h"
#include "account.h"
#include "account_proxy.h"
#include "user.h"
#include "user_proxy.h"
#include "group.h"
#include "group_proxy.h"
#include "acl.h"
#include "request_tracker.h"
#include "config.h"

#include <core/ypath/token.h>

#include <core/profiling/profiling_manager.h>

#include <ytlib/object_client/helpers.h>

#include <server/hydra/entity_map.h>
#include <server/hydra/composite_automaton.h>

#include <server/object_server/type_handler_detail.h>

#include <server/transaction_server/transaction.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/serialize.h>

#include <server/transaction_server/transaction.h>

#include <server/cypress_server/node.h>

// COMPAT(babenko)
#include <server/cypress_server/cypress_manager.h>
#include <server/transaction_server/transaction_manager.h>

namespace NYT {
namespace NSecurityServer {

using namespace NHydra;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYPath;
using namespace NCypressServer;
using namespace NSecurityClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;
static auto& Profiler = SecurityServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TPermissionCheckResult::TPermissionCheckResult()
    : Action(ESecurityAction::Undefined)
    , Object(nullptr)
    , Subject(nullptr)
{ }

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard::TAuthenticatedUserGuard(TSecurityManagerPtr securityManager, TUser* user)
    : SecurityManager(securityManager)
    , IsNull(user == nullptr)
{
    if (user) {
        SecurityManager->PushAuthenticatedUser(user);
    }
}

TAuthenticatedUserGuard::~TAuthenticatedUserGuard()
{
    if (!IsNull) {
        SecurityManager->PopAuthenticatedUser();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TAccountTypeHandler
    : public TObjectTypeHandlerWithMapBase<TAccount>
{
public:
    explicit TAccountTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::Account;
    }

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return TTypeCreationOptions(
            EObjectTransactionMode::Forbidden,
            EObjectAccountMode::Forbidden);
    }

    virtual TObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override;

    virtual EPermissionSet GetSupportedPermissions() const override
    {
        return EPermissionSet(
            EPermissionSet::Read |
            EPermissionSet::Write |
            EPermissionSet::Administer |
            EPermission::Use);
    }

private:
    TImpl* Owner;

    virtual Stroka DoGetName(TAccount* object) override
    {
        return Format("account %Qv", object->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TAccount* account, TTransaction* transaction) override;
    
    virtual void DoDestroy(TAccount* account) override;

    virtual TAccessControlDescriptor* DoFindAcd(TAccount* account) override
    {
        return &account->Acd();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TUserTypeHandler
    : public TObjectTypeHandlerWithMapBase<TUser>
{
public:
    explicit TUserTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::User;
    }

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return TTypeCreationOptions(
            EObjectTransactionMode::Forbidden,
            EObjectAccountMode::Forbidden);
    }

    virtual TObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override;

private:
    TImpl* Owner;

    virtual Stroka DoGetName(TUser* user) override
    {
        return Format("user %Qv", user->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TUser* user, TTransaction* transaction) override;

    virtual void DoDestroy(TUser* user) override;

};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TGroupTypeHandler
    : public TObjectTypeHandlerWithMapBase<TGroup>
{
public:
    explicit TGroupTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::Group;
    }

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return TTypeCreationOptions(
            EObjectTransactionMode::Forbidden,
            EObjectAccountMode::Forbidden);
    }

    virtual TObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override;

private:
    TImpl* Owner;

    virtual Stroka DoGetName(TGroup* group) override
    {
        return Format("group %Qv", group->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TGroup* group, TTransaction* transaction) override;

    virtual void DoDestroy(TGroup* group) override;

};

/////////////////////////////////////////////////////////////////////////// /////

class TSecurityManager::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(
        TSecurityManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap)
        , Config(config)
        , RecomputeResources(false)
        , SysAccount(nullptr)
        , TmpAccount(nullptr)
        , RootUser(nullptr)
        , GuestUser(nullptr)
        , EveryoneGroup(nullptr)
        , UsersGroup(nullptr)
        , RequestTracker(New<TRequestTracker>(config, bootstrap))
    {
        RegisterLoader(
            "SecurityManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "SecurityManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "SecurityManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "SecurityManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        auto cellId = Bootstrap->GetCellId();

        SysAccountId = MakeWellKnownId(EObjectType::Account, cellId, 0xffffffffffffffff);
        TmpAccountId = MakeWellKnownId(EObjectType::Account, cellId, 0xfffffffffffffffe);

        RootUserId = MakeWellKnownId(EObjectType::User, cellId, 0xffffffffffffffff);
        GuestUserId = MakeWellKnownId(EObjectType::User, cellId, 0xfffffffffffffffe);

        EveryoneGroupId = MakeWellKnownId(EObjectType::Group, cellId, 0xffffffffffffffff);
        UsersGroupId = MakeWellKnownId(EObjectType::Group, cellId, 0xfffffffffffffffe);

        RegisterMethod(BIND(&TImpl::UpdateRequestStatistics, Unretained(this)));
    }

    void Initialize()
    {
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->RegisterHandler(New<TAccountTypeHandler>(this));
        objectManager->RegisterHandler(New<TUserTypeHandler>(this));
        objectManager->RegisterHandler(New<TGroupTypeHandler>(this));
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Account, TAccount, TAccountId);
    DECLARE_ENTITY_MAP_ACCESSORS(User, TUser, TUserId);
    DECLARE_ENTITY_MAP_ACCESSORS(Group, TGroup, TGroupId);


    TMutationPtr CreateUpdateRequestStatisticsMutation(
        const NProto::TReqUpdateRequestStatistics& request)
    {
        return CreateMutation(
            Bootstrap->GetHydraFacade()->GetHydraManager(),
            request,
            this,
            &TImpl::UpdateRequestStatistics);
    }


    TAccount* CreateAccount(const Stroka& name)
    {
        if (FindAccountByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Account %v already exists",
                ~name.Quote());
        }

        auto objectManager = Bootstrap->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Account);
        return DoCreateAccount(id, name);
    }

    void DestroyAccount(TAccount* account)
    {
        YCHECK(AccountNameMap.erase(account->GetName()) == 1);
    }

    TAccount* FindAccountByName(const Stroka& name)
    {
        auto it = AccountNameMap.find(name);
        return it == AccountNameMap.end() ? nullptr : it->second;
    }

    TAccount* GetAccountByNameOrThrow(const Stroka& name)
    {
        auto* account = FindAccountByName(name);
        if (!account) {
            THROW_ERROR_EXCEPTION("No such account %v", ~name.Quote());
        }
        return account;
    }


    TAccount* GetSysAccount()
    {
        YCHECK(SysAccount);
        return SysAccount;
    }

    TAccount* GetTmpAccount()
    {
        YCHECK(TmpAccount);
        return TmpAccount;
    }


    void SetAccount(TCypressNodeBase* node, TAccount* account)
    {
        YCHECK(node);
        YCHECK(account);

        auto* oldAccount = node->GetAccount();
        if (oldAccount == account)
            return;

        auto objectManager = Bootstrap->GetObjectManager();

        bool isAccountingEnabled = IsUncommittedAccountingEnabled(node);

        if (oldAccount) {
            if (isAccountingEnabled) {
                UpdateResourceUsage(node, oldAccount, -1);
            }
            objectManager->UnrefObject(oldAccount);
        }

        node->SetAccount(account);
        node->CachedResourceUsage() = node->GetResourceUsage();

        if (isAccountingEnabled) {
            UpdateResourceUsage(node, account, +1);
        }
        objectManager->RefObject(account);
    }

    void ResetAccount(TCypressNodeBase* node)
    {
        auto* account = node->GetAccount();
        if (!account)
            return;

        auto objectManager = Bootstrap->GetObjectManager();

        bool isAccountingEnabled = IsUncommittedAccountingEnabled(node);

        if (isAccountingEnabled) {
            UpdateResourceUsage(node, account, -1);
        }

        node->CachedResourceUsage() = ZeroClusterResources();
        node->SetAccount(nullptr);

        objectManager->UnrefObject(account);
    }

    void RenameAccount(TAccount* account, const Stroka& newName)
    {
        if (newName == account->GetName())
            return;

        if (FindAccountByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Account %v already exists",
                ~newName.Quote());
        }

        YCHECK(AccountNameMap.erase(account->GetName()) == 1);
        YCHECK(AccountNameMap.insert(std::make_pair(newName, account)).second);
        account->SetName(newName);
    }

    void UpdateAccountNodeUsage(TCypressNodeBase* node)
    {
        auto* account = node->GetAccount();
        if (!account)
            return;

        if (!IsUncommittedAccountingEnabled(node))
            return;

        UpdateResourceUsage(node, account, -1);

        node->CachedResourceUsage() = node->GetResourceUsage();

        UpdateResourceUsage(node, account, +1);
    }

    void UpdateAccountStagingUsage(
        TTransaction* transaction,
        TAccount* account,
        const TClusterResources& delta)
    {
        if (!IsStagedAccountingEnabled(transaction))
            return;

        account->ResourceUsage() += delta;

        auto* transactionUsage = GetTransactionAccountUsage(transaction, account);
        *transactionUsage += delta;
    }


    void DestroySubject(TSubject* subject)
    {
        for (auto* group  : subject->MemberOf()) {
            YCHECK(group->Members().erase(subject) == 1);
        }

        for (const auto& pair : subject->LinkedObjects()) {
            auto* acd = GetAcd(pair.first);
            acd->OnSubjectDestroyed(subject, GuestUser);
        }
    }


    TUser* CreateUser(const Stroka& name)
    {
        if (FindUserByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "User %v already exists",
                ~name.Quote());
        }

        if (FindGroupByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Group %v already exists",
                ~name.Quote());
        }

        auto objectManager = Bootstrap->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::User);
        return DoCreateUser(id, name);
    }

    void DestroyUser(TUser* user)
    {
        YCHECK(UserNameMap.erase(user->GetName()) == 1);
        DestroySubject(user);
    }

    TUser* FindUserByName(const Stroka& name)
    {
        auto it = UserNameMap.find(name);
        return it == UserNameMap.end() ? nullptr : it->second;
    }

    TUser* GetUserByNameOrThrow(const Stroka& name)
    {
        auto* user = FindUserByName(name);
        if (!IsObjectAlive(user)) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthenticationError,
                "No such user %v",
                ~name.Quote());
        }
        return user;
    }

    TUser* GetUserOrThrow(const TUserId& id)
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
        YCHECK(RootUser);
        return RootUser;
    }

    TUser* GetGuestUser()
    {
        YCHECK(GuestUser);
        return GuestUser;
    }


    TGroup* CreateGroup(const Stroka& name)
    {
        if (FindGroupByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Group %v already exists",
                ~name.Quote());
        }

        if (FindUserByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "User %v already exists",
                ~name.Quote());
        }

        auto objectManager = Bootstrap->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Group);
        return DoCreateGroup(id, name);
    }

    void DestroyGroup(TGroup* group)
    {
        YCHECK(GroupNameMap.erase(group->GetName()) == 1);

        for (auto* subject : group->Members()) {
            YCHECK(subject->MemberOf().erase(group) == 1);
        }

        DestroySubject(group);

        RecomputeMembershipClosure();
    }

    TGroup* FindGroupByName(const Stroka& name)
    {
        auto it = GroupNameMap.find(name);
        return it == GroupNameMap.end() ? nullptr : it->second;
    }


    TGroup* GetEveryoneGroup()
    {
        YCHECK(EveryoneGroup);
        return EveryoneGroup;
    }

    TGroup* GetUsersGroup()
    {
        YCHECK(UsersGroup);
        return UsersGroup;
    }


    TSubject* FindSubjectByName(const Stroka& name)
    {
        auto* user = FindUserByName(name);
        if (user) {
            return user;
        }

        auto* group = FindGroupByName(name);
        if (group) {
            return group;
        }

        return nullptr;
    }

    TSubject* GetSubjectByNameOrThrow(const Stroka& name)
    {
        auto* subject = FindSubjectByName(name);
        if (!IsObjectAlive(subject)) {
            THROW_ERROR_EXCEPTION("No such subject %v", ~name.Quote());
        }
        return subject;
    }


    void AddMember(TGroup* group, TSubject* member)
    {
        ValidateMembershipUpdate(group, member);

        if (group->Members().find(member) != group->Members().end()) {
            THROW_ERROR_EXCEPTION("Member %v is already present in group %v",
                ~member->GetName().Quote(),
                ~group->GetName().Quote());
        }

        if (member->GetType() == EObjectType::Group) {
            auto* memberGroup = member->AsGroup();
            if (group->RecursiveMemberOf().find(memberGroup) != group->RecursiveMemberOf().end()) {
                THROW_ERROR_EXCEPTION("Adding group %v to group %v would produce a cycle",
                    ~memberGroup->GetName().Quote(),
                    ~group->GetName().Quote());
            }
        }

        DoAddMember(group, member);
    }

    void RemoveMember(TGroup* group, TSubject* member)
    {
        ValidateMembershipUpdate(group, member);

        if (group->Members().find(member) == group->Members().end()) {
            THROW_ERROR_EXCEPTION("Member %v is not present in group %v",
                ~member->GetName().Quote(),
                ~group->GetName().Quote());
        }

        DoRemoveMember(group, member);
    }


    void RenameSubject(TSubject* subject, const Stroka& newName)
    {
        if (newName == subject->GetName())
            return;

        if (FindSubjectByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Subject %v already exists",
                ~newName.Quote());
        }

        switch (subject->GetType()) {
            case EObjectType::User:
                YCHECK(UserNameMap.erase(subject->GetName()) == 1);
                YCHECK(UserNameMap.insert(std::make_pair(newName, subject->AsUser())).second);
                break;

            case EObjectType::Group:
                YCHECK(GroupNameMap.erase(subject->GetName()) == 1);
                YCHECK(GroupNameMap.insert(std::make_pair(newName, subject->AsGroup())).second);
                break;

            default:
                YUNREACHABLE();
        }
        subject->SetName(newName);
    }


    EPermissionSet GetSupportedPermissions(TObjectBase* object)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto handler = objectManager->GetHandler(object);
        return handler->GetSupportedPermissions();
    }

    TAccessControlDescriptor* FindAcd(TObjectBase* object)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto handler = objectManager->GetHandler(object);
        return handler->FindAcd(object);
    }

    TAccessControlDescriptor* GetAcd(TObjectBase* object)
    {
        auto* acd = FindAcd(object);
        YCHECK(acd);
        return acd;
    }

    TAccessControlList GetEffectiveAcl(NObjectServer::TObjectBase* object)
    {
        TAccessControlList result;
        auto objectManager = Bootstrap->GetObjectManager();
        while (object) {
            auto handler = objectManager->GetHandler(object);
            auto* acd = handler->FindAcd(object);
            if (acd) {
                result.Entries.insert(result.Entries.end(), acd->Acl().Entries.begin(), acd->Acl().Entries.end());
                if (!acd->GetInherit()) {
                    break;
                }
            }

            object = handler->GetParent(object);
        }

        return result;
    }


    void PushAuthenticatedUser(TUser* user)
    {
        AuthenticatedUserStack.push_back(user);
    }

    void PopAuthenticatedUser()
    {
        AuthenticatedUserStack.pop_back();
    }

    TUser* GetAuthenticatedUser()
    {
        return AuthenticatedUserStack.back();
    }


    TPermissionCheckResult CheckPermission(
        TObjectBase* object,
        TUser* user,
        EPermission permission)
    {
        TPermissionCheckResult result;

        // Fast lane: "root" needs to authorization.
        if (user == RootUser) {
            result.Action = ESecurityAction::Allow;
            return result;
        }

        // Slow lane: check ACLs through the object hierarchy.
        auto objectManager = Bootstrap->GetObjectManager();
        auto* currentObject = object;
        while (currentObject) {
            auto handler = objectManager->GetHandler(currentObject);
            auto* acd = handler->FindAcd(currentObject);

            // Check the current ACL, if any.
            if (acd) {
                for (const auto& ace : acd->Acl().Entries) {
                    if (CheckPermissionMatch(ace.Permissions, permission)) {
                        for (auto* subject : ace.Subjects) {
                            if (CheckSubjectMatch(subject, user)) {
                                result.Action = ace.Action;
                                result.Object = currentObject;
                                result.Subject = subject;
                                // At least one denying ACE is found, deny the request.
                                if (result.Action == ESecurityAction::Deny) {
                                    LOG_INFO_UNLESS(IsRecovery(), "Permission check failed: explicit denying ACE found (CheckObjectId: %v, Permission: %v, User: %v, AclObjectId: %v, AclSubject: %v)",
                                        object->GetId(),
                                        permission,
                                        ~user->GetName(),
                                        result.Object->GetId(),
                                        ~result.Subject->GetName());
                                    return result;
                                }
                            }
                        }
                    }
                }

                // Proceed to the parent object unless the current ACL explicitly forbids inheritance.
                if (!acd->GetInherit()) {
                    break;
                }
            }

            currentObject = handler->GetParent(currentObject);
        }

        // No allowing ACE, deny the request.
        if (result.Action == ESecurityAction::Undefined) {
            LOG_INFO_UNLESS(IsRecovery(), "Permission check failed: no matching ACE found (CheckObjectId: %v, Permission: %v, User: %v)",
                object->GetId(),
                permission,
                ~user->GetName());
            result.Action = ESecurityAction::Deny;
            return result;
        } else {
            YASSERT(result.Action == ESecurityAction::Allow);
            LOG_TRACE_UNLESS(IsRecovery(), "Permission check succeeded: explicit allowing ACE found (CheckObjectId: %v, Permission: %v, User: %v, AclObjectId: %v, AclSubject: %v)",
                object->GetId(),
                permission,
                ~user->GetName(),
                result.Object->GetId(),
                ~result.Subject->GetName());
            return result;
        }
    }

    void ValidatePermission(
        TObjectBase* object,
        TUser* user,
        EPermission permission)
    {
        auto result = CheckPermission(object, user, permission);
        if (result.Action == ESecurityAction::Deny) {
            auto objectManager = Bootstrap->GetObjectManager();
            TError error;
            if (result.Object && result.Subject) {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: %v permission for %v is denied for %v by ACE at %v",
                    ~FormatEnum(permission).Quote(),
                    ~objectManager->GetHandler(object)->GetName(object),
                    ~result.Subject->GetName().Quote(),
                    ~objectManager->GetHandler(object)->GetName(result.Object));
            } else {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: %v permission for %v is not allowed by any matching ACE",
                    ~FormatEnum(permission).Quote(),
                    ~objectManager->GetHandler(object)->GetName(object));
            }
            error.Attributes().Set("permission", ~FormatEnum(permission));
            error.Attributes().Set("user", user->GetName());
            error.Attributes().Set("object", object->GetId());
            if (result.Object) {
                error.Attributes().Set("denied_by", result.Object->GetId());
            }
            if (result.Subject) {
                error.Attributes().Set("denied_for", result.Subject->GetId());
            }
            THROW_ERROR(error);
        }
    }

    void ValidatePermission(
        TObjectBase* object,
        EPermission permission)
    {
        ValidatePermission(
            object,
            GetAuthenticatedUser(),
            permission);
    }


    void SetUserBanned(TUser* user, bool banned)
    {
        if (banned && user == RootUser) {
            THROW_ERROR_EXCEPTION("User %v cannot be banned",
                ~user->GetName().Quote());
        }

        user->SetBanned(banned);
        if (banned) {
            LOG_INFO_UNLESS(IsRecovery(), "User is now banned (User: %v)", ~user->GetName());
        } else {
            LOG_INFO_UNLESS(IsRecovery(), "User is now unbanned (User: %v)", ~user->GetName());
        }
    }

    void ValidateUserAccess(TUser* user, int requestCount)
    {
        if (user->GetBanned()) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::UserBanned,
                "User %v is banned",
                ~user->GetName().Quote());
        }

        if (user != RootUser && GetRequestRate(user) > user->GetRequestRateLimit()) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::UserBanned,
                "User %v has exceeded its request rate limit",
                ~user->GetName().Quote())
                << TErrorAttribute("limit", user->GetRequestRateLimit());
        }

        RequestTracker->ChargeUser(user, requestCount);
    }

    double GetRequestRate(TUser* user)
    {
        return
            TInstant::Now() > user->GetCheckpointTime() + Config->RequestRateSmoothingPeriod
            ? 0.0
            : user->GetRequestRate();
    }

private:
    friend class TAccountTypeHandler;
    friend class TUserTypeHandler;
    friend class TGroupTypeHandler;


    TSecurityManagerConfigPtr Config;

    bool RecomputeResources;

    NHydra::TEntityMap<TAccountId, TAccount> AccountMap;
    yhash_map<Stroka, TAccount*> AccountNameMap;

    TAccountId SysAccountId;
    TAccount* SysAccount;

    TAccountId TmpAccountId;
    TAccount* TmpAccount;

    NHydra::TEntityMap<TUserId, TUser> UserMap;
    yhash_map<Stroka, TUser*> UserNameMap;

    TUserId RootUserId;
    TUser* RootUser;

    TUserId GuestUserId;
    TUser* GuestUser;

    NHydra::TEntityMap<TGroupId, TGroup> GroupMap;
    yhash_map<Stroka, TGroup*> GroupNameMap;

    TGroupId EveryoneGroupId;
    TGroup* EveryoneGroup;

    TGroupId UsersGroupId;
    TGroup* UsersGroup;

    std::vector<TUser*> AuthenticatedUserStack;

    TRequestTrackerPtr RequestTracker;


    static bool IsUncommittedAccountingEnabled(TCypressNodeBase* node)
    {
        auto* transaction = node->GetTransaction();
        return !transaction || transaction->GetUncommittedAccountingEnabled();
    }

    static bool IsStagedAccountingEnabled(TTransaction* transaction)
    {
        return transaction->GetStagedAccountingEnabled();
    }

    static void UpdateResourceUsage(TCypressNodeBase* node, TAccount* account, int delta)
    {
        auto resourceUsage = node->CachedResourceUsage() * delta;

        account->ResourceUsage() += resourceUsage;
        if (node->IsTrunk()) {
            account->CommittedResourceUsage() += resourceUsage;
        }

        auto* transactionUsage = FindTransactionAccountUsage(node);
        if (transactionUsage) {
            *transactionUsage += resourceUsage;
        }
    }

    static TClusterResources* FindTransactionAccountUsage(TCypressNodeBase* node)
    {
        auto* account = node->GetAccount();
        auto* transaction = node->GetTransaction();
        if (!transaction) {
            return nullptr;
        }

        return GetTransactionAccountUsage(transaction, account);
    }

    static TClusterResources* GetTransactionAccountUsage(TTransaction* transaction, TAccount* account)
    {
        auto it = transaction->AccountResourceUsage().find(account);
        if (it == transaction->AccountResourceUsage().end()) {
            auto pair = transaction->AccountResourceUsage().insert(std::make_pair(account, ZeroClusterResources()));
            YCHECK(pair.second);
            return &pair.first->second;
        } else {
            return &it->second;
        }
    }


    TAccount* DoCreateAccount(const TAccountId& id, const Stroka& name)
    {
        auto* account = new TAccount(id);
        account->SetName(name);

        AccountMap.Insert(id, account);
        YCHECK(AccountNameMap.insert(std::make_pair(account->GetName(), account)).second);

        // Make the fake reference.
        YCHECK(account->RefObject() == 1);

        return account;
    }
    
    TUser* DoCreateUser(const TUserId& id, const Stroka& name)
    {
        auto* user = new TUser(id);
        user->SetName(name);

        UserMap.Insert(id, user);
        YCHECK(UserNameMap.insert(std::make_pair(user->GetName(), user)).second);

        // Make the fake reference.
        YCHECK(user->RefObject() == 1);

        // Every user except for "guest" is a member of "users" group.
        // "guest is a member of "everyone" group.
        if (id == GuestUserId) {
            DoAddMember(EveryoneGroup, user);
        } else {
            DoAddMember(UsersGroup, user);
        }

        return user;
    }

    TGroup* DoCreateGroup(const TGroupId& id, const Stroka& name)
    {
        auto* group = new TGroup(id);
        group->SetName(name);

        GroupMap.Insert(id, group);
        YCHECK(GroupNameMap.insert(std::make_pair(group->GetName(), group)).second);

        // Make the fake reference.
        YCHECK(group->RefObject() == 1);

        return group;
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

    void RecomputeMembershipClosure()
    {
        for (const auto& pair : UserMap) {
            pair.second->RecursiveMemberOf().clear();
        }

        for (const auto& pair : GroupMap) {
            pair.second->RecursiveMemberOf().clear();
        }

        for (const auto& pair : GroupMap) {
            auto* group = pair.second;
            for (auto* member : group->Members()) {
                PropagateRecursiveMemberOf(member, group);
            }
        }
    }


    void DoAddMember(TGroup* group, TSubject* member)
    {
        YCHECK(group->Members().insert(member).second);
        YCHECK(member->MemberOf().insert(group).second);

        RecomputeMembershipClosure();
    }

    void DoRemoveMember(TGroup* group, TSubject* member)
    {
        YCHECK(group->Members().erase(member) == 1);
        YCHECK(member->MemberOf().erase(group) == 1);

        RecomputeMembershipClosure();
    }


    void ValidateMembershipUpdate(TGroup* group, TSubject* member)
    {
        if (group == EveryoneGroup || group == UsersGroup) {
            THROW_ERROR_EXCEPTION("Cannot modify a built-in group");
        }

        ValidatePermission(group, EPermission::Write);
        ValidatePermission(member, EPermission::Write);
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
                YUNREACHABLE();
        }
    }

    static bool CheckPermissionMatch(EPermissionSet permissions, EPermission requestedPermission)
    {
        return permissions & requestedPermission;
    }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        AccountMap.SaveKeys(context);
        UserMap.SaveKeys(context);
        GroupMap.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        AccountMap.SaveValues(context);
        UserMap.SaveValues(context);
        GroupMap.SaveValues(context);
    }


    virtual void OnBeforeSnapshotLoaded() override
    {
        DoClear();

        RecomputeResources = false;
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        AccountMap.LoadKeys(context);
        UserMap.LoadKeys(context);
        GroupMap.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        AccountMap.LoadValues(context);
        UserMap.LoadValues(context);
        GroupMap.LoadValues(context);
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        // Reconstruct account name map.
        AccountNameMap.clear();
        for (const auto& pair : AccountMap) {
            auto* account = pair.second;
            YCHECK(AccountNameMap.insert(std::make_pair(account->GetName(), account)).second);
        }

        // Reconstruct user name map.
        UserNameMap.clear();
        for (const auto& pair : UserMap) {
            auto* user = pair.second;
            YCHECK(UserNameMap.insert(std::make_pair(user->GetName(), user)).second);
        }

        // Reconstruct group name map.
        GroupNameMap.clear();
        for (const auto& pair : GroupMap) {
            auto* group = pair.second;
            YCHECK(GroupNameMap.insert(std::make_pair(group->GetName(), group)).second);
        }

        InitBuiltin();
        InitAuthenticatedUser();

        // COMPAT(babenko)
        if (RecomputeResources) {
            LOG_INFO("Recomputing resource usage");

            YCHECK(Bootstrap->GetTransactionManager()->Transactions().GetSize() == 0);

            for (const auto& pair : AccountMap) {
                auto* account = pair.second;
                account->ResourceUsage() = ZeroClusterResources();
                account->CommittedResourceUsage() = ZeroClusterResources();
            }

            auto cypressManager = Bootstrap->GetCypressManager();
            for (const auto& pair : cypressManager->Nodes()) {
                auto* node = pair.second;
                auto resourceUsage = node->GetResourceUsage();
                auto* account = node->GetAccount();
                if (account) {
                    if (IsUncommittedAccountingEnabled(node)) {
                        account->ResourceUsage() += resourceUsage;
                        if (node->IsTrunk()) {
                            account->CommittedResourceUsage() += resourceUsage;
                        }
                    }
                    node->CachedResourceUsage() = resourceUsage;
                } else {
                    node->CachedResourceUsage() = ZeroClusterResources();
                }
            }
        }
    }


    void DoClear()
    {
        AccountMap.Clear();
        AccountNameMap.clear();

        UserMap.Clear();
        UserNameMap.clear();

        GroupMap.Clear();
        GroupNameMap.clear();
    }

    virtual void Clear() override
    {
        DoClear();
        InitBuiltin();
        InitAuthenticatedUser();
        InitDefaultSchemaAcds();
    }

    void InitAuthenticatedUser()
    {
        AuthenticatedUserStack.clear();
        AuthenticatedUserStack.push_back(RootUser);
    }

    void InitDefaultSchemaAcds()
    {
        auto objectManager = Bootstrap->GetObjectManager();
        for (auto type : objectManager->GetRegisteredTypes()) {
            if (HasSchema(type)) {
                auto* schema = objectManager->GetSchema(type);
                auto* acd = GetAcd(schema);
                if (!IsVersionedType(type)) {
                    acd->AddEntry(TAccessControlEntry(
                        ESecurityAction::Allow,
                        GetUsersGroup(),
                        EPermissionSet(EPermission::Write)));
                    acd->AddEntry(TAccessControlEntry(
                        ESecurityAction::Allow,
                        GetEveryoneGroup(),
                        EPermissionSet(EPermission::Read)));
                }
                if (IsUserType(type)) {
                    acd->AddEntry(TAccessControlEntry(
                        ESecurityAction::Allow,
                        GetUsersGroup(),
                        EPermissionSet(EPermission::Create)));
                }
            }
        }
    }

    void InitBuiltin()
    {
        UsersGroup = FindGroup(UsersGroupId);
        if (!UsersGroup) {
            // users
            UsersGroup = DoCreateGroup(UsersGroupId, UsersGroupName);
        }

        EveryoneGroup = FindGroup(EveryoneGroupId);
        if (!EveryoneGroup) {
            // everyone
            EveryoneGroup = DoCreateGroup(EveryoneGroupId, EveryoneGroupName);
            DoAddMember(EveryoneGroup, UsersGroup);
        }

        RootUser = FindUser(RootUserId);
        if (!RootUser) {
            // root
            RootUser = DoCreateUser(RootUserId, RootUserName);
            RootUser->SetRequestRateLimit(1000000.0);
        }

        GuestUser = FindUser(GuestUserId);
        if (!GuestUser) {
            // guest
            GuestUser = DoCreateUser(GuestUserId, GuestUserName);
        }

        SysAccount = FindAccount(SysAccountId);
        if (!SysAccount) {
            // sys, 1 TB disk space, 100000 nodes, usage allowed for: root
            SysAccount = DoCreateAccount(SysAccountId, SysAccountName);
            SysAccount->ResourceLimits() = TClusterResources((i64) 1024 * 1024 * 1024 * 1024, 100000);
            SysAccount->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                RootUser,
                EPermission::Use));
        }

        TmpAccount = FindAccount(TmpAccountId);
        if (!TmpAccount) {
            // tmp, 1 TB disk space, 100000 nodes, usage allowed for: users
            TmpAccount = DoCreateAccount(TmpAccountId, TmpAccountName);
            TmpAccount->ResourceLimits() = TClusterResources((i64) 1024 * 1024 * 1024 * 1024, 100000);
            TmpAccount->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                UsersGroup,
                EPermission::Use));
        }
    }


    virtual void OnLeaderActive() override
    {
        RequestTracker->Start();

        for (const auto& pair : UserMap) {
            auto* user = pair.second;
            user->ResetRequestRate();
        }
    }

    virtual void OnStopLeading() override
    {
        RequestTracker->Stop();
    }


    void UpdateRequestStatistics(const NProto::TReqUpdateRequestStatistics& request)
    {
        auto* profilingManager = NProfiling::TProfilingManager::Get();
        auto now = TInstant::Now();
        for (const auto& update : request.updates()) {
            auto userId = FromProto<TUserId>(update.user_id());
            auto* user = FindUser(userId);
            if (user) {
                // Update access time.
                auto accessTime = TInstant(update.access_time());
                if (accessTime > user->GetAccessTime()) {
                    user->SetAccessTime(accessTime);
                }

                // Update request counter.
                i64 requestCounter = user->GetRequestCounter() + update.request_counter_delta();
                user->SetRequestCounter(requestCounter);

                NProfiling::TTagIdList tags;
                tags.push_back(profilingManager->RegisterTag("user", user->GetName()));
                Profiler.Enqueue("/user_request_counter", requestCounter, tags);

                // Recompute request rate.
                if (now > user->GetCheckpointTime() + Config->RequestRateSmoothingPeriod) {
                    if (user->GetCheckpointTime() != TInstant::Zero()) {
                        double requestRate =
                            static_cast<double>(requestCounter - user->GetCheckpointRequestCounter()) /
                            (now - user->GetCheckpointTime()).SecondsFloat();
                        user->SetRequestRate(requestRate);
                        // TODO(babenko): use tags in master
                        Profiler.Enqueue("/user_request_rate/" + ToYPathLiteral(user->GetName()), static_cast<int>(requestRate));
                    }
                    user->SetCheckpointTime(now);
                    user->SetCheckpointRequestCounter(requestCounter);
                }
            }
        }
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, Account, TAccount, TAccountId, AccountMap)
DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, User, TUser, TUserId, UserMap)
DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, Group, TGroup, TGroupId, GroupMap)

///////////////////////////////////////////////////////////////////////////////

TSecurityManager::TAccountTypeHandler::TAccountTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->AccountMap)
    , Owner(owner)
{ }

TObjectBase* TSecurityManager::TAccountTypeHandler::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* attributes,
    TReqCreateObjects* request,
    TRspCreateObjects* response)
{
    UNUSED(transaction);
    UNUSED(account);
    UNUSED(request);
    UNUSED(response);

    auto name = attributes->Get<Stroka>("name");
    attributes->Remove("name");

    auto* newAccount = Owner->CreateAccount(name);
    return newAccount;
}

IObjectProxyPtr TSecurityManager::TAccountTypeHandler::DoGetProxy(
    TAccount* account,
    TTransaction* transaction)
{
    UNUSED(transaction);
    return CreateAccountProxy(Owner->Bootstrap, account);
}

void TSecurityManager::TAccountTypeHandler::DoDestroy(TAccount* account)
{
    Owner->DestroyAccount(account);
}

///////////////////////////////////////////////////////////////////////////////

TSecurityManager::TUserTypeHandler::TUserTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->UserMap)
    , Owner(owner)
{ }

TObjectBase* TSecurityManager::TUserTypeHandler::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* attributes,
    TReqCreateObjects* request,
    TRspCreateObjects* response)
{
    UNUSED(transaction);
    UNUSED(account);
    UNUSED(request);
    UNUSED(response);

    auto name = attributes->Get<Stroka>("name");
    attributes->Remove("name");

    auto* newUser = Owner->CreateUser(name);
    return newUser;
}

IObjectProxyPtr TSecurityManager::TUserTypeHandler::DoGetProxy(
    TUser* user,
    TTransaction* transaction)
{
    UNUSED(transaction);
    return CreateUserProxy(Owner->Bootstrap, user);
}

void TSecurityManager::TUserTypeHandler::DoDestroy(TUser* user)
{
    Owner->DestroyUser(user);
}

///////////////////////////////////////////////////////////////////////////////

TSecurityManager::TGroupTypeHandler::TGroupTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->GroupMap)
    , Owner(owner)
{ }

TObjectBase* TSecurityManager::TGroupTypeHandler::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* attributes,
    TReqCreateObjects* request,
    TRspCreateObjects* response)
{
    UNUSED(transaction);
    UNUSED(account);
    UNUSED(request);
    UNUSED(response);

    auto name = attributes->Get<Stroka>("name");
    attributes->Remove("name");

    auto* newGroup = Owner->CreateGroup(name);
    return newGroup;
}

IObjectProxyPtr TSecurityManager::TGroupTypeHandler::DoGetProxy(
    TGroup* group,
    TTransaction* transaction)
{
    UNUSED(transaction);
    return CreateGroupProxy(Owner->Bootstrap, group);
}

void TSecurityManager::TGroupTypeHandler::DoDestroy(TGroup* group)
{
    Owner->DestroyGroup(group);
}

///////////////////////////////////////////////////////////////////////////////

TSecurityManager::TSecurityManager(
    TSecurityManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

TSecurityManager::~TSecurityManager()
{ }

void TSecurityManager::Initialize()
{
    return Impl->Initialize();
}

TMutationPtr TSecurityManager::CreateUpdateRequestStatisticsMutation(
    const NProto::TReqUpdateRequestStatistics& request)
{
    return Impl->CreateUpdateRequestStatisticsMutation(request);
}

TAccount* TSecurityManager::FindAccountByName(const Stroka& name)
{
    return Impl->FindAccountByName(name);
}

TAccount* TSecurityManager::GetAccountByNameOrThrow(const Stroka& name)
{
    return Impl->GetAccountByNameOrThrow(name);
}

TAccount* TSecurityManager::GetSysAccount()
{
    return Impl->GetSysAccount();
}

TAccount* TSecurityManager::GetTmpAccount()
{
    return Impl->GetTmpAccount();
}

void TSecurityManager::SetAccount(TCypressNodeBase* node, TAccount* account)
{
    Impl->SetAccount(node, account);
}

void TSecurityManager::ResetAccount(TCypressNodeBase* node)
{
    Impl->ResetAccount(node);
}

void TSecurityManager::RenameAccount(TAccount* account, const Stroka& newName)
{
    Impl->RenameAccount(account, newName);
}

void TSecurityManager::UpdateAccountNodeUsage(TCypressNodeBase* node)
{
    Impl->UpdateAccountNodeUsage(node);
}

void TSecurityManager::UpdateAccountStagingUsage(
    TTransaction* transaction,
    TAccount* account,
    const TClusterResources& delta)
{
    Impl->UpdateAccountStagingUsage(transaction, account, delta);
}

TUser* TSecurityManager::FindUserByName(const Stroka& name)
{
    return Impl->FindUserByName(name);
}

TUser* TSecurityManager::GetUserByNameOrThrow(const Stroka& name)
{
    return Impl->GetUserByNameOrThrow(name);
}

TUser* TSecurityManager::GetUserOrThrow(const TUserId& id)
{
    return Impl->GetUserOrThrow(id);
}

TUser* TSecurityManager::GetRootUser()
{
    return Impl->GetRootUser();
}

TUser* TSecurityManager::GetGuestUser()
{
    return Impl->GetGuestUser();
}

TGroup* TSecurityManager::FindGroupByName(const Stroka& name)
{
    return Impl->FindGroupByName(name);
}

TGroup* TSecurityManager::GetEveryoneGroup()
{
    return Impl->GetEveryoneGroup();
}

TGroup* TSecurityManager::GetUsersGroup()
{
    return Impl->GetUsersGroup();
}

TSubject* TSecurityManager::FindSubjectByName(const Stroka& name)
{
    return Impl->FindSubjectByName(name);
}

TSubject* TSecurityManager::GetSubjectByNameOrThrow(const Stroka& name)
{
    return Impl->GetSubjectByNameOrThrow(name);
}

void TSecurityManager::AddMember(TGroup* group, TSubject* member)
{
    Impl->AddMember(group, member);
}

void TSecurityManager::RemoveMember(TGroup* group, TSubject* member)
{
    Impl->RemoveMember(group, member);
}

void TSecurityManager::RenameSubject(TSubject* subject, const Stroka& newName)
{
    Impl->RenameSubject(subject, newName);
}

EPermissionSet TSecurityManager::GetSupportedPermissions(TObjectBase* object)
{
    return Impl->GetSupportedPermissions(object);
}

TAccessControlDescriptor* TSecurityManager::FindAcd(TObjectBase* object)
{
    return Impl->FindAcd(object);
}

TAccessControlDescriptor* TSecurityManager::GetAcd(TObjectBase* object)
{
    return Impl->GetAcd(object);
}

TAccessControlList TSecurityManager::GetEffectiveAcl(TObjectBase* object)
{
    return Impl->GetEffectiveAcl(object);
}

void TSecurityManager::PushAuthenticatedUser(TUser* user)
{
    Impl->PushAuthenticatedUser(user);
}

void TSecurityManager::PopAuthenticatedUser()
{
    Impl->PopAuthenticatedUser();
}

TUser* TSecurityManager::GetAuthenticatedUser()
{
    return Impl->GetAuthenticatedUser();
}

TPermissionCheckResult TSecurityManager::CheckPermission(
    TObjectBase* object,
    TUser* user,
    EPermission permission)
{
    return Impl->CheckPermission(
        object,
        user,
        permission);
}

void TSecurityManager::ValidatePermission(
    TObjectBase* object,
    TUser* user,
    EPermission permission)
{
    Impl->ValidatePermission(
        object,
        user,
        permission);
}

void TSecurityManager::ValidatePermission(
    TObjectBase* object,
    EPermission permission)
{
    Impl->ValidatePermission(
        object,
        permission);
}

void TSecurityManager::SetUserBanned(TUser* user, bool banned)
{
    Impl->SetUserBanned(user, banned);
}

void TSecurityManager::ValidateUserAccess(TUser* user, int requestCount)
{
    Impl->ValidateUserAccess(user, requestCount);
}

double TSecurityManager::GetRequestRate(TUser* user)
{
    return Impl->GetRequestRate(user);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, Account, TAccount, TAccountId, *Impl)
DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, User, TUser, TUserId, *Impl)
DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, Group, TGroup, TGroupId, *Impl)

///////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
