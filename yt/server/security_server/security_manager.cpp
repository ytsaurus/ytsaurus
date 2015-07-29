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

#include <core/concurrency/periodic_executor.h>

#include <core/profiling/profile_manager.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/security_client/group_ypath_proxy.h>
#include <ytlib/security_client/helpers.h>

#include <server/hydra/entity_map.h>
#include <server/hydra/composite_automaton.h>

#include <server/hive/hive_manager.h>

#include <server/object_server/type_handler_detail.h>

#include <server/transaction_server/transaction.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/multicell_manager.h>
#include <server/cell_master/serialize.h>

#include <server/transaction_server/transaction.h>

#include <server/cypress_server/cypress_manager.h>
#include <server/cypress_server/node.h>
#include <server/cypress_server/type_handler.h>

namespace NYT {
namespace NSecurityServer {

using namespace NConcurrency;
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

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;
static const auto& Profiler = SecurityServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard::TAuthenticatedUserGuard(TSecurityManagerPtr securityManager, TUser* user)
    : SecurityManager_(std::move(securityManager))
    , IsNull_(!user)
{
    if (!IsNull_) {
        SecurityManager_->SetAuthenticatedUser(user);
    }
}

TAuthenticatedUserGuard::~TAuthenticatedUserGuard()
{
    if (!IsNull_) {
        SecurityManager_->ResetAuthenticatedUser();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TAccountTypeHandler
    : public TObjectTypeHandlerWithMapBase<TAccount>
{
public:
    explicit TAccountTypeHandler(TImpl* owner);

    virtual EObjectReplicationFlags GetReplicationFlags() const override
    {
        return
            EObjectReplicationFlags::ReplicateCreate |
            EObjectReplicationFlags::ReplicateDestroy |
            EObjectReplicationFlags::ReplicateAttributes;
    }

    virtual TCellTag GetReplicationCellTag(const TObjectBase* /*object*/) override
    {
        return AllSecondaryMastersCellTag;
    }

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

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObject* request,
        TRspCreateObject* response) override;

    virtual EPermissionSet GetSupportedPermissions() const override
    {
        return TObjectTypeHandlerWithMapBase<TAccount>::GetSupportedPermissions() | EPermissionSet::Use;
    }

private:
    TImpl* const Owner_;

    virtual Stroka DoGetName(TAccount* object) override
    {
        return Format("account %Qv", object->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TAccount* account, TTransaction* transaction) override;

    virtual void DoDestroyObject(TAccount* account) override;

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

    virtual EObjectReplicationFlags GetReplicationFlags() const override
    {
        return
            EObjectReplicationFlags::ReplicateCreate |
            EObjectReplicationFlags::ReplicateDestroy |
            EObjectReplicationFlags::ReplicateAttributes;
    }

    virtual TCellTag GetReplicationCellTag(const TObjectBase* /*object*/) override
    {
        return AllSecondaryMastersCellTag;
    }

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

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObject* request,
        TRspCreateObject* response) override;

private:
    TImpl* const Owner_;

    virtual Stroka DoGetName(TUser* user) override
    {
        return Format("user %Qv", user->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TUser* user, TTransaction* transaction) override;

    virtual void DoDestroyObject(TUser* user) override;

};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TGroupTypeHandler
    : public TObjectTypeHandlerWithMapBase<TGroup>
{
public:
    explicit TGroupTypeHandler(TImpl* owner);

    virtual EObjectReplicationFlags GetReplicationFlags() const override
    {
        return
            EObjectReplicationFlags::ReplicateCreate |
            EObjectReplicationFlags::ReplicateDestroy |
            EObjectReplicationFlags::ReplicateAttributes;
    }

    virtual TCellTag GetReplicationCellTag(const TObjectBase* /*object*/) override
    {
        return AllSecondaryMastersCellTag;
    }

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

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObject* request,
        TRspCreateObject* response) override;

private:
    TImpl* const Owner_;

    virtual Stroka DoGetName(TGroup* group) override
    {
        return Format("group %Qv", group->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TGroup* group, TTransaction* transaction) override;

    virtual void DoDestroyObject(TGroup* group) override;

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
        , Config_(config)
        , RequestTracker_(New<TRequestTracker>(config, bootstrap))
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

        auto cellTag = Bootstrap_->GetPrimaryCellTag();

        SysAccountId_ = MakeWellKnownId(EObjectType::Account, cellTag, 0xffffffffffffffff);
        TmpAccountId_ = MakeWellKnownId(EObjectType::Account, cellTag, 0xfffffffffffffffe);
        IntermediateAccountId_ = MakeWellKnownId(EObjectType::Account, cellTag, 0xfffffffffffffffd);

        RootUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xffffffffffffffff);
        GuestUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffe);
        JobUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffd);
        SchedulerUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffc);

        EveryoneGroupId_ = MakeWellKnownId(EObjectType::Group, cellTag, 0xffffffffffffffff);
        UsersGroupId_ = MakeWellKnownId(EObjectType::Group, cellTag, 0xfffffffffffffffe);
        SuperusersGroupId_ = MakeWellKnownId(EObjectType::Group, cellTag, 0xfffffffffffffffd);

        RegisterMethod(BIND(&TImpl::HydraIncreaseUserStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetUserStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetAccountStatistics, Unretained(this)));
    }

    void Initialize()
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TAccountTypeHandler>(this));
        objectManager->RegisterHandler(New<TUserTypeHandler>(this));
        objectManager->RegisterHandler(New<TGroupTypeHandler>(this));

        auto multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->SubscribeSecondaryMasterRegistered(BIND(&TImpl::OnSecondaryMasterRegistered, MakeWeak(this)));
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Account, TAccount, TAccountId);
    DECLARE_ENTITY_MAP_ACCESSORS(User, TUser, TUserId);
    DECLARE_ENTITY_MAP_ACCESSORS(Group, TGroup, TGroupId);


    TAccount* CreateAccount(const Stroka& name, const TObjectId& hintId)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Account name cannot be empty");
        }

        if (FindAccountByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Account %Qv already exists",
                name);
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Account, hintId);
        return DoCreateAccount(id, name);
    }

    void DestroyAccount(TAccount* account)
    {
        YCHECK(AccountNameMap_.erase(account->GetName()) == 1);
    }

    TAccount* FindAccountByName(const Stroka& name)
    {
        auto it = AccountNameMap_.find(name);
        return it == AccountNameMap_.end() ? nullptr : it->second;
    }

    TAccount* GetAccountByNameOrThrow(const Stroka& name)
    {
        auto* account = FindAccountByName(name);
        if (!account) {
            THROW_ERROR_EXCEPTION("No such account %Qv", name);
        }
        return account;
    }


    TAccount* GetSysAccount()
    {
        YCHECK(SysAccount_);
        return SysAccount_;
    }

    TAccount* GetTmpAccount()
    {
        YCHECK(TmpAccount_);
        return TmpAccount_;
    }

    TAccount* GetIntermediateAccount()
    {
        return IntermediateAccount_;
    }


    void SetAccount(TCypressNodeBase* node, TAccount* account)
    {
        YCHECK(node);
        YCHECK(account);

        auto* oldAccount = node->GetAccount();
        if (oldAccount == account)
            return;

        auto objectManager = Bootstrap_->GetObjectManager();

        bool isAccountingEnabled = IsUncommittedAccountingEnabled(node);

        if (oldAccount) {
            if (isAccountingEnabled) {
                UpdateAccountResourceUsage(node, oldAccount, -1);
            }
            objectManager->UnrefObject(oldAccount);
        }

        node->SetAccount(account);

        UpdateNodeCachedResourceUsage(node);

        if (isAccountingEnabled) {
            UpdateAccountResourceUsage(node, account, +1);
        }
        objectManager->RefObject(account);
    }

    void ResetAccount(TCypressNodeBase* node)
    {
        auto* account = node->GetAccount();
        if (!account)
            return;

        auto objectManager = Bootstrap_->GetObjectManager();

        bool isAccountingEnabled = IsUncommittedAccountingEnabled(node);

        if (isAccountingEnabled) {
            UpdateAccountResourceUsage(node, account, -1);
        }

        node->CachedResourceUsage() = TClusterResources();
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
                "Account %Qv already exists",
                newName);
        }

        YCHECK(AccountNameMap_.erase(account->GetName()) == 1);
        YCHECK(AccountNameMap_.insert(std::make_pair(newName, account)).second);
        account->SetName(newName);
    }

    void UpdateAccountNodeUsage(TCypressNodeBase* node)
    {
        auto* account = node->GetAccount();
        if (!account)
            return;

        if (!IsUncommittedAccountingEnabled(node))
            return;

        UpdateAccountResourceUsage(node, account, -1);

        UpdateNodeCachedResourceUsage(node);

        UpdateAccountResourceUsage(node, account, +1);
    }

    void UpdateAccountStagingUsage(
        TTransaction* transaction,
        TAccount* account,
        const TClusterResources& delta)
    {
        if (!IsStagedAccountingEnabled(transaction))
            return;

        account->ClusterStatistics().ResourceUsage += delta;
        account->LocalStatistics().ResourceUsage += delta;

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
            acd->OnSubjectDestroyed(subject, GuestUser_);
        }
    }


    TUser* CreateUser(const Stroka& name, const TObjectId& hintId)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("User name cannot be empty");
        }

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

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::User, hintId);
        return DoCreateUser(id, name);
    }

    void DestroyUser(TUser* user)
    {
        YCHECK(UserNameMap_.erase(user->GetName()) == 1);
        DestroySubject(user);
    }

    TUser* FindUserByName(const Stroka& name)
    {
        auto it = UserNameMap_.find(name);
        return it == UserNameMap_.end() ? nullptr : it->second;
    }

    TUser* GetUserByNameOrThrow(const Stroka& name)
    {
        auto* user = FindUserByName(name);
        if (!IsObjectAlive(user)) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthenticationError,
                "No such user %Qv",
                name);
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
        YCHECK(RootUser_);
        return RootUser_;
    }

    TUser* GetGuestUser()
    {
        YCHECK(GuestUser_);
        return GuestUser_;
    }


    TGroup* CreateGroup(const Stroka& name, const TObjectId& hintId)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Group name cannot be empty");
        }

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

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Group, hintId);
        return DoCreateGroup(id, name);
    }

    void DestroyGroup(TGroup* group)
    {
        YCHECK(GroupNameMap_.erase(group->GetName()) == 1);

        for (auto* subject : group->Members()) {
            YCHECK(subject->MemberOf().erase(group) == 1);
        }

        DestroySubject(group);

        RecomputeMembershipClosure();
    }

    TGroup* FindGroupByName(const Stroka& name)
    {
        auto it = GroupNameMap_.find(name);
        return it == GroupNameMap_.end() ? nullptr : it->second;
    }


    TGroup* GetEveryoneGroup()
    {
        YCHECK(EveryoneGroup_);
        return EveryoneGroup_;
    }

    TGroup* GetUsersGroup()
    {
        YCHECK(UsersGroup_);
        return UsersGroup_;
    }

    TGroup* GetSuperusersGroup()
    {
        YCHECK(SuperusersGroup_);
        return SuperusersGroup_;
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
            THROW_ERROR_EXCEPTION("No such subject %Qv", name);
        }
        return subject;
    }


    void AddMember(TGroup* group, TSubject* member)
    {
        ValidateMembershipUpdate(group, member);

        if (group->Members().find(member) != group->Members().end()) {
            THROW_ERROR_EXCEPTION("Member %Qv is already present in group %Qv",
                member->GetName(),
                group->GetName());
        }

        if (member->GetType() == EObjectType::Group) {
            auto* memberGroup = member->AsGroup();
            if (group->RecursiveMemberOf().find(memberGroup) != group->RecursiveMemberOf().end()) {
                THROW_ERROR_EXCEPTION("Adding group %Qv to group %Qv would produce a cycle",
                    memberGroup->GetName(),
                    group->GetName());
            }
        }

        DoAddMember(group, member);
    }

    void RemoveMember(TGroup* group, TSubject* member)
    {
        ValidateMembershipUpdate(group, member);

        if (group->Members().find(member) == group->Members().end()) {
            THROW_ERROR_EXCEPTION("Member %Qv is not present in group %Qv",
                member->GetName(),
                group->GetName());
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
                "Subject %Qv already exists",
                newName);
        }

        switch (subject->GetType()) {
            case EObjectType::User:
                YCHECK(UserNameMap_.erase(subject->GetName()) == 1);
                YCHECK(UserNameMap_.insert(std::make_pair(newName, subject->AsUser())).second);
                break;

            case EObjectType::Group:
                YCHECK(GroupNameMap_.erase(subject->GetName()) == 1);
                YCHECK(GroupNameMap_.insert(std::make_pair(newName, subject->AsGroup())).second);
                break;

            default:
                YUNREACHABLE();
        }
        subject->SetName(newName);
    }


    EPermissionSet GetSupportedPermissions(TObjectBase* object)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        const auto& handler = objectManager->GetHandler(object);
        return handler->GetSupportedPermissions();
    }

    TAccessControlDescriptor* FindAcd(TObjectBase* object)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        const auto& handler = objectManager->GetHandler(object);
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
        auto objectManager = Bootstrap_->GetObjectManager();
        while (object) {
            const auto& handler = objectManager->GetHandler(object);
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


    void SetAuthenticatedUser(TUser* user)
    {
        AuthenticatedUser_ = user;
    }

    void ResetAuthenticatedUser()
    {
        AuthenticatedUser_ = nullptr;
    }

    TUser* GetAuthenticatedUser()
    {
        return AuthenticatedUser_ ? AuthenticatedUser_ : RootUser_;
    }


    TPermissionCheckResult CheckPermission(
        TObjectBase* object,
        TUser* user,
        EPermission permission)
    {
        TPermissionCheckResult result;

        // Fast lane: "root" needs to authorization.
        // NB: This is also useful for migration when "superusers" is initially created.
        if (user == RootUser_) {
            result.Action = ESecurityAction::Allow;
            return result;
        }

        // Fast lane: "superusers" need to authorization.
        if (user->RecursiveMemberOf().find(SuperusersGroup_) != user->RecursiveMemberOf().end()) {
            result.Action = ESecurityAction::Allow;
            return result;
        }

        // Slow lane: check ACLs through the object hierarchy.
        auto objectManager = Bootstrap_->GetObjectManager();
        auto* currentObject = object;
        while (currentObject) {
            const auto& handler = objectManager->GetHandler(currentObject);
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
                                        user->GetName(),
                                        result.Object->GetId(),
                                        result.Subject->GetName());
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
                user->GetName());
            result.Action = ESecurityAction::Deny;
            return result;
        } else {
            YASSERT(result.Action == ESecurityAction::Allow);
            LOG_TRACE_UNLESS(IsRecovery(), "Permission check succeeded: explicit allowing ACE found (CheckObjectId: %v, Permission: %v, User: %v, AclObjectId: %v, AclSubject: %v)",
                object->GetId(),
                permission,
                user->GetName(),
                result.Object->GetId(),
                result.Subject->GetName());
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
            auto objectManager = Bootstrap_->GetObjectManager();
            TError error;
            if (result.Object && result.Subject) {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission for %v is denied for %Qv by ACE at %v",
                    permission,
                    objectManager->GetHandler(object)->GetName(object),
                    result.Subject->GetName(),
                    objectManager->GetHandler(result.Object)->GetName(result.Object));
            } else {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission for %v is not allowed by any matching ACE",
                    permission,
                    objectManager->GetHandler(object)->GetName(object));
            }
            error.Attributes().Set("permission", permission);
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
        if (banned && user == RootUser_) {
            THROW_ERROR_EXCEPTION("User %Qv cannot be banned",
                user->GetName());
        }

        if (user->GetBanned() != banned) {
            user->SetBanned(banned);
            if (banned) {
                LOG_INFO_UNLESS(IsRecovery(), "User is banned (User: %v)", user->GetName());
            } else {
                LOG_INFO_UNLESS(IsRecovery(), "User is no longer banned (User: %v)", user->GetName());
            }
        }
    }

    void ValidateUserAccess(TUser* user, int requestCount)
    {
        if (user->GetBanned()) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::UserBanned,
                "User %Qv is banned",
                user->GetName());
        }

        if (user != RootUser_ && GetRequestRate(user) > user->GetRequestRateLimit()) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::RequestRateLimitExceeded,
                "User %Qv has exceeded its request rate limit",
                user->GetName())
                << TErrorAttribute("limit", user->GetRequestRateLimit());
        }

        RequestTracker_->ChargeUser(user, requestCount);
    }

    double GetRequestRate(TUser* user)
    {
        return
            TInstant::Now() > user->GetCheckpointTime() + Config_->RequestRateSmoothingPeriod
            ? 0.0
            : user->GetRequestRate();
    }

private:
    friend class TAccountTypeHandler;
    friend class TUserTypeHandler;
    friend class TGroupTypeHandler;


    const TSecurityManagerConfigPtr Config_;

    const TRequestTrackerPtr RequestTracker_;

    TPeriodicExecutorPtr AccountStatiticsGossipExecutor_;
    TPeriodicExecutorPtr UserStatiticsGossipExecutor_;

    bool InitMulticell_ = false;

    NHydra::TEntityMap<TAccountId, TAccount> AccountMap_;
    yhash_map<Stroka, TAccount*> AccountNameMap_;

    TAccountId SysAccountId_;
    TAccount* SysAccount_ = nullptr;

    TAccountId TmpAccountId_;
    TAccount* TmpAccount_ = nullptr;

    TAccountId IntermediateAccountId_;
    TAccount* IntermediateAccount_ = nullptr;

    NHydra::TEntityMap<TUserId, TUser> UserMap_;
    yhash_map<Stroka, TUser*> UserNameMap_;

    TUserId RootUserId_;
    TUser* RootUser_ = nullptr;

    TUserId GuestUserId_;
    TUser* GuestUser_ = nullptr;

    TUserId JobUserId_;
    TUser* JobUser_ = nullptr;

    TUserId SchedulerUserId_;
    TUser* SchedulerUser_ = nullptr;

    NHydra::TEntityMap<TGroupId, TGroup> GroupMap_;
    yhash_map<Stroka, TGroup*> GroupNameMap_;

    TGroupId EveryoneGroupId_;
    TGroup* EveryoneGroup_ = nullptr;

    TGroupId UsersGroupId_;
    TGroup* UsersGroup_ = nullptr;

    TGroupId SuperusersGroupId_;
    TGroup* SuperusersGroup_ = nullptr;

    TUser* AuthenticatedUser_ = nullptr;


    static bool IsUncommittedAccountingEnabled(TCypressNodeBase* node)
    {
        auto* transaction = node->GetTransaction();
        return !transaction || transaction->GetUncommittedAccountingEnabled();
    }

    static bool IsStagedAccountingEnabled(TTransaction* transaction)
    {
        return transaction->GetStagedAccountingEnabled();
    }


    void UpdateNodeCachedResourceUsage(TCypressNodeBase* node)
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto handler = cypressManager->GetHandler(node);
        node->CachedResourceUsage() = handler->GetIncrementalResourceUsage(node);
    }

    static void UpdateAccountResourceUsage(TCypressNodeBase* node, TAccount* account, int delta)
    {
        auto resourceUsage = node->CachedResourceUsage() * delta;

        account->ClusterStatistics().ResourceUsage += resourceUsage;
        account->LocalStatistics().ResourceUsage += resourceUsage;
        if (node->IsTrunk()) {
            account->ClusterStatistics().CommittedResourceUsage += resourceUsage;
            account->LocalStatistics().CommittedResourceUsage += resourceUsage;
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
            auto pair = transaction->AccountResourceUsage().insert(std::make_pair(account, TClusterResources()));
            YCHECK(pair.second);
            return &pair.first->second;
        } else {
            return &it->second;
        }
    }


    TAccount* DoCreateAccount(const TAccountId& id, const Stroka& name)
    {
        auto accountHolder = std::make_unique<TAccount>(id);
        accountHolder->SetName(name);
        // Give some reasonable initial resource limits.
        accountHolder->ClusterResourceLimits().DiskSpace = (i64) 1024 * 1024 * 1024; // 1 GB
        accountHolder->ClusterResourceLimits().NodeCount = 1000;
        accountHolder->ClusterResourceLimits().ChunkCount = 100000;

        auto* account = AccountMap_.Insert(id, std::move(accountHolder));
        YCHECK(AccountNameMap_.insert(std::make_pair(account->GetName(), account)).second);

        InitializeAccountStatistics(account);

        // Make the fake reference.
        YCHECK(account->RefObject() == 1);

        return account;
    }

    TGroup* GetBuiltinGroupForUser(TUser* user)
    {
        // "guest" is a member of "everyone" group
        // "root", "job", and "scheduler" are members of "superusers" group
        // others are members of "users" group
        const auto& id = user->GetId();
        if (id == GuestUserId_) {
            return EveryoneGroup_;
        } else if (id == RootUserId_ || id == JobUserId_ || id == SchedulerUserId_) {
            return SuperusersGroup_;
        } else {
            return UsersGroup_;
        }
    }

    TUser* DoCreateUser(const TUserId& id, const Stroka& name)
    {
        auto userHolder = std::make_unique<TUser>(id);
        userHolder->SetName(name);

        auto* user = UserMap_.Insert(id, std::move(userHolder));
        YCHECK(UserNameMap_.insert(std::make_pair(user->GetName(), user)).second);

        InitializeUserStatistics(user);

        YCHECK(user->RefObject() == 1);
        DoAddMember(GetBuiltinGroupForUser(user), user);

        return user;
    }

    TGroup* DoCreateGroup(const TGroupId& id, const Stroka& name)
    {
        auto groupHolder = std::make_unique<TGroup>(id);
        groupHolder->SetName(name);

        auto* group = GroupMap_.Insert(id, std::move(groupHolder));
        YCHECK(GroupNameMap_.insert(std::make_pair(group->GetName(), group)).second);

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
        for (const auto& pair : UserMap_) {
            pair.second->RecursiveMemberOf().clear();
        }

        for (const auto& pair : GroupMap_) {
            pair.second->RecursiveMemberOf().clear();
        }

        for (const auto& pair : GroupMap_) {
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
        if (group == EveryoneGroup_ || group == UsersGroup_) {
            THROW_ERROR_EXCEPTION("Cannot modify group");
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
        return (permissions & requestedPermission) != NonePermissions;
    }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        AccountMap_.SaveKeys(context);
        UserMap_.SaveKeys(context);
        GroupMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        AccountMap_.SaveValues(context);
        UserMap_.SaveValues(context);
        GroupMap_.SaveValues(context);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        AccountMap_.LoadKeys(context);
        UserMap_.LoadKeys(context);
        GroupMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        AccountMap_.LoadValues(context);
        UserMap_.LoadValues(context);
        GroupMap_.LoadValues(context);
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        AccountNameMap_.clear();
        for (const auto& pair : AccountMap_) {
            auto* account = pair.second;

            // Reconstruct account name map.
            YCHECK(AccountNameMap_.insert(std::make_pair(account->GetName(), account)).second);

            // Initialize statistics for this cell.
            // NB: This also provides the necessary data migration for pre-0.18 versions.
            InitializeAccountStatistics(account);
        }

        UserNameMap_.clear();
        for (const auto& pair : UserMap_) {
            auto* user = pair.second;

            // Reconstruct user name map.
            YCHECK(UserNameMap_.insert(std::make_pair(user->GetName(), user)).second);

            // Initialize statistics for this cell.
            // NB: This also provides the necessary data migration for pre-0.18 versions.
            InitializeUserStatistics(user);
        }

        GroupNameMap_.clear();
        for (const auto& pair : GroupMap_) {
            auto* group = pair.second;

            // Reconstruct group name map.
            YCHECK(GroupNameMap_.insert(std::make_pair(group->GetName(), group)).second);
        }

        InitBuiltins();
        ResetAuthenticatedUser();
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

        InitBuiltins();
        ResetAuthenticatedUser();
        InitDefaultSchemaAcds();
    }

    void InitDefaultSchemaAcds()
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        for (auto type : objectManager->GetRegisteredTypes()) {
            if (HasSchema(type)) {
                auto* schema = objectManager->GetSchema(type);
                auto* acd = GetAcd(schema);
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

    void InitBuiltins()
    {
        // Groups

        UsersGroup_ = FindGroup(UsersGroupId_);
        if (!UsersGroup_) {
            // users
            UsersGroup_ = DoCreateGroup(UsersGroupId_, UsersGroupName);
        }

        EveryoneGroup_ = FindGroup(EveryoneGroupId_);
        if (!EveryoneGroup_) {
            // everyone
            EveryoneGroup_ = DoCreateGroup(EveryoneGroupId_, EveryoneGroupName);
            DoAddMember(EveryoneGroup_, UsersGroup_);
        }

        SuperusersGroup_ = FindGroup(SuperusersGroupId_);
        if (!SuperusersGroup_) {
            // superusers
            SuperusersGroup_ = DoCreateGroup(SuperusersGroupId_, SuperusersGroupName);
            DoAddMember(UsersGroup_, SuperusersGroup_);
        }

        // Users

        RootUser_ = FindUser(RootUserId_);
        if (!RootUser_) {
            // root
            RootUser_ = DoCreateUser(RootUserId_, RootUserName);
            RootUser_->SetRequestRateLimit(1000000.0);
        }

        GuestUser_ = FindUser(GuestUserId_);
        if (!GuestUser_) {
            // guest
            GuestUser_ = DoCreateUser(GuestUserId_, GuestUserName);
        }

        JobUser_ = FindUser(JobUserId_);
        if (!JobUser_) {
            // job
            JobUser_ = DoCreateUser(JobUserId_, JobUserName);
            JobUser_->SetRequestRateLimit(1000000.0);
        }

        SchedulerUser_ = FindUser(SchedulerUserId_);
        if (!SchedulerUser_) {
            // scheduler
            SchedulerUser_ = DoCreateUser(SchedulerUserId_, SchedulerUserName);
            SchedulerUser_->SetRequestRateLimit(1000000.0);
        }

        // Accounts

        SysAccount_ = FindAccount(SysAccountId_);
        if (!SysAccount_) {
            // sys, 1 TB disk space, 100 000 nodes, 1 000 000 chunks allowed for: root
            SysAccount_ = DoCreateAccount(SysAccountId_, SysAccountName);
            SysAccount_->ClusterResourceLimits() = TClusterResources((i64) 1024 * 1024 * 1024 * 1024, 100000, 1000000000);
            SysAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                RootUser_,
                EPermission::Use));
        }

        TmpAccount_ = FindAccount(TmpAccountId_);
        if (!TmpAccount_) {
            // tmp, 1 TB disk space, 100 000 nodes, 1 000 000 chunks allowed for: users
            TmpAccount_ = DoCreateAccount(TmpAccountId_, TmpAccountName);
            TmpAccount_->ClusterResourceLimits() = TClusterResources((i64) 1024 * 1024 * 1024 * 1024, 100000, 1000000000);
            TmpAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                UsersGroup_,
                EPermission::Use));
        }

        IntermediateAccount_ = FindAccount(IntermediateAccountId_);
        if (!IntermediateAccount_) {
            // tmp, 1 TB disk space, 100 000 nodes, 1 000 000 chunks allowed for: users
            IntermediateAccount_ = DoCreateAccount(IntermediateAccountId_, IntermediateAccountName);
            IntermediateAccount_->ClusterResourceLimits() = TClusterResources((i64) 1024 * 1024 * 1024 * 1024, 100000, 1000000000);
            IntermediateAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                UsersGroup_,
                EPermission::Use));
        }
    }

protected:
    virtual void OnRecoveryComplete() override
    {
        TMasterAutomatonPart::OnRecoveryComplete();

        RequestTracker_->Start();
    }

    virtual void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        AccountStatiticsGossipExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
            BIND(&TImpl::OnAccountStatisticsGossip, MakeWeak(this)),
            Config_->AccountStatisticsGossipPeriod);
        AccountStatiticsGossipExecutor_->Start();

        UserStatiticsGossipExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
            BIND(&TImpl::OnUserStatisticsGossip, MakeWeak(this)),
            Config_->UserStatisticsGossipPeriod);
        UserStatiticsGossipExecutor_->Start();
    }

    virtual void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        RequestTracker_->Stop();

        if (AccountStatiticsGossipExecutor_) {
            AccountStatiticsGossipExecutor_->Stop();
            AccountStatiticsGossipExecutor_.Reset();
        }

        if (UserStatiticsGossipExecutor_) {
            UserStatiticsGossipExecutor_->Stop();
            UserStatiticsGossipExecutor_.Reset();
        }
    }

    virtual void OnStopFollowing() override
    {
        TMasterAutomatonPart::OnStopFollowing();

        RequestTracker_->Stop();
    }


    void InitializeAccountStatistics(TAccount* account)
    {
        auto cellTag = Bootstrap_->GetCellTag();
        const auto& secondaryCellTags = Bootstrap_->GetSecondaryCellTags();

        auto& multicellStatistics = account->MulticellStatistics();
        if (multicellStatistics.find(cellTag) == multicellStatistics.end()) {
            multicellStatistics[cellTag] = account->ClusterStatistics();
        }

        for (auto secondaryCellTag : secondaryCellTags) {
            multicellStatistics[secondaryCellTag];
        }

        account->SetLocalStatisticsPtr(&multicellStatistics[cellTag]);
    }

    void OnAccountStatisticsGossip()
    {
        LOG_INFO("Sending account statistics gossip message");


        NProto::TReqSetAccountStatistics request;
        request.set_cell_tag(Bootstrap_->GetCellTag());
        for (const auto& pair : AccountMap_) {
            auto* account = pair.second;
            if (!IsObjectAlive(account))
                continue;

            auto* entry = request.add_entries();
            ToProto(entry->mutable_account_id(), account->GetId());
            if (Bootstrap_->IsPrimaryMaster()) {
                ToProto(entry->mutable_statistics(), account->ClusterStatistics());
            } else {
                ToProto(entry->mutable_statistics(), account->LocalStatistics());
            }
        }

        auto multicellManager = Bootstrap_->GetMulticellManager();
        if (Bootstrap_->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToPrimaryMaster(request, false);
        }
    }

    void HydraSetAccountStatistics(const NProto::TReqSetAccountStatistics& request)
    {
        auto cellTag = request.cell_tag();
        LOG_INFO_UNLESS(IsRecovery(), "Received account statistics gossip message (CellTag: %v)",
            cellTag);

        YCHECK(Bootstrap_->IsPrimaryMaster() || cellTag == Bootstrap_->GetPrimaryCellTag());

        for (const auto& entry : request.entries()) {
            auto accountId = FromProto<TAccountId>(entry.account_id());
            auto* account = FindAccount(accountId);
            if (!IsObjectAlive(account))
                continue;

            auto newStatistics = FromProto<TAccountStatistics>(entry.statistics());
            if (Bootstrap_->IsPrimaryMaster()) {
                *account->GetCellStatistics(cellTag) = newStatistics;
                account->ClusterStatistics() = TAccountStatistics();
                for (const auto& pair : account->MulticellStatistics()) {
                    account->ClusterStatistics() += pair.second;
                }
            } else {
                account->ClusterStatistics() = newStatistics;
            }
        }
    }


    void InitializeUserStatistics(TUser* user)
    {
        auto cellTag = Bootstrap_->GetCellTag();
        const auto& secondaryCellTags = Bootstrap_->GetSecondaryCellTags();

        auto& multicellStatistics = user->MulticellStatistics();
        if (multicellStatistics.find(cellTag) == multicellStatistics.end()) {
            multicellStatistics[cellTag] = user->ClusterStatistics();
        }

        for (auto secondaryCellTag : secondaryCellTags) {
            multicellStatistics[secondaryCellTag];
        }

        user->SetLocalStatisticsPtr(&multicellStatistics[cellTag]);
    }

    void OnUserStatisticsGossip()
    {
        LOG_INFO("Sending user statistics gossip message");


        NProto::TReqSetUserStatistics request;
        request.set_cell_tag(Bootstrap_->GetCellTag());
        for (const auto& pair : UserMap_) {
            auto* user = pair.second;
            if (!IsObjectAlive(user))
                continue;

            auto* entry = request.add_entries();
            ToProto(entry->mutable_user_id(), user->GetId());
            if (Bootstrap_->IsPrimaryMaster()) {
                ToProto(entry->mutable_statistics(), user->ClusterStatistics());
            } else {
                ToProto(entry->mutable_statistics(), user->LocalStatistics());
            }
        }

        auto multicellManager = Bootstrap_->GetMulticellManager();
        if (Bootstrap_->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToPrimaryMaster(request, false);
        }
    }

    void HydraIncreaseUserStatistics(const NProto::TReqIncreaseUserStatistics& request)
    {
        auto* profilingManager = NProfiling::TProfileManager::Get();
        auto now = TInstant::Now();
        for (const auto& entry : request.entries()) {
            auto userId = FromProto<TUserId>(entry.user_id());
            auto* user = FindUser(userId);
            if (!IsObjectAlive(user))
                continue;

            // Update access time.
            auto statistics = FromProto<TUserStatistics>(entry.statistics());
            user->LocalStatistics() += statistics;
            user->ClusterStatistics() += statistics;

            NProfiling::TTagIdList tags;
            tags.push_back(profilingManager->RegisterTag("user", user->GetName()));

            i64 localRequestCounter = user->LocalStatistics().RequestCounter;
            Profiler.Enqueue("/user_request_counter", localRequestCounter, tags);

            // Recompute request rate.
            if (now > user->GetCheckpointTime() + Config_->RequestRateSmoothingPeriod) {
                if (user->GetCheckpointTime() != TInstant::Zero()) {
                    double requestRate =
                        static_cast<double>(localRequestCounter - user->GetCheckpointRequestCounter()) /
                        (now - user->GetCheckpointTime()).SecondsFloat();
                    user->SetRequestRate(requestRate);
                    Profiler.Enqueue("/user_request_rate", static_cast<int>(requestRate), tags);
                }
                user->SetCheckpointTime(now);
                user->SetCheckpointRequestCounter(localRequestCounter);
            }
        }
    }

    void HydraSetUserStatistics(const NProto::TReqSetUserStatistics& request)
    {
        auto cellTag = request.cell_tag();
        LOG_INFO_UNLESS(IsRecovery(), "Received user statistics gossip message (CellTag: %v)",
            cellTag);

        YCHECK(Bootstrap_->IsPrimaryMaster() || cellTag == Bootstrap_->GetPrimaryCellTag());

        for (const auto& entry : request.entries()) {
            auto userId = FromProto<TAccountId>(entry.user_id());
            auto* user = FindUser(userId);
            if (!IsObjectAlive(user))
                continue;

            auto newStatistics = FromProto<TUserStatistics>(entry.statistics());
            if (Bootstrap_->IsPrimaryMaster()) {
                user->CellStatistics(cellTag) = newStatistics;
                user->ClusterStatistics() = TUserStatistics();
                for (const auto& pair : user->MulticellStatistics()) {
                    user->ClusterStatistics() += pair.second;
                }
            } else {
                user->ClusterStatistics() = newStatistics;
            }
        }
    }


    void OnSecondaryMasterRegistered(TCellTag cellTag)
    {
        auto objectManager = Bootstrap_->GetObjectManager();

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

        auto multicellManager = Bootstrap_->GetMulticellManager();
        auto replicateMembership = [&] (TSubject* subject) {
            if (subject->IsBuiltin())
                return;

            for (auto* group : subject->MemberOf()) {
                if (!group->IsBuiltin()) {
                    auto req = TGroupYPathProxy::AddMember(GetGroupPath(group->GetName()));
                    req->set_name(subject->GetName());
                    multicellManager->PostToSecondaryMaster(req, cellTag);
                }
            }
        };
        for (auto* user : users) {
            replicateMembership(user);
        }
        for (auto* group : groups) {
            replicateMembership(group);
        }
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, Account, TAccount, TAccountId, AccountMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, User, TUser, TUserId, UserMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, Group, TGroup, TGroupId, GroupMap_)

///////////////////////////////////////////////////////////////////////////////

TSecurityManager::TAccountTypeHandler::TAccountTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->AccountMap_)
    , Owner_(owner)
{ }

TObjectBase* TSecurityManager::TAccountTypeHandler::CreateObject(
    const TObjectId& hintId,
    TTransaction* /*transaction*/,
    TAccount* /*account*/,
    IAttributeDictionary* attributes,
    TReqCreateObject* /*request*/,
    TRspCreateObject* /*response*/)
{
    auto name = attributes->Get<Stroka>("name");
    attributes->Remove("name");

    return Owner_->CreateAccount(name, hintId);
}

IObjectProxyPtr TSecurityManager::TAccountTypeHandler::DoGetProxy(
    TAccount* account,
    TTransaction* /*transaction*/)
{
    return CreateAccountProxy(Owner_->Bootstrap_, account);
}

void TSecurityManager::TAccountTypeHandler::DoDestroyObject(TAccount* account)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(account);
    Owner_->DestroyAccount(account);
}

///////////////////////////////////////////////////////////////////////////////

TSecurityManager::TUserTypeHandler::TUserTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->UserMap_)
    , Owner_(owner)
{ }

TObjectBase* TSecurityManager::TUserTypeHandler::CreateObject(
    const TObjectId& hintId,
    TTransaction* /*transaction*/,
    TAccount* /*account*/,
    IAttributeDictionary* attributes,
    TReqCreateObject* /*request*/,
    TRspCreateObject* /*response*/)
{
    auto name = attributes->Get<Stroka>("name");
    attributes->Remove("name");

    return Owner_->CreateUser(name, hintId);
}

IObjectProxyPtr TSecurityManager::TUserTypeHandler::DoGetProxy(
    TUser* user,
    TTransaction* /*transaction*/)
{
    return CreateUserProxy(Owner_->Bootstrap_, user);
}

void TSecurityManager::TUserTypeHandler::DoDestroyObject(TUser* user)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(user);
    Owner_->DestroyUser(user);
}

///////////////////////////////////////////////////////////////////////////////

TSecurityManager::TGroupTypeHandler::TGroupTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->GroupMap_)
    , Owner_(owner)
{ }

TObjectBase* TSecurityManager::TGroupTypeHandler::CreateObject(
    const TObjectId& hintId,
    TTransaction* /*transaction*/,
    TAccount* /*account*/,
    IAttributeDictionary* attributes,
    TReqCreateObject* /*request*/,
    TRspCreateObject* /*response*/)
{
    auto name = attributes->Get<Stroka>("name");
    attributes->Remove("name");

    return Owner_->CreateGroup(name, hintId);
}

IObjectProxyPtr TSecurityManager::TGroupTypeHandler::DoGetProxy(
    TGroup* group,
    TTransaction* /*transaction*/)
{
    return CreateGroupProxy(Owner_->Bootstrap_, group);
}

void TSecurityManager::TGroupTypeHandler::DoDestroyObject(TGroup* group)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(group);
    Owner_->DestroyGroup(group);
}

///////////////////////////////////////////////////////////////////////////////

TSecurityManager::TSecurityManager(
    TSecurityManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TSecurityManager::~TSecurityManager()
{ }

void TSecurityManager::Initialize()
{
    return Impl_->Initialize();
}

TAccount* TSecurityManager::FindAccountByName(const Stroka& name)
{
    return Impl_->FindAccountByName(name);
}

TAccount* TSecurityManager::GetAccountByNameOrThrow(const Stroka& name)
{
    return Impl_->GetAccountByNameOrThrow(name);
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

void TSecurityManager::SetAccount(TCypressNodeBase* node, TAccount* account)
{
    Impl_->SetAccount(node, account);
}

void TSecurityManager::ResetAccount(TCypressNodeBase* node)
{
    Impl_->ResetAccount(node);
}

void TSecurityManager::RenameAccount(TAccount* account, const Stroka& newName)
{
    Impl_->RenameAccount(account, newName);
}

void TSecurityManager::UpdateAccountNodeUsage(TCypressNodeBase* node)
{
    Impl_->UpdateAccountNodeUsage(node);
}

void TSecurityManager::UpdateAccountStagingUsage(
    TTransaction* transaction,
    TAccount* account,
    const TClusterResources& delta)
{
    Impl_->UpdateAccountStagingUsage(transaction, account, delta);
}

TUser* TSecurityManager::FindUserByName(const Stroka& name)
{
    return Impl_->FindUserByName(name);
}

TUser* TSecurityManager::GetUserByNameOrThrow(const Stroka& name)
{
    return Impl_->GetUserByNameOrThrow(name);
}

TUser* TSecurityManager::GetUserOrThrow(const TUserId& id)
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

TGroup* TSecurityManager::FindGroupByName(const Stroka& name)
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

TSubject* TSecurityManager::FindSubjectByName(const Stroka& name)
{
    return Impl_->FindSubjectByName(name);
}

TSubject* TSecurityManager::GetSubjectByNameOrThrow(const Stroka& name)
{
    return Impl_->GetSubjectByNameOrThrow(name);
}

void TSecurityManager::AddMember(TGroup* group, TSubject* member)
{
    Impl_->AddMember(group, member);
}

void TSecurityManager::RemoveMember(TGroup* group, TSubject* member)
{
    Impl_->RemoveMember(group, member);
}

void TSecurityManager::RenameSubject(TSubject* subject, const Stroka& newName)
{
    Impl_->RenameSubject(subject, newName);
}

EPermissionSet TSecurityManager::GetSupportedPermissions(TObjectBase* object)
{
    return Impl_->GetSupportedPermissions(object);
}

TAccessControlDescriptor* TSecurityManager::FindAcd(TObjectBase* object)
{
    return Impl_->FindAcd(object);
}

TAccessControlDescriptor* TSecurityManager::GetAcd(TObjectBase* object)
{
    return Impl_->GetAcd(object);
}

TAccessControlList TSecurityManager::GetEffectiveAcl(TObjectBase* object)
{
    return Impl_->GetEffectiveAcl(object);
}

void TSecurityManager::SetAuthenticatedUser(TUser* user)
{
    Impl_->SetAuthenticatedUser(user);
}

void TSecurityManager::ResetAuthenticatedUser()
{
    Impl_->ResetAuthenticatedUser();
}

TUser* TSecurityManager::GetAuthenticatedUser()
{
    return Impl_->GetAuthenticatedUser();
}

TPermissionCheckResult TSecurityManager::CheckPermission(
    TObjectBase* object,
    TUser* user,
    EPermission permission)
{
    return Impl_->CheckPermission(
        object,
        user,
        permission);
}

void TSecurityManager::ValidatePermission(
    TObjectBase* object,
    TUser* user,
    EPermission permission)
{
    Impl_->ValidatePermission(
        object,
        user,
        permission);
}

void TSecurityManager::ValidatePermission(
    TObjectBase* object,
    EPermission permission)
{
    Impl_->ValidatePermission(
        object,
        permission);
}

void TSecurityManager::SetUserBanned(TUser* user, bool banned)
{
    Impl_->SetUserBanned(user, banned);
}

void TSecurityManager::ValidateUserAccess(TUser* user, int requestCount)
{
    Impl_->ValidateUserAccess(user, requestCount);
}

double TSecurityManager::GetRequestRate(TUser* user)
{
    return Impl_->GetRequestRate(user);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, Account, TAccount, TAccountId, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, User, TUser, TUserId, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, Group, TGroup, TGroupId, *Impl_)

///////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
