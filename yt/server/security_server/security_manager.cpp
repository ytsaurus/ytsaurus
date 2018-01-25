#include "security_manager.h"
#include "private.h"
#include "account.h"
#include "account_proxy.h"
#include "acl.h"
#include "config.h"
#include "group.h"
#include "group_proxy.h"
#include "request_tracker.h"
#include "user.h"
#include "user_proxy.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config_manager.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/cypress_server/node.h>
#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/hydra/composite_automaton.h>
#include <yt/server/hydra/entity_map.h>

#include <yt/server/object_server/type_handler_detail.h>

#include <yt/server/transaction_server/transaction.h>

#include <yt/server/hive/hive_manager.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/security_client/group_ypath_proxy.h>
#include <yt/ytlib/security_client/helpers.h>

#include <yt/core/misc/nullable.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/ypath/token.h>

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
using namespace NHiveServer;
using namespace NProfiling;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;
static const auto& Profiler = SecurityServerProfiler;

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard::TAuthenticatedUserGuard(TSecurityManagerPtr securityManager, TUser* user)
{
    if (user) {
        securityManager->SetAuthenticatedUser(user);
        SecurityManager_ = std::move(securityManager);
    }
}

TAuthenticatedUserGuard::TAuthenticatedUserGuard(TAuthenticatedUserGuard&& other)
{
    std::swap(SecurityManager_, other.SecurityManager_);
}

TAuthenticatedUserGuard::~TAuthenticatedUserGuard()
{
    if (SecurityManager_) {
        SecurityManager_->ResetAuthenticatedUser();
    }
}

TAuthenticatedUserGuard& TAuthenticatedUserGuard::operator=(TAuthenticatedUserGuard&& other)
{
    Release();
    std::swap(SecurityManager_, other.SecurityManager_);
    return *this;
}

void TAuthenticatedUserGuard::Release()
{
    if (SecurityManager_) {
        SecurityManager_->ResetAuthenticatedUser();
        SecurityManager_.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TAccountTypeHandler
    : public TObjectTypeHandlerWithMapBase<TAccount>
{
public:
    explicit TAccountTypeHandler(TImpl* owner);

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Account;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TAccount* /*object*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual TString DoGetName(const TAccount* object) override
    {
        return Format("account %Qv", object->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TAccount* account, TTransaction* transaction) override;

    virtual void DoZombifyObject(TAccount* account) override;

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

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable;
    }

    virtual TCellTagList GetReplicationCellTags(const TObjectBase* /*object*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::User;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TString DoGetName(const TUser* user) override
    {
        return Format("user %Qv", user->GetName());
    }

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
            ETypeFlags::Creatable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Group;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TGroup* /*group*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual TString DoGetName(const TGroup* group) override
    {
        return Format("group %Qv", group->GetName());
    }

    virtual TAccessControlDescriptor* DoFindAcd(TGroup* group) override
    {
        return &group->Acd();
    }

    virtual IObjectProxyPtr DoGetProxy(TGroup* group, TTransaction* transaction) override;
    virtual void DoZombifyObject(TGroup* group) override;
};

////////////////////////////////////////////////////////////////////////////////

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
        ReplicatorUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffb);
        OwnerUserId_ = MakeWellKnownId(EObjectType::User, cellTag, 0xfffffffffffffffa);

        EveryoneGroupId_ = MakeWellKnownId(EObjectType::Group, cellTag, 0xffffffffffffffff);
        UsersGroupId_ = MakeWellKnownId(EObjectType::Group, cellTag, 0xfffffffffffffffe);
        SuperusersGroupId_ = MakeWellKnownId(EObjectType::Group, cellTag, 0xfffffffffffffffd);

        RegisterMethod(BIND(&TImpl::HydraIncreaseUserStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetUserStatistics, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetAccountStatistics, Unretained(this)));
    }

    void Initialize()
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TAccountTypeHandler>(this));
        objectManager->RegisterHandler(New<TUserTypeHandler>(this));
        objectManager->RegisterHandler(New<TGroupTypeHandler>(this));

        if (Bootstrap_->IsPrimaryMaster()) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND(&TImpl::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND(&TImpl::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Account, TAccount);
    DECLARE_ENTITY_MAP_ACCESSORS(User, TUser);
    DECLARE_ENTITY_MAP_ACCESSORS(Group, TGroup);


    TAccount* CreateAccount(const TString& name, const TObjectId& hintId)
    {
        ValidateAccountName(name);

        if (FindAccountByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Account %Qv already exists",
                name);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Account, hintId);
        return DoCreateAccount(id, name);
    }

    void DestroyAccount(TAccount* account)
    {
        YCHECK(AccountNameMap_.erase(account->GetName()) == 1);
    }

    TAccount* FindAccountByName(const TString& name)
    {
        auto it = AccountNameMap_.find(name);
        return it == AccountNameMap_.end() ? nullptr : it->second;
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


    void SetAccount(TCypressNodeBase* node, TAccount* account)
    {
        YCHECK(node);
        YCHECK(account);

        auto* oldAccount = node->GetAccount();
        if (oldAccount == account) {
            return;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();

        if (oldAccount) {
            AddNodeCachedResourceUsageToAccount(node, oldAccount, -1);
            objectManager->UnrefObject(oldAccount);
        }

        node->SetAccount(account);

        UpdateNodeCachedResourceUsage(node);

        AddNodeCachedResourceUsageToAccount(node, account, +1);

        objectManager->RefObject(account);
    }

    void ResetAccount(TCypressNodeBase* node)
    {
        auto* account = node->GetAccount();
        if (!account) {
            return;
        }

        AddNodeCachedResourceUsageToAccount(node, account, -1);

        node->CachedResourceUsage() = TClusterResources();
        node->SetAccount(nullptr);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(account);
    }

    void RenameAccount(TAccount* account, const TString& newName)
    {
        ValidateAccountName(newName);

        if (newName == account->GetName()) {
            return;
        }

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

    void IncrementAccountNodeUsage(TCypressNodeBase* node, const TClusterResources& delta)
    {
        IncrementNodeCachedResourceUsage(node, delta);
    }

    void UpdateAccountNodeUsage(TCypressNodeBase* node)
    {
        auto* account = node->GetAccount();
        if (!account) {
            return;
        }

        AddNodeCachedResourceUsageToAccount(node, account, -1);

        UpdateNodeCachedResourceUsage(node);

        AddNodeCachedResourceUsageToAccount(node, account, +1);
    }

    void SetNodeResourceAccounting(TCypressNodeBase* node, bool enable)
    {
        if (node->GetAccountingEnabled() == enable) {
            return;
        }

        node->SetAccountingEnabled(enable);

        UpdateAccountNodeUsage(node);
    }

    void UpdateAccountStagingUsage(
        TTransaction* transaction,
        TAccount* account,
        const TClusterResources& delta)
    {
        if (!transaction->GetAccountingEnabled()) {
            return;
        }

        IncrementAccountResourceUsage(account, delta, false);

        auto* transactionUsage = GetTransactionAccountUsage(transaction, account);
        *transactionUsage += delta;
    }

    void DestroySubject(TSubject* subject)
    {
        for (auto* group  : subject->MemberOf()) {
            YCHECK(group->Members().erase(subject) == 1);
        }
        subject->MemberOf().clear();

        for (const auto& pair : subject->LinkedObjects()) {
            auto* acd = GetAcd(pair.first);
            acd->OnSubjectDestroyed(subject, GuestUser_);
        }
        subject->LinkedObjects().clear();
    }


    TUser* CreateUser(const TString& name, const TObjectId& hintId)
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
        auto user = DoCreateUser(id, name);
        if (user) {
            LOG_DEBUG("User %Qv created", name);
        }
        return user;
    }

    void DestroyUser(TUser* user)
    {
        YCHECK(UserNameMap_.erase(user->GetName()) == 1);
        DestroySubject(user);
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


    TGroup* CreateGroup(const TString& name, const TObjectId& hintId)
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
        auto group = DoCreateGroup(id, name);
        if (group) {
            LOG_DEBUG("Group %Qv created", name);
        }
        return group;
    }

    void DestroyGroup(TGroup* group)
    {
        YCHECK(GroupNameMap_.erase(group->GetName()) == 1);

        for (auto* subject : group->Members()) {
            YCHECK(subject->MemberOf().erase(group) == 1);
        }
        group->Members().clear();

        DestroySubject(group);

        RecomputeMembershipClosure();
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


    TSubject* FindSubjectByName(const TString& name)
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

    TSubject* GetSubjectByNameOrThrow(const TString& name)
    {
        auto* subject = FindSubjectByName(name);
        if (!IsObjectAlive(subject)) {
            THROW_ERROR_EXCEPTION("No such subject %Qv", name);
        }
        return subject;
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

        LOG_DEBUG_UNLESS(IsRecovery(), "Group member added (Group: %v, Member: %v)",
            group->GetName(),
            member->GetName());
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

        LOG_DEBUG_UNLESS(IsRecovery(), "Group member removed (Group: %v, Member: %v)",
            group->GetName(),
            member->GetName());
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
                YCHECK(UserNameMap_.erase(subject->GetName()) == 1);
                YCHECK(UserNameMap_.insert(std::make_pair(newName, subject->AsUser())).second);
                break;

            case EObjectType::Group:
                YCHECK(GroupNameMap_.erase(subject->GetName()) == 1);
                YCHECK(GroupNameMap_.insert(std::make_pair(newName, subject->AsGroup())).second);
                break;

            default:
                Y_UNREACHABLE();
        }
        subject->SetName(newName);
    }


    TAccessControlDescriptor* FindAcd(TObjectBase* object)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
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

    static TNullable<EAceInheritanceMode> GetInheritedInheritanceMode(EAceInheritanceMode mode, int depth)
    {
        auto nothing = TNullable<EAceInheritanceMode>();
        switch (mode) {
            case EAceInheritanceMode::ObjectAndDescendants:
                return EAceInheritanceMode::ObjectAndDescendants;
            case EAceInheritanceMode::ObjectOnly:
                return (depth == 0 ? EAceInheritanceMode::ObjectOnly : nothing);
            case EAceInheritanceMode::DescendantsOnly:
                return (depth > 0 ? EAceInheritanceMode::ObjectAndDescendants : nothing);
            case EAceInheritanceMode::ImmediateDescendantsOnly:
                return (depth == 1 ? EAceInheritanceMode::ObjectOnly : nothing);
        }
        Y_UNREACHABLE();
    }

    static bool CheckInheritanceMode(EAceInheritanceMode mode, int depth)
    {
        return GetInheritedInheritanceMode(mode, depth).HasValue();
    }

    bool IsUserRootOrSuperuser(const TUser* user)
    {
        // NB: This is also useful for migration when "superusers" is initially created.
        if (user == RootUser_) {
            return true;
        }

        if (user->RecursiveMemberOf().find(SuperusersGroup_) != user->RecursiveMemberOf().end()) {
            return true;
        }

        return false;
    }

    TPermissionCheckResult CheckPermission(
        TObjectBase* object,
        TUser* user,
        EPermission permission)
    {
        TPermissionCheckResult result;

        // Fast lane: "root" and "superusers" need no autorization.
        if (IsUserRootOrSuperuser(user)) {
            result.Action = ESecurityAction::Allow;
            return result;
        }

        // Fast lane: banned users are denied any permission.
        if (user->GetBanned()) {
            result.Action = ESecurityAction::Deny;
            return result;
        }

        // Slow lane: check ACLs through the object hierarchy.
        const auto& objectManager = Bootstrap_->GetObjectManager();
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

                for (const auto& ace : acd->Acl().Entries) {
                    if (!CheckInheritanceMode(ace.InheritanceMode, depth)) {
                        continue;
                    }
  
                    if (CheckPermissionMatch(ace.Permissions, permission)) {
                        for (auto* subject : ace.Subjects) {
                            auto* adjustedSubject = subject == GetOwnerUser() && owner
                                ? owner
                                : subject;
                            if (CheckSubjectMatch(adjustedSubject, user)) {
                                result.Action = ace.Action;
                                result.Object = currentObject;
                                result.Subject = subject;
                                // At least one denying ACE is found, deny the request.
                                if (result.Action == ESecurityAction::Deny) {
                                    LOG_DEBUG_UNLESS(IsRecovery(), "Permission check failed: explicit denying ACE found "
                                        "(CheckObjectId: %v, Permission: %v, User: %v, AclObjectId: %v, AclSubject: %v)",
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
            ++depth;
        }

        // No allowing ACE, deny the request.
        if (result.Action == ESecurityAction::Undefined) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Permission check failed: no matching ACE found "
                "(CheckObjectId: %v, Permission: %v, User: %v)",
                object->GetId(),
                permission,
                user->GetName());
            result.Action = ESecurityAction::Deny;
            return result;
        } else {
            Y_ASSERT(result.Action == ESecurityAction::Allow);
            LOG_TRACE_UNLESS(IsRecovery(), "Permission check succeeded: explicit allowing ACE found "
                "(CheckObjectId: %v, Permission: %v, User: %v, AclObjectId: %v, AclSubject: %v)",
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
        if (IsHiveMutation()) {
            return;
        }

        if (!IsUserRootOrSuperuser(user) &&
            permission != EPermission::Read &&
            Bootstrap_->GetConfigManager()->GetConfig()->EnableSafeMode)
        {
            THROW_ERROR_EXCEPTION("Access denied: cluster is in safe mode. "
                "Check for the announces before reporting any issues")
                << TErrorAttribute("permission", permission)
                << TErrorAttribute("user", user->GetName())
                << TErrorAttribute("object", object->GetId());
        }

        auto result = CheckPermission(object, user, permission);
        if (result.Action == ESecurityAction::Deny) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
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


    void ValidateResourceUsageIncrease(
        TAccount* account,
        const TClusterResources& delta)
    {
        if (IsHiveMutation()) {
            return;
        }

        const auto& usage = account->ClusterStatistics().ResourceUsage;
        const auto& limits = account->ClusterResourceLimits();

        for (int index = 0; index < NChunkClient::MaxMediumCount; ++index) {
            if (delta.DiskSpace[index] > 0 && usage.DiskSpace[index] + delta.DiskSpace[index] > limits.DiskSpace[index]) {
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                const auto* medium = chunkManager->GetMediumByIndex(index);
                THROW_ERROR_EXCEPTION(
                    NSecurityClient::EErrorCode::AccountLimitExceeded,
                    "Account %Qv is over disk space limit in medium %Qv",
                    account->GetName(),
                    medium->GetName())
                    << TErrorAttribute("usage", usage.DiskSpace)
                    << TErrorAttribute("limit", limits.DiskSpace);
            }
        }
        if (delta.NodeCount > 0 && usage.NodeCount + delta.NodeCount > limits.NodeCount) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AccountLimitExceeded,
                "Account %Qv is over Cypress node count limit",
                account->GetName())
                << TErrorAttribute("usage", usage.NodeCount)
                << TErrorAttribute("limit", limits.NodeCount);
        }
        if (delta.ChunkCount > 0 && usage.ChunkCount + delta.ChunkCount > limits.ChunkCount) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AccountLimitExceeded,
                "Account %Qv is over chunk count limit",
                account->GetName())
                << TErrorAttribute("usage", usage.ChunkCount)
                << TErrorAttribute("limit", limits.ChunkCount);
        }
        if (delta.TabletCount > 0 && usage.TabletCount + delta.TabletCount > limits.TabletCount) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AccountLimitExceeded,
                "Account %Qv is over tablet count limit",
                account->GetName())
                << TErrorAttribute("usage", usage.TabletCount)
                << TErrorAttribute("limit", limits.TabletCount);
        }
        if (delta.TabletStaticMemory > 0 && usage.TabletStaticMemory + delta.TabletStaticMemory > limits.TabletStaticMemory) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AccountLimitExceeded,
                "Account %Qv is over tablet static memory limit",
                account->GetName())
                << TErrorAttribute("usage", usage.TabletStaticMemory)
                << TErrorAttribute("limit", limits.TabletStaticMemory);
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
                LOG_INFO_UNLESS(IsRecovery(), "User is banned (User: %v)", user->GetName());
            } else {
                LOG_INFO_UNLESS(IsRecovery(), "User is no longer banned (User: %v)", user->GetName());
            }
        }
    }

    void ValidateUserAccess(TUser* user)
    {
        if (user->GetBanned()) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::UserBanned,
                "User %Qv is banned",
                user->GetName());
        }

        if (user == GetOwnerUser()) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthenticationError,
                "Cannot authenticate as %Qv",
                user->GetName());
        }
    }

    void ChargeUserRead(
        TUser* user,
        int requestCount,
        TDuration requestTime)
    {
        RequestTracker_->ChargeUserRead(
            user,
            requestCount,
            requestTime);
    }

    void ChargeUserWrite(
        TUser* user,
        int requestCount,
        TDuration requestTime)
    {
        RequestTracker_->ChargeUserWrite(
            user,
            requestCount,
            requestTime);
    }

    TFuture<void> ThrottleUser(TUser* user, int requestCount)
    {
        return RequestTracker_->ThrottleUser(user, requestCount);
    }

    void SetUserRequestRateLimit(TUser* user, int limit)
    {
        RequestTracker_->SetUserRequestRateLimit(user, limit);
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

private:
    friend class TAccountTypeHandler;
    friend class TUserTypeHandler;
    friend class TGroupTypeHandler;


    const TSecurityManagerConfigPtr Config_;

    const TRequestTrackerPtr RequestTracker_;

    TPeriodicExecutorPtr AccountStatisticsGossipExecutor_;
    TPeriodicExecutorPtr UserStatisticsGossipExecutor_;

    NHydra::TEntityMap<TAccount> AccountMap_;
    THashMap<TString, TAccount*> AccountNameMap_;

    TAccountId SysAccountId_;
    TAccount* SysAccount_ = nullptr;

    TAccountId TmpAccountId_;
    TAccount* TmpAccount_ = nullptr;

    TAccountId IntermediateAccountId_;
    TAccount* IntermediateAccount_ = nullptr;

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

    NHydra::TEntityMap<TGroup> GroupMap_;
    THashMap<TString, TGroup*> GroupNameMap_;

    TGroupId EveryoneGroupId_;
    TGroup* EveryoneGroup_ = nullptr;

    TGroupId UsersGroupId_;
    TGroup* UsersGroup_ = nullptr;

    TGroupId SuperusersGroupId_;
    TGroup* SuperusersGroup_ = nullptr;

    TUser* AuthenticatedUser_ = nullptr;

    // COMPAT(babenko)
    bool RecomputeNodeResourceUsage_ = false;
    bool ValidateAccountResourceUsage_ = false;
    bool RecomputeAccountResourceUsage_ = false;


    void UpdateNodeCachedResourceUsage(TCypressNodeBase* node)
    {
        if (!node->IsExternal() && node->GetAccountingEnabled()) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            const auto& handler = cypressManager->GetHandler(node);
            node->CachedResourceUsage() = handler->GetAccountingResourceUsage(node);
        } else {
            node->CachedResourceUsage() = TClusterResources();
        }
    }

    void IncrementNodeCachedResourceUsage(TCypressNodeBase* node, const TClusterResources& delta)
    {
        if (!node->IsExternal() && node->GetAccountingEnabled()) {
            node->CachedResourceUsage() += delta;
            auto* account = node->GetAccount();
            if (account) {
                IncrementAccountResourceUsage(account, delta, node->IsTrunk());
            }
        }
    }

    static void AddNodeCachedResourceUsageToAccount(TCypressNodeBase* node, TAccount* account, int delta)
    {
        auto resourceUsage = node->CachedResourceUsage() * delta;

        IncrementAccountResourceUsage(account, resourceUsage, node->IsTrunk());

        auto* transactionUsage = FindTransactionAccountUsage(node);
        if (transactionUsage) {
            *transactionUsage += resourceUsage;
        }
    }

    static void IncrementAccountResourceUsage(
        TAccount* account,
        const TClusterResources& delta,
        bool incrementCommittedResourceUsage)
    {
        account->ClusterStatistics().ResourceUsage += delta;
        account->LocalStatistics().ResourceUsage += delta;

        if (incrementCommittedResourceUsage) {
            account->ClusterStatistics().CommittedResourceUsage += delta;
            account->LocalStatistics().CommittedResourceUsage += delta;
        }

        CheckSanity(account);
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


    TAccount* DoCreateAccount(const TAccountId& id, const TString& name)
    {
        auto accountHolder = std::make_unique<TAccount>(id);
        accountHolder->SetName(name);
        // Give some reasonable initial resource limits.
        accountHolder->ClusterResourceLimits()
            .DiskSpace[NChunkServer::DefaultStoreMediumIndex] = 1_GB;
        accountHolder->ClusterResourceLimits().NodeCount = 1000;
        accountHolder->ClusterResourceLimits().ChunkCount = 100000;
        accountHolder->ClusterResourceLimits().TabletCount = 100000;

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
        // "root", "job", "scheduler", and "replicator" are members of "superusers" group
        // others are members of "users" group
        const auto& id = user->GetId();
        if (id == GuestUserId_) {
            return EveryoneGroup_;
        } else if (
            id == RootUserId_ ||
            id == JobUserId_ ||
            id == SchedulerUserId_ ||
            id == ReplicatorUserId_)
        {
            return SuperusersGroup_;
        } else {
            return UsersGroup_;
        }
    }

    TUser* DoCreateUser(const TUserId& id, const TString& name)
    {
        auto userHolder = std::make_unique<TUser>(id);
        userHolder->SetName(name);

        auto* user = UserMap_.Insert(id, std::move(userHolder));
        YCHECK(UserNameMap_.insert(std::make_pair(user->GetName(), user)).second);

        InitializeUserStatistics(user);

        YCHECK(user->RefObject() == 1);
        DoAddMember(GetBuiltinGroupForUser(user), user);

        if (!IsRecovery()) {
            RequestTracker_->ReconfigureUserRequestRateThrottler(user);
        }

        return user;
    }

    TTagId GetProfilingTagForUser(TUser* user)
    {
        auto it = UserNameToProfilingTagId_.find(user->GetName());
        if (it != UserNameToProfilingTagId_.end()) {
            return it->second;
        }

        auto tagId = TProfileManager::Get()->RegisterTag("user", user->GetName());
        YCHECK(UserNameToProfilingTagId_.insert(std::make_pair(user->GetName(), tagId)).second);
        return tagId;
    }

    TGroup* DoCreateGroup(const TGroupId& id, const TString& name)
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
                Y_UNREACHABLE();
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
        // COMPAT(savrus)
        ValidateAccountResourceUsage_ = context.GetVersion() >= 626;
        RecomputeAccountResourceUsage_ = context.GetVersion() < 626;
        RecomputeNodeResourceUsage_ = context.GetVersion() < 626;
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

        // COMPAT(babenko)
        if (RecomputeNodeResourceUsage_) {
            const auto& cypressManager = Bootstrap_->GetCypressManager();
            for (const auto& pair : cypressManager->Nodes()) {
                auto* node = pair.second;
                UpdateAccountNodeUsage(node);
            }
        }

        // COMPAT(babenko)
        if (ValidateAccountResourceUsage_ || RecomputeAccountResourceUsage_) {
            struct TStat
            {
                TClusterResources NodeUsage;
                TClusterResources NodeCommittedUsage;
                TClusterResources StagingUsage;
            };

            THashMap<TAccount*, TStat> statMap;
            for (const auto& pair : AccountMap_) {
                statMap.emplace(pair.second, TStat());
            }

            const auto& cypressManager = Bootstrap_->GetCypressManager();

            for (const auto& pair : cypressManager->Nodes()) {
                const auto* node = pair.second;
                const auto& usage = node->CachedResourceUsage();
                auto* account = node->GetAccount();
                auto& stat = statMap[account];
                stat.NodeUsage += usage;
                if (node->IsTrunk()) {
                    stat.NodeCommittedUsage += usage;
                }
            }

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            for (const auto& pair : chunkManager->Chunks()) {
                const auto* chunk = pair.second;
                if (!chunk->IsStaged() || !chunk->IsConfirmed() || chunk->IsJournal()) {
                    continue;
                }
                auto* transaction = chunk->GetStagingTransaction();
                if (!transaction->GetAccountingEnabled()) {
                    continue;
                }
                auto* account = chunk->GetStagingAccount();
                auto& stat = statMap[account];
                stat.StagingUsage += chunk->GetResourceUsage();
            }

            for (const auto& pair : statMap) {
                auto* account = pair.first;
                const auto& stat = pair.second;
                bool log = false;
                auto expectedUsage = stat.NodeUsage + stat.StagingUsage;
                auto expectedCommittedUsage = stat.NodeCommittedUsage;
                if (ValidateAccountResourceUsage_) {
                    if (account->LocalStatistics().ResourceUsage != expectedUsage) {
                        LOG_ERROR("XXX %v account usage mismatch",
                            account->GetName());
                        log = true;
                    }
                    if (account->LocalStatistics().CommittedResourceUsage != expectedCommittedUsage) {
                        LOG_ERROR("XXX %v account committed usage mismatch",
                            account->GetName());
                        log = true;
                    }
                    if (log) {
                        LOG_ERROR("XXX %v account usage %v",
                            account->GetName(),
                            account->LocalStatistics().ResourceUsage);
                        LOG_ERROR("XXX %v account committed usage %v",
                            account->GetName(),
                            account->LocalStatistics().CommittedResourceUsage);
                        LOG_ERROR("XXX %v node usage %v",
                            account->GetName(),
                            stat.NodeUsage);
                        LOG_ERROR("XXX %v node committed usage %v",
                            account->GetName(),
                            stat.NodeCommittedUsage);
                        LOG_ERROR("XXX %v staging usage %v",
                            account->GetName(),
                            stat.StagingUsage);
                    }
                }
                if (RecomputeAccountResourceUsage_) {
                    account->LocalStatistics().ResourceUsage = expectedUsage;
                    account->LocalStatistics().CommittedResourceUsage = expectedCommittedUsage;
                    if (Bootstrap_->IsPrimaryMaster()) {
                        account->ClusterStatistics() = TAccountStatistics();
                        for (const auto& pair : account->MulticellStatistics()) {
                            account->ClusterStatistics() += pair.second;
                        }
                    }
                }
            }
        }
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


        RootUser_ = nullptr;
        GuestUser_ = nullptr;
        JobUser_ = nullptr;
        SchedulerUser_ = nullptr;
        ReplicatorUser_ = nullptr;
        OwnerUser_ = nullptr;
        EveryoneGroup_ = nullptr;
        UsersGroup_ = nullptr;
        SuperusersGroup_ = nullptr;

        SysAccount_ = nullptr;
        TmpAccount_ = nullptr;
        IntermediateAccount_ = nullptr;

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
        YCHECK(builtin);
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

        // Users

        // root
        if (EnsureBuiltinUserInitialized(RootUser_, RootUserId_, RootUserName)) {
            RootUser_->SetRequestRateLimit(1000000);
            RootUser_->SetRequestQueueSizeLimit(1000000);
        }

        // guest
        EnsureBuiltinUserInitialized(GuestUser_, GuestUserId_, GuestUserName);

        if (EnsureBuiltinUserInitialized(JobUser_, JobUserId_, JobUserName)) {
            // job
            JobUser_->SetRequestRateLimit(1000000);
            JobUser_->SetRequestQueueSizeLimit(1000000);
        }

        // scheduler
        if (EnsureBuiltinUserInitialized(SchedulerUser_, SchedulerUserId_, SchedulerUserName)) {
            SchedulerUser_->SetRequestRateLimit(1000000);
            SchedulerUser_->SetRequestQueueSizeLimit(1000000);
        }

        // replicator
        if (EnsureBuiltinUserInitialized(ReplicatorUser_, ReplicatorUserId_, ReplicatorUserName)) {
            ReplicatorUser_->SetRequestRateLimit(1000000);
            ReplicatorUser_->SetRequestQueueSizeLimit(1000000);
        }

        // owner
        EnsureBuiltinUserInitialized(OwnerUser_, OwnerUserId_, OwnerUserName);

        // Accounts

        // sys, 1 TB disk space, 100 000 nodes, 1 000 000 chunks, 100 000 tablets, 10TB tablet static memory, allowed for: root
        if (EnsureBuiltinAccountInitialized(SysAccount_, SysAccountId_, SysAccountName)) {
            SysAccount_->ClusterResourceLimits() = TClusterResources()
                .SetNodeCount(100000)
                .SetChunkCount(1000000000)
                .SetTabletCount(100000)
                .SetTabletStaticMemory(10_TB)
                .SetMediumDiskSpace(NChunkServer::DefaultStoreMediumIndex, 1_TB);
            SysAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                RootUser_,
                EPermission::Use));
        }

        // tmp, 1 TB disk space, 100 000 nodes, 1 000 000 chunks allowed for: users
        if (EnsureBuiltinAccountInitialized(TmpAccount_, TmpAccountId_, TmpAccountName)) {
            TmpAccount_->ClusterResourceLimits() = TClusterResources()
                .SetNodeCount(100000)
                .SetChunkCount(1000000000)
                .SetMediumDiskSpace(NChunkServer::DefaultStoreMediumIndex, 1_TB);
            TmpAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                UsersGroup_,
                EPermission::Use));
        }

        // intermediate, 1 TB disk space, 100 000 nodes, 1 000 000 chunks allowed for: users
        if (EnsureBuiltinAccountInitialized(IntermediateAccount_, IntermediateAccountId_, IntermediateAccountName)) {
            IntermediateAccount_->ClusterResourceLimits() = TClusterResources()
                .SetNodeCount(100000)
                .SetChunkCount(1000000000)
                .SetMediumDiskSpace(NChunkServer::DefaultStoreMediumIndex, 1_TB);
            IntermediateAccount_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                UsersGroup_,
                EPermission::Use));
        }
    }

    bool EnsureBuiltinGroupInitialized(TGroup*& group, const TGroupId& id, const TString& name)
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

    bool EnsureBuiltinUserInitialized(TUser*& user, const TUserId& id, const TString& name)
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

    bool EnsureBuiltinAccountInitialized(TAccount*& account, const TAccountId& id, const TString& name)
    {
        if (account) {
            return false;
        }
        account = FindAccount(id);
        if (account) {
            return false;
        }
        account = DoCreateAccount(id, name);
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
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
            BIND(&TImpl::OnAccountStatisticsGossip, MakeWeak(this)),
            Config_->AccountStatisticsGossipPeriod);
        AccountStatisticsGossipExecutor_->Start();

        UserStatisticsGossipExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
            BIND(&TImpl::OnUserStatisticsGossip, MakeWeak(this)),
            Config_->UserStatisticsGossipPeriod);
        UserStatisticsGossipExecutor_->Start();
    }

    virtual void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        RequestTracker_->Stop();

        if (AccountStatisticsGossipExecutor_) {
            AccountStatisticsGossipExecutor_->Stop();
            AccountStatisticsGossipExecutor_.Reset();
        }

        if (UserStatisticsGossipExecutor_) {
            UserStatisticsGossipExecutor_->Stop();
            UserStatisticsGossipExecutor_.Reset();
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
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

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

        if (Bootstrap_->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToMaster(request, PrimaryMasterCellTag, false);
        }
    }

    void HydraSetAccountStatistics(NProto::TReqSetAccountStatistics* request)
    {
        auto cellTag = request->cell_tag();
        YCHECK(Bootstrap_->IsPrimaryMaster() || cellTag == Bootstrap_->GetPrimaryCellTag());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            LOG_ERROR_UNLESS(IsRecovery(), "Received account statistics gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Received account statistics gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
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
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

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

        if (Bootstrap_->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToMaster(request, PrimaryMasterCellTag, false);
        }
    }

    void HydraIncreaseUserStatistics(NProto::TReqIncreaseUserStatistics* request)
    {
        for (const auto& entry : request->entries()) {
            auto userId = FromProto<TUserId>(entry.user_id());
            auto* user = FindUser(userId);
            if (!IsObjectAlive(user))
                continue;

            // Update access time.
            auto statisticsDelta = FromProto<TUserStatistics>(entry.statistics());
            user->LocalStatistics() += statisticsDelta;
            user->ClusterStatistics() += statisticsDelta;

            TTagIdList tagIds{
                GetProfilingTagForUser(user)
            };

            const auto& localStatistics = user->LocalStatistics();
            Profiler.Enqueue("/user_read_time", localStatistics.ReadRequestTime.MicroSeconds(), EMetricType::Counter, tagIds);
            Profiler.Enqueue("/user_write_time", localStatistics.WriteRequestTime.MicroSeconds(), EMetricType::Counter, tagIds);
            Profiler.Enqueue("/user_request_count", localStatistics.RequestCount, EMetricType::Counter, tagIds);
            Profiler.Enqueue("/user_request_queue_size", user->GetRequestQueueSize(), EMetricType::Gauge, tagIds);
        }
    }

    void HydraSetUserStatistics(NProto::TReqSetUserStatistics* request)
    {
        auto cellTag = request->cell_tag();
        YCHECK(Bootstrap_->IsPrimaryMaster() || cellTag == Bootstrap_->GetPrimaryCellTag());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            LOG_ERROR_UNLESS(IsRecovery(), "Received user statistics gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Received user statistics gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
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


    static void ValidateAccountName(const TString& name)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Account name cannot be empty");
        }
    }

    static void ValidateSubjectName(const TString& name)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Subject name cannot be empty");
        }
    }


    // XXX(babenko)
    static void CheckSanity(TAccount* account)
    {
        // XXX(babenko)
        if (account->LocalStatistics().ResourceUsage.DiskSpace[NChunkClient::DefaultStoreMediumIndex] <
            account->LocalStatistics().CommittedResourceUsage.DiskSpace[NChunkClient::DefaultStoreMediumIndex])
        {
            LOG_ERROR("Unexpected error: usage < committed usage for %v", account->GetName());
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, Account, TAccount, AccountMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, User, TUser, UserMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TSecurityManager::TImpl, Group, TGroup, GroupMap_)

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TAccountTypeHandler::TAccountTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->AccountMap_)
    , Owner_(owner)
{ }

TObjectBase* TSecurityManager::TAccountTypeHandler::CreateObject(
    const TObjectId& hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<TString>("name");

    return Owner_->CreateAccount(name, hintId);
}

IObjectProxyPtr TSecurityManager::TAccountTypeHandler::DoGetProxy(
    TAccount* account,
    TTransaction* /*transaction*/)
{
    return CreateAccountProxy(Owner_->Bootstrap_, &Metadata_, account);
}

void TSecurityManager::TAccountTypeHandler::DoZombifyObject(TAccount* account)
{
    TObjectTypeHandlerWithMapBase::DoZombifyObject(account);
    Owner_->DestroyAccount(account);
}

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TUserTypeHandler::TUserTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->UserMap_)
    , Owner_(owner)
{ }

TObjectBase* TSecurityManager::TUserTypeHandler::CreateObject(
    const TObjectId& hintId,
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

TObjectBase* TSecurityManager::TGroupTypeHandler::CreateObject(
    const TObjectId& hintId,
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

TSecurityManager::TSecurityManager(
    TSecurityManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TSecurityManager::~TSecurityManager() = default;

void TSecurityManager::Initialize()
{
    return Impl_->Initialize();
}

TAccount* TSecurityManager::FindAccountByName(const TString& name)
{
    return Impl_->FindAccountByName(name);
}

TAccount* TSecurityManager::GetAccountByNameOrThrow(const TString& name)
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

void TSecurityManager::RenameAccount(TAccount* account, const TString& newName)
{
    Impl_->RenameAccount(account, newName);
}

void TSecurityManager::UpdateAccountNodeUsage(TCypressNodeBase* node)
{
    Impl_->UpdateAccountNodeUsage(node);
}

void TSecurityManager::IncrementAccountNodeUsage(TCypressNodeBase* node, const TClusterResources& delta)
{
    Impl_->IncrementAccountNodeUsage(node, delta);
}

void TSecurityManager::SetNodeResourceAccounting(NCypressServer::TCypressNodeBase* node, bool enable)
{
    Impl_->SetNodeResourceAccounting(node, enable);
}

void TSecurityManager::UpdateAccountStagingUsage(
    TTransaction* transaction,
    TAccount* account,
    const TClusterResources& delta)
{
    Impl_->UpdateAccountStagingUsage(transaction, account, delta);
}

TUser* TSecurityManager::FindUserByName(const TString& name)
{
    return Impl_->FindUserByName(name);
}

TUser* TSecurityManager::GetUserByNameOrThrow(const TString& name)
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

TSubject* TSecurityManager::FindSubjectByName(const TString& name)
{
    return Impl_->FindSubjectByName(name);
}

TSubject* TSecurityManager::GetSubjectByNameOrThrow(const TString& name)
{
    return Impl_->GetSubjectByNameOrThrow(name);
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

void TSecurityManager::ValidateResourceUsageIncrease(
    TAccount* account,
    const TClusterResources& delta)
{
    Impl_->ValidateResourceUsageIncrease(
        account,
        delta);
}

void TSecurityManager::SetUserBanned(TUser* user, bool banned)
{
    Impl_->SetUserBanned(user, banned);
}

void TSecurityManager::ValidateUserAccess(TUser* user)
{
    Impl_->ValidateUserAccess(user);
}

void TSecurityManager::ChargeUserRead(
    TUser* user,
    int requestCount,
    TDuration requestTime)
{
    Impl_->ChargeUserRead(user, requestCount, requestTime);
}

void TSecurityManager::ChargeUserWrite(
    TUser* user,
    int requestCount,
    TDuration requestTime)
{
    Impl_->ChargeUserWrite(user, requestCount, requestTime);
}

TFuture<void> TSecurityManager::ThrottleUser(TUser* user, int requestCount)
{
    return Impl_->ThrottleUser(user, requestCount);
}

void TSecurityManager::SetUserRequestRateLimit(TUser* user, int limit)
{
    Impl_->SetUserRequestRateLimit(user, limit);
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

DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, Account, TAccount, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, User, TUser, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TSecurityManager, Group, TGroup, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
