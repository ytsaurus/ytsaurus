#pragma once

#include "public.h"
#include "cluster_resources.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/rpc/authentication_identity.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Describes an object (or its part) for which permission check
//! was carried out.
struct TPermissionCheckTarget
{
    NObjectServer::TObject* Object;
    std::optional<TString> Column;
};

//! Specifies additional options for permission check.
struct TPermissionCheckOptions
{
    //! If given, indicates that only a subset of columns are to affected by the operation.
    std::optional<std::vector<TString>> Columns;
};

//! Describes the result of a permission check for a single entity.
struct TPermissionCheckResult
{
    //! Was request allowed or declined?
    //! Note that this concerns the object as a whole, even if #TPermissionCheckOptions::Columns are given.
    ESecurityAction Action = ESecurityAction::Undefined;

    //! The object whose ACL contains the matching ACE.
    //! Can be |nullptr|.
    NObjectServer::TObject* Object = nullptr;

    //! Subject to which the decision applies.
    //! Can be |nullptr|.
    TSubject* Subject = nullptr;
};

//! Describes the complete response of a permission check.
//! This includes the result for the principal object and also its parts (e.g. columns).
struct TPermissionCheckResponse
    : public TPermissionCheckResult
{
    //! If TPermissionCheckOptions::Columns are given, this array contains
    //! results for individual columns.
    std::optional<std::vector<TPermissionCheckResult>> Columns;
};

////////////////////////////////////////////////////////////////////////////////

//! A simple RAII guard for setting the current authenticated user.
/*!
 *  \see #TSecurityManager::SetAuthenticatedUser
 *  \see #TSecurityManager::ResetAuthenticatedUser
 */
class TAuthenticatedUserGuard
    : public TNonCopyable
{
public:
    TAuthenticatedUserGuard(
        TSecurityManagerPtr securityManager,
        TUser* user,
        const TString& userTag = {});
    TAuthenticatedUserGuard(
        TSecurityManagerPtr securityManager,
        NRpc::TAuthenticationIdentity identity);
    explicit TAuthenticatedUserGuard(
        TSecurityManagerPtr securityManager);
    ~TAuthenticatedUserGuard();

    TUser* GetUser() const;

private:
    TUser* User_ = nullptr;
    TSecurityManagerPtr SecurityManager_;
    NRpc::TAuthenticationIdentity AuthenticationIdentity_;
    NRpc::TCurrentAuthenticationIdentityGuard AuthenticationIdentityGuard_;
};

////////////////////////////////////////////////////////////////////////////////

struct TUserWorkload
{
    EUserWorkloadType Type;
    int RequestCount;
    TDuration RequestTime;
};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
    : public TRefCounted
{
public:
    TSecurityManager(
        const TSecurityManagerConfigPtr& config,
        NCellMaster::TBootstrap* bootstrap);
    virtual ~TSecurityManager() override;

    void Initialize();

    DECLARE_ENTITY_MAP_ACCESSORS(Account, TAccount);
    DECLARE_ENTITY_MAP_ACCESSORS(User, TUser);
    DECLARE_ENTITY_MAP_ACCESSORS(Group, TGroup);
    DECLARE_ENTITY_MAP_ACCESSORS(NetworkProject, TNetworkProject);

    //! Creates an account.
    TAccount* CreateAccount(NCypressClient::TObjectId hintId = NCypressClient::NullObjectId);

    //! Returns account with a id (throws if none).
    TAccount* GetAccountOrThrow(TAccountId id);

    //! Returns account with a given name (|nullptr| if none).
    TAccount* FindAccountByName(const TString& name, bool activeLifeStageOnly);

    //! Returns account with a given name (throws if none).
    TAccount* GetAccountByNameOrThrow(const TString& name, bool activeLifeStageOnly);


    //! Returns "root" built-in account.
    TAccount* GetRootAccount();

    //! Returns "sys" built-in account.
    TAccount* GetSysAccount();

    //! Returns "tmp" built-in account.
    TAccount* GetTmpAccount();

    //! Returns "intermediate" built-in account.
    TAccount* GetIntermediateAccount();

    //! Return "chunk_wise_accounting_migration" built-in account ID.
    /*!
     *  When migrating to chunk-wise accounting, all the chunks will belong to
     *  this account for some time.
     *
     *  The account will be deprecated shortly after the migration.
     */
    TAccount* GetChunkWiseAccountingMigrationAccount();


    using TViolatedResourceLimits = TClusterResourceLimits;
    //! Returns recursive violated resource limits for account's subtree.
    TViolatedResourceLimits GetAccountRecursiveViolatedResourceLimits(const TAccount* account) const;

    //! Sets |resourceLimits| as |account|'s cluster resource limits.
    //! Throws if that would violate the invariants.
    void TrySetResourceLimits(TAccount* account, const TClusterResourceLimits& resourceLimits);

    //! Subtracts |resourceDelta| from |srcAccount| and all its ancestors up to (but not including)
    //! LCA(|srcAccount|, |dstAccount|), then adds it to |dstAccount| and all its ancestors up to
    //! (but not including) LCA(|srcAccount|, |dstAccount|).
    /*!
     * Throws if transferring resources would violate the invariants or if authenticated user lacks
     * the write permission for any of the modified accounts.
     */
    void TransferAccountResources(
        TAccount* srcAccount,
        TAccount* dstAccount,
        const TClusterResourceLimits& resourceDelta);

    //! Adds the #chunk to the resource usage of accounts mentioned in #requisition.
    void UpdateResourceUsage(
        const NChunkServer::TChunk* chunk,
        const NChunkServer::TChunkRequisition& requisition,
        i64 delta);

    //! Updates tablet-related resource usage. Only table count and static
    //! memory are used; everything else in #resourceUsageDelta must be zero.
    void UpdateTabletResourceUsage(
        NCypressServer::TCypressNode* node,
        const TClusterResources& resourceUsageDelta);

    //! Adds the #chunk to the resource usage of its staging transaction.
    void UpdateTransactionResourceUsage(
        const NChunkServer::TChunk* chunk,
        const NChunkServer::TChunkRequisition& requisition,
        i64 delta);

    void UpdateMasterMemoryUsage(NCypressServer::TCypressNode* node);

    //! Clears the transaction per-account usage statistics releasing the references to accounts.
    void ResetTransactionAccountResourceUsage(NTransactionServer::TTransaction* transaction);

    //! Recomputes the transaction per-account usage statistics from scratch.
    void RecomputeTransactionAccountResourceUsage(NTransactionServer::TTransaction* transaction);


    //! Assigns node to a given account, updates the total resource usage.
    void SetAccount(
        NCypressServer::TCypressNode* node,
        TAccount* newAccount,
        NTransactionServer::TTransaction* transaction) noexcept;

    //! Removes account association (if any) from the node.
    void ResetAccount(NCypressServer::TCypressNode* node);


    //! Returns user with a given name (|nullptr| if none).
    TUser* FindUserByName(const TString& name, bool activeLifeStageOnly);

    //! Returns user with a given name or alias (|nullptr| if none).
    TUser* FindUserByNameOrAlias(const TString& name, bool activeLifeStageOnly);

    //! Returns user with a given name (throws if none).
    TUser* GetUserByNameOrThrow(const TString& name, bool activeLifeStageOnly);

    //! Finds user by id, throws if nothing is found.
    TUser* GetUserOrThrow(TUserId id);

    //! Returns "root" built-in user.
    TUser* GetRootUser();

    //! Returns "guest" built-in user.
    TUser* GetGuestUser();

    //! Returns "owner" built-in user.
    TUser* GetOwnerUser();


    //! Returns group with a given name or alias (|nullptr| if none).
    TGroup* FindGroupByNameOrAlias(const TString& name);

    //! Returns "everyone" built-in group.
    TGroup* GetEveryoneGroup();

    //! Returns "users" built-in group.
    TGroup* GetUsersGroup();

    //! Returns "superusers" built-in group.
    TGroup* GetSuperusersGroup();


    //! Returns subject with a given id (|nullptr| if none).
    TSubject* FindSubject(TSubjectId id);

    //! Finds subject by id, throws if nothing is found.
    TSubject* GetSubjectOrThrow(TSubjectId id);

    //! Returns subject (a user or a group) with a given name or alias (|nullptr| if none).
    TSubject* FindSubjectByNameOrAlias(const TString& name, bool activeLifeStageOnly);

    //! Returns subject (a user or a group) with a given name or alias (throws if none).
    TSubject* GetSubjectByNameOrAliasOrThrow(const TString& name, bool activeLifeStageOnly);

    //! Adds a new member into the group. Throws on failure.
    void AddMember(TGroup* group, TSubject* member, bool ignoreExisting);

    //! Removes an existing member from the group. Throws on failure.
    void RemoveMember(TGroup* group, TSubject* member, bool ignoreMissing);


    //! Updates the name of the subject.
    void RenameSubject(TSubject* subject, const TString& newName);


    //! Returns network project with a given name (|nullptr| if none).
    TNetworkProject* FindNetworkProjectByName(const TString& name);

    //! Updates the name of the network project.
    void RenameNetworkProject(TNetworkProject* networkProject, const TString& newName);


    //! Returns a map from proxy role name to proxy role for proxy roles
    //! with a given proxy kind.
    const THashMap<TString, TProxyRole*>& GetProxyRolesWithProxyKind(EProxyKind proxyKind) const;


    //! Returns the object ACD or |nullptr| if access is not controlled.
    TAccessControlDescriptor* FindAcd(NObjectServer::TObject* object);

    //! Returns the object ACD. Fails if no ACD exists.
    TAccessControlDescriptor* GetAcd(NObjectServer::TObject* object);

    //! Returns the ACL obtained by combining ACLs of the object and its parents.
    //! The returned ACL is a fake one, i.e. does not exist explicitly anywhere.
    TAccessControlList GetEffectiveAcl(NObjectServer::TObject* object);

    //! Returns annotation or std::nullopt if there are no annotations available.
    std::optional<TString> GetEffectiveAnnotation(NCypressServer::TCypressNode* node);


    //! Sets the authenticated user.
    void SetAuthenticatedUser(TUser* user);

    //! Returns the current user; root if none is set.
    TUser* GetAuthenticatedUser();

    //! Resets the authenticated user.
    virtual void ResetAuthenticatedUser();


    //! Returns |true| if safe mode is active.
    bool IsSafeMode();


    //! Checks if #object ACL allows access with #permission.
    /*!
     *  NB: All permission checked are suppressed (== always succeed)
     *  when invoked from a Hive mutation (unless this is a boomerang one).
     */
    TPermissionCheckResponse CheckPermission(
        NObjectServer::TObject* object,
        TUser* user,
        EPermission permission,
        TPermissionCheckOptions options = {});

    //! Checks if given ACL allows access with #permission.
    TPermissionCheckResponse CheckPermission(
        TUser* user,
        EPermission permission,
        const TAccessControlList& acl,
        TPermissionCheckOptions options = {});

    //! Similar to #CheckPermission but throws a human-readable exception on failure.
    void ValidatePermission(
        NObjectServer::TObject* object,
        TUser* user,
        EPermission permission,
        TPermissionCheckOptions options = {});

    //! Another overload that uses the current user.
    void ValidatePermission(
        NObjectServer::TObject* object,
        EPermission permission,
        TPermissionCheckOptions options = {});

    //! If #result is denying then logs a record into a security log and throw an exception.
    void LogAndThrowAuthorizationError(
        const TPermissionCheckTarget& target,
        TUser* user,
        EPermission permission,
        const TPermissionCheckResult& result);


    //! Throws if account limit is exceeded for some resource type with positive delta.
    /*!
     *  If NHive::IsHiveMutation returns |true| then this check is suppressed.
     */
    void ValidateResourceUsageIncrease(
        TAccount* account,
        const TClusterResources& delta,
        bool allowRootAccount = false);

    //! Throws if making #childAccount a child of #parentAccount would violate the invariants.
    void ValidateAttachChildAccount(TAccount* parentAccount, TAccount* childAccount);

    //! Sets the 'AllowChildrenLimitOvercommit' flag for the specified account.
    //! Throws if setting the flag would violate the invariants.
    void SetAccountAllowChildrenLimitOvercommit(TAccount* account, bool overcommitAllowed);

    //! Sets or resets banned flag for a given user.
    void SetUserBanned(TUser* user, bool banned);

    //! Checks if request handling is possible from a given user.
    TError CheckUserAccess(TUser* user);

    //! Called when some time has been spent processing user requests.
    /*
     * For read workload, increments per-user counters.
     *
     * For write workloadThe behavior differs at leaders and at followers:
     * 1) At leaders, this increments per-user counters.
     * 2) At followers, no counters are incremented (the leader is responsible for this) but
     * the request rate throttler is acquired unconditionally.
     *
     * Also raises UserCharged signal.
     */
    void ChargeUser(
        TUser* user,
        const TUserWorkload& workload);

    //! Enforces request rate limits.
    TFuture<void> ThrottleUser(
        TUser* user,
        int requestCount,
        EUserWorkloadType workloadType);

    //! Updates the user request rate limit.
    void SetUserRequestRateLimit(TUser* user, int limit, EUserWorkloadType type);

    //! Updates the user request queue size limit.
    void SetUserRequestQueueSizeLimit(TUser* user, int limit);

    //! Updates the user request limit options.
    void SetUserRequestLimits(TUser* user, TUserRequestLimitsConfigPtr config);

    //! Attempts to increase the queue size for a given #user and validates the limit.
    //! Returns |true| on success.
    bool TryIncreaseRequestQueueSize(TUser* user);

    //! Unconditionally decreases the queue size for a given #user.
    void DecreaseRequestQueueSize(TUser* user);


    //! Returns the interned security tags registry.
    const TSecurityTagsRegistryPtr& GetSecurityTagsRegistry() const;


    //! Raised each time #ChargeUser is called.
    DECLARE_SIGNAL(void(TUser*, const TUserWorkload&), UserCharged);

    //! Set list of aliases for subject. Throws on failure.
    void SetSubjectAliases(TSubject* subject, const std::vector<TString>& aliases);

private:
    class TImpl;
    class TAccountTypeHandler;
    class TUserTypeHandler;
    class TGroupTypeHandler;
    class TNetworkProjectTypeHandler;
    class TProxyRoleTypeHandler;

    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TSecurityManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
