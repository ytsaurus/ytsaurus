#pragma once

#include "public.h"
#include "cluster_resources.h"
#include "ace_iterator.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/library/ytprof/api/api.h>

#include <yt/yt/core/rpc/authentication_identity.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

inline const NProfiling::TProfiler AccountProfiler("/accounts");

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
    //! Should be given whenever RegisterQueueConsumer permission is checked; defined vitality
    //! of the consumer to be registered.
    std::optional<bool> Vital;

    TAcdOverride FirstObjectAcdOverride;
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
 *  \see #ISecurityManager::SetAuthenticatedUser
 *  \see #ISecurityManager::ResetAuthenticatedUser
 */
class TAuthenticatedUserGuard
    : public TNonCopyable
{
public:
    TAuthenticatedUserGuard(
        ISecurityManagerPtr securityManager,
        TUser* user,
        const TString& userTag = {});
    TAuthenticatedUserGuard(
        ISecurityManagerPtr securityManager,
        NRpc::TAuthenticationIdentity identity);
    explicit TAuthenticatedUserGuard(
        ISecurityManagerPtr securityManager);
    ~TAuthenticatedUserGuard();

    TUser* GetUser() const;

private:
    TUser* User_ = nullptr;
    ISecurityManagerPtr SecurityManager_;
    NRpc::TAuthenticationIdentity AuthenticationIdentity_;
    NRpc::TCurrentAuthenticationIdentityGuard AuthenticationIdentityGuard_;
    NYTProf::TCpuProfilerTagGuard CpuProfilerTagGuard_;
};

////////////////////////////////////////////////////////////////////////////////

struct TUserWorkload
{
    EUserWorkloadType Type;
    int RequestCount;
    TDuration RequestTime;
};

////////////////////////////////////////////////////////////////////////////////

struct ISecurityManager
    : public virtual TRefCounted
{
public:
    virtual void Initialize() = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Account, TAccount);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(AccountResourceUsageLease, TAccountResourceUsageLease);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(User, TUser);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Group, TGroup);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(NetworkProject, TNetworkProject);

    DEFINE_SIGNAL(void(TUser*), UserRequestThrottlerConfigChanged);

    //! Creates an account.
    virtual TAccount* CreateAccount(NCypressClient::TObjectId hintId = NCypressClient::NullObjectId) = 0;

    //! Returns account with a id (throws if none).
    virtual TAccount* GetAccountOrThrow(TAccountId id) = 0;

    //! Returns account with a given name (|nullptr| if none).
    virtual TAccount* FindAccountByName(const TString& name, bool activeLifeStageOnly) = 0;

    //! Returns account with a given name (throws if none).
    virtual TAccount* GetAccountByNameOrThrow(const TString& name, bool activeLifeStageOnly) = 0;


    //! Returns "root" built-in account.
    virtual TAccount* GetRootAccount() = 0;

    //! Returns "sys" built-in account.
    virtual TAccount* GetSysAccount() = 0;

    //! Returns "tmp" built-in account.
    virtual TAccount* GetTmpAccount() = 0;

    //! Returns "intermediate" built-in account.
    virtual TAccount* GetIntermediateAccount() = 0;

    //! Return "chunk_wise_accounting_migration" built-in account ID.
    /*!
     *  When migrating to chunk-wise accounting, all the chunks will belong to
     *  this account for some time.
     *
     *  The account will be deprecated shortly after the migration.
     */
    virtual TAccount* GetChunkWiseAccountingMigrationAccount() = 0;

    //! Sets |resourceLimits| as |account|'s cluster resource limits.
    //! Throws if that would violate the invariants.
    virtual void TrySetResourceLimits(TAccount* account, const TClusterResourceLimits& resourceLimits) = 0;

    //! Subtracts |resourceDelta| from |srcAccount| and all its ancestors up to (but not including)
    //! LCA(|srcAccount|, |dstAccount|), then adds it to |dstAccount| and all its ancestors up to
    //! (but not including) LCA(|srcAccount|, |dstAccount|).
    /*!
     * Throws if transferring resources would violate the invariants or if authenticated user lacks
     * the write permission for any of the modified accounts.
     */
    virtual void TransferAccountResources(
        TAccount* srcAccount,
        TAccount* dstAccount,
        const TClusterResourceLimits& resourceDelta) = 0;

    //! Adds the #chunk schemas usage to the resource usage of accounts mentioned in #requisition.
    //! As a result of this, account can become strongly referenced by schema.
    virtual void UpdateChunkSchemaMasterMemoryUsage(
        const NChunkServer::TChunk* chunk,
        const NChunkServer::TChunkRequisition& requisition,
        i64 delta)  = 0;

    //! Adds the #chunk to the resource usage of accounts mentioned in #requisition.
    virtual void UpdateResourceUsage(
        const NChunkServer::TChunk* chunk,
        const NChunkServer::TChunkRequisition& requisition,
        i64 delta) = 0;

    //! Updates resources of account resource usage lease.
    virtual void UpdateAccountResourceUsageLease(
        TAccountResourceUsageLease* accountResourceUsageLease,
        const TClusterResources& resources) = 0;

    //! Updates tablet-related resource usage. Only table count and static
    //! memory are used; everything else in #resourceUsageDelta must be zero.
    virtual void UpdateTabletResourceUsage(
        NCypressServer::TCypressNode* node,
        const TClusterResources& resourceUsageDelta) = 0;

    //! Adds the #chunk to the resource usage of its staging transaction.
    virtual void UpdateTransactionResourceUsage(
        const NChunkServer::TChunk* chunk,
        const NChunkServer::TChunkRequisition& requisition,
        i64 delta) = 0;

    virtual void UpdateMasterMemoryUsage(
        NCypressServer::TCypressNode* node,
        bool accountChanged = false) = 0;

    virtual void UpdateMasterMemoryUsage(
        NTableServer::TMasterTableSchema* schema,
        TAccount* account,
        int delta) = 0;

    //! Clears the transaction per-account usage statistics releasing the references to accounts.
    virtual void ResetTransactionAccountResourceUsage(NTransactionServer::TTransaction* transaction) = 0;

    //! Recomputes the transaction per-account usage statistics from scratch.
    virtual void RecomputeTransactionAccountResourceUsage(NTransactionServer::TTransaction* transaction) = 0;


    //! Assigns node to a given account, updates the total resource usage.
    virtual void SetAccount(
        NCypressServer::TCypressNode* node,
        TAccount* newAccount,
        NTransactionServer::TTransaction* transaction) noexcept = 0;

    //! Removes account association (if any) from the node.
    virtual void ResetAccount(NCypressServer::TCypressNode* node) = 0;


    //! Returns user with a given name (|nullptr| if none).
    virtual TUser* FindUserByName(const TString& name, bool activeLifeStageOnly) = 0;

    //! Returns user with a given name or alias (|nullptr| if none).
    virtual TUser* FindUserByNameOrAlias(const TString& name, bool activeLifeStageOnly) = 0;

    //! Returns user with a given name (throws if none).
    virtual TUser* GetUserByNameOrThrow(const TString& name, bool activeLifeStageOnly) = 0;

    //! Finds user by id, throws if nothing is found.
    virtual TUser* GetUserOrThrow(TUserId id) = 0;

    //! Returns "root" built-in user.
    virtual TUser* GetRootUser() = 0;

    //! Returns "guest" built-in user.
    virtual TUser* GetGuestUser() = 0;

    //! Returns "owner" built-in user.
    virtual TUser* GetOwnerUser() = 0;


    //! Returns group with a given name or alias (|nullptr| if none).
    virtual TGroup* FindGroupByNameOrAlias(const TString& name) = 0;

    //! Returns "everyone" built-in group.
    virtual TGroup* GetEveryoneGroup() = 0;

    //! Returns "users" built-in group.
    virtual TGroup* GetUsersGroup() = 0;

    //! Returns "superusers" built-in group.
    virtual TGroup* GetSuperusersGroup() = 0;


    //! Returns subject with a given id (|nullptr| if none).
    virtual TSubject* FindSubject(TSubjectId id) = 0;

    //! Finds subject by id, throws if nothing is found.
    virtual TSubject* GetSubjectOrThrow(TSubjectId id) = 0;

    //! Returns subject (a user or a group) with a given name or alias (|nullptr| if none).
    virtual TSubject* FindSubjectByNameOrAlias(const TString& name, bool activeLifeStageOnly) = 0;

    //! Returns subject (a user or a group) with a given name or alias (throws if none).
    virtual TSubject* GetSubjectByNameOrAliasOrThrow(const TString& name, bool activeLifeStageOnly) = 0;

    //! Adds a new member into the group. Throws on failure.
    virtual void AddMember(TGroup* group, TSubject* member, bool ignoreExisting) = 0;

    //! Removes an existing member from the group. Throws on failure.
    virtual void RemoveMember(TGroup* group, TSubject* member, bool ignoreMissing) = 0;


    //! Updates the name of the subject.
    virtual void RenameSubject(TSubject* subject, const TString& newName) = 0;


    //! Returns network project with a given name (|nullptr| if none).
    virtual TNetworkProject* FindNetworkProjectByName(const TString& name) = 0;

    //! Updates the name of the network project.
    virtual void RenameNetworkProject(TNetworkProject* networkProject, const TString& newName) = 0;


    //! Returns a map from proxy role name to proxy role for proxy roles
    //! with a given proxy kind.
    virtual const THashMap<TString, TProxyRole*>& GetProxyRolesWithProxyKind(EProxyKind proxyKind) const = 0;


    //! Returns the object ACD or |nullptr| if access is not controlled.
    virtual TAccessControlDescriptor* FindAcd(NObjectServer::TObject* object) = 0;

    //! Returns the object ACD. Fails if no ACD exists.
    virtual TAccessControlDescriptor* GetAcd(NObjectServer::TObject* object) = 0;

    //! Returns the ACL obtained by combining ACLs of the object and its parents.
    //! The returned ACL is a fake one, i.e. does not exist explicitly anywhere.
    virtual TAccessControlList GetEffectiveAcl(NObjectServer::TObject* object) = 0;

    //! Sets the authenticated user.
    virtual void SetAuthenticatedUser(TUser* user) = 0;

    //! Returns the current user; root if none is set.
    virtual TUser* GetAuthenticatedUser() = 0;

    //! Resets the authenticated user.
    virtual void ResetAuthenticatedUser() = 0;


    //! Returns |true| if safe mode is active.
    virtual bool IsSafeMode() = 0;


    //! Checks if #object ACL allows access with #permission.
    /*!
     *  NB: All permission checks are suppressed (== always succeed)
     *  when invoked from a non-boomerang Hive mutation.
     */
    virtual TPermissionCheckResponse CheckPermission(
        NObjectServer::TObject* object,
        TUser* user,
        EPermission permission,
        TPermissionCheckOptions options = {}) = 0;

    //! Checks if given ACL allows access with #permission.
    virtual TPermissionCheckResponse CheckPermission(
        TUser* user,
        EPermission permission,
        const TAccessControlList& acl,
        TPermissionCheckOptions options = {}) = 0;

    //! Checks if given user is a member of superusers group.
    virtual bool IsSuperuser(const TUser* user) const = 0;

    //! Similar to #CheckPermission but throws a human-readable exception on failure.
    virtual void ValidatePermission(
        NObjectServer::TObject* object,
        TUser* user,
        EPermission permission,
        TPermissionCheckOptions options = {}) = 0;

    //! Another overload that uses the current user.
    virtual void ValidatePermission(
        NObjectServer::TObject* object,
        EPermission permission,
        TPermissionCheckOptions options = {}) = 0;

    //! If #result is denying then logs a record into a security log and throw an exception.
    virtual void LogAndThrowAuthorizationError(
        const TPermissionCheckTarget& target,
        TUser* user,
        EPermission permission,
        const TPermissionCheckResult& result) = 0;


    //! Throws if account limit is exceeded for some resource type with positive delta.
    /*!
     *  If NHive::IsHiveMutation returns |true| then this check is suppressed.
     */
    virtual void ValidateResourceUsageIncrease(
        TAccount* account,
        const TClusterResources& delta,
        bool allowRootAccount = false) = 0;

    //! Throws if making #childAccount a child of #parentAccount would violate the invariants.
    virtual void ValidateAttachChildAccount(TAccount* parentAccount, TAccount* childAccount) = 0;

    //! Sets the 'AllowChildrenLimitOvercommit' flag for the specified account.
    //! Throws if setting the flag would violate the invariants.
    virtual void SetAccountAllowChildrenLimitOvercommit(TAccount* account, bool overcommitAllowed) = 0;

    //! Sets or resets banned flag for a given user.
    virtual void SetUserBanned(TUser* user, bool banned) = 0;

    //! Checks if request handling is possible from a given user.
    virtual TError CheckUserAccess(TUser* user) = 0;

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
    virtual void ChargeUser(
        TUser* user,
        const TUserWorkload& workload) = 0;

    //! Enforces request rate limits.
    virtual TFuture<void> ThrottleUser(
        TUser* user,
        int requestCount,
        EUserWorkloadType workloadType) = 0;

    //! Updates the user request rate limit.
    virtual void SetUserRequestRateLimit(TUser* user, int limit, EUserWorkloadType type) = 0;

    //! Updates the user request queue size limit.
    virtual void SetUserRequestQueueSizeLimit(TUser* user, int limit) = 0;

    //! Updates the user request limit options.
    virtual void SetUserRequestLimits(TUser* user, TUserRequestLimitsConfigPtr config) = 0;

    //! Updates RequestQueue throttler config and fires UserRequestThrottlerConfigChanged.
    virtual void SetChunkServiceUserRequestWeightThrottlerConfig(TUser* user, const NConcurrency::TThroughputThrottlerConfigPtr& config) = 0;
    virtual void SetChunkServiceUserRequestBytesThrottlerConfig(TUser* user, const NConcurrency::TThroughputThrottlerConfigPtr& config) = 0;

    //! Attempts to increase the queue size for a given #user and validates the limit.
    //! Returns |true| on success.
    virtual bool TryIncreaseRequestQueueSize(TUser* user) = 0;

    //! Unconditionally decreases the queue size for a given #user.
    virtual void DecreaseRequestQueueSize(TUser* user) = 0;


    //! Returns the interned security tags registry.
    virtual const TSecurityTagsRegistryPtr& GetSecurityTagsRegistry() const = 0;


    //! Raised each time #ChargeUser is called.
    DECLARE_INTERFACE_SIGNAL(void(TUser*, const TUserWorkload&), UserCharged);

    //! Set list of aliases for subject. Throws on failure.
    virtual void SetSubjectAliases(TSubject* subject, const std::vector<TString>& aliases) = 0;

    //! Returns CPU profiler tag for a specific user.
    virtual NYTProf::TProfilerTagPtr GetUserCpuProfilerTag(TUser* user) = 0;

    virtual void ValidateAclSubjectTagFilters(TAccessControlList& acl) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISecurityManager)

////////////////////////////////////////////////////////////////////////////////

ISecurityManagerPtr CreateSecurityManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
