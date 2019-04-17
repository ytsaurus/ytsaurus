#pragma once

#include "public.h"
#include "cluster_resources.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/lib/hydra/entity_map.h>
#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/lib/security_server/security_manager.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/chunk_server/public.h>

#include <yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/server/master/table_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/core/rpc/service.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Describes an object (or its part) for which permission check
//! was carried out.
struct TPermissionCheckTarget
{
    NObjectServer::TObjectBase* Object;
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
    NObjectServer::TObjectBase* Object = nullptr;

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
    TAuthenticatedUserGuard(TSecurityManagerPtr securityManager, TUser* user);
    ~TAuthenticatedUserGuard();

private:
    TSecurityManagerPtr SecurityManager_;
};

////////////////////////////////////////////////////////////////////////////////

struct TUserWorkload
{
    EUserWorkloadType Type;
    int RequestCount;
    TDuration Time;
};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
    : public IUsersManager
{
public:
    TSecurityManager(
        TSecurityManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    virtual ~TSecurityManager() override;

    void Initialize();

    DECLARE_ENTITY_MAP_ACCESSORS(Account, TAccount);
    DECLARE_ENTITY_MAP_ACCESSORS(User, TUser);
    DECLARE_ENTITY_MAP_ACCESSORS(Group, TGroup);

    //! Returns account with a given name (|nullptr| if none).
    TAccount* FindAccountByName(const TString& name);

    //! Returns user with a given name (throws if none).
    TAccount* GetAccountByNameOrThrow(const TString& name);

    //! Returns "root" built-in account.
    TAccount* GetSysAccount();

    //! Returns "tmp" built-in account.
    TAccount* GetTmpAccount();

    //! Return "intermediate" built-in account.
    TAccount* GetIntermediateAccount();

    //! Return "chunk_wise_accounting_migration" built-in account ID.
    /*!
     *  When migrating to chunk-wise accounting, all the chunks will belong to
     *  this account for some time.
     *
     *  The account will be deprecated shortly after the migration.
     */
    TAccount* GetChunkWiseAccountingMigrationAccount();

    //! Adds the #chunk to the resource usage of accounts mentioned in #requisition.
    void UpdateResourceUsage(const NChunkServer::TChunk* chunk, const NChunkServer::TChunkRequisition& requisition, i64 delta);

    //! Updates tablet-related resource usage. Only table count and static
    //! memory are used; everything else in #resourceUsageDelta must be zero.
    void UpdateTabletResourceUsage(NCypressServer::TCypressNodeBase* node, const TClusterResources& resourceUsageDelta);

    //! Adds the #chunk to the resource usage of its staging transaction.
    void UpdateTransactionResourceUsage(const NChunkServer::TChunk* chunk, const NChunkServer::TChunkRequisition& requisition, i64 delta);

    //! Assigns node to a given account, updates the total resource usage.
    void SetAccount(
        NCypressServer::TCypressNodeBase* node,
        TAccount* oldAccount,
        TAccount* newAccount,
        NTransactionServer::TTransaction* transaction);

    //! Removes account association (if any) from the node.
    void ResetAccount(NCypressServer::TCypressNodeBase* node);

    //! Updates the name of the account.
    void RenameAccount(TAccount* account, const TString& newName);

    //! Returns user with a given name (|nullptr| if none).
    TUser* FindUserByName(const TString& name);

    //! Returns user with a given name (throws if none).
    TUser* GetUserByNameOrThrow(const TString& name);

    //! Finds user by id, throws if nothing is found.
    TUser* GetUserOrThrow(TUserId id);

    //! Returns "root" built-in user.
    TUser* GetRootUser();

    //! Returns "guest" built-in user.
    TUser* GetGuestUser();

    //! Returns "owner" built-in user.
    TUser* GetOwnerUser();


    //! Returns group with a given name (|nullptr| if none).
    TGroup* FindGroupByName(const TString& name);

    //! Returns "everyone" built-in group.
    TGroup* GetEveryoneGroup();

    //! Returns "users" built-in group.
    TGroup* GetUsersGroup();

    //! Returns "superusers" built-in group.
    TGroup* GetSuperusersGroup();


    //! Returns subject (a user or a group) with a given name (|nullptr| if none).
    TSubject* FindSubjectByName(const TString& name);

    //! Returns subject (a user or a group) with a given name (throws if none).
    TSubject* GetSubjectByNameOrThrow(const TString& name);

    //! Adds a new member into the group. Throws on failure.
    void AddMember(TGroup* group, TSubject* member, bool ignoreExisting);

    //! Removes an existing member from the group. Throws on failure.
    void RemoveMember(TGroup* group, TSubject* member, bool ignoreMissing);


    //! Updates the name of the subject.
    void RenameSubject(TSubject* subject, const TString& newName);


    //! Returns the object ACD or |nullptr| if access is not controlled.
    TAccessControlDescriptor* FindAcd(NObjectServer::TObjectBase* object);

    //! Returns the object ACD. Fails if no ACD exists.
    TAccessControlDescriptor* GetAcd(NObjectServer::TObjectBase* object);

    //! Returns the ACL obtained by combining ACLs of the object and its parents.
    //! The returned ACL is a fake one, i.e. does not exist explicitly anywhere.
    TAccessControlList GetEffectiveAcl(NObjectServer::TObjectBase* object);


    //! Sets the authenticated user.
    void SetAuthenticatedUser(TUser* user);

    //! Sets the authenticated user by user name.
    virtual void SetAuthenticatedUserByNameOrThrow(const TString& userName) override;

    //! Returns the current user or |nullptr| if there's no one.
    TUser* GetAuthenticatedUser();

    //! Returns the current user or null if there's no one.
    virtual std::optional<TString> GetAuthenticatedUserName() override;

    //! Resets the authenticated user.
    virtual void ResetAuthenticatedUser() override;


    //! Checks if #object ACL allows access with #permission.
    TPermissionCheckResponse CheckPermission(
        NObjectServer::TObjectBase* object,
        TUser* user,
        EPermission permission,
        const TPermissionCheckOptions& options = {});

    //! Checks if given ACL allows access with #permission.
    TPermissionCheckResponse CheckPermission(
        TUser* user,
        EPermission permission,
        const TAccessControlList& acl,
        const TPermissionCheckOptions& options = {});

    //! Similar to #CheckPermission but throws a human-readable exception on failure.
    /*!
     *  If NHiveServer::IsHiveMutation returns |true| then this check is suppressed.
     */
    void ValidatePermission(
        NObjectServer::TObjectBase* object,
        TUser* user,
        EPermission permission,
        const TPermissionCheckOptions& options = {});

    //! Another overload that uses the current user.
    void ValidatePermission(
        NObjectServer::TObjectBase* object,
        EPermission permission,
        const TPermissionCheckOptions& options = {});

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
    void ValidateResourceUsageIncrease(TAccount* account, const TClusterResources& delta);


    //! Sets or resets banned flag for a given user.
    void SetUserBanned(TUser* user, bool banned);

    //! Checks if request handling is possible from a given user.
    /*!
     *  Throws if the user is banned.
     */
    void ValidateUserAccess(TUser* user);

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

    //! Attempts to increase the queue size for a given #user and validates the limit.
    //! Returns |true| on success.
    bool TryIncreaseRequestQueueSize(TUser* user);

    //! Unconditionally decreases the queue size for a given #user.
    void DecreaseRequestQueueSize(TUser* user);


    //! Returns the interned security tags registry.
    const TSecurityTagsRegistryPtr& GetSecurityTagsRegistry() const;


    //! Raised each time #ChargeUser is called.
    DECLARE_SIGNAL(void(TUser*, const TUserWorkload&), UserCharged);

private:
    class TImpl;
    class TAccountTypeHandler;
    class TUserTypeHandler;
    class TGroupTypeHandler;

    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TSecurityManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
