#pragma once

#include "public.h"

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/orm/server/objects/public.h>

#include <yt/yt/orm/client/objects/key.h>
#include <yt/yt/orm/client/objects/object_filter.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

struct TPermissionCheckResult
{
    //! Was request allowed or declined?
    EAccessControlAction Action;

    //! The object whose ACL contains the matching ACE. Can be null.
    NObjects::TObjectKey ObjectKey;

    //! The type of object referred by #ObjectId. Can be null.
    NObjects::TObjectTypeValue ObjectType = TObjectTypeValues::Null;

    //! Subject to which the decision applies. Can be null.
    NObjects::TObjectId SubjectId;

    TAccessControlPermissionValue Permission;

    NYPath::TYPath AttributePath;

    NObjects::TObjectKey OriginObjectKey;

    NObjects::TObjectTypeValue OriginObjectType = TObjectTypeValues::Null;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TPermissionCheckResult& result,
    TStringBuf format);

using TUserIdList = std::vector<NObjects::TObjectId>;

////////////////////////////////////////////////////////////////////////////////

// TODO(dgolear): Unify with NRpc::SetCurrentAuthenticationIdentity?
bool HasAuthenticatedUser();
NRpc::TAuthenticationIdentity GetAuthenticatedUserIdentity();
std::optional<NRpc::TAuthenticationIdentity> TryGetAuthenticatedUserIdentity();
std::string TryGetAuthenticatedUserTicket();

////////////////////////////////////////////////////////////////////////////////

//! A simple RAII guard for setting the current authenticated user.
/*!
 *  \see #TAccessControlManager::SetAuthenticatedUser
 *  \see #TAccessControlManager::ResetAuthenticatedUser
 */
class TAuthenticatedUserGuard
{
public:
    TAuthenticatedUserGuard(
        TAccessControlManagerPtr accessControlManager,
        NRpc::TAuthenticationIdentity identity,
        std::string userTicket = "");
    TAuthenticatedUserGuard(const TAuthenticatedUserGuard& other) = delete;
    TAuthenticatedUserGuard(TAuthenticatedUserGuard&& other);

    ~TAuthenticatedUserGuard();

    TAuthenticatedUserGuard& operator=(const TAuthenticatedUserGuard& other) = delete;
    TAuthenticatedUserGuard& operator=(TAuthenticatedUserGuard&& other);

    TFuture<void> ThrottleUserRequest(int requestCount, i64 requestWeight, bool soft);

private:
    TAccessControlManagerPtr AccessControlManager_;
    int EnqueuedRequests_ = 0;

    std::optional<NRpc::TAuthenticationIdentity> OldUserIdentity_;
    std::string OldUserTicket_;

    void Release();
};

TAuthenticatedUserGuard MakeAuthenticatedUserGuard(
    const NRpc::IServiceContextPtr& context,
    const NAccessControl::TAccessControlManagerPtr& accessControlManager);

TAuthenticatedUserGuard MakeAuthenticatedUserGuardAndThrottle(
    const NRpc::IServiceContextPtr& context,
    const TAccessControlManagerPtr& accessControlManager,
    i64 requestWeight,
    NClient::NObjects::TTransactionId transactionId = {});

////////////////////////////////////////////////////////////////////////////////

struct TGetUserAccessAllowedToOptions
{
    std::optional<TString> ContinuationToken;
    std::optional<int> Limit;
};

struct TGetUserAccessAllowedToResult
{
    std::vector<NObjects::TObjectKey> ObjectKeys;
    TString ContinuationToken;
};

////////////////////////////////////////////////////////////////////////////////

struct TObjectPermission
{
    const NObjects::TObject* Object;
    NYPath::TYPath AttributePath;
    TAccessControlPermissionValue Permission;
};

////////////////////////////////////////////////////////////////////////////////

class TAccessControlManager
    : public TRefCounted
{
public:
    TAccessControlManager(
        NMaster::IBootstrap* bootstrap,
        const NYson::TProtobufEnumType* permissionsType,
        TAccessControlManagerConfigPtr config);
    ~TAccessControlManager();

    void Initialize();

    bool AccessControlSnapshotsPreloaded();

    bool IsSuperuser(std::string_view subjectId) const;

    //! Best-effort conversion of access control permission value to name.
    //! Does not throw even when #value is unknown.
    TString FormatPermissionValue(TAccessControlPermissionValue value) const;

    TAccessControlPermissionValue CheckedPermissionValueCast(int value) const;

    TPermissionCheckResult CheckPermission(
        std::string_view subjectId,
        const NObjects::TObject* object,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath = "",
        std::source_location location = std::source_location::current());

    std::vector<TPermissionCheckResult> CheckPermissions(
        std::string_view subjectId,
        std::vector<TObjectPermission> permissions,
        std::source_location location = std::source_location::current());

    TPermissionCheckResult CheckCachedPermission(
        std::string_view subjectId,
        NObjects::TObjectTypeValue objectType,
        const NObjects::TObjectKey& objectKey,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath = "");

    TUserIdList GetObjectAccessAllowedFor(
        const NObjects::TObject* object,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath = "",
        std::source_location location = std::source_location::current());

    TGetUserAccessAllowedToResult GetUserAccessAllowedTo(
        std::string_view userId,
        NObjects::TObjectTypeValue objectType,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath = "",
        const std::optional<NObjects::TObjectFilter>& filter = std::nullopt,
        const TGetUserAccessAllowedToOptions& options = TGetUserAccessAllowedToOptions());

    void SetAuthenticatedUser(NRpc::TAuthenticationIdentity userIdentity, std::string userTicket = "");
    void ResetAuthenticatedUser();

    const TRequestTrackerPtr& GetRequestTracker() const;

    bool ShouldPreloadAcl() const;
    void ValidatePermission(
        const NObjects::TObject* object,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath = "",
        std::source_location location = std::source_location::current());
    void ValidatePermissions(
        std::vector<TObjectPermission> requests,
        std::source_location location = std::source_location::current());

    void ValidateSuperuser(TStringBuf doWhat);

    NYTree::IYPathServicePtr CreateOrchidService();
    TAccessControlManagerConfigPtr GetConfig() const;

    const TClusterSubjectSnapshotPtr GetClusterSubjectSnapshot() const;
    const TClusterSubjectSnapshotPtr TryGetClusterSubjectSnapshot() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAccessControlManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
