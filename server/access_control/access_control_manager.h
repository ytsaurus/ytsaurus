#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yp/server/master/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/public.h>

namespace NYP::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

struct TPermissionCheckResult
{
    //! Was request allowed or declined?
    EAccessControlAction Action;

    //! The object whose ACL contains the matching ACE. Can be null.
    NObjects::TObjectId ObjectId;

    //! The type of object referred by #ObjectId. Can be null.
    NObjects::EObjectType ObjectType = NObjects::EObjectType::Null;

    //! Subject to which the decision applies. Can be null.
    NObjects::TObjectId SubjectId;
};

using TUserIdList = std::vector<NObjects::TObjectId>;

////////////////////////////////////////////////////////////////////////////////

//! A simple RAII guard for setting the current authenticated user.
/*!
 *  \see #TAccessControlManager::SetAuthenticatedUser
 *  \see #TAccessControlManager::ResetAuthenticatedUser
 */
class TAuthenticatedUserGuard
{
public:
    TAuthenticatedUserGuard(TAccessControlManagerPtr accessControlManager, const NObjects::TObjectId& userId);
    TAuthenticatedUserGuard(const TAuthenticatedUserGuard& other) = delete;
    TAuthenticatedUserGuard(TAuthenticatedUserGuard&& other);

    ~TAuthenticatedUserGuard();

    TAuthenticatedUserGuard& operator=(const TAuthenticatedUserGuard& other) = delete;
    TAuthenticatedUserGuard& operator=(TAuthenticatedUserGuard&& other);

private:
    TAccessControlManagerPtr AccessControlManager_;

private:
    void Release();
};

////////////////////////////////////////////////////////////////////////////////

struct TGetUserAccessAllowedToOptions
{
    NObjects::TObjectId ContinuationId;
    std::optional<int> Limit;
};

////////////////////////////////////////////////////////////////////////////////

class TAccessControlManager
    : public TRefCounted
{
public:
    TAccessControlManager(
        NMaster::TBootstrap* bootstrap,
        TAccessControlManagerConfigPtr config);

    void Initialize();

    TPermissionCheckResult CheckPermission(
        const NObjects::TObjectId& subjectId,
        NObjects::TObject* object,
        EAccessControlPermission permission);

    TUserIdList GetObjectAccessAllowedFor(
        NObjects::TObject* object,
        EAccessControlPermission permission);

    std::vector<NObjects::TObjectId> GetUserAccessAllowedTo(
        const NObjects::TObjectId& userId,
        NObjects::EObjectType objectType,
        EAccessControlPermission permission,
        const TGetUserAccessAllowedToOptions& options);

    void SetAuthenticatedUser(const NObjects::TObjectId& userId);
    void ResetAuthenticatedUser();
    bool HasAuthenticatedUser();
    NObjects::TObjectId GetAuthenticatedUser();

    void ValidatePermission(
        NObjects::TObject* object,
        EAccessControlPermission permission);
    void ValidateSuperuser(TStringBuf doWhat);

    NYTree::IYPathServicePtr CreateOrchidService();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAccessControlManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NAccessControl
