#pragma once

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Describes an object (or its part) for which permission check
//! was carried out.
struct TPermissionCheckTarget
{
    NObjectClient::TObjectId ObjectId;
    std::optional<std::string> Column;
};

//! Specifies additional options for permission check.
struct TPermissionCheckBasicOptions
{
    //! If given, indicates that only a subset of columns are to affected by the operation.
    std::optional<std::vector<std::string>> Columns;
    //! Should be given whenever RegisterQueueConsumer permission is checked; defined vitality
    //! of the consumer to be registered.
    std::optional<bool> Vital;
};

//! Describes the result of a permission check for a single entity.
struct TPermissionCheckResult
{
    //! Was request allowed or declined?
    //! Note that this concerns the object as a whole, even if #TPermissionCheckBasicOptions::Columns are given.
    NSecurityClient::ESecurityAction Action = NSecurityClient::ESecurityAction::Undefined;

    //! The object whose ACL contains the matching ACE.
    NObjectClient::TObjectId ObjectId = NObjectClient::NullObjectId;

    //! Subject to which the decision applies.
    NSecurityClient::TSubjectId SubjectId = NObjectClient::NullObjectId;
};

//! Describes the complete response of a permission check.
//! This includes the result for the principal object and also its parts (e.g. columns).
struct TPermissionCheckResponse
    : public TPermissionCheckResult
{
    //! If TPermissionCheckBasicOptions::Columns are given, this array contains
    //! results for individual columns.
    std::optional<std::vector<TPermissionCheckResult>> Columns;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IResourceLimitsManager)
DECLARE_REFCOUNTED_STRUCT(IUserAccessValidator)

DECLARE_REFCOUNTED_STRUCT(TUserAccessValidatorDynamicConfig)

DEFINE_ENUM(EAccessControlEvent,
    (UserCreated)
    (GroupCreated)
    (UserDestroyed)
    (GroupDestroyed)
    (MemberAdded)
    (MemberRemoved)
    (SubjectRenamed)
    (AccessDenied)
    (ObjectAcdUpdated)
    (NetworkProjectCreated)
    (NetworkProjectDestroyed)
    (ProxyRoleCreated)
    (ProxyRoleDestroyed)
);

DEFINE_ENUM(EAccessDenialReason,
    (DeniedByAce)
    (NoAllowingAce)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
