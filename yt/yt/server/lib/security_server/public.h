#pragma once

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TPermissionCheckTarget;
struct TPermissionCheckBasicOptions;
struct TPermissionCheckResult;
struct TPermissionCheckResponse;

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
