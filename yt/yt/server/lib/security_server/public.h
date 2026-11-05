#pragma once

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TDetailedMasterMemory;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TPermissionCheckTarget;
struct TPermissionCheckBasicOptions;
struct TPermissionCheckResult;
struct TPermissionCheckResponse;

class TDetailedMasterMemory;

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

DEFINE_ENUM(EMasterMemoryType,
    ((Nodes)          (0))
    ((Chunks)         (1))
    ((Attributes)     (2))
    ((Tablets)        (3))
    ((Schemas)        (4))
);

////////////////////////////////////////////////////////////////////////////////

constexpr int TypicalAccessLogAttributeCount = 2;
using TAccessLogAttributes = TCompactVector<std::pair<TStringBuf, TStringBuf>, TypicalAccessLogAttributeCount>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
