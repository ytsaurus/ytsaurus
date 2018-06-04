#pragma once

#include <yp/server/misc/public.h>

namespace NYP {
namespace NServer {
namespace NAccessControl {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;

DEFINE_ENUM(EAccessControlAction,
    ((Allow)(1))
    ((Deny) (2))
);

DEFINE_ENUM(EAccessControlPermission,
    ((Read)          (1))
    ((Write)         (2))
    ((Create)        (3))
    ((SshAccess)     (4))
    ((RootSshAccess) (5))
);

DEFINE_ENUM(EErrorCode,
    ((AuthenticationError)          (20000))
    ((AuthorizationError)           (20001))
);

struct TPermissionCheckResult;

// Built-in users.
extern const TString RootUserId;

// Built-in groups.
extern const TString SuperusersSubjectId;

// Pseudo-subjects.
extern const TString OwnerSubjectId;
extern const TString EveryoneSubjectId;

DECLARE_REFCOUNTED_CLASS(TAccessControlManagerConfig)

DECLARE_REFCOUNTED_CLASS(TAccessControlManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NAccessControl
} // namespace NServer
} // namespace NYP
