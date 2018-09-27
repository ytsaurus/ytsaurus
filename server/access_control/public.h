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
    ((Use)           (6))
    ((GetQypVMStatus)(7))
);

struct TPermissionCheckResult;

DECLARE_REFCOUNTED_CLASS(TAccessControlManagerConfig)

DECLARE_REFCOUNTED_CLASS(TAccessControlManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NAccessControl
} // namespace NServer
} // namespace NYP
