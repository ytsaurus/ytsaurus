#pragma once

// This header is the first intentionally.
#include <yp/server/lib/misc/public.h>

namespace NYP::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;

DEFINE_ENUM(EAccessControlAction,
    ((Allow)(1))
    ((Deny) (2))
);

DEFINE_ENUM(EAccessControlPermission,
    ((None)          (0))
    ((Read)          (1))
    ((ReadSecrets)   (8))
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

} // namespace NYP::NServer::NAccessControl
