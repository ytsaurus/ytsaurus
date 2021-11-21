#pragma once

#include <yt/yt/core/misc/intrusive_ptr.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/guid.h>

namespace NYT::NShell {

////////////////////////////////////////////////////////////////////////////////

using TShellId = TGuid;

DEFINE_ENUM(EShellOperation,
    ((Spawn)     (0))
    ((Update)    (1))
    ((Poll)      (2))
    ((Terminate) (3))
);

DECLARE_REFCOUNTED_STRUCT(IShell)
DECLARE_REFCOUNTED_STRUCT(IShellManager)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((ShellExited)          (1800))
    ((ShellManagerShutDown) (1801))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
