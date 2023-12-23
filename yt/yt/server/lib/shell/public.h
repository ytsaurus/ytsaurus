#pragma once

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

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

YT_DEFINE_ERROR_ENUM(
    ((ShellExited)          (1800))
    ((ShellManagerShutDown) (1801))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShell
