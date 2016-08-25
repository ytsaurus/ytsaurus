#pragma once

#include <yt/core/misc/guid.h>
#include <yt/core/misc/intrusive_ptr.h>

namespace NYT {
namespace NShell {

///////////////////////////////////////////////////////////////////////////////

using TShellId = TGuid;

DEFINE_ENUM(EShellOperation,
    ((Spawn)     (0))
    ((Update)    (1))
    ((Poll)      (2))
    ((Terminate) (3))
);

DECLARE_REFCOUNTED_STRUCT(IShell)
DECLARE_REFCOUNTED_STRUCT(IShellManager)

///////////////////////////////////////////////////////////////////////////////

} // namespace NShell
} // namespace NYT
