#include "helpers.h"

#include <yt/yt/core/misc/cast.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

void ValidateDiskSpace(i64 diskSpace)
{
    if (diskSpace < 0) {
        THROW_ERROR_EXCEPTION("Invalid disk space size: expected >= 0, found %v",
            diskSpace);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
