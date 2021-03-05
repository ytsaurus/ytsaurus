#include "helpers.h"

#include <yt/yt/core/misc/error.h>

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

} // namespace NYT::NChunkServer
