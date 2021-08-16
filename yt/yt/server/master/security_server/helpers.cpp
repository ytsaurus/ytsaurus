#include "helpers.h"

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/misc/cast.h>
#include <yt/yt/core/misc/error.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

void ValidateDiskSpace(i64 diskSpace)
{
    if (diskSpace < 0) {
        THROW_ERROR_EXCEPTION("Invalid disk space size: expected >= 0, found %v",
            diskSpace);
    }
}

////////////////////////////////////////////////////////////////////////////////

i64 GetOptionalNonNegativeI64ChildOrThrow(const NYTree::IMapNodePtr mapNode, const char* key)
{
    auto fieldNode = mapNode->FindChild(key);
    if (!fieldNode) {
        return 0;
    }

    auto result = fieldNode->AsInt64()->GetValue();
    if (result < 0) {
        THROW_ERROR_EXCEPTION("%Qv cannot be negative, found %v", key, result);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
