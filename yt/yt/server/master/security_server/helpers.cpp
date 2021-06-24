#include "helpers.h"

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/misc/cast.h>
#include <yt/yt/core/misc/error.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void ValidateDiskSpace(i64 diskSpace)
{
    if (diskSpace < 0) {
        THROW_ERROR_EXCEPTION("Invalid disk space size: expected >= 0, found %v",
            diskSpace);
    }
}

////////////////////////////////////////////////////////////////////////////////

void SerializeClusterResources(
    const NChunkServer::TChunkManagerPtr& chunkManager,
    const TClusterResources& clusterResources,
    const TAccount* account,
    NYson::IYsonConsumer* consumer)
{
    auto resourceSerializer = New<TSerializableClusterResources>(
        chunkManager,
        clusterResources);

    if (account) {
        // Make sure medium disk space usage is serialized even if it's zero - for media with limits set.
        for (auto [mediumIndex, _] : account->ClusterResourceLimits().DiskSpace()) {
            const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
            if (!medium || medium->GetCache()) {
                continue;
            }
            resourceSerializer->AddToMediumDiskSpace(medium->GetName(), 0);
        }
    }

    BuildYsonFluently(consumer)
        .Value(resourceSerializer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
