#pragma once

#include "public.h"

#include <yt/ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

struct TTotalNodeStatistics
{
    i64 AvailableSpace = 0;
    i64 UsedSpace = 0;
    int ChunkReplicaCount = 0;

    int OnlineNodeCount = 0;
    int OfflineNodeCount = 0;
    int BannedNodeCount = 0;
    int DecommissinedNodeCount = 0;
    int WithAlertsNodeCount = 0;
    int FullNodeCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

Stroka ToString(const TNodeStatistics& statistics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT
