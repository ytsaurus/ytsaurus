#pragma once

#include "public.h"

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

struct TTotalNodeStatistics
{
    i64 AvailableSpace = 0;
    i64 UsedSpace = 0;
    int ChunkCount = 0;
    int SessionCount = 0;
    int OnlineNodeCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

Stroka ToString(const TNodeStatistics& statistics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT
