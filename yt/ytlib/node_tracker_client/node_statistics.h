#pragma once

#include "public.h"

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

struct TTotalNodeStatistics
{
    i64 AvailableSpace;
    i64 UsedSpace;
    int ChunkCount;
    int SessionCount;
    int OnlineNodeCount;

    TTotalNodeStatistics()
        : AvailableSpace(0)
        , UsedSpace(0)
        , ChunkCount(0)
        , SessionCount(0)
        , OnlineNodeCount(0)
    { }
};

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TNodeStatistics;

Stroka ToString(const TNodeStatistics& statistics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT
