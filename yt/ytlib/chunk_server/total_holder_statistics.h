#pragma once

#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TTotalHolderStatistics
{
    i64 AvailbaleSpace;
    i64 UsedSpace;
    i32 ChunkCount;
    i32 SessionCount;

    TTotalHolderStatistics()
        : AvailbaleSpace(0)
        , UsedSpace(0)
        , ChunkCount(0)
        , SessionCount(0)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
