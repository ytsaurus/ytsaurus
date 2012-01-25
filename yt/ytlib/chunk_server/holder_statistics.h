#pragma once

#include "common.h"

#include "chunk_service.pb.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TTotalHolderStatistics
{
    i64 AvailbaleSpace;
    i64 UsedSpace;
    i32 ChunkCount;
    i32 SessionCount;
    i32 AliveHolderCount;

    TTotalHolderStatistics()
        : AvailbaleSpace(0)
        , UsedSpace(0)
        , ChunkCount(0)
        , SessionCount(0)
        , AliveHolderCount(0)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const NYT::NChunkServer::NProto::THolderStatistics& statistics);

////////////////////////////////////////////////////////////////////////////////

