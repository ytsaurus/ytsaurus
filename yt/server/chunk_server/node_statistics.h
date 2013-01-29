#pragma once

#include "public.h"
#include <server/chunk_server/chunk_service.pb.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TTotalNodeStatistics
{
    i64 AvailbaleSpace;
    i64 UsedSpace;
    int ChunkCount;
    int SessionCount;
    int OnlineNodeCount;

    TTotalNodeStatistics()
        : AvailbaleSpace(0)
        , UsedSpace(0)
        , ChunkCount(0)
        , SessionCount(0)
        , OnlineNodeCount(0)
    { }
};

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

Stroka ToString(const NYT::NChunkServer::NProto::TNodeStatistics& statistics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
