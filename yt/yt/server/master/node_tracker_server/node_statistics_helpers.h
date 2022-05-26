#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const NNodeTrackerClient::NProto::TChunkLocationStatistics& statistics,
    NYTree::TFluentMap fluent,
    const NChunkServer::IChunkManagerPtr& chunkManager);

void Serialize(
    const NNodeTrackerClient::NProto::TSlotLocationStatistics& statistics,
    NYTree::TFluentMap fluent,
    const NChunkServer::IChunkManagerPtr& chunkManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
