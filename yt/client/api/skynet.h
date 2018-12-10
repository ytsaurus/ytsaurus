#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/yson/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TSkynetSharePartsLocations
    : public TIntrinsicRefCounted
{
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
};

DEFINE_REFCOUNTED_TYPE(TSkynetSharePartsLocations)

void Serialize(
    const TSkynetSharePartsLocations& skynetPartsLocations,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
