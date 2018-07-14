#pragma once

#include "public.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/yson/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct TSkynetSharePartsLocations
    : public TIntrinsicRefCounted
{
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
};

DEFINE_REFCOUNTED_TYPE(TSkynetSharePartsLocations);

void Serialize(
    const TSkynetSharePartsLocations& skynetPartsLocations,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
