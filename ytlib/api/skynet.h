#pragma once

#include "public.h"
#include "client.h"

#include <yt/core/actions/future.h>

#include <yt/ytlib/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct TSkynetSharePartsLocations
    : public TIntrinsicRefCounted
{
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TSkynetSharePartsLocations& skynetPartsLocations, NYson::IYsonConsumer* consumer);

TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
    INativeClientPtr client,
    const NYPath::TRichYPath& path,
    const TLocateSkynetShareOptions& options);

DEFINE_REFCOUNTED_TYPE(TSkynetSharePartsLocations);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
