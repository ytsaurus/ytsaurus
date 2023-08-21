#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

bool IsUnavailable(
    const NProto::TChunkSpec& chunkSpec,
    EChunkAvailabilityPolicy policy);
bool IsUnavailable(
    const TChunkReplicaWithMediumList& replicas,
    NErasure::ECodec codecId,
    EChunkAvailabilityPolicy policy);
bool IsUnavailable(
    const TChunkReplicaList& replicas,
    NErasure::ECodec codecId,
    EChunkAvailabilityPolicy policy);

i64 GetCumulativeRowCount(
    const std::vector<NProto::TChunkSpec>& chunkSpecs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
