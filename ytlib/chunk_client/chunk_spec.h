#pragma once

#include "public.h"

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ytree/attributes.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

bool IsUnavailable(
    const NProto::TChunkSpec& chunkSpec,
    bool checkParityParts = false);
bool IsUnavailable(
    const TChunkReplicaList& replicas,
    NErasure::ECodec codecId,
    bool checkParityParts = false);

i64 GetCumulativeRowCount(const std::vector<NProto::TChunkSpec>& chunkSpecs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
