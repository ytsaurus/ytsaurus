#pragma once

#include "public.h"

#include <core/ytree/attributes.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/schema.pb.h>

#include <core/erasure/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TRefCountedChunkSpec
    : public TIntrinsicRefCounted
    , public NProto::TChunkSpec
{
public:
    TRefCountedChunkSpec();
    TRefCountedChunkSpec(const TRefCountedChunkSpec& other);
    TRefCountedChunkSpec(TRefCountedChunkSpec&& other);

    explicit TRefCountedChunkSpec(const NProto::TChunkSpec& other);
    explicit TRefCountedChunkSpec(NProto::TChunkSpec&& other);

};

DEFINE_REFCOUNTED_TYPE(TRefCountedChunkSpec)

////////////////////////////////////////////////////////////////////////////////

bool IsUnavailable(
    const NProto::TChunkSpec& chunkSpec,
    bool checkParityParts = false);
bool IsUnavailable(
    const TChunkReplicaList& replicas,
    NErasure::ECodec codecId,
    bool checkParityParts = false);

//! Extracts various chunk statistics by first looking at
//! TSizeOverrideExt (if present) and then at TMiscExt.
void GetStatistics(
    const NProto::TChunkSpec& chunkSpec,
    i64* dataSize = nullptr,
    i64* rowCount = nullptr,
    i64* valueCount = nullptr);

i64 GetCumulativeRowCount(const std::vector<NProto::TChunkSpec>& chunkSpecs);

//! Constructs a new chunk slice removing any limits from origin.
TRefCountedChunkSpecPtr CreateCompleteChunk(TRefCountedChunkSpecPtr chunkSpec);

TChunkId EncodeChunkId(
    const NProto::TChunkSpec& chunkSpec,
    NNodeTrackerClient::TNodeId nodeId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
