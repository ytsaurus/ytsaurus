#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_slice.pb.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkSlice
{
    NChunkClient::TReadLimit LowerLimit;
    NChunkClient::TReadLimit UpperLimit;

    i64 DataWeight;
    i64 RowCount;
};

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TChunkSlice& slice);

////////////////////////////////////////////////////////////////////////////////

std::vector<TChunkSlice> SliceChunk(
    const NChunkClient::NProto::TSliceRequest& sliceReq,
    const NChunkClient::NProto::TChunkMeta& meta);

void ToProto(
    const TKeySetWriterPtr& keysWriter,
    const TKeySetWriterPtr& keyBoundsWriter,
    NChunkClient::NProto::TChunkSlice* protoChunkSlice,
    const TChunkSlice& chunkSlice);

////////////////////////////////////////////////////////////////////////////////

i64 GetChunkSliceDataWeight(
    const NChunkClient::NProto::TReqGetChunkSliceDataWeights::TChunkSlice& weightedChunkRequest,
    const NChunkClient::NProto::TChunkMeta& meta);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

