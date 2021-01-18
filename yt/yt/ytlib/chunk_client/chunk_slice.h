#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/proto/chunk_slice.pb.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/new.h>
#include <yt/core/misc/optional.h>
#include <yt/core/misc/phoenix.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): move to NTableClient.

struct TChunkSlice
{
    TReadLimit LowerLimit;
    TReadLimit UpperLimit;

    i64 DataWeight;
    i64 RowCount;
};

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TChunkSlice& slice);

////////////////////////////////////////////////////////////////////////////////

std::vector<TChunkSlice> SliceChunk(
    const NProto::TSliceRequest& sliceReq,
    const NProto::TChunkMeta& meta);

void ToProto(
    const TKeySetWriterPtr& keysWriter,
    const TKeySetWriterPtr& keyBoundsWriter,
    NProto::TChunkSlice* protoChunkSlice,
    const TChunkSlice& chunkSlice);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

