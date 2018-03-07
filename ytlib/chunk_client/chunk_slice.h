#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_slice.pb.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/misc/new.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// ToDo(psushin): move to NTableClient.

class TChunkSlice
{
    DEFINE_BYREF_RO_PROPERTY(TReadLimit, LowerLimit);
    DEFINE_BYREF_RO_PROPERTY(TReadLimit, UpperLimit);

    DEFINE_BYVAL_RO_PROPERTY(i64, DataWeight);
    DEFINE_BYVAL_RO_PROPERTY(i64, RowCount);

    DEFINE_BYVAL_RO_PROPERTY(bool, SizeOverridden);

public:
    TChunkSlice() = default;
    TChunkSlice(TChunkSlice&& other) = default;

    TChunkSlice(
        const NProto::TSliceRequest& sliceReq,
        const NProto::TChunkMeta& meta,
        const NTableClient::TOwningKey& lowerKey,
        const NTableClient::TOwningKey& upperKey,
        TNullable<i64> dataWeight = Null,
        TNullable<i64> rowCount = Null);

    TChunkSlice(
        const TChunkSlice& chunkSlice,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataWeight);

    TChunkSlice(
        const NProto::TSliceRequest& sliceReq,
        const NProto::TChunkMeta& meta,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataWeight);

    //! Tries to split chunk slice into parts of almost equal size, about #sliceDataSize.
    void SliceEvenly(std::vector<TChunkSlice>& result, i64 sliceDataWeight) const;

    void SetKeys(const NTableClient::TOwningKey& lowerKey, const NTableClient::TOwningKey& upperKey);
};

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TChunkSlice& slice);

////////////////////////////////////////////////////////////////////////////////

std::vector<TChunkSlice> SliceChunk(
    const NProto::TSliceRequest& sliceReq,
    const NProto::TChunkMeta& meta,
    i64 sliceDataWeight,
    int keyColumnCount,
    bool sliceByKeys);

void ToProto(NProto::TChunkSlice* protoChunkSlice, const TChunkSlice& chunkSlice);
void ToProto(
    const TKeySetWriterPtr& keysWireWriter,
    NProto::TChunkSlice* protoChunkSlice,
    const TChunkSlice& chunkSlice);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

