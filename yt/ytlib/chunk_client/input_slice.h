#pragma once

#include "public.h"
#include "input_chunk.h"
#include "read_limit.h"

#include <yt/ytlib/chunk_client/chunk_slice.pb.h>

#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/misc/new.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern const int DefaultPartIndex;

////////////////////////////////////////////////////////////////////////////////

class TInputSlice
    : public TIntrinsicRefCounted
{
    DECLARE_BYVAL_RW_PROPERTY(i64, DataSize);
    DECLARE_BYVAL_RW_PROPERTY(i64, RowCount);

    DECLARE_BYVAL_RO_PROPERTY(bool, SizeOverridden);
    DECLARE_BYVAL_RO_PROPERTY(int, PartIndex);
    DECLARE_BYVAL_RO_PROPERTY(i64, MaxBlockSize);

    DEFINE_BYVAL_RO_PROPERTY(TInputChunkPtr, InputChunk);
    DEFINE_BYREF_RO_PROPERTY(TReadLimit, LowerLimit);
    DEFINE_BYREF_RO_PROPERTY(TReadLimit, UpperLimit);

public:
    TInputSlice() = default;
    TInputSlice(TInputSlice&& other) = default;

    TInputSlice(
        TInputChunkPtr inputChunk,
        const TNullable<NTableClient::TOwningKey>& lowerKey,
        const TNullable<NTableClient::TOwningKey>& upperKey);

    explicit TInputSlice(
        const TIntrusivePtr<const TInputSlice>& chunkSlice,
        const TNullable<NTableClient::TOwningKey>& lowerKey = Null,
        const TNullable<NTableClient::TOwningKey>& upperKey = Null);

    TInputSlice(
        const TIntrusivePtr<const TInputSlice>& chunkSlice,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataSize);

    TInputSlice(
        TInputChunkPtr inputChunk,
        int partIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataSize);

    TInputSlice(
        TInputChunkPtr inputChunk,
        const NProto::TChunkSlice& protoChunkSlice);

    //! Tries to split chunk slice into parts of almost equal size, about #sliceDataSize.
    std::vector<TInputSlicePtr> SliceEvenly(i64 sliceDataSize) const;

    i64 GetLocality(int replicaIndex) const;

    void Persist(NPhoenix::TPersistenceContext& context);

    friend size_t SpaceUsed(const TInputSlicePtr& chunkSlice);

private:
    int PartIndex_ = DefaultPartIndex;

    bool SizeOverridden_ = false;
    i64 DataSize_ = 0;
    i64 RowCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TInputSlice)

////////////////////////////////////////////////////////////////////////////////

//! Returns the size allocated for TInputSlice.
//! This function is used for ref counted tracking.
size_t SpaceUsed(const TInputSlicePtr& slice);

Stroka ToString(const TInputSlicePtr& slice);

////////////////////////////////////////////////////////////////////////////////

//! Constructs a new chunk slice from the chunk spec, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TInputSlicePtr CreateInputSlice(
    TInputChunkPtr inputChunk,
    const TNullable<NTableClient::TOwningKey>& lowerKey = Null,
    const TNullable<NTableClient::TOwningKey>& upperKey = Null);

//! Constructs a new chunk slice from another slice, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TInputSlicePtr CreateInputSlice(
    TInputSlicePtr inputSlice,
    const TNullable<NTableClient::TOwningKey>& lowerKey = Null,
    const TNullable<NTableClient::TOwningKey>& upperKey = Null);

TInputSlicePtr CreateInputSlice(
    TInputChunkPtr inputChunk,
    const NProto::TChunkSlice& protoChunkSlice);

//! Constructs separate chunk slice for each part of erasure chunk.
std::vector<TInputSlicePtr> CreateErasureInputSlices(
    TInputChunkPtr inputChunk,
    NErasure::ECodec codecId);

std::vector<TInputSlicePtr> SliceChunkByRowIndexes(
    TInputChunkPtr inputChunk,
    i64 sliceDataSize);

void ToProto(NProto::TChunkSpec* chunkSpec, TInputSlicePtr inputSlice);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

