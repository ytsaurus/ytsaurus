#pragma once

#include "public.h"
#include "input_chunk.h"
#include "read_limit.h"

#include <yt/ytlib/chunk_client/chunk_slice.pb.h>

#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/misc/new.h>
#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! A lightweight representation of NProto::TReadLimit for input slices.
struct TInputSliceLimit
{
    TInputSliceLimit() = default;
    explicit TInputSliceLimit(const TReadLimit& other);
    TInputSliceLimit(const NProto::TReadLimit& other, const NTableClient::TRowBufferPtr& rowBuffer);

    TNullable<i64> RowIndex;
    NTableClient::TKey Key;

    void MergeLowerRowIndex(i64 rowIndex);
    void MergeUpperRowIndex(i64 rowIndex);

    void MergeLowerKey(NTableClient::TKey key);
    void MergeUpperKey(NTableClient::TKey key);

    void MergeLowerLimit(const TInputSliceLimit& limit);
    void MergeUpperLimit(const TInputSliceLimit& limit);

    void Persist(const NTableClient::TPersistenceContext& context);
};

void FormatValue(TStringBuilder* builder, const TInputSliceLimit& limit, const TStringBuf& format);

bool IsTrivial(const TInputSliceLimit& limit);

void ToProto(NProto::TReadLimit* protoLimit, const TInputSliceLimit& limit);

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
    DEFINE_BYREF_RO_PROPERTY(TInputSliceLimit, LowerLimit);
    DEFINE_BYREF_RO_PROPERTY(TInputSliceLimit, UpperLimit);

public:
    TInputSlice() = default;
    TInputSlice(TInputSlice&& other) = default;

    TInputSlice(
        const TInputChunkPtr& inputChunk,
        NTableClient::TKey lowerKey = NTableClient::TKey(),
        NTableClient::TKey upperKey = NTableClient::TKey());

    explicit TInputSlice(
        const TInputSlice& inputSlice,
        NTableClient::TKey lowerKey = NTableClient::TKey(),
        NTableClient::TKey upperKey = NTableClient::TKey());

    TInputSlice(
        const TInputSlice& inputSlice,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataSize);

    TInputSlice(
        const TInputChunkPtr& inputChunk,
        int partIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataSize);

    TInputSlice(
        const TInputChunkPtr& inputChunk,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const NProto::TChunkSlice& protoChunkSlice);

    //! Tries to split chunk slice into parts of almost equal size, about #sliceDataSize.
    std::vector<TInputSlicePtr> SliceEvenly(i64 sliceDataSize) const;

    i64 GetLocality(int replicaIndex) const;

    void Persist(const NTableClient::TPersistenceContext& context);

private:
    int PartIndex_ = DefaultPartIndex;

    bool SizeOverridden_ = false;
    i64 DataSize_ = 0;
    i64 RowCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TInputSlice)

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TInputSlicePtr& slice);

bool CompareSlicesByLowerLimit(
    const TInputSlicePtr& slice1,
    const TInputSlicePtr& slice2);
bool CanMergeSlices(
    const TInputSlicePtr& slice1,
    const TInputSlicePtr& slice2);

////////////////////////////////////////////////////////////////////////////////

//! Constructs a new chunk slice from the chunk spec, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TInputSlicePtr CreateInputSlice(
    const TInputChunkPtr& inputChunk,
    NTableClient::TKey lowerKey = NTableClient::TKey(),
    NTableClient::TKey upperKey = NTableClient::TKey());

//! Constructs a new chunk slice from another slice, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TInputSlicePtr CreateInputSlice(
    const TInputSlice& inputSlice,
    NTableClient::TKey lowerKey = NTableClient::TKey(),
    NTableClient::TKey upperKey = NTableClient::TKey());

TInputSlicePtr CreateInputSlice(
    const TInputChunkPtr& inputChunk,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const NProto::TChunkSlice& protoChunkSlice);

//! Constructs separate chunk slice for each part of erasure chunk.
std::vector<TInputSlicePtr> CreateErasureInputSlices(
    const TInputChunkPtr& inputChunk,
    NErasure::ECodec codecId);

std::vector<TInputSlicePtr> SliceChunkByRowIndexes(
    const TInputChunkPtr& inputChunk,
    i64 sliceDataSize);

void ToProto(
    NProto::TChunkSpec* chunkSpec,
    const TInputSlicePtr& inputSlice);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

