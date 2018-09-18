#pragma once

#include "public.h"
#include "data_source.h"
#include "input_chunk.h"

#include <yt/client/chunk_client/read_limit.h>

#include <yt/ytlib/chunk_client/chunk_slice.pb.h>

#include <yt/client/table_client/unversioned_row.h>

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
    TInputSliceLimit(
        const NProto::TReadLimit& other,
        const NTableClient::TRowBufferPtr& rowBuffer,
        TRange<NTableClient::TKey> keySet);

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

TString ToString(const TInputSliceLimit& limit);

void FormatValue(TStringBuilder* builder, const TInputSliceLimit& limit, TStringBuf format);

bool IsTrivial(const TInputSliceLimit& limit);

void ToProto(NProto::TReadLimit* protoLimit, const TInputSliceLimit& limit);

////////////////////////////////////////////////////////////////////////////////

class TInputChunkSlice
    : public TIntrinsicRefCounted
{
public:
    DECLARE_BYVAL_RO_PROPERTY(i64, DataWeight);
    DECLARE_BYVAL_RO_PROPERTY(i64, RowCount);

    DECLARE_BYVAL_RO_PROPERTY(bool, SizeOverridden);
    DECLARE_BYVAL_RO_PROPERTY(int, PartIndex);
    DECLARE_BYVAL_RO_PROPERTY(i64, MaxBlockSize);

    DEFINE_BYVAL_RW_PROPERTY(TInputChunkPtr, InputChunk);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, LowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, UpperLimit);

public:
    TInputChunkSlice() = default;
    TInputChunkSlice(TInputChunkSlice&& other) = default;

    explicit TInputChunkSlice(
        const TInputChunkPtr& inputChunk,
        NTableClient::TKey lowerKey = NTableClient::TKey(),
        NTableClient::TKey upperKey = NTableClient::TKey());

    explicit TInputChunkSlice(
        const TInputChunkSlice& inputSlice,
        NTableClient::TKey lowerKey = NTableClient::TKey(),
        NTableClient::TKey upperKey = NTableClient::TKey());

    TInputChunkSlice(
        const TInputChunkSlice& inputSlice,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataSize);

    TInputChunkSlice(
        const TInputChunkPtr& inputChunk,
        int partIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataSize);

    TInputChunkSlice(
        const TInputChunkPtr& inputChunk,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const NProto::TChunkSlice& protoChunkSlice,
        TRange<NTableClient::TKey> keySet);

    TInputChunkSlice(
        const TInputChunkPtr& inputChunk,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const NProto::TChunkSpec& protoChunkSpec);

    //! Tries to split chunk slice into parts of almost equal size, about #sliceDataSize.
    //! If #rowBuffer is given, also capture
    std::vector<TInputChunkSlicePtr> SliceEvenly(
        i64 sliceDataWeight,
        i64 sliceRowCount,
        NTableClient::TRowBufferPtr rowBuffer = nullptr) const;
    std::pair<TInputChunkSlicePtr, TInputChunkSlicePtr>  SplitByRowIndex(i64 splitRow) const;

    i64 GetLocality(int replicaIndex) const;

    void Persist(const NTableClient::TPersistenceContext& context);

    void OverrideSize(i64 rowCount, i64 dataWeight);

private:
    int PartIndex_ = DefaultPartIndex;

    bool SizeOverridden_ = false;
    i64 DataWeight_ = 0;
    i64 RowCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TInputChunkSlice)

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TInputChunkSlicePtr& slice);

////////////////////////////////////////////////////////////////////////////////

//! Constructs a new chunk slice from the chunk spec, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    NTableClient::TKey lowerKey = NTableClient::TKey(),
    NTableClient::TKey upperKey = NTableClient::TKey());

//! Constructs a new chunk slice from another slice, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkSlice& inputSlice,
    NTableClient::TKey lowerKey = NTableClient::TKey(),
    NTableClient::TKey upperKey = NTableClient::TKey());

//! Constructs a new chunk slice based on inputChunk with limits from protoChunkSpec.
TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const NProto::TChunkSpec& protoChunkSpec);

//! Constructs separate chunk slice for each part of erasure chunk.
std::vector<TInputChunkSlicePtr> CreateErasureInputChunkSlices(
    const TInputChunkPtr& inputChunk,
    NErasure::ECodec codecId);

void InferLimitsFromBoundaryKeys(
    const TInputChunkSlicePtr& chunkSlice,
    const NTableClient::TRowBufferPtr& rowBuffer,
    int keyColumnCount);

std::vector<TInputChunkSlicePtr> SliceChunkByRowIndexes(
    const TInputChunkPtr& inputChunk,
    i64 sliceDataSize,
    i64 sliceRowCount);

void ToProto(
    NProto::TChunkSpec* chunkSpec,
    const TInputChunkSlicePtr& inputSlice,
    EDataSourceType dataSourceType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

