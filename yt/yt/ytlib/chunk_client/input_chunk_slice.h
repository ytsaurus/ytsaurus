#pragma once

#include "public.h"
#include "data_source.h"

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_slice.pb.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/key_bound.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <library/cpp/yt/memory/new.h>

#include <optional>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! A lightweight representation of NProto::TReadLimit for input slices.
struct TInputSliceLimit
{
    TInputSliceLimit() = default;
    TInputSliceLimit(
        const NProto::TReadLimit& other,
        const NTableClient::TRowBufferPtr& rowBuffer,
        TRange<NTableClient::TLegacyKey> keySet,
        TRange<NTableClient::TLegacyKey> keyBoundPrefixes,
        int keyLength,
        bool isUpper);

    //! If comparator is not present, these methods verify that no key bound is present in #other.
    void MergeLower(const TInputSliceLimit& other, const NTableClient::TComparator& comparator);
    void MergeUpper(const TInputSliceLimit& other, const NTableClient::TComparator& comparator);

    bool IsTrivial() const;

    explicit TInputSliceLimit(bool isUpper);

    std::optional<i64> RowIndex;
    NTableClient::TKeyBound KeyBound;

    PHOENIX_DECLARE_TYPE(TInputSliceLimit, 0x8d271cad);
};

void FormatValue(TStringBuilderBase* builder, const TInputSliceLimit& limit, TStringBuf spec);

bool IsTrivial(const TInputSliceLimit& limit);

void ToProto(NProto::TReadLimit* protoLimit, const TInputSliceLimit& limit);

void Serialize(const TInputSliceLimit& limit, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TInputChunkSlice
    : public TRefCounted
{
public:
    DECLARE_BYVAL_RO_PROPERTY(i64, DataWeight);
    DECLARE_BYVAL_RO_PROPERTY(i64, RowCount);
    DECLARE_BYVAL_RO_PROPERTY(i64, CompressedDataSize);
    DECLARE_BYVAL_RO_PROPERTY(i64, UncompressedDataSize);

    DECLARE_BYVAL_RO_PROPERTY(bool, SizeOverridden);
    DECLARE_BYVAL_RO_PROPERTY(int, PartIndex);
    DECLARE_BYVAL_RO_PROPERTY(i64, MaxBlockSize);
    DECLARE_BYVAL_RO_PROPERTY(i64, ValueCount);

    DEFINE_BYVAL_RW_PROPERTY(TInputChunkPtr, InputChunk);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, LowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, UpperLimit);
    //! Index of this chunk slice among all slices of the same chunk returned by chunk slice fetcher.
    DEFINE_BYVAL_RW_PROPERTY(int, SliceIndex, 0);

public:
    TInputChunkSlice() = default;
    TInputChunkSlice(TInputChunkSlice&& other) = default;

    TInputChunkSlice(
        const TInputChunkPtr& inputChunk,
        TInputSliceLimit lowerLimit,
        TInputSliceLimit upperLimit);

    explicit TInputChunkSlice(const TInputChunkSlice& inputSlice);

    TInputChunkSlice(
        const TInputChunkSlice& inputSlice,
        const NTableClient::TComparator& comparator,
        NTableClient::TKeyBound lowerKeyBound = NTableClient::TKeyBound::MakeUniversal(/*isUpper*/ false),
        NTableClient::TKeyBound upperKeyBound = NTableClient::TKeyBound::MakeUniversal(/*isUpper*/ true));

    TInputChunkSlice(
        const TInputChunkSlice& inputSlice,
        i64 lowerRowIndex,
        std::optional<i64> upperRowIndex,
        i64 dataWeight,
        i64 compressedDataSize,
        i64 uncompressedDataSize);

    TInputChunkSlice(
        const TInputChunkSlice& chunkSlice,
        const NTableClient::TComparator& comparator,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const NProto::TChunkSlice& protoChunkSlice,
        TRange<NTableClient::TLegacyKey> keySet,
        TRange<NTableClient::TLegacyKey> keyBoundPrefixes);

    TInputChunkSlice(
        const TInputChunkPtr& inputChunk,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const NProto::TChunkSpec& protoChunkSpec,
        const NTableClient::TComparator& comparator);

    //! Tries to split chunk slice into parts of almost equal size, about #sliceDataSize.
    //! If #rowBuffer is given, also capture
    std::vector<TInputChunkSlicePtr> SliceEvenly(
        i64 sliceDataWeight,
        i64 sliceRowCount,
        NTableClient::TRowBufferPtr rowBuffer = nullptr) const;
    std::pair<TInputChunkSlicePtr, TInputChunkSlicePtr>  SplitByRowIndex(i64 splitRow) const;

    i64 GetLocality(int replicaIndex) const;

    void OverrideSize(i64 rowCount, i64 dataWeight, i64 compressedDataSize, i64 uncompressedDataSize);

    void ApplySamplingSelectivityFactor(double samplingSelectivityFactor);

private:
    friend TInputChunkSlicePtr CreateInputChunkSliceFromCompleteErasureChunkPart(
        const TInputChunkPtr& inputChunk,
        int partIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        i64 dataWeight,
        i64 compressedDataSize,
        i64 uncompressedDataSize);

    int PartIndex_ = DefaultPartIndex;

    bool SizeOverridden_ = false;
    i64 DataWeight_ = 0;
    i64 RowCount_ = 0;
    i64 CompressedDataSize_ = 0;
    i64 UncompressedDataSize_ = 0;

    // Selectivity factors are applied. Data node is not yet capable to estimate selectivity on its side.
    void OverrideSize(const TInputChunkPtr& inputChunk, const NProto::TChunkSlice& protoChunkSlice);

    // Selectivity factors are not applied. Overrides are taken as-is from proto chunk spec.
    void OverrideSize(const TInputChunkPtr& inputChunk, const NProto::TChunkSpec& protoChunkSpec);

    PHOENIX_DECLARE_TYPE(TInputChunkSlice, 0xe177a42);
};

DEFINE_REFCOUNTED_TYPE(TInputChunkSlice)

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TInputChunkSlicePtr& slice, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

//! Constructs a chunk slice and copies keyless read limits from the input chunk.
TInputChunkSlicePtr CreateKeylessInputChunkSlice(
    const TInputChunkPtr& inputChunk);

//! Constructs a chunk slice with explicit key-bound limits.
TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    TInputSliceLimit lowerLimit,
    TInputSliceLimit upperLimit);

//! Constructs a chunk slice from the input chunk read limits,
//! preserving row indices and converting legacy keys to key bounds.
TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const NTableClient::TComparator& comparator);

//! Constructs a copy of a chunk slice.
TInputChunkSlicePtr CreateInputChunkSlice(const TInputChunkSlice& inputSlice);

//! Constructs a new chunk slice from another slice, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkSlice& inputSlice,
    const NTableClient::TComparator& comparator,
    NTableClient::TKeyBound lowerKeyBound = NTableClient::TKeyBound::MakeUniversal(/*isUpper*/ false),
    NTableClient::TKeyBound upperKeyBound = NTableClient::TKeyBound::MakeUniversal(/*isUpper*/ true));

//! Constructs a new chunk slice based on inputChunk with limits from protoChunkSpec.
TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkPtr& inputChunk,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const NProto::TChunkSpec& protoChunkSpec,
    const NTableClient::TComparator& comparator);

TInputChunkSlicePtr CreateInputChunkSliceFromCompleteErasureChunkPart(
    const TInputChunkPtr& inputChunk,
    int partIndex,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    i64 dataWeight,
    i64 compressedDataSize,
    i64 uncompressedDataSize);

//! Constructs a separate chunk slice for each data part of a complete erasure chunk.
std::vector<TInputChunkSlicePtr> CreateInputChunkSlicesFromCompleteErasureChunk(
    const TInputChunkPtr& inputChunk,
    NErasure::ECodec codecId);

void InferLimitsFromBoundaryKeys(
    const TInputChunkSlicePtr& chunkSlice,
    const NTableClient::TRowBufferPtr& rowBuffer,
    std::optional<int> keyColumnCount = std::nullopt,
    NTableClient::TComparator comparator = NTableClient::TComparator());

//! Comparator should correspond to table this containing chunk.
void ToProto(
    NProto::TChunkSpec* chunkSpec,
    const TInputChunkSlicePtr& inputSlice,
    NTableClient::TComparator comparator,
    EDataSourceType dataSourceType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
