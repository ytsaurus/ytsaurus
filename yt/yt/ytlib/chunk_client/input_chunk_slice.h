#pragma once

#include "public.h"
#include "data_source.h"

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_slice.pb.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/key_bound.h>

#include <yt/yt/library/erasure/public.h>

#include <library/cpp/yt/memory/new.h>

#include <optional>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! A lightweight representation of NProto::TReadLimit for input slices.
struct TLegacyInputSliceLimit
{
    TLegacyInputSliceLimit() = default;
    explicit TLegacyInputSliceLimit(const TLegacyReadLimit& other);
    TLegacyInputSliceLimit(
        const NProto::TReadLimit& other,
        const NTableClient::TRowBufferPtr& rowBuffer,
        TRange<NTableClient::TLegacyKey> keySet);

    std::optional<i64> RowIndex;
    NTableClient::TLegacyKey Key;

    void MergeLowerRowIndex(i64 rowIndex);
    void MergeUpperRowIndex(i64 rowIndex);

    void MergeLowerKey(NTableClient::TLegacyKey key);
    void MergeUpperKey(NTableClient::TLegacyKey key);

    void MergeLowerLimit(const TLegacyInputSliceLimit& limit);
    void MergeUpperLimit(const TLegacyInputSliceLimit& limit);

    void Persist(const NTableClient::TPersistenceContext& context);
};

TString ToString(const TLegacyInputSliceLimit& limit);

void FormatValue(TStringBuilderBase* builder, const TLegacyInputSliceLimit& limit, TStringBuf format);

bool IsTrivial(const TLegacyInputSliceLimit& limit);

void ToProto(NProto::TReadLimit* protoLimit, const TLegacyInputSliceLimit& limit);

////////////////////////////////////////////////////////////////////////////////

//! A lightweight representation of NProto::TReadLimit for input slices.
//! This version uses TKeyBound instead of TLegacyKey to represent slices by key.
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

    void Persist(const NTableClient::TPersistenceContext& context);
};

TString ToString(const TInputSliceLimit& limit);

void FormatValue(TStringBuilderBase* builder, const TInputSliceLimit& limit, TStringBuf format);

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

    DECLARE_BYVAL_RO_PROPERTY(bool, SizeOverridden);
    DECLARE_BYVAL_RO_PROPERTY(int, PartIndex);
    DECLARE_BYVAL_RO_PROPERTY(i64, MaxBlockSize);
    DECLARE_BYVAL_RO_PROPERTY(i64, ValueCount);

    DEFINE_BYVAL_RW_PROPERTY(TInputChunkPtr, InputChunk);
    DEFINE_BYREF_RW_PROPERTY(TLegacyInputSliceLimit, LegacyLowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TLegacyInputSliceLimit, LegacyUpperLimit);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, LowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, UpperLimit);
    //! Index of this chunk slice among all slices of the same chunk returned by chunk slice fetcher.
    DEFINE_BYVAL_RW_PROPERTY(int, SliceIndex, 0);

    bool IsLegacy = true;

public:
    TInputChunkSlice() = default;
    TInputChunkSlice(TInputChunkSlice&& other) = default;

    explicit TInputChunkSlice(
        const TInputChunkPtr& inputChunk,
        NTableClient::TLegacyKey lowerKey = NTableClient::TLegacyKey(),
        NTableClient::TLegacyKey upperKey = NTableClient::TLegacyKey());

    // Suitable both for legacy and new data slices.
    explicit TInputChunkSlice(const TInputChunkSlice& inputSlice);

    // COMPAT(max42): Legacy.
    TInputChunkSlice(
        const TInputChunkSlice& inputSlice,
        NTableClient::TLegacyKey lowerKey,
        NTableClient::TLegacyKey upperKey = NTableClient::TLegacyKey());

    TInputChunkSlice(
        const TInputChunkSlice& inputSlice,
        const NTableClient::TComparator& comparator,
        NTableClient::TKeyBound lowerKeyBound = NTableClient::TKeyBound::MakeUniversal(/*isUpper*/ false),
        NTableClient::TKeyBound upperKeyBound = NTableClient::TKeyBound::MakeUniversal(/*isUpper*/ true));

    TInputChunkSlice(
        const TInputChunkSlice& inputSlice,
        i64 lowerRowIndex,
        std::optional<i64> upperRowIndex,
        i64 dataSize);

    TInputChunkSlice(
        const TInputChunkPtr& inputChunk,
        int partIndex,
        i64 lowerRowIndex,
        std::optional<i64> upperRowIndex,
        i64 dataSize);

    TInputChunkSlice(
        const TInputChunkPtr& inputChunk,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const NProto::TChunkSlice& protoChunkSlice,
        TRange<NTableClient::TLegacyKey> keySet);

    TInputChunkSlice(
        const TInputChunkSlice& chunkSlice,
        const NTableClient::TRowBufferPtr& rowBuffer,
        const NProto::TChunkSlice& protoChunkSlice,
        TRange<NTableClient::TLegacyKey> keySet);

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

    void ApplySamplingSelectivityFactor(double samplingSelectivityFactor);

    void TransformToLegacy(const NTableClient::TRowBufferPtr& rowBuffer);
    void TransformToNew(const NTableClient::TRowBufferPtr& rowBuffer, std::optional<int> keyLength);

    //! Transform to new assuming that there are no non-trivial key bounds in read limits.
    void TransformToNewKeyless();

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
    NTableClient::TLegacyKey lowerKey = NTableClient::TLegacyKey(),
    NTableClient::TLegacyKey upperKey = NTableClient::TLegacyKey());

//! Constructs a copy of a chunk slice. Suitable both for legacy and new chunk slices.
TInputChunkSlicePtr CreateInputChunkSlice(const TInputChunkSlice& inputSlice);

// COMPAT(max42): Legacy.
//! Constructs a new chunk slice from another slice, restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TInputChunkSlicePtr CreateInputChunkSlice(
    const TInputChunkSlice& inputSlice,
    NTableClient::TLegacyKey lowerKey,
    NTableClient::TLegacyKey upperKey = NTableClient::TLegacyKey());

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
    const NProto::TChunkSpec& protoChunkSpec);

//! Constructs separate chunk slice for each part of erasure chunk.
std::vector<TInputChunkSlicePtr> CreateErasureInputChunkSlices(
    const TInputChunkPtr& inputChunk,
    NErasure::ECodec codecId);

void InferLimitsFromBoundaryKeys(
    const TInputChunkSlicePtr& chunkSlice,
    const NTableClient::TRowBufferPtr& rowBuffer,
    std::optional<int> keyColumnCount = std::nullopt,
    NTableClient::TComparator comparator = NTableClient::TComparator());

std::vector<TInputChunkSlicePtr> SliceChunkByRowIndexes(
    const TInputChunkPtr& inputChunk,
    i64 sliceDataSize,
    i64 sliceRowCount);

//! Comparator should correspond to table this containing chunk.
void ToProto(
    NProto::TChunkSpec* chunkSpec,
    const TInputChunkSlicePtr& inputSlice,
    NTableClient::TComparator comparator,
    EDataSourceType dataSourceType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
