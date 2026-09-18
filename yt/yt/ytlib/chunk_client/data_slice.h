#pragma once

#include "public.h"
#include "data_source.h"
#include "input_chunk_slice.h"

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <optional>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TDataSlice
    : public TRefCounted
{
public:
    using TChunkSliceList = TCompactVector<TInputChunkSlicePtr, 1>;

public:
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, LowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, UpperLimit);

public:
    TDataSlice() = default;

    TDataSlice(
        EDataSourceType type,
        TChunkSliceList chunkSlices,
        TInputSliceLimit lowerLimit = TInputSliceLimit(),
        TInputSliceLimit upperLimit = TInputSliceLimit(/*isUpper*/ true),
        std::optional<i64> tag = std::nullopt);

    int GetChunkCount() const;
    i64 GetDataWeight() const;
    i64 GetRowCount() const;
    i64 GetMaxBlockSize() const;
    i64 GetValueCount() const;
    i64 GetCompressedDataSize() const;
    i64 GetUncompressedDataSize() const;

    int GetTableIndex() const;
    int GetRangeIndex() const;

    //! Check that data slice is an unversioned single-chunk slice.
    bool IsTrivial() const;

    //! Check that at least one limit is set.
    bool HasLimits() const;

    //! Copy some fields from the originating data slice.
    void CopyPayloadFrom(const TDataSlice& dataSlice);

    TInputChunkPtr GetSingleUnversionedChunk() const;
    TInputChunkSlicePtr GetSingleUnversionedChunkSlice() const;

    std::pair<TDataSlicePtr, TDataSlicePtr> SplitByRowIndex(i64 splitRow) const;

    //! For unversioned slices, returns index of this chunk slice among all slices of the same chunk.
    //! For versioned tables, returns 0.
    int GetSliceIndex() const;

    int GetInputStreamIndex() const;
    void SetInputStreamIndex(int inputStreamIndex);

    TChunkSliceList ChunkSlices;
    EDataSourceType Type;

    //! A tag that helps us restore the correspondence between
    //! the unread data slices and the original data slices.
    // TODO(apollo1321): Remove this tag.
    std::optional<i64> Tag;

    //! Used to recover the original read ranges in task before serializing to job spec.
    std::optional<i64> ReadRangeIndex;

    //! Flag indicating that the basic conditions for teleporting are met:
    //! data slice corresponds to an unversioned chunk with no non-trivial read limits.
    //! Used by the sorted pool.
    bool IsTeleportable = false;

    std::optional<i64> VirtualRowIndex = std::nullopt;

private:
    //! An index of an input stream this data slice corresponds to. If this is a data
    //! slice of some input table, it should normally be equal to `GetTableIndex()`.
    std::optional<int> InputStreamIndex_;

    PHOENIX_DECLARE_TYPE(TDataSlice, 0x1e21b076);
};

DEFINE_REFCOUNTED_TYPE(TDataSlice)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDataSlicePtr& dataSlice, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TDataSlicePtr& dataSlice, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

TDataSlicePtr CreateInputDataSlice(
    NChunkClient::EDataSourceType type,
    const std::vector<TInputChunkSlicePtr>& inputChunks,
    const NTableClient::TComparator& comparator,
    NTableClient::TKeyBound lowerBound,
    NTableClient::TKeyBound upperBound);

//! Copy given input data slice.
TDataSlicePtr CreateInputDataSlice(const TDataSlicePtr& dataSlice);

//! Copy given input data slice, possible restricting it to the given key bounds.
TDataSlicePtr CreateInputDataSlice(
    const TDataSlicePtr& dataSlice,
    const NTableClient::TComparator& comparator,
    NTableClient::TKeyBound lowerKeyBound,
    NTableClient::TKeyBound upperKeyBound = NTableClient::TKeyBound::MakeUniversal(/*isUpper*/ true));

TDataSlicePtr CreateUnversionedInputDataSlice(TInputChunkSlicePtr chunkSlice);

TDataSlicePtr CreateVersionedInputDataSlice(
    const std::vector<TInputChunkSlicePtr>& inputChunkSlices);

void InferLimitsFromBoundaryKeys(
    const TDataSlicePtr& dataSlice,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const NTableClient::TComparator& comparator = NTableClient::TComparator());

//! Set data slice limits to be equal to chunk boundary keys shortened to given prefix length.
void SetLimitsFromShortenedBoundaryKeys(
    const TDataSlicePtr& dataSlice,
    int prefixLength,
    const NTableClient::TRowBufferPtr& rowBuffer);

std::optional<TChunkId> IsUnavailable(
    const TDataSlicePtr& dataSlice,
    EChunkAvailabilityPolicy policy);

bool CompareChunkSlicesByLowerLimit(const TInputChunkSlicePtr& slice1, const TInputChunkSlicePtr& slice2);
i64 GetCumulativeRowCount(const std::vector<TDataSlicePtr>& dataSlices);
i64 GetCumulativeDataWeight(const std::vector<TDataSlicePtr>& dataSlices);

////////////////////////////////////////////////////////////////////////////////

std::vector<TDataSlicePtr> CombineVersionedChunkSlices(
    const std::vector<TInputChunkSlicePtr>& chunkSlices,
    const NTableClient::TComparator& comparator);

////////////////////////////////////////////////////////////////////////////////

//! Return a compact debug representation without exposing physical chunk slices.
std::string GetDataSliceDebugString(const TDataSlicePtr& dataSlice);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
