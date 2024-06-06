#pragma once

#include "public.h"
#include "data_source.h"
#include "input_chunk_slice.h"

#include <yt/yt/core/misc/phoenix.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <optional>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TLegacyDataSlice
    : public TRefCounted
{
public:
    using TChunkSliceList = TCompactVector<TInputChunkSlicePtr, 1>;

public:
    DEFINE_BYREF_RW_PROPERTY(TLegacyInputSliceLimit, LegacyLowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TLegacyInputSliceLimit, LegacyUpperLimit);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, LowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, UpperLimit);

    bool IsLegacy = true;

public:
    TLegacyDataSlice() = default;

    // COMPAT(max42): Legacy.
    TLegacyDataSlice(
        EDataSourceType type,
        TChunkSliceList chunkSlices,
        TLegacyInputSliceLimit lowerLimit = TLegacyInputSliceLimit(),
        TLegacyInputSliceLimit upperLimit = TLegacyInputSliceLimit(),
        std::optional<i64> tag = std::nullopt);

    TLegacyDataSlice(
        EDataSourceType type,
        TChunkSliceList chunkSlices,
        TInputSliceLimit lowerLimit,
        TInputSliceLimit upperLimit = TInputSliceLimit(/* isUpper */ true),
        std::optional<i64> tag = std::nullopt);

    int GetChunkCount() const;
    i64 GetDataWeight() const;
    i64 GetRowCount() const;
    i64 GetMaxBlockSize() const;
    i64 GetValueCount() const;

    int GetTableIndex() const;
    int GetRangeIndex() const;

    void Persist(const NTableClient::TPersistenceContext& context);

    //! Check that data slice is an old single-chunk slice. Used for compatibility.
    bool IsTrivial() const;

    //! Check that lower limit >= upper limit, i.e. that slice must be empty.
    bool IsEmpty() const;

    //! Check that at least one limit is set.
    bool HasLimits() const;

    //! Copy some fields from the originating data slice.
    void CopyPayloadFrom(const TLegacyDataSlice& dataSlice);

    TInputChunkPtr GetSingleUnversionedChunk() const;
    TInputChunkSlicePtr GetSingleUnversionedChunkSlice() const;

    std::pair<TLegacyDataSlicePtr, TLegacyDataSlicePtr> SplitByRowIndex(i64 splitRow) const;

    void TransformToLegacy(const NTableClient::TRowBufferPtr& rowBuffer);
    void TransformToNew(const NTableClient::TRowBufferPtr& rowBuffer, int keyLength, bool trimChunkSliceKeys = false);

    //! Transform to new assuming that there are no non-trivial key bounds in read limits.
    void TransformToNewKeyless();

    //! Helper around two previous versions for cases when we either have sorted data slice with keys
    //! and we have some comparator or when we do not have comparator but data slice is actually keyless.
    void TransformToNew(
        const NTableClient::TRowBufferPtr& rowBuffer,
        NTableClient::TComparator comparator);

    //! For unversioned slices, returns index of this chunk slice among all slices of the same chunk.
    //! For versioned tables, returns 0.
    int GetSliceIndex() const;

    int GetInputStreamIndex() const;
    void SetInputStreamIndex(int inputStreamIndex);

    TChunkSliceList ChunkSlices;
    EDataSourceType Type;

    //! A tag that helps us restore the correspondence between
    //! the unread data slices and the original data slices.
    std::optional<i64> Tag;

    //! Used to recover the original read ranges in task before serializing to job spec.
    std::optional<i64> ReadRangeIndex;

    // COMPAT(max42)
    //! Flag indicating that this basic conditions for teleporting are met:
    //! data slice corresponds to an unversioned chunk with no non-trivial read limits.
    //! Currently used only for the new sorted pool.
    bool IsTeleportable = false;

    std::optional<i64> VirtualRowIndex = std::nullopt;

private:
    //! An index of an input stream this data slice corresponds to. If this is a data
    //! slice of some input table, it should normally be equal to `GetTableIndex()`.
    std::optional<int> InputStreamIndex_;
};

DEFINE_REFCOUNTED_TYPE(TLegacyDataSlice)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TLegacyDataSlicePtr& dataSlice, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TLegacyDataSlicePtr& dataSlice);

////////////////////////////////////////////////////////////////////////////////

TLegacyDataSlicePtr CreateInputDataSlice(
    NChunkClient::EDataSourceType type,
    const std::vector<TInputChunkSlicePtr>& inputChunks,
    NTableClient::TLegacyKey lowerKey,
    NTableClient::TLegacyKey upperKey);

TLegacyDataSlicePtr CreateInputDataSlice(
    NChunkClient::EDataSourceType type,
    const std::vector<TInputChunkSlicePtr>& inputChunks,
    const NTableClient::TComparator& comparator,
    NTableClient::TKeyBound lowerBound,
    NTableClient::TKeyBound upperBound);

//! Copy given input data slice. Suitable both for legacy and new data slices.
TLegacyDataSlicePtr CreateInputDataSlice(const TLegacyDataSlicePtr& dataSlice);

//! Copy given input data slice, possibly restricting it to the given legacy key range.
TLegacyDataSlicePtr CreateInputDataSlice(
    const TLegacyDataSlicePtr& dataSlice,
    NTableClient::TLegacyKey lowerKey,
    NTableClient::TLegacyKey upperKey = NTableClient::TLegacyKey());

//! Copy given input data slice, possible restricting it to the given key bounds.
TLegacyDataSlicePtr CreateInputDataSlice(
    const TLegacyDataSlicePtr& dataSlice,
    const NTableClient::TComparator& comparator,
    NTableClient::TKeyBound lowerKeyBound,
    NTableClient::TKeyBound upperKeyBound = NTableClient::TKeyBound::MakeUniversal(/* isUpper */ true));

TLegacyDataSlicePtr CreateUnversionedInputDataSlice(TInputChunkSlicePtr chunkSlice);

TLegacyDataSlicePtr CreateVersionedInputDataSlice(
    const std::vector<TInputChunkSlicePtr>& inputChunkSlices);

void InferLimitsFromBoundaryKeys(
    const TLegacyDataSlicePtr& dataSlice,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const NTableClient::TComparator& comparator = NTableClient::TComparator());

//! Set data slice limits to be equal to chunk boundary keys shortened to given prefix length.
//! Works only for new data slices.
void SetLimitsFromShortenedBoundaryKeys(
    const TLegacyDataSlicePtr& dataSlice,
    int prefixLength,
    const NTableClient::TRowBufferPtr& rowBuffer);

std::optional<TChunkId> IsUnavailable(
    const TLegacyDataSlicePtr& dataSlice,
    EChunkAvailabilityPolicy policy);

bool CompareChunkSlicesByLowerLimit(const TInputChunkSlicePtr& slice1, const TInputChunkSlicePtr& slice2);
i64 GetCumulativeRowCount(const std::vector<TLegacyDataSlicePtr>& dataSlices);
i64 GetCumulativeDataWeight(const std::vector<TLegacyDataSlicePtr>& dataSlices);

////////////////////////////////////////////////////////////////////////////////

std::vector<TLegacyDataSlicePtr> CombineVersionedChunkSlices(
    const std::vector<TInputChunkSlicePtr>& chunkSlices,
    const NTableClient::TComparator& comparator);

////////////////////////////////////////////////////////////////////////////////

//! This debug string will eventually become ToString for new data slices.
//! In particular, it does not mention anything related to data slice
//! internal structure (i.e. chunk slices).
TString GetDataSliceDebugString(const TLegacyDataSlicePtr& dataSlice);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
