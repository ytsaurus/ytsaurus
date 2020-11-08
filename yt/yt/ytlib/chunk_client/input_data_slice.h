#pragma once

#include "public.h"
#include "data_source.h"
#include "input_chunk_slice.h"

#include <yt/core/misc/optional.h>
#include <yt/core/misc/phoenix.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TInputDataSlice
    : public TRefCounted
{
public:
    using TChunkSliceList = SmallVector<TInputChunkSlicePtr, 1>;

public:
    DEFINE_BYREF_RW_PROPERTY(TLegacyInputSliceLimit, LegacyLowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TLegacyInputSliceLimit, LegacyUpperLimit);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, LowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TInputSliceLimit, UpperLimit);

    bool IsLegacy = true;

public:
    TInputDataSlice() = default;

    // COMPAT(max42): Legacy.
    TInputDataSlice(
        EDataSourceType type,
        TChunkSliceList chunkSlices,
        TLegacyInputSliceLimit lowerLimit = TLegacyInputSliceLimit(),
        TLegacyInputSliceLimit upperLimit = TLegacyInputSliceLimit(),
        std::optional<i64> tag = std::nullopt);

    TInputDataSlice(
        EDataSourceType type,
        TChunkSliceList chunkSlices,
        TInputSliceLimit lowerLimit,
        TInputSliceLimit upperLimit = TInputSliceLimit(/* isUpper */ true),
        std::optional<i64> tag = std::nullopt);

    int GetChunkCount() const;
    i64 GetDataWeight() const;
    i64 GetRowCount() const;
    i64 GetMaxBlockSize() const;

    int GetTableIndex() const;
    int GetRangeIndex() const;

    void Persist(NTableClient::TPersistenceContext& context);

    //! Check that data slice is an old single-chunk slice. Used for compatibility.
    bool IsTrivial() const;

    //! Check that lower limit >= upper limit, i.e. that slice must be empty.
    bool IsEmpty() const;

    //! Check that at least one limit is set.
    bool HasLimits() const;

    //! Copy some fields from the originating data slice.
    void CopyPayloadFrom(const TInputDataSlice& dataSlice);

    TInputChunkPtr GetSingleUnversionedChunkOrThrow() const;

    std::pair<TInputDataSlicePtr, TInputDataSlicePtr> SplitByRowIndex(i64 splitRow) const;

    void TransformToLegacy(const NTableClient::TRowBufferPtr& rowBuffer);
    void TransformToNew(const NTableClient::TRowBufferPtr& rowBuffer, int keyLength);

    TChunkSliceList ChunkSlices;
    EDataSourceType Type;

    //! A tag that helps us restore the correspondence between
    //! the unread data slices and the original data slices.
    std::optional<i64> Tag;

    //! An index of an input stream this data slice corresponds to. If this is a data
    //! slice of some input table, it should normally be equal to `GetTableIndex()`.
    int InputStreamIndex = -1;

    std::optional<i64> VirtualRowIndex = std::nullopt;
};

DEFINE_REFCOUNTED_TYPE(TInputDataSlice)

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TInputDataSlicePtr& dataSlice);

////////////////////////////////////////////////////////////////////////////////

TInputDataSlicePtr CreateInputDataSlice(
    NChunkClient::EDataSourceType type,
    const std::vector<TInputChunkSlicePtr>& inputChunks,
    NTableClient::TLegacyKey lowerKey,
    NTableClient::TLegacyKey upperKey);

//! Copy given input data slice. Suitable both for legacy and new data slices.
TInputDataSlicePtr CreateInputDataSlice(const TInputDataSlicePtr& dataSlice);

//! Copy given input data slice, possibly restricting it to the given legacy key range.
TInputDataSlicePtr CreateInputDataSlice(
    const TInputDataSlicePtr& dataSlice,
    NTableClient::TLegacyKey lowerKey,
    NTableClient::TLegacyKey upperKey = NTableClient::TLegacyKey());

//! Copy given input data slice, possible restricting it to the given key bounds.
TInputDataSlicePtr CreateInputDataSlice(
    const TInputDataSlicePtr& dataSlice,
    const NTableClient::TComparator& comparator,
    NTableClient::TKeyBound lowerKeyBound,
    NTableClient::TKeyBound upperKeyBound = NTableClient::TKeyBound::MakeUniversal(/* isUpper */ true));

TInputDataSlicePtr CreateUnversionedInputDataSlice(TInputChunkSlicePtr chunkSlice);

TInputDataSlicePtr CreateVersionedInputDataSlice(
    const std::vector<TInputChunkSlicePtr>& inputChunkSlices);

// TODO(max42): stop infering limits each time you pass something into sorted chunk pool,
// it is better to always infer them inside chunk pool.
void InferLimitsFromBoundaryKeys(
    const TInputDataSlicePtr& dataSlice,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const NTableClient::TVirtualValueDirectoryPtr& virtualValueDirectory = nullptr);

std::optional<TChunkId> IsUnavailable(const TInputDataSlicePtr& dataSlice, bool checkParityParts);
bool CompareChunkSlicesByLowerLimit(const TInputChunkSlicePtr& slice1, const TInputChunkSlicePtr& slice2);
i64 GetCumulativeRowCount(const std::vector<TInputDataSlicePtr>& dataSlices);
i64 GetCumulativeDataWeight(const std::vector<TInputDataSlicePtr>& dataSlices);

////////////////////////////////////////////////////////////////////////////////

std::vector<TInputDataSlicePtr> CombineVersionedChunkSlices(
    const std::vector<TInputChunkSlicePtr>& chunkSlices);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
