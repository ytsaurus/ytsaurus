#include "pivot_keys_builder.h"

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include "yt/yt/core/misc/numeric_helpers.h"

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TReshardPivotKeysBuilder::TReshardPivotKeysBuilder(
    TComparator comparator,
    int keyColumnCount,
    int tabletCount,
    double accuracy,
    i64 expectedTabletSize,
    TLegacyOwningKey nextPivot)
    : ExpectedTabletSize_(expectedTabletSize)
    , KeyColumnCount_(keyColumnCount)
    , TabletCount_(tabletCount)
    , Accuracy_(accuracy)
    , NextPivot_(nextPivot)
    , Pivots_(tabletCount)
    , SliceBoundaryKeyCompare_(
        [comparator = std::move(comparator)]
            (const TSliceBoundaryKey& lhs, const TSliceBoundaryKey& rhs) {
        return comparator.CompareKeyBounds(lhs.GetKeyBound(), rhs.GetKeyBound(), 1) < 0;
    })
{ }

void TReshardPivotKeysBuilder::AddChunk(const NYT::NChunkClient::NProto::TChunkSpec& chunkSpec)
{
    auto boundaryKeys = FindBoundaryKeys(chunkSpec.chunk_meta(), KeyColumnCount_);
    YT_VERIFY(boundaryKeys);
    YT_VERIFY(boundaryKeys->MinKey);
    YT_VERIFY(boundaryKeys->MaxKey);

    TOwningKeyBound chunkMinKey;
    if (chunkSpec.has_lower_limit()) {
        auto lowerLimit = TReadLimit(chunkSpec.lower_limit(), /*isUpper*/ false);
        chunkMinKey = lowerLimit.KeyBound();
    } else {
        chunkMinKey = TOwningKeyBound::FromRow() >= boundaryKeys->MinKey;
    }

    TOwningKeyBound chunkMaxKey;
    if (chunkSpec.has_upper_limit()) {
        auto upperLimit = TReadLimit(chunkSpec.upper_limit(), /*isUpper*/ true);
        chunkMaxKey = upperLimit.KeyBound();
    } else {
        chunkMaxKey = TOwningKeyBound::FromRow() <= boundaryKeys->MaxKey;
    }

    auto dataWeight = GetChunkDataWeight(chunkSpec);
    auto inputChunk = New<TInputChunk>(chunkSpec);

    ChunkBoundaryKeys_.emplace_back(
        chunkMinKey,
        inputChunk,
        dataWeight);
    ChunkBoundaryKeys_.emplace_back(
        chunkMaxKey,
        inputChunk,
        dataWeight);
}

void TReshardPivotKeysBuilder::AddChunk(const TWeightedInputChunkPtr& chunk)
{
    auto chunkMinKey = TOwningKeyBound::FromRow() >= chunk->GetInputChunk()->BoundaryKeys()->MinKey;
    auto chunkMaxKey = TOwningKeyBound::FromRow() <= chunk->GetInputChunk()->BoundaryKeys()->MaxKey;

    if (chunk->GetInputChunk()->LowerLimit()) {
        chunkMinKey = KeyBoundFromLegacyRow(
            chunk->GetInputChunk()->LowerLimit()->GetLegacyKey(),
            /*isUpper*/ false,
            KeyColumnCount_);
    }

    if (chunk->GetInputChunk()->UpperLimit()) {
        chunkMaxKey = KeyBoundFromLegacyRow(
            chunk->GetInputChunk()->UpperLimit()->GetLegacyKey(),
            /*isUpper*/ true,
            KeyColumnCount_);
    }

    ChunkBoundaryKeys_.emplace_back(
        chunkMinKey,
        chunk->GetInputChunk(),
        chunk->GetDataWeight());
    ChunkBoundaryKeys_.emplace_back(
        chunkMaxKey,
        chunk->GetInputChunk(),
        chunk->GetDataWeight());
}

void TReshardPivotKeysBuilder::AddSlice(const TInputChunkSlicePtr& slice)
{
    SliceBoundaryKeys_.emplace_back(
        slice->LowerLimit().KeyBound.ToOwning(),
        slice->GetInputChunk(),
        slice->GetDataWeight());

    SliceBoundaryKeys_.emplace_back(
        slice->UpperLimit().KeyBound.ToOwning(),
        slice->GetInputChunk(),
        slice->GetDataWeight());

    TotalSizeAfterSlicing_ += slice->GetDataWeight();
}

void TReshardPivotKeysBuilder::ComputeChunksForSlicing()
{
    std::sort(ChunkBoundaryKeys_.begin(), ChunkBoundaryKeys_.end(), SliceBoundaryKeyCompare_);

    i64 tabletIndex = 1;
    auto boundaryIt = ChunkBoundaryKeys_.begin();
    auto boundaryEnd = ChunkBoundaryKeys_.end();
    UpdateCurrentChunksAndSizes(boundaryIt, boundaryEnd);

    while (boundaryIt < boundaryEnd && tabletIndex < TabletCount_) {
        if (!IsPivotKeyZone(State_.CurrentStartedChunksSize, tabletIndex)) {
            if (State_.CurrentStartedChunksSize < LowerPivotZone(tabletIndex)) {
                ++boundaryIt;
                UpdateCurrentChunksAndSizes(boundaryIt, boundaryEnd);
            } else {
                YT_VERIFY(State_.CurrentStartedChunksSize > UpperPivotZone(tabletIndex));
                ++tabletIndex;
                State_.ChunkForSlicingToSize[boundaryIt->GetChunk()] = boundaryIt->GetDataWeight();
                State_.ChunkForSlicingToSize.insert(
                    State_.CurrentChunkToSize.begin(),
                    State_.CurrentChunkToSize.end());
            }
            continue;
        }

        boundaryIt = AddChunksToSplit(boundaryIt, tabletIndex);
        ++tabletIndex;
    }

    if (boundaryIt < boundaryEnd && TabletCount_ > 1) {
        AddChunksToSplit(boundaryIt, TabletCount_ - 1);
    }
}

void TReshardPivotKeysBuilder::ComputeSlicedChunksPivotKeys()
{
    AddFullChunks();
    RecalculateExpectedTabletSize();

    std::sort(SliceBoundaryKeys_.begin(), SliceBoundaryKeys_.end(), SliceBoundaryKeyCompare_);

    State_ = {};
    i64 tabletIndex = 1;
    auto boundaryIt = SliceBoundaryKeys_.begin();
    auto boundaryEnd = SliceBoundaryKeys_.end();
    UpdateCurrentChunksAndSizes(boundaryIt, boundaryEnd);

    while (boundaryIt < boundaryEnd && tabletIndex < TabletCount_) {
        i64 tabletSize = State_.CurrentFinishedChunksSize - ExpectedTabletSize_ * (tabletIndex - 1);
        if (CanSplitHere(boundaryIt, tabletIndex)) {
            if (!Pivots_[tabletIndex].TabletSize ||
                IsBetterSize(tabletSize, *Pivots_[tabletIndex].TabletSize))
            {
                Pivots_[tabletIndex].Key = boundaryIt->GetKeyBound().Prefix;
                Pivots_[tabletIndex].TabletSize = tabletSize;
            }

            ++boundaryIt;
            UpdateCurrentChunksAndSizes(boundaryIt, boundaryEnd);
            continue;
        }

        if (!IsPivotKeyZone(State_.CurrentFinishedChunksSize, tabletIndex)) {
            if (State_.CurrentFinishedChunksSize < LowerPivotZone(tabletIndex)) {
                ++boundaryIt;
                UpdateCurrentChunksAndSizes(boundaryIt, boundaryEnd);
                continue;
            } else {
                YT_VERIFY(State_.CurrentFinishedChunksSize > UpperPivotZone(tabletIndex));

                if (Pivots_[tabletIndex].Key) {
                    ++tabletIndex;
                    continue;
                }
            }
        }

        if (!IsKeyGreaterThanPreviousPivot(boundaryIt, tabletIndex)) {
            ++boundaryIt;
            UpdateCurrentChunksAndSizes(boundaryIt, boundaryEnd);
            continue;
        }

        if (!IsKeyLowerThanNextPivot(boundaryIt, tabletIndex)) {
            ++boundaryIt;
            UpdateCurrentChunksAndSizes(boundaryIt, boundaryEnd);
            continue;
        }

        if (Pivots_[tabletIndex].TabletSize) {
            ++boundaryIt;
            UpdateCurrentChunksAndSizes(boundaryIt, boundaryEnd);
            continue;
        }

        if (!Pivots_[tabletIndex].BruteTabletSize ||
            (IsBetterSize(tabletSize, *Pivots_[tabletIndex].BruteTabletSize) &&
             tabletSize <= ExpectedTabletSize_))
        {
            Pivots_[tabletIndex].Key = boundaryIt->GetKeyBound().Prefix;
            Pivots_[tabletIndex].BruteTabletSize = tabletSize;
        }

        ++boundaryIt;
        UpdateCurrentChunksAndSizes(boundaryIt, boundaryEnd);
    }
}

void TReshardPivotKeysBuilder::SetFirstPivotKey(const TLegacyOwningKey& key)
{
    Pivots_[0].Key = key;
}

bool TReshardPivotKeysBuilder::AreAllPivotsFound() const
{
    for (const auto& pivot : Pivots_) {
        if (!pivot.Key) {
            return false;
        }
    }
    return true;
}

std::vector<TLegacyOwningKey> TReshardPivotKeysBuilder::GetPivotKeys() const
{
    std::vector<TLegacyOwningKey> pivotKeys;
    for (const auto& pivot : Pivots_) {
        YT_VERIFY(pivot.Key);
        pivotKeys.push_back(pivot.Key);
    }
    return pivotKeys;
}

const THashMap<TInputChunkPtr, i64>& TReshardPivotKeysBuilder::GetChunksForSlicing() const
{
    return State_.ChunkForSlicingToSize;
}

void TReshardPivotKeysBuilder::UpdateCurrentChunksAndSizes(
    TBoundaryKeyIterator boundaryKey,
    TBoundaryKeyIterator boundaryEnd)
{
    if (!(boundaryKey < boundaryEnd)) {
        return;
    }

    if (!boundaryKey->GetKeyBound().IsUpper) {
        State_.CurrentStartedChunksSize += boundaryKey->GetDataWeight();
        State_.CurrentChunkToSize[boundaryKey->GetChunk()] = boundaryKey->GetDataWeight();
    } else {
        State_.CurrentFinishedChunksSize += boundaryKey->GetDataWeight();
        State_.CurrentChunkToSize.erase(boundaryKey->GetChunk());
    }
}

i64 TReshardPivotKeysBuilder::UpperPivotZone(i64 tabletIndex) const
{
    return tabletIndex * ExpectedTabletSize_ + ExpectedTabletSize_ * Accuracy_;
}

i64 TReshardPivotKeysBuilder::LowerPivotZone(i64 tabletIndex) const
{
    return tabletIndex * ExpectedTabletSize_ - ExpectedTabletSize_ * Accuracy_;
}

bool TReshardPivotKeysBuilder::IsPivotKeyZone(i64 size, i64 tabletIndex) const
{
    return
        IsLowerPivotZone(size, tabletIndex) ||
        IsUpperPivotZone(size, tabletIndex);
}

bool TReshardPivotKeysBuilder::IsUpperPivotZone(i64 size, i64 tabletIndex) const
{
    return
        size >= tabletIndex * ExpectedTabletSize_ &&
        size <= UpperPivotZone(tabletIndex);
}

bool TReshardPivotKeysBuilder::IsLowerPivotZone(i64 size, i64 tabletIndex) const
{
    return
        size >= LowerPivotZone(tabletIndex) &&
        size <= tabletIndex * ExpectedTabletSize_;
}

bool TReshardPivotKeysBuilder::IsBetterSize(i64 newSize, i64 currentSize) const
{
    i64 newDiff = std::abs(newSize - ExpectedTabletSize_);
    i64 currentDiff = std::abs(currentSize - ExpectedTabletSize_);
    return newDiff <= currentDiff;
}

bool TReshardPivotKeysBuilder::CanSplitHere(TBoundaryKeyIterator boundaryKey, i64 tabletIndex) const
{
    if (!IsPivotKeyZone(State_.CurrentFinishedChunksSize, tabletIndex) ||
        !IsKeyGreaterThanPreviousPivot(boundaryKey, tabletIndex) ||
        !IsKeyLowerThanNextPivot(boundaryKey, tabletIndex))
    {
        return false;
    }

    if (State_.CurrentChunkToSize.empty()) {
        return boundaryKey->GetKeyBound().IsUpper && !boundaryKey->GetKeyBound().IsInclusive;
    } else if (std::ssize(State_.CurrentChunkToSize) == 1) {
        return !boundaryKey->GetKeyBound().IsUpper;
    }

    return false;
}

bool TReshardPivotKeysBuilder::IsKeyGreaterThanPreviousPivot(
    TBoundaryKeyIterator boundaryKey,
    i64 tabletIndex) const
{
    return CompareRows(Pivots_[tabletIndex - 1].Key, boundaryKey->GetKeyBound().Prefix) < 0;
}

bool TReshardPivotKeysBuilder::IsKeyLowerThanNextPivot(
    TBoundaryKeyIterator boundaryKey,
    i64 tabletIndex) const
{
    if (tabletIndex + 1 == TabletCount_) {
        return CompareRows(NextPivot_, boundaryKey->GetKeyBound().Prefix) > 0;
    }
    return !Pivots_[tabletIndex + 1].Key ||
        CompareRows(Pivots_[tabletIndex + 1].Key, boundaryKey->GetKeyBound().Prefix) > 0;
}

TReshardPivotKeysBuilder::TBoundaryKeyIterator TReshardPivotKeysBuilder::AddChunksToSplit(
    TBoundaryKeyIterator boundaryIt,
    i64 tabletIndex)
{
    THashMap<TInputChunkPtr, i64> chunkForSlicingToSize(State_.CurrentChunkToSize);
    chunkForSlicingToSize[boundaryIt->GetChunk()] = boundaryIt->GetDataWeight();

    while (State_.CurrentFinishedChunksSize <= UpperPivotZone(tabletIndex) &&
           ++boundaryIt < ChunkBoundaryKeys_.end())
    {
        chunkForSlicingToSize[boundaryIt->GetChunk()] = boundaryIt->GetDataWeight();
        UpdateCurrentChunksAndSizes(boundaryIt, ChunkBoundaryKeys_.end());

        if (CanSplitHere(boundaryIt, tabletIndex)) {
            i64 tabletSize = State_.CurrentFinishedChunksSize - ExpectedTabletSize_ * (tabletIndex - 1);
            if (!Pivots_[tabletIndex].TabletSize ||
                IsBetterSize(tabletSize, *Pivots_[tabletIndex].TabletSize))
            {
                Pivots_[tabletIndex].Key = boundaryIt->GetKeyBound().Prefix;
                Pivots_[tabletIndex].TabletSize = tabletSize;
            }
        }
    }

    if (!Pivots_[tabletIndex].Key) {
        State_.ChunkForSlicingToSize.insert(chunkForSlicingToSize.begin(), chunkForSlicingToSize.end());
    }

    return boundaryIt;
}

void TReshardPivotKeysBuilder::AddFullChunks()
{
    i64 dupChunksTotalSize = 0;
    for (const auto& boundaryKey : ChunkBoundaryKeys_) {
        if (!State_.ChunkForSlicingToSize.contains(boundaryKey.GetChunk())) {
            SliceBoundaryKeys_.push_back(boundaryKey);
            dupChunksTotalSize += boundaryKey.GetDataWeight();
        }
    }
    TotalSizeAfterSlicing_ += dupChunksTotalSize / 2;
}

void TReshardPivotKeysBuilder::RecalculateExpectedTabletSize()
{
    ExpectedTabletSize_ = DivCeil<i64>(TotalSizeAfterSlicing_, TabletCount_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
