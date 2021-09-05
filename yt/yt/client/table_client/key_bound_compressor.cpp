#include "key_bound_compressor.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/logging/fluent_log.h>

namespace NYT::NTableClient {

using namespace NLogging;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TKeyBoundCompressor::TKeyBoundCompressor(const TComparator& comparator)
    : Comparator_(comparator)
{ }

void TKeyBoundCompressor::Add(TKeyBound keyBound)
{
    YT_VERIFY(keyBound);

    AddedKeyBounds_.insert(keyBound);
}

void TKeyBoundCompressor::InitializeMapping()
{
    YT_VERIFY(!MappingInitialized_);
    MappingInitialized_ = true;

    SortedKeyBounds_.reserve(AddedKeyBounds_.size());
    SortedKeyBounds_.insert(SortedKeyBounds_.end(), AddedKeyBounds_.begin(), AddedKeyBounds_.end());
    std::sort(
        SortedKeyBounds_.begin(),
        SortedKeyBounds_.end(),
        [&] (const TKeyBound& lhs, const TKeyBound& rhs) {
            return Comparator_.CompareKeyBounds(lhs, rhs) < 0;
        });

    // Prepare images.
    for (const auto& keyBound : AddedKeyBounds_) {
        Mapping_[keyBound];
    }

    // Prepare component-wise image prefixes.
    ComponentWisePrefixes_.resize(SortedKeyBounds_.size());
    for (ssize_t index = 0; index < ssize(SortedKeyBounds_); ++index) {
        ComponentWisePrefixes_[index] = RowBuffer_->AllocateUnversioned(SortedKeyBounds_[index].Prefix.GetCount());
    }

    // First, calculate global images.
    for (
        ssize_t beginIndex = 0, endIndex = 0, currentImage = 0;
        beginIndex < ssize(SortedKeyBounds_);
        beginIndex = endIndex, ++currentImage)
    {
        while (
            endIndex < ssize(SortedKeyBounds_) &&
            Comparator_.CompareKeyBounds(SortedKeyBounds_[beginIndex], SortedKeyBounds_[endIndex]) == 0)
        {
            Mapping_[SortedKeyBounds_[endIndex]].Global = currentImage;
            ++endIndex;
        }
    }

    // Second, calculate component-wise images.
    CalculateComponentWise(/*fromIndex*/ 0, /*toIndex*/ SortedKeyBounds_.size(), /*componentIndex*/ 0);
    for (ssize_t index = 0; index < ssize(SortedKeyBounds_); ++index) {
        const auto& keyBound = SortedKeyBounds_[index];
        auto& componentWiseImage = Mapping_[keyBound].ComponentWise;
        componentWiseImage.Prefix = ComponentWisePrefixes_[index];
        componentWiseImage.IsInclusive = SortedKeyBounds_[index].IsInclusive;
        componentWiseImage.IsUpper = SortedKeyBounds_[index].IsUpper;
    }
}

void TKeyBoundCompressor::CalculateComponentWise(ssize_t fromIndex, ssize_t toIndex, ssize_t componentIndex)
{
    if (fromIndex == toIndex || componentIndex == Comparator_.GetLength()) {
        return;
    }
    for (
        ssize_t beginIndex = fromIndex, endIndex = fromIndex, currentImage = 0;
        beginIndex < toIndex;
        beginIndex = endIndex, ++currentImage)
    {
        // Skip a bunch of key bounds that are too short to have currently processed component.
        while (
            beginIndex < toIndex &&
            SortedKeyBounds_[beginIndex].Prefix.GetCount() <= componentIndex)
        {
            ++beginIndex;
        }
        endIndex = beginIndex;

        while (
            endIndex < toIndex &&
            SortedKeyBounds_[endIndex].Prefix.GetCount() > componentIndex &&
            SortedKeyBounds_[endIndex].Prefix[componentIndex] == SortedKeyBounds_[beginIndex].Prefix[componentIndex])
        {
            ComponentWisePrefixes_[endIndex][componentIndex] = MakeUnversionedInt64Value(currentImage, componentIndex);
            ++endIndex;
        }

        CalculateComponentWise(beginIndex, endIndex, componentIndex + 1);
    }
}

TKeyBoundCompressor::TImage TKeyBoundCompressor::GetImage(TKeyBound keyBound) const
{
    YT_VERIFY(MappingInitialized_);
    return GetOrCrash(Mapping_, keyBound);
}

void TKeyBoundCompressor::Dump(const TLogger& logger)
{
    constexpr i64 batchDataWeightLimit = 10_KB;

    std::vector<std::pair<TKeyBound, TImage>> batch;
    i64 currentDataWeight = 0;

    // Split rows into batches by data weight in order to avoid extremely long log lines.
    auto flush = [&] {
        if (batch.empty()) {
            return;
        }

        LogStructuredEventFluently(logger, ELogLevel::Info)
            .Item("batch")
                .DoListFor(batch, [] (TFluentList fluent, std::pair<TKeyBound, TImage> pair) {
                    fluent
                        .Item()
                            .BeginList()
                                .Item().Value(pair.first)
                                .Item().Value(pair.second.ComponentWise)
                                .Item().Value(pair.second.Global)
                            .EndList();
                });

        batch.clear();
        currentDataWeight = 0;
    };

    for (const auto& keyBound : SortedKeyBounds_) {
        auto image = GetImage(keyBound);
        batch.emplace_back(keyBound, image);
        currentDataWeight += GetDataWeight(keyBound.Prefix) + GetDataWeight(image.ComponentWise.Prefix);
        if (currentDataWeight > batchDataWeightLimit) {
            flush();
        }
    }
    flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
