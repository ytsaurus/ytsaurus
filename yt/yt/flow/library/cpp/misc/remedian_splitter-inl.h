#pragma once

#ifndef REMEDIAN_SPLITTER_INL_H_
    #error "Direct inclusion of this file is not allowed, include remedian_splitter.h"
    // For the sake of sane code completion.
    #include "remedian_splitter.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TCompare>
TRemedianSplitter<T, TCompare>::TRemedianSplitter(ssize_t partCount, ssize_t desiredWindowSize, const TCompare& compare)
    : SplitterCount_(partCount - 1)
    , Compare_(compare)
{
    // Constraints.
    static_assert(MinInnerSizeMultiplier % 2 == 0);
    YT_VERIFY(partCount >= 2);
    YT_VERIFY(desiredWindowSize > 0);

    // Calculate node sizes as a first approximation.
    LeafSizeMultiplier_ = MinLeafSizeMultiplier;
    LeafSize_ = LeafSizeMultiplier_ * partCount;
    if (LeafSize_ < DesiredNodeSize) {
        LeafSizeMultiplier_ = DesiredNodeSize / partCount;
        LeafSize_ = LeafSizeMultiplier_ * partCount;
    }
    ssize_t innerSizeMultiplier = MinInnerSizeMultiplier;
    ssize_t innerSize = innerSizeMultiplier * partCount + 1;
    if (innerSize < DesiredNodeSize) {
        innerSizeMultiplier = DesiredNodeSize / partCount;
        innerSize = innerSizeMultiplier * partCount + 1;
    }

    // Calculate InnerHeight_ and RealWindowSize_.
    InnerHeight_ = 1;
    RealWindowSize_ = LeafSize_ * innerSize;
    ssize_t sizeLeft = desiredWindowSize / RealWindowSize_;
    while (sizeLeft >= innerSize) {
        InnerHeight_++;
        RealWindowSize_ *= innerSize;
        sizeLeft /= innerSize;
    }

    // Resize Inner_ and set sizes.
    InnerLevels_.resize(InnerHeight_);
    for (auto& innerLevel : InnerLevels_) {
        innerLevel.LevelSize = innerSize;
        innerLevel.LevelSizeMultiplier = innerSizeMultiplier;
    }

    // Try to adjust level sizes in order to get more close approximation of desiredWindowSize.
    while (true) {
        bool fault = true;
        for (int k = 0; k < 2; k++) {
            // Inner level multiplier is increased by 2, so increase leaf size multiplier by 1 twice.
            ssize_t testLevelSize = (LeafSizeMultiplier_ + 1) * partCount;
            ssize_t testSize = RealWindowSize_ / LeafSize_ * testLevelSize;
            if (testSize <= desiredWindowSize) {
                RealWindowSize_ = testSize;
                LeafSizeMultiplier_++;
                LeafSize_ = testLevelSize;
                fault = false;
            }
        }
        for (auto& innerLevel : InnerLevels_) {
            // Inner level size must remain odd, so increase multiplier by 2.
            ssize_t testLevelSize = (innerLevel.LevelSizeMultiplier + 2) * partCount + 1;
            ssize_t testSize = RealWindowSize_ / innerLevel.LevelSize * testLevelSize;
            if (testSize <= desiredWindowSize) {
                RealWindowSize_ = testSize;
                innerLevel.LevelSizeMultiplier += 2;
                innerLevel.LevelSize = testLevelSize;
                innerSize = testLevelSize;
                fault = false;
            }
        }
        if (fault) {
            break;
        }
    }

    UpdateWindowSize_ = RealWindowSize_ / InnerLevels_[InnerHeight_ - 1].LevelSize;

    // Reserve/resize the rest internal structures. No more allocations will be required further.
    Result_.resize(SplitterCount_);
    Leaf_.reserve(LeafSize_);
    Temp_.reserve(innerSize);
    for (auto& innerLevel : InnerLevels_) {
        innerLevel.Quantiles.resize(SplitterCount_);
        for (auto& quantile : innerLevel.Quantiles) {
            quantile.Data.resize(innerLevel.LevelSize);
        }
    }
}

template <class T, class TCompare>
ssize_t TRemedianSplitter<T, TCompare>::GetInnerSize(ssize_t level) const
{
    return InnerLevels_[level].LevelSize;
}

template <class T, class TCompare>
ssize_t TRemedianSplitter<T, TCompare>::GetInnerSizeMultiplier(ssize_t level) const
{
    return InnerLevels_[level].LevelSizeMultiplier;
}

template <class T, class TCompare>
void TRemedianSplitter<T, TCompare>::Clear()
{
    Result_.clear();
    Result_.resize(SplitterCount_);
    Leaf_.clear();
    for (auto& innerLevel : InnerLevels_) {
        for (auto& quantile : innerLevel.Quantiles) {
            quantile.Data.clear();
            quantile.Data.resize(innerLevel.LevelSize);
        }
    }
    ResultReady_ = false;
    ResultVersion_ = 0;
    CollectLeafRound_ = 0;
}

template <class T, class TCompare>
template <std::convertible_to<T> TRef>
void TRemedianSplitter<T, TCompare>::Push(TRef&& value)
{
    PushLeaf(std::forward<TRef>(value));
}

template <class T, class TCompare>
template <std::convertible_to<T> TRef>
void TRemedianSplitter<T, TCompare>::PushLeaf(TRef&& value)
{
    Leaf_.push_back(std::forward<TRef>(value));
    if (std::ssize(Leaf_) != LeafSize_) {
        return;
    }

    // Actually we could find splitters by calling std::nth_element SplitterCount_ times.
    // If carefully done (find central splitter first, use halfranges then etc) that would cost LeafSize * log(PartCount_).
    // But simple std::sort is much simpler and will cost:
    //  LeafSize * log(LeafSize) = LeafSize * log(PartCount_ * const) = LeafSize * log(PartCount_) + LeafSize * log(const).
    // So it's not a big difference.
    std::ranges::sort(Leaf_, Compare_);

    for (ssize_t i = 0; i < SplitterCount_; i++) {
        PushInner(std::move(Leaf_[(i + 1) * LeafSizeMultiplier_ - (CollectLeafRound_ & 1)]), 0, i);
    }
    CollectLeafRound_++;
    Leaf_.clear();
}

template <class T, class TCompare>
template <std::convertible_to<T> TRef>
void TRemedianSplitter<T, TCompare>::PushInner(TRef&& value, ssize_t level, ssize_t quantile)
{
    auto& inner = InnerLevels_[level].Quantiles[quantile];
    inner.Data[inner.WritePos++] = std::forward<TRef>(value);

    // For inner levels there's a dilemma:
    //  for random data the most accurate result can be achieved by taking median; taking appropriate quantile is several times worse.
    //  for ordered data the perfect result will be achieved by taking appropriate quantile; median doesn't work at all.
    // So the solution is to measure the orderliness and
    auto chooseValue = [&] (std::vector<T>& data, ssize_t writePos) -> T&& {
        ssize_t nodeSize = InnerLevels_[level].LevelSize;
        ssize_t orders[2] = {0, 0};
        T* last = &data[writePos];
        for (ssize_t i = 1, pos = writePos + 1; i < nodeSize; i++, pos++) {
            if (pos >= nodeSize) {
                pos = 0;
            }
            T* current = &data[pos];
            orders[Compare_(*current, *last) ? 0 : 1]++;
            last = current;
        }
        ssize_t orderDiff = std::abs(orders[0] - orders[1]) / 2;
        ssize_t maxDiff = (nodeSize - 1) / 2;
        ssize_t medianPosition = nodeSize / 2;
        ssize_t chosenPosition = medianPosition;
        if (orderDiff * orderDiff > maxDiff) {
            ssize_t quantilePosition = (quantile + 1) * InnerLevels_[level].LevelSizeMultiplier;
            // orderDiff == 0 -> medianPosition, orderDiff == innerSize - 1 -> quantilePosition.
            chosenPosition = (maxDiff * medianPosition + orderDiff * (quantilePosition - medianPosition)) / maxDiff;
        }
        std::nth_element(data.begin(), data.begin() + chosenPosition, data.end(), Compare_);
        return std::move(data[chosenPosition]);
    };

    if (level < InnerHeight_ - 1) {
        // Intermediate level: once full, push median to the next level and clear this level.
        if (inner.WritePos == InnerLevels_[level].LevelSize) {
            // Find median in-place.
            inner.WritePos = 0;
            PushInner(chooseValue(inner.Data, inner.WritePos), level + 1, quantile);
        }
    } else {
        // Last level: once full, save median in result but don't clear the level;
        //  after that the level will be used as cycle buffer of last pushed InnerSize medians.
        if (inner.WritePos == InnerLevels_[level].LevelSize) {
            ResultReady_ = true;
            inner.WritePos = 0;
        }
        if (ResultReady_) {
            // Find median in a copy to preserve overwrite order of the last level.
            Temp_ = inner.Data;
            Result_[quantile] = chooseValue(Temp_, inner.WritePos);
            Temp_.clear();
            ResultVersion_++;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
