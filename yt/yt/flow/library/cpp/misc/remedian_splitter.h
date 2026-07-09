#pragma once

#include "public.h"

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Streaming probabilistic data structure that accumulates a set of values
//!  that would split the stream into parts of approximately equal flux.
//! Splits the stream into given number of parts N (specified in constructor),
//!  providing N-1 splitters - values that divide the parts.
//! Works approximately with given window (specified in constructor) - approximate
//!  number of last values from the stream that are accounted for the result.
//! Based on the remedian algorithm, has an amortized cost of O(log(number of parts)) per value.
template <class T, class TCompare = std::less<T>>
class TRemedianSplitter
{
public:
    //! Construct new splitter.
    //! |partCount| - number of parts to which the stream of values must be divided.
    //! |desiredWindowSize| - approximate desired window size. Actual size may be acquired by GetRealWindowSize().
    //! |compare| - optional compare function.
    TRemedianSplitter(ssize_t partCount, ssize_t desiredWindowSize, const TCompare& compare = TCompare());

    //! Add new value from stream.
    template <std::convertible_to<T> TRef>
    void Push(TRef&& value);

    //! Call IsResultReady() to check whether the result is ready (which is exactly after first GetRealWindowSize() pushes).
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(ResultReady, false);

    //! Call Result() to get the result. If not ready, Result() may return garbage.
    DEFINE_BYREF_RO_PROPERTY(std::vector<T>, Result);

    //! Actual window size can be slightly less that desired.
    DEFINE_BYVAL_RO_PROPERTY(ssize_t, RealWindowSize);

    //! Once the result is ready it will updated after every GetUpdateWindowSize() pushed values.
    DEFINE_BYVAL_RO_PROPERTY(ssize_t, UpdateWindowSize);

    //! Initially zero, and increased when result is updated (after first GetRealWindowSize() pushes and then every GetUpdateWindowSize() pushes).
    DEFINE_BYVAL_RO_PROPERTY(ssize_t, ResultVersion);

    //! Revert the object to the initial state like it was just constructed.
    void Clear();

    //! Internal properties, exposed mostly for debug.
    //! Exposed template parameter.
    using TValueType = T;
    //! Approximate target leaf and inner node sizes.
    static constexpr ssize_t DesiredNodeSize = 16;
    //! Also leaf node must have size at least MinLeafSizeMultiplier * partCount.
    static constexpr ssize_t MinLeafSizeMultiplier = 3;
    //! Also inner node must have size at least MinInnerSizeMultiplier * partCount + 1 and odd.
    static constexpr ssize_t MinInnerSizeMultiplier = 2;
    //! Splitter const, that is partCount-1.
    DEFINE_BYVAL_RO_PROPERTY(ssize_t, SplitterCount);
    //! The first level (lead) will have this size.
    DEFINE_BYVAL_RO_PROPERTY(ssize_t, LeafSize);
    //! Also leaf size will be equal to LeafSizeMultiplier * partCount.
    DEFINE_BYVAL_RO_PROPERTY(ssize_t, LeafSizeMultiplier);
    //! Count of inner (non-leaf) levels.
    DEFINE_BYVAL_RO_PROPERTY(ssize_t, InnerHeight);
    //! One can get inner size of each inner level.
    ssize_t GetInnerSize(ssize_t level) const;
    //! Also inner size will be equal to InnerSizeMultiplier * partCount.
    ssize_t GetInnerSizeMultiplier(ssize_t level) const;

private:
    struct TInner
    {
        std::vector<T> Data;
        ssize_t WritePos = 0;
    };

    struct TInnerLevel
    {
        ssize_t LevelSize{};
        ssize_t LevelSizeMultiplier{};
        std::vector<TInner> Quantiles;
    };

    TCompare Compare_;
    ssize_t CollectLeafRound_ = 0;
    std::vector<T> Leaf_;
    std::vector<T> Temp_;
    std::vector<TInnerLevel> InnerLevels_;

    template <std::convertible_to<T> TRef>
    void PushLeaf(TRef&& value);

    template <std::convertible_to<T> TRef>
    void PushInner(TRef&& value, ssize_t level, ssize_t quantile);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define REMEDIAN_SPLITTER_INL_H_
#include "remedian_splitter-inl.h"
#undef REMEDIAN_SPLITTER_INL_H_
