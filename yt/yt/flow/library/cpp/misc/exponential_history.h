#pragma once

#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Appends |item| and thins |history| so that every merged element spans at most 1/|thinningFactor| of its age.
//! Element i covers (getTime(history[i - 1]), getTime(history[i])]; history[0] is an anchor that is never merged.
//! |merge(older, newer)| folds two adjacent elements into one.
template <class T, class TGetTime, class TMerge>
void AppendToExponentialHistory(
    std::vector<T>* history,
    T item,
    const TGetTime& getTime,
    const TMerge& merge,
    int thinningFactor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define EXPONENTIAL_HISTORY_INL_H_
#include "exponential_history-inl.h"
#undef EXPONENTIAL_HISTORY_INL_H_
