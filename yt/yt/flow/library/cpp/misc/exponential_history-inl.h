#pragma once

#ifndef EXPONENTIAL_HISTORY_INL_H_
    #error "Direct inclusion of this file is not allowed, include exponential_history.h"
    // For the sake of sane code completion.
    #include "exponential_history.h"
#endif

#include <iterator>
#include <utility>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TGetTime, class TMerge>
void AppendToExponentialHistory(
    std::vector<T>* history,
    T item,
    const TGetTime& getTime,
    const TMerge& merge,
    int thinningFactor)
{
    history->push_back(std::move(item));

    if (std::ssize(*history) < 3) {
        return;
    }

    const auto now = getTime(history->back());

    // Merging is decided by time span against age, so a long gap stays a single element
    // until it is |thinningFactor| times older than its span.
    int last = 1;
    for (int index = 2; index < std::ssize(*history); ++index) {
        const auto newerTime = getTime((*history)[index]);
        if ((newerTime - getTime((*history)[last - 1])) * thinningFactor <= now - newerTime) {
            (*history)[last] = merge((*history)[last], (*history)[index]);
        } else {
            ++last;

            if (last != index) {
                (*history)[last] = std::move((*history)[index]);
            }
        }
    }

    history->erase(history->begin() + last + 1, history->end());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
