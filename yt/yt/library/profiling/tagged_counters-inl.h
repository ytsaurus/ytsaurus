#pragma once

#ifndef TAGGED_COUNTERS_INL_H_
#error "Direct inclusion of this file is not allowed, include tagged_counters.h"
#endif

#include <util/system/guard.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T TTaggedCounters<T>::Increment(const NProfiling::TTagSet& tagSet)
{
    auto guard = Guard(Lock_);
    return ++Counters_[tagSet.Tags()];
}

template <typename T>
T TTaggedCounters<T>::Decrement(const NProfiling::TTagSet& tagSet)
{
    auto guard = Guard(Lock_);
    return --Counters_[tagSet.Tags()];
}

template <typename T>
T TTaggedCounters<T>::Get(const NProfiling::TTagSet& tagSet)
{
    auto guard = Guard(Lock_);
    return Counters_[tagSet.Tags()];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
