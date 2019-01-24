#pragma once
#ifndef LOCK_FREE_INL_H_
#error "Direct inclusion of this file is not allowed, include profiling_helpers.h"
// For the sake of sane code completion.
#include "profiling_helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename TCountersKey, typename TCounters>
typename TProfilerTrait<TCountersKey, TCounters>::TKey TProfilerTrait<TCountersKey, TCounters>::ToKey(const TKey& key)
{
    return key;
}

template <typename TCountersKey, typename TCounters>
typename TProfilerTrait<TCountersKey, TCounters>::TValue TProfilerTrait<TCountersKey, TCounters>::ToValue(const TKey& key)
{
    return TCounters{key};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
