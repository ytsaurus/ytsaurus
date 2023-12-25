#pragma once

#include "tag.h"

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/hash.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T = int>
class TTaggedCounters
{
public:
    T Increment(const NProfiling::TTagSet& tagSet);

    T Decrement(const NProfiling::TTagSet& tagSet);

    T Get(const NProfiling::TTagSet& tagSet);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<NProfiling::TTagList, T> Counters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define TAGGED_COUNTERS_INL_H_
#include "tagged_counters-inl.h"
#undef TAGGED_COUNTERS_INL_H_
