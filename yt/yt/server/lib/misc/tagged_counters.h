#include <yt/yt/library/profiling/tag.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T = int>
class TTaggedCounters
{
public:
    T Increment(const NProfiling::TTagSet& tagSet)
    {
        auto guard = Guard(Lock_);
        return ++Counters_[tagSet.Tags()];
    }

    T Decrement(const NProfiling::TTagSet& tagSet)
    {
        auto guard = Guard(Lock_);
        return --Counters_[tagSet.Tags()];
    }

    T Get(const NProfiling::TTagSet& tagSet)
    {
        auto guard = Guard(Lock_);
        return Counters_[tagSet.Tags()];
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<NProfiling::TTagList, T> Counters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

