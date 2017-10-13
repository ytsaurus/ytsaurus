#pragma once

#include <yt/core/profiling/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// Adds user tags to specified tags and returns them.
NProfiling::TTagIdList GetUserProfilerTags(const TString& user, NProfiling::TTagIdList tags = {});

////////////////////////////////////////////////////////////////////////////////

// Trait to deal with tablet tags list and user tag.
struct TTabletProfilerTraitBase
{
    using TKey = ui64;

    static ui64 ToKey(const NProfiling::TTagIdList& list);
};

////////////////////////////////////////////////////////////////////////////////

// Trait to deal with simple tags. Example: only user tag or only tablet tags.
struct TSimpleProfilerTraitBase
{
    using TKey = NProfiling::TTagId;

    static TKey ToKey(const NProfiling::TTagIdList& list);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBase, typename TCounters>
struct TProfilerTrait
    : public TBase
{
    using TValue = TCounters;

    static TValue ToValue(const NProfiling::TTagIdList& list)
    {
        return {list};
    }
};

template <typename TCounters>
using TSimpleProfilerTrait = TProfilerTrait<TSimpleProfilerTraitBase, TCounters>;

template <typename TCounters>
using TTabletProfilerTrait = TProfilerTrait<TTabletProfilerTraitBase, TCounters>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
