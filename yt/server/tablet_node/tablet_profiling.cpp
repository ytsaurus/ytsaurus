#include "tablet_profiling.h"

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/misc/tls_cache.h>

namespace NYT {
namespace NTabletNode {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

struct TUserTagTrait
{
    using TKey = TString;
    using TValue = TTagId;

    static const TString& ToKey(const TString& user)
    {
        return user;
    }

    static TTagId ToValue(const TString& user)
    {
        return TProfileManager::Get()->RegisterTag("user", user);
    }
};

TTagIdList UserProfilerTags(TTagIdList tags, const TString& user)
{
    tags.push_back(GetLocallyCachedValue<TUserTagTrait>(user));
    return tags;
}

ui64 TTabletProfilerTraitBase::ToKey(const TTagIdList& list)
{
    /*
     * The magic is the following:
     * - front() returns tablet id tag,
     * - back()  returns user id tag.
     *
     * Those 2 tags are unique to lookup appropriate counters for specific tablet.
     */
    return (ui64(list.front()) << 32) | (ui64(list.back()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
