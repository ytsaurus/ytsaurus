#pragma once

#include <yt/core/profiling/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagIdList GetUserProfilerTags(NProfiling::TTagIdList tags, const TString& user);

struct TTabletProfilerTraitBase
{
    using TKey = ui64;

    static ui64 ToKey(const NProfiling::TTagIdList& list);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
