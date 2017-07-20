#pragma once

#include <yt/core/profiling/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagIdList GetUserProfilerTags(const TString& user, NProfiling::TTagIdList tags = {});

struct TTabletProfilerTraitBase
{
    using TKey = ui64;

    static ui64 ToKey(const NProfiling::TTagIdList& list);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
