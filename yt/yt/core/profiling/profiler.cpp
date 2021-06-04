#include "profiler.h"
#include "profile_manager.h"
#include "timing.h"

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/farm_hash.h>

#include <yt/yt/core/ypath/token.h>

#include <util/system/sanitizers.h>

namespace NYT::NProfiling {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TTagIdList operator + (const TTagIdList& a, const TTagIdList& b)
{
    auto result = a;
    result += b;
    return result;
}

TTagIdList& operator += (TTagIdList& a, const TTagIdList& b)
{
    a.append(b.begin(), b.end());
    return a;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NProfiling::TTagIdList>::operator()(const NYT::NProfiling::TTagIdList& list) const
{
    size_t result = 1;
    for (auto tag : list) {
        result = NYT::FarmFingerprint(result, tag);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////
