#include "helpers.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

THashSet<int> GetDiskQuotaMedia(const TDiskQuota& diskQuota)
{
    THashSet<int> media;
    for (const auto& [index, _] : diskQuota.DiskSpacePerMedium) {
        media.insert(index);
    }
    return media;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
