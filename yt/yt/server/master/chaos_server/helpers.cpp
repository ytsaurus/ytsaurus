#include "helpers.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

TErrorOr<int> GetMinimalTabletCount(std::vector<TErrorOr<int>> tabletCounts)
{
    if (tabletCounts.empty()) {
        return 0;
    }

    std::erase_if(
        tabletCounts,
        [] (const auto& tabletCount) {
            return !tabletCount.IsOK();
        });

    auto minElementIt = std::min_element(
        tabletCounts.begin(),
        tabletCounts.end(),
        [] (const auto& a, const auto& b) {
            return a.Value() < b.Value();
        });

    if (minElementIt == tabletCounts.end()) {
        return TError("Failed to get tablet count from any replica");
    }

    return minElementIt->Value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
