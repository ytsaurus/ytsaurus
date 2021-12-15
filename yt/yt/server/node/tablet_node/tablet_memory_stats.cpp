#include "tablet_memory_stats.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void operator+=(TMemoryStats::TValue& dest, const TMemoryStats::TValue& src)
{
    dest.Usage += src.Usage;
    dest.Limit += src.Limit;
}

void operator+=(TMemoryStats& dest, const TMemoryStats& src)
{
    dest.Dynamic += src.Dynamic;
    dest.DynamicBacking += src.DynamicBacking;
    dest.Static += src.Static;
    dest.RowCache += src.RowCache;
    dest.PreloadStoreCount += src.PreloadStoreCount;
    dest.PendingStoreCount += src.PendingStoreCount;
    dest.PendingStoreBytes += src.PendingStoreBytes;
    dest.PreloadStoreFailedCount += src.PreloadStoreFailedCount;

    static const int MaxErrorsMessagesToKeep = 10;

    if (std::ssize(dest.PreloadErrors) < MaxErrorsMessagesToKeep) {
        dest.PreloadErrors.insert(
            dest.PreloadErrors.end(),
            src.PreloadErrors.begin(), 
            src.PreloadErrors.end());
        dest.PreloadErrors.resize(std::min<int>(MaxErrorsMessagesToKeep, std::ssize(dest.PreloadErrors)));
    }
}

TNodeMemoryUsageSummary CalcNodeMemoryUsageSummary(const std::vector<TTabletCellMemoryStats>& rawStats)
{
    TNodeMemoryUsageSummary result;

    for (const auto& cell : rawStats) {
        auto& bundleSummary = result.Bundles[cell.BundleName];
        auto& cellSummary = bundleSummary.TabletCells[cell.CellId];

        for (const auto& tablet : cell.Tablets) {
            result.Overall += tablet.Stats;
            result.Tables[tablet.TablePath] += tablet.Stats;
            bundleSummary.Overall += tablet.Stats;
            cellSummary += tablet.Stats;
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

}
