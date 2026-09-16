#include "tablet_memory_statistics.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void operator+=(TMemoryStatistics::TValue& dest, const TMemoryStatistics::TValue& src)
{
    dest.Usage += src.Usage;
    if (dest.Limit || src.Limit) {
        dest.Limit = dest.Limit.value_or(0) + src.Limit.value_or(0);
    }
}

void operator+=(TMemoryStatistics& dest, const TMemoryStatistics& src)
{
    if (dest.Dynamic) {
        if (src.Dynamic) {
            *dest.Dynamic += *src.Dynamic;
        }
    } else {
        dest.Dynamic = src.Dynamic;
    }

    dest.DynamicActive += src.DynamicActive;
    dest.DynamicPassive += src.DynamicPassive;
    dest.DynamicBacking += src.DynamicBacking;
    dest.Static += src.Static;
    dest.RowCache += src.RowCache;
    dest.PreloadStoreCount += src.PreloadStoreCount;
    dest.PreloadPendingStoreCount += src.PreloadPendingStoreCount;
    dest.PreloadPendingBytes += src.PreloadPendingBytes;
    dest.PreloadFailedStoreCount += src.PreloadFailedStoreCount;

    static constexpr int MaxErrorsMessagesToKeep = 10;

    if (std::ssize(dest.PreloadErrors) < MaxErrorsMessagesToKeep) {
        dest.PreloadErrors.insert(
            dest.PreloadErrors.end(),
            src.PreloadErrors.begin(),
            src.PreloadErrors.end());
        dest.PreloadErrors.resize(std::min<int>(MaxErrorsMessagesToKeep, std::ssize(dest.PreloadErrors)));
    }
}

TNodeMemoryUsageSummary CalculateNodeMemoryUsageSummary(const std::vector<TTabletCellMemoryStatistics>& rawStatistics)
{
    TNodeMemoryUsageSummary result;

    for (const auto& cell : rawStatistics) {
        auto& bundleSummary = result.Bundles[cell.BundleName];
        auto& cellSummary = bundleSummary.TabletCells[cell.CellId];

        for (const auto& tablet : cell.Tablets) {
            result.Total += tablet.Statistics;
            result.Tables[tablet.TablePath] += tablet.Statistics;
            result.TablePathToBundleName[tablet.TablePath] = cell.BundleName;
            bundleSummary.Total += tablet.Statistics;
            cellSummary += tablet.Statistics;
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
