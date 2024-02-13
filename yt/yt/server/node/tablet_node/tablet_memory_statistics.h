#pragma once

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// General tablet memory usage info.
struct TMemoryStatistics
{
    struct TValue
    {
        i64 Usage = 0;
        std::optional<i64> Limit;
    };

    i64 DynamicActive = 0;
    i64 DynamicPassive = 0;
    i64 DynamicBacking = 0;

    // Makes sense only at top levels (total and per-bundle).
    std::optional<TValue> Dynamic;

    TValue Static;

    TValue RowCache;

    i64 PreloadStoreCount = 0;
    i64 PreloadPendingStoreCount = 0;
    i64 PreloadPendingBytes = 0;
    i64 PreloadFailedStoreCount = 0;

    std::vector<TError> PreloadErrors;
};

// Raw memory statistics for tablet.
struct TTabletMemoryStatistics
{
    TTabletId TabletId;
    NYPath::TYPath TablePath;
    TMemoryStatistics Statistics;
};

// Raw memory statistics for cell.
struct TTabletCellMemoryStatistics
{
    TTabletCellId CellId;
    TString BundleName;

    std::vector<TTabletMemoryStatistics> Tablets;
};

// Aggregated bundle statistics.
struct TBundleMemoryUsageSummary
{
    TMemoryStatistics Total;

    // Per tablet cells memory usage summary.
    THashMap<TTabletCellId, TMemoryStatistics> TabletCells;
};

// Aggregated tablet node statistics.
struct TNodeMemoryUsageSummary
{
    TMemoryStatistics Total;

    // Per bundle memory usage summary.
    THashMap<TString /*bundleName*/, TBundleMemoryUsageSummary> Bundles;

    // Per table memory usage summary.
    THashMap<TString /*tablePath*/, TMemoryStatistics> Tables;

    THashMap<TString, TString> TablePathToBundleName;
};


TNodeMemoryUsageSummary CalculateNodeMemoryUsageSummary(const std::vector<TTabletCellMemoryStatistics>& rawStatistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
