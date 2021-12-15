#pragma once

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ypath/public.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <vector>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// General tablet memory usage info.
struct TMemoryStats
{
    struct TValue
    {
        i64 Usage = 0;
        i64 Limit = 0;
    };

    TValue Dynamic;
    TValue DynamicBacking;
    TValue Static;

    TValue RowCache;

    i64 PreloadStoreCount = 0;
    i64 PendingStoreCount = 0;
    i64 PendingStoreBytes = 0;
    i64 PreloadStoreFailedCount = 0;

    std::vector<TError> PreloadErrors;
};

// Raw memory stats for tablet.
struct TTabletMemoryStats
{
    NTabletClient::TTabletId TabletId;
    NYPath::TYPath TablePath; 
    TMemoryStats Stats;
};

// Raw memory stats for cell.
struct TTabletCellMemoryStats
{
    NTabletClient::TTabletCellId CellId;
    TString BundleName;
    
    std::vector<TTabletMemoryStats> Tablets;
};

// Aggregated bundle stats.
struct TBundleMemoryUsageSummary
{
    TMemoryStats Overall;

    // Per tablet cells memory usage summary.
    THashMap<NTabletClient::TTabletCellId, TMemoryStats> TabletCells;
};

// Aggregated tablet node stats.
struct TNodeMemoryUsageSummary
{
    TMemoryStats Overall;

    // Per bundle memory usage summary.
    THashMap<TString /* bundle name */, TBundleMemoryUsageSummary> Bundles;

    // Per table memory usage summary.
    THashMap<TString /* table path */, TMemoryStats> Tables;
};


TNodeMemoryUsageSummary CalcNodeMemoryUsageSummary(const std::vector<TTabletCellMemoryStats>& rawStats);

////////////////////////////////////////////////////////////////////////////////

}
