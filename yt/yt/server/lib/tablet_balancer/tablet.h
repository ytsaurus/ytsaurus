#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletStatistics
{
    i64 CompressedDataSize;
    i64 UncompressedDataSize;
    i64 MemorySize;

    int PartitionCount;

    NYTree::INodePtr OriginalNode;
};

////////////////////////////////////////////////////////////////////////////////

struct TTablet final
{
    const TTabletId Id;
    const TTable* Table;

    i64 Index;
    TTabletCell* Cell = nullptr;

    TTabletStatistics Statistics;
    NYTree::INodePtr PerformanceCounters;
    ETabletState State;

    TTablet(
        TTabletId tabletId,
        TTable* table);
};

DEFINE_REFCOUNTED_TYPE(TTablet)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
