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
};

void Deserialize(TTabletStatistics& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TTablet final
{
    const TTabletId Id;
    const TTable* Table;

    i64 Index;
    TTabletCell* Cell = nullptr;

    TTabletStatistics Statistics;
    ETabletState State;

    TTablet(
        TTabletId tabletId,
        TTable* table);
};

DEFINE_REFCOUNTED_TYPE(TTablet)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
