#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTable final
{
    const bool Sorted;
    const NYPath::TYPath Path;
    const NObjectClient::TCellTag ExternalCellTag;
    const TTabletCellBundle* Bundle;
    const TTableId Id;

    bool Dynamic;

    i64 CompressedDataSize;
    i64 UncompressedDataSize;
    i64 DataWeight;

    EInMemoryMode InMemoryMode;
    TTableTabletBalancerConfigPtr TableConfig;
    std::vector<TTabletPtr> Tablets;

    TTable(
        bool sorted,
        NYPath::TYPath path,
        NObjectClient::TCellTag cellTag,
        TTableId tableId,
        TTabletCellBundle* bundle);

    bool IsBalancingAllowed() const;
};

DEFINE_REFCOUNTED_TYPE(TTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
