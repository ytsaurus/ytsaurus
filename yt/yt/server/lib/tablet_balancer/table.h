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
    TTabletCellBundle* const Bundle;
    const TTableId Id;

    bool Dynamic = false;

    i64 CompressedDataSize = 0;
    i64 UncompressedDataSize = 0;

    EInMemoryMode InMemoryMode = EInMemoryMode::None;
    TTableTabletBalancerConfigPtr TableConfig;
    std::vector<TTabletPtr> Tablets;

    TTable(
        bool sorted,
        NYPath::TYPath path,
        NObjectClient::TCellTag cellTag,
        TTableId tableId,
        TTabletCellBundle* bundle);

    std::optional<TGroupName> GetBalancingGroup() const;

    bool IsLegacyMoveBalancingEnabled() const;

    bool IsParameterizedMoveBalancingEnabled() const;
    bool IsParameterizedReshardBalancingEnabled(bool enableParameterizedByDefault) const;
};

DEFINE_REFCOUNTED_TYPE(TTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
