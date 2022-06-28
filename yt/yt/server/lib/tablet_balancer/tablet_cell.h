#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellStatus
{
    NTabletClient::ETabletCellHealth Health;
    bool Decommissioned;
};

void Deserialize(TTabletCellStatus& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellStatistics
{
    i64 MemorySize;
};

void Deserialize(TTabletCellStatistics& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TTabletCell final
{
    const TTabletCellId Id;

    TTabletCellStatistics Statistics;
    TTabletCellStatus Status;
    std::vector<TTabletPtr> Tablets;

    TTabletCell(
        TTabletCellId cellId,
        const TTabletCellStatistics& statistics,
        const TTabletCellStatus& status);

    bool IsAlive() const;
};

DEFINE_REFCOUNTED_TYPE(TTabletCell)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
