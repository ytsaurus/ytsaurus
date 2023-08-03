#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TTableRegistry final
{
public:
    using TTableMap = THashMap<TTableId, TTablePtr>;
    DEFINE_BYREF_RO_PROPERTY(TTableMap, Tables);

public:
    void AddTable(const TTablePtr& table);
    void RemoveTable(const TTableId& tableId);

private:
    void UnlinkTableFromOldBundle(const TTablePtr& table);
    void UnlinkTabletFromCell(const TTabletPtr& tablet);
};

DEFINE_REFCOUNTED_TYPE(TTableRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
