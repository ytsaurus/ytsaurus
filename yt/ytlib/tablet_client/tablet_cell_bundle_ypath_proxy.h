#pragma once

#include "public.h"

#include <yt/ytlib/tablet_client/proto/tablet_cell_bundle_ypath.pb.h>

#include <yt/core/ytree/ypath_client.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellBundleYPathProxy
{
    DEFINE_YPATH_PROXY(TabletCellBundle);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, BalanceTabletCells);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
