#pragma once

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NTabletBalancer::NDryRun {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TTableId;
using NTabletClient::EInMemoryMode;
using NTabletClient::TTabletCellId;
using NTabletClient::TTabletId;

using NTabletBalancer::EBalancingMode;

DECLARE_REFCOUNTED_STRUCT(TBundleHolder)
DECLARE_REFCOUNTED_STRUCT(TNodeHolder)
DECLARE_REFCOUNTED_STRUCT(TTableHolder)
DECLARE_REFCOUNTED_STRUCT(TTabletHolder)
DECLARE_REFCOUNTED_STRUCT(TTabletCellHolder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer::NDryRun
