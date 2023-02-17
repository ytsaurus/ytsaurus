#pragma once

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/misc/intrusive_ptr.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TTableId;
using NTabletClient::EInMemoryMode;
using NTabletClient::ETabletState;
using NTabletClient::TTabletCellId;
using NTabletClient::TTabletId;

DECLARE_REFCOUNTED_CLASS(TTableTabletBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TBundleTabletBalancerConfig)

struct TTabletStatistics;
struct TTabletCellStatistics;

DECLARE_REFCOUNTED_STRUCT(TTable)
DECLARE_REFCOUNTED_STRUCT(TTablet)
DECLARE_REFCOUNTED_STRUCT(TTabletCell)
DECLARE_REFCOUNTED_STRUCT(TTabletCellBundle)

struct TReshardDescriptor;
struct TMoveDescriptor;
using TActionDescriptor = std::variant<TMoveDescriptor, TReshardDescriptor>;
using TNodeAddress = TString;

DECLARE_REFCOUNTED_STRUCT(IParameterizedReassignSolver)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
