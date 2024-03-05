#pragma once

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TTableId;
using NTabletClient::EInMemoryMode;
using NTabletClient::ETabletCellLifeStage;
using NTabletClient::ETabletState;
using NTabletClient::TTabletCellId;
using NTabletClient::TTabletId;

DECLARE_REFCOUNTED_CLASS(TTableTabletBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TBundleTabletBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TMasterTableTabletBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TMasterBundleTabletBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletBalancingGroupConfig)
DECLARE_REFCOUNTED_CLASS(TParameterizedBalancingConfig)

DEFINE_ENUM(EBalancingType,
    ((Legacy)         (0))
    ((Parameterized)  (1))
);

DEFINE_ENUM(EBalancingMode,
    (ParameterizedMove)
    (InMemoryMove)
    (OrdinaryMove)
    (Reshard)
    (ParameterizedReshard)
);

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
using TGroupName = TString;

struct TParameterizedReassignSolverConfig;

DECLARE_REFCOUNTED_STRUCT(TTableParameterizedMetricTracker)
DECLARE_REFCOUNTED_STRUCT(IParameterizedReassignSolver)
DECLARE_REFCOUNTED_STRUCT(IParameterizedResharder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
