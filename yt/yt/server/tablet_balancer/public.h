#pragma once

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TTableId;
using NTabletClient::TTabletCellId;
using NTabletClient::TTabletId;
using NTabletClient::TTabletActionId;
using NTabletClient::ETabletActionKind;
using NTabletClient::ETabletActionState;

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

DECLARE_REFCOUNTED_STRUCT(TStandaloneTabletBalancerConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletBalancerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TActionManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletBalancerBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TTabletBalancerProgramConfig)

DECLARE_REFCOUNTED_STRUCT(ITabletBalancer)

struct TTableProfilingCounters;

DECLARE_REFCOUNTED_STRUCT(TBundleProfilingCounters)
DECLARE_REFCOUNTED_CLASS(TBundleState)

DECLARE_REFCOUNTED_CLASS(TTabletAction)
DECLARE_REFCOUNTED_STRUCT(IActionManager)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

YT_DEFINE_ERROR_ENUM(
    ((IncorrectConfig)                                  (2901))
    ((StatisticsFetchFailed)                            (2902))
    ((ParameterizedBalancingFailed)                     (2903))

    ((ScheduleFormulaEvaluationFailed)                  (2910))
    ((GroupActionLimitExceeded)                         (2911))
);

DECLARE_REFCOUNTED_CLASS(TTableRegistry)

DECLARE_REFCOUNTED_STRUCT(IReshardIteration)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
