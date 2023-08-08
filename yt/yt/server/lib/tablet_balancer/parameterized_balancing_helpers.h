#pragma once

#include "public.h"

#include <yt/yt/core/logging/public.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

//! The ultimate goal of this class is to evenly distribute tablets between cells.
//!
//! A metric is calculated for each tablet based on its statistics and performance counters.
//! Then the cell metric is defined as the square of the sum of metrics of tablets belonging to this cell.
//! Tablets are moved in order to minimize total cell metric.
//!
//! There are two kinds of actions: move a tablet to another cell and swap two tablets from different cells.
//! On every step we greedily pick the action which minimizes total metric the most and repeat
//! until maxMoveActionCount is reached.
struct IParameterizedReassignSolver
    : public TRefCounted
{
    virtual std::vector<TMoveDescriptor> BuildActionDescriptors() = 0;
};

DEFINE_REFCOUNTED_TYPE(IParameterizedReassignSolver)

////////////////////////////////////////////////////////////////////////////////

struct TParameterizedReassignSolverConfig
{
    bool EnableSwaps = true;
    int MaxMoveActionCount = 0;
    double NodeDeviationThreshold = 0;
    double CellDeviationThreshold = 0;
    double MinRelativeMetricImprovement = 0;
    TString Metric;

    TParameterizedReassignSolverConfig MergeWith(const TParameterizedBalancingConfigPtr& groupConfig) const;
};

bool IsTableMovable(TTableId tableId);

IParameterizedReassignSolverPtr CreateParameterizedReassignSolver(
    TTabletCellBundlePtr bundle,
    std::vector<TString> performanceCountersKeys,
    TParameterizedReassignSolverConfig config,
    TString groupName,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
