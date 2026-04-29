#pragma once

#include "public.h"
#include "tablet.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TReshardDescriptor final
{
    std::vector<TTabletId> Tablets;
    int TabletCount;
    i64 DataSize;
    TGuid CorrelationId;
    std::vector<NTableClient::TLegacyOwningKey> PivotKeys;
    bool Inplace = false;
    TTabletCellId TargetCellId;
    // Tablets that should be moved to target cell before reshard can be executed.
    THashSet<TTabletId> PendingTabletIds;
    bool UseSmoothMovementToUniteTablets = false;

    // IsSplit, TabletCountDiff, Deviation.
    std::tuple<bool, int, double> Priority;

    bool operator<(const TReshardDescriptor& descriptor) const;
};

struct TMoveDescriptor
{
    TTabletId TabletId;
    TTabletCellId TabletCellId;
    TGuid CorrelationId;
    bool Smooth = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletSizeConfig
{
    i64 MinTabletSize = 0;
    i64 MaxTabletSize = 0;
    i64 DesiredTabletSize = 0;
    std::optional<int> MinTabletCount;
};

////////////////////////////////////////////////////////////////////////////////

bool IsTabletReshardable(const TTabletPtr& tablet);

TTabletSizeConfig GetTabletSizeConfig(
    const TTable* table,
    i64 minDesiredTabletSize,
    const NLogging::TLogger& logger = {});

i64 GetTabletBalancingSize(const TTabletPtr& tablet);

////////////////////////////////////////////////////////////////////////////////

std::vector<TReshardDescriptor> MergeSplitTabletsOfTable(
    std::vector<TTabletPtr> tablets,
    i64 minDesiredTabletSize,
    bool pickPivotKeys = true,
    const NLogging::TLogger& logger = {});

// Reshard minor table tablets as close as possible to the major table's pivot keys,
// respecting the per-action tablet limit.
std::vector<TReshardDescriptor> MergeSplitReplicaTable(
    const TTablePtr& minorTable,
    const TAlienTablePtr& majorTable,
    int maxTabletCountPerAction,
    NLogging::TLogger logger = {});

std::vector<TMoveDescriptor> ReassignInMemoryTablets(
    const TTabletCellBundlePtr& bundle,
    const std::optional<THashSet<TTableId>>& movableTables,
    bool ignoreTableWiseConfig,
    const NLogging::TLogger& logger = {});

std::vector<TMoveDescriptor> ReassignOrdinaryTablets(
    const TTabletCellBundlePtr& bundle,
    const std::optional<THashSet<TTableId>>& movableTables,
    const NLogging::TLogger& logger = {});

std::vector<TMoveDescriptor> ReassignTabletsParameterized(
    const TTabletCellBundlePtr& bundle,
    const std::vector<std::string>& performanceCountersKeys,
    const TParameterizedReassignSolverConfig& config,
    const TGroupName& groupName,
    const TTableParameterizedMetricTrackerPtr& metricTracker,
    const NLogging::TLogger& logger = {});

std::vector<TMoveDescriptor> ReassignTabletsReplica(
    const TTabletCellBundlePtr& bundle,
    const std::vector<std::string>& performanceCountersKeys,
    const TParameterizedReassignSolverConfig& config,
    const TGroupName& groupName,
    const TTableParameterizedMetricTrackerPtr& metricTracker,
    const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

void ApplyMoveTabletAction(const TTabletPtr& tablet, const TTabletCellId& cell);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
