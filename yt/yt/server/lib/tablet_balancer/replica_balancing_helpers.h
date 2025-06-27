#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

std::vector<std::pair<int, int>> GetCommonKeyIndices(
    const std::vector<NTableClient::TLegacyOwningKey>& majorTablePivotKeys,
    const std::vector<NTableClient::TLegacyOwningKey>& minorTablePivotKeys,
    const NLogging::TLogger& logger = {},
    bool enableVerboseLogging = false);

std::vector<double> GetCumulativeDistribution(
    TRange<i64> sizes,
    const NLogging::TLogger& logger = {},
    bool enableVerboseLogging = false);

// Distribute metrics from minor tablets onto major tablets using their size distributions.
// Within a single minor tablet, its metric's distribution is assumed to be uniform.
// Note that the distribution in a situation like this would be contrary to the sorting order of the pivot keys.
// minor table: [] ......100mb...... [2] ..10mb.. [5]; distribution: 0, 10/11, 1
// major table: [] ...10mb.. [3] ......100mb..... [5]; distribution: 0,  1/11, 1
// minor metrics:  [.......m0......]     [..m1..]
// minor by major: [1/10 m0]     [ 9/10 m0 + m1 ]
std::vector<double> CalculateMajorMetricsBetweenSamePivots(
    const TRange<double>& minorTableMetrics,
    const TRange<i64>& majorTabletSizes,
    const TRange<i64>& minorTabletSizes,
    const NLogging::TLogger& logger = {},
    bool enableVerboseLogging = false);

std::vector<double> CalculateMajorMetrics(
    const std::vector<double>& minorTableMetrics,
    const std::vector<i64>& majorTabletSizes,
    const std::vector<i64>& minorTabletSizes,
    const std::vector<NTableClient::TLegacyOwningKey>& majorTablePivotKeys,
    const std::vector<NTableClient::TLegacyOwningKey>& minorTablePivotKeys,
    const NLogging::TLogger& logger = {},
    bool enableVerboseLogging = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
