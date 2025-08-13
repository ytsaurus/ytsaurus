#include "replica_balancing_helpers.h"

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NTabletBalancer {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

std::vector<double> GetUniformDistribution(
    i64 size,
    const NLogging::TLogger& Logger,
    bool enableVerboseLogging)
{
    std::vector<double> distribution;
    for (int index = 0; index < size; ++index) {
        distribution.push_back(static_cast<double>(index) / size);
    }
    distribution.push_back(1.0);

    YT_LOG_DEBUG_IF(enableVerboseLogging,
        "Calculated distribution by zero sizes as uniform (Distribution: %v)",
        distribution);

    return distribution;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::vector<std::pair<int, int>> GetCommonKeyIndices(
    const std::vector<TLegacyOwningKey>& majorTableKeys,
    const std::vector<TLegacyOwningKey>& minorTableKeys,
    const NLogging::TLogger& Logger,
    bool enableVerboseLogging)
{
    std::vector<std::pair<int, int>> indices;
    for (int majorIndex = 0, minorIndex = 0;
        majorIndex < std::ssize(majorTableKeys) && minorIndex < std::ssize(minorTableKeys);)
    {
        if (majorTableKeys[majorIndex] == minorTableKeys[minorIndex]) {
            indices.emplace_back(majorIndex, minorIndex);
            ++majorIndex;
            ++minorIndex;
        } else if (majorTableKeys[majorIndex] < minorTableKeys[minorIndex]) {
            ++majorIndex;
        } else {
            ++minorIndex;
        }
    }

    // First pivot of both tables must be empty
    YT_VERIFY(!indices.empty());
    YT_VERIFY(indices.front() == std::pair(0, 0));
    indices.emplace_back(std::ssize(majorTableKeys), std::ssize(minorTableKeys));

    YT_LOG_DEBUG_IF(enableVerboseLogging,
        "Found common key indices (CommonIndices: %v, MajorKeys: %v, MinorKeys: %v)",
        indices,
        majorTableKeys,
        minorTableKeys);
    return indices;
}

std::vector<double> GetCumulativeDistribution(
    TRange<i64> sizes,
    const NLogging::TLogger& Logger,
    bool enableVerboseLogging)
{
    std::vector<double> distribution;
    auto totalSize = std::accumulate(
        sizes.begin(),
        sizes.end(),
        0ll);

    if (totalSize == 0) {
        return GetUniformDistribution(sizes.size(), Logger, enableVerboseLogging);
    }

    YT_VERIFY(totalSize > 0);

    distribution.push_back(0.0);
    double prefixTotalSize = 0;
    for (int index = 0; index < std::ssize(sizes); ++index) {
        prefixTotalSize += sizes[index];
        distribution.push_back(prefixTotalSize / totalSize);
    }

    // To ensure that the first and last elements are equal to the same elements in another distribution
    distribution.back() = 1.0;

    YT_LOG_DEBUG_IF(enableVerboseLogging,
        "Calculated distribution by sizes (Distribution: %v, TotalSize: %v, Sizes: %v)",
        distribution,
        prefixTotalSize,
        MakeFormattableView(sizes, TDefaultFormatter{}));

    return distribution;
}

std::vector<double> CalculateMajorMetricsBetweenSamePivots(
    const TRange<double>& minorTableMetrics,
    const TRange<i64>& majorTabletSizes,
    const TRange<i64>& minorTabletSizes,
    const NLogging::TLogger& Logger,
    bool enableVerboseLogging)
{
    auto majorDistribution = GetCumulativeDistribution(majorTabletSizes, Logger, enableVerboseLogging);
    auto minorDistribution = GetCumulativeDistribution(minorTabletSizes, Logger, enableVerboseLogging);
    YT_VERIFY(majorDistribution.front() == minorDistribution.front());
    YT_VERIFY(majorDistribution.back() == minorDistribution.back());

    std::vector<double> metrics(majorTabletSizes.size());
    for (int majorIndex = 1, minorIndex = 1;
        majorIndex < std::ssize(majorDistribution) && minorIndex < std::ssize(minorDistribution);)
    {
        auto addedPart = std::min(minorDistribution[minorIndex], majorDistribution[majorIndex]) -
            std::max(minorDistribution[minorIndex - 1], majorDistribution[majorIndex - 1]);

        YT_VERIFY(addedPart >= 0);

        auto minorAddedPart = 0.0;
        if (auto minorTabletPart = minorDistribution[minorIndex] - minorDistribution[minorIndex - 1]; minorTabletPart > 0.0) {
            minorAddedPart = addedPart / minorTabletPart;
            YT_VERIFY(minorAddedPart <= 1.0);
        }

        metrics[majorIndex - 1] += minorAddedPart * minorTableMetrics[minorIndex - 1];

        if (minorDistribution[minorIndex] <= majorDistribution[majorIndex]) {
            ++minorIndex;
        } else {
            ++majorIndex;
        }
    }

    YT_LOG_DEBUG_IF(enableVerboseLogging,
        "Calculated major metrics by minor metrics between same pivot keys (Metrics: %v)",
        metrics);

    return metrics;
}

std::vector<double> CalculateMajorMetrics(
    const std::vector<double>& minorTableMetrics,
    const std::vector<i64>& majorTabletSizes,
    const std::vector<i64>& minorTabletSizes,
    const std::vector<TLegacyOwningKey>& majorTablePivotKeys,
    const std::vector<TLegacyOwningKey>& minorTablePivotKeys,
    const NLogging::TLogger& Logger,
    bool enableVerboseLogging)
{
    YT_LOG_DEBUG_IF(enableVerboseLogging,
        "Calculating major metrics by minor table "
        "(MinorMetrics: %v, MajorTabletSizes: %v, MinorTabletSizes: %v, MajorPivotKeys: %v, MinorPivotKeys: %v)",
        minorTableMetrics,
        majorTabletSizes,
        minorTabletSizes,
        majorTablePivotKeys,
        minorTablePivotKeys);

    YT_VERIFY(std::ssize(minorTableMetrics) == std::ssize(minorTablePivotKeys));
    YT_VERIFY(std::ssize(minorTableMetrics) == std::ssize(minorTabletSizes));
    YT_VERIFY(std::ssize(majorTabletSizes) == std::ssize(majorTablePivotKeys));

    auto commonPivotKeys = GetCommonKeyIndices(
        majorTablePivotKeys,
        minorTablePivotKeys,
        Logger,
        enableVerboseLogging);

    std::vector<double> metrics;
    for (auto rightCommonPivotIndex = 1; rightCommonPivotIndex < std::ssize(commonPivotKeys); ++rightCommonPivotIndex) {
        auto [majorLeftPivotIndex, minorLeftPivotIndex] = commonPivotKeys[rightCommonPivotIndex - 1];
        auto [majorRightPivotIndex, minorRightPivotIndex] = commonPivotKeys[rightCommonPivotIndex];
        auto reshardedMinorMetrics = CalculateMajorMetricsBetweenSamePivots(
            TRange<double>(minorTableMetrics.begin() + minorLeftPivotIndex, minorTableMetrics.begin() + minorRightPivotIndex),
            TRange<i64>(majorTabletSizes.begin() + majorLeftPivotIndex, majorTabletSizes.begin() + majorRightPivotIndex),
            TRange<i64>(minorTabletSizes.begin() + minorLeftPivotIndex, minorTabletSizes.begin() + minorRightPivotIndex),
            Logger,
            enableVerboseLogging);
        metrics.insert(metrics.end(), reshardedMinorMetrics.begin(), reshardedMinorMetrics.end());
    }
    YT_LOG_DEBUG_IF(enableVerboseLogging,
        "Calculated major table metrics by minor table (Metrics: %v)",
        metrics);
    return metrics;
}

std::vector<std::pair<int, std::vector<TLegacyOwningKey>>> ReshardByReferencePivots(
    const TRange<TLegacyOwningKey>& keys,
    const TRange<TLegacyOwningKey>& referenceKeys,
    int maxTabletCountPerAction,
    const NLogging::TLogger& Logger,
    bool enableVerboseLogging)
{
    YT_LOG_DEBUG_IF(enableVerboseLogging,
        "Building reshard descriptors to split table by reference table between same pivots "
        "(Keys: %v, ReferenceKeys: %v, MaxTabletCountPerAction: %v)",
        MakeFormattableView(keys, TDefaultFormatter{}),
        MakeFormattableView(referenceKeys, TDefaultFormatter{}),
        maxTabletCountPerAction);

    std::vector<std::pair<int, std::vector<TLegacyOwningKey>>> actions;
    int leftIndex = 0;
    int rightIndex = 0;
    auto referencePivot = referenceKeys.begin();
    YT_VERIFY(referencePivot != referenceKeys.end());

    while (rightIndex < std::ssize(keys)) {
        while (referencePivot != referenceKeys.end() && *referencePivot <= keys[leftIndex]) {
            YT_LOG_DEBUG_IF(enableVerboseLogging,
                "Shifting reference pivot iterator to right to skip extra pivots "
                "(NextFirstPivot: %v, ReferenceTablePivot: %v)",
                keys[leftIndex],
                *referencePivot);
            ++referencePivot;
        }

        // Tablet action first pivot key must be the same as the key of the first tablet in it.
        std::vector<TLegacyOwningKey> pivotKeys{keys[leftIndex]};

        while (rightIndex - leftIndex < maxTabletCountPerAction &&
            std::ssize(pivotKeys) < maxTabletCountPerAction &&
            (referencePivot != referenceKeys.end() || rightIndex < std::ssize(keys)))
        {
            if (referencePivot == referenceKeys.end() ||
                rightIndex < std::ssize(keys) && keys[rightIndex] < *referencePivot)
            {
                YT_LOG_DEBUG_IF(enableVerboseLogging,
                    "Added minor tablet to descriptor "
                    "(FirstIndex: %v, LastIndex: %v, Pivot: %v, ReferencePivot: %v)",
                    leftIndex,
                    rightIndex,
                    keys[rightIndex],
                    referencePivot != referenceKeys.end() ? std::optional(*referencePivot) : std::nullopt);
                ++rightIndex;
            } else {
                YT_LOG_DEBUG_IF(enableVerboseLogging,
                    "Added pivot key to descriptor "
                    "(FirstIndex: %v, LastIndex: %v, Pivot: %v, AddedPivot: %v)",
                    leftIndex,
                    rightIndex,
                    rightIndex < std::ssize(keys) ? std::optional(keys[rightIndex]) : std::nullopt,
                    *referencePivot);

                YT_VERIFY(pivotKeys.back() != *referencePivot);
                pivotKeys.push_back(*referencePivot);
                ++referencePivot;
            }
        }

        actions.emplace_back(rightIndex - leftIndex, std::move(pivotKeys));
        leftIndex = rightIndex;
    }

    return actions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
