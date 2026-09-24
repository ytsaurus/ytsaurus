#include "resharder.h"

#include "balancing_helpers.h"
#include "config.h"
#include "metrics_calculator.h"
#include "table.h"
#include "tablet.h"
#include "tablet_cell_bundle.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NTabletBalancer {

using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TParameterizedResharderConfig TParameterizedResharderConfig::MergeWith(
    const TParameterizedBalancingConfigPtr& groupConfig) const
{
    return TParameterizedResharderConfig{
        .Metric = groupConfig->Metric.empty()
            ? Metric
            : groupConfig->Metric
    };
}

void FormatValue(TStringBuilderBase* builder, const TParameterizedResharderConfig& config, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "EnableReshardByDefault: %v, Metric: %v",
        config.EnableReshardByDefault,
        config.Metric);
}

////////////////////////////////////////////////////////////////////////////////

class TParameterizedResharder
    : public IParameterizedResharder
{
public:
    TParameterizedResharder(
        TTabletCellBundlePtr bundle,
        std::vector<std::string> performanceCountersKeys,
        TParameterizedResharderConfig config,
        TGroupName groupName,
        const TLogger& logger)
        : Bundle_(std::move(bundle))
        , Logger(logger
            .WithTag("BundleName", Bundle_->Name)
            .WithTag("Group", groupName))
        , Config_(std::move(config))
        , GroupName_(std::move(groupName))
        , Calculator_(New<TParameterizedMetricsCalculator>(
            Config_.Metric,
            std::move(performanceCountersKeys),
            Bundle_->PerformanceCountersTableSchema,
            Logger))
    {
        YT_TLOG_DEBUG("Reporting parameterized resharder config")
            .With("Config", Config_);
    }

    std::vector<TReshardDescriptor> BuildTableActionDescriptors(const TTablePtr& table) override
    {
        LogMessageCount_ = 0;

        if (!IsParameterizedReshardEnabled(table)) {
            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Parameterized balancing via reshard is not enabled")
                .With("TableId", table->Id);
            return {};
        }

        YT_VERIFY(table->TableConfig->DesiredTabletCount.has_value() ||
            table->TableConfig->DesiredTabletMetric.has_value());

        if (table->TableConfig->DesiredTabletCount.has_value() && *table->TableConfig->DesiredTabletCount <= 0) {
            YT_TLOG_WARNING("Table desired tablet count is not positive")
                .With("TableId", table->Id)
                .With("TablePath", table->Path)
                .With("DesiredTabletCount", table->TableConfig->DesiredTabletCount);
            return {};
        }

        auto statistics = GetTableStatistics(table, table->TableConfig);
        YT_VERIFY(statistics.DesiredTabletMetric > 0);
        std::vector<TReshardDescriptor> actions;
        THashSet<int> touchedTabletIndexes;

        int tabletCount = std::ssize(table->Tablets);
        for (int tabletIndex = 0; tabletIndex < std::ssize(table->Tablets); ++tabletIndex) {
            if (touchedTabletIndexes.contains(tabletIndex)) {
                continue;
            }

            auto action = TryMakeTabletFit(table, tabletIndex, &touchedTabletIndexes, statistics);
            if (action) {
                actions.push_back(*action);
                tabletCount += action->TabletCount - std::ssize(action->Tablets);
            }
        }

        YT_TLOG_DEBUG_UNLESS(actions.empty(), "Parameterized reshard action creation requested")
            .With("TabletCount", std::ssize(table->Tablets))
            .With("NewTabletCount", tabletCount)
            .With("DesiredTabletCount", statistics.DesiredTabletCount);

        SortTabletActionsByUsefulness(&actions);
        TrimTabletActions(std::ssize(table->Tablets), &actions);

        return actions;
    }

private:
    struct TTableStatistics
    {
        int DesiredTabletCount;

        i64 MinTabletSize;
        i64 DesiredTabletSize;
        i64 MaxTabletSize;

        i64 TableSize;

        double MinTabletMetric;
        double DesiredTabletMetric;
        double MaxTabletMetric;

        double TableMetric;

        std::vector<i64> TabletSizes;
        std::vector<double> TabletMetrics;

        bool IsTooSmallBySomeMeasure(
            i64 tabletSize,
            double tabletMetric) const
        {
            return tabletSize <= MaxTabletSize && tabletMetric <= MaxTabletMetric &&
                (tabletSize < MinTabletSize || tabletMetric < MinTabletMetric);
        }

        bool IsLessThanDesiredByEachMeasure(
            i64 tabletSize,
            double tabletMetric) const
        {
            return tabletSize < DesiredTabletSize && tabletMetric < DesiredTabletMetric;
        }
    };

    const TTabletCellBundlePtr Bundle_;
    const TLogger Logger;
    const TParameterizedResharderConfig Config_;
    const TGroupName GroupName_;
    TParameterizedMetricsCalculatorPtr Calculator_;

    mutable int LogMessageCount_ = 0;

    void SortTabletActionsByUsefulness(std::vector<TReshardDescriptor>* actions) const
    {
        std::sort(
            actions->begin(),
            actions->end(),
            [] (auto lhs, auto rhs) {
                if (lhs.TabletCount == 1 || rhs.TabletCount == 1) {
                    return lhs.TabletCount < rhs.TabletCount;
                }
                return lhs.TabletCount > rhs.TabletCount;
        });
    }

    void TrimTabletActions(int currentTabletCount, std::vector<TReshardDescriptor>* actions) const
    {
        // We calculate the tablet count in the worst case, when all split actions are executed before merge actions.
        for (int actionIndex = 0; actionIndex < std::ssize(*actions); ++actionIndex) {
            const auto& action = actions->at(actionIndex);
            if (action.TabletCount == 1) {
                continue;
            }

            currentTabletCount += action.TabletCount - 1;
            if (currentTabletCount > NTabletClient::MaxTabletCount) {
                actions->resize(actionIndex);
                return;
            }
        }
    }

    std::optional<TReshardDescriptor> TryMakeTabletFit(
        const TTablePtr& table,
        int tabletIndex,
        THashSet<int>* touchedTabletIndexes,
        const TTableStatistics& statistics)
    {
        const auto& tablet = table->Tablets[tabletIndex];
        if (tablet->State != ETabletState::Mounted) {
            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Tablet is not mounted, skipping reshard")
                .With("TabletId", tablet->Id)
                .With("TabletState", tablet->State);
            return std::nullopt;
        }

        auto tabletMetric = statistics.TabletMetrics[tabletIndex];
        auto tabletSize = statistics.TabletSizes[tabletIndex];

        // Tablet is too large by at least one of the metrics.
        if (tabletMetric > statistics.MaxTabletMetric ||
            tabletSize > statistics.MaxTabletSize)
        {
            if (tabletSize == 0) {
                // Should not happen othen.
                YT_TLOG_WARNING_IF(
                    (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                    LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                    "Trying to split an empty tablet; skipping it")
                    .With("TableId", table->Id)
                    .With("TabletId", tablet->Id)
                    .WithFormat("TabletMetric", "%e", tabletMetric)
                    .With("TableSize", statistics.TableSize)
                    .With("DesiredTabletSize", statistics.DesiredTabletSize)
                    .With("MaxTabletSize", statistics.MaxTabletSize);
                return std::nullopt;
            }

            return SplitTablet(table, tabletIndex, touchedTabletIndexes, statistics);
        }

        // Tablet is just right.
        if (tabletMetric >= statistics.MinTabletMetric &&
            tabletSize >= statistics.MinTabletSize)
        {
            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Tablet is just right")
                .With("TabletId", tablet->Id)
                .WithFormat("TabletMetric", "%e", tabletMetric)
                .With("TabletSize", tabletSize);
            return std::nullopt;
        }

        return MergeTablets(table, tabletIndex, touchedTabletIndexes, statistics);
    }

    TReshardDescriptor SplitTablet(
        const TTablePtr& table,
        int tabletIndex,
        THashSet<int>* touchedTabletIndexes,
        const TTableStatistics& statistics)
    {
        EmplaceOrCrash(*touchedTabletIndexes, tabletIndex);
        auto tabletSize = statistics.TabletSizes[tabletIndex];
        auto tabletMetric = statistics.TabletMetrics[tabletIndex];
        auto tabletId = table->Tablets[tabletIndex]->Id;

        auto tabletCount = static_cast<int>(std::ceil(tabletMetric / statistics.DesiredTabletMetric));
        tabletCount = std::max<i64>({
            DivCeil(tabletSize, statistics.DesiredTabletSize),
            tabletCount,
            1});

        YT_VERIFY(tabletCount > 0);

        auto correlationId = TGuid::Create();
        YT_TLOG_DEBUG("Splitting tablet")
            .With("Tablet", tabletId)
            .With("TabletSize", DivCeil<i64>(tabletSize, tabletCount))
            .WithFormat("TabletMetric", "%e", tabletMetric / tabletCount)
            .With("CorrelationId", correlationId);

        auto deviation = std::max(
            tabletMetric / statistics.DesiredTabletMetric,
            static_cast<double>(tabletSize) / statistics.DesiredTabletSize);

        return TReshardDescriptor{
            .Tablets = std::vector<TTabletId>{tabletId},
            .TabletCount = tabletCount,
            .DataSize = tabletSize,
            .CorrelationId = correlationId,
            .Priority = std::tuple(/*IsSplit*/ true, -tabletCount, -deviation)
        };
    }

    std::optional<TReshardDescriptor> MergeTablets(
        const TTablePtr& table,
        int tabletIndex,
        THashSet<int>* touchedTabletIndexes,
        const TTableStatistics& statistics)
    {
        auto enlargedTabletMetric = statistics.TabletMetrics[tabletIndex];
        auto enlargedTabletSize = statistics.TabletSizes[tabletIndex];

        auto leftTabletIndex = tabletIndex;
        auto rightTabletIndex = tabletIndex + 1;

        auto tabletCount = std::ssize(table->Tablets);

        auto deviation = std::min(
            statistics.TabletMetrics[tabletIndex] / statistics.DesiredTabletMetric,
            static_cast<double>(statistics.TabletSizes[tabletIndex]) / statistics.DesiredTabletSize);

        auto isMergeableNeighbor = [&] (int index) {
            return index >= 0 &&
                index < tabletCount &&
                !touchedTabletIndexes->contains(index) &&
                table->Tablets[index]->State == ETabletState::Mounted;
        };

        while (AreMoreTabletsNeeded(statistics, enlargedTabletSize, enlargedTabletMetric) &&
            isMergeableNeighbor(leftTabletIndex - 1) &&
            IsPossibleToAddTablet(statistics, enlargedTabletSize, enlargedTabletMetric, leftTabletIndex - 1))
        {
            --leftTabletIndex;

            enlargedTabletSize += statistics.TabletSizes[leftTabletIndex];
            enlargedTabletMetric += statistics.TabletMetrics[leftTabletIndex];

            deviation = std::min({
                deviation,
                statistics.TabletMetrics[leftTabletIndex] / statistics.DesiredTabletMetric,
                static_cast<double>(statistics.TabletSizes[leftTabletIndex]) / statistics.DesiredTabletSize});
        }

        while (AreMoreTabletsNeeded(statistics, enlargedTabletSize, enlargedTabletMetric) &&
            isMergeableNeighbor(rightTabletIndex) &&
            IsPossibleToAddTablet(statistics, enlargedTabletSize, enlargedTabletMetric, rightTabletIndex))
        {
            enlargedTabletSize += statistics.TabletSizes[rightTabletIndex];
            enlargedTabletMetric += statistics.TabletMetrics[rightTabletIndex];

            deviation = std::min({
                deviation,
                statistics.TabletMetrics[rightTabletIndex] / statistics.DesiredTabletMetric,
                static_cast<double>(statistics.TabletSizes[rightTabletIndex]) / statistics.DesiredTabletSize});

            ++rightTabletIndex;
        }

        if (rightTabletIndex - leftTabletIndex == 1) {
            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "The tablet is too small, but there are no tablets to merge with it")
                .With("TabletId", table->Tablets[tabletIndex]->Id)
                .With("TabletIndex", tabletIndex)
                .With("TabletSize", enlargedTabletSize)
                .WithFormat("TabletMetric", "%e", enlargedTabletMetric);
            return std::nullopt;
        }

        std::vector<TTabletId> tabletsToMerge;
        for (int index = leftTabletIndex; index < rightTabletIndex; ++index) {
            tabletsToMerge.push_back(table->Tablets[index]->Id);
            EmplaceOrCrash(*touchedTabletIndexes, index);
        }

        auto correlationId = TGuid::Create();
        YT_TLOG_DEBUG("Merging tablets")
            .With("Tablets", tabletsToMerge)
            .With("TabletSize", enlargedTabletSize)
            .WithFormat("TabletMetric", "%e", enlargedTabletMetric)
            .With("CorrelationId", correlationId);

        return TReshardDescriptor{
            .Tablets = std::move(tabletsToMerge),
            .TabletCount = 1,
            .DataSize = enlargedTabletSize,
            .CorrelationId = correlationId,
            .Priority = std::tuple(false, -(rightTabletIndex - leftTabletIndex), deviation)
        };
    }

    bool AreMoreTabletsNeeded(
        const TTableStatistics& statistics,
        i64 tabletSize,
        double tabletMetric) const
    {
        return statistics.IsTooSmallBySomeMeasure(tabletSize, tabletMetric) ||
            statistics.IsLessThanDesiredByEachMeasure(tabletSize, tabletMetric);
    }

    bool IsPossibleToAddTablet(
        const TTableStatistics& statistics,
        i64 tabletSize,
        double tabletMetric,
        int nextTabletIndex) const
    {
        return tabletSize + statistics.TabletSizes[nextTabletIndex] <= statistics.MaxTabletSize &&
            tabletMetric + statistics.TabletMetrics[nextTabletIndex] <= statistics.MaxTabletMetric;
    }

    bool IsParameterizedReshardEnabled(const TTablePtr& table) const
    {
        if (TypeFromId(table->Id) != EObjectType::Table) {
            return false;
        }

        if (table->GetBalancingGroup() != GroupName_) {
            return false;
        }

        if (!table->IsParameterizedReshardBalancingEnabled(Config_.EnableReshardByDefault)) {
            return false;
        }

        return true;
    }

    TTableStatistics GetTableStatistics(
        const TTablePtr& table,
        const TTableTabletBalancerConfigPtr& config) const
    {
        TTableStatistics statistics {};

        for (const auto& tablet : table->Tablets) {
            statistics.TabletSizes.push_back(GetTabletBalancingSize(tablet));
            statistics.TableSize += statistics.TabletSizes.back();

            auto tabletMetric = Calculator_->GetTabletMetric(tablet);
            if (tabletMetric < 0.0) {
                THROW_ERROR_EXCEPTION("Tablet metric must be nonnegative, got %v", tabletMetric)
                    .With("tablet_metric_value", tabletMetric)
                    .With("tablet_id", tablet->Id)
                    .With("metric_formula", Config_.Metric);
            }

            statistics.TabletMetrics.push_back(tabletMetric);
            statistics.TableMetric += tabletMetric;

            YT_TLOG_DEBUG_IF(
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Reporting tablet statistics")
                .With("TabletId", tablet->Id)
                .With("Size", statistics.TabletSizes.back())
                .WithFormat("Metric", "%e", tabletMetric)
                .With("TableId", table->Id);
        }

        if (config->DesiredTabletCount.has_value()) {
            YT_TLOG_DEBUG_IF(
                config->DesiredTabletMetric.has_value() &&
                (Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging) &&
                LogMessageCount_++ < MaxVerboseLogMessagesPerIteration,
                "Desired tablet count and desired tablet metric both set in config, use desired tablet count")
                .With("TableId", table->Id)
                .With("DesiredTabletCount", config->DesiredTabletCount)
                .With("DesiredTabletMetric", config->DesiredTabletMetric);

            statistics.DesiredTabletCount = config->DesiredTabletCount.value();
            statistics.DesiredTabletMetric = statistics.TableMetric / statistics.DesiredTabletCount;

            statistics.DesiredTabletSize = statistics.TableSize / statistics.DesiredTabletCount;
        } else {
            statistics.DesiredTabletMetric = config->DesiredTabletMetric.value();
            statistics.DesiredTabletCount = statistics.TableMetric / statistics.DesiredTabletMetric;

            // NB(dave11ar): For accuracy purposes.
            statistics.DesiredTabletSize = statistics.DesiredTabletMetric * statistics.TableSize / statistics.TableMetric;
        }

        statistics.MinTabletSize = statistics.DesiredTabletSize / 1.9;
        statistics.MaxTabletSize = statistics.DesiredTabletSize * 1.9;

        statistics.MinTabletMetric = statistics.DesiredTabletMetric / 1.9;

        if (statistics.TableMetric == 0.0 || statistics.DesiredTabletMetric == 0.0) {
            YT_TLOG_DEBUG("Calculated table metric for parameterized balancing via reshard is zero or almost zero")
                .With("TableId", table->Id)
                .With("TablePath", table->Path)
                .WithFormat("TableMetric", "%e", statistics.TableMetric);
            statistics.DesiredTabletMetric = 1;
        }

        statistics.MaxTabletMetric = statistics.DesiredTabletMetric * 1.9;

        YT_TLOG_DEBUG_IF(
            Bundle_->Config->EnableVerboseLogging || table->TableConfig->EnableVerboseLogging,
            "Reporting reshard limits and statistics")
            .With("MinTabletSize", statistics.MinTabletSize)
            .With("DesiredTabletSize", statistics.DesiredTabletSize)
            .With("MaxTabletSize", statistics.MaxTabletSize)
            .With("TableSize", statistics.TableSize)
            .WithFormat("MinTabletMetric", "%e", statistics.MinTabletMetric)
            .WithFormat("DesiredTabletMetric", "%e", statistics.DesiredTabletMetric)
            .WithFormat("MaxTabletMetric", "%e", statistics.MaxTabletMetric)
            .WithFormat("TableMetric", "%e", statistics.TableMetric)
            .With("TableId", table->Id);

        return statistics;
    }
};

////////////////////////////////////////////////////////////////////////////////

IParameterizedResharderPtr CreateParameterizedResharder(
    TTabletCellBundlePtr bundle,
    std::vector<std::string> performanceCountersKeys,
    TParameterizedResharderConfig config,
    TGroupName groupName,
    const NLogging::TLogger& logger)
{
    return New<TParameterizedResharder>(
        std::move(bundle),
        std::move(performanceCountersKeys),
        std::move(config),
        std::move(groupName),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
