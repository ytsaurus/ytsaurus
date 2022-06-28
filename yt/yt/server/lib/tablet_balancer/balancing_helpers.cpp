#include "balancing_helpers.h"
#include "config.h"
#include "public.h"
#include "table.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/numeric_helpers.h>

namespace NYT::NTabletBalancer {

using namespace NTabletClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

struct TTabletSizeConfig
{
    i64 MinTabletSize = 0;
    i64 MaxTabletSize = 0;
    i64 DesiredTabletSize = 0;
    std::optional<int> MinTabletCount;
};

bool TTabletBalancerContext::IsTabletUntouched(TTabletId tabletId) const
{
    return !TouchedTablets.contains(tabletId);
}

////////////////////////////////////////////////////////////////////////////////

i64 GetTabletBalancingSize(const TTabletPtr& tablet)
{
    return tablet->Table->InMemoryMode == EInMemoryMode::None
        ? tablet->Statistics.UncompressedDataSize
        : tablet->Statistics.MemorySize;
}

TTabletSizeConfig GetTabletSizeConfig(
    const TTable* table,
    const NLogging::TLogger& Logger)
{
    i64 minTabletSize;
    i64 maxTabletSize;
    i64 desiredTabletSize = 0;
    const auto& bundle = table->Bundle;

    const auto& tableConfig = table->TableConfig;

    i64 tableSize = table->InMemoryMode == EInMemoryMode::Compressed
        ? table->CompressedDataSize
        : table->UncompressedDataSize;

    bool enableVerboseLogging = tableConfig->EnableVerboseLogging || bundle->Config->EnableVerboseLogging;

    if (tableConfig->DesiredTabletCount) {
        // DesiredTabletCount from table config.
        i64 desiredTabletCount = std::max(1, *tableConfig->DesiredTabletCount);
        desiredTabletSize = DivCeil(tableSize, desiredTabletCount);
        minTabletSize = static_cast<i64>(desiredTabletSize / 1.9);
        maxTabletSize = static_cast<i64>(desiredTabletSize * 1.9);
    } else if (tableConfig->MinTabletSize &&
        tableConfig->MaxTabletSize &&
        tableConfig->DesiredTabletSize)
    {
        // Tablet size attributes from table config.
        minTabletSize = *tableConfig->MinTabletSize;
        maxTabletSize = *tableConfig->MaxTabletSize;
        desiredTabletSize = *tableConfig->DesiredTabletSize;

        // This should probably never happen.
        if (tableConfig->MinTabletSize >= tableConfig->DesiredTabletSize ||
            tableConfig->DesiredTabletSize >= tableConfig->MaxTabletSize)
        {
            YT_LOG_WARNING("Tablet size inequalities violated in tablet balancer config "
                "(TableId: %v, Config: %v)",
                table->Id,
                ConvertToYsonString(tableConfig, NYson::EYsonFormat::Text));
            desiredTabletSize = std::max(desiredTabletSize, minTabletSize + 1);
            maxTabletSize = std::max(maxTabletSize, desiredTabletSize + 1);
        }
    } else if (table->InMemoryMode == EInMemoryMode::None) {
        // Tablet size attributes from bundle for ext-memory tables.
        minTabletSize = bundle->Config->MinTabletSize;
        maxTabletSize = bundle->Config->MaxTabletSize;
        desiredTabletSize = bundle->Config->DesiredTabletSize;
    } else {
        // Tablet size attributes from bundle for in-memory tables.
        minTabletSize = bundle->Config->MinInMemoryTabletSize;
        maxTabletSize = bundle->Config->MaxInMemoryTabletSize;
        desiredTabletSize = bundle->Config->DesiredInMemoryTabletSize;
    }

    // Balancer would not create too many tablets unless desired_tablet_count is set.
    if (!tableConfig->DesiredTabletCount) {
        i64 maxTabletCount = std::ssize(bundle->TabletCells) *
            bundle->Config->TabletToCellRatio;
        auto tabletSizeLimit = DivCeil(tableSize, maxTabletCount);
        if (desiredTabletSize < tabletSizeLimit) {
            desiredTabletSize = tabletSizeLimit;
            minTabletSize = static_cast<i64>(desiredTabletSize / 1.9);
            maxTabletSize = static_cast<i64>(desiredTabletSize * 1.9);

            YT_LOG_DEBUG_IF(enableVerboseLogging,
                "Tablet size config overridden by tablet to cell ratio"
                "(TableId: %v, MaxTabletCount: %v, MinTabletSize: %v, DesiredTabletSize: %v, "
                "MaxTabletSize: %v)",
                table->Id,
                maxTabletCount,
                minTabletSize,
                desiredTabletSize,
                maxTabletSize);
        }
    }

    if (tableConfig->MinTabletCount) {
        i64 minTabletCount = *tableConfig->MinTabletCount;
        auto tabletSizeLimit = tableSize / minTabletCount;
        if (desiredTabletSize > tabletSizeLimit) {
            // minTabletSize should be nonzero so desiredTabletSize is bounded with 2.
            desiredTabletSize = std::max<i64>(2, tabletSizeLimit);
            minTabletSize = std::min(minTabletSize, desiredTabletSize - 1);
            YT_LOG_DEBUG_IF(enableVerboseLogging,
                "Tablet size config overridden by min tablet count (TableId: %v, "
                "MinTabletSize: %v, DesiredTabletSize: %v, MaxTabletSize: %v)",
                table->Id,
                minTabletSize,
                desiredTabletSize,
                maxTabletSize);
        }
    }

    return TTabletSizeConfig{
        minTabletSize,
        maxTabletSize,
        desiredTabletSize,
        tableConfig->MinTabletCount,
    };
}

std::optional<TReshardDescriptor> MergeSplitTablet(
    const TTabletPtr& tablet,
    TTabletBalancerContext* context,
    const TTabletSizeConfig& bounds,
    std::vector<int>* mergeBudgetByIndex,
    const NLogging::TLogger& Logger)
{
    // If a descriptor with a previous tablet is created and contains the current tablet,
    // that tablet will already be marked as touched.
    if (!context->IsTabletUntouched(tablet->Id)) {
        return {};
    }

    i64 desiredSize = bounds.DesiredTabletSize;
    i64 size = GetTabletBalancingSize(tablet);
    const auto& table = tablet->Table;

    if (size >= bounds.MinTabletSize && size <= bounds.MaxTabletSize) {
        return {};
    }

    if (size < bounds.MinTabletSize && table->Tablets.size() == 1) {
        return {};
    }

    if (bounds.MinTabletCount &&
        std::ssize(table->Tablets) <= bounds.MinTabletCount &&
        size < bounds.MinTabletSize)
    {
        return {};
    }

    if (desiredSize == 0) {
        desiredSize = 1;
    }

    int startIndex = tablet->Index;
    int endIndex = tablet->Index;

    auto sizeGood = [&] () {
        int tabletCount = std::clamp<i64>(DivRound(size, desiredSize), 1, MaxTabletCount);
        i64 tabletSize = size / tabletCount;
        return tabletSize >= bounds.MinTabletSize && tabletSize <= bounds.MaxTabletSize;
    };

    auto takeMergeBudget = [&] (int index) {
        if (mergeBudgetByIndex) {
            int result = (*mergeBudgetByIndex)[index];
            (*mergeBudgetByIndex)[index] = 0;
            return result;
        } else {
            return 0;
        }
    };

    // If the tablet is going to be split then we are not constrained
    // by MinTabletCount and set mergeBudget to infinity for convenience.
    int mergeBudget = bounds.MinTabletCount && size < bounds.MinTabletSize
        ? takeMergeBudget(startIndex)
        : std::numeric_limits<int>::max() / 2;

    while (!sizeGood() &&
        startIndex > 0 &&
        context->IsTabletUntouched(table->Tablets[startIndex - 1]->Id) &&
        table->Tablets[startIndex - 1]->State == tablet->State)
    {
        mergeBudget += takeMergeBudget(startIndex - 1);
        if (mergeBudget == 0) {
            break;
        }
        --mergeBudget;
        --startIndex;
        size += GetTabletBalancingSize(table->Tablets[startIndex]);
    }

    while (!sizeGood() &&
        endIndex < std::ssize(table->Tablets) - 1 &&
        context->IsTabletUntouched(table->Tablets[endIndex + 1]->Id) &&
        table->Tablets[endIndex + 1]->State == tablet->State)
    {
        mergeBudget += takeMergeBudget(endIndex + 1);
        if (mergeBudget == 0) {
            break;
        }
        --mergeBudget;
        ++endIndex;
        size += GetTabletBalancingSize(table->Tablets[endIndex]);
    }

    int newTabletCount = std::clamp<i64>(DivRound(size, desiredSize), 1, MaxTabletCount);

    if (endIndex == startIndex && tablet->Statistics.PartitionCount == 1) {
        return {};
    }

    if (endIndex == startIndex && newTabletCount == 1) {
        YT_LOG_DEBUG("Tablet balancer is unable to reshard tablet (TableId: %v, TabletId: %v, TabletSize: %v)",
            table->Id,
            tablet->Id,
            size);
        return {};
    }

    if (std::ssize(table->Tablets) + newTabletCount -
        (endIndex - startIndex + 1) >= MaxTabletCount)
    {
        YT_LOG_DEBUG("Tablet balancer will not split tablets since tablet count "
            "would exceed the limit (TableId: %v, TabletId: %v, TabletsSize: %v, DesiredCount: %v)",
            table->Id,
            tablet->Id,
            size,
            newTabletCount);
    }

    std::vector<TTabletId> tablets;
    for (int index = startIndex; index <= endIndex; ++index) {
        auto tabletId = table->Tablets[index]->Id;
        tablets.push_back(tabletId);
        context->TouchedTablets.insert(tabletId);
    }

    return {TReshardDescriptor{tablets, newTabletCount, size}};
}

std::vector<TReshardDescriptor> MergeSplitTabletsOfTable(
    TRange<TTabletPtr> tabletRange,
    TTabletBalancerContext* context,
    const NLogging::TLogger& Logger)
{
    YT_VERIFY(!tabletRange.empty());
    std::vector<TTabletPtr> tablets(
        tabletRange.Begin(),
        tabletRange.End());
    std::sort(
        tablets.begin(),
        tablets.end(),
        [] (const TTabletPtr& lhs, const TTabletPtr& rhs) {
            return lhs->Index < rhs->Index;
        });

    const auto& table = tabletRange.Front()->Table;
    auto config = GetTabletSizeConfig(table, Logger);

    // If MinTabletCount is set then the number of merges is limited. We want
    // to distribute merges evenly across the table. Merge budget (the number
    // of allowed merges) is distributed between starving tablets. When the
    // tablet is going to merge with its neighbour, the action should be paid
    // for from either of their budgets.
    std::vector<int> mergeBudgetByIndex;
    if (config.MinTabletCount) {
        mergeBudgetByIndex.resize(table->Tablets.size());

        int mergeBudget = std::max(
            0,
            static_cast<int>(table->Tablets.size()) - *config.MinTabletCount);

        std::vector<int> tabletsPendingMerge;
        for (const auto& tablet : tablets) {
            if (GetTabletBalancingSize(tablet) < config.MinTabletSize) {
                tabletsPendingMerge.push_back(tablet->Index);
            }
        }

        mergeBudget = std::min<int>(mergeBudget, tabletsPendingMerge.size());
        for (i64 multiplier = 0; multiplier < mergeBudget; ++multiplier) {
            int position = tabletsPendingMerge.size() * multiplier / mergeBudget;
            // Subsequent merging works more uniformly if budget tends to the right.
            position = tabletsPendingMerge.size() - position - 1;
            ++mergeBudgetByIndex[tabletsPendingMerge[position]];
        }
    }

    // TODO(alexelex): remove it.
    for (int i = 0; i < std::ssize(tablets); ++i) {
        YT_VERIFY(tablets[i]->Index == i);
        YT_VERIFY(table->Tablets[i]->Index == i);
    }

    std::vector<TReshardDescriptor> descriptors;
    for (const auto& tablet : tablets) {
        auto descriptor = MergeSplitTablet(
            tablet,
            context,
            config,
            config.MinTabletCount ? &mergeBudgetByIndex : nullptr,
            Logger);
        if (descriptor) {
            descriptors.push_back(*descriptor);

            // TODO(alexelex): remove it.
            YT_VERIFY(!context->IsTabletUntouched(tablet->Id));
        }
    }
    return descriptors;
}

bool IsTabletReshardable(const TTabletPtr tablet, bool ignoreConfig)
{
    // TODO(alexelex): Check if the tablet has unfinished action.
    return (tablet->State == ETabletState::Mounted || tablet->State == ETabletState::Frozen) &&
        (ignoreConfig || tablet->Table->TableConfig->EnableAutoReshard) &&
        tablet->Table->Sorted &&
        (ignoreConfig || tablet->Table->Bundle->Config->EnableTabletSizeBalancer);
}

std::vector<TMoveDescriptor> ReassignInMemoryTablets(
    const TTabletCellBundlePtr& bundle,
    const std::optional<THashSet<TTableId>>& movableTables,
    bool ignoreTableWiseConfig,
    const NLogging::TLogger& /*Logger*/)
{
    auto softThresholdViolated = [&] (i64 min, i64 max) {
        return max > 0 && 1.0 * (max - min) / max > bundle->Config->SoftInMemoryCellBalanceThreshold;
    };

    auto hardThresholdViolated = [&] (i64 min, i64 max) {
        return max > 0 && 1.0 * (max - min) / max > bundle->Config->HardInMemoryCellBalanceThreshold;
    };

    auto cells = bundle->GetAliveCells();

    if (cells.empty()) {
        return {};
    }

    struct TMemoryUsage
    {
        i64 Memory;
        const TTabletCell* TabletCell;

        bool operator<(const TMemoryUsage& other) const
        {
            if (Memory != other.Memory) {
                return Memory < other.Memory;
            }
            return TabletCell->Id < other.TabletCell->Id;
        }

        bool operator>(const TMemoryUsage& other) const
        {
            return other < *this;
        }
    };

    std::vector<TMemoryUsage> memoryUsage;
    i64 total = 0;
    memoryUsage.reserve(cells.size());
    for (const auto& cell : cells) {
        i64 size = cell->Statistics.MemorySize;
        total += size;
        memoryUsage.push_back({size, cell.Get()});
    }

    auto minmaxCells = std::minmax_element(memoryUsage.begin(), memoryUsage.end());
    if (!hardThresholdViolated(minmaxCells.first->Memory, minmaxCells.second->Memory)) {
        return {};
    }

    std::sort(memoryUsage.begin(), memoryUsage.end());
    i64 mean = total / cells.size();
    std::priority_queue<TMemoryUsage, std::vector<TMemoryUsage>, std::greater<TMemoryUsage>> queue;

    for (const auto& cell : memoryUsage) {
        if (cell.Memory >= mean) {
            break;
        }
        queue.push(cell);
    }

    std::vector<TMoveDescriptor> moveDescriptors;
    for (auto memoryUsageIt = memoryUsage.rbegin(); memoryUsageIt != memoryUsage.rend(); ++memoryUsageIt) {
        auto cellSize = memoryUsageIt->Memory;
        const auto& cell = memoryUsageIt->TabletCell;

        std::vector<TTabletPtr> tablets(cell->Tablets.begin(), cell->Tablets.end());
        std::sort(tablets.begin(), tablets.end(), [] (const TTabletPtr& lhs, const TTabletPtr& rhs) {
            return lhs->Id < rhs->Id;
        });

        for (const auto& tablet : tablets) {
            if (tablet->Table->InMemoryMode == EInMemoryMode::None) {
                continue;
            }

            if (TypeFromId(tablet->Table->Id) != EObjectType::Table) {
                continue;
            }

            if (!ignoreTableWiseConfig && !tablet->Table->TableConfig->EnableAutoTabletMove) {
                continue;
            }

            if (movableTables && !movableTables->contains(tablet->Id)) {
                continue;
            }

            if (queue.empty() || cellSize <= mean) {
                break;
            }

            auto top = queue.top();

            if (!softThresholdViolated(top.Memory, cellSize)) {
                break;
            }

            auto tabletSize = tablet->Statistics.MemorySize;

            if (tabletSize == 0) {
                continue;
            }

            if (tabletSize < cellSize - top.Memory) {
                queue.pop();
                top.Memory += tabletSize;
                cellSize -= tabletSize;
                if (top.Memory < mean) {
                    queue.push(top);
                }

                moveDescriptors.push_back({tablet->Id, top.TabletCell->Id});
            }
        }
    }

    return moveDescriptors;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
