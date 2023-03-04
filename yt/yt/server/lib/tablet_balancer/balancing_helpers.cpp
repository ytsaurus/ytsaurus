#include "balancing_helpers.h"
#include "config.h"
#include "parameterized_balancing_helpers.h"
#include "public.h"
#include "table.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/library/query/base/public.h>

#include <util/random/shuffle.h>

namespace NYT::NTabletBalancer {

using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

struct TTabletSizeConfig
{
    i64 MinTabletSize = 0;
    i64 MaxTabletSize = 0;
    i64 DesiredTabletSize = 0;
    std::optional<int> MinTabletCount;
};

struct TTabletBalancerContext
{
    THashSet<TTabletId> TouchedTablets;

    bool IsTabletUntouched(TTabletId tabletId) const
    {
        return !TouchedTablets.contains(tabletId);
    }
};

////////////////////////////////////////////////////////////////////////////////

i64 GetTabletBalancingSize(const TTabletPtr& tablet)
{
    return tablet->Table->InMemoryMode == EInMemoryMode::None
        ? tablet->Statistics.UncompressedDataSize
        : tablet->Statistics.MemorySize;
}

bool IsTabletReshardable(const TTabletPtr& tablet, bool ignoreConfig)
{
    return (tablet->State == ETabletState::Mounted || tablet->State == ETabletState::Frozen) &&
        (ignoreConfig || tablet->Table->TableConfig->EnableAutoReshard) &&
        (ignoreConfig || tablet->Table->Bundle->Config->EnableTabletSizeBalancer) &&
        tablet->Table->Sorted;
}

TTabletSizeConfig GetTabletSizeConfig(
    const TTable* table,
    const TLogger& Logger)
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

////////////////////////////////////////////////////////////////////////////////

std::optional<TReshardDescriptor> MergeSplitTablet(
    const TTabletPtr& tablet,
    TTabletBalancerContext* context,
    const TTabletSizeConfig& bounds,
    std::vector<int>* mergeBudgetByIndex,
    const TLogger& Logger)
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
    std::vector<TTabletPtr> tablets,
    const TLogger& Logger)
{
    YT_VERIFY(!tablets.empty());
    TTabletBalancerContext context;

    std::sort(
        tablets.begin(),
        tablets.end(),
        [] (const TTabletPtr& lhs, const TTabletPtr& rhs) {
            return lhs->Index < rhs->Index;
        });

    const auto& table = tablets.front()->Table;
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
        YT_LOG_FATAL_IF(
            tablets[i]->Table != table,
            "Tablet belongs to another table (TabletId: %v, ExpectedTableId: %v, ActualTableId: %v)",
            table->Id,
            tablets[i]->Table->Id,
            tablets[i]->Id);

        YT_LOG_FATAL_IF(
            tablets[i]->Index >= std::ssize(table->Tablets),
            "Tablet index is not less than the number of tablets in the table "
            "(TabletId: %v, TabletIndex: %v, TabletCount: %v)",
            tablets[i]->Id,
            tablets[i]->Index,
            std::ssize(table->Tablets));

        YT_LOG_FATAL_IF(
            tablets[i]->Index < i,
            "Tablet index is less than expected (TabletIndex: %v, ExpectedAtLeast: %v)",
            tablets[i]->Index,
            i);

        YT_LOG_FATAL_IF(
            table->Tablets[tablets[i]->Index].Get() != tablets[i].Get(),
            "Tablets are expected to be the same, but they are different "
            "(ExpectedTabletId: %v, ActualTabletId: %v, ExpectedTabletPtr: %v, ActialTabletPtr: %v)",
            table->Tablets[tablets[i]->Index]->Id,
            tablets[i]->Id,
            table->Tablets[tablets[i]->Index].Get(),
            tablets[i].Get());
    }

    std::vector<TReshardDescriptor> descriptors;
    for (const auto& tablet : tablets) {
        auto descriptor = MergeSplitTablet(
            tablet,
            &context,
            config,
            config.MinTabletCount ? &mergeBudgetByIndex : nullptr,
            Logger);
        if (descriptor) {
            descriptors.push_back(*descriptor);
        }
    }
    return descriptors;
}

std::vector<TMoveDescriptor> ReassignInMemoryTablets(
    const TTabletCellBundlePtr& bundle,
    const std::optional<THashSet<TTableId>>& movableTables,
    bool ignoreTableWiseConfig,
    const TLogger& /*Logger*/)
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

        std::vector<TTabletPtr> tablets;
        for (const auto& [id, tablet] : cell->Tablets) {
            tablets.push_back(tablet);
        }

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

            if (!tablet->Table->IsLegacyMoveBalancingEnabled()) {
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

void ReassignOrdinaryTabletsOfTable(
    const std::vector<TTabletPtr>& tablets,
    const std::vector<TTabletCellPtr>& bundleCells,
    THashMap<const TTablet*, TTabletCellId>* tabletToTargetCell,
    THashMap<const TTabletCell*, std::vector<TTabletPtr>>* slackTablets,
    const TLogger& /*Logger*/)
{
    YT_VERIFY(!tablets.empty());

    THashMap<const TTabletCell*, std::vector<TTabletPtr>> cellToTablets;
    for (const auto& tablet : tablets) {
        cellToTablets[tablet->Cell].push_back(tablet);
    }

    std::vector<std::pair<int, const TTabletCell*>> cells;
    for (const auto& [cell, cellTablets] : cellToTablets) {
        cells.emplace_back(cellTablets.size(), cell);
    }

    // Cells with the same number of tablets of current table should be distributed
    // randomly each time. It gives better per-cell distribution on average.
    Shuffle(cells.begin(), cells.end());
    std::sort(cells.begin(), cells.end(), [] (auto lhs, auto rhs) {
        return lhs.first > rhs.first;
    });

    int expectedCellCount = std::min(std::ssize(tablets), std::ssize(bundleCells));

    for (const auto& cell : bundleCells) {
        if (std::ssize(cells) == expectedCellCount) {
            break;
        }
        if (!cellToTablets.contains(cell.Get())) {
            cells.emplace_back(0, cell.Get());
        }
    }

    auto getExpectedTabletCount = [&] (int cellIndex) {
        int cellCount = std::ssize(bundleCells);
        int tabletCount = std::ssize(tablets);
        return tabletCount / cellCount + (cellIndex < tabletCount % cellCount);
    };

    const int minCellSize = tablets.size() / bundleCells.size();

    auto moveTablets = [&] (int srcIndex, int dstIndex, int limit) {
        int moveCount = 0;
        auto& srcTablets = cellToTablets[cells[srcIndex].second];
        while (moveCount < limit && !srcTablets.empty()) {
            auto tablet = srcTablets.back();
            srcTablets.pop_back();

            (*tabletToTargetCell)[tablet.Get()] = cells[dstIndex].second->Id;
            ++moveCount;
            --cells[srcIndex].first;
            ++cells[dstIndex].first;

            if (slackTablets && cells[dstIndex].first > minCellSize) {
                GetOrCrash(*slackTablets, cells[dstIndex].second).push_back(std::move(tablet));
            }
        }
        YT_VERIFY(moveCount == limit);
    };

    YT_VERIFY(!cells.empty());
    int dstIndex = cells.size() - 1;

    for (int srcIndex = 0; srcIndex < dstIndex; ++srcIndex) {
        int srcLimit = cells[srcIndex].first - getExpectedTabletCount(srcIndex);
        while (srcLimit > 0 && srcIndex < dstIndex) {
            int dstLimit = getExpectedTabletCount(dstIndex) - cells[dstIndex].first;
            int moveCount = std::min(srcLimit, dstLimit);
            YT_VERIFY(moveCount >= 0);
            moveTablets(srcIndex, dstIndex, moveCount);
            if (moveCount == dstLimit) {
                --dstIndex;
            }
            srcLimit -= moveCount;
        }
    }

    if (slackTablets) {
        for (auto [cellId, cell] : cells) {
            auto& tablets = cellToTablets[cell];
            if (std::ssize(tablets) > minCellSize) {
                GetOrCrash(*slackTablets, cell).push_back(cellToTablets[cell].back());
            } else {
                break;
            }
        }
    }

    for (int cellIndex = 0; cellIndex < std::ssize(cells); ++cellIndex) {
        YT_VERIFY(cells[cellIndex].first == getExpectedTabletCount(cellIndex));
    }
}

void ReassignSlackTablets(
    std::vector<std::pair<const TTabletCell*, std::vector<TTabletPtr>>> cellTablets,
    THashMap<const TTablet*, TTabletCellId>* tabletToTargetCell,
    const TLogger& /*Logger*/)
{
    std::sort(
        cellTablets.begin(),
        cellTablets.end(),
        [](const auto& lhs, const auto& rhs) {
            return lhs.second.size() > rhs.second.size();
        });

    std::vector<THashSet<const TTable*>> presentTables;
    std::vector<int> tabletCount;
    int totalTabletCount = 0;
    for (const auto& [cell, tablets] : cellTablets) {
        totalTabletCount += std::ssize(tablets);
        tabletCount.push_back(std::ssize(tablets));

        presentTables.emplace_back();
        for (const auto& tablet : tablets) {
            InsertOrCrash(presentTables.back(), tablet->Table);
        }
    }

    auto getExpectedTabletCount = [&] (int cellIndex) {
        int cellCount = std::ssize(cellTablets);
        return totalTabletCount / cellCount + (cellIndex < totalTabletCount % cellCount);
    };

    auto moveTablets = [&] (int srcIndex, int dstIndex, int limit) {
        int moveCount = 0;
        auto& srcTablets = cellTablets[srcIndex].second;
        for (int tabletIndex = 0; tabletIndex < std::ssize(srcTablets) && moveCount < limit; ++tabletIndex) {
            const auto& tablet = srcTablets[tabletIndex];
            if (!presentTables[dstIndex].contains(tablet->Table)) {
                presentTables[dstIndex].insert(tablet->Table);
                (*tabletToTargetCell)[tablet.Get()] = cellTablets[dstIndex].first->Id;
                std::swap(srcTablets[tabletIndex], srcTablets.back());
                srcTablets.pop_back();

                ++moveCount;
                --tabletIndex;
                --tabletCount[srcIndex];
                ++tabletCount[dstIndex];
            }
        }
        YT_VERIFY(moveCount == limit);
    };

    YT_VERIFY(!cellTablets.empty());
    int dstIndex = std::ssize(cellTablets) - 1;

    for (int srcIndex = 0; srcIndex < dstIndex; ++srcIndex) {
        int srcLimit = tabletCount[srcIndex] - getExpectedTabletCount(srcIndex);
        while (srcLimit > 0 && srcIndex < dstIndex) {
            int dstLimit = getExpectedTabletCount(dstIndex) - tabletCount[dstIndex];
            int moveCount = std::min(srcLimit, dstLimit);
            YT_VERIFY(moveCount >= 0);
            moveTablets(srcIndex, dstIndex, moveCount);
            if (moveCount == dstLimit) {
                --dstIndex;
            }
            srcLimit -= moveCount;
        }
    }

    for (int cellIndex = 0; cellIndex < std::ssize(cellTablets); ++cellIndex) {
        YT_ASSERT(tabletCount[cellIndex] == getExpectedTabletCount(cellIndex));
    }
}

std::vector<TMoveDescriptor> ReassignOrdinaryTablets(
    const TTabletCellBundlePtr& bundle,
    const std::optional<THashSet<TTableId>>& movableTables,
    const TLogger& Logger)
{
    /*
        Balancing happens in two iterations. First iteration goes per-table.
        Tablets of each table are spread between cells as evenly as possible.
        Due to rounding errors some cells will contain more tablets than
        the others. These extra tablets are called slack tablets. In the
        picture C are slack tablets, T are the others.

        | C  C       |
        | T  T  T  T |
        | T  T  T  T |
        +------------+

        Next iteration runs only if there are empty cells (usually when new
        cells are added). All slack tablets are spead between cells
        once again. Tablets are moved from cells with many tablets to cells
        with fewer. After balancing no two slack tablets from the same
        table may be on the same cell.
    */

    auto cells = bundle->GetAliveCells();

    bool haveEmptyCells = false;
    THashMap<const TTabletCell*, std::vector<TTabletPtr>> slackTablets;
    THashMap<const TTable*, std::vector<TTabletPtr>> tableToTablets;
    for (const auto& cell : cells) {
        slackTablets[cell.Get()] = {};
        haveEmptyCells |= cell->Tablets.empty();

        for (const auto& [id, tablet] : cell->Tablets) {
            if (!tablet->Table->IsLegacyMoveBalancingEnabled()) {
                continue;
            }

            if (movableTables && !movableTables->contains(tablet->Table->Id)) {
                continue;
            }

            if (tablet->Table->InMemoryMode != EInMemoryMode::None) {
                continue;
            }

            if (!tablet->Cell) {
                // Unmounted tablet.
                continue;
            }

            tableToTablets[tablet->Table].push_back(tablet);
        }
    }

    THashMap<const TTablet*, TTabletCellId> tabletToTargetCell;
    for (const auto& [table, tablets] : tableToTablets) {
        ReassignOrdinaryTabletsOfTable(
            tablets,
            cells,
            &tabletToTargetCell,
            haveEmptyCells ? &slackTablets : nullptr,
            Logger);
    }

    if (haveEmptyCells) {
        std::vector<std::pair<const TTabletCell*, std::vector<TTabletPtr>>> slackTabletsVector;
        for (auto&& pair : slackTablets) {
            slackTabletsVector.emplace_back(pair.first, std::move(pair.second));
        }
        ReassignSlackTablets(
            std::move(slackTabletsVector),
            &tabletToTargetCell,
            Logger);
    }

    std::vector<TMoveDescriptor> descriptors;
    for (const auto& [tablet, cellId] : tabletToTargetCell) {
        if (!tablet->Cell || tablet->Cell->Id != cellId) {
            descriptors.emplace_back(TMoveDescriptor{
                .TabletId = tablet->Id,
                .TabletCellId = cellId
            });
        }
    }

    return descriptors;
}

std::vector<TMoveDescriptor> ReassignTabletsParameterized(
    const TTabletCellBundlePtr& bundle,
    const std::vector<TString>& performanceCountersKeys,
    const TParameterizedReassignSolverConfig& config,
    const TGroupName& groupName,
    const TLogger& logger)
{
    auto solver = CreateParameterizedReassignSolver(
        bundle,
        performanceCountersKeys,
        config,
        groupName,
        logger);

    return solver->BuildActionDescriptors();
}

////////////////////////////////////////////////////////////////////////////////

void ApplyMoveTabletAction(const TTabletPtr& tablet, const TTabletCellId& cellId)
{
    YT_VERIFY(tablet->Cell);

    auto bundle = tablet->Table->Bundle;
    YT_VERIFY(bundle);

    auto cell = GetOrCrash(bundle->TabletCells, cellId);
    auto sourceCell = tablet->Cell;

    tablet->Cell = cell.Get();
    EmplaceOrCrash(cell->Tablets, tablet->Id, tablet);
    EraseOrCrash(sourceCell->Tablets, tablet->Id);

    auto size = tablet->Statistics.MemorySize;
    if (size > 0) {
        YT_VERIFY(tablet->Table->InMemoryMode != EInMemoryMode::None);

        sourceCell->Statistics.MemorySize -= size;
        cell->Statistics.MemorySize += size;

        bundle->NodeMemoryStatistics[*sourceCell->NodeAddress].Used -= size;
        bundle->NodeMemoryStatistics[*cell->NodeAddress].Used += size;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
