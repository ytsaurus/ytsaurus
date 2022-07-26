#include "balancing_helpers.h"
#include "config.h"
#include "private.h"
#include "tablet.h"
#include "tablet_action.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_manager.h"

#include <yt/yt/server/lib/tablet_balancer/config.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/core/misc/numeric_helpers.h>

namespace NYT::NTabletServer {

using namespace NTabletClient;
using namespace NTableServer;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

bool TTabletBalancerContext::IsTabletUntouched(const TTablet* tablet) const
{
    return !TouchedTablets.contains(tablet);
}

////////////////////////////////////////////////////////////////////////////////

TTabletSizeConfig GetTabletSizeConfig(const TTableNode* table)
{
    i64 minTabletSize;
    i64 maxTabletSize;
    i64 desiredTabletSize = 0;

    const auto& bundleConfig = table->TabletCellBundle()->TabletBalancerConfig();
    const auto& tableConfig = table->TabletBalancerConfig();
    const auto statistics = table->ComputeTotalStatistics();
    i64 tableSize = table->GetInMemoryMode() == EInMemoryMode::Compressed
        ? statistics.compressed_data_size()
        : statistics.uncompressed_data_size();
    bool enableVerboseLogging = tableConfig->EnableVerboseLogging || bundleConfig->EnableVerboseLogging;

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
        if (*tableConfig->MinTabletSize >= *tableConfig->DesiredTabletSize ||
            *tableConfig->DesiredTabletSize >= *tableConfig->MaxTabletSize)
        {
            YT_LOG_WARNING("Tablet size inequalities violated in tablet balancer config "
                "(TableId: %v, Config: %v)",
                table->GetId(),
                ConvertToYsonString(tableConfig, NYson::EYsonFormat::Text));
            desiredTabletSize = std::max(desiredTabletSize, minTabletSize + 1);
            maxTabletSize = std::max(maxTabletSize, desiredTabletSize + 1);
        }
    } else if (table->GetInMemoryMode() == EInMemoryMode::None) {
        // Tablet size attributes from bundle for ext-memory tables.
        minTabletSize = bundleConfig->MinTabletSize;
        maxTabletSize = bundleConfig->MaxTabletSize;
        desiredTabletSize = bundleConfig->DesiredTabletSize;
    } else {
        // Tablet size attributes from bundle for in-memory tables.
        minTabletSize = bundleConfig->MinInMemoryTabletSize;
        maxTabletSize = bundleConfig->MaxInMemoryTabletSize;
        desiredTabletSize = bundleConfig->DesiredInMemoryTabletSize;
    }

    // Balancer would not create too many tablets unless desired_tablet_count is set.
    if (!tableConfig->DesiredTabletCount) {
        i64 maxTabletCount = table->TabletCellBundle()->Cells().size() *
            bundleConfig->TabletToCellRatio;
        auto tabletSizeLimit = DivCeil(tableSize, maxTabletCount);
        if (desiredTabletSize < tabletSizeLimit) {
            desiredTabletSize = tabletSizeLimit;
            minTabletSize = static_cast<i64>(desiredTabletSize / 1.9);
            maxTabletSize = static_cast<i64>(desiredTabletSize * 1.9);

            YT_LOG_DEBUG_IF(enableVerboseLogging,
                "Tablet size config overridden by tablet to cell ratio"
                "(TableId: %v, MaxTabletCount: %v, MinTabletSize: %v, DesiredTabletSize: %v, "
                "MaxTabletSize: %v)",
                table->GetId(),
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
                table->GetId(),
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

i64 GetTabletBalancingSize(TTablet* tablet)
{
    auto statistics = tablet->GetTabletStatistics();
    return tablet->GetInMemoryMode() == EInMemoryMode::None
        ? statistics.UncompressedDataSize
        : statistics.MemorySize;
}

bool IsTabletReshardable(const TTablet* tablet, bool ignoreConfig)
{
    return tablet &&
        IsObjectAlive(tablet) &&
        (tablet->GetState() == ETabletState::Mounted || tablet->GetState() == ETabletState::Frozen) &&
        (!tablet->GetAction() || tablet->GetAction()->IsFinished()) &&
        IsObjectAlive(tablet->GetTable()) &&
        (ignoreConfig || tablet->GetTable()->TabletBalancerConfig()->EnableAutoReshard) &&
        tablet->GetTable()->IsPhysicallySorted() &&
        IsObjectAlive(tablet->GetCell()) &&
        IsObjectAlive(tablet->GetCell()->GetTabletCellBundle()) &&
        (ignoreConfig || tablet->GetCell()->GetTabletCellBundle()->TabletBalancerConfig()->EnableTabletSizeBalancer) &&
        tablet->Replicas().empty();
}

std::optional<TReshardDescriptor> MergeSplitTablet(
    TTablet* tablet,
    const TTabletSizeConfig& bounds,
    std::vector<int>* mergeBudgetByTabletIndex,
    TTabletBalancerContext* context)
{
    auto* table = tablet->GetTable();

    i64 desiredSize = bounds.DesiredTabletSize;
    i64 size = GetTabletBalancingSize(tablet);

    if (size >= bounds.MinTabletSize && size <= bounds.MaxTabletSize) {
        return {};
    }

    if (size < bounds.MinTabletSize && table->Tablets().size() == 1) {
        return {};
    }

    if (bounds.MinTabletCount &&
        ssize(table->Tablets()) <= *bounds.MinTabletCount &&
        size < bounds.MinTabletSize)
    {
        return {};
    }

    if (desiredSize == 0) {
        desiredSize = 1;
    }

    int startIndex = tablet->GetIndex();
    int endIndex = tablet->GetIndex();

    auto sizeGood = [&] () {
        int tabletCount = std::clamp<i64>(DivRound(size, desiredSize), 1, MaxTabletCount);
        i64 tabletSize = size / tabletCount;
        return tabletSize >= bounds.MinTabletSize && tabletSize <= bounds.MaxTabletSize;
    };

    auto takeMergeBudget = [&] (int index) {
        if (mergeBudgetByTabletIndex) {
            int result = (*mergeBudgetByTabletIndex)[index];
            (*mergeBudgetByTabletIndex)[index] = 0;
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
        context->IsTabletUntouched(table->Tablets()[startIndex - 1]->As<TTablet>()) &&
        table->Tablets()[startIndex - 1]->GetState() == tablet->GetState())
    {
        mergeBudget += takeMergeBudget(startIndex - 1);
        if (mergeBudget == 0) {
            break;
        }
        --mergeBudget;
        --startIndex;
        size += GetTabletBalancingSize(table->Tablets()[startIndex]->As<TTablet>());
    }
    while (!sizeGood() &&
        endIndex < std::ssize(table->Tablets()) - 1 &&
        context->IsTabletUntouched(table->Tablets()[endIndex + 1]->As<TTablet>()) &&
        table->Tablets()[endIndex + 1]->GetState() == tablet->GetState())
    {
        mergeBudget += takeMergeBudget(endIndex + 1);
        if (mergeBudget == 0) {
            break;
        }
        --mergeBudget;
        ++endIndex;
        size += GetTabletBalancingSize(table->Tablets()[endIndex]->As<TTablet>());
    }

    int newTabletCount = std::clamp<i64>(DivRound(size, desiredSize), 1, MaxTabletCount);

    if (endIndex == startIndex && tablet->NodeStatistics().partition_count() == 1) {
        return {};
    }

    if (endIndex == startIndex && newTabletCount == 1) {
        YT_LOG_DEBUG("Tablet balancer is unable to reshard tablet (TableId: %v, TabletId: %v, TabletSize: %v)",
            table->GetId(),
            tablet->GetId(),
            size);
        return {};
    }

    if (static_cast<int>(table->Tablets().size()) + newTabletCount -
        (endIndex - startIndex + 1) >= MaxTabletCount)
    {
        YT_LOG_DEBUG("Tablet balancer will not split tablets since tablet count "
            "would exceed the limit (TableId: %v, TabletId: %v, TabletsSize: %v, DesiredCount: %v)",
            table->GetId(),
            tablet->GetId(),
            size,
            newTabletCount);
    }

    std::vector<TTablet*> tablets;
    for (int index = startIndex; index <= endIndex; ++index) {
        auto* tablet = table->Tablets()[index]->As<TTablet>();
        tablets.push_back(tablet);
        context->TouchedTablets.insert(tablet);
    }

    return {TReshardDescriptor{tablets, newTabletCount, size}};
}

std::vector<TReshardDescriptor> MergeSplitTabletsOfTable(
    TRange<TTablet*> tabletRange,
    TTabletBalancerContext* context)
{
    YT_VERIFY(!tabletRange.Empty());

    std::vector<TTablet*> tablets(tabletRange.Begin(), tabletRange.End());
    std::sort(
        tablets.begin(),
        tablets.end(),
        [] (const TTablet* lhs, const TTablet* rhs) {
            return lhs->GetIndex() < rhs->GetIndex();
        });

    auto* table = tablets.front()->GetTable();
    auto config = GetTabletSizeConfig(table);

    // If MinTabletCount is set then the number of merges is limited. We want
    // to distribute merges evenly across the table. Merge budget (the number
    // of allowed merges) is distributed between starving tablets. When the
    // tablet is going to merge with its neighbour, the action should be paid
    // for from either of their budgets.
    std::vector<int> mergeBudgetByTabletIndex;
    if (config.MinTabletCount) {
        mergeBudgetByTabletIndex.resize(table->Tablets().size());

        int mergeBudget = std::max(
            0,
            static_cast<int>(table->Tablets().size()) - *config.MinTabletCount);
        std::vector<int> tabletsPendingMerge;
        for (auto* tablet : tablets) {
            if (GetTabletBalancingSize(tablet) < config.MinTabletSize) {
                tabletsPendingMerge.push_back(tablet->GetIndex());
            }
        }

        mergeBudget = std::min<int>(mergeBudget, tabletsPendingMerge.size());
        for (i64 multiplier = 0; multiplier < mergeBudget; ++multiplier) {
            int position = tabletsPendingMerge.size() * multiplier / mergeBudget;
            // Subsequent merging works more uniformly if budget tends to the right.
            position = tabletsPendingMerge.size() - position - 1;
            ++mergeBudgetByTabletIndex[tabletsPendingMerge[position]];
        }
    }

    std::vector<TReshardDescriptor> descriptors;
    for (auto* tablet : tablets) {
        auto descriptor = MergeSplitTablet(
            tablet,
            config,
            config.MinTabletCount ? &mergeBudgetByTabletIndex : nullptr,
            context);
        if (descriptor) {
            descriptors.push_back(*descriptor);
        }
    }
    return descriptors;
}

std::vector<TTabletMoveDescriptor> ReassignInMemoryTablets(
    const TTabletCellBundle* bundle,
    const std::optional<THashSet<const NTableServer::TTableNode*>>& movableTables,
    bool ignoreTableWiseConfig)
{
    const auto& config = bundle->TabletBalancerConfig();

    auto softThresholdViolated = [&] (i64 min, i64 max) {
        return max > 0 && 1.0 * (max - min) / max > config->SoftInMemoryCellBalanceThreshold;
    };

    auto hardThresholdViolated = [&] (i64 min, i64 max) {
        return max > 0 && 1.0 * (max - min) / max > config->HardInMemoryCellBalanceThreshold;
    };

    auto cells = bundle->GetAliveCells();

    if (cells.empty()) {
        return {};
    }

    // Not really necessary but makes the code look safer.
    std::sort(cells.begin(), cells.end(), TObjectIdComparer());

    struct TMemoryUsage {
        i64 Memory;
        const TTabletCell* TabletCell;

        bool operator<(const TMemoryUsage& other) const
        {
            if (Memory != other.Memory) {
                return Memory < other.Memory;
            }
            return TabletCell->GetId() < other.TabletCell->GetId();
        }
        bool operator>(const TMemoryUsage& other) const
        {
            return other < *this;
        }
    };

    std::vector<TMemoryUsage> memoryUsage;
    i64 total = 0;
    memoryUsage.reserve(cells.size());
    for (const auto* cell : cells) {
        i64 size = cell->GossipStatistics().Local().MemorySize;
        total += size;
        memoryUsage.push_back({size, cell});
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

    std::vector<TTabletMoveDescriptor> moveDescriptors;

    for (int index = memoryUsage.size() - 1; index >= 0; --index) {
        auto cellSize = memoryUsage[index].Memory;
        auto* cell = memoryUsage[index].TabletCell;

        std::vector<TTablet*> tablets;
        tablets.reserve(cell->Tablets().size());
        for (auto* tablet : cell->Tablets()) {
            if (tablet->GetType() != EObjectType::Tablet) {
                continue;
            }
            tablets.push_back(tablet->As<TTablet>());
        }

        std::sort(tablets.begin(), tablets.end(), [] (TTablet* lhs, TTablet* rhs) {
            return lhs->GetId() < rhs->GetId();
        });

        for (auto* tablet : tablets) {
            if (tablet->GetInMemoryMode() == EInMemoryMode::None) {
                continue;
            }

            if (!ignoreTableWiseConfig && !tablet->GetTable()->TabletBalancerConfig()->EnableAutoTabletMove) {
                continue;
            }

            if (movableTables && !movableTables->contains(tablet->GetTable())) {
                continue;
            }

            if (queue.empty() || cellSize <= mean) {
                break;
            }

            auto top = queue.top();

            if (!softThresholdViolated(top.Memory, cellSize)) {
                break;
            }

            auto statistics = tablet->GetTabletStatistics();
            auto tabletSize = statistics.MemorySize;

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

                moveDescriptors.push_back({tablet, top.TabletCell->GetId()});
            }
        }
    }

    return moveDescriptors;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
