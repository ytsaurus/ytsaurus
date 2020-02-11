#include "balancing_helpers.h"
#include "config.h"
#include "private.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_manager.h"

#include <yt/server/master/table_server/table_node.h>

#include <yt/core/misc/numeric_helpers.h>

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

TTabletSizeConfig GetTabletSizeConfig(TTablet* tablet)
{
    i64 minTabletSize;
    i64 maxTabletSize;
    i64 desiredTabletSize = 0;

    const auto& config = tablet->GetCell()->GetTabletCellBundle()->TabletBalancerConfig();
    auto* table = tablet->GetTable();
    auto desiredTabletCount = table->GetDesiredTabletCount();
    auto statistics = table->ComputeTotalStatistics();
    i64 tableSize = tablet->GetInMemoryMode() == EInMemoryMode::Compressed
        ? statistics.compressed_data_size()
        : statistics.uncompressed_data_size();
    i64 cellCount = tablet->GetCell()->GetTabletCellBundle()->Cells().size() *
        config->TabletToCellRatio;

    if (!desiredTabletCount) {
        minTabletSize = tablet->GetInMemoryMode() == EInMemoryMode::None
            ? config->MinTabletSize
            : config->MinInMemoryTabletSize;
        maxTabletSize = tablet->GetInMemoryMode() == EInMemoryMode::None
            ? config->MaxTabletSize
            : config->MaxInMemoryTabletSize;
        desiredTabletSize = tablet->GetInMemoryMode() == EInMemoryMode::None
            ? config->DesiredTabletSize
            : config->DesiredInMemoryTabletSize;

        auto tableMinTabletSize = table->GetMinTabletSize();
        auto tableMaxTabletSize = table->GetMaxTabletSize();
        auto tableDesiredTabletSize = table->GetDesiredTabletSize();

        if (tableMinTabletSize && tableMaxTabletSize && tableDesiredTabletSize &&
            *tableMinTabletSize < *tableDesiredTabletSize &&
            *tableDesiredTabletSize < *tableMaxTabletSize)
        {
            minTabletSize = *tableMinTabletSize;
            maxTabletSize = *tableMaxTabletSize;
            desiredTabletSize = *tableDesiredTabletSize;
        }
    } else {
        cellCount = *desiredTabletCount;
    }

    if (cellCount == 0) {
        cellCount = 1;
    }

    auto tabletSize = DivCeil(tableSize, cellCount);
    if (desiredTabletSize < tabletSize) {
        desiredTabletSize = tabletSize;
        minTabletSize = static_cast<i64>(desiredTabletSize / 1.9);
        maxTabletSize = static_cast<i64>(desiredTabletSize * 1.9);
    }

    return TTabletSizeConfig{minTabletSize, maxTabletSize, desiredTabletSize};
}

i64 GetTabletBalancingSize(TTablet* tablet, const TTabletManagerPtr& tabletManager)
{
    auto statistics = tabletManager->GetTabletStatistics(tablet);
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

std::vector<TReshardDescriptor> MergeSplitTablet(
    TTablet* tablet,
    const TTabletSizeConfig& bounds,
    TTabletBalancerContext* context,
    const TTabletManagerPtr& tabletManager)
{
    auto* table = tablet->GetTable();

    i64 desiredSize = bounds.DesiredTabletSize;
    i64 size = GetTabletBalancingSize(tablet, tabletManager);

    if (size >= bounds.MinTabletSize && size <= bounds.MaxTabletSize) {
        return {};
    }

    if (size < bounds.MinTabletSize && table->Tablets().size() == 1) {
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

    while (!sizeGood() &&
        startIndex > 0 &&
        context->IsTabletUntouched(table->Tablets()[startIndex - 1]) &&
        table->Tablets()[startIndex - 1]->GetState() == tablet->GetState())
    {
        --startIndex;
        size += GetTabletBalancingSize(table->Tablets()[startIndex], tabletManager);
    }
    while (!sizeGood() &&
        endIndex < table->Tablets().size() - 1 &&
        context->IsTabletUntouched(table->Tablets()[endIndex + 1]) &&
        table->Tablets()[endIndex + 1]->GetState() == tablet->GetState())
    {
        ++endIndex;
        size += GetTabletBalancingSize(table->Tablets()[endIndex], tabletManager);
    }

    int newTabletCount = std::clamp<i64>(DivRound(size, desiredSize), 1, MaxTabletCount);

    if (endIndex == startIndex && tablet->NodeStatistics().partition_count() == 1) {
        return {};
    }

    if (newTabletCount == endIndex - startIndex + 1 && newTabletCount == 1) {
        YT_LOG_DEBUG("Tablet balancer is unable to reshard tablet (TableId: %v, TabletId: %v, TabletSize: %v)",
            table->GetId(),
            tablet->GetId(),
            size);
        return {};
    }

    std::vector<TTablet*> tablets;
    for (int index = startIndex; index <= endIndex; ++index) {
        auto* tablet = table->Tablets()[index];
        tablets.push_back(tablet);
        context->TouchedTablets.insert(tablet);
    }

    return {TReshardDescriptor{tablets, newTabletCount, size}};
}

std::vector<TTabletMoveDescriptor> ReassignInMemoryTablets(
    const TTabletCellBundle* bundle,
    const std::optional<THashSet<const NTableServer::TTableNode*>>& movableTables,
    bool ignoreTableWiseConfig,
    TTabletBalancerContext* context,
    const TTabletManagerPtr& tabletManager)
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
    std::sort(cells.begin(), cells.end(), TObjectRefComparer::Compare);

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

        std::vector<TTablet*> tablets(cell->Tablets().begin(), cell->Tablets().end());
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

            auto statistics = tabletManager->GetTabletStatistics(tablet);
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
