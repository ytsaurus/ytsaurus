#include "helpers.h"

#include "config.h"
#include "tablet_cell.h"

#include <yt/yt/server/master/table_server/table_node.h>

namespace NYT::NTabletServer {

using namespace NTableServer;
using namespace NCypressClient;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NHydra;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

bool IsHunkChunk(const TTabletBase* tablet, const TChunkTree* child)
{
    if (!child) {
        return false;
    }

    if (tablet->GetType() == EObjectType::Tablet && IsJournalChunkType(child->GetType())) {
        return true;
    }

    if (!IsBlobChunkType(child->GetType())) {
        return false;
    }

    const auto* chunk = child->AsChunk();
    return chunk->GetChunkType() == EChunkType::Hunk;
}

bool IsDynamicStoreReadEnabled(
    const TTableNode* table,
    const TDynamicTabletManagerConfigPtr& dynamicConfig)
{
    if (table->IsPhysicallyLog() && !table->IsReplicated()) {
        return false;
    }

    if (table->GetActualTabletState() == ETabletState::Unmounted) {
        return table->GetEnableDynamicStoreRead().value_or(
            dynamicConfig->EnableDynamicStoreReadByDefault);
    } else {
        return table->GetMountedWithEnabledDynamicStoreRead();
    }
}

static TError TryParseTabletRange(const TTabletOwnerBase* table, int* first, int* last)
{
    const auto& tablets = table->Tablets();
    if (*first == -1 && *last == -1) {
        *first = 0;
        *last = ssize(tablets) - 1;
    } else {
        if (*first < 0 || *first >= std::ssize(tablets)) {
            return TError("First tablet index %v is out of range [%v, %v]",
                *first,
                0,
                tablets.size() - 1);
        }
        if (*last < 0 || *last >= std::ssize(tablets)) {
            return TError("Last tablet index %v is out of range [%v, %v]",
                *last,
                0,
                tablets.size() - 1);
        }
        if (*first > *last) {
            return TError("First tablet index is greater than last tablet index");
        }
    }

    return TError();
}

void ParseTabletRange(TTabletOwnerBase* owner, int* first, int* last)
{
    auto error = TryParseTabletRange(owner, first, last);
    YT_VERIFY(error.IsOK());
}

void ParseTabletRangeOrThrow(const TTabletOwnerBase* table, int* first, int* last)
{
    TryParseTabletRange(table, first, last)
        .ThrowOnError();
}

bool IsCellActive(TTabletCell* cell)
{
    return IsObjectAlive(cell) && !cell->IsDecommissionStarted();
}

bool CheckHasHealthyCells(TTabletCellBundle* bundle)
{
    for (auto* cellBase : bundle->Cells()) {
        if (cellBase->GetType() != EObjectType::TabletCell) {
            continue;
        }

        auto* cell = cellBase->As<TTabletCell>();
        if (!IsCellActive(cell)) {
            continue;
        }
        if (cell->IsHealthy()) {
            return true;
        }
    }

    return false;
}

void ValidateHasHealthyCells(TTabletCellBundle* bundle)
{
    if (!CheckHasHealthyCells(bundle)) {
        THROW_ERROR_EXCEPTION("No healthy tablet cells in bundle %Qv",
            bundle->GetName());
    }
}

std::vector<std::pair<TTabletBase*, TTabletCell*>> ComputeTabletAssignment(
    TTabletOwnerBase* table,
    TTabletCell* hintCell,
    std::vector<TTabletBaseRawPtr> tabletsToMount,
    i64 tabletDataSizeFootprint)
{
    if (IsCellActive(hintCell)) {
        std::vector<std::pair<TTabletBase*, TTabletCell*>> assignment;
        for (auto tablet : tabletsToMount) {
            assignment.emplace_back(tablet, hintCell);
        }
        return assignment;
    }

    struct TCellKey
    {
        i64 Size;
        TTabletCell* Cell;

        //! Compares by |(size, cellId)|.
        bool operator<(const TCellKey& other) const
        {
            if (Size < other.Size) {
                return true;
            } else if (Size > other.Size) {
                return false;
            }
            return Cell->GetId() < other.Cell->GetId();
        }
    };

    auto mutationContext = GetCurrentMutationContext();

    auto getCellSize = [&] (const TTabletCell* cell) -> i64 {
        i64 result = 0;
        i64 tabletCount;
        switch (table->GetInMemoryMode()) {
            case EInMemoryMode::None:
                result = mutationContext->RandomGenerator()->Generate<i64>();
                break;
            case EInMemoryMode::Uncompressed:
            case EInMemoryMode::Compressed: {
                result += cell->GossipStatistics().Local().MemorySize;
                tabletCount = cell->GossipStatistics().Local().TabletCountPerMemoryMode[EInMemoryMode::Uncompressed] +
                    cell->GossipStatistics().Local().TabletCountPerMemoryMode[EInMemoryMode::Compressed];
                result += tabletCount * tabletDataSizeFootprint;
                break;
            }
            default:
                YT_ABORT();
        }
        return result;
    };

    std::vector<TCellKey> cellKeys;
    for (auto* cellBase : GetValuesSortedByKey(table->TabletCellBundle()->Cells())) {
        if (cellBase->GetType() != EObjectType::TabletCell) {
            continue;
        }

        auto* cell = cellBase->As<TTabletCell>();
        if (!IsCellActive(cell)) {
            continue;
        }

        if (cell->CellBundle() == table->TabletCellBundle()) {
            cellKeys.push_back(TCellKey{getCellSize(cell), cell});
        }
    }
    if (cellKeys.empty()) {
        cellKeys.push_back(TCellKey{0, nullptr});
    }
    std::sort(cellKeys.begin(), cellKeys.end());

    auto getTabletSize = [&] (const TTabletBase* tablet) -> i64 {
        i64 result = 0;
        auto statistics = tablet->GetTabletStatistics();
        switch (table->GetInMemoryMode()) {
            case EInMemoryMode::None:
                result += statistics.UncompressedDataSize;
                break;
            case EInMemoryMode::Uncompressed:
            case EInMemoryMode::Compressed:
                result += tablet->GetTabletStaticMemorySize(table->GetInMemoryMode());
                break;
            default:
                YT_ABORT();
        }
        result += tabletDataSizeFootprint;
        return result;
    };

    // Sort tablets by decreasing size to improve greedy heuristic performance.
    std::sort(
        tabletsToMount.begin(),
        tabletsToMount.end(),
        [&] (const TTabletBase* lhs, const TTabletBase* rhs) {
            return
                std::tuple(getTabletSize(lhs), lhs->GetId()) >
                std::tuple(getTabletSize(rhs), rhs->GetId());
        });

    // Assign tablets to cells iteratively looping over cell array.
    int cellIndex = 0;
    std::vector<std::pair<TTabletBase*, TTabletCell*>> assignment;
    for (auto tablet : tabletsToMount) {
        assignment.emplace_back(tablet, cellKeys[cellIndex].Cell);
        if (++cellIndex == std::ssize(cellKeys)) {
            cellIndex = 0;
        }
    }

    return assignment;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
