#include "chunk_tree_traverser.h"
#include "chunk.h"
#include "chunk_view.h"
#include "dynamic_store.h"
#include "chunk_list.h"
#include "helpers.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>

#include <yt/server/master/object_server/object.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/singleton.h>

#include <yt/core/profiling/timing.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const int MaxChunksPerStep = 1000;

static const auto RowCountMember = &TCumulativeStatisticsEntry::RowCount;
static const auto ChunkCountMember = &TCumulativeStatisticsEntry::ChunkCount;
static const auto DataSizeMember = &TCumulativeStatisticsEntry::DataSize;

////////////////////////////////////////////////////////////////////////////////

template <class TIterator, class TKey, class TIsLess, class TIsMissing>
TIterator UpperBoundWithMissingValues(
    TIterator start,
    TIterator end,
    const TKey& key,
    TIsLess isLess,
    TIsMissing isMissing)
{
    while (true) {
        auto distance = std::distance(start, end);
        if (distance <= 1) {
            break;
        }
        auto median = start + (distance / 2);
        auto cur = median;
        while (cur > start && isMissing(*cur)) {
            --cur;
        }
        if (isMissing(*cur)) {
            start = median;
        } else {
            if (isLess(key, *cur)) {
                end = cur;
            } else {
                start = median;
            }
        }
    }
    if (!isMissing(*start) && isLess(key, *start)) {
        return start;
    } else {
        return end;
    }
}

class TChunkTreeTraverser
    : public TRefCounted
{
protected:
    struct TStackEntry
    {
        TChunkList* ChunkList;
        int ChunkListVersion;
        int ChildIndex;
        i64 RowIndex;
        std::optional<i32> TabletIndex;
        TReadLimit LowerBound;
        TReadLimit UpperBound;

        TStackEntry(
            TChunkList* chunkList,
            int childIndex,
            i64 rowIndex,
            std::optional<i32> tabletIndex,
            const TReadLimit& lowerBound,
            const TReadLimit& upperBound)
            : ChunkList(chunkList)
            , ChunkListVersion(chunkList->GetVersion())
            , ChildIndex(childIndex)
            , RowIndex(rowIndex)
            , TabletIndex(tabletIndex)
            , LowerBound(lowerBound)
            , UpperBound(upperBound)
        {
            YT_VERIFY(childIndex >= 0);
            YT_VERIFY(rowIndex >= 0);
        }
    };

    void OnTimeSpent(TDuration elapsed)
    {
        CpuTime_ += elapsed;
        Callbacks_->OnTimeSpent(elapsed);
    }

    void OnFinish(const TError& error)
    {
        YT_LOG_DEBUG(error, "Chunk tree traversal finished (CpuTime: %v, WallTime: %v, ChunkCount: %v, ChunkListCount: %v)",
            CpuTime_,
            TInstant::Now() - StartInstant_,
            ChunkCount_,
            ChunkListCount_);
        Visitor_->OnFinish(error);
    }

    void DoTraverse()
    {
        try {
            GuardedTraverse();
        } catch (const std::exception& ex) {
            Shutdown();
            OnFinish(TError(ex));
        }
    }

    void GuardedTraverse()
    {
        NProfiling::TWallTimer timer;
        auto invoker = Callbacks_->GetInvoker();
        int visitedChunkCount = 0;
        while (visitedChunkCount < MaxChunksPerStep || !invoker) {
            if (IsStackEmpty()) {
                OnTimeSpent(timer.GetElapsedTime());
                Shutdown();
                OnFinish(TError());
                return;
            }

            auto& entry = PeekStack();
            auto* chunkList = entry.ChunkList;

            if (!chunkList->IsAlive() || chunkList->GetVersion() != entry.ChunkListVersion) {
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::OptimisticLockFailure,
                    "Optimistic locking failed for chunk list %v",
                    chunkList->GetId());
            }

            if (entry.ChildIndex == chunkList->Children().size()) {
                PopStack();
                continue;
            }

            auto* child = chunkList->Children()[entry.ChildIndex];

            // YT-4840: Skip empty children since Get(Min|Max)Key will not work for them.
            if (IsEmpty(child)) {
                ++entry.ChildIndex;
                continue;
            }

            switch (chunkList->GetKind()) {
                case EChunkListKind::Static:
                    VisitEntryStatic(&entry, &visitedChunkCount);
                    break;

                case EChunkListKind::SortedDynamicRoot:
                case EChunkListKind::OrderedDynamicRoot:
                    VisitEntryDynamicRoot(&entry);
                    break;

                case EChunkListKind::SortedDynamicTablet:
                case EChunkListKind::SortedDynamicSubtablet:
                case EChunkListKind::OrderedDynamicTablet:
                    VisitEntryDynamic(&entry, &visitedChunkCount);
                    break;

                default:
                    YT_ABORT();
            }

        }

        // Schedule continuation.
        Callbacks_->OnTimeSpent(timer.GetElapsedTime());
        invoker->Invoke(BIND(&TChunkTreeTraverser::DoTraverse, MakeStrong(this)));
    }

    i64 FetchPrevSum(
        const TChunkList* chunkList,
        int childIndex,
        i64 TCumulativeStatisticsEntry::* member)
    {
        return chunkList->CumulativeStatistics().GetPreviousSum(childIndex).*member;
    }

    i64 FetchCurrentSum(
        const TChunkList* chunkList,
        int childIndex,
        i64 TCumulativeStatisticsEntry::* member)
    {
        return chunkList->CumulativeStatistics().GetCurrentSum(childIndex).*member;
    }

    i64 FetchCurrentSumWithFallback(
        const TChunkList* chunkList,
        int childIndex,
        i64 TCumulativeStatisticsEntry::* member,
        i64 fallback)
    {
        return childIndex == chunkList->Children().size() - 1
            ? fallback
            : chunkList->CumulativeStatistics().GetCurrentSum(childIndex).*member;
    }

    void VisitEntryStatic(TStackEntry* entry, int* visitedChunkCount)
    {
        auto* chunkList = entry->ChunkList;
        const auto& statistics = chunkList->Statistics();
        auto* child = chunkList->Children()[entry->ChildIndex];

        // Tablet Index
        YT_VERIFY(!entry->LowerBound.HasTabletIndex() && !entry->UpperBound.HasTabletIndex());

        TReadLimit childLowerBound;
        TReadLimit childUpperBound;

        i64 rowIndex = 0;

        // Row index
        {
            i64 childLimit = FetchPrevSum(chunkList, entry->ChildIndex, RowCountMember);
            rowIndex = entry->RowIndex + childLimit;
            if (entry->UpperBound.HasRowIndex()) {
                if (entry->UpperBound.GetRowIndex() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetRowIndex(childLimit);
                i64 totalRowCount = statistics.Sealed ? statistics.LogicalRowCount : std::numeric_limits<i64>::max();
                childUpperBound.SetRowIndex(
                    FetchCurrentSumWithFallback(
                        chunkList,
                        entry->ChildIndex,
                        RowCountMember,
                        totalRowCount));
            } else if (entry->LowerBound.HasRowIndex()) {
                childLowerBound.SetRowIndex(childLimit);
            }
        }

        // Chunk index
        {
            i64 childLimit = FetchPrevSum(chunkList, entry->ChildIndex, ChunkCountMember);
            if (entry->UpperBound.HasChunkIndex()) {
                if (entry->UpperBound.GetChunkIndex() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetChunkIndex(childLimit);
                childUpperBound.SetChunkIndex(FetchCurrentSum(chunkList, entry->ChildIndex, ChunkCountMember));
            } else if (entry->LowerBound.HasChunkIndex()) {
                childLowerBound.SetChunkIndex(childLimit);
            }
        }

        // Offset
        {
            i64 childLimit = FetchPrevSum(chunkList, entry->ChildIndex, DataSizeMember);
            if (entry->UpperBound.HasOffset()) {
                if (entry->UpperBound.GetOffset() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetOffset(childLimit);
                childUpperBound.SetOffset(FetchCurrentSum(chunkList, entry->ChildIndex, DataSizeMember));
            } else if (entry->LowerBound.HasOffset()) {
                childLowerBound.SetOffset(childLimit);
            }
        }

        // Key
        {
            if (entry->UpperBound.HasKey()) {
                YT_LOG_ALERT_UNLESS(KeyColumnCount_, "Chunk tree traverser entry has key bounds, "
                    "but `key_column_count` parameter is not set");

                childLowerBound.SetKey(GetMinKeyOrThrow(child, KeyColumnCount_));
                if (entry->UpperBound.GetKey() <= childLowerBound.GetKey()) {
                    PopStack();
                    return;
                }
                childUpperBound.SetKey(GetUpperBoundKeyOrThrow(child, KeyColumnCount_));
            } else if (entry->LowerBound.HasKey()) {
                YT_LOG_ALERT_UNLESS(KeyColumnCount_, "Chunk tree traverser entry has key bounds, "
                    "but `key_column_count` parameter is not set");

                childLowerBound.SetKey(GetMinKeyOrThrow(child, KeyColumnCount_));
            }
        }

        ++entry->ChildIndex;

        TReadLimit subtreeStartLimit;
        TReadLimit subtreeEndLimit;
        GetInducedSubtreeLimits(
            *entry,
            childLowerBound,
            childUpperBound,
            &subtreeStartLimit,
            &subtreeEndLimit);

        switch (child->GetType()) {
            case EObjectType::ChunkList: {
                auto* childChunkList = child->AsChunkList();
                GetStartChildIndex(childChunkList, rowIndex, std::nullopt, subtreeStartLimit, subtreeEndLimit);
                break;
            }

            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::JournalChunk:
            case EObjectType::ErasureJournalChunk: {
                auto* childChunk = child->AsChunk();
                if (!Visitor_->OnChunk(
                    childChunk,
                    rowIndex,
                    std::nullopt, /*tabletIndex*/
                    subtreeStartLimit,
                    subtreeEndLimit,
                    {} /*timestampTransactionId*/))
                {
                    Shutdown();
                    return;
                }
                ++ChunkCount_;
                *visitedChunkCount += 1;
                break;
            }

            default:
                YT_ABORT();
        }
    }

    void VisitEntryDynamicRoot(TStackEntry* entry)
    {
        auto* chunkList = entry->ChunkList;
        auto* child = chunkList->Children()[entry->ChildIndex];
        bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot;

        // Row Index
        YT_VERIFY((!entry->LowerBound.HasRowIndex() && !entry->UpperBound.HasRowIndex()) || isOrdered);

        // Offset
        YT_VERIFY(!entry->LowerBound.HasOffset() && !entry->UpperBound.HasOffset());

        // Tablet Index
        YT_VERIFY((!entry->LowerBound.HasTabletIndex() && !entry->UpperBound.HasTabletIndex()) || isOrdered);

        TReadLimit childLowerBound;
        TReadLimit childUpperBound;

        auto pivotKey = chunkList->Children()[entry->ChildIndex]->AsChunkList()->GetPivotKey();
        auto nextPivotKey = entry->ChildIndex + 1 < chunkList->Children().size()
            ? chunkList->Children()[entry->ChildIndex + 1]->AsChunkList()->GetPivotKey()
            : MaxKey();

        // Row Index
        if (isOrdered) {
            i64 childLimit = FetchPrevSum(chunkList, entry->ChildIndex, RowCountMember);
            if (entry->UpperBound.HasRowIndex()) {
                if (entry->UpperBound.GetRowIndex() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetRowIndex(childLimit);
                childUpperBound.SetRowIndex(FetchCurrentSum(chunkList, entry->ChildIndex, RowCountMember));
            } else if (entry->LowerBound.HasRowIndex()) {
                childLowerBound.SetRowIndex(childLimit);
            }
        }

        // Tablet Index
        std::optional<i32> tabletIndex;
        {
            if (entry->UpperBound.HasTabletIndex() && entry->UpperBound.GetTabletIndex() < entry->ChildIndex) {
                PopStack();
                return;
            }
            if (isOrdered) {
                tabletIndex = entry->ChildIndex;
            }
        }

        // Chunk index
        {
            i64 childLimit = FetchPrevSum(chunkList, entry->ChildIndex, ChunkCountMember);
            if (entry->UpperBound.HasChunkIndex()) {
                if (entry->UpperBound.GetChunkIndex() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetChunkIndex(childLimit);
                childUpperBound.SetChunkIndex(FetchCurrentSum(chunkList, entry->ChildIndex, ChunkCountMember));
            } else if (entry->LowerBound.HasChunkIndex()) {
                childLowerBound.SetChunkIndex(childLimit);
            }
        }

        // Key
        {
            if (entry->LowerBound.HasKey() || entry->UpperBound.HasKey()) {
                YT_LOG_ALERT_UNLESS(KeyColumnCount_, "Chunk tree traverser entry has key bounds, "
                    "but `key_column_count` parameter is not set");
            }

            if (entry->UpperBound.HasKey()) {
                // NB: It's OK here without key widening, however there may be some inefficiency,
                // e.g. with 2 key columns, pivotKey = [0] and upperBound = [0, #Min] we could have return.

                if (entry->UpperBound.GetKey() <= pivotKey) {
                    PopStack();
                    return;
                }
            }

            childLowerBound.SetKey(pivotKey);
            childUpperBound.SetKey(nextPivotKey);
        }

        ++entry->ChildIndex;

        TReadLimit subtreeStartLimit;
        TReadLimit subtreeEndLimit;
        GetInducedSubtreeLimits(
            *entry,
            childLowerBound,
            childUpperBound,
            &subtreeStartLimit,
            &subtreeEndLimit);

        // NB: Chunks may cross tablet boundaries.
        if (chunkList->GetKind() == EChunkListKind::SortedDynamicRoot) {
            if (!subtreeStartLimit.HasKey() || subtreeStartLimit.GetKey() < pivotKey) {
                subtreeStartLimit.SetKey(pivotKey);
            }
            if (!subtreeEndLimit.HasKey() || subtreeEndLimit.GetKey() > nextPivotKey) {
                subtreeEndLimit.SetKey(nextPivotKey);
            }
        }

        auto* childChunkList = child->AsChunkList();
        GetStartChildIndex(childChunkList, 0, tabletIndex, subtreeStartLimit, subtreeEndLimit);
    }

    void VisitEntryDynamic(TStackEntry* entry, int* visitedChunkCount)
    {
        auto* chunkList = entry->ChunkList;

        auto* child = chunkList->Children()[entry->ChildIndex];
        const auto& statistics = chunkList->Statistics();

        bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet;

        // Row Index
        YT_VERIFY((!entry->LowerBound.HasRowIndex() && !entry->UpperBound.HasRowIndex()) || isOrdered);

        // Offset
        YT_VERIFY(!entry->LowerBound.HasOffset() && !entry->UpperBound.HasOffset());

        // Tablet Index
        YT_VERIFY((!entry->LowerBound.HasTabletIndex() && !entry->UpperBound.HasTabletIndex()) || isOrdered);

        auto tabletIndex = entry->TabletIndex;

        TReadLimit childLowerBound;
        TReadLimit childUpperBound;

        int childIndex = entry->ChildIndex;
        ++entry->ChildIndex;

        // Row index
        // NB: In tablet entry->RowIndex is always zero.
        i64 rowIndex = 0;
        if (isOrdered) {
            i64 childLimit = FetchPrevSum(chunkList, childIndex, RowCountMember);
            rowIndex = childLimit;
            if (entry->UpperBound.HasRowIndex()) {
                if (entry->UpperBound.GetRowIndex() <= childLimit) {
                    PopStack();
                    return;
                }
                YT_VERIFY(statistics.Sealed);
                childLowerBound.SetRowIndex(childLimit);
                childUpperBound.SetRowIndex(FetchCurrentSum(chunkList, childIndex, RowCountMember));
            } else if (entry->LowerBound.HasRowIndex()) {
                childLowerBound.SetRowIndex(childLimit);
            }
        }


        // Chunk index
        {
            i64 childLimit = FetchPrevSum(chunkList, childIndex, ChunkCountMember);

            if (entry->UpperBound.HasChunkIndex()) {
                if (entry->UpperBound.GetChunkIndex() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetChunkIndex(childLimit);
                childUpperBound.SetChunkIndex(FetchCurrentSum(chunkList, childIndex, ChunkCountMember));
            } else if (entry->LowerBound.HasChunkIndex()) {
                childLowerBound.SetChunkIndex(childLimit);
            }
        }

        // Tablet index
        {
            if (entry->LowerBound.HasTabletIndex() && entry->LowerBound.GetTabletIndex() > *tabletIndex) {
                return;
            }
            if (entry->UpperBound.HasTabletIndex() && entry->UpperBound.GetTabletIndex() < *tabletIndex) {
                PopStack();
                return;
            }
        }

        // Key
        {
            if (entry->UpperBound.HasKey() || entry->LowerBound.HasKey()) {
                childLowerBound.SetKey(GetMinKeyOrThrow(child, KeyColumnCount_));
                childUpperBound.SetKey(GetUpperBoundKeyOrThrow(child, KeyColumnCount_));

                if (entry->UpperBound.HasKey() && entry->UpperBound.GetKey() <= childLowerBound.GetKey()) {
                    return;
                }

                if (entry->LowerBound.HasKey() && entry->LowerBound.GetKey() > childUpperBound.GetKey()) {
                    return;
                }
            }
        }

        TReadLimit subtreeStartLimit;
        TReadLimit subtreeEndLimit;
        GetInducedSubtreeLimits(
            *entry,
            childLowerBound,
            childUpperBound,
            &subtreeStartLimit,
            &subtreeEndLimit);

        switch (child->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::ChunkView: {
                TChunk* childChunk = nullptr;
                TTransactionId timestampTransactionId;
                if (child->GetType() == EObjectType::ChunkView) {
                    auto* chunkView = child->AsChunkView();
                    if (Visitor_->OnChunkView(chunkView)) {
                        ++ChunkCount_;
                        *visitedChunkCount += 1;
                        return;
                    }

                    subtreeStartLimit = chunkView->GetAdjustedLowerReadLimit(subtreeStartLimit);
                    subtreeEndLimit = chunkView->GetAdjustedUpperReadLimit(subtreeEndLimit);
                    timestampTransactionId = chunkView->GetTransactionId();

                    childChunk = chunkView->GetUnderlyingChunk();
                } else {
                    childChunk = child->AsChunk();
                }

                // NB: For non-trivial subtreeStartLimit or subtreeEndLimit we set row_index
                // which is different from rowIndex parameter.
                // First one means rowIndex in chunk, but second one means rowIndex in tablet.
                if (!Visitor_->OnChunk(
                    childChunk,
                    rowIndex,
                    tabletIndex,
                    subtreeStartLimit,
                    subtreeEndLimit,
                    timestampTransactionId))
                {
                    Shutdown();
                    return;
                }

                ++ChunkCount_;
                *visitedChunkCount += 1;
                break;
            }

            case EObjectType::SortedDynamicTabletStore:
            case EObjectType::OrderedDynamicTabletStore: {
                auto* dynamicStore = child->AsDynamicStore();
                if (!Visitor_->OnDynamicStore(dynamicStore, subtreeStartLimit, subtreeEndLimit)) {
                    Shutdown();
                    return;
                }
                break;
            }

            case EObjectType::ChunkList: {
                auto* childChunkList = child->AsChunkList();
                YT_VERIFY(childChunkList->GetKind() == EChunkListKind::SortedDynamicSubtablet);
                GetStartChildIndex(childChunkList, 0, tabletIndex, subtreeStartLimit, subtreeEndLimit);
                break;
            }

            default:
                Y_UNREACHABLE();
        }
    }

    void GetStartChildIndex(
        TChunkList* chunkList,
        i64 rowIndex,
        std::optional<i32> tabletIndex,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        if (chunkList->Children().empty()) {
            return;
        }

        switch (chunkList->GetKind()) {
            case EChunkListKind::Static:
                return GetStartChildIndexStatic(chunkList, rowIndex, lowerBound, upperBound);

            case EChunkListKind::SortedDynamicRoot:
            case EChunkListKind::OrderedDynamicRoot:
                return GetStartChildIndexDynamicRoot(chunkList, rowIndex, lowerBound, upperBound);

            case EChunkListKind::SortedDynamicTablet:
            case EChunkListKind::SortedDynamicSubtablet:
            case EChunkListKind::OrderedDynamicTablet:
                return GetStartChildIndexDynamic(chunkList, rowIndex, tabletIndex, lowerBound, upperBound);

            default:
                YT_ABORT();
        }
    }

    int AdjustStartChildIndex(
        int currentIndex,
        const TChunkList* chunkList,
        i64 TCumulativeStatisticsEntry::* member,
        i64 limit,
        i64 total)
    {
        const auto& cumulativeStatistics = chunkList->CumulativeStatistics();

        if (limit < total) {
            // We should not take the last chunk into account because it may be unsealed
            // and so have incorrect cumulative statistics.
            return std::max(
                currentIndex,
                std::min<int>(
                    cumulativeStatistics.UpperBound(limit, member),
                    chunkList->Children().size() - 1));
        } else {
            return chunkList->Children().size();
        }
    }

    void GetStartChildIndexStatic(
        TChunkList* chunkList,
        i64 rowIndex,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        int result = 0;
        const auto& statistics = chunkList->Statistics();

        YT_VERIFY(!lowerBound.HasTabletIndex());

        // Row Index
        if (lowerBound.HasRowIndex()) {
            i64 totalRowCount = statistics.Sealed ? statistics.LogicalRowCount : std::numeric_limits<i64>::max();
            result = AdjustStartChildIndex(
                result,
                chunkList,
                RowCountMember,
                lowerBound.GetRowIndex(),
                totalRowCount);
        }

        // Chunk index
        if (lowerBound.HasChunkIndex()) {
            result = AdjustStartChildIndex(
                result,
                chunkList,
                ChunkCountMember,
                lowerBound.GetChunkIndex(),
                statistics.LogicalChunkCount);
        }

        // Offset
        if (lowerBound.HasOffset()) {
            result = AdjustStartChildIndex(
                result,
                chunkList,
                DataSizeMember,
                lowerBound.GetOffset(),
                statistics.UncompressedDataSize);
        }

        // Key
        if (lowerBound.HasKey()) {
            typedef std::vector<TChunkTree*>::const_iterator TChildrenIterator;
            std::reverse_iterator<TChildrenIterator> rbegin(chunkList->Children().end());
            std::reverse_iterator<TChildrenIterator> rend(chunkList->Children().begin());

            auto it = UpperBoundWithMissingValues(
                rbegin,
                rend,
                lowerBound.GetKey(),
                // isLess
                [keyColumnCount = KeyColumnCount_] (const TOwningKey& key, const TChunkTree* chunkTree) {
                    return key > GetUpperBoundKeyOrThrow(chunkTree, keyColumnCount);
                },
                // isMissing
                [] (const TChunkTree* chunkTree) {
                    return IsEmpty(chunkTree);
                });

            result = std::max(result, static_cast<int>(rend - it));
        }

        PushStack(TStackEntry(
            chunkList,
            result,
            rowIndex,
            std::nullopt,
            lowerBound,
            upperBound));
    }

    void GetStartChildIndexDynamicRoot(
        TChunkList* chunkList,
        i64 rowIndex,
        TReadLimit lowerBound,
        TReadLimit upperBound)
    {
        int result = 0;
        const auto& statistics = chunkList->Statistics();
        bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot;

        // Offset
        YT_VERIFY(!lowerBound.HasOffset());

        // Tablet Index
        if (lowerBound.HasTabletIndex()) {
            YT_VERIFY(isOrdered);
            result = std::max(
                result,
                std::min(
                    lowerBound.GetTabletIndex(),
                    static_cast<int>(chunkList->Children().size())));
        }

        // Row Index
        if (lowerBound.HasRowIndex() || upperBound.HasRowIndex()) {
            YT_VERIFY(isOrdered);
            if (lowerBound.HasRowIndex() &&
                lowerBound.GetTabletIndex() < chunkList->Children().size())
            {
                // We want user to operate with pairs (tabletIndex, rowIndex), however rowIndex in traverser is global.
                i64 tabletLowerBound = FetchPrevSum(chunkList, lowerBound.GetTabletIndex(), RowCountMember);
                i64 tabletUpperBound = FetchCurrentSum(chunkList, lowerBound.GetTabletIndex(), RowCountMember);

                // This is necessary in case when rowIndex is greater than rowCount of the current tablet.
                auto lowerBoundRowIndex = std::min(lowerBound.GetRowIndex() + tabletLowerBound, tabletUpperBound);
                lowerBound.SetRowIndex(lowerBoundRowIndex);
            }
            if (upperBound.HasRowIndex()) {
                // This is necessary in case when upperBoundTabletIndex > maxTabletIndex.
                auto upperTabletIndex = std::min(upperBound.GetTabletIndex(), static_cast<int>(chunkList->Children().size()));
                i64 tabletLowerBound = FetchPrevSum(chunkList, upperTabletIndex, RowCountMember);

                upperBound.SetRowIndex(upperBound.GetRowIndex() + tabletLowerBound);
            }
        }

        // Chunk Index
        if (lowerBound.HasChunkIndex()) {
            result = AdjustStartChildIndex(
                result,
                chunkList,
                ChunkCountMember,
                lowerBound.GetChunkIndex(),
                statistics.LogicalChunkCount);
        }

        // Key
        if (lowerBound.HasKey()) {
            auto it = std::upper_bound(
                chunkList->Children().begin(),
                chunkList->Children().end(),
                lowerBound.GetKey(),
                [] (const TOwningKey& key, const TChunkTree* chunkTree) {
                    // NB: It's OK here without key widening: even in case of key_column_count=2 and pivot_key=[0],
                    // widening to [0, Null] will have no effect as there are no real keys between [0] and [0, Null].
                    return key < chunkTree->AsChunkList()->GetPivotKey();
                });
            result = std::max(result, static_cast<int>(std::distance(chunkList->Children().begin(), it) - 1));
        }

        PushStack(TStackEntry(
            chunkList,
            result,
            rowIndex,
            std::nullopt,
            std::move(lowerBound),
            std::move(upperBound)));
    }

    void GetStartChildIndexDynamic(
        TChunkList* chunkList,
        i64 rowIndex,
        std::optional<i32> tabletIndex,
        TReadLimit lowerBound,
        TReadLimit upperBound)
    {
        int result = 0;
        bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet;
        const auto& statistics = chunkList->Statistics();

        // Row Index
        YT_VERIFY(!lowerBound.HasRowIndex() || isOrdered);

        // Tablet Index
        YT_VERIFY(!lowerBound.HasTabletIndex() || isOrdered);

        // Offset
        YT_VERIFY(!lowerBound.HasOffset());

        // Row Index
        if (isOrdered) {
            if (lowerBound.HasRowIndex()) {
                YT_VERIFY(statistics.Sealed);
                result = AdjustStartChildIndex(
                    result,
                    chunkList,
                    RowCountMember,
                    lowerBound.GetRowIndex(),
                    statistics.LogicalRowCount);
            }
        }

        // Chunk Index
        if (lowerBound.HasChunkIndex()) {
            result = AdjustStartChildIndex(
                result,
                chunkList,
                ChunkCountMember,
                lowerBound.GetChunkIndex(),
                chunkList->Statistics().LogicalChunkCount);
        }

        // NB: Tablet Index lower bound is checked above in tablet root.

        // NB: Key is not used here since tablet/subtablet chunk list is never sorted.

        PushStack(TStackEntry(
            chunkList,
            result,
            rowIndex,
            tabletIndex,
            lowerBound,
            upperBound));
    }

    static void GetInducedSubtreeLimits(
        const TStackEntry& stackEntry,
        const TReadLimit& childLowerBound,
        const TReadLimit& childUpperBound,
        TReadLimit* startLimit,
        TReadLimit* endLimit)
    {
        // Row index
        if (stackEntry.LowerBound.HasRowIndex()) {
            i64 newLowerBound = stackEntry.LowerBound.GetRowIndex() - childLowerBound.GetRowIndex();
            if (newLowerBound > 0) {
                startLimit->SetRowIndex(newLowerBound);
            }
        }
        if (stackEntry.UpperBound.HasRowIndex() &&
            stackEntry.UpperBound.GetRowIndex() < childUpperBound.GetRowIndex())
        {
            i64 newUpperBound = stackEntry.UpperBound.GetRowIndex() - childLowerBound.GetRowIndex();
            YT_ASSERT(newUpperBound > 0);
            endLimit->SetRowIndex(newUpperBound);
        }

        // NB: Tablet index is not needed here, because only chunks inside correct tablets
        // will be visited and they know their tabletIndex.

        // Chunk index
        if (stackEntry.LowerBound.HasChunkIndex()) {
            i64 newLowerBound = stackEntry.LowerBound.GetChunkIndex() - childLowerBound.GetChunkIndex();
            if (newLowerBound > 0) {
                startLimit->SetChunkIndex(newLowerBound);
            }
        }
        if (stackEntry.UpperBound.HasChunkIndex() &&
            stackEntry.UpperBound.GetChunkIndex() < childUpperBound.GetChunkIndex())
        {
            i64 newUpperBound = stackEntry.UpperBound.GetChunkIndex() - childLowerBound.GetChunkIndex();
            YT_VERIFY(newUpperBound > 0);
            endLimit->SetChunkIndex(newUpperBound);
        }

        // Offset
        if (stackEntry.LowerBound.HasOffset()) {
            i64 newLowerBound = stackEntry.LowerBound.GetOffset() - childLowerBound.GetOffset();
            if (newLowerBound > 0) {
                startLimit->SetOffset(newLowerBound);
            }
        }
        if (stackEntry.UpperBound.HasOffset() &&
            stackEntry.UpperBound.GetOffset() < childUpperBound.GetOffset())
        {
            i64 newUpperBound = stackEntry.UpperBound.GetOffset() - childLowerBound.GetOffset();
            YT_ASSERT(newUpperBound > 0);
            endLimit->SetOffset(newUpperBound);
        }

        // Key
        // NB: If any key widening was required, it was performed prior to this function call.
        if (stackEntry.LowerBound.HasKey() &&
            stackEntry.LowerBound.GetKey() > childLowerBound.GetKey())
        {
            startLimit->SetKey(stackEntry.LowerBound.GetKey());
        }
        if (stackEntry.UpperBound.HasKey() &&
            stackEntry.UpperBound.GetKey() < childUpperBound.GetKey())
        {
            endLimit->SetKey(stackEntry.UpperBound.GetKey());
        }
    }

    bool IsStackEmpty()
    {
        return Stack_.empty();
    }

    void PushStack(const TStackEntry& newEntry)
    {
        ++ChunkListCount_;
        Callbacks_->OnPush(newEntry.ChunkList);
        Stack_.push_back(newEntry);
    }

    TStackEntry& PeekStack()
    {
        return Stack_.back();
    }

    void PopStack()
    {
        auto& entry = Stack_.back();
        Callbacks_->OnPop(entry.ChunkList);
        Stack_.pop_back();
    }

    void Shutdown()
    {
        std::vector<TChunkTree*> nodes;
        for (const auto& entry : Stack_) {
            nodes.push_back(entry.ChunkList);
        }
        Callbacks_->OnShutdown(nodes);
        Stack_.clear();
    }

    const IChunkTraverserCallbacksPtr Callbacks_;
    const IChunkVisitorPtr Visitor_;
    const std::optional<int> KeyColumnCount_;

    NLogging::TLogger Logger;

    TInstant StartInstant_;
    TDuration CpuTime_;
    int ChunkCount_ = 0;
    int ChunkListCount_ = 0;

    std::vector<TStackEntry> Stack_;

public:
    TChunkTreeTraverser(
        IChunkTraverserCallbacksPtr callbacks,
        IChunkVisitorPtr visitor,
        std::optional<int> keyColumnCount)
        : Callbacks_(std::move(callbacks))
        , Visitor_(std::move(visitor))
        , KeyColumnCount_(keyColumnCount)
        , Logger(ChunkServerLogger)
    { }

    void Run(
        TChunkList* chunkList,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        Logger.AddTag("RootId: %v", chunkList->GetId());

        StartInstant_ = TInstant::Now();

        YT_LOG_DEBUG("Chunk tree traversal started (LowerBound: %v, UpperBound: %v",
            lowerBound,
            upperBound);

        GetStartChildIndex(chunkList, 0, std::nullopt, lowerBound, upperBound);

        // Do actual traversing in the proper queue.
        auto invoker = Callbacks_->GetInvoker();
        if (invoker) {
            invoker->Invoke(BIND(&TChunkTreeTraverser::DoTraverse, MakeStrong(this)));
        } else {
            DoTraverse();
        }
    }
};

void TraverseChunkTree(
    IChunkTraverserCallbacksPtr traverserCallbacks,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    std::optional<int> keyColumnCount)
{
    auto traverser = New<TChunkTreeTraverser>(
        std::move(traverserCallbacks),
        std::move(visitor),
        keyColumnCount);
    traverser->Run(root, lowerLimit, upperLimit);
}

////////////////////////////////////////////////////////////////////////////////

class TPreemptableChunkTraverserCallbacks
    : public IChunkTraverserCallbacks
{
public:
    TPreemptableChunkTraverserCallbacks(
        NCellMaster::TBootstrap* bootstrap,
        NCellMaster::EAutomatonThreadQueue threadQueue)
        : Bootstrap_(bootstrap)
        , UserName_(Bootstrap_
            ->GetSecurityManager()
            ->GetAuthenticatedUser()
            ->GetName())
        , ThreadQueue_(threadQueue)
    { }

    virtual IInvokerPtr GetInvoker() const override
    {
        return Bootstrap_
            ->GetHydraFacade()
            ->GetEpochAutomatonInvoker(ThreadQueue_);
    }

    virtual void OnPop(TChunkTree* node) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->EphemeralUnrefObject(node);
    }

    virtual void OnPush(TChunkTree* node) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->EphemeralRefObject(node);
    }

    virtual void OnShutdown(const std::vector<TChunkTree*>& nodes) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (const auto& node : nodes) {
            objectManager->EphemeralUnrefObject(node);
        }
    }

    virtual void OnTimeSpent(TDuration time) override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->FindUserByName(UserName_);
        securityManager->ChargeUser(user, {EUserWorkloadType::Read, 0, time});
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TString UserName_;
    const NCellMaster::EAutomatonThreadQueue ThreadQueue_;

};

IChunkTraverserCallbacksPtr CreatePreemptableChunkTraverserCallbacks(
    NCellMaster::TBootstrap* bootstrap,
    NCellMaster::EAutomatonThreadQueue threadQueue)
{
    return New<TPreemptableChunkTraverserCallbacks>(
        bootstrap,
        threadQueue);
}

////////////////////////////////////////////////////////////////////////////////

class TNonpreemptableChunkTraverserCallbacks
    : public IChunkTraverserCallbacks
{
public:
    virtual IInvokerPtr GetInvoker() const override
    {
        return nullptr;
    }

    virtual void OnPop(TChunkTree* /*node*/) override
    { }

    virtual void OnPush(TChunkTree* /*node*/) override
    { }

    virtual void OnShutdown(const std::vector<TChunkTree*>& /*nodes*/) override
    { }

    virtual void OnTimeSpent(TDuration /*time*/) override
    { }
};

IChunkTraverserCallbacksPtr GetNonpreemptableChunkTraverserCallbacks()
{
    return RefCountedSingleton<TNonpreemptableChunkTraverserCallbacks>();
}

////////////////////////////////////////////////////////////////////////////////

class TEnumeratingChunkVisitor
    : public IChunkVisitor
{
public:
    explicit TEnumeratingChunkVisitor(std::vector<TChunk*>* chunks)
        : Chunks_(chunks)
    { }

    virtual bool OnChunkView(TChunkView* chunkView) override
    {
        return false;
    }

    virtual bool OnDynamicStore(
        TDynamicStore* /*dynamicStore*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/) override
    {
        return true;
    }

    virtual bool OnChunk(
        TChunk* chunk,
        i64 /*rowIndex*/,
        std::optional<i32> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/,
        TTransactionId /*timestampTransactionId*/) override
    {
        Chunks_->push_back(chunk);
        return true;
    }

    virtual void OnFinish(const TError& error) override
    {
        YT_VERIFY(error.IsOK());
    }

private:
    std::vector<TChunk*>* const Chunks_;
};

void EnumerateChunksInChunkTree(
    TChunkList* root,
    std::vector<TChunk*>* chunks,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit)
{
    auto visitor = New<TEnumeratingChunkVisitor>(chunks);
    TraverseChunkTree(
        GetNonpreemptableChunkTraverserCallbacks(),
        visitor,
        root,
        lowerLimit,
        upperLimit);
}

std::vector<TChunk*> EnumerateChunksInChunkTree(
    TChunkList* root,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit)
{
    std::vector<TChunk*> chunks;
    EnumerateChunksInChunkTree(
        root,
        &chunks,
        lowerLimit,
        upperLimit);
    return chunks;
}

////////////////////////////////////////////////////////////////////////////////

void EnumerateStoresInChunkTree(
    TChunkList* root,
    std::vector<TChunkTree*>* chunks)
{
    class TVisitor
        : public IChunkVisitor
    {
    public:
        explicit TVisitor(std::vector<TChunkTree*>* stores)
            : Stores_(stores)
        { }

        virtual bool OnChunkView(TChunkView* chunkView) override
        {
            Stores_->push_back(chunkView);
            return true;
        }

        virtual bool OnDynamicStore(
            TDynamicStore* dynamicStore,
            const NChunkClient::TReadLimit& /*startLimit*/,
            const NChunkClient::TReadLimit& /*endLimit*/) override
        {
            Stores_->push_back(dynamicStore);
            return true;
        }

        virtual bool OnChunk(
            TChunk* chunk,
            i64 /*rowIndex*/,
            std::optional<i32> /*tabletIndex*/,
            const NChunkClient::TReadLimit& /*startLimit*/,
            const NChunkClient::TReadLimit& /*endLimit*/,
            TTransactionId /*timestampTransactionId*/) override
        {
            Stores_->push_back(chunk);
            return true;
        }

        virtual void OnFinish(const TError& error) override
        {
            YT_VERIFY(error.IsOK());
        }

    private:
        std::vector<TChunkTree*>* const Stores_;
    };

    auto visitor = New<TVisitor>(chunks);
    TraverseChunkTree(
        GetNonpreemptableChunkTraverserCallbacks(),
        visitor,
        root);
}

std::vector<TChunkTree*> EnumerateStoresInChunkTree(
    TChunkList* root)
{
    std::vector<TChunkTree*> stores;
    stores.reserve(root->Statistics().ChunkCount);
    EnumerateStoresInChunkTree(root, &stores);
    return stores;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
