#include "chunk_tree_traverser.h"
#include "chunk_manager.h"
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

static const int MaxChunksPerIteration = 1000;

static const auto RowCountMember = &TCumulativeStatisticsEntry::RowCount;
static const auto ChunkCountMember = &TCumulativeStatisticsEntry::ChunkCount;
static const auto DataSizeMember = &TCumulativeStatisticsEntry::DataSize;

////////////////////////////////////////////////////////////////////////////////

template <class TIterator, class TKey, class TIsLess, class TIsMissing>
TIterator UpperBoundWithMissingValues(
    TIterator start,
    TIterator end,
    const TKey& key,
    const TIsLess& isLess,
    const TIsMissing& isMissing)
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
        std::optional<i64> RowIndex;
        std::optional<int> TabletIndex;
        TLegacyReadLimit LowerBound;
        TLegacyReadLimit UpperBound;

        TStackEntry(
            TChunkList* chunkList,
            int childIndex,
            std::optional<i64> rowIndex,
            std::optional<int> tabletIndex,
            const TLegacyReadLimit& lowerBound,
            const TLegacyReadLimit& upperBound)
            : ChunkList(chunkList)
            , ChunkListVersion(chunkList->GetVersion())
            , ChildIndex(childIndex)
            , RowIndex(rowIndex)
            , TabletIndex(tabletIndex)
            , LowerBound(lowerBound)
            , UpperBound(upperBound)
        {
            YT_VERIFY(childIndex >= 0);
            YT_VERIFY(!rowIndex || *rowIndex >= 0);
            YT_VERIFY(!tabletIndex || *tabletIndex >= 0);
        }
    };

    void OnTimeSpent(TDuration elapsed)
    {
        CpuTime_ += elapsed;
        Context_->OnTimeSpent(elapsed);
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
        int chunkCountLimit = Context_->IsSynchronous()
            ? std::numeric_limits<int>::max()
            : ChunkCount_ + MaxChunksPerIteration;
        auto rescheduleAfterFuture = VoidFuture;
        while (ChunkCount_ < chunkCountLimit) {
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
                case EChunkListKind::JournalRoot: {
                    if (auto future = VisitEntryStatic(&entry)) {
                        rescheduleAfterFuture = std::move(future);
                    }
                    break;
                }

                case EChunkListKind::SortedDynamicRoot:
                case EChunkListKind::OrderedDynamicRoot:
                    VisitEntryDynamicRoot(&entry);
                    break;

                case EChunkListKind::SortedDynamicTablet:
                case EChunkListKind::SortedDynamicSubtablet:
                case EChunkListKind::OrderedDynamicTablet:
                    VisitEntryDynamic(&entry);
                    break;

                default:
                    YT_ABORT();
            }
        }

        // Schedule continuation.
        Context_->OnTimeSpent(timer.GetElapsedTime());
        rescheduleAfterFuture.Subscribe(
            BIND([=, this_ = MakeStrong(this)] (const TError& error) {
                if (error.IsOK()) {
                    DoTraverse();
                } else {
                    OnFinish(error);
                }
            }).Via(Context_->GetInvoker()));
    }

    TFuture<void> RequestUnsealedChunksStatistics(const TStackEntry& entry)
    {
        YT_VERIFY(EnforceBounds_);

        // Scan to the right of the current child extracing the maximum
        // segment of unsealed chunks.
        const auto* chunkList = entry.ChunkList;
        for (int index = entry.ChildIndex; index < static_cast<int>(chunkList->Children().size()); ++index) {
            auto* child = chunkList->Children()[index];
            if (!IsPhysicalChunkType(child->GetType())) {
                break;
            }
            auto* chunk = child->AsChunk();
            if (chunk->IsSealed()) {
                break;
            }
            auto chunkId = chunk->GetId();
            if (UnsealedChunkIdToStatisticsFuture_.contains(chunkId)) {
                break;
            }
            YT_VERIFY(UnsealedChunkIdToStatisticsFuture_.emplace(
                chunkId,
                Context_->GetUnsealedChunkStatistics(chunk)).second);
        }

        auto it = UnsealedChunkIdToStatisticsFuture_.find(chunkList->Children()[entry.ChildIndex]->GetId());
        return it == UnsealedChunkIdToStatisticsFuture_.end() ? TFuture<void>() : it->second.AsVoid();
    }

    void InferJournalChunkRowRange(const TStackEntry& entry)
    {
        YT_VERIFY(EnforceBounds_);

        const auto* chunkList = entry.ChunkList;
        const auto& cumulativeStatistics = chunkList->CumulativeStatistics();
        const auto* child = chunkList->Children()[entry.ChildIndex];
        YT_VERIFY(IsJournalChunkType(child->GetType()));
        const auto* chunk = child->AsChunk();

        i64 startRowIndex;
        i64 endRowIndex;

        if (chunk->IsSealed()) {
            startRowIndex = cumulativeStatistics.GetPreviousSum(entry.ChildIndex).RowCount;
            endRowIndex = cumulativeStatistics.GetCurrentSum(entry.ChildIndex).RowCount;
        } else {
            // Compute start row index.
            if (entry.ChildIndex > 0) {
                const auto* prevChild = chunkList->Children()[entry.ChildIndex - 1];
                YT_VERIFY(IsJournalChunkType(prevChild->GetType()));
                auto* prevChunk = prevChild->AsChunk();
                if (prevChunk->IsSealed()) {
                    startRowIndex = cumulativeStatistics.GetPreviousSum(entry.ChildIndex).RowCount;
                } else {
                    startRowIndex = GetJournalChunkRowRange(prevChunk).second;
                }
            } else {
                startRowIndex = 0;
            }

            // Compute end row index.
            {
                auto unsealedChunkStatistics = GetUnsealedChunkStatistics(chunk);
                if (chunk->GetOverlayed()) {
                    endRowIndex = unsealedChunkStatistics.FirstOverlayedRowIndex
                        ? *unsealedChunkStatistics.FirstOverlayedRowIndex + unsealedChunkStatistics.RowCount
                        : -1;
                } else {
                    endRowIndex = startRowIndex + unsealedChunkStatistics.RowCount;
                }
            }

            // Final adjustments.
            endRowIndex = std::max(startRowIndex, endRowIndex);
        }

        YT_VERIFY(JournalChunkIdToRowRange_.emplace(chunk->GetId(), std::pair{startRowIndex, endRowIndex}).second);

        YT_LOG_DEBUG("Journal chunk row range inferred (ChunkId: %v, RowIndexes: %v-%v)",
            chunk->GetId(),
            startRowIndex,
            endRowIndex - 1);
    }

    std::pair<i64, i64> GetJournalChunkRowRange(const TChunk* chunk)
    {
        YT_VERIFY(EnforceBounds_);

        return GetOrCrash(JournalChunkIdToRowRange_, chunk->GetId());
    }

    IChunkTraverserContext::TUnsealedChunkStatistics GetUnsealedChunkStatistics(const TChunk* chunk)
    {
        YT_VERIFY(EnforceBounds_);

        auto unsealedChunkStatisticsFuture = GetOrCrash(UnsealedChunkIdToStatisticsFuture_, chunk->GetId());
        YT_VERIFY(unsealedChunkStatisticsFuture.IsSet());
        return unsealedChunkStatisticsFuture
            .Get()
            .ValueOrThrow();
    }

    std::tuple<TCumulativeStatisticsEntry, TCumulativeStatisticsEntry> GetCumulativeStatisticsRange(const TStackEntry& entry)
    {
        YT_VERIFY(EnforceBounds_);

        const auto* chunkList = entry.ChunkList;
        const auto& cumulativeStatistics = chunkList->CumulativeStatistics();
        const auto* child = chunkList->Children()[entry.ChildIndex];
        auto childType = child->GetType();

        auto lowerStatistics = cumulativeStatistics.GetPreviousSum(entry.ChildIndex);
        auto upperStatistics = cumulativeStatistics.GetCurrentSum(entry.ChildIndex);

        if (IsJournalChunkType(childType)) {
            auto [startRowIndex, endRowIndex] = GetJournalChunkRowRange(child->AsChunk());
            lowerStatistics.RowCount = startRowIndex;
            upperStatistics.RowCount = endRowIndex;
        }

        return {lowerStatistics, upperStatistics};
    }

    TFuture<void> VisitEntryStatic(TStackEntry* entry)
    {
        auto* chunkList = entry->ChunkList;
        auto* child = chunkList->Children()[entry->ChildIndex];
        auto childType = child->GetType();

        TLegacyReadLimit subtreeStartLimit;
        TLegacyReadLimit subtreeEndLimit;
        std::optional<i64> rowIndex;

        if (EnforceBounds_) {
            if (auto future = RequestUnsealedChunksStatistics(*entry); future && !future.IsSet()) {
                return future;
            }

            if (IsJournalChunkType(childType)) {
                InferJournalChunkRowRange(*entry);
            }

            auto [childLowerStatistics, childUpperStatistics] = GetCumulativeStatisticsRange(*entry);

            // Tablet index.
            YT_VERIFY(!entry->LowerBound.HasTabletIndex() && !entry->UpperBound.HasTabletIndex());

            TLegacyReadLimit childLowerBound;
            TLegacyReadLimit childUpperBound;

            // Row index.
            {
                auto childLimit = childLowerStatistics.RowCount;
                rowIndex = *entry->RowIndex + childLimit;
                if (entry->UpperBound.HasRowIndex()) {
                    if (entry->UpperBound.GetRowIndex() <= childLimit) {
                        PopStack();
                        return {};
                    }
                    childLowerBound.SetRowIndex(childLimit);
                    childUpperBound.SetRowIndex(childUpperStatistics.RowCount);
                } else if (entry->LowerBound.HasRowIndex()) {
                    childLowerBound.SetRowIndex(childLimit);
                }
            }

            // Chunk index.
            {
                if (entry->UpperBound.HasChunkIndex()) {
                    if (entry->UpperBound.GetChunkIndex() <= childLowerStatistics.ChunkCount) {
                        PopStack();
                        return {};
                    }
                    childLowerBound.SetChunkIndex(childLowerStatistics.ChunkCount);
                    childUpperBound.SetChunkIndex(childUpperStatistics.ChunkCount);
                } else if (entry->LowerBound.HasChunkIndex()) {
                    childLowerBound.SetChunkIndex(childLowerStatistics.ChunkCount);
                }
            }

            // Offset.
            {
                if (entry->UpperBound.HasOffset()) {
                    if (entry->UpperBound.GetOffset() <= childLowerStatistics.DataSize) {
                        PopStack();
                        return {};
                    }
                    childLowerBound.SetOffset(childLowerStatistics.DataSize);
                    childUpperBound.SetOffset(childUpperStatistics.DataSize);
                } else if (entry->LowerBound.HasOffset()) {
                    childLowerBound.SetOffset(childLowerStatistics.DataSize);
                }
            }

            // Key.
            {
                if (entry->UpperBound.HasLegacyKey()) {
                    YT_LOG_ALERT_UNLESS(KeyColumnCount_, "Chunk tree traverser entry has key bounds, "
                        "but `key_column_count` parameter is not set");

                    childLowerBound.SetLegacyKey(GetMinKeyOrThrow(child, KeyColumnCount_));
                    if (entry->UpperBound.GetLegacyKey() <= childLowerBound.GetLegacyKey()) {
                        PopStack();
                        return {};
                    }
                    childUpperBound.SetLegacyKey(GetUpperBoundKeyOrThrow(child, KeyColumnCount_));
                } else if (entry->LowerBound.HasLegacyKey()) {
                    YT_LOG_ALERT_UNLESS(KeyColumnCount_, "Chunk tree traverser entry has key bounds, "
                        "but `key_column_count` parameter is not set");

                    childLowerBound.SetLegacyKey(GetMinKeyOrThrow(child, KeyColumnCount_));
                }
            }

            GetInducedSubtreeRange(
                *entry,
                childLowerBound,
                childUpperBound,
                &subtreeStartLimit,
                &subtreeEndLimit);
        }

        ++entry->ChildIndex;

        if (childType == EObjectType::ChunkList) {
            auto* childChunkList = child->AsChunkList();
            PushFirstChild(
                childChunkList,
                rowIndex,
                {} /*tabletIndex*/,
                subtreeStartLimit,
                subtreeEndLimit);
        } else if (IsPhysicalChunkType(childType)) {
            auto* childChunk = child->AsChunk();
            if (!Visitor_->OnChunk(
                childChunk,
                rowIndex,
                {} /*tabletIndex*/,
                subtreeStartLimit,
                subtreeEndLimit,
                {} /*timestampTransactionId*/))
            {
                Shutdown();
                return {};
            }
            ++ChunkCount_;
        } else {
            YT_ABORT();
        }

        return {};
    }

    void VisitEntryDynamicRoot(TStackEntry* entry)
    {
        auto* chunkList = entry->ChunkList;
        const auto& cumulativeStatistics = chunkList->CumulativeStatistics();
        auto* child = chunkList->Children()[entry->ChildIndex];
        bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot;

        TLegacyReadLimit subtreeStartLimit;
        TLegacyReadLimit subtreeEndLimit;
        std::optional<int> tabletIndex;

        if (EnforceBounds_) {
            // Row index.
            YT_VERIFY((!entry->LowerBound.HasRowIndex() && !entry->UpperBound.HasRowIndex()) || isOrdered);

            // Offset.
            YT_VERIFY(!entry->LowerBound.HasOffset() && !entry->UpperBound.HasOffset());

            // Tablet index.
            YT_VERIFY((!entry->LowerBound.HasTabletIndex() && !entry->UpperBound.HasTabletIndex()) || isOrdered);

            TLegacyReadLimit childLowerBound;
            TLegacyReadLimit childUpperBound;

            auto pivotKey = chunkList->Children()[entry->ChildIndex]->AsChunkList()->GetPivotKey();
            auto nextPivotKey = entry->ChildIndex + 1 < chunkList->Children().size()
                ? chunkList->Children()[entry->ChildIndex + 1]->AsChunkList()->GetPivotKey()
                : MaxKey();

            // Tablet index.
            {
                if (entry->UpperBound.HasTabletIndex() && entry->UpperBound.GetTabletIndex() < entry->ChildIndex) {
                    PopStack();
                    return;
                }
                if (isOrdered) {
                    tabletIndex = entry->ChildIndex;
                }
            }

            // Chunk index.
            {
                i64 childLimit = cumulativeStatistics.GetPreviousSum(entry->ChildIndex).ChunkCount;
                if (entry->UpperBound.HasChunkIndex()) {
                    if (entry->UpperBound.GetChunkIndex() <= childLimit) {
                        PopStack();
                        return;
                    }
                    childLowerBound.SetChunkIndex(childLimit);
                    childUpperBound.SetChunkIndex(cumulativeStatistics.GetCurrentSum(entry->ChildIndex).ChunkCount);
                } else if (entry->LowerBound.HasChunkIndex()) {
                    childLowerBound.SetChunkIndex(childLimit);
                }
            }

            // Key.
            {
                if (entry->LowerBound.HasLegacyKey() || entry->UpperBound.HasLegacyKey()) {
                    YT_LOG_ALERT_UNLESS(KeyColumnCount_, "Chunk tree traverser entry has key bounds, "
                        "but `key_column_count` parameter is not set");
                }

                if (entry->UpperBound.HasLegacyKey()) {
                    // NB: It's OK here without key widening, however there may be some inefficiency,
                    // e.g. with 2 key columns, pivotKey = [0] and upperBound = [0, #Min] we could have returned.
                    if (entry->UpperBound.GetLegacyKey() <= pivotKey) {
                        PopStack();
                        return;
                    }
                }

                childLowerBound.SetLegacyKey(pivotKey);
                childUpperBound.SetLegacyKey(nextPivotKey);
            }

            GetInducedSubtreeRange(
                *entry,
                childLowerBound,
                childUpperBound,
                &subtreeStartLimit,
                &subtreeEndLimit);

            // NB: Chunks may cross tablet boundaries.
            if (chunkList->GetKind() == EChunkListKind::SortedDynamicRoot) {
                if (!subtreeStartLimit.HasLegacyKey() || subtreeStartLimit.GetLegacyKey() < pivotKey) {
                    subtreeStartLimit.SetLegacyKey(pivotKey);
                }
                if (!subtreeEndLimit.HasLegacyKey() || subtreeEndLimit.GetLegacyKey() > nextPivotKey) {
                    subtreeEndLimit.SetLegacyKey(nextPivotKey);
                }
            }

            // NB: Row index is tablet-wise for ordered tables.
            if (chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot) {
                if (entry->LowerBound.HasTabletIndex() &&
                    entry->LowerBound.GetTabletIndex() == *tabletIndex &&
                    entry->LowerBound.HasRowIndex())
                {
                    subtreeStartLimit.SetRowIndex(entry->LowerBound.GetRowIndex());
                }
                if (entry->UpperBound.HasTabletIndex() &&
                    entry->UpperBound.GetTabletIndex() == *tabletIndex &&
                    entry->UpperBound.HasRowIndex())
                {
                    subtreeEndLimit.SetRowIndex(entry->UpperBound.GetRowIndex());
                }
            }

        }

        ++entry->ChildIndex;

        auto* childChunkList = child->AsChunkList();
        PushFirstChild(
            childChunkList,
            {} /*rowIndex*/,
            tabletIndex,
            subtreeStartLimit,
            subtreeEndLimit);
    }

    void VisitEntryDynamic(TStackEntry* entry)
    {
        auto* chunkList = entry->ChunkList;
        auto* child = chunkList->Children()[entry->ChildIndex];
        const auto& statistics = chunkList->Statistics();
        const auto& cumulativeStatistics = chunkList->CumulativeStatistics();

        bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet;

        // Row index.
        YT_VERIFY((!entry->LowerBound.HasRowIndex() && !entry->UpperBound.HasRowIndex()) || isOrdered);

        // Offset.
        YT_VERIFY(!entry->LowerBound.HasOffset() && !entry->UpperBound.HasOffset());

        // Tablet index.
        YT_VERIFY((!entry->LowerBound.HasTabletIndex() && !entry->UpperBound.HasTabletIndex()) || isOrdered);

        auto tabletIndex = entry->TabletIndex;

        TLegacyReadLimit subtreeStartLimit;
        TLegacyReadLimit subtreeEndLimit;
        std::optional<i64> rowIndex;

        if (EnforceBounds_) {
            TLegacyReadLimit childLowerBound;
            TLegacyReadLimit childUpperBound;

            // Row index.
            if (isOrdered) {
                i64 childLimit = cumulativeStatistics.GetPreviousSum(entry->ChildIndex).RowCount;
                rowIndex = childLimit;
                if (entry->UpperBound.HasRowIndex()) {
                    if (entry->UpperBound.GetRowIndex() <= childLimit) {
                        PopStack();
                        return;
                    }
                    YT_VERIFY(statistics.Sealed);
                    childLowerBound.SetRowIndex(childLimit);

                    // NB: Dynamic stores at the end of the chunk list may be arbitrarily large
                    // but their size is not accounted in cumulative statistics.
                    if (entry->ChunkList->Children()[entry->ChildIndex]->GetType() == EObjectType::OrderedDynamicTabletStore) {
                        childUpperBound.SetRowIndex(std::numeric_limits<i64>::max());
                    } else {
                        childUpperBound.SetRowIndex(cumulativeStatistics.GetCurrentSum(entry->ChildIndex).RowCount);
                    }
                } else if (entry->LowerBound.HasRowIndex()) {
                    childLowerBound.SetRowIndex(childLimit);
                }
            }

            // Chunk index.
            {
                i64 childLimit = cumulativeStatistics.GetPreviousSum(entry->ChildIndex).ChunkCount;

                if (entry->UpperBound.HasChunkIndex()) {
                    if (entry->UpperBound.GetChunkIndex() <= childLimit) {
                        PopStack();
                        return;
                    }
                    childLowerBound.SetChunkIndex(childLimit);
                    childUpperBound.SetChunkIndex(cumulativeStatistics.GetCurrentSum(entry->ChildIndex).ChunkCount);
                } else if (entry->LowerBound.HasChunkIndex()) {
                    childLowerBound.SetChunkIndex(childLimit);
                }
            }

            // Tablet index.
            {
                if (entry->LowerBound.HasTabletIndex() && entry->LowerBound.GetTabletIndex() > *tabletIndex) {
                    ++entry->ChildIndex;
                    return;
                }
                if (entry->UpperBound.HasTabletIndex() && entry->UpperBound.GetTabletIndex() < *tabletIndex) {
                    PopStack();
                    return;
                }
            }

            // Key.
            {
                if (entry->UpperBound.HasLegacyKey() || entry->LowerBound.HasLegacyKey()) {
                    childLowerBound.SetLegacyKey(GetMinKeyOrThrow(child, KeyColumnCount_));
                    childUpperBound.SetLegacyKey(GetUpperBoundKeyOrThrow(child, KeyColumnCount_));

                    if (entry->UpperBound.HasLegacyKey() && entry->UpperBound.GetLegacyKey() <= childLowerBound.GetLegacyKey()) {
                        ++entry->ChildIndex;
                        return;
                    }

                    if (entry->LowerBound.HasLegacyKey() && entry->LowerBound.GetLegacyKey() > childUpperBound.GetLegacyKey()) {
                        ++entry->ChildIndex;
                        return;
                    }
                }
            }

            GetInducedSubtreeRange(
                *entry,
                childLowerBound,
                childUpperBound,
                &subtreeStartLimit,
                &subtreeEndLimit);
        }

        ++entry->ChildIndex;

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
                        return;
                    }

                    subtreeStartLimit = chunkView->GetAdjustedLowerReadLimit(subtreeStartLimit);
                    subtreeEndLimit = chunkView->GetAdjustedUpperReadLimit(subtreeEndLimit);
                    timestampTransactionId = chunkView->GetTransactionId();

                    childChunk = chunkView->GetUnderlyingChunk();
                } else {
                    childChunk = child->AsChunk();
                }

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
                break;
            }

            case EObjectType::SortedDynamicTabletStore:
            case EObjectType::OrderedDynamicTabletStore: {
                auto* dynamicStore = child->AsDynamicStore();
                if (!Visitor_->OnDynamicStore(dynamicStore, tabletIndex, subtreeStartLimit, subtreeEndLimit)) {
                    Shutdown();
                    return;
                }
                break;
            }

            case EObjectType::ChunkList: {
                auto* childChunkList = child->AsChunkList();
                YT_VERIFY(childChunkList->GetKind() == EChunkListKind::SortedDynamicSubtablet);
                PushFirstChild(childChunkList, 0, tabletIndex, subtreeStartLimit, subtreeEndLimit);
                break;
            }

            default:
                Y_UNREACHABLE();
        }
    }

    void PushFirstChild(
        TChunkList* chunkList,
        std::optional<i64> rowIndex,
        std::optional<int> tabletIndex,
        const TLegacyReadLimit& lowerBound,
        const TLegacyReadLimit& upperBound)
    {
        if (chunkList->Children().empty()) {
            return;
        }

        switch (chunkList->GetKind()) {
            case EChunkListKind::Static:
            case EChunkListKind::JournalRoot:
                PushFirstChildStatic(chunkList, rowIndex, lowerBound, upperBound);
                break;

            case EChunkListKind::SortedDynamicRoot:
            case EChunkListKind::OrderedDynamicRoot:
                PushFirstChildDynamicRoot(chunkList, rowIndex, lowerBound, upperBound);
                break;

            case EChunkListKind::SortedDynamicTablet:
            case EChunkListKind::SortedDynamicSubtablet:
            case EChunkListKind::OrderedDynamicTablet:
                PushFirstChildDynamicTablet(chunkList, rowIndex, tabletIndex, lowerBound, upperBound);
                break;

            default:
                YT_ABORT();
        }
    }

    static bool IsSealedChild(const TChunkTree* child)
    {
        // NB: nulls are possible in ordered tablets.
        if (!child) {
            return true;
        }
        if (!IsPhysicalChunkType(child->GetType())) {
            return true;
        }
        const auto* chunk = child->AsChunk();
        return chunk->IsSealed();
    }

    static int AdjustStartChildIndex(
        int currentIndex,
        const TChunkList* chunkList,
        i64 TCumulativeStatisticsEntry::* member,
        i64 limit,
        i64 total)
    {
        const auto& children = chunkList->Children();
        if (limit >= total) {
            return static_cast<int>(children.size());
        }
        const auto& cumulativeStatistics = chunkList->CumulativeStatistics();
        int adjustedIndex = std::max(currentIndex, cumulativeStatistics.UpperBound(limit, member));
        while (adjustedIndex > 0 && !IsSealedChild(children[adjustedIndex - 1]) ) {
            --adjustedIndex;
        }
        return adjustedIndex;
    }

    void PushFirstChildStatic(
        TChunkList* chunkList,
        std::optional<i64> rowIndex,
        const TLegacyReadLimit& lowerBound,
        const TLegacyReadLimit& upperBound)
    {
        int chunkIndex = 0;

        if (EnforceBounds_) {
            const auto& statistics = chunkList->Statistics();
            YT_VERIFY(!lowerBound.HasTabletIndex());

            // Row index.
            if (lowerBound.HasRowIndex()) {
                chunkIndex = AdjustStartChildIndex(
                    chunkIndex,
                    chunkList,
                    RowCountMember,
                    lowerBound.GetRowIndex(),
                    statistics.Sealed ? statistics.LogicalRowCount : Max<i64>());
            }

            // Chunk index.
            if (lowerBound.HasChunkIndex()) {
                chunkIndex = AdjustStartChildIndex(
                    chunkIndex,
                    chunkList,
                    ChunkCountMember,
                    lowerBound.GetChunkIndex(),
                    statistics.LogicalChunkCount);
            }

            // Offset.
            if (lowerBound.HasOffset()) {
                chunkIndex = AdjustStartChildIndex(
                    chunkIndex,
                    chunkList,
                    DataSizeMember,
                    lowerBound.GetOffset(),
                    statistics.UncompressedDataSize);
            }

            // Key.
            if (lowerBound.HasLegacyKey()) {
                typedef std::vector<TChunkTree*>::const_iterator TChildrenIterator;
                std::reverse_iterator<TChildrenIterator> rbegin(chunkList->Children().end());
                std::reverse_iterator<TChildrenIterator> rend(chunkList->Children().begin());

                auto it = UpperBoundWithMissingValues(
                    rbegin,
                    rend,
                    lowerBound.GetLegacyKey(),
                    // isLess
                    [keyColumnCount = KeyColumnCount_] (const TLegacyOwningKey& key, const TChunkTree* chunkTree) {
                        return key > GetUpperBoundKeyOrThrow(chunkTree, keyColumnCount);
                    },
                    // isMissing
                    [] (const TChunkTree* chunkTree) {
                        return IsEmpty(chunkTree);
                    });

                chunkIndex = std::max(chunkIndex, static_cast<int>(rend - it));
            }
        }

        PushStack(TStackEntry(
            chunkList,
            chunkIndex,
            rowIndex,
            std::nullopt,
            lowerBound,
            upperBound));
    }

    void PushFirstChildDynamicRoot(
        TChunkList* chunkList,
        std::optional<i64> rowIndex,
        TLegacyReadLimit lowerBound,
        TLegacyReadLimit upperBound)
    {
        int chunkIndex = 0;

        if (EnforceBounds_) {
            const auto& statistics = chunkList->Statistics();
            bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot;

            // Offset.
            YT_VERIFY(!lowerBound.HasOffset());

            // Tablet index.
            if (lowerBound.HasTabletIndex()) {
                YT_VERIFY(isOrdered);
                chunkIndex = std::max(
                    chunkIndex,
                    std::min(
                        lowerBound.GetTabletIndex(),
                        static_cast<int>(chunkList->Children().size())));
            }

            // Row index.
            if (lowerBound.HasRowIndex() || upperBound.HasRowIndex()) {
                YT_VERIFY(isOrdered);
                // Row indices remain tablet-wise, nothing to change here.
            }

            // Chunk index.
            if (lowerBound.HasChunkIndex()) {
                chunkIndex = AdjustStartChildIndex(
                    chunkIndex,
                    chunkList,
                    ChunkCountMember,
                    lowerBound.GetChunkIndex(),
                    statistics.LogicalChunkCount);
            }

            // Key.
            if (lowerBound.HasLegacyKey()) {
                auto it = std::upper_bound(
                    chunkList->Children().begin(),
                    chunkList->Children().end(),
                    lowerBound.GetLegacyKey(),
                    [] (const TLegacyOwningKey& key, const TChunkTree* chunkTree) {
                        // NB: It's OK here without key widening: even in case of key_column_count=2 and pivot_key=[0],
                        // widening to [0, Null] will have no effect as there are no real keys between [0] and [0, Null].
                        return key < chunkTree->AsChunkList()->GetPivotKey();
                    });
                chunkIndex = std::max(chunkIndex, static_cast<int>(std::distance(chunkList->Children().begin(), it) - 1));
            }
        }

        PushStack(TStackEntry(
            chunkList,
            chunkIndex,
            rowIndex,
            std::nullopt,
            std::move(lowerBound),
            std::move(upperBound)));
    }

    void PushFirstChildDynamicTablet(
        TChunkList* chunkList,
        std::optional<i64> rowIndex,
        std::optional<int> tabletIndex,
        const TLegacyReadLimit& lowerBound,
        const TLegacyReadLimit& upperBound)
    {
        int chunkIndex = 0;

        if (EnforceBounds_) {
            bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet;
            const auto& statistics = chunkList->Statistics();

            // Row index.
            YT_VERIFY(!lowerBound.HasRowIndex() || isOrdered);

            // Tablet index.
            YT_VERIFY(!lowerBound.HasTabletIndex() || isOrdered);

            // Offset.
            YT_VERIFY(!lowerBound.HasOffset());

            // Row index.
            if (isOrdered) {
                if (lowerBound.HasRowIndex()) {
                    YT_VERIFY(statistics.Sealed);
                    chunkIndex = AdjustStartChildIndex(
                        chunkIndex,
                        chunkList,
                        RowCountMember,
                        lowerBound.GetRowIndex(),
                        statistics.LogicalRowCount);

                    while (chunkIndex > 0 &&
                        chunkList->Children()[chunkIndex - 1]->GetType() ==
                            EObjectType::OrderedDynamicTabletStore)
                    {
                        --chunkIndex;
                    }
                }
            }

            // Chunk index.
            if (lowerBound.HasChunkIndex()) {
                chunkIndex = AdjustStartChildIndex(
                    chunkIndex,
                    chunkList,
                    ChunkCountMember,
                    lowerBound.GetChunkIndex(),
                    chunkList->Statistics().LogicalChunkCount);
            }

            // NB: Tablet index lower bound is checked above in tablet root.

            // NB: Key is not used here since tablet/subtablet chunk list is never sorted.
        }

        PushStack(TStackEntry(
            chunkList,
            chunkIndex,
            rowIndex,
            tabletIndex,
            lowerBound,
            upperBound));
    }

    i64 GetFirstOverlayedRowIndex(const TChunk* chunk)
    {
        return chunk->IsSealed()
            ? chunk->MiscExt().first_overlayed_row_index()
            : *GetUnsealedChunkStatistics(chunk).FirstOverlayedRowIndex;
    }

    void GetInducedSubtreeRange(
        const TStackEntry& entry,
        const TLegacyReadLimit& childLowerBound,
        const TLegacyReadLimit& childUpperBound,
        TLegacyReadLimit* startLimit,
        TLegacyReadLimit* endLimit)
    {
        YT_VERIFY(EnforceBounds_);

        const auto* child = entry.ChunkList->Children()[entry.ChildIndex];

        // Row index.

        // Ordered dynamic root is skipped since row index inside tablet is tablet-wise.
        // Ordered dynamic stores are skipped since they should have absolute row index
        // (0 is the beginning of the tablet) rather than relative (0 is the beginning of the store).
        if (entry.ChunkList->GetKind() != EChunkListKind::OrderedDynamicRoot) {
            if (entry.LowerBound.HasRowIndex()) {
                i64 newLowerBound = entry.LowerBound.GetRowIndex();
                if (child->GetType() != EObjectType::OrderedDynamicTabletStore) {
                    newLowerBound -= childLowerBound.GetRowIndex();
                }
                if (newLowerBound > 0) {
                    startLimit->SetRowIndex(newLowerBound);
                }
            }
            if (entry.UpperBound.HasRowIndex() &&
                entry.UpperBound.GetRowIndex() < childUpperBound.GetRowIndex())
            {
                i64 newUpperBound = entry.UpperBound.GetRowIndex();
                if (child->GetType() != EObjectType::OrderedDynamicTabletStore) {
                    newUpperBound -= childLowerBound.GetRowIndex();
                }
                YT_ASSERT(newUpperBound > 0);
                endLimit->SetRowIndex(newUpperBound);
            }
        }

        // Adjust for journal chunks.
        if (IsJournalChunkType(child->GetType())) {
            const auto* chunk = child->AsChunk();
            auto [startRowIndex, endRowIndex] = GetJournalChunkRowRange(chunk);

            if (!startLimit->HasRowIndex()) {
                startLimit->SetRowIndex(0);
            }
            if (!endLimit->HasRowIndex()) {
                endLimit->SetRowIndex(endRowIndex - startRowIndex);
            }

            auto logicalStartRowIndex = startLimit->GetRowIndex();
            auto logicalEndRowIndex = endLimit->GetRowIndex();

            if (chunk->GetOverlayed() && startLimit->GetRowIndex() < endLimit->GetRowIndex()) {
                auto firstOverlayedRowIndex = GetFirstOverlayedRowIndex(chunk);
                if (startRowIndex < firstOverlayedRowIndex) {
                    THROW_ERROR_EXCEPTION("Row gap detected in overlayed chunk %v",
                        chunk->GetId())
                        << TErrorAttribute("start_row_index", startRowIndex)
                        << TErrorAttribute("first_overlayed_row_index", firstOverlayedRowIndex);
                }
                auto rowIndexDelta =
                    (startRowIndex - firstOverlayedRowIndex) + // rows overlayed with the previous chunk
                    1;                                         // header row
                startLimit->SetRowIndex(rowIndexDelta + startLimit->GetRowIndex());
                endLimit->SetRowIndex(rowIndexDelta + endLimit->GetRowIndex());
            }

            auto physicalStartRowIndex = startLimit->GetRowIndex();
            auto physicalEndRowIndex = endLimit->GetRowIndex();

            YT_LOG_DEBUG("Journal chunk fetched (ChunkId: %v, Overlayed: %v, LogicalRowIndexes: %v-%v, PhysicalRowIndexes: %v-%v)",
                chunk->GetId(),
                chunk->GetOverlayed(),
                logicalStartRowIndex,
                logicalEndRowIndex - 1,
                physicalStartRowIndex,
                physicalEndRowIndex - 1);
        }

        // NB: Tablet index is not needed here, because only chunks inside correct tablets
        // will be visited and they know their tabletIndex.

        // Chunk index.
        if (entry.LowerBound.HasChunkIndex()) {
            i64 newLowerBound = entry.LowerBound.GetChunkIndex() - childLowerBound.GetChunkIndex();
            if (newLowerBound > 0) {
                startLimit->SetChunkIndex(newLowerBound);
            }
        }
        if (entry.UpperBound.HasChunkIndex() &&
            entry.UpperBound.GetChunkIndex() < childUpperBound.GetChunkIndex())
        {
            i64 newUpperBound = entry.UpperBound.GetChunkIndex() - childLowerBound.GetChunkIndex();
            YT_VERIFY(newUpperBound > 0);
            endLimit->SetChunkIndex(newUpperBound);
        }

        // Offset.
        if (entry.LowerBound.HasOffset()) {
            i64 newLowerBound = entry.LowerBound.GetOffset() - childLowerBound.GetOffset();
            if (newLowerBound > 0) {
                startLimit->SetOffset(newLowerBound);
            }
        }
        if (entry.UpperBound.HasOffset() &&
            entry.UpperBound.GetOffset() < childUpperBound.GetOffset())
        {
            i64 newUpperBound = entry.UpperBound.GetOffset() - childLowerBound.GetOffset();
            YT_VERIFY(newUpperBound > 0);
            endLimit->SetOffset(newUpperBound);
        }

        // Key.
        // NB: If any key widening was required, it was performed prior to this function call.
        if (entry.LowerBound.HasLegacyKey() &&
            entry.LowerBound.GetLegacyKey() > childLowerBound.GetLegacyKey())
        {
            startLimit->SetLegacyKey(entry.LowerBound.GetLegacyKey());
        }
        if (entry.UpperBound.HasLegacyKey() &&
            entry.UpperBound.GetLegacyKey() < childUpperBound.GetLegacyKey())
        {
            endLimit->SetLegacyKey(entry.UpperBound.GetLegacyKey());
        }
    }

    bool IsStackEmpty()
    {
        return Stack_.empty();
    }

    void PushStack(const TStackEntry& newEntry)
    {
        ++ChunkListCount_;
        Context_->OnPush(newEntry.ChunkList);
        Stack_.push_back(newEntry);
    }

    TStackEntry& PeekStack()
    {
        return Stack_.back();
    }

    void PopStack()
    {
        auto& entry = Stack_.back();
        Context_->OnPop(entry.ChunkList);
        Stack_.pop_back();
    }

    void Shutdown()
    {
        std::vector<TChunkTree*> nodes;
        for (const auto& entry : Stack_) {
            nodes.push_back(entry.ChunkList);
        }
        Context_->OnShutdown(nodes);
        Stack_.clear();
    }

    const IChunkTraverserContextPtr Context_;
    const IChunkVisitorPtr Visitor_;
    const bool EnforceBounds_;
    const std::optional<int> KeyColumnCount_;

    const NLogging::TLogger Logger;

    const TInstant StartInstant_ = TInstant::Now();

    TDuration CpuTime_;
    int ChunkCount_ = 0;
    int ChunkListCount_ = 0;

    std::vector<TStackEntry> Stack_;

    THashMap<TChunkId, TFuture<IChunkTraverserContext::TUnsealedChunkStatistics>> UnsealedChunkIdToStatisticsFuture_;
    THashMap<TChunkId, std::pair<i64, i64>> JournalChunkIdToRowRange_;

public:
    TChunkTreeTraverser(
        IChunkTraverserContextPtr context,
        IChunkVisitorPtr visitor,
        TChunkList* chunkList,
        bool enforceBounds,
        std::optional<int> keyColumnCount,
        TLegacyReadLimit lowerBound,
        TLegacyReadLimit upperBound)
        : Context_(std::move(context))
        , Visitor_(std::move(visitor))
        , EnforceBounds_(enforceBounds)
        , KeyColumnCount_(keyColumnCount)
        , Logger(ChunkServerLogger.WithTag("RootId: %v", chunkList->GetId()))
    {
        YT_LOG_DEBUG("Chunk tree traversal started (LowerBound: %v, UpperBound: %v, EnforceBounds: %v)",
            lowerBound,
            upperBound,
            EnforceBounds_);

        PushFirstChild(chunkList, 0, std::nullopt, lowerBound, upperBound);
    }

    void Run()
    {
        if (Context_->IsSynchronous()) {
            DoTraverse();
        } else {
            Context_->GetInvoker()->Invoke(
                BIND(&TChunkTreeTraverser::DoTraverse, MakeStrong(this)));
        }
    }
};

void TraverseChunkTree(
    IChunkTraverserContextPtr traverserContext,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const TLegacyReadLimit& lowerLimit,
    const TLegacyReadLimit& upperLimit,
    std::optional<int> keyColumnCount)
{
    return New<TChunkTreeTraverser>(
        std::move(traverserContext),
        std::move(visitor),
        root,
        true,
        keyColumnCount,
        lowerLimit,
        upperLimit)
        ->Run();
}

void TraverseChunkTree(
    IChunkTraverserContextPtr traverserContext,
    IChunkVisitorPtr visitor,
    TChunkList* root)
{
    return New<TChunkTreeTraverser>(
        std::move(traverserContext),
        std::move(visitor),
        root,
        false,
        std::nullopt,
        TLegacyReadLimit(),
        TLegacyReadLimit())
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncChunkTraverserContext
    : public IChunkTraverserContext
{
public:
    TAsyncChunkTraverserContext(
        NCellMaster::TBootstrap* bootstrap,
        NCellMaster::EAutomatonThreadQueue threadQueue)
        : Bootstrap_(bootstrap)
        , UserName_(Bootstrap_
            ->GetSecurityManager()
            ->GetAuthenticatedUser()
            ->GetName())
        , ThreadQueue_(threadQueue)
    { }

    virtual bool IsSynchronous() const override
    {
        return false;
    }

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
        auto* user = securityManager->FindUserByName(UserName_, true /*activeLifeStageOnly*/);
        securityManager->ChargeUser(user, {EUserWorkloadType::Read, 0, time});
    }

    virtual TFuture<TUnsealedChunkStatistics> GetUnsealedChunkStatistics(TChunk* chunk) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return chunkManager->GetChunkQuorumInfo(chunk).Apply(
            BIND([] (const NJournalClient::TChunkQuorumInfo& info) {
                return TUnsealedChunkStatistics{
                    .FirstOverlayedRowIndex = info.FirstOverlayedRowIndex,
                    .RowCount = info.RowCount
                };
            }));
    }

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const TString UserName_;
    const NCellMaster::EAutomatonThreadQueue ThreadQueue_;

};

IChunkTraverserContextPtr CreateAsyncChunkTraverserContext(
    NCellMaster::TBootstrap* bootstrap,
    NCellMaster::EAutomatonThreadQueue threadQueue)
{
    return New<TAsyncChunkTraverserContext>(
        bootstrap,
        threadQueue);
}

////////////////////////////////////////////////////////////////////////////////

class TSyncTraverserContext
    : public IChunkTraverserContext
{
public:
    virtual bool IsSynchronous() const override
    {
        return true;
    }

    virtual IInvokerPtr GetInvoker() const override
    {
        YT_ABORT();
    }

    virtual void OnPop(TChunkTree* /*node*/) override
    { }

    virtual void OnPush(TChunkTree* /*node*/) override
    { }

    virtual void OnShutdown(const std::vector<TChunkTree*>& /*nodes*/) override
    { }

    virtual void OnTimeSpent(TDuration /*time*/) override
    { }

    virtual TFuture<TUnsealedChunkStatistics> GetUnsealedChunkStatistics(TChunk* chunk) override
    {
        THROW_ERROR_EXCEPTION("Synchronous chunk tree traverser is unable to handle unsealed chunk %v",
            chunk->GetId());
    }
};

IChunkTraverserContextPtr GetSyncChunkTraverserContext()
{
    return RefCountedSingleton<TSyncTraverserContext>();
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
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TLegacyReadLimit& /*startLimit*/,
        const NChunkClient::TLegacyReadLimit& /*endLimit*/) override
    {
        return true;
    }

    virtual bool OnChunk(
        TChunk* chunk,
        std::optional<i64> /*rowIndex*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TLegacyReadLimit& /*startLimit*/,
        const NChunkClient::TLegacyReadLimit& /*endLimit*/,
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
    std::vector<TChunk*>* chunks)
{
    auto visitor = New<TEnumeratingChunkVisitor>(chunks);
    TraverseChunkTree(
        GetSyncChunkTraverserContext(),
        visitor,
        root);
}

std::vector<TChunk*> EnumerateChunksInChunkTree(
    TChunkList* root)
{
    std::vector<TChunk*> chunks;
    EnumerateChunksInChunkTree(root, &chunks);
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
            std::optional<int> /*tabletIndex*/,
            const NChunkClient::TLegacyReadLimit& /*startLimit*/,
            const NChunkClient::TLegacyReadLimit& /*endLimit*/) override
        {
            Stores_->push_back(dynamicStore);
            return true;
        }

        virtual bool OnChunk(
            TChunk* chunk,
            std::optional<i64> /*rowIndex*/,
            std::optional<int> /*tabletIndex*/,
            const NChunkClient::TLegacyReadLimit& /*startLimit*/,
            const NChunkClient::TLegacyReadLimit& /*endLimit*/,
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
        GetSyncChunkTraverserContext(),
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
