#include "chunk_tree_traverser.h"
#include "chunk_manager.h"
#include "chunk.h"
#include "chunk_view.h"
#include "dynamic_store.h"
#include "chunk_list.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/profiling/timing.h>

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

//! Returns smallest iterator it s.t.:
//! 1) start <= it <= end
//! 2) it == end || (!isMissing(*it) && isLess(key, *it))
//! If it (start <= it < end) is such that isMissing(*it) holds,
//! *it will not be an argument for isLess call.
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
public:
    struct TStackEntry
    {
        TChunkList* ChunkList;
        int ChunkListVersion;
        int ChildIndex;
        std::optional<i64> RowIndex;
        std::optional<int> TabletIndex;
        TReadLimit LowerLimit;
        TReadLimit UpperLimit;

        TStackEntry(
            TChunkList* chunkList,
            int childIndex,
            std::optional<i64> rowIndex,
            std::optional<int> tabletIndex,
            const TReadLimit& lowerLimit,
            const TReadLimit& upperLimit)
            : ChunkList(chunkList)
            , ChunkListVersion(chunkList->GetVersion())
            , ChildIndex(childIndex)
            , RowIndex(rowIndex)
            , TabletIndex(tabletIndex)
            , LowerLimit(lowerLimit)
            , UpperLimit(upperLimit)
        {
            YT_VERIFY(childIndex >= 0);
            YT_VERIFY(!rowIndex || *rowIndex >= 0);
            YT_VERIFY(!tabletIndex || *tabletIndex >= 0);
        }
    };

protected:
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

            YT_LOG_TRACE("Iteration started (Stack: %v)", Stack_);

            auto& entry = PeekStack();
            auto* chunkList = entry.ChunkList;

            if (!chunkList->IsAlive() || chunkList->GetVersion() != entry.ChunkListVersion) {
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::OptimisticLockFailure,
                    "Optimistic locking failed for chunk list %v",
                    chunkList->GetId());
            }

            if (entry.ChildIndex == std::ssize(chunkList->Children())) {
                PopStack();
                continue;
            }

            YT_LOG_TRACE("Current entry (Entry: %v)", entry);

            auto* child = chunkList->Children()[entry.ChildIndex];

            YT_LOG_TRACE("Current child (Index: %v, Id: %v, Kind: %v)",
                entry.ChildIndex,
                child->GetId(),
                child->GetType());

            // YT-4840: Skip empty children since Get(Min|Max)Key will not work for them.
            if (IsEmpty(child)) {
                YT_LOG_TRACE("Child is empty");
                ++entry.ChildIndex;
                continue;
            }

            switch (chunkList->GetKind()) {
                case EChunkListKind::Static:
                case EChunkListKind::JournalRoot:
                    if (auto future = VisitEntryStatic(&entry)) {
                        rescheduleAfterFuture = std::move(future);
                    }
                    break;

                case EChunkListKind::SortedDynamicRoot:
                case EChunkListKind::OrderedDynamicRoot:
                    VisitEntryDynamicRoot(&entry);
                    break;

                case EChunkListKind::SortedDynamicTablet:
                case EChunkListKind::SortedDynamicSubtablet:
                case EChunkListKind::OrderedDynamicTablet:
                case EChunkListKind::HunkRoot:
                    VisitEntryDynamic(&entry);
                    break;

                default:
                    THROW_ERROR_EXCEPTION("Attempting to traverse chunk list %v of unexpected kind %Qlv",
                        chunkList->GetId(),
                        chunkList->GetKind());
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

        TReadLimit subtreeStartLimit;
        TReadLimit subtreeEndLimit;
        std::optional<i64> rowIndex;

        YT_LOG_TRACE("Visiting static entry (Entry: %v)", *entry);

        if (EnforceBounds_) {
            if (auto future = RequestUnsealedChunksStatistics(*entry); future && !future.IsSet()) {
                return future;
            }

            if (IsJournalChunkType(childType)) {
                InferJournalChunkRowRange(*entry);
            }

            auto [childLowerStatistics, childUpperStatistics] = GetCumulativeStatisticsRange(*entry);

            // Tablet index.
            YT_VERIFY(!entry->LowerLimit.GetTabletIndex() && !entry->UpperLimit.GetTabletIndex());

            TReadLimit childLowerLimit;
            TReadLimit childUpperLimit;

            // Row index.
            {
                auto childLimit = childLowerStatistics.RowCount;
                rowIndex = *entry->RowIndex + childLimit;
                if (const auto& upperRowIndex = entry->UpperLimit.GetRowIndex()) {
                    if (*upperRowIndex <= childLimit) {
                        PopStack();
                        return {};
                    }
                    childLowerLimit.SetRowIndex(childLimit);
                    childUpperLimit.SetRowIndex(childUpperStatistics.RowCount);
                } else if (entry->LowerLimit.GetRowIndex()) {
                    childLowerLimit.SetRowIndex(childLimit);
                }
            }

            // Chunk index.
            {
                if (const auto& upperChunkIndex = entry->UpperLimit.GetChunkIndex()) {
                    if (*upperChunkIndex <= childLowerStatistics.ChunkCount) {
                        PopStack();
                        return {};
                    }
                    childLowerLimit.SetChunkIndex(childLowerStatistics.ChunkCount);
                    childUpperLimit.SetChunkIndex(childUpperStatistics.ChunkCount);
                } else if (entry->LowerLimit.GetChunkIndex()) {
                    childLowerLimit.SetChunkIndex(childLowerStatistics.ChunkCount);
                }
            }

            // Offset.
            {
                if (const auto& upperOffset = entry->UpperLimit.GetOffset()) {
                    if (*upperOffset <= childLowerStatistics.DataSize) {
                        PopStack();
                        return {};
                    }
                    childLowerLimit.SetOffset(childLowerStatistics.DataSize);
                    childUpperLimit.SetOffset(childUpperStatistics.DataSize);
                } else if (entry->LowerLimit.GetOffset()) {
                    childLowerLimit.SetOffset(childLowerStatistics.DataSize);
                }
            }

            // Key.
            {
                if (entry->UpperLimit.KeyBound()) {
                    YT_LOG_ALERT_UNLESS(Comparator_, "Chunk tree traverser entry has key bounds, "
                        "but comparator is not provided");

                    childLowerLimit.KeyBound() = GetLowerKeyBoundOrThrow(child, Comparator_.GetLength());
                    if (Comparator_.IsRangeEmpty(childLowerLimit.KeyBound(), entry->UpperLimit.KeyBound())) {
                        PopStack();
                        return {};
                    }
                    childUpperLimit.KeyBound() = GetUpperKeyBoundOrThrow(child, Comparator_.GetLength());
                } else if (entry->LowerLimit.KeyBound()) {
                    YT_LOG_ALERT_UNLESS(Comparator_, "Chunk tree traverser entry has key bounds, "
                        "but comparator is not provided");

                    childLowerLimit.KeyBound() = GetLowerKeyBoundOrThrow(child, Comparator_.GetLength());
                }
            }

            GetInducedSubtreeRange(
                *entry,
                childLowerLimit,
                childUpperLimit,
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
            YT_LOG_TRACE(
                "Visiting static chunk (Id: %v, RowIndex: %v, StartLimit: %v, EndLimit: %v)",
                childChunk->GetId(),
                rowIndex,
                subtreeStartLimit,
                subtreeEndLimit);
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
            THROW_ERROR_EXCEPTION("Child %v has unexpected type %Qlv",
                child->GetId(),
                childType);
        }

        return {};
    }

    void VisitEntryDynamicRoot(TStackEntry* entry)
    {
        auto* chunkList = entry->ChunkList;
        const auto& cumulativeStatistics = chunkList->CumulativeStatistics();
        auto* child = chunkList->Children()[entry->ChildIndex];
        bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot;
        bool isSorted = chunkList->GetKind() == EChunkListKind::SortedDynamicRoot;

        TReadLimit subtreeStartLimit;
        TReadLimit subtreeEndLimit;
        std::optional<int> tabletIndex;

        YT_LOG_TRACE("Visiting dynamic root entry (Entry: %v)", *entry);

        if (EnforceBounds_) {
            // Row index.
            YT_VERIFY((!entry->LowerLimit.GetRowIndex() && !entry->UpperLimit.GetRowIndex()) || isOrdered);

            // Offset.
            YT_VERIFY(!entry->LowerLimit.GetOffset() && !entry->UpperLimit.GetOffset());

            // Tablet index.
            YT_VERIFY((!entry->LowerLimit.GetTabletIndex() && !entry->UpperLimit.GetTabletIndex()) || isOrdered);

            if (isOrdered) {
                // Read limit {tablet_index = 42} is equivalent to {tablet_index = 42; row_index = 0},
                // for ordered dynamic tables. Perform such transformation.
                entry->LowerLimit.SetRowIndex(entry->LowerLimit.GetRowIndex().value_or(0));
                entry->UpperLimit.SetRowIndex(entry->UpperLimit.GetRowIndex().value_or(0));
            }

            TReadLimit childLowerBound;
            TReadLimit childUpperBound;

            TOwningKeyBound pivotKeyLowerBound;
            TOwningKeyBound nextPivotKeyUpperBound;

            if (isSorted) {
                pivotKeyLowerBound = chunkList->Children()[entry->ChildIndex]->AsChunkList()->GetPivotKeyBound().ToOwning();
                nextPivotKeyUpperBound = entry->ChildIndex + 1 < std::ssize(chunkList->Children())
                    ? chunkList->Children()[entry->ChildIndex + 1]->AsChunkList()->GetPivotKeyBound().Invert().ToOwning()
                    : TOwningKeyBound::MakeUniversal(/* isUpper */ true);
            }

            // Tablet index.
            {
                if (const auto& upperTabletIndex = entry->UpperLimit.GetTabletIndex();
                    upperTabletIndex && *upperTabletIndex < entry->ChildIndex)
                {
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
                if (const auto& upperChunkIndex = entry->UpperLimit.GetChunkIndex()) {
                    if (*upperChunkIndex <= childLimit) {
                        PopStack();
                        return;
                    }
                    childLowerBound.SetChunkIndex(childLimit);
                    childUpperBound.SetChunkIndex(cumulativeStatistics.GetCurrentSum(entry->ChildIndex).ChunkCount);
                } else if (entry->LowerLimit.GetChunkIndex()) {
                    childLowerBound.SetChunkIndex(childLimit);
                }
            }

            // Key.
            if (isSorted) {
                if (entry->LowerLimit.KeyBound() || entry->UpperLimit.KeyBound()) {
                    YT_LOG_ALERT_UNLESS(Comparator_, "Chunk tree traverser entry has key bounds, "
                        "but comparator is not provided");
                }

                if (entry->UpperLimit.KeyBound()) {
                    if (Comparator_.IsRangeEmpty(pivotKeyLowerBound, entry->UpperLimit.KeyBound())) {
                        PopStack();
                        return;
                    }
                }

                childLowerBound.KeyBound() = pivotKeyLowerBound;
                childUpperBound.KeyBound() = nextPivotKeyUpperBound;
            }

            GetInducedSubtreeRange(
                *entry,
                childLowerBound,
                childUpperBound,
                &subtreeStartLimit,
                &subtreeEndLimit);

            // NB: Chunks may cross tablet boundaries.
            if (isSorted) {
                if (!subtreeStartLimit.KeyBound() ||
                    Comparator_.CompareKeyBounds(subtreeStartLimit.KeyBound(), pivotKeyLowerBound) < 0)
                {
                    subtreeStartLimit.KeyBound() = pivotKeyLowerBound;
                }
                if (!subtreeEndLimit.KeyBound() ||
                    Comparator_.CompareKeyBounds(subtreeEndLimit.KeyBound(), nextPivotKeyUpperBound) > 0)
                {
                    subtreeEndLimit.KeyBound() = nextPivotKeyUpperBound;
                }
            }

            // NB: Row index is tablet-wise for ordered tables.
            if (isOrdered) {
                YT_VERIFY(tabletIndex);
                if (const auto& lowerTabletIndex = entry->LowerLimit.GetTabletIndex();
                    lowerTabletIndex && *lowerTabletIndex == *tabletIndex)
                {
                    subtreeStartLimit.SetRowIndex(entry->LowerLimit.GetRowIndex());
                }
                if (const auto& upperTabletIndex = entry->UpperLimit.GetTabletIndex();
                    upperTabletIndex && *upperTabletIndex == *tabletIndex)
                {
                    subtreeEndLimit.SetRowIndex(entry->UpperLimit.GetRowIndex());
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
        auto childType = child->GetType();
        const auto& cumulativeStatistics = chunkList->CumulativeStatistics();

        bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet;

        YT_LOG_TRACE("Visiting dynamic entry (Entry: %v)", *entry);

        // Row index.
        YT_VERIFY((!entry->LowerLimit.GetRowIndex() && !entry->UpperLimit.GetRowIndex()) || isOrdered);

        // Offset.
        YT_VERIFY(!entry->LowerLimit.GetOffset() && !entry->UpperLimit.GetOffset());

        // Tablet index.
        YT_VERIFY((!entry->LowerLimit.GetTabletIndex() && !entry->UpperLimit.GetTabletIndex()) || isOrdered);

        auto tabletIndex = entry->TabletIndex;

        TReadLimit subtreeStartLimit;
        TReadLimit subtreeEndLimit;
        std::optional<i64> rowIndex;

        if (EnforceBounds_) {
            TReadLimit childLowerLimit;
            TReadLimit childUpperLimit;

            // Row index.
            if (isOrdered) {
                i64 childLimit = cumulativeStatistics.GetPreviousSum(entry->ChildIndex).RowCount;
                rowIndex = childLimit;
                if (const auto& upperRowIndex = entry->UpperLimit.GetRowIndex()) {
                    if (*upperRowIndex <= childLimit) {
                        PopStack();
                        return;
                    }
                    childLowerLimit.SetRowIndex(childLimit);

                    // NB: Dynamic stores at the end of the chunk list may be arbitrarily large
                    // but their size is not accounted in cumulative statistics.
                    if (entry->ChunkList->Children()[entry->ChildIndex]->GetType() == EObjectType::OrderedDynamicTabletStore) {
                        childUpperLimit.SetRowIndex(std::numeric_limits<i64>::max());
                    } else {
                        childUpperLimit.SetRowIndex(cumulativeStatistics.GetCurrentSum(entry->ChildIndex).RowCount);
                    }
                } else if (entry->LowerLimit.GetRowIndex()) {
                    childLowerLimit.SetRowIndex(childLimit);
                }
            }

            // Chunk index.
            {
                i64 childLimit = cumulativeStatistics.GetPreviousSum(entry->ChildIndex).ChunkCount;

                if (const auto& upperChunkIndex = entry->UpperLimit.GetChunkIndex()) {
                    if (*upperChunkIndex <= childLimit) {
                        PopStack();
                        return;
                    }
                    childLowerLimit.SetChunkIndex(childLimit);
                    childUpperLimit.SetChunkIndex(cumulativeStatistics.GetCurrentSum(entry->ChildIndex).ChunkCount);
                } else if (entry->LowerLimit.GetChunkIndex()) {
                    childLowerLimit.SetChunkIndex(childLimit);
                }
            }

            // Tablet index.
            {
                if (const auto& lowerTabletIndex = entry->LowerLimit.GetTabletIndex()) {
                    YT_VERIFY(tabletIndex);
                    if (*lowerTabletIndex > *tabletIndex) {
                        ++entry->ChildIndex;
                        return;
                    }
                }
                if (const auto& upperTabletIndex = entry->UpperLimit.GetTabletIndex()) {
                    YT_VERIFY(tabletIndex);
                    if (*upperTabletIndex < *tabletIndex) {
                        PopStack();
                        return;
                    }
                }
            }

            // Key.
            {
                if (entry->LowerLimit.KeyBound() || entry->UpperLimit.KeyBound()) {
                    YT_VERIFY(Comparator_);

                    // NB: If child is a chunk list, its children can be unsorted, so we can't prune by lower or upper key bounds.
                    if (childType == EObjectType::ChunkList) {
                        childLowerLimit.KeyBound() = entry->LowerLimit.KeyBound();
                        childUpperLimit.KeyBound() = entry->UpperLimit.KeyBound();
                    } else {
                        childLowerLimit.KeyBound() = GetLowerKeyBoundOrThrow(child, Comparator_.GetLength());
                        childUpperLimit.KeyBound() = GetUpperKeyBoundOrThrow(child, Comparator_.GetLength());

                        // NB: tablet children are NOT sorted by keys, so we should not perform pruning in
                        // any of two branches below, full scan is intended.

                        if (entry->UpperLimit.KeyBound() && Comparator_.IsRangeEmpty(childLowerLimit.KeyBound(), entry->UpperLimit.KeyBound())) {
                            ++entry->ChildIndex;
                            return;
                        }

                        if (entry->LowerLimit.KeyBound() && Comparator_.IsRangeEmpty(entry->LowerLimit.KeyBound(), childUpperLimit.KeyBound())) {
                            ++entry->ChildIndex;
                            return;
                        }
                    }
                }
            }

            GetInducedSubtreeRange(
                *entry,
                childLowerLimit,
                childUpperLimit,
                &subtreeStartLimit,
                &subtreeEndLimit);
        }

        ++entry->ChildIndex;

        switch (childType) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::ChunkView: {
                TChunk* childChunk = nullptr;
                TTransactionId timestampTransactionId;
                if (childType == EObjectType::ChunkView) {
                    auto* chunkView = child->AsChunkView();

                    YT_LOG_TRACE(
                        "Visiting chunk view (Id: %v, UnderlyingChunkId: %v, LowerLimit: %v, UpperLimit: %v)",
                        chunkView->GetId(),
                        chunkView->GetUnderlyingChunk()->GetId(),
                        chunkView->ReadRange().LowerLimit(),
                        chunkView->ReadRange().UpperLimit());

                    if (Visitor_->OnChunkView(chunkView)) {
                        ++ChunkCount_;
                        YT_LOG_TRACE("Not visiting underlying chunk");
                        return;
                    }

                    if (EnforceBounds_) {
                        YT_VERIFY(Comparator_);
                        YT_VERIFY(!Comparator_.HasDescendingSortOrder());

                        {
                            // COMPAT(max42): YT-14140.
                            auto legacySubtreeStartLimit = ReadLimitToLegacyReadLimit(subtreeStartLimit);
                            auto legacySubtreeEndLimit = ReadLimitToLegacyReadLimit(subtreeEndLimit);

                            legacySubtreeStartLimit = chunkView->GetAdjustedLowerReadLimit(legacySubtreeStartLimit);
                            legacySubtreeEndLimit = chunkView->GetAdjustedUpperReadLimit(legacySubtreeEndLimit);

                            subtreeStartLimit = ReadLimitFromLegacyReadLimit(legacySubtreeStartLimit, /* isUpper */ false, Comparator_.GetLength());
                            subtreeEndLimit = ReadLimitFromLegacyReadLimit(legacySubtreeEndLimit, /* isUpper */ true, Comparator_.GetLength());

                            YT_LOG_TRACE(
                                "Adjusting subtree limits using chunk view (SubtreeStartLimit: %v, SubtreeEndLimit: %v)",
                                subtreeStartLimit,
                                subtreeEndLimit);
                        }
                    }

                    timestampTransactionId = chunkView->GetTransactionId();

                    childChunk = chunkView->GetUnderlyingChunk();
                } else {
                    childChunk = child->AsChunk();
                }

                YT_LOG_TRACE(
                    "Visiting dynamic chunk (Id: %v, RowIndex: %v, TabletIndex: %v, StartLimit: %v, EndLimit: %v)",
                    childChunk->GetId(),
                    rowIndex,
                    tabletIndex,
                    subtreeStartLimit,
                    subtreeEndLimit);

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
                YT_LOG_TRACE(
                    "Visiting dynamic store (Id: %v, TabletIndex: %v, StartLimit: %v, EndLimit: %v)",
                    dynamicStore->GetId(),
                    tabletIndex,
                    subtreeStartLimit,
                    subtreeEndLimit);

                if (!Visitor_->OnDynamicStore(dynamicStore, tabletIndex, subtreeStartLimit, subtreeEndLimit)) {
                    Shutdown();
                    return;
                }
                break;
            }

            case EObjectType::ChunkList: {
                auto* childChunkList = child->AsChunkList();
                auto childChunkListKind = childChunkList->GetKind();
                if (childChunkListKind != EChunkListKind::SortedDynamicSubtablet &&
                    childChunkListKind != EChunkListKind::HunkRoot)
                {
                    THROW_ERROR_EXCEPTION("Chunk list %v has unexpected kind %Qlv",
                        childChunkList->GetId(),
                        childChunkListKind);
                }
                // Don't traverse hunks when bounds are enforced.
                if (childChunkListKind != EChunkListKind::HunkRoot || !EnforceBounds_) {
                    PushFirstChild(childChunkList, 0, tabletIndex, subtreeStartLimit, subtreeEndLimit);
                }
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Child %v has unexpected type %Qlv",
                    child->GetId(),
                    childType);
        }
    }

    void PushFirstChild(
        TChunkList* chunkList,
        std::optional<i64> rowIndex,
        std::optional<int> tabletIndex,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit)
    {
        if (chunkList->Children().empty()) {
            return;
        }

        switch (chunkList->GetKind()) {
            case EChunkListKind::Static:
            case EChunkListKind::JournalRoot:
                PushFirstChildStatic(chunkList, rowIndex, lowerLimit, upperLimit);
                break;

            case EChunkListKind::SortedDynamicRoot:
            case EChunkListKind::OrderedDynamicRoot:
                PushFirstChildDynamicRoot(chunkList, rowIndex, lowerLimit, upperLimit);
                break;

            case EChunkListKind::SortedDynamicTablet:
            case EChunkListKind::SortedDynamicSubtablet:
            case EChunkListKind::OrderedDynamicTablet:
            case EChunkListKind::HunkRoot:
                PushFirstChildDynamic(chunkList, rowIndex, tabletIndex, lowerLimit, upperLimit);
                break;

            default:
                THROW_ERROR_EXCEPTION("Chunk list %v has unexpected kind %Qlv",
                    chunkList->GetId(),
                    chunkList->GetKind());
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
        int adjustedIndex = children.size();
        if (limit < total) {
            const auto& cumulativeStatistics = chunkList->CumulativeStatistics();
            adjustedIndex = std::max(currentIndex, cumulativeStatistics.UpperBound(limit, member));
        }
        // NB: Unsealed chunks are not accounted in chunk list statistics.
        while (adjustedIndex > 0 && !IsSealedChild(children[adjustedIndex - 1]) ) {
            --adjustedIndex;
        }
        return adjustedIndex;
    }

    void PushFirstChildStatic(
        TChunkList* chunkList,
        std::optional<i64> rowIndex,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit)
    {
        int childIndex = 0;

        if (EnforceBounds_) {
            const auto& statistics = chunkList->Statistics();
            YT_VERIFY(!lowerLimit.GetTabletIndex());

            // Row index.
            if (const auto& lowerRowIndex = lowerLimit.GetRowIndex()) {
                childIndex = AdjustStartChildIndex(
                    childIndex,
                    chunkList,
                    RowCountMember,
                    *lowerRowIndex,
                    statistics.LogicalRowCount);
            }

            // Chunk index.
            if (const auto& lowerChunkIndex = lowerLimit.GetChunkIndex()) {
                childIndex = AdjustStartChildIndex(
                    childIndex,
                    chunkList,
                    ChunkCountMember,
                    *lowerChunkIndex,
                    statistics.LogicalChunkCount);
            }

            // Offset.
            if (const auto& lowerOffset = lowerLimit.GetOffset()) {
                childIndex = AdjustStartChildIndex(
                    childIndex,
                    chunkList,
                    DataSizeMember,
                    *lowerOffset,
                    statistics.UncompressedDataSize);
            }

            // Key.
            if (const auto& lowerBound = lowerLimit.KeyBound()) {
                YT_VERIFY(Comparator_);

                auto it = UpperBoundWithMissingValues(
                    chunkList->Children().begin(),
                    chunkList->Children().end(),
                    lowerBound,
                    // isLess
                    [comparator = Comparator_] (const TKeyBound& lowerBound, const TChunkTree* chunkTree) {
                        // If corresponding range is non-empty, a chunk tree is interesting for us.
                        // Thus we are seeking for the leftmost chunk tree such that this range is non-empty.
                        return !comparator.IsRangeEmpty(lowerBound, GetUpperKeyBoundOrThrow(chunkTree, comparator.GetLength()));
                    },
                    // isMissing
                    [] (const TChunkTree* chunkTree) {
                        return IsEmpty(chunkTree);
                    });

                childIndex = std::max(childIndex, static_cast<int>(it - chunkList->Children().begin()));
            }
        }

        PushStack(TStackEntry(
            chunkList,
            childIndex,
            rowIndex,
            std::nullopt,
            lowerLimit,
            upperLimit));
    }

    void PushFirstChildDynamicRoot(
        TChunkList* chunkList,
        std::optional<i64> rowIndex,
        TReadLimit lowerLimit,
        TReadLimit upperLimit)
    {
        int childIndex = 0;

        if (EnforceBounds_) {
            const auto& statistics = chunkList->Statistics();
            bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicRoot;

            // Offset.
            YT_VERIFY(!lowerLimit.GetOffset());

            // Tablet index.
            if (const auto& lowerTabletIndex = lowerLimit.GetTabletIndex()) {
                YT_VERIFY(isOrdered);
                childIndex = std::max(
                    childIndex,
                    std::min(
                        *lowerTabletIndex,
                        static_cast<int>(chunkList->Children().size())));
            }

            // Row index.
            if (lowerLimit.GetRowIndex() || upperLimit.GetRowIndex()) {
                YT_VERIFY(isOrdered);
                // Row indices remain tablet-wise, nothing to change here.
            }

            // Chunk index.
            if (const auto& lowerChunkIndex = lowerLimit.GetChunkIndex()) {
                childIndex = AdjustStartChildIndex(
                    childIndex,
                    chunkList,
                    ChunkCountMember,
                    *lowerChunkIndex,
                    statistics.LogicalChunkCount);
            }

            // Key.
            if (const auto& lowerBound = lowerLimit.KeyBound()) {
                YT_VERIFY(Comparator_);
                auto it = std::upper_bound(
                    chunkList->Children().begin(),
                    chunkList->Children().end(),
                    lowerBound,
                    [comparator = Comparator_] (const TKeyBound& lowerBound, const TChunkTree* chunkTree) {
                        // This method should meet following semantics:
                        // true if requested lower bound is strictly before the pivot key lower bound.
                        return comparator.CompareKeyBounds(lowerBound, chunkTree->AsChunkList()->GetPivotKeyBound()) < 0;
                    });
                childIndex = std::max(childIndex, static_cast<int>(std::distance(chunkList->Children().begin(), it) - 1));
            }
        }

        PushStack(TStackEntry(
            chunkList,
            childIndex,
            rowIndex,
            std::nullopt,
            lowerLimit,
            upperLimit));
    }

    void PushFirstChildDynamic(
        TChunkList* chunkList,
        std::optional<i64> rowIndex,
        std::optional<int> tabletIndex,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit)
    {
        int chunkIndex = 0;

        if (EnforceBounds_) {
            bool isOrdered = chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet;
            const auto& statistics = chunkList->Statistics();

            // Row index.
            YT_VERIFY(!lowerLimit.GetRowIndex() || isOrdered);

            // Tablet index.
            YT_VERIFY(!lowerLimit.GetTabletIndex() || isOrdered);

            // Offset.
            YT_VERIFY(!lowerLimit.GetOffset());

            // Row index.
            if (isOrdered) {
                if (const auto& lowerRowIndex = lowerLimit.GetRowIndex()) {
                    chunkIndex = AdjustStartChildIndex(
                        chunkIndex,
                        chunkList,
                        RowCountMember,
                        *lowerRowIndex,
                        statistics.LogicalRowCount);

                    while (chunkIndex > 0) {
                        auto* child = chunkList->Children()[chunkIndex - 1];
                        if (child && child->GetType() == EObjectType::OrderedDynamicTabletStore) {
                            --chunkIndex;
                        } else {
                            break;
                        }
                    }
                }
            }

            // Chunk index.
            if (const auto& lowerChunkIndex = lowerLimit.GetChunkIndex()) {
                chunkIndex = AdjustStartChildIndex(
                    chunkIndex,
                    chunkList,
                    ChunkCountMember,
                    *lowerChunkIndex,
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
            lowerLimit,
            upperLimit));
    }

    i64 GetFirstOverlayedRowIndex(const TChunk* chunk)
    {
        YT_ASSERT(chunk->GetOverlayed());
        return chunk->IsSealed()
            ? *chunk->GetFirstOverlayedRowIndex()
            : *GetUnsealedChunkStatistics(chunk).FirstOverlayedRowIndex;
    }

    void GetInducedSubtreeRange(
        const TStackEntry& entry,
        const TReadLimit& childLowerLimit,
        const TReadLimit& childUpperLimit,
        TReadLimit* startLimit,
        TReadLimit* endLimit)
    {
        YT_VERIFY(EnforceBounds_);

        const auto* child = entry.ChunkList->Children()[entry.ChildIndex];

        // Row index.

        // Ordered dynamic root is skipped since row index inside tablet is tablet-wise.
        // Ordered dynamic stores are skipped since they should have absolute row index
        // (0 is the beginning of the tablet) rather than relative (0 is the beginning of the store).
        if (entry.ChunkList->GetKind() != EChunkListKind::OrderedDynamicRoot) {
            if (const auto& lowerRowIndex = entry.LowerLimit.GetRowIndex()) {
                i64 newLowerBound = *lowerRowIndex;
                if (child->GetType() != EObjectType::OrderedDynamicTabletStore) {
                    YT_VERIFY(childLowerLimit.GetRowIndex());
                    newLowerBound -= *childLowerLimit.GetRowIndex();
                }
                if (newLowerBound > 0) {
                    startLimit->SetRowIndex(newLowerBound);
                }
            }
            if (entry.UpperLimit.GetRowIndex()) {
                YT_VERIFY(childUpperLimit.GetRowIndex());
                if (*entry.UpperLimit.GetRowIndex() < *childUpperLimit.GetRowIndex()) {
                    i64 newUpperRowIndex = *entry.UpperLimit.GetRowIndex();
                    if (child->GetType() != EObjectType::OrderedDynamicTabletStore) {
                        newUpperRowIndex -= *childLowerLimit.GetRowIndex();
                    }
                    YT_ASSERT(newUpperRowIndex > 0);
                    endLimit->SetRowIndex(newUpperRowIndex);
                }
            }
        }

        // Adjust for journal chunks.
        if (IsJournalChunkType(child->GetType())) {
            const auto* chunk = child->AsChunk();
            auto [startRowIndex, endRowIndex] = GetJournalChunkRowRange(chunk);

            if (!startLimit->GetRowIndex()) {
                startLimit->SetRowIndex(0);
            }
            if (!endLimit->GetRowIndex()) {
                endLimit->SetRowIndex(endRowIndex - startRowIndex);
            }

            auto logicalStartRowIndex = *startLimit->GetRowIndex();
            auto logicalEndRowIndex = *endLimit->GetRowIndex();

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
                startLimit->SetRowIndex(rowIndexDelta + *startLimit->GetRowIndex());
                endLimit->SetRowIndex(rowIndexDelta + *endLimit->GetRowIndex());
            }

            auto physicalStartRowIndex = *startLimit->GetRowIndex();
            auto physicalEndRowIndex = *endLimit->GetRowIndex();

            YT_LOG_DEBUG("Journal chunk fetched (ChunkId: %v, Overlayed: %v, LogicalRowIndexes: %v-%v, PhysicalRowIndexes: %v-%v, JournalRowIndexes: %v-%v)",
                chunk->GetId(),
                chunk->GetOverlayed(),
                logicalStartRowIndex,
                logicalEndRowIndex - 1,
                physicalStartRowIndex,
                physicalEndRowIndex - 1,
                startRowIndex + logicalStartRowIndex,
                startRowIndex + logicalEndRowIndex - 1);
        }

        // NB: Tablet index is not needed here, because only chunks inside correct tablets
        // will be visited and they know their tabletIndex.

        // Chunk index.
        if (const auto& lowerChunkIndex = entry.LowerLimit.GetChunkIndex()) {
            YT_VERIFY(childLowerLimit.GetChunkIndex());
            i64 newLowerChunkIndex = *lowerChunkIndex - *childLowerLimit.GetChunkIndex();
            if (newLowerChunkIndex > 0) {
                startLimit->SetChunkIndex(newLowerChunkIndex);
            }
        }
        if (const auto& upperChunkIndex = entry.UpperLimit.GetChunkIndex()) {
            YT_VERIFY(childUpperLimit.GetChunkIndex());
            if (*upperChunkIndex < *childUpperLimit.GetChunkIndex()) {
                YT_VERIFY(childLowerLimit.GetChunkIndex());
                i64 newUpperChunkIndex = *upperChunkIndex - *childLowerLimit.GetChunkIndex();
                YT_VERIFY(newUpperChunkIndex > 0);
                endLimit->SetChunkIndex(newUpperChunkIndex);
            }
        }

        // Offset.
        if (const auto& lowerOffset = entry.LowerLimit.GetOffset()) {
            YT_VERIFY(childLowerLimit.GetOffset());
            i64 newLowerOffset = *lowerOffset - *childLowerLimit.GetOffset();
            if (newLowerOffset > 0) {
                startLimit->SetOffset(newLowerOffset);
            }
        }
        if (const auto& upperOffset = entry.UpperLimit.GetOffset()) {
            YT_VERIFY(childUpperLimit.GetOffset());
            if (*upperOffset < *childUpperLimit.GetOffset()) {
                YT_VERIFY(childLowerLimit.GetOffset());
                i64 newUpperOffset = *upperOffset - *childLowerLimit.GetOffset();
                YT_VERIFY(newUpperOffset > 0);
                endLimit->SetOffset(newUpperOffset);
            }
        }

        // Key.
        if (const auto& lowerBound = entry.LowerLimit.KeyBound()) {
            YT_VERIFY(childLowerLimit.KeyBound());
            YT_VERIFY(Comparator_);
            if (Comparator_.CompareKeyBounds(lowerBound, childLowerLimit.KeyBound()) > 0) {
                startLimit->KeyBound() = lowerBound;
            }
        }
        if (const auto& upperBound = entry.UpperLimit.KeyBound()) {
            YT_VERIFY(childUpperLimit.KeyBound());
            YT_VERIFY(Comparator_);
            if (Comparator_.CompareKeyBounds(upperBound, childUpperLimit.KeyBound()) < 0) {
                endLimit->KeyBound() = upperBound;
            }
        }

        YT_LOG_TRACE(
            "Subtree range induced (Entry: %v, ChildLowerLimit: %v, ChildUpperLimit: %v, "
            "StartLimit: %v, EndLimit: %v)",
            entry,
            childLowerLimit,
            childUpperLimit,
            *startLimit,
            *endLimit);
    }

    bool IsStackEmpty()
    {
        return Stack_.empty();
    }

    void PushStack(const TStackEntry& newEntry)
    {
        ++ChunkListCount_;
        YT_LOG_TRACE("Pushing new entry to stack (Entry: %v)", newEntry);
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
        YT_LOG_TRACE("Poping stack (Entry: %v)", entry);
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
    const TComparator Comparator_;

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
        TComparator comparator,
        TReadLimit lowerLimit,
        TReadLimit upperLimit)
        : Context_(std::move(context))
        , Visitor_(std::move(visitor))
        , EnforceBounds_(enforceBounds)
        , Comparator_(std::move(comparator))
        , Logger(ChunkServerLogger.WithTag("RootId: %v", chunkList->GetId()))
    {
        YT_LOG_DEBUG("Chunk tree traversal started (LowerLimit: %v, UpperLimit: %v, EnforceBounds: %v)",
            lowerLimit,
            upperLimit,
            EnforceBounds_);

        PushFirstChild(chunkList, 0, std::nullopt, lowerLimit, upperLimit);
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

TString ToString(const TChunkTreeTraverser::TStackEntry& entry)
{
    return Format(
        "{Id: %v, Kind: %v, ChildIndex: %v, LowerLimit: %v, UpperLimit: %v}",
        entry.ChunkList->GetId(),
        entry.ChunkList->GetKind(),
        entry.ChildIndex,
        entry.LowerLimit,
        entry.UpperLimit);
}

void TraverseChunkTree(
    IChunkTraverserContextPtr traverserContext,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    TComparator comparator)
{
    return New<TChunkTreeTraverser>(
        std::move(traverserContext),
        std::move(visitor),
        root,
        true,
        comparator,
        lowerLimit,
        upperLimit)
        ->Run();
}

void TraverseChunkTree(
    IChunkTraverserContextPtr traverserContext,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const TLegacyReadLimit& legacyLowerLimit,
    const TLegacyReadLimit& legacyUpperLimit,
    TComparator comparator)
{
    TReadLimit lowerLimit;
    TReadLimit upperLimit;

    if (legacyLowerLimit.HasLegacyKey() || legacyUpperLimit.HasLegacyKey()) {
        YT_VERIFY(comparator);
        lowerLimit = ReadLimitFromLegacyReadLimit(legacyLowerLimit, /* isUpper */ false, comparator.GetLength());
        upperLimit = ReadLimitFromLegacyReadLimit(legacyUpperLimit, /* isUpper */ true, comparator.GetLength());
    } else {
        lowerLimit = ReadLimitFromLegacyReadLimitKeyless(legacyLowerLimit);
        upperLimit = ReadLimitFromLegacyReadLimitKeyless(legacyUpperLimit);
    }

    return New<TChunkTreeTraverser>(
        std::move(traverserContext),
        std::move(visitor),
        root,
        true,
        comparator,
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
        TComparator(),
        TReadLimit(),
        TReadLimit())
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

    virtual bool OnChunkView(TChunkView* /*chunkView*/) override
    {
        return false;
    }

    virtual bool OnDynamicStore(
        TDynamicStore* /*dynamicStore*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/) override
    {
        return true;
    }

    virtual bool OnChunk(
        TChunk* chunk,
        std::optional<i64> /*rowIndex*/,
        std::optional<int> /*tabletIndex*/,
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
            const NChunkClient::TReadLimit& /*startLimit*/,
            const NChunkClient::TReadLimit& /*endLimit*/) override
        {
            Stores_->push_back(dynamicStore);
            return true;
        }

        virtual bool OnChunk(
            TChunk* chunk,
            std::optional<i64> /*rowIndex*/,
            std::optional<int> /*tabletIndex*/,
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
