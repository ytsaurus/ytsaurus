#include "chunk_tree_traverser.h"
#include "chunk.h"
#include "chunk_list.h"
#include "helpers.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>

#include <yt/server/object_server/object.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/singleton.h>

#include <yt/core/profiling/timing.h>
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

static const auto RowCountMember = &TChunkList::TCumulativeStatisticsEntry::RowCount;
static const auto ChunkCountMember = &TChunkList::TCumulativeStatisticsEntry::ChunkCount;
static const auto DataSizeMember = &TChunkList::TCumulativeStatisticsEntry::DataSize;

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
        TReadLimit LowerBound;
        TReadLimit UpperBound;
        int ChunkCount;

        TStackEntry(
            TChunkList* chunkList,
            int childIndex,
            i64 rowIndex,
            const TReadLimit& lowerBound,
            const TReadLimit& upperBound,
            int chunkCount = 0)
            : ChunkList(chunkList)
            , ChunkListVersion(chunkList->GetVersion())
            , ChildIndex(childIndex)
            , RowIndex(rowIndex)
            , LowerBound(lowerBound)
            , UpperBound(upperBound)
            , ChunkCount(chunkCount)
        {
            YCHECK(childIndex >= 0);
            YCHECK(rowIndex >= 0);
            YCHECK(chunkCount >= 0);
        }
    };

    void OnTimeSpent(TDuration elapsed)
    {
        CpuTime_ += elapsed;
        Callbacks_->OnTimeSpent(elapsed);
    }

    void OnFinish(const TError& error)
    {
        LOG_DEBUG(error, "Chunk tree traversal finished (CpuTime: %v, WallTime: %v, ChunkCount: %v, ChunkListCount: %v)",
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
                case EChunkListKind::OrderedDynamicTablet:
                    VisitEntryDynamicTablet(&entry, &visitedChunkCount);
                    break;

                default:
                    Y_UNREACHABLE();
            }

        }

        // Schedule continuation.
        Callbacks_->OnTimeSpent(timer.GetElapsedTime());
        invoker->Invoke(BIND(&TChunkTreeTraverser::DoTraverse, MakeStrong(this)));
    }

    void VisitEntryStatic(TStackEntry* entry, int* visitedChunkCount)
    {
        auto* chunkList = entry->ChunkList;
        const auto& statistics = chunkList->Statistics();
        auto* child = chunkList->Children()[entry->ChildIndex];

        auto fetchPrevSum = [&] (i64 TChunkList::TCumulativeStatisticsEntry::* member) -> i64 {
            return entry->ChildIndex == 0
                ? 0
                : chunkList->CumulativeStatistics()[entry->ChildIndex - 1].*member;
        };

        auto fetchCurrentSum = [&] (i64 TChunkList::TCumulativeStatisticsEntry::* member, i64 fallback) {
            return entry->ChildIndex == chunkList->Children().size() - 1
                ? fallback
                : chunkList->CumulativeStatistics()[entry->ChildIndex].*member;
        };

        TReadLimit childLowerBound;
        TReadLimit childUpperBound;

        i64 rowIndex = 0;

        // Row index
        {
            i64 childLimit = fetchPrevSum(RowCountMember);
            rowIndex = entry->RowIndex + childLimit;
            if (entry->UpperBound.HasRowIndex()) {
                if (entry->UpperBound.GetRowIndex() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetRowIndex(childLimit);
                i64 totalRowCount = statistics.Sealed ? statistics.LogicalRowCount : std::numeric_limits<i64>::max();
                childUpperBound.SetRowIndex(fetchCurrentSum(RowCountMember, totalRowCount));
            } else if (entry->LowerBound.HasRowIndex()) {
                childLowerBound.SetRowIndex(childLimit);
            }
        }

        // Chunk index
        {
            i64 childLimit = fetchPrevSum(ChunkCountMember);
            if (entry->UpperBound.HasChunkIndex()) {
                if (entry->UpperBound.GetChunkIndex() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetChunkIndex(childLimit);
                childUpperBound.SetChunkIndex(fetchCurrentSum(ChunkCountMember, statistics.LogicalChunkCount));
            } else if (entry->LowerBound.HasChunkIndex()) {
                childLowerBound.SetChunkIndex(childLimit);
            }
        }

        // Offset
        {
            i64 childLimit = fetchPrevSum(DataSizeMember);
            if (entry->UpperBound.HasOffset()) {
                if (entry->UpperBound.GetOffset() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetOffset(childLimit);
                childUpperBound.SetOffset(fetchCurrentSum(DataSizeMember, statistics.UncompressedDataSize));
            } else if (entry->LowerBound.HasOffset()) {
                childLowerBound.SetOffset(childLimit);
            }
        }

        // Key
        {
            if (entry->UpperBound.HasKey()) {
                childLowerBound.SetKey(GetMinKey(child));
                if (entry->UpperBound.GetKey() <= childLowerBound.GetKey()) {
                    PopStack();
                    return;
                }
                childUpperBound.SetKey(GetMaxKey(child));
            } else if (entry->LowerBound.HasKey()) {
                childLowerBound.SetKey(GetMinKey(child));
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
                GetStartChildIndex(childChunkList, rowIndex, subtreeStartLimit, subtreeEndLimit);
                break;
            }

            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::JournalChunk: {
                auto* childChunk = child->AsChunk();
                if (!Visitor_->OnChunk(childChunk, rowIndex, subtreeStartLimit, subtreeEndLimit)) {
                    Shutdown();
                    return;
                }
                ++ChunkCount_;
                *visitedChunkCount += 1;
                break;
             }

            default:
                Y_UNREACHABLE();
        }
    }

    void VisitEntryDynamicRoot(TStackEntry* entry)
    {
        auto* chunkList = entry->ChunkList;
        auto* child = chunkList->Children()[entry->ChildIndex];

        // Row Index
        YCHECK(!entry->LowerBound.HasRowIndex() && !entry->UpperBound.HasRowIndex());

        // Offset
        YCHECK(!entry->LowerBound.HasOffset() && !entry->UpperBound.HasOffset());

        TReadLimit childLowerBound;
        TReadLimit childUpperBound;

        auto pivotKey = chunkList->Children()[entry->ChildIndex]->AsChunkList()->GetPivotKey();
        auto nextPivotKey = entry->ChildIndex + 1 < chunkList->Children().size()
            ? chunkList->Children()[entry->ChildIndex + 1]->AsChunkList()->GetPivotKey()
            : MaxKey();

        // Chunk index
        {
            if (entry->UpperBound.HasChunkIndex()) {
                if (entry->UpperBound.GetChunkIndex() <= entry->ChunkCount) {
                    PopStack();
                    return;
                }
                childLowerBound.SetChunkIndex(entry->ChunkCount);
                childUpperBound.SetChunkIndex(entry->ChunkCount + child->AsChunkList()->Children().size());
            } else if (entry->LowerBound.HasChunkIndex()) {
                childLowerBound.SetChunkIndex(entry->ChunkCount);
            }
        }

        // Key
        {
            if (entry->UpperBound.HasKey()) {
                if (entry->UpperBound.GetKey() <= pivotKey) {
                    PopStack();
                    return;
                }
            }

            childLowerBound.SetKey(pivotKey);
            childUpperBound.SetKey(nextPivotKey);
        }

        entry->ChunkCount += child->AsChunkList()->Children().size();
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
        GetStartChildIndex(childChunkList, 0, subtreeStartLimit, subtreeEndLimit);
    }

    void VisitEntryDynamicTablet(TStackEntry* entry, int* visitedChunkCount)
    {
        auto* chunkList = entry->ChunkList;
        auto* child = chunkList->Children()[entry->ChildIndex];

        // Row Index
        YCHECK(!entry->LowerBound.HasRowIndex() && !entry->UpperBound.HasRowIndex());

        // Offset
        YCHECK(!entry->LowerBound.HasOffset() && !entry->UpperBound.HasOffset());

        TReadLimit childLowerBound;
        TReadLimit childUpperBound;

        int childIndex = entry->ChildIndex;
        ++entry->ChildIndex;

        // Chunk index
        {
            i64 childLimit = childIndex;
            if (entry->UpperBound.HasChunkIndex()) {
                if (entry->UpperBound.GetChunkIndex() <= childLimit) {
                    PopStack();
                    return;
                }
                childLowerBound.SetChunkIndex(childLimit);
                childUpperBound.SetChunkIndex(childLimit + 1);
            } else if (entry->LowerBound.HasChunkIndex()) {
                childLowerBound.SetChunkIndex(childLimit);
            }
        }

        // Key
        {
            if (entry->UpperBound.HasKey() || entry->LowerBound.HasKey()) {
                childLowerBound.SetKey(GetMinKey(child));
                childUpperBound.SetKey(GetMaxKey(child));

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

        YCHECK(child->GetType() == EObjectType::Chunk || child->GetType() == EObjectType::ErasureChunk);
        auto* childChunk = child->AsChunk();
        if (!Visitor_->OnChunk(childChunk, 0, subtreeStartLimit, subtreeEndLimit)) {
            Shutdown();
            return;
        }

        ++ChunkCount_;
        *visitedChunkCount += 1;
    }

    void GetStartChildIndex(
        TChunkList* chunkList,
        i64 rowIndex,
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
            case EChunkListKind::OrderedDynamicTablet:
                return GetStartChildIndexTablet(chunkList, rowIndex, lowerBound, upperBound);

            default:
                Y_UNREACHABLE();
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

        auto adjustResult = [&] (i64 TChunkList::TCumulativeStatisticsEntry::* member, i64 limit, i64 total) {
            const auto& cumulativeStatistics = chunkList->CumulativeStatistics();
            if (limit < total) {
                auto it = std::upper_bound(
                    cumulativeStatistics.begin(),
                    cumulativeStatistics.end(),
                    limit,
                    [&] (i64 lhs, const TChunkList::TCumulativeStatisticsEntry& rhs) {
                        return lhs < rhs.*member;
                    });
                result = std::max(result, static_cast<int>(it - cumulativeStatistics.begin()));
            } else {
                result = chunkList->Children().size();
            }
        };

        // Row Index
        if (lowerBound.HasRowIndex()) {
            i64 totalRowCount = statistics.Sealed ? statistics.LogicalRowCount : std::numeric_limits<i64>::max();
            adjustResult(RowCountMember, lowerBound.GetRowIndex(), totalRowCount);
        }

        // Chunk index
        if (lowerBound.HasChunkIndex()) {
            adjustResult(ChunkCountMember, lowerBound.GetChunkIndex(), statistics.LogicalChunkCount);
        }

        // Offset
        if (lowerBound.HasOffset()) {
            adjustResult(DataSizeMember, lowerBound.GetOffset(), statistics.UncompressedDataSize);
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
                [] (const TOwningKey& key, const TChunkTree* chunkTree) {
                    return key > GetMaxKey(chunkTree);
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
            lowerBound,
            upperBound));
    }

    void GetStartChildIndexDynamicRoot(
        TChunkList* chunkList,
        i64 rowIndex,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        int result = 0;

        // Row Index
        YCHECK(!lowerBound.HasRowIndex());

        // Offset
        YCHECK(!lowerBound.HasOffset());

        std::vector<int> childSizes;
        for (const auto* child : chunkList->Children()) {
            childSizes.push_back(child->AsChunkList()->Children().size());
        }

        // Chunk index
        if (lowerBound.HasChunkIndex()) {
            int chunkCount = 0;
            for (const auto* child : chunkList->Children()) {
                chunkCount += child->AsChunkList()->Children().size();
                if (chunkCount > lowerBound.GetChunkIndex()) {
                    break;
                }
                ++result;
            }
        }

        // Key
        if (lowerBound.HasKey()) {
            auto it = std::upper_bound(
                chunkList->Children().begin(),
                chunkList->Children().end(),
                lowerBound.GetKey(),
                [] (const TOwningKey& key, const TChunkTree* chunkTree) {
                    return key < chunkTree->AsChunkList()->GetPivotKey();
                });
            result = std::max(result, static_cast<int>(std::distance(chunkList->Children().begin(), it) - 1));
        }

        int chunkCount = 0;
        for (int index = 0; index < result; ++index) {
            chunkCount += chunkList->Children()[index]->AsChunkList()->Children().size();
        }

        PushStack(TStackEntry(
            chunkList,
            result,
            rowIndex,
            lowerBound,
            upperBound,
            chunkCount));
    }

    void GetStartChildIndexTablet(
        TChunkList* chunkList,
        i64 rowIndex,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        int result = 0;

        // Row Index
        YCHECK(!lowerBound.HasRowIndex());

        // Offset
        YCHECK(!lowerBound.HasOffset());

        // Chunk index
        if (lowerBound.HasChunkIndex()) {
            result = lowerBound.GetChunkIndex();
        }

        // NB: Key is not used here since tablet chunk list is never sorted.

        PushStack(TStackEntry(
            chunkList,
            result,
            rowIndex,
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
            Y_ASSERT(newUpperBound > 0);
            endLimit->SetRowIndex(newUpperBound);
        }

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
            YCHECK(newUpperBound > 0);
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
            Y_ASSERT(newUpperBound > 0);
            endLimit->SetOffset(newUpperBound);
        }

        // Key
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

    NLogging::TLogger Logger;

    TInstant StartInstant_;
    TDuration CpuTime_;
    int ChunkCount_ = 0;
    int ChunkListCount_ = 0;

    std::vector<TStackEntry> Stack_;

public:
    TChunkTreeTraverser(
        IChunkTraverserCallbacksPtr callbacks,
        IChunkVisitorPtr visitor)
        : Callbacks_(std::move(callbacks))
        , Visitor_(std::move(visitor))
        , Logger(ChunkServerLogger)
    { }

    void Run(
        TChunkList* chunkList,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        Logger.AddTag("RootId: %v", chunkList->GetId());

        StartInstant_ = TInstant::Now();

        LOG_DEBUG("Chunk tree traversal started (LowerBound: %v, UpperBound: %v",
            lowerBound,
            upperBound);

        GetStartChildIndex(chunkList, 0, lowerBound, upperBound);

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
    const TReadLimit& upperLimit)
{
    auto traverser = New<TChunkTreeTraverser>(
        std::move(traverserCallbacks),
        std::move(visitor));
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
        if (IsObjectAlive(user)) {
            securityManager->ChargeUser(user, {EUserWorkloadType::Read, 0, time});
        }
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

    virtual bool OnChunk(
        TChunk* chunk,
        i64 /*rowIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/) override
    {
        Chunks_->push_back(chunk);
        return true;
    }

    virtual void OnFinish(const TError& error) override
    {
        YCHECK(error.IsOK());
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

} // namespace NYT::NChunkServer
