#include "stdafx.h"
#include "chunk_tree_traversing.h"
#include "chunk_list.h"
#include "chunk.h"
#include "chunk_manager.h"

#include <core/misc/singleton.h>

#include <ytlib/object_client/public.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NTableClient::NProto;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static const int MaxChunksPerAction = 1000;

////////////////////////////////////////////////////////////////////////////////

TOwningKey GetMaxKey(const TChunk* chunk);
TOwningKey GetMaxKey(const TChunkList* chunkList);
TOwningKey GetMaxKey(const TChunkTree* chunkTree);

TOwningKey GetMinKey(const TChunk* chunk);
TOwningKey GetMinKey(const TChunkList* chunkList);
TOwningKey GetMinKey(const TChunkTree* chunkTree);

TOwningKey GetMaxKey(const TChunk* chunk)
{
    // XXX(psushin): check chunk version.
    auto boundaryKeysExt = GetProtoExtension<TOldBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    TOwningKey key;
    FromProto(&key, boundaryKeysExt.end());
    return GetKeySuccessor(key.Get());
}

TOwningKey GetMaxKey(const TChunkList* chunkList)
{
    const auto& children = chunkList->Children();
    YASSERT(!children.empty());
    return GetMaxKey(children.back());
}

TOwningKey GetMaxKey(const TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return GetMaxKey(chunkTree->AsChunk());

        case EObjectType::ChunkList:
            return GetMaxKey(chunkTree->AsChunkList());

        default:
            YUNREACHABLE();
    }
}

TOwningKey GetMinKey(const TChunk* chunk)
{
    auto boundaryKeysExt = GetProtoExtension<TOldBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    TOwningKey key;
    FromProto(&key, boundaryKeysExt.start());
    return key;
}

TOwningKey GetMinKey(const TChunkList* chunkList)
{
    const auto& children = chunkList->Children();
    YASSERT(!children.empty());
    return GetMinKey(children.front());
}

TOwningKey GetMinKey(const TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return GetMinKey(chunkTree->AsChunk());

        case EObjectType::ChunkList:
            return GetMinKey(chunkTree->AsChunkList());

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

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

        TStackEntry(
            TChunkList* chunkList,
            int childIndex,
            i64 rowIndex,
            const TReadLimit& lowerBound,
            const TReadLimit& upperBound)
            : ChunkList(chunkList)
            , ChunkListVersion(chunkList->GetVersion())
            , ChildIndex(childIndex)
            , RowIndex(rowIndex)
            , LowerBound(lowerBound)
            , UpperBound(upperBound)
        { }
    };

    void DoTraverse()
    {
        int visitedChunkCount = 0;
        while (visitedChunkCount < MaxChunksPerAction || !Callbacks_->IsPreemptable()) {
            if (IsStackEmpty()) {
                Shutdown();
                Visitor_->OnFinish();
                return;
            }

            auto& entry = PeekStack();
            auto* chunkList = entry.ChunkList;

            if (chunkList->GetVersion() != entry.ChunkListVersion) {
                Shutdown();
                Visitor_->OnError(TError(
                    NRpc::EErrorCode::Unavailable,
                    "Optimistic locking failed for chunk list %s",
                    ~ToString(chunkList->GetId())));
                return;
            }

            if (entry.ChildIndex == chunkList->Children().size()) {
                PopStack();
                continue;
            }

            const auto& statistics = chunkList->Statistics();
            auto* child = chunkList->Children()[entry.ChildIndex];

            TReadLimit childLowerBound;
            TReadLimit childUpperBound;

            auto fetchPrevSum = [&] (const std::vector<i64>& sums) -> i64 {
                return entry.ChildIndex == 0
                    ? 0
                    : sums[entry.ChildIndex - 1];
            };

            auto fetchCurrentSum = [&] (const std::vector<i64>& sums, i64 fallback) {
                return entry.ChildIndex == chunkList->Children().size() - 1
                    ? fallback
                    : sums[entry.ChildIndex];
            };

            // Row index
            i64 rowIndex;
            {
                i64 childLimit = fetchPrevSum(chunkList->RowCountSums());
                rowIndex =  entry.RowIndex + childLimit;
                if (entry.UpperBound.HasRowIndex()) {
                    if (entry.UpperBound.GetRowIndex() <= childLimit) {
                        PopStack();
                        continue;
                    }
                    childLowerBound.SetRowIndex(childLimit);
                    i64 totalRowCount = statistics.Sealed ? statistics.RowCount : std::numeric_limits<i64>::max();
                    childUpperBound.SetRowIndex(fetchCurrentSum(chunkList->RowCountSums(), totalRowCount));
                } else if (entry.LowerBound.HasRowIndex()) {
                    childLowerBound.SetRowIndex(childLimit);
                }
            }

            // Chunk index
            {
                i64 childLimit = fetchPrevSum(chunkList->ChunkCountSums());
                if (entry.UpperBound.HasChunkIndex()) {
                    if (entry.UpperBound.GetChunkIndex() <= childLimit) {
                        PopStack();
                        continue;
                    }
                    childLowerBound.SetChunkIndex(childLimit);
                    childUpperBound.SetChunkIndex(fetchCurrentSum(chunkList->ChunkCountSums(), statistics.ChunkCount));
                } else if (entry.LowerBound.HasChunkIndex()) {
                    childLowerBound.SetChunkIndex(childLimit);
                }
            }

            // Offset
            {
                i64 childLimit = fetchPrevSum(chunkList->DataSizeSums());
                if (entry.UpperBound.HasOffset()) {
                    if (entry.UpperBound.GetOffset() <= childLimit) {
                        PopStack();
                        continue;
                    }
                    childLowerBound.SetOffset(childLimit);
                    childUpperBound.SetOffset(fetchCurrentSum(chunkList->DataSizeSums(), statistics.UncompressedDataSize));
                } else if (entry.LowerBound.HasOffset()) {
                    childLowerBound.SetOffset(childLimit);
                }
            }

            // Key
            {
                if (entry.UpperBound.HasKey()) {
                    childLowerBound.SetKey(GetMinKey(child));
                    if (entry.UpperBound.GetKey() <= childLowerBound.GetKey()) {
                        PopStack();
                        continue;
                    }
                    childUpperBound.SetKey(GetMaxKey(child));
                } else if (entry.LowerBound.HasKey()) {
                    childLowerBound.SetKey(GetMinKey(child));
                }
            }

            ++entry.ChildIndex;

            TReadLimit subtreeStartLimit;
            TReadLimit subtreeEndLimit;
            GetInducedSubtreeLimits(
                entry,
                childLowerBound,
                childUpperBound,
                &subtreeStartLimit,
                &subtreeEndLimit);

            switch (child->GetType()) {
                case EObjectType::ChunkList: {
                    auto* childChunkList = child->AsChunkList();
                    int childIndex = GetStartChildIndex(childChunkList, subtreeStartLimit);
                    PushStack(TStackEntry(
                        childChunkList,
                        childIndex,
                        rowIndex,
                        subtreeStartLimit,
                        subtreeEndLimit));
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
                    ++visitedChunkCount;
                    break;
                 }

                default:
                    YUNREACHABLE();
            }
        }

        // Schedule continuation.
        Callbacks_
            ->GetInvoker()
            ->Invoke(BIND(&TChunkTreeTraverser::DoTraverse, MakeStrong(this)));
    }

    static int GetStartChildIndex(
        const TChunkList* chunkList,
        const TReadLimit& lowerBound)
    {
        if (chunkList->Children().empty()) {
            return 0;
        }

        int result = 0;
        const auto& statistics = chunkList->Statistics();

        auto adjustResult = [&] (i64 limit, i64 total, const std::vector<i64>& sums) {
            if (limit < total) {
                auto it = std::upper_bound(
                    sums.begin(),
                    sums.end(),
                    limit);
                result = std::max(result, static_cast<int>(it - sums.begin()));
            } else {
                result = chunkList->Children().size();
            }
        };

        // Row Index
        if (lowerBound.HasRowIndex()) {
            i64 totalRowCount = statistics.Sealed ? statistics.RowCount : std::numeric_limits<i64>::max();
            adjustResult(lowerBound.GetRowIndex(), totalRowCount, chunkList->RowCountSums());
        }

        // Chunk index
        if (lowerBound.HasChunkIndex()) {
            adjustResult(lowerBound.GetChunkIndex(), statistics.ChunkCount, chunkList->ChunkCountSums());
        }

        // Offset
        if (lowerBound.HasOffset()) {
            adjustResult(lowerBound.GetOffset(), statistics.UncompressedDataSize, chunkList->DataSizeSums());
        }

        // Key
        if (lowerBound.HasKey()) {
            typedef std::vector<TChunkTree*>::const_iterator TChildrenIterator;
            std::reverse_iterator<TChildrenIterator> rbegin(chunkList->Children().end());
            std::reverse_iterator<TChildrenIterator> rend(chunkList->Children().begin());
            auto it = std::upper_bound(
                rbegin,
                rend,
                lowerBound.GetKey(),
                [] (const TOwningKey& key, const TChunkTree* chunkTree) {
                    return key > GetMaxKey(chunkTree);
                });
            result = std::max(result, static_cast<int>(rend - it));
        }

        return result;
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
            YASSERT(newUpperBound > 0);
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
            YASSERT(newUpperBound > 0);
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
            YASSERT(newUpperBound > 0);
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

    IChunkTraverserCallbacksPtr Callbacks_;
    IChunkVisitorPtr Visitor_;

    std::vector<TStackEntry> Stack_;

public:
    TChunkTreeTraverser(
        IChunkTraverserCallbacksPtr callbacks,
        IChunkVisitorPtr visitor)
        : Callbacks_(callbacks)
        , Visitor_(visitor)
    { }

    void Run(
        TChunkList* chunkList,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        int childIndex = GetStartChildIndex(chunkList, lowerBound);
        PushStack(TStackEntry(
            chunkList,
            childIndex,
            0,
            lowerBound,
            upperBound));

        // Run first iteration synchronously.
        DoTraverse();
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
    explicit TPreemptableChunkTraverserCallbacks(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual bool IsPreemptable() const override
    {
        return true;
    }

    virtual IInvokerPtr GetInvoker() const override
    {
        return Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker();
    }

    virtual void OnPop(TChunkTree* node) override
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->WeakUnrefObject(node);
    }

    virtual void OnPush(TChunkTree* node) override
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->WeakRefObject(node);
    }

    virtual void OnShutdown(const std::vector<TChunkTree*>& nodes) override
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        for (const auto& node : nodes) {
            objectManager->WeakUnrefObject(node);
        }
    }

private:
    NCellMaster::TBootstrap* Bootstrap_;

};

IChunkTraverserCallbacksPtr CreatePreemptableChunkTraverserCallbacks(
    NCellMaster::TBootstrap* bootstrap)
{
    return New<TPreemptableChunkTraverserCallbacks>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class TNonpreemptableChunkTraverserCallbacks
    : public IChunkTraverserCallbacks
{
public:
    virtual bool IsPreemptable() const override
    {
        return false;
    }

    virtual IInvokerPtr GetInvoker() const override
    {
        return GetSyncInvoker();
    }

    virtual void OnPop(TChunkTree* /*node*/) override
    { }

    virtual void OnPush(TChunkTree* /*node*/) override
    { }

    virtual void OnShutdown(const std::vector<TChunkTree*>& /*nodes*/) override
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

    virtual void OnError(const TError& /*error*/) override
    {
        YUNREACHABLE();
    }

    virtual void OnFinish() override
    { }

private:
    std::vector<TChunk*>* Chunks_;

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

} // namespace NChunkServer
} // namespace NYT
