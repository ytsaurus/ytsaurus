#include "stdafx.h"
#include "chunk_tree_traversing.h"
#include "chunk_list.h"
#include "chunk.h"
#include "chunk_manager.h"

#include <ytlib/object_client/public.h>

#include <ytlib/chunk_client/key.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;

namespace {

static const int MaxChunksPerAction = 1000;

////////////////////////////////////////////////////////////////////////////////

TKey GetMaxKey(const TChunk* chunk);
TKey GetMaxKey(const TChunkList* chunkList);
TKey GetMaxKey(const TChunkTree* chunkTree);

TKey GetMinKey(const TChunk* chunk);
TKey GetMinKey(const TChunkList* chunkList);
TKey GetMinKey(const TChunkTree* chunkTree);

TKey GetMaxKey(const TChunk* chunk)
{
    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    return GetKeySuccessor(boundaryKeysExt.end());
}

TKey GetMaxKey(const TChunkList* chunkList)
{
    const auto& children = chunkList->Children();
    YASSERT(!children.empty());
    return GetMaxKey(children.back());
}

TKey GetMaxKey(const TChunkTree* chunkTree)
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

TKey GetMinKey(const TChunk* chunk)
{
    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    return boundaryKeysExt.start();
}

TKey GetMinKey(const TChunkList* chunkList)
{
    const auto& children = chunkList->Children();
    YASSERT(!children.empty());
    return GetMinKey(children.front());
}

TKey GetMinKey(const TChunkTree* chunkTree)
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

bool LessComparer(const TKey& key, const TChunkTree* chunkTree)
{
    auto maxKey = GetMaxKey(chunkTree);
    return CompareKeys(key, maxKey) > 0;
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
        while (visitedChunkCount < MaxChunksPerAction) {
            if (IsStackEmpty()) {
                Shutdown();
                Visitor->OnFinish();
                return;
            }

            auto& entry = PeekStack();
            auto* chunkList = entry.ChunkList;

            if (chunkList->GetVersion() != entry.ChunkListVersion) {
                Shutdown();
                Visitor->OnError(TError(
                    NRpc::EErrorCode::Unavailable,
                    "Optimistic locking failed for chunk list %s",
                    ~ToString(chunkList->GetId())));
                return;
            }

            if (entry.ChildIndex == chunkList->Children().size()) {
                PopStack();
                continue;
            }

            auto* child = chunkList->Children()[entry.ChildIndex];
            TReadLimit childLowerBound;
            TReadLimit childUpperBound;

            auto childLowerRowIndex = 
                (entry.ChildIndex == 0)
                    ? 0
                    : chunkList->RowCountSums()[entry.ChildIndex - 1];

            if (entry.UpperBound.has_row_index()) {
                if (entry.UpperBound.row_index() <= childLowerRowIndex) {
                    PopStack();
                    continue;
                }
                
                childLowerBound.set_row_index(childLowerRowIndex);
                childUpperBound.set_row_index(
                    entry.ChildIndex == chunkList->Children().size() - 1
                        ? chunkList->Statistics().RowCount
                        : chunkList->RowCountSums()[entry.ChildIndex]
                );
            } else if (entry.LowerBound.has_row_index()) {
                childLowerBound.set_row_index(childLowerRowIndex);
            }

            auto childLowerChunkIndex = 
                (entry.ChildIndex == 0)
                    ? 0
                    : chunkList->ChunkCountSums()[entry.ChildIndex - 1];
            if (entry.UpperBound.has_chunk_index()) {
                if (entry.UpperBound.chunk_index() <= childLowerChunkIndex) {
                    PopStack();
                    continue;
                }
                
                childLowerBound.set_chunk_index(childLowerChunkIndex);
                childUpperBound.set_chunk_index(
                    entry.ChildIndex == chunkList->Children().size() - 1
                        ? chunkList->Statistics().ChunkCount
                        : chunkList->ChunkCountSums()[entry.ChildIndex]
                );
            } else if (entry.LowerBound.has_chunk_index()) {
                childLowerBound.set_chunk_index(childLowerChunkIndex);
            }

            auto childLowerOffset =
                (entry.ChildIndex == 0)
                    ? 0
                    : chunkList->DataSizeSums()[entry.ChildIndex - 1];
            if (entry.UpperBound.has_offset()) {
                if (entry.UpperBound.offset() <= childLowerOffset) {
                    PopStack();
                    continue;
                }

                childLowerBound.set_offset(childLowerOffset);
                childUpperBound.set_offset(
                    entry.ChildIndex == chunkList->Children().size() - 1
                        ? chunkList->Statistics().UncompressedDataSize
                        : chunkList->DataSizeSums()[entry.ChildIndex]
                );
            } else if (entry.LowerBound.has_offset()) {
                childLowerBound.set_offset(childLowerOffset);
            }

            if (entry.UpperBound.has_key()) {
                *childLowerBound.mutable_key() = GetMinKey(child);

                if (entry.UpperBound.key() <= childLowerBound.key()) {
                    PopStack();
                    continue;
                }
                *childUpperBound.mutable_key() = GetMaxKey(child);
            } else if (entry.LowerBound.has_key()) {
                *childLowerBound.mutable_key() = GetMinKey(child);
            }

            ++entry.ChildIndex;

            auto rowIndex = entry.RowIndex + childLowerRowIndex;

            TReadLimit subtreeStartLimit;
            TReadLimit subtreeEndLimit;
            GetSubtreeLimits(
                entry,
                childLowerBound,
                childUpperBound,
                &subtreeStartLimit,
                &subtreeEndLimit);

            switch (child->GetType()) {
                case EObjectType::ChunkList: {
                    auto* childChunkList = child->AsChunkList();
                    int index = GetStartChildIndex(childChunkList, subtreeStartLimit);
                    PushStack(TStackEntry(
                        childChunkList,
                        index,
                        rowIndex,
                        subtreeStartLimit,
                        subtreeEndLimit));
                    break;
                }

                case EObjectType::Chunk:
                case EObjectType::ErasureChunk: {
                    auto* childChunk = child->AsChunk();
                    if (!Visitor->OnChunk(childChunk, rowIndex, subtreeStartLimit, subtreeEndLimit)) {
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
        {
            auto invoker = TraverserCallbacks->GetInvoker();
            auto result = invoker->Invoke(BIND(&TChunkTreeTraverser::DoTraverse, MakeStrong(this)));
            if (!result) {
                Shutdown();
                Visitor->OnError(TError(NRpc::EErrorCode::Unavailable, "Yield error"));
            }
        }
    }

    int GetStartChildIndex(
        const TChunkList* chunkList,
        const TReadLimit& lowerBound)
    {
        if (chunkList->Children().empty()) {
            return 0;
        }

        int index = 0;

        if (lowerBound.has_row_index()) {
            if (lowerBound.row_index() >= chunkList->Statistics().RowCount) {
                return chunkList->Children().size();
            }

            auto begin = chunkList->RowCountSums().begin();
            int childIndex = std::upper_bound(
                begin,
                chunkList->RowCountSums().end(),
                lowerBound.row_index()) - begin;
            index = std::max(index, childIndex);
            YCHECK(index < chunkList->Children().size());
        }

        if (lowerBound.has_chunk_index()) {
            if (lowerBound.chunk_index() >= chunkList->Statistics().ChunkCount) {
                return chunkList->Children().size();
            }

            auto begin = chunkList->ChunkCountSums().begin();
            int childIndex = std::upper_bound(
                begin,
                chunkList->ChunkCountSums().end(),
                lowerBound.chunk_index()) - begin;
            index = std::max(index, childIndex);
            YCHECK(index < chunkList->Children().size());
        }

        if (lowerBound.has_offset()) {
            if (lowerBound.offset() >= chunkList->Statistics().UncompressedDataSize) {
                return chunkList->Children().size();
            }

            auto begin = chunkList->DataSizeSums().begin();
            int childIndex = std::upper_bound(
                begin,
                chunkList->DataSizeSums().end(),
                lowerBound.offset()) - begin;
            index = std::max(index, childIndex);
            YCHECK(index < chunkList->Children().size());
        }

        if (lowerBound.has_key()) {
            typedef decltype(chunkList->Children().begin()) TChildIter;
            std::reverse_iterator<TChildIter> rbegin(chunkList->Children().end());
            std::reverse_iterator<TChildIter> rend(chunkList->Children().begin());

            int childIndex = rend - std::upper_bound(
                rbegin,
                rend,
                lowerBound.key(),
                LessComparer);
            index = std::max(index, childIndex);
            YCHECK(index <= chunkList->Children().size());
        }

        return index;
    }

    void GetSubtreeLimits(
        const TStackEntry& stackEntry,
        const TReadLimit& childLowerBound,
        const TReadLimit& childUpperBound,
        TReadLimit* startLimit,
        TReadLimit* endLimit)
    {
        if (stackEntry.LowerBound.has_row_index()) {
            i64 newLowerBound = stackEntry.LowerBound.row_index() - childLowerBound.row_index();
            if (newLowerBound > 0) {
                startLimit->set_row_index(newLowerBound);
            }
        }

        if (stackEntry.UpperBound.has_row_index() &&
            stackEntry.UpperBound.row_index() < childUpperBound.row_index())
        {
            i64 newUpperBound = stackEntry.UpperBound.row_index() - childLowerBound.row_index();
            YCHECK(newUpperBound > 0);
            endLimit->set_row_index(newUpperBound);
        }

        if (stackEntry.LowerBound.has_chunk_index()) {
            int newLowerBound = stackEntry.LowerBound.chunk_index() - childLowerBound.chunk_index();
            if (newLowerBound > 0) {
                startLimit->set_chunk_index(newLowerBound);
            }
        }

        if (stackEntry.UpperBound.has_chunk_index() &&
            stackEntry.UpperBound.chunk_index() < childUpperBound.chunk_index())
        {
            int newUpperBound = stackEntry.UpperBound.chunk_index() - childLowerBound.chunk_index();
            YCHECK(newUpperBound > 0);
            endLimit->set_chunk_index(newUpperBound);
        }

        if (stackEntry.LowerBound.has_offset()) {
            i64 newLowerBound = stackEntry.LowerBound.offset() - childLowerBound.offset();
            if (newLowerBound > 0) {
                startLimit->set_offset(newLowerBound);
            }
        }

        if (stackEntry.UpperBound.has_offset() &&
            stackEntry.UpperBound.offset() < childUpperBound.offset())
        {
            i64 newUpperBound = stackEntry.UpperBound.offset() - childLowerBound.offset();
            YCHECK(newUpperBound > 0);
            endLimit->set_offset(newUpperBound);
        }

        if (stackEntry.LowerBound.has_key() &&
            stackEntry.LowerBound.key() > childLowerBound.key())
        {
            *startLimit->mutable_key() = stackEntry.LowerBound.key();
        }

        if (stackEntry.UpperBound.has_key() &&
            stackEntry.UpperBound.key() < childUpperBound.key())
        {
            *endLimit->mutable_key() = stackEntry.UpperBound.key();
        }
    }

    bool IsStackEmpty()
    {
        return Stack.empty();
    }

    void PushStack(const TStackEntry& newEntry)
    {
        TraverserCallbacks->OnPush(newEntry.ChunkList);
        Stack.push_back(newEntry);
    }

    TStackEntry& PeekStack()
    {
        return Stack.back();
    }

    void PopStack()
    {
        auto& entry = Stack.back();
        TraverserCallbacks->OnPop(entry.ChunkList);
        Stack.pop_back();
    }

    void Shutdown()
    {
        std::vector<TChunkTree*> nodes;
        FOREACH (const auto& entry, Stack) {
            nodes.push_back(entry.ChunkList);
        }
        TraverserCallbacks->OnShutdown(nodes);
        Stack.clear();
    }

    IChunkTraverserCallbacksPtr TraverserCallbacks;
    IChunkVisitorPtr Visitor;

    std::vector<TStackEntry> Stack;

public:
    TChunkTreeTraverser(
        IChunkTraverserCallbacksPtr traverserCallbacks,
        IChunkVisitorPtr visitor)
        : TraverserCallbacks(traverserCallbacks)
        , Visitor(visitor)
    { }

    void Run(
        TChunkList* chunkList,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        bool keyRangeRequest = lowerBound.has_key() || upperBound.has_key();
        if (keyRangeRequest && chunkList->SortedBy().empty()) {
            Visitor->OnError(TError("Cannot fetch a keyed range of unsorted table"));
            return;
        }

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

////////////////////////////////////////////////////////////////////////////////

class TChunkTraverserCallbacks
    : public IChunkTraverserCallbacks
{
public:
    explicit TChunkTraverserCallbacks(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual IInvokerPtr GetInvoker() const override
    {
        return Bootstrap->GetMetaStateFacade()->GetGuardedInvoker();
    }

    virtual void OnPop(TChunkTree* node) override
    {
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->WeakUnrefObject(node);
    }

    virtual void OnPush(TChunkTree* node) override
    {
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->WeakRefObject(node);
    }

    virtual void OnShutdown(const std::vector<TChunkTree*>& nodes) override
    {
        auto objectManager = Bootstrap->GetObjectManager();
        FOREACH (const auto& node, nodes) {
            objectManager->WeakUnrefObject(node);
        }
    }

private:
    NCellMaster::TBootstrap* Bootstrap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IChunkTraverserCallbacksPtr CreateTraverserCallbacks(
    NCellMaster::TBootstrap* bootstrap)
{
    return New<TChunkTraverserCallbacks>(bootstrap);
}

void TraverseChunkTree(
    IChunkTraverserCallbacksPtr traverserCallbacks,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const TReadLimit& lowerBound,
    const TReadLimit& upperBound)
{
    auto traverser = New<TChunkTreeTraverser>(traverserCallbacks, visitor);
    traverser->Run(root, lowerBound, upperBound);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
