#include "stdafx.h"
#include "chunk_tree_traversing.h"
#include "chunk_list.h"
#include "chunk.h"
#include "chunk_manager.h"

#include <ytlib/object_client/public.h>

#include <ytlib/table_client/key.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NTableClient;

using NTableClient::NProto::TKey;
using NTableClient::NProto::TReadLimit;

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
    auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(
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
        case EObjectType::Chunk: {
            return GetMaxKey(chunkTree->AsChunk());
        }
        case EObjectType::ChunkList: {
            return GetMaxKey(chunkTree->AsChunkList());
        }
        default:
            YUNREACHABLE();
    }
}

TKey GetMinKey(const TChunk* chunk)
{
    auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(
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
        case EObjectType::Chunk: {
            return GetMinKey(chunkTree->AsChunk());
        }
        case EObjectType::ChunkList: {
            return GetMinKey(chunkTree->AsChunkList());
        }
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
        TReadLimit LowerBound;
        TReadLimit UpperBound;

        TStackEntry(
            TChunkList* chunkList,
            int childIndex,
            const TReadLimit& lowerBound,
            const TReadLimit& upperBound)
            : ChunkList(chunkList)
            , ChunkListVersion(chunkList->GetVersion())
            , ChildIndex(childIndex)
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

            auto getChildLowerRowIndex = [&] () {
                return entry.ChildIndex == 0
                    ? 0
                    : chunkList->RowCountSums()[entry.ChildIndex - 1];
            };

            if (entry.UpperBound.has_row_index()) {
                childLowerBound.set_row_index(getChildLowerRowIndex());

                if (entry.UpperBound.row_index() <= childLowerBound.row_index()) {
                    PopStack();
                    continue;
                }

                if (entry.ChildIndex == chunkList->Children().size() - 1) {
                    childUpperBound.set_row_index(chunkList->Statistics().RowCount);
                } else {
                    childUpperBound.set_row_index(chunkList->RowCountSums()[entry.ChildIndex]);
                }
            } else if (entry.LowerBound.has_row_index()) {
                childLowerBound.set_row_index(getChildLowerRowIndex());
            }

            if (entry.UpperBound.has_chunk_index()) {
                childLowerBound.set_chunk_index(getChildLowerRowIndex());

                if (entry.UpperBound.chunk_index() <= childLowerBound.chunk_index()) {
                    PopStack();
                    continue;
                }

                if (entry.ChildIndex == chunkList->Children().size() - 1) {
                    childUpperBound.set_chunk_index(chunkList->Statistics().RowCount);
                } else {
                    childUpperBound.set_chunk_index(chunkList->RowCountSums()[entry.ChildIndex]);
                }
            } else if (entry.LowerBound.has_chunk_index()) {
                childLowerBound.set_chunk_index(getChildLowerRowIndex());
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

            NTableClient::NProto::TReadLimit subtreeStartLimit;
            NTableClient::NProto::TReadLimit subtreeEndLimit;
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
                        subtreeStartLimit,
                        subtreeEndLimit));
                    break;
                }

                case EObjectType::Chunk: {
                    auto* childChunk = child->AsChunk();
                    if (!Visitor->OnChunk(childChunk, subtreeStartLimit, subtreeEndLimit)) {
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
            auto invoker = Bootstrap->GetMetaStateFacade()->GetGuardedInvoker();
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
            auto begin = chunkList->RowCountSums().begin();
            int childIndex = std::upper_bound(
                begin,
                chunkList->RowCountSums().end(),
                lowerBound.row_index()) - begin;
            index = std::max(index, childIndex);
            YCHECK(index < chunkList->Children().size());
        }

        if (lowerBound.has_chunk_index()) {
            auto begin = chunkList->ChunkCountSums().begin();
            int childIndex = std::upper_bound(
                begin,
                chunkList->ChunkCountSums().end(),
                lowerBound.chunk_index()) - begin;
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
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->LockObject(newEntry.ChunkList);
        Stack.push_back(newEntry);
    }

    TStackEntry& PeekStack()
    {
        return Stack.back();
    }

    void PopStack()
    {
        auto& entry = Stack.back();
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->UnlockObject(entry.ChunkList);
        Stack.pop_back();
    }

    void Shutdown()
    {
        auto objectManager = Bootstrap->GetObjectManager();
        FOREACH (const auto& entry, Stack) {
            objectManager->UnlockObject(entry.ChunkList);
        }
        Stack.clear();
    }

    TBootstrap* Bootstrap;
    IChunkVisitorPtr Visitor;

    std::vector<TStackEntry> Stack;

public:
    TChunkTreeTraverser(
        TBootstrap* bootstrap,
        IChunkVisitorPtr visitor)
        : Bootstrap(bootstrap)
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
            lowerBound,
            upperBound));

        // Run first iteration synchronously.
        DoTraverse();
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TraverseChunkTree(
    NCellMaster::TBootstrap* bootstrap,
    IChunkVisitorPtr visitor,
    TChunkList* root,
    const TReadLimit& lowerBound,
    const TReadLimit& upperBound)
{
    auto traverser = New<TChunkTreeTraverser>(bootstrap, visitor);
    traverser->Run(root, lowerBound, upperBound);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
