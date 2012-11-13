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
TKey GetMaxKey(TChunkTreeRef ref);

TKey GetMinKey(const TChunk* chunk);
TKey GetMinKey(const TChunkList* chunkList);
TKey GetMinKey(TChunkTreeRef ref);

TKey GetMaxKey(const TChunk* chunk)
{
    auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    return GetSuccessorKey(boundaryKeysExt.end());
}

TKey GetMaxKey(const TChunkList* chunkList)
{
    const auto& children = chunkList->Children();
    YASSERT(!children.empty());
    return GetMaxKey(children.back());
}

TKey GetMaxKey(TChunkTreeRef ref)
{
    switch (ref.GetType()) {
        case EObjectType::Chunk: {
            return GetMaxKey(ref.AsChunk());
        }
        case EObjectType::ChunkList: {
            return GetMaxKey(ref.AsChunkList());
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

TKey GetMinKey(TChunkTreeRef ref)
{
    switch (ref.GetType()) {
        case EObjectType::Chunk: {
            return GetMinKey(ref.AsChunk());
        }
        case EObjectType::ChunkList: {
            return GetMinKey(ref.AsChunkList());
        }
        default:
            YUNREACHABLE();
    }
}

bool LessComparer(const TKey& key, TChunkTreeRef ref)
{
    auto maxKey = GetMaxKey(ref);
    return CompareKeys(key, maxKey) > 0;
}

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeTraverser
    : public TRefCounted
{
protected:
    struct TStackEntry
    {
        TVersionedChunkListId ChunkListId;
        int ChildIndex;
        TReadLimit LowerBound;
        TReadLimit UpperBound;

        TStackEntry(
            const TVersionedChunkListId& chunkListId,
            int childIndex,
            const TReadLimit& lowerBound,
            const TReadLimit& upperBound)
            : ChunkListId(chunkListId)
            , ChildIndex(childIndex)
            , LowerBound(lowerBound)
            , UpperBound(upperBound)
        { }
    };

    void DoTraverse()
    {
        auto chunkManager = Bootstrap->GetChunkManager();

        int visitedChunkCount = 0;
        while (visitedChunkCount < MaxChunksPerAction) {
            if (Stack.empty()) {
                Visitor->OnFinish();
                return;
            }

            auto& stackEntry = Stack.back();
            const auto& versionedId = stackEntry.ChunkListId;
            auto* chunkList = chunkManager->FindChunkList(versionedId.Id);
            if (!chunkList || !chunkList->IsAlive()) {
                Visitor->OnError(TError(
                    ETraversingError::Retriable, 
                    "Missing chunk list: %s",
                    ~ToString(versionedId.Id)));
                return;
            }
            if (chunkList->GetVersion() != versionedId.Version) {
                Visitor->OnError(TError(
                    ETraversingError::Retriable, 
                    "Chunk list %s version mismatch: expected %d but found %d",
                    ~ToString(versionedId.Id),
                    versionedId.Version,
                    chunkList->GetVersion()));
                return;
            }

            if (stackEntry.ChildIndex == chunkList->Children().size()) {
                Stack.pop_back();
                continue;
            }

            auto child = chunkList->Children()[stackEntry.ChildIndex];
            TReadLimit childLowerBound;
            TReadLimit childUpperBound;

            auto getChildLowerRowIndex = [&] () {
                return stackEntry.ChildIndex == 0 
                    ? 0 
                    : chunkList->RowCountSums()[stackEntry.ChildIndex - 1];
            };

            if (stackEntry.UpperBound.has_row_index()) {
                childLowerBound.set_row_index(getChildLowerRowIndex());

                if (stackEntry.UpperBound.row_index() <= childLowerBound.row_index()) {
                    Stack.pop_back();
                    continue;
                }

                if (stackEntry.ChildIndex == chunkList->Children().size() - 1) {
                    childUpperBound.set_row_index(chunkList->Statistics().RowCount);
                } else {
                    childUpperBound.set_row_index(chunkList->RowCountSums()[stackEntry.ChildIndex]);
                }
            } else if (stackEntry.LowerBound.has_row_index()) {
                childLowerBound.set_row_index(getChildLowerRowIndex());
            }

            if (stackEntry.UpperBound.has_key()) {
                *childLowerBound.mutable_key() = GetMinKey(child);

                if (stackEntry.UpperBound.key() <= childLowerBound.key()) {
                    Stack.pop_back();
                    continue;
                }
                *childUpperBound.mutable_key() = GetMaxKey(child);
            } else if (stackEntry.LowerBound.has_key()) {
                *childLowerBound.mutable_key() = GetMinKey(child);
            }

            ++stackEntry.ChildIndex;

            NTableClient::NProto::TReadLimit subtreeStartLimit;
            NTableClient::NProto::TReadLimit subtreeEndLimit;
            GetSubtreeLimits(
                stackEntry, 
                childLowerBound, 
                childUpperBound, 
                &subtreeStartLimit, 
                &subtreeEndLimit);

            switch (child.GetType()) {
                case EObjectType::ChunkList: {
                    auto index = GetStartChild(child.AsChunkList(), subtreeStartLimit);
                    Stack.push_back(TStackEntry(
                        child.AsChunkList()->GetVersionedId(), 
                        index,
                        subtreeStartLimit,
                        subtreeEndLimit));
                    break;
                }

                case EObjectType::Chunk:
                    Visitor->OnChunk(child.AsChunk(), subtreeStartLimit, subtreeEndLimit);
                    ++visitedChunkCount;
                    break;

                default:
                    YUNREACHABLE();
            }
        }

        // Schedule continuation.
        {
            auto invoker = Bootstrap->GetMetaStateFacade()->GetGuardedInvoker();
            auto result = invoker->Invoke(BIND(&TChunkTreeTraverser::DoTraverse, MakeStrong(this)));
            if (!result) {
                Visitor->OnError(TError(
                    ETraversingError::Retriable,
                    "Yield error"));
            }
        }
    }

    int GetStartChild(
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
        const TChunkList* chunkList,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        bool keyRangeRequest = lowerBound.has_key() || upperBound.has_key();
        if (keyRangeRequest && chunkList->SortedBy().empty()) {
            Visitor->OnError(TError(
                ETraversingError::Fatal,
                "Cannot fetch a keyed range of unsorted table"));
            return;
        }

        int childIndex = GetStartChild(chunkList, lowerBound);

        Stack.push_back(TStackEntry(
            chunkList->GetVersionedId(),
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
    const TChunkList* root,
    const TReadLimit& lowerBound,
    const TReadLimit& upperBound)
{
    auto traverser = New<TChunkTreeTraverser>(bootstrap, visitor);
    traverser->Run(root, lowerBound, upperBound);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
