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
TKey GetMaxKey(const TChunkTreeRef& ref);

TKey GetMinKey(const TChunk* chunk);
TKey GetMinKey(const TChunkList* chunkList);
TKey GetMinKey(const TChunkTreeRef& ref);

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

TKey GetMaxKey(const TChunkTreeRef& ref)
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

TKey GetMinKey(const TChunkTreeRef& ref)
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
    TKey maxKey = GetMaxKey(ref);
    return CompareKeys(key, maxKey) > 0;
}

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeTraverser
    : public virtual TRefCounted
{
protected:
    struct TStackEntry
    {
        TVersionedChunkListId ChunkListId;
        int ChildPosition;
        TReadLimit LowerBound;
        TReadLimit UpperBound;

        TStackEntry(
            const TVersionedChunkListId& chunkListId,
            int childPosition,
            const TReadLimit& lowerBound,
            const TReadLimit& upperBound)
            : ChunkListId(chunkListId)
            , ChildPosition(childPosition)
            , LowerBound(lowerBound)
            , UpperBound(upperBound)
        { }

        void NextChild()
        {
            ++ChildPosition;
        }
    };

    void DoTraverse()
    {
        auto chunkManager = Bootstrap->GetChunkManager();

        int currentChunkCount = 0;
        while (currentChunkCount < MaxChunksPerAction) {
            if (ChunkTreeStack.empty()) {
                ChunkProcessor->OnComplete();
                return;
            }

            auto& stackEntry = ChunkTreeStack.back();
            // value or error
            auto result = chunkManager->GetVersionedChunkList(stackEntry.ChunkListId);
            if (!result.IsOK()) {
                ChunkProcessor->OnError(TError(
                    ETraversingError::Retriable, 
                    "Couldn't get versioned chunk list") << result);
                return;
            }

            auto* chunkList = result.Value();

            if (stackEntry.ChildPosition == chunkList->Children().size()) {
                ChunkTreeStack.pop_back();
                continue;
            }

            auto child = chunkList->Children()[stackEntry.ChildPosition];
            TReadLimit childLowerBound;
            TReadLimit childUpperBound;

            auto getChildLowerRowIndex = [&] () {
                return stackEntry.ChildPosition == 0 
                    ? 0 
                    : chunkList->RowCountSums()[stackEntry.ChildPosition - 1];
            };

            if (stackEntry.UpperBound.has_row_index()) {
                childLowerBound.set_row_index(getChildLowerRowIndex());

                if (stackEntry.UpperBound.row_index() <= childLowerBound.row_index()) {
                    ChunkTreeStack.pop_back();
                    continue;
                }

                if (stackEntry.ChildPosition == chunkList->Children().size() - 1) {
                    childUpperBound.set_row_index(chunkList->Statistics().RowCount);
                } else {
                    childUpperBound.set_row_index(chunkList->RowCountSums()[stackEntry.ChildPosition]);
                }
            } else if (stackEntry.LowerBound.has_row_index()) {
                childLowerBound.set_row_index(getChildLowerRowIndex());
            }

            if (stackEntry.UpperBound.has_key()) {
                *childLowerBound.mutable_key() = GetMinKey(child);

                if (stackEntry.UpperBound.key() <= childLowerBound.key()) {
                    ChunkTreeStack.pop_back();
                    continue;
                }
                *childUpperBound.mutable_key() = GetMaxKey(child);
            } else if (stackEntry.LowerBound.has_key()) {
                *childLowerBound.mutable_key() = GetMinKey(child);
            }

            stackEntry.NextChild();

            NTableClient::NProto::TReadLimit subtreeStartLimit;
            NTableClient::NProto::TReadLimit subtreeEndLimit;

            GetSubtreeLimits(
                stackEntry, 
                childLowerBound, 
                childUpperBound, 
                &subtreeStartLimit, 
                &subtreeEndLimit);

            switch (child.GetType()) {
            case EObjectType::ChunkList: 
            {
                auto index = GetStartChild(child.AsChunkList(), subtreeStartLimit);
                ChunkTreeStack.push_back(TStackEntry(
                    child.AsChunkList()->GetVersionedId(), 
                    index,
                    subtreeStartLimit,
                    subtreeEndLimit));
                break;
            }

            case EObjectType::Chunk:
            {
                ChunkProcessor->ProcessChunk(child.AsChunk(), subtreeStartLimit, subtreeEndLimit);
                ++currentChunkCount;
                break;
            }

            default:
                YUNREACHABLE();
            }
        }

        // Schedule continuation.
        auto invoker = Bootstrap->GetMetaStateFacade()->GetGuardedInvoker();
        if (!invoker->Invoke(BIND(&TChunkTreeTraverser::DoTraverse, MakeStrong(this)))) {
            ChunkProcessor->OnError(TError(
                ETraversingError::Retriable,
                "Yield error"));
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
    IChunkProcessorPtr ChunkProcessor;
    bool CheckSorted;

    std::vector<TStackEntry> ChunkTreeStack;

public:
    TChunkTreeTraverser(
        TBootstrap* bootstrap,
        IChunkProcessorPtr processor)
        : Bootstrap(bootstrap)
        , ChunkProcessor(processor)
        , CheckSorted(false)
    { }

    virtual void Run(
        const TChunkList* chunkList,
        const TReadLimit& lowerBound,
        const TReadLimit& upperBound)
    {
        bool keyRangeRequest = lowerBound.has_key() || upperBound.has_key();
        if (keyRangeRequest && chunkList->SortedBy().empty()) {
            ChunkProcessor->OnError(TError(
                ETraversingError::Fatal,
                "Key range fetch request to the unsorted chunk list."));
            return;
        }

        int childIndex = GetStartChild(chunkList, lowerBound);

        ChunkTreeStack.push_back(TStackEntry(
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
    IChunkProcessorPtr processor,
    const TChunkList* root,
    const TReadLimit& lowerBound,
    const TReadLimit& upperBound)
{
    auto traverser = New<TChunkTreeTraverser>(bootstrap, processor);
    traverser->Run(root, lowerBound, upperBound);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
