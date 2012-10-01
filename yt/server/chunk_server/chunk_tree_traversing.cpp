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

namespace {

static const int MaxChunksPerAction = 1000;

////////////////////////////////////////////////////////////////////////////////

template <class TBoundary>
class TChunkTreeTraverserBase
    : public virtual TRefCounted
{
protected:
    struct TStackEntry
    {
        TVersionedChunkListId ChunkListId;
        int ChildPosition;
        TBoundary LowerBound;
        TNullable<TBoundary> UpperBound;

        TStackEntry(
            const TVersionedChunkListId& chunkListId,
            int childPosition,
            const TBoundary& lowerBound,
            TNullable<TBoundary> upperBound)
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

    TChunkTreeTraverserBase(
        TBootstrap* bootstrap,
        IChunkProcessorPtr processor)
        : Bootstrap(bootstrap)
        , ChunkProcessor(processor)
    { }

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
                // may be static cast here
                ChunkProcessor->OnError(result);
                return;
            }

            auto* chunkList = result.Value();

            if (stackEntry.ChildPosition == chunkList->Children().size()) {
                ChunkTreeStack.pop_back();
                continue;
            }

            auto child = chunkList->Children()[stackEntry.ChildPosition];
            auto childLowerBound = GetChildLowerBound(chunkList, stackEntry, child);

            if (stackEntry.UpperBound && *stackEntry.UpperBound <= childLowerBound) {
                ChunkTreeStack.pop_back();
                continue;
            }

            stackEntry.NextChild();

            switch (child.GetType()) {
            case EObjectType::ChunkList: 
            {
                ProcessChildChunkList(child.AsChunkList(), stackEntry, childLowerBound);
                break;
            }

            case EObjectType::Chunk:
            {
                ProcessChildChunk(child.AsChunk(), stackEntry, childLowerBound);
                ++currentChunkCount;
                break;
            }

            default:
                YUNREACHABLE();
            }
        }

        // Schedule continuation.
        auto result = Bootstrap->GetMetaStateFacade()->GetGuardedEpochInvoker()->Invoke(BIND(
            &TChunkTreeTraverserBase<TBoundary>::DoTraverse,
            MakeStrong(this)));

        if (!result) {
            ChunkProcessor->OnError(TError("Unable to schedule continuation through GuardedEpochInvoker"));
        }
    }

    virtual int GetStartChild(
        const TChunkList* chunkList,
        const TBoundary& lowerBound) = 0;

    virtual TBoundary GetChildLowerBound(
        const TChunkList* chunkList, 
        const TStackEntry& stackEntry,
        const TChunkTreeRef& child) = 0;

    virtual void ProcessChildChunkList(
        const TChunkList* chunkList,
        const TStackEntry& stackEntry,
        const TBoundary& childLowerBound) = 0;

    virtual void ProcessChildChunk(
        const TChunk* chunk,
        const TStackEntry& stackEntry,
        const TBoundary& childLowerBound) = 0;


    TBootstrap* Bootstrap;
    IChunkProcessorPtr ChunkProcessor;

    std::vector<TStackEntry> ChunkTreeStack;

public:
    virtual void Run(
        const TChunkList* chunkList,
        const TBoundary& lowerBound,
        TNullable<TBoundary>& upperBound)
    {
        int childIndex = GetStartChild(chunkList, lowerBound);

        ChunkTreeStack.push_back(TStackEntry(
            chunkList->GetVersionedId(),
            childIndex,
            lowerBound,
            upperBound));
        // Run first round synchronously.
        DoTraverse();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TIndexRangeChunkTreeTraverser
    : public TChunkTreeTraverserBase<i64>
{
public:
    TIndexRangeChunkTreeTraverser(
        TBootstrap* bootstrap,
        IChunkProcessorPtr processor)
        : TChunkTreeTraverserBase<i64>(bootstrap, processor)
    { }

    virtual void Run(
        const TChunkList* chunkList,
        const i64& lowerBound,
        TNullable<i64>& upperBound) override
    {
        YCHECK(lowerBound >= 0);
        YCHECK(!upperBound || *upperBound >= lowerBound);

        TChunkTreeTraverserBase<i64>::Run(
            chunkList, 
            lowerBound, 
            upperBound);
    }

private:
    virtual int GetStartChild(const TChunkList* chunkList, const i64& lowerBound) override
    {
        if (chunkList->Children().empty()) {
            return 0;
        }

        auto begin = chunkList->RowCountSums().begin();
        int index =
            std::upper_bound(
                begin,
                chunkList->RowCountSums().end(),
                lowerBound) - begin;
        YCHECK(index < chunkList->Children().size());
        return index;
    }

    virtual i64 GetChildLowerBound(
        const TChunkList* chunkList, 
        const TStackEntry& stackEntry,
        const TChunkTreeRef& child) override
    {
        UNUSED(child);
        return stackEntry.ChildPosition == 0 
            ? 0 
            : chunkList->RowCountSums()[stackEntry.ChildPosition - 1];
    }

    virtual void ProcessChildChunkList(
        const TChunkList* chunkList,
        const TStackEntry& stackEntry,
        const i64& childLowerBound) override
    {
        i64 newLowerBound = stackEntry.LowerBound - childLowerBound;
        auto index = GetStartChild(chunkList, newLowerBound);
        ChunkTreeStack.push_back(TStackEntry(
            chunkList->GetVersionedId(), 
            index,
            newLowerBound,
            stackEntry.UpperBound 
                ? MakeNullable(*stackEntry.UpperBound - childLowerBound) 
                : Null));
    }

    virtual void ProcessChildChunk(
        const TChunk* chunk,
        const TStackEntry& stackEntry,
        const i64& childLowerBound) override
    {
        NTableClient::NProto::TReadLimit startLimit;
        NTableClient::NProto::TReadLimit endLimit;

        i64 childUpperBound = childLowerBound + chunk->GetStatistics().RowCount;

        YASSERT(stackEntry.LowerBound < childUpperBound);
        if (stackEntry.LowerBound > childLowerBound) {
            startLimit.set_row_index(stackEntry.LowerBound - childLowerBound);
        }

        if (stackEntry.UpperBound && *stackEntry.UpperBound < childUpperBound) {
            endLimit.set_row_index(*stackEntry.UpperBound - childLowerBound);
        }

        ChunkProcessor->ProcessChunk(chunk, startLimit, endLimit);
    }

};

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
    return boundaryKeysExt.end();
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

class TKeyRangeChunkTreeTraverser
    : public TChunkTreeTraverserBase<TKey>
{
public:
    TKeyRangeChunkTreeTraverser(
        TBootstrap* bootstrap,
        IChunkProcessorPtr processor)
        : TChunkTreeTraverserBase<TKey>(bootstrap, processor)
    { }

    void Run(
        const TChunkList* chunkList,
        const TKey& lowerBound,
        TNullable<TKey>& upperBound)
    {
        YCHECK(!upperBound || *upperBound >= lowerBound);

        if (chunkList->SortedBy().empty()) {
            ChunkProcessor->OnError(TError(
                "Key range fetch request to the unsorted chunk list."));
            return;
        }

        TChunkTreeTraverserBase<TKey>::Run(chunkList, lowerBound, upperBound);
    }

private:
    virtual int GetStartChild(const TChunkList* chunkList, const TKey& lowerBound) override
    {
        if (chunkList->Children().empty()) {
            return 0;
        }

        typedef decltype(chunkList->Children().begin()) TChildIter;
        std::reverse_iterator<TChildIter> rbegin(chunkList->Children().end());
        std::reverse_iterator<TChildIter> rend(chunkList->Children().begin());

        int index = rend -
            std::upper_bound(
                rbegin,
                rend,
                lowerBound,
                LessComparer);
        YCHECK(index <= chunkList->Children().size());
        return index;
    }

    virtual TKey GetChildLowerBound(
        const TChunkList* chunkList, 
        const TStackEntry& stackEntry,
        const TChunkTreeRef& child) override
    {
        UNUSED(chunkList);
        UNUSED(stackEntry);

        return GetMinKey(child);
    }

    virtual void ProcessChildChunkList(
        const TChunkList* chunkList,
        const TStackEntry& stackEntry,
        const TKey& childLowerBound) override
    {
        TKey maxKey = GetMaxKey(chunkList);

        if (stackEntry.LowerBound  > maxKey) {
            return;
        }

        auto index = GetStartChild(chunkList, stackEntry.LowerBound);
        ChunkTreeStack.push_back(TStackEntry(
            chunkList->GetVersionedId(), 
            index,
            TKey(stackEntry.LowerBound),
            stackEntry.UpperBound ? MakeNullable(*stackEntry.UpperBound) : Null));
    }

    virtual void ProcessChildChunk(
        const TChunk* chunk,
        const TStackEntry& stackEntry,
        const TKey& childLowerBound) override
    {
        TKey childUpperBound = GetMaxKey(chunk);

        if (stackEntry.LowerBound  > childUpperBound) {
            return;
        }

        NTableClient::NProto::TReadLimit startLimit;
        NTableClient::NProto::TReadLimit endLimit;

        if (stackEntry.LowerBound > childLowerBound) {
            *startLimit.mutable_key() = stackEntry.LowerBound;
        }

        if (stackEntry.UpperBound && *stackEntry.UpperBound <= childUpperBound) {
            *endLimit.mutable_key() = *stackEntry.UpperBound;
        }

        ChunkProcessor->ProcessChunk(chunk, startLimit, endLimit);
    }

};

}

////////////////////////////////////////////////////////////////////////////////

void TraverseChunkTree(
    NCellMaster::TBootstrap* bootstrap,
    IChunkProcessorPtr processor,
    const TChunkList* root,
    i64 lowerBound,
    TNullable<i64> upperBound)
{
    auto traverser = New<TIndexRangeChunkTreeTraverser>(bootstrap, processor);
    traverser->Run(root, lowerBound, upperBound);
}

void TraverseChunkTree(
    NCellMaster::TBootstrap* bootstrap,
    IChunkProcessorPtr processor,
    const TChunkList* root,
    const TKey& lowerBound,
    TNullable<TKey> upperBound)
{
    auto traverser = New<TKeyRangeChunkTreeTraverser>(bootstrap, processor);
    traverser->Run(root, lowerBound, upperBound);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
