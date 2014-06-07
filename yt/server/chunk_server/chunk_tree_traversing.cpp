#include "stdafx.h"
#include "chunk_tree_traversing.h"
#include "chunk_list.h"
#include "chunk.h"
#include "chunk_manager.h"

#include <core/misc/singleton.h>

#include <ytlib/object_client/public.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

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

            auto* child = chunkList->Children()[entry.ChildIndex];
            TReadLimit childLowerBound;
            TReadLimit childUpperBound;

            auto childLowerRowIndex = 
                (entry.ChildIndex == 0)
                    ? 0
                    : chunkList->RowCountSums()[entry.ChildIndex - 1];

            if (entry.UpperBound.HasRowIndex()) {
                if (entry.UpperBound.GetRowIndex() <= childLowerRowIndex) {
                    PopStack();
                    continue;
                }
                
                childLowerBound.SetRowIndex(childLowerRowIndex);
                childUpperBound.SetRowIndex(
                    entry.ChildIndex == chunkList->Children().size() - 1
                        ? chunkList->Statistics().RowCount
                        : chunkList->RowCountSums()[entry.ChildIndex]);
            } else if (entry.LowerBound.HasRowIndex()) {
                childLowerBound.SetRowIndex(childLowerRowIndex);
            }

            auto childLowerChunkIndex = 
                (entry.ChildIndex == 0)
                    ? 0
                    : chunkList->ChunkCountSums()[entry.ChildIndex - 1];
            if (entry.UpperBound.HasChunkIndex()) {
                if (entry.UpperBound.GetChunkIndex() <= childLowerChunkIndex) {
                    PopStack();
                    continue;
                }
                
                childLowerBound.SetChunkIndex(childLowerChunkIndex);
                childUpperBound.SetChunkIndex(
                    entry.ChildIndex == chunkList->Children().size() - 1
                        ? chunkList->Statistics().ChunkCount
                        : chunkList->ChunkCountSums()[entry.ChildIndex]);
            } else if (entry.LowerBound.HasChunkIndex()) {
                childLowerBound.SetChunkIndex(childLowerChunkIndex);
            }

            auto childLowerOffset =
                (entry.ChildIndex == 0)
                    ? 0
                    : chunkList->DataSizeSums()[entry.ChildIndex - 1];
            if (entry.UpperBound.HasOffset()) {
                if (entry.UpperBound.GetOffset() <= childLowerOffset) {
                    PopStack();
                    continue;
                }

                childLowerBound.SetOffset(childLowerOffset);
                childUpperBound.SetOffset(
                    entry.ChildIndex == chunkList->Children().size() - 1
                        ? chunkList->Statistics().UncompressedDataSize
                        : chunkList->DataSizeSums()[entry.ChildIndex]);
            } else if (entry.LowerBound.HasOffset()) {
                childLowerBound.SetOffset(childLowerOffset);
            }

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

    int GetStartChildIndex(
        const TChunkList* chunkList,
        const TReadLimit& lowerBound)
    {
        if (chunkList->Children().empty()) {
            return 0;
        }

        int index = 0;

        if (lowerBound.HasRowIndex()) {
            if (lowerBound.GetRowIndex() >= chunkList->Statistics().RowCount) {
                return chunkList->Children().size();
            }

            auto begin = chunkList->RowCountSums().begin();
            int childIndex = std::upper_bound(
                begin,
                chunkList->RowCountSums().end(),
                lowerBound.GetRowIndex()) - begin;
            index = std::max(index, childIndex);
            YCHECK(index < chunkList->Children().size());
        }

        if (lowerBound.HasChunkIndex()) {
            if (lowerBound.GetChunkIndex() >= chunkList->Statistics().ChunkCount) {
                return chunkList->Children().size();
            }

            auto begin = chunkList->ChunkCountSums().begin();
            int childIndex = std::upper_bound(
                begin,
                chunkList->ChunkCountSums().end(),
                lowerBound.GetChunkIndex()) - begin;
            index = std::max(index, childIndex);
            YCHECK(index < chunkList->Children().size());
        }

        if (lowerBound.HasOffset()) {
            if (lowerBound.GetOffset() >= chunkList->Statistics().UncompressedDataSize) {
                return chunkList->Children().size();
            }

            auto begin = chunkList->DataSizeSums().begin();
            int childIndex = std::upper_bound(
                begin,
                chunkList->DataSizeSums().end(),
                lowerBound.GetOffset()) - begin;
            index = std::max(index, childIndex);
            YCHECK(index < chunkList->Children().size());
        }

        if (lowerBound.HasKey()) {
            typedef decltype(chunkList->Children().begin()) TChildIter;
            std::reverse_iterator<TChildIter> rbegin(chunkList->Children().end());
            std::reverse_iterator<TChildIter> rend(chunkList->Children().begin());

            int childIndex = rend - std::upper_bound(
                rbegin,
                rend,
                lowerBound.GetKey(),
                [] (const TOwningKey& key, const TChunkTree* chunkTree) {
                    return key > GetMaxKey(chunkTree);
                });
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
            YCHECK(newUpperBound > 0);
            endLimit->SetRowIndex(newUpperBound);
        }

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
            YCHECK(newUpperBound > 0);
            endLimit->SetOffset(newUpperBound);
        }

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
        return Bootstrap_->GetMetaStateFacade()->GetGuardedInvoker();
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
