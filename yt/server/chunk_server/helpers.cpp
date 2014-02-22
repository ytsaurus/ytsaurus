#include "stdafx.h"
#include "helpers.h"

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            child->AsChunk()->Parents().push_back(parent);
            break;
        case EObjectType::ChunkList:
            child->AsChunkList()->Parents().insert(parent);
            break;
        default:
            YUNREACHABLE();
    }
}

void ResetChunkTreeParent(TChunkList* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk: {
            auto& parents = child->AsChunk()->Parents();
            auto it = std::find(parents.begin(), parents.end(), parent);
            YASSERT(it != parents.end());
            parents.erase(it);
            break;
        }
        case EObjectType::ChunkList: {
            auto& parents = child->AsChunkList()->Parents();
            auto it = parents.find(parent);
            YASSERT(it != parents.end());
            parents.erase(it);
            break;
        }
        default:
            YUNREACHABLE();
    }
}

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return chunkTree->AsChunk()->GetStatistics();
        case EObjectType::ChunkList:
            return chunkTree->AsChunkList()->Statistics();
        default:
            YUNREACHABLE();
    }
}

void AddChildStatistics(
    TChunkList* chunkList,
    TChunkTree* child,
    TChunkTreeStatistics* delta)
{
    if (!chunkList->Children().empty()) {
        chunkList->RowCountSums().push_back(
            chunkList->Statistics().RowCount +
            delta->RowCount);
        chunkList->ChunkCountSums().push_back(
            chunkList->Statistics().ChunkCount +
            delta->ChunkCount);
        chunkList->DataSizeSums().push_back(
            chunkList->Statistics().UncompressedDataSize +
            delta->UncompressedDataSize);

    }
    delta->Accumulate(GetChunkTreeStatistics(child));
}

void ResetChunkListStatistics(TChunkList* chunkList)
{
    chunkList->RowCountSums().clear();
    chunkList->ChunkCountSums().clear();
    chunkList->Statistics() = TChunkTreeStatistics();
    chunkList->Statistics().ChunkListCount = 1;
}

void RecomputeChunkListStatistics(TChunkList* chunkList)
{
    ResetChunkListStatistics(chunkList);

    std::vector<TChunkTree*> existingChildren;
    existingChildren.swap(chunkList->Children());

    TChunkTreeStatistics delta;
    for (auto* child : existingChildren) {
        AddChildStatistics(chunkList, child, &delta);
        chunkList->Children().push_back(child);
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
