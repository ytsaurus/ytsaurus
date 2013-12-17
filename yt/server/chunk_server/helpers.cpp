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

void AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children)
{
    AttachToChunkList(
        chunkList,
        const_cast<TChunkTree**>(children.data()),
        const_cast<TChunkTree**>(children.data() + children.size()),
        [] (TChunkTree* /*chunk*/) { });
}

void AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree* child)
{
    AttachToChunkList(
        chunkList,
        &child,
        &child + 1,
        [] (TChunkTree* /*chunk*/) { });
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
