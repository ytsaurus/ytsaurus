#include "chunk_tree.h"
#include "chunk.h"
#include "chunk_list.h"

namespace NYT::NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TChunkTree::TChunkTree(const TChunkTreeId& id)
    : TStagedObject(id)
{ }

TChunk* TChunkTree::AsChunk()
{
    Y_ASSERT(
        GetType() == EObjectType::Chunk ||
        GetType() == EObjectType::ErasureChunk ||
        GetType() == EObjectType::JournalChunk);
    return As<TChunk>();
}

const TChunk* TChunkTree::AsChunk() const
{
    Y_ASSERT(
        GetType() == EObjectType::Chunk ||
        GetType() == EObjectType::ErasureChunk ||
        GetType() == EObjectType::JournalChunk);
    return As<TChunk>();
}

TChunkList* TChunkTree::AsChunkList()
{
    Y_ASSERT(GetType() == EObjectType::ChunkList);
    return As<TChunkList>();
}

const TChunkList* TChunkTree::AsChunkList() const
{
    Y_ASSERT(GetType() == EObjectType::ChunkList);
    return As<TChunkList>();
}

void TChunkTree::Save(NCellMaster::TSaveContext& context) const
{
    TStagedObject::Save(context);
}

void TChunkTree::Load(NCellMaster::TLoadContext& context)
{
    TStagedObject::Load(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
