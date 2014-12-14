#include "stdafx.h"
#include "chunk_tree.h"
#include "chunk.h"
#include "chunk_list.h"

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TChunkTree::TChunkTree(const TChunkTreeId& id)
    : TStagedObject(id)
{ }

TChunk* TChunkTree::AsChunk()
{
    YASSERT(
        GetType() == EObjectType::Chunk ||
        GetType() == EObjectType::ErasureChunk ||
        GetType() == EObjectType::JournalChunk);
    return static_cast<TChunk*>(this);
}

const TChunk* TChunkTree::AsChunk() const
{
    YASSERT(
        GetType() == EObjectType::Chunk ||
        GetType() == EObjectType::ErasureChunk ||
        GetType() == EObjectType::JournalChunk);
    return static_cast<const TChunk*>(this);
}

TChunkList* TChunkTree::AsChunkList()
{
    YASSERT(GetType() == EObjectType::ChunkList);
    return static_cast<TChunkList*>(this);
}

const TChunkList* TChunkTree::AsChunkList() const
{
    YASSERT(GetType() == EObjectType::ChunkList);
    return static_cast<const TChunkList*>(this);
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

} // namespace NChunkServer
} // namespace NYT
