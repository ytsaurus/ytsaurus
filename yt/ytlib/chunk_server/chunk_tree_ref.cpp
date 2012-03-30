#include "stdafx.h"
#include "chunk_tree_ref.h"

#include "chunk.h"
#include "chunk_list.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

using NObjectServer::EObjectType;

////////////////////////////////////////////////////////////////////////////////

TChunkTreeRef::TChunkTreeRef(TChunk* chunk)
    : Pointer(reinterpret_cast<uintptr_t>(chunk))
{
    YASSERT(!(reinterpret_cast<uintptr_t>(chunk) & 1));
}

TChunkTreeRef::TChunkTreeRef(TChunkList* chunkList)
    : Pointer(reinterpret_cast<uintptr_t>(chunkList) | 1)
{
    YASSERT(!(reinterpret_cast<uintptr_t>(chunkList) & 1));
}

bool TChunkTreeRef::operator == (const TChunkTreeRef& other) const
{
    return Pointer == other.Pointer;
}


EObjectType TChunkTreeRef::GetType() const
{
    if (Pointer & 1) {
        return EObjectType::ChunkList;
    } else {
        return EObjectType::Chunk;
    }
}

TChunk* TChunkTreeRef::AsChunk() const
{
    YASSERT(!(Pointer & 1));
    return reinterpret_cast<TChunk*>(Pointer);
}

TChunkList* TChunkTreeRef::AsChunkList() const
{
    YASSERT(Pointer & 1);
    return reinterpret_cast<TChunkList*>(Pointer & ~1);
}

TChunkTreeId TChunkTreeRef::GetId() const
{
    if (Pointer & 1) {
        return AsChunkList()->GetId();
    } else {
        return AsChunk()->GetId();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
