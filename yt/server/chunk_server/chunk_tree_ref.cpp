#include "stdafx.h"
#include "chunk_tree_ref.h"
#include "chunk.h"
#include "chunk_list.h"

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

namespace {

const intptr_t TypeMask = 3;
const intptr_t NullType = 0;
const intptr_t ChunkType = 1;
const intptr_t ChunkListType = 2;

inline intptr_t AddressToCookie(void* addr, intptr_t type)
{
    auto cookie = reinterpret_cast<uintptr_t>(addr);
    YASSERT((cookie & TypeMask) == 0);
    return cookie | type;
}

template <class T>
inline T* CookieToAddress(intptr_t cookie)
{
    return reinterpret_cast<T*>(cookie & ~TypeMask);
}

} // namespace

TChunkTreeRef::TChunkTreeRef()
    : Cookie(NullType)
{ }

TChunkTreeRef::TChunkTreeRef(TChunk* chunk)
    : Cookie(AddressToCookie(chunk, ChunkType))
{ }

TChunkTreeRef::TChunkTreeRef(TChunkList* chunkList)
    : Cookie(AddressToCookie(chunkList, ChunkListType))
{ }

bool TChunkTreeRef::operator == (const TChunkTreeRef& other) const
{
    return Cookie == other.Cookie;
}

bool TChunkTreeRef::operator != (const TChunkTreeRef& other) const
{
    return Cookie != other.Cookie;
}

EObjectType TChunkTreeRef::GetType() const
{
    switch (Cookie & TypeMask) {
        case NullType:      return EObjectType::Null;
        case ChunkType:     return EObjectType::Chunk;
        case ChunkListType: return EObjectType::ChunkList;
        default:            YUNREACHABLE();
    }
}

TChunk* TChunkTreeRef::AsChunk() const
{
    YASSERT(GetType() == EObjectType::Chunk);
    return CookieToAddress<TChunk>(Cookie);
}

TChunkList* TChunkTreeRef::AsChunkList() const
{
    YASSERT(GetType() == EObjectType::ChunkList);
    return CookieToAddress<TChunkList>(Cookie);
}

TChunkTreeId TChunkTreeRef::GetId() const
{
    switch (Cookie & TypeMask) {
        case ChunkType:     return AsChunk()->GetId();
        case ChunkListType: return AsChunkList()->GetId();
        default:            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
