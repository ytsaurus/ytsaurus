#include "stdafx.h"
#include "public.h"

namespace NYT {
namespace NChunkClient {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TChunkId NullChunkId = NObjectClient::NullObjectId;
TChunkListId NullChunkListId = NObjectClient::NullObjectId;
TChunkTreeId NullChunkTreeId = NObjectClient::NullObjectId;

////////////////////////////////////////////////////////////////////////////////

TChunkIdWithIndex::TChunkIdWithIndex()
    : Index(0)
{ }

TChunkIdWithIndex::TChunkIdWithIndex(const TChunkId& id, int index)
    : Id(id)
    , Index(index)
{ }

Stroka ToString(const TChunkIdWithIndex& id)
{
    return Sprintf("%s/%d", ~ToString(id.Id), id.Index);
}

////////////////////////////////////////////////////////////////////////////////

bool IsErasureChunkId(const TChunkId& id)
{
    return TypeFromId(id) == EObjectType::ErasureChunk;
}

bool IsErasureChunkPartId(const TChunkId& id)
{
    auto type = TypeFromId(id);
    return type >= EObjectType::ErasureChunkPart_0 &&
           type <= EObjectType::ErasureChunkPart_15;
}

TChunkId ErasurePartIdFromChunkId(const TChunkId& id, int index)
{
    return ReplaceTypeInId(id, EObjectType(EObjectType::ErasureChunkPart_0 + index));
}

TChunkId ErasureChunkIdFromPartId(const TChunkId& id)
{
    return ReplaceTypeInId(id, EObjectType::ErasureChunk);
}

int IndexFromErasurePartId(const TChunkId& id)
{
    int index = static_cast<int>(TypeFromId(id)) - static_cast<int>(EObjectType::ErasureChunkPart_0);
    YCHECK(index >= 0 && index <= 15);
    return index;
}

TChunkId EncodeChunkId(const TChunkIdWithIndex& idWithIndex)
{
    return IsErasureChunkId(idWithIndex.Id)
           ? ErasurePartIdFromChunkId(idWithIndex.Id, idWithIndex.Index)
           : idWithIndex.Id;
}

TChunkIdWithIndex DecodeChunkId(const TChunkId& id)
{
    return IsErasureChunkPartId(id)
           ? TChunkIdWithIndex(ErasureChunkIdFromPartId(id), IndexFromErasurePartId(id))
           : TChunkIdWithIndex(id, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

