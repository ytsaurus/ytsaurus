#include "stdafx.h"
#include "id.h"

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TObjectId NullObjectId(0, 0, 0, 0);

EObjectType TypeFromId(const TObjectId& id)
{
    return EObjectType(id.Parts[1] & 0xffff);
}

TObjectId CreateId(EObjectType type, ui16 cellId, ui64 counter)
{
    int typeValue = type.ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);

    char data[12];
    *reinterpret_cast<ui64*>(&data[ 0]) = counter;
    *reinterpret_cast<ui16*>(&data[ 8]) = typeValue;
    *reinterpret_cast<ui16*>(&data[10]) = cellId;
    ui32 hash = MurmurHash<ui32>(&data, sizeof (data), 0);

    return TObjectId(
        hash,
        (cellId << 16) + type.ToValue(),
        counter & 0xffffffff,
        counter >> 32);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

