#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <util/random/random.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

inline EObjectType TypeFromId(TObjectId id)
{
    return EObjectType(id.Parts32[1] & 0xffff);
}

inline TCellTag CellTagFromId(TObjectId id)
{
    return id.Parts32[1] >> 16;
}

inline ui64 CounterFromId(TObjectId id)
{
    ui64 result;
    result   = id.Parts32[3];
    result <<= 32;
    result  |= id.Parts32[2];
    return result;
}

inline EObjectType SchemaTypeFromType(EObjectType type)
{
    YT_ASSERT(HasSchema(type));
    return EObjectType(static_cast<int>(type) | SchemaObjectTypeMask);
}

inline EObjectType TypeFromSchemaType(EObjectType type)
{
    YT_ASSERT(static_cast<int>(type) & SchemaObjectTypeMask);
    return EObjectType(static_cast<int>(type) & ~SchemaObjectTypeMask);
}

inline TObjectId MakeId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter,
    ui32 hash)
{
    return TObjectId(
        hash,
        (cellTag << 16) | static_cast<ui32>(type),
        counter & 0xffffffff,
        counter >> 32);
}

inline TObjectId MakeRandomId(
    EObjectType type,
    TCellTag cellTag)
{
    return MakeId(
        type,
        cellTag,
        RandomNumber<ui64>(),
        RandomNumber<ui32>());
}

inline bool IsWellKnownId(TObjectId id)
{
    return CounterFromId(id) & WellKnownCounterMask;
}

inline TObjectId MakeRegularId(
    EObjectType type,
    TCellTag cellTag,
    NHydra::TVersion version,
    ui32 hash)
{
    return TObjectId(
        hash,
        (cellTag << 16) | static_cast<ui32>(type),
        version.RecordId,
        version.SegmentId);
}

inline TObjectId MakeWellKnownId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter /*= 0xffffffffffffffff*/)
{
    YT_VERIFY(counter & WellKnownCounterMask);
    return MakeId(
        type,
        cellTag,
        counter,
        static_cast<ui32>(cellTag * 901517) ^ 0x140a8383);
}

inline TObjectId MakeSchemaObjectId(
    EObjectType type,
    TCellTag cellTag)
{
    return MakeWellKnownId(SchemaTypeFromType(type), cellTag);
}

inline TObjectId ReplaceTypeInId(
    TObjectId id,
    EObjectType type)
{
    auto result = id;
    result.Parts32[1] &= ~0x0000ffff;
    result.Parts32[1] |= static_cast<ui32>(type);
    return result;
}

inline TObjectId ReplaceCellTagInId(
    TObjectId id,
    TCellTag cellTag)
{
    auto result = id;
    result.Parts32[1] &= ~0xffff0000;
    result.Parts32[1] |= static_cast<ui32>(cellTag) << 16;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE size_t TDirectObjectIdHash::operator()(TObjectId id) const
{
    return id.Parts32[0];
}

Y_FORCE_INLINE size_t TDirectVersionedObjectIdHash::operator()(const TVersionedObjectId& id) const
{
    return
        TDirectObjectIdHash()(id.TransactionId) * 497 +
        TDirectObjectIdHash()(id.ObjectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
