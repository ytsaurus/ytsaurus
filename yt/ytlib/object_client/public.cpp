#include "stdafx.h"
#include "public.h"

#include <util/string/vector.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

TObjectId NullObjectId(0, 0, 0, 0);
TTransactionId NullTransactionId(0, 0, 0, 0);

////////////////////////////////////////////////////////////////////////////////

EObjectType TypeFromId(const TObjectId& id)
{
    return EObjectType(id.Parts[1] & 0xffff);
}

TObjectId CreateId(EObjectType type, TCellId cellId, ui64 counter)
{
    int typeValue = type.ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType && typeValue < (1 << 16));

#if defined(__GNUC__) || defined(__clang__)
#ifndef offset_of
#define offset_of(type, member) \
    ((intptr_t) ((char *) &(((type *) 8)->member) - 8))
#endif
    struct TSalt {
        ui64 Counter;
        ui16 TypeValue;
        ui16 CellId;
    } __attribute__((aligned(4),packed)) salt = {
        counter,
        static_cast<ui16>(typeValue),
        static_cast<ui16>(cellId)
    };
    // XXX(sandello): Those subtle aliasing/alignment/whatever issues
    // are really creppy. Therefore I decided to plug in a lot of static_asserts.
    //static_assert(sizeof(salt) == 12,
    //    "You can run into subtle problems while generating object ids");
    //static_assert(offset_of(TSalt, Counter)   == 0,  ".Counter field has shifted");
    //static_assert(offset_of(TSalt, TypeValue) == 8,  ".TypeValue field has shifted");
    //static_assert(offset_of(TSalt, CellId)    == 10, ".CellId field has shifted");
    ui32 hash = MurmurHash<ui32>(&salt, sizeof(salt), 0);
#else
    char data[12] = { 0 };
    *reinterpret_cast<ui64*>(&data[ 0]) = counter;
    *reinterpret_cast<ui16*>(&data[ 8]) = typeValue;
    *reinterpret_cast<ui16*>(&data[10]) = cellId;
    ui32 hash = MurmurHash<ui32>(&data, sizeof(data), 0);
#endif

    return TObjectId(
        hash,
        (cellId << 16) + type.ToValue(),
        counter & 0xffffffff,
        counter >> 32);
}

////////////////////////////////////////////////////////////////////////////////

TVersionedObjectId::TVersionedObjectId()
{ }

TVersionedObjectId::TVersionedObjectId(const TObjectId& objectId)
    : ObjectId(objectId)
{ }

TVersionedObjectId::TVersionedObjectId(
	const TObjectId& objectId,
	const TTransactionId& transactionId)
    : ObjectId(objectId)
    , TransactionId(transactionId)
{ }

bool TVersionedObjectId::IsBranched() const
{
    return TransactionId != NullTransactionId;
}

Stroka TVersionedObjectId::ToString() const
{
    return Sprintf("%s:%s",
        ~ObjectId.ToString(),
        ~TransactionId.ToString());
}

TVersionedObjectId TVersionedObjectId::FromString(const TStringBuf& str)
{
    TStringBuf objectToken, transactionToken;
    str.Split(':', objectToken, transactionToken);

    auto objectId = TObjectId::FromString(objectToken);
    auto transactionId =
        transactionToken.empty()
        ? NullTransactionId
        : TTransactionId::FromString(transactionToken);
    return TVersionedObjectId(objectId, transactionId);
}

bool operator == (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TVersionedObjectId)) == 0;
}

bool operator != (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs)
{
    return !(lhs == rhs);
}

bool operator < (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TVersionedObjectId)) < 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT

