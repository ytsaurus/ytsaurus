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

TObjectId CreateId(EObjectType type, TCellId cellId, ui64 counter)
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

TTransactionId NullTransactionId = NObjectServer::NullObjectId;

////////////////////////////////////////////////////////////////////////////////

TVersionedObjectId::TVersionedObjectId()
{ }

TVersionedObjectId::TVersionedObjectId(const TObjectId& objectId)
    : ObjectId(objectId)
{ }

TVersionedObjectId::TVersionedObjectId(const TObjectId& objectId, const TTransactionId& transactionId)
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

TVersionedObjectId TVersionedObjectId::FromString(const Stroka& str)
{
    auto tokens = splitStroku(str, ":");
    if (tokens.size() < 1 || 2 < tokens.size()) {
        ythrow yexception() << Sprintf("Invalid number of tokens in %s", ~str.Quote());
    }

    auto objectId = TObjectId::FromString(tokens[0]);
    auto transactionId =
        tokens.size() == 2
        ? TTransactionId::FromString(tokens[1])
        : NullTransactionId;
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

} // namespace NObjectServer
} // namespace NYT

