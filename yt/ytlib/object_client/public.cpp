#include "stdafx.h"
#include "public.h"

#include <util/string/vector.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

TObjectId NullObjectId(0, 0, 0, 0);
TTransactionId NullTransactionId(0, 0, 0, 0);

////////////////////////////////////////////////////////////////////////////////

bool IsVersionedType(EObjectType type)
{
    return
        type == EObjectType::StringNode ||
        type == EObjectType::IntegerNode ||
        type == EObjectType::DoubleNode ||
        type == EObjectType::MapNode ||
        type == EObjectType::ListNode ||
        type == EObjectType::File ||
        type == EObjectType::Table ||
        type == EObjectType::ChunkMap ||
        type == EObjectType::LostChunkMap ||
        type == EObjectType::OverreplicatedChunkMap ||
        type == EObjectType::UnderreplicatedChunkMap ||
        type == EObjectType::DataMissingChunkMap ||
        type == EObjectType::ParityMissingChunkMap ||
        type == EObjectType::ChunkListMap ||
        type == EObjectType::TransactionMap ||
        type == EObjectType::TopmostTransactionMap ||
        type == EObjectType::CellNodeMap ||
        type == EObjectType::CellNode ||
        type == EObjectType::Orchid ||
        type == EObjectType::LostVitalChunkMap ||
        type == EObjectType::AccountMap ||
        type == EObjectType::UserMap ||
        type == EObjectType::GroupMap ||
        type == EObjectType::Link ||
        type == EObjectType::Document ||
        type == EObjectType::LockMap ||
        type == EObjectType::TabletCellMap ||
        type == EObjectType::TabletMap;
}

bool IsUserType(EObjectType type)
{
    return
        type == EObjectType::Transaction ||
        type == EObjectType::Chunk ||
        type == EObjectType::ChunkList ||
        type == EObjectType::StringNode ||
        type == EObjectType::IntegerNode ||
        type == EObjectType::DoubleNode ||
        type == EObjectType::MapNode ||
        type == EObjectType::ListNode ||
        type == EObjectType::File ||
        type == EObjectType::Table ||
        type == EObjectType::Link ||
        type == EObjectType::Document;
}

EObjectType TypeFromId(const TObjectId& id)
{
    return EObjectType(id.Parts[1] & 0xffff);
}

TCellId CellIdFromId(const TObjectId& id)
{
    return id.Parts[1] >> 16;
}

ui64 CounterFromId(const TObjectId& id)
{
    ui64 result;
    result   = id.Parts[3];
    result <<= 32;
    result  |= id.Parts[2];
    return result;
}

bool HasSchema(EObjectType type)
{
    return (type & 0x8000) == 0 &&
        type != EObjectType::Master &&
        type != EObjectType::QueryFragment;
}

EObjectType SchemaTypeFromType(EObjectType type)
{
    YASSERT(HasSchema(type));
    return EObjectType(type | 0x8000);
}

EObjectType TypeFromSchemaType(EObjectType type)
{
    YASSERT((type & 0x8000) != 0);
    return EObjectType(type & ~0x8000);
}

TObjectId MakeId(
    EObjectType type,
    TCellId cellId,
    ui64 counter,
    ui32 hash)
{
    return TObjectId(
        hash,
        (cellId << 16) + static_cast<int>(type),
        counter & 0xffffffff,
        counter >> 32);
}

TObjectId MakeWellKnownId(
    EObjectType type,
    TCellId cellId,
    ui64 counter /*= 0xffffffffffffffff*/)
{
    return MakeId(
        type,
        cellId,
        counter,
        static_cast<ui32>(cellId * 901517) ^ 0x140a8383);
}

TObjectId MakeSchemaObjectId(
    EObjectType type,
    TCellId cellId)
{
    return MakeWellKnownId(SchemaTypeFromType(type), cellId);
}

TObjectId ReplaceTypeInId(
    const TObjectId& id,
    EObjectType type)
{
    auto result = id;
    result.Parts[1] &= ~0xffff;
    result.Parts[1] |= type;
    return result;
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

Stroka ToString(const TVersionedObjectId& id)
{
    return Sprintf("%s:%s",
        ~ToString(id.ObjectId),
        ~ToString(id.TransactionId));
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

