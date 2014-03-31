#include "stdafx.h"
#include "helpers.h"
#include "object_service_proxy.h"

namespace NYT {
namespace NObjectClient {

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
        type == EObjectType::LockMap;
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

bool HasSchema(EObjectType type)
{
    return (type & 0x8000) == 0 &&
           type != EObjectType::Master;
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

TCellId GetCellId(const TObjectId& id, EObjectType type)
{
    return (id.Parts[1] - type) >> 16;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT

