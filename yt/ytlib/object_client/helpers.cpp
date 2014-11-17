#include "stdafx.h"
#include "helpers.h"
#include "object_service_proxy.h"

namespace NYT {
namespace NObjectClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TStringBuf ObjectIdPathPrefix("#");

TYPath FromObjectId(const TObjectId& id)
{
    return Stroka(ObjectIdPathPrefix) + ToString(id);
}

bool IsVersionedType(EObjectType type)
{
    return
        type == EObjectType::StringNode ||
        type == EObjectType::Int64Node ||
        type == EObjectType::Uint64Node ||
        type == EObjectType::DoubleNode ||
        type == EObjectType::BooleanNode ||
        type == EObjectType::MapNode ||
        type == EObjectType::ListNode ||
        type == EObjectType::File ||
        type == EObjectType::Table ||
        type == EObjectType::Journal ||
        type == EObjectType::ChunkMap ||
        type == EObjectType::LostChunkMap ||
        type == EObjectType::OverreplicatedChunkMap ||
        type == EObjectType::UnderreplicatedChunkMap ||
        type == EObjectType::DataMissingChunkMap ||
        type == EObjectType::ParityMissingChunkMap ||
        type == EObjectType::QuorumMissingChunkMap ||
        type == EObjectType::UnsafelyPlacedChunkMap ||
        type == EObjectType::RackMap ||
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
        type == EObjectType::TabletMap ||
        type == EObjectType::TabletCellNode;
}

bool IsUserType(EObjectType type)
{
    return
        type == EObjectType::Transaction ||
        type == EObjectType::Chunk ||
        type == EObjectType::ChunkList ||
        type == EObjectType::StringNode ||
        type == EObjectType::Int64Node ||
        type == EObjectType::Uint64Node ||
        type == EObjectType::DoubleNode ||
        type == EObjectType::BooleanNode ||
        type == EObjectType::MapNode ||
        type == EObjectType::ListNode ||
        type == EObjectType::File ||
        type == EObjectType::Table ||
        type == EObjectType::Journal ||
        type == EObjectType::Link ||
        type == EObjectType::Document;
}

EObjectType TypeFromId(const TObjectId& id)
{
    return EObjectType(id.Parts32[1] & 0xffff);
}

TCellTag CellTagFromId(const TObjectId& id)
{
    return id.Parts32[1] >> 16;
}

ui64 CounterFromId(const TObjectId& id)
{
    ui64 result;
    result   = id.Parts32[3];
    result <<= 32;
    result  |= id.Parts32[2];
    return result;
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
    TCellTag cellTag,
    ui64 counter,
    ui32 hash)
{
    return TObjectId(
        hash,
        (cellTag << 16) + static_cast<int>(type),
        counter & 0xffffffff,
        counter >> 32);
}

TObjectId MakeWellKnownId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter /*= 0xffffffffffffffff*/)
{
    return MakeId(
        type,
        cellTag,
        counter,
        static_cast<ui32>(cellTag * 901517) ^ 0x140a8383);
}

TObjectId MakeSchemaObjectId(
    EObjectType type,
    TCellTag cellTag)
{
    return MakeWellKnownId(SchemaTypeFromType(type), cellTag);
}

TObjectId ReplaceTypeInId(
    const TObjectId& id,
    EObjectType type)
{
    auto result = id;
    result.Parts32[1] &= ~0xffff;
    result.Parts32[1] |= type;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT

