#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

inline NYPath::TYPath FromObjectId(const TObjectId& id)
{
    return Stroka(ObjectIdPathPrefix) + ToString(id);
}

inline bool IsVersionedType(EObjectType type)
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
        type == EObjectType::ClusterNodeNode ||
        type == EObjectType::ClusterNodeMap ||
        type == EObjectType::Orchid ||
        type == EObjectType::LostVitalChunkMap ||
        type == EObjectType::AccountMap ||
        type == EObjectType::UserMap ||
        type == EObjectType::GroupMap ||
        type == EObjectType::Link ||
        type == EObjectType::Document ||
        type == EObjectType::LockMap ||
        type == EObjectType::TabletMap ||
        type == EObjectType::TabletCellNode ||
        type == EObjectType::SysNode;
}

inline bool IsUserType(EObjectType type)
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

inline bool IsMapLikeType(EObjectType type)
{
    return
        type == EObjectType::MapNode ||
        type == EObjectType::ClusterNodeNode ||
        type == EObjectType::ClusterNodeMap ||
        type == EObjectType::TabletCellNode ||
        type == EObjectType::SysNode;
}

inline bool IsListLikeType(EObjectType type)
{
    return
        type == EObjectType::ListNode;
}

inline EObjectType
TypeFromId(const TObjectId& id)
{
    return EObjectType(id.Parts32[1] & 0xffff);
}

inline TCellTag CellTagFromId(const TObjectId& id)
{
    return id.Parts32[1] >> 16;
}

inline ui64 CounterFromId(const TObjectId& id)
{
    ui64 result;
    result   = id.Parts32[3];
    result <<= 32;
    result  |= id.Parts32[2];
    return result;
}

inline bool HasSchema(EObjectType type)
{
    if (type == EObjectType::Master) {
        return false;
    }
    if (static_cast<int>(type) & SchemaObjectTypeMask) {
        return false;
    }
    return true;
}

inline EObjectType SchemaTypeFromType(EObjectType type)
{
    YASSERT(HasSchema(type));
    return EObjectType(static_cast<int>(type) | SchemaObjectTypeMask);
}

inline EObjectType TypeFromSchemaType(EObjectType type)
{
    YASSERT(static_cast<int>(type) & SchemaObjectTypeMask);
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
        (cellTag << 16) + static_cast<int>(type),
        counter & 0xffffffff,
        counter >> 32);
}

inline bool IsWellKnownId(const TObjectId& id)
{
    return CounterFromId(id) & WellKnownCounterMask;
}

inline TObjectId MakeRegularId(
    EObjectType type,
    TCellTag cellTag,
    ui64 random,
    NHydra::TVersion version)
{
    return TObjectId(
        random,
        (cellTag << 16) + static_cast<int>(type),
        version.RecordId,
        version.SegmentId);
}

inline TObjectId MakeWellKnownId(
    EObjectType type,
    TCellTag cellTag,
    ui64 counter /*= 0xffffffffffffffff*/)
{
    YCHECK(counter & WellKnownCounterMask);
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
    const TObjectId& id,
    EObjectType type)
{
    auto result = id;
    result.Parts32[1] &= ~0xffff;
    result.Parts32[1] |= static_cast<ui32>(type);
    return result;
}

inline TObjectId ReplaceCellTagInId(
    const TObjectId& id,
    TCellTag cellTag)
{
    auto result = id;
    result.Parts32[1] &= ~0xffff0000;
    result.Parts32[1] |= static_cast<ui32>(cellTag) << 16;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
