#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <util/random/random.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

inline NYPath::TYPath FromObjectId(const TObjectId& id)
{
    return TString(ObjectIdPathPrefix) + ToString(id);
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
        type == EObjectType::ReplicatedTable ||
        type == EObjectType::Journal ||
        type == EObjectType::ChunkMap ||
        type == EObjectType::LostChunkMap ||
        type == EObjectType::PrecariousChunkMap ||
        type == EObjectType::OverreplicatedChunkMap ||
        type == EObjectType::UnderreplicatedChunkMap ||
        type == EObjectType::DataMissingChunkMap ||
        type == EObjectType::ParityMissingChunkMap ||
        type == EObjectType::QuorumMissingChunkMap ||
        type == EObjectType::UnsafelyPlacedChunkMap ||
        type == EObjectType::ForeignChunkMap ||
        type == EObjectType::RackMap ||
        type == EObjectType::DataCenterMap ||
        type == EObjectType::ChunkListMap ||
        type == EObjectType::MediumMap ||
        type == EObjectType::TransactionMap ||
        type == EObjectType::TopmostTransactionMap ||
        type == EObjectType::ClusterNodeNode ||
        type == EObjectType::ClusterNodeMap ||
        type == EObjectType::Orchid ||
        type == EObjectType::LostVitalChunkMap ||
        type == EObjectType::PrecariousVitalChunkMap ||
        type == EObjectType::AccountMap ||
        type == EObjectType::UserMap ||
        type == EObjectType::GroupMap ||
        type == EObjectType::Link ||
        type == EObjectType::Document ||
        type == EObjectType::LockMap ||
        type == EObjectType::TabletMap ||
        type == EObjectType::TabletCellMap ||
        type == EObjectType::TabletCellNode ||
        type == EObjectType::TabletCellBundleMap ||
        type == EObjectType::TabletActionMap ||
        type == EObjectType::SysNode;
}

inline bool IsUserType(EObjectType type)
{
    return
        type == EObjectType::Transaction ||
        type == EObjectType::Chunk ||
        type == EObjectType::JournalChunk ||
        type == EObjectType::ErasureChunk ||
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
        type == EObjectType::ReplicatedTable ||
        type == EObjectType::TableReplica ||
        type == EObjectType::Journal ||
        type == EObjectType::Link ||
        type == EObjectType::Document;
}

inline EObjectType TypeFromId(const TObjectId& id)
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
    Y_ASSERT(HasSchema(type));
    return EObjectType(static_cast<int>(type) | SchemaObjectTypeMask);
}

inline EObjectType TypeFromSchemaType(EObjectType type)
{
    Y_ASSERT(static_cast<int>(type) & SchemaObjectTypeMask);
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

inline bool IsWellKnownId(const TObjectId& id)
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
    result.Parts32[1] &= ~0x0000ffff;
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

Y_FORCE_INLINE size_t TDirectObjectIdHash::operator()(const TObjectId& id) const
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
