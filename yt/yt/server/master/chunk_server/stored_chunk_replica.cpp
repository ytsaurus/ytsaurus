#include "stored_chunk_replica.h"

#include "medium_base.h"
#include "chunk_location.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;
using namespace NObjectServer;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TAugmentedStoredChunkReplicaPtr::operator bool() const
{
    auto type = GetStoredReplicaType();
    switch (type) {
        case EStoredReplicaType::ChunkLocation:
            return As<EStoredReplicaType::ChunkLocation>()->AsChunkLocationPtr();
        case EStoredReplicaType::OffshoreMedia:
            return As<EStoredReplicaType::OffshoreMedia>()->AsMediumPtr();
        default:
            return false;
    }
}

EStoredReplicaType TAugmentedStoredChunkReplicaPtr::GetStoredReplicaType() const
{
    return static_cast<EStoredReplicaType>((Value_ >> 48) & 0xff);
}

std::strong_ordering TAugmentedStoredChunkReplicaPtr::operator<=>(const TAugmentedStoredChunkReplicaPtr& other) const
{
    if (auto cmp = GetStoredReplicaType() <=> other.GetStoredReplicaType(); cmp != 0) {
        return cmp;
    }

    if (auto cmp = GetReplicaIndex() <=> other.GetReplicaIndex(); cmp != 0) {
        return cmp;
    }

    if (auto cmp = GetReplicaState() <=> other.GetReplicaState(); cmp != 0) {
        return cmp;
    }

    return GetId() <=> other.GetId();
}

TAugmentedStoredChunkReplicaPtr TAugmentedStoredChunkReplicaPtr::ToGenericState() const
{
    auto type = GetStoredReplicaType();
    switch (type) {
        case EStoredReplicaType::ChunkLocation:
            return TAugmentedStoredChunkReplicaPtr(As<EStoredReplicaType::ChunkLocation>()->AsChunkLocationPtr(), GetReplicaIndex());
        case EStoredReplicaType::OffshoreMedia:
            return TAugmentedStoredChunkReplicaPtr(As<EStoredReplicaType::OffshoreMedia>()->AsMediumPtr(), GetReplicaIndex());
    }
}

EChunkReplicaState TAugmentedStoredChunkReplicaPtr::GetReplicaState() const
{
    return static_cast<EChunkReplicaState>(Value_ & 0x3);
}

int TAugmentedStoredChunkReplicaPtr::GetReplicaIndex() const
{
    return static_cast<int>((Value_ >> 56) & 0xff);
}

int TAugmentedStoredChunkReplicaPtr::GetEffectiveMediumIndex() const
{
    auto type = GetStoredReplicaType();
    switch (type) {
        case EStoredReplicaType::ChunkLocation:
            return As<EStoredReplicaType::ChunkLocation>()->AsChunkLocationPtr()->GetEffectiveMediumIndex();
        case EStoredReplicaType::OffshoreMedia:
            return As<EStoredReplicaType::OffshoreMedia>()->AsMediumPtr()->GetIndex();
    }
}

TChunkLocationUuid TAugmentedStoredChunkReplicaPtr::GetLocationUuid() const
{
    switch (GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            auto* location = As<EStoredReplicaType::ChunkLocation>()->AsChunkLocationPtr();
            if (!IsObjectAlive(location)) {
                return InvalidChunkLocationUuid;
            }
            return location->GetUuid();
        }
        case EStoredReplicaType::OffshoreMedia: {
            return InvalidChunkLocationUuid;
        }
    }
}

TNodeId TAugmentedStoredChunkReplicaPtr::GetNodeId() const
{
    switch (GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            auto* location = As<EStoredReplicaType::ChunkLocation>()->AsChunkLocationPtr();
            if (!IsObjectAlive(location)) {
                return InvalidNodeId;
            }
            auto node = location->GetNode();
            if (!IsObjectAlive(node)) {
                return InvalidNodeId;
            }
            return node->GetId();
        }
        case EStoredReplicaType::OffshoreMedia: {
            return OffshoreNodeId;
        }
    }
}

void TAugmentedStoredChunkReplicaPtr::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    auto type = GetStoredReplicaType();
    Save(context, type);
    Save(context, GetReplicaIndex());
    Save(context, GetReplicaState());

    switch (type) {
        case EStoredReplicaType::ChunkLocation: {
            SaveWith<NCellMaster::TRawNonversionedObjectPtrSerializer>(context, As<EStoredReplicaType::ChunkLocation>()->AsChunkLocationPtr());
            break;
        }
        case EStoredReplicaType::OffshoreMedia: {
            SaveWith<NCellMaster::TRawNonversionedObjectPtrSerializer>(context, As<EStoredReplicaType::OffshoreMedia>()->AsMediumPtr());
            break;
        }
    }
}

void TAugmentedStoredChunkReplicaPtr::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    // COMPAT(cherepashka)
    if (context.GetVersion() >= NCellMaster::EMasterReign::RefactoringAroundChunkStoredReplicas) {
        auto type = Load<EStoredReplicaType>(context);
        auto index = Load<int>(context);
        auto state = Load<EChunkReplicaState>(context);
        switch (type) {
            case EStoredReplicaType::ChunkLocation: {
                auto* ptr = LoadWith<NCellMaster::TRawNonversionedObjectPtrSerializer, TChunkLocation*>(context);
                *this = TAugmentedStoredChunkReplicaPtr(ptr, index,state);
                break;
            }
            case EStoredReplicaType::OffshoreMedia: {
                auto* ptr = LoadWith<NCellMaster::TRawNonversionedObjectPtrSerializer, TMedium*>(context);
                *this = TAugmentedStoredChunkReplicaPtr(ptr, index,state);
                break;
            }
        }
    } else {
        auto chunkLocation = Load<TChunkLocationPtrWithReplicaInfo>(context);
        *this = TAugmentedStoredChunkReplicaPtr(chunkLocation.GetPtr(), chunkLocation.GetReplicaIndex(), chunkLocation.GetReplicaState());
    }
}

NObjectClient::TObjectId TAugmentedStoredChunkReplicaPtr::GetId() const
{
    switch (GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            auto locationReplica = As<EStoredReplicaType::ChunkLocation>();
            auto* location = locationReplica->AsChunkLocationPtr();
            return IsObjectAlive(location) ? location->GetId() : NullObjectId;
        }
        case EStoredReplicaType::OffshoreMedia: {
            auto* medium = As<EStoredReplicaType::OffshoreMedia>()->AsMediumPtr();
            return IsObjectAlive(medium) ? medium->GetId() : NullObjectId;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkLocationIndex TAugmentedLocationChunkReplicaPtr::GetChunkLocationIndex() const
{
    auto* location = AsChunkLocationPtr();
    if (!IsObjectAlive(location)) {
        return InvalidChunkLocationIndex;
    }
    return location->GetIndex();
}

TChunkLocation* TAugmentedLocationChunkReplicaPtr::AsChunkLocationPtr() const
{
    YT_ASSERT(GetStoredReplicaType() == EStoredReplicaType::ChunkLocation);
    return reinterpret_cast<TChunkLocation*>(Value_ & 0x0000fffffffffffcLL);
}

////////////////////////////////////////////////////////////////////////////////

TMedium* TAugmentedMediumChunkReplicaPtr::AsMediumPtr() const
{
    YT_ASSERT(GetStoredReplicaType() == EStoredReplicaType::OffshoreMedia);
    return reinterpret_cast<TMedium*>(Value_ & 0x0000fffffffffffcLL);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TAugmentedStoredChunkReplicaPtr value, TStringBuf spec)
{
    switch (value.GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            FormatValue(builder, value.As<EStoredReplicaType::ChunkLocation>(), spec);
            break;
        }
        case EStoredReplicaType::OffshoreMedia: {
            FormatValue(builder, value.As<EStoredReplicaType::OffshoreMedia>(), spec);
            break;
        }
    }
}

void FormatValue(TStringBuilderBase* builder, TAugmentedLocationChunkReplicaPtr value, TStringBuf spec)
{
    FormatValue(builder, TChunkLocationPtrWithReplicaInfo(value.AsChunkLocationPtr(), value.GetReplicaIndex()), spec);
}

void FormatValue(TStringBuilderBase* builder, TAugmentedMediumChunkReplicaPtr value, TStringBuf spec)
{
    FormatValue(builder, TMediumPtrWithReplicaInfo(value.AsMediumPtr(), value.GetReplicaIndex()), spec);
}

void ToProto(ui64* protoValue, TAugmentedStoredChunkReplicaPtr value)
{
    switch (value.GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            ToProto(protoValue, *value.As<EStoredReplicaType::ChunkLocation>());
            break;
        }
        case EStoredReplicaType::OffshoreMedia: {
            ToProto(protoValue, *value.As<EStoredReplicaType::OffshoreMedia>());
            break;
        }
    }
}

void ToProto(ui64* protoValue, TAugmentedLocationChunkReplicaPtr value)
{
    ToProto(protoValue, TChunkLocationPtrWithReplicaInfo(value.AsChunkLocationPtr(), value.GetReplicaIndex()));
}

void ToProto(ui64* protoValue, TAugmentedMediumChunkReplicaPtr value)
{
    ToProto(protoValue, TMediumPtrWithReplicaInfo(value.AsMediumPtr(), value.GetReplicaIndex()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
