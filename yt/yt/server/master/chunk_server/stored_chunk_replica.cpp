#include "stored_chunk_replica.h"

#include "medium_base.h"
#include "chunk_location.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;
using namespace NObjectServer;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TAugmentedStoredChunkReplicaPtr::operator bool() const
{
    return Value_ & 0x0000fffffffffffcLL;
}

bool TAugmentedStoredChunkReplicaPtr::IsChunkLocationPtr() const
{
    return GetStoredReplicaType() == EStoredReplicaType::ChunkLocation;
}

bool TAugmentedStoredChunkReplicaPtr::IsMediumPtr() const
{
    return GetStoredReplicaType() == EStoredReplicaType::OffshoreMedia;
}

TChunkLocation* TAugmentedStoredChunkReplicaPtr::AsChunkLocationPtr() const
{
    YT_ASSERT(GetStoredReplicaType() == EStoredReplicaType::ChunkLocation);
    return reinterpret_cast<TChunkLocation*>(Value_ & 0x0000fffffffffffcLL);
}

TMedium* TAugmentedStoredChunkReplicaPtr::AsMediumPtr() const
{
    YT_ASSERT(GetStoredReplicaType() == EStoredReplicaType::OffshoreMedia);
    return reinterpret_cast<TMedium*>(Value_ & 0x0000fffffffffffcLL);
}

bool TAugmentedStoredChunkReplicaPtr::operator==(TAugmentedStoredChunkReplicaPtr other) const
{
    return Value_ == other.Value_;
}

bool TAugmentedStoredChunkReplicaPtr::operator<(TAugmentedStoredChunkReplicaPtr other) const
{
    auto thisStoredReplicaType = GetStoredReplicaType();
    auto otherStoredReplicaType = other.GetStoredReplicaType();

    auto thisReplicaIndex = GetReplicaIndex();
    auto otherReplicaIndex = other.GetReplicaIndex();

    auto thisState = GetReplicaState();
    auto otherState = other.GetReplicaState();

    return std::tuple(thisStoredReplicaType, thisReplicaIndex, thisState, GetId()) < std::tuple(otherStoredReplicaType, otherReplicaIndex, otherState, other.GetId());
}

bool TAugmentedStoredChunkReplicaPtr::operator<=(TAugmentedStoredChunkReplicaPtr other) const
{
    auto thisStoredReplicaType = GetStoredReplicaType();
    auto otherStoredReplicaType = other.GetStoredReplicaType();

    auto thisReplicaIndex = GetReplicaIndex();
    auto otherReplicaIndex = other.GetReplicaIndex();

    auto thisState = GetReplicaState();
    auto otherState = other.GetReplicaState();
    return std::tuple(thisStoredReplicaType, thisReplicaIndex, thisState, GetId()) <= std::tuple(otherStoredReplicaType, otherReplicaIndex, otherState, other.GetId());
}

bool TAugmentedStoredChunkReplicaPtr::operator>(TAugmentedStoredChunkReplicaPtr other) const
{
    return !operator<=(other);
}

bool TAugmentedStoredChunkReplicaPtr::operator>=(TAugmentedStoredChunkReplicaPtr other) const
{
    return !operator<(other);
}

TAugmentedStoredChunkReplicaPtr TAugmentedStoredChunkReplicaPtr::ToGenericState() const
{
    auto type = GetStoredReplicaType();
    switch (type) {
        case EStoredReplicaType::ChunkLocation:
            return TAugmentedStoredChunkReplicaPtr(AsChunkLocationPtr(), GetReplicaIndex());
        case EStoredReplicaType::OffshoreMedia:
            return TAugmentedStoredChunkReplicaPtr(AsMediumPtr(), GetReplicaIndex());
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
        return AsChunkLocationPtr()->GetEffectiveMediumIndex();
    case EStoredReplicaType::OffshoreMedia:
        return AsMediumPtr()->GetIndex();
    }
}

TChunkLocationIndex TAugmentedStoredChunkReplicaPtr::GetChunkLocationIndex() const
{
    switch (GetStoredReplicaType()) {
    case EStoredReplicaType::ChunkLocation: {
        auto* location = AsChunkLocationPtr();
        if (!IsObjectAlive(location)) {
            return InvalidChunkLocationIndex;
        }
        return location->GetIndex();
    }
    case EStoredReplicaType::OffshoreMedia:
        return InvalidChunkLocationIndex;
    }
}

TChunkLocationUuid TAugmentedStoredChunkReplicaPtr::GetLocationUuid() const
{
    switch (GetStoredReplicaType()) {
    case EStoredReplicaType::ChunkLocation: {
        auto* location = AsChunkLocationPtr();
        if (!IsObjectAlive(location)) {
            return InvalidChunkLocationUuid;
        }
        return location->GetUuid();
    }
    case EStoredReplicaType::OffshoreMedia:
        return InvalidChunkLocationUuid;
    }
}

TNodeId TAugmentedStoredChunkReplicaPtr::GetNodeId() const
{
    switch (GetStoredReplicaType()) {
    case EStoredReplicaType::ChunkLocation: {
        auto* location = AsChunkLocationPtr();
        if (!IsObjectAlive(location)) {
            return InvalidNodeId;
        }
        auto node = location->GetNode();
        if (!IsObjectAlive(node)) {
            return InvalidNodeId;
        }
        return node->GetId();
    }
    case EStoredReplicaType::OffshoreMedia:
        return OffshoreNodeId;
    }
}

EStoredReplicaType TAugmentedStoredChunkReplicaPtr::GetStoredReplicaType() const
{
    return static_cast<EStoredReplicaType>((Value_ >> 48) & 0xff);
}

NObjectClient::TObjectId TAugmentedStoredChunkReplicaPtr::GetId() const
{
    switch (GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            auto* location = AsChunkLocationPtr();
            return IsObjectAlive(location) ? location->GetId() : NullObjectId;
        }
        case EStoredReplicaType::OffshoreMedia: {
            auto* medium = AsMediumPtr();
            return IsObjectAlive(medium) ? medium->GetId() : NullObjectId;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TAugmentedStoredChunkReplicaPtr value, TStringBuf spec)
{
    switch (value.GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            FormatValue(builder, TChunkLocationPtrWithReplicaInfo(value.AsChunkLocationPtr(), value.GetReplicaIndex()), spec);
            break;
        }
        case EStoredReplicaType::OffshoreMedia:
        {
            FormatValue(builder, TMediumPtrWithReplicaInfo(value.AsMediumPtr(), value.GetReplicaIndex()), spec);
            break;
        }
    }
}

void ToProto(ui64* protoValue, TAugmentedStoredChunkReplicaPtr value)
{
    switch (value.GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            ToProto(protoValue, TChunkLocationPtrWithReplicaInfo(value.AsChunkLocationPtr(), value.GetReplicaIndex()));
            break;
        }
        case EStoredReplicaType::OffshoreMedia: {
            ToProto(protoValue, TMediumPtrWithReplicaInfo(value.AsMediumPtr(), value.GetReplicaIndex()));
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
