#pragma once

#include "public.h"
#include "chunk_replica.h"
#include "chunk_location.h"
#include <yt/yt/server/master/chunk_server/medium_base.h>
#include <yt/yt/server/master/node_tracker_server/node.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStoredReplicaType,
    ((ChunkLocation)   (0))
    ((OffshoreMedia)   (1))
);

static_assert(
    static_cast<int>(TEnumTraits<EStoredReplicaType>::GetMaxValue()) < (1LL << 3),
    "Stored replica replica type must fit into 2 bits.");

//! A compact representation for |(variant(TChunkLocation*, TMedium*), replica_index, replica_state)|.
class TStoredChunkReplicaPtrWithReplicaInfo
    // : public TAugmentationAccessor<TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>>
{
private:
    // static_assert(1 <= IndexCount && IndexCount <= 2);
    // friend class TAugmentationAccessor<TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>>;
    // friend class TStoredReplicaAugmentedPtr<!WithReplicaState>;

public:
    TStoredChunkReplicaPtrWithReplicaInfo() {}
    template <class T>
    TStoredChunkReplicaPtrWithReplicaInfo(T* ptr, int index, EChunkReplicaState replicaState = EChunkReplicaState::Generic)
        requires ((std::is_same_v<T, TChunkLocation> || std::is_same_v<T, TMedium>))
    : Value_(
        reinterpret_cast<uintptr_t>(ptr) |
        static_cast<uintptr_t>(replicaState) |
        (static_cast<uintptr_t>(index) << 56) |
        (static_cast<uintptr_t>((std::is_same_v<T, TChunkLocation> ? EStoredReplicaType::ChunkLocation : EStoredReplicaType::OffshoreMedia)) << 48))
    {
        YT_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xffff000000000003LL) == 0);
        YT_ASSERT(index >= 0 && index <= 0xff);
        YT_ASSERT(static_cast<uintptr_t>(replicaState) <= 0x3);
    }

    TStoredChunkReplicaPtrWithReplicaInfo(const TStoredChunkReplicaPtrWithReplicaInfo& other) = default;
    TStoredChunkReplicaPtrWithReplicaInfo(TStoredChunkReplicaPtrWithReplicaInfo&& other) = default;

    TStoredChunkReplicaPtrWithReplicaInfo& operator=(const TStoredChunkReplicaPtrWithReplicaInfo& other) = default;

    bool HasPtr() const
    {
        return Value_ & 0x0000fffffffffffcLL;
    }

    bool IsChunkLocationPtr() const
    {
        return GetStoredReplicaType() == EStoredReplicaType::ChunkLocation;
    }

    bool IsMediumPtr() const
    {
        return GetStoredReplicaType() == EStoredReplicaType::OffshoreMedia;
    }

    TChunkLocation* AsChunkLocationPtr() const
    {
        YT_ASSERT(GetStoredReplicaType() == EStoredReplicaType::ChunkLocation);
        return reinterpret_cast<TChunkLocation*>(Value_ & 0x0000fffffffffffcLL);
    }

    TMedium* AsMediumPtr() const
    {
        YT_ASSERT(GetStoredReplicaType() == EStoredReplicaType::OffshoreMedia);
        return reinterpret_cast<TMedium*>(Value_ & 0x0000fffffffffffcLL);
    }

    size_t GetHash() const;

    bool operator==(TStoredChunkReplicaPtrWithReplicaInfo other) const
    {
        auto thisStoredReplicaType = GetStoredReplicaType();
        auto otherStoredReplicaType = other.GetStoredReplicaType();
        return std::tuple(thisStoredReplicaType, GetId()) == std::tuple(otherStoredReplicaType, other.GetId());
    }
    bool operator!=(TStoredChunkReplicaPtrWithReplicaInfo other) const
    {
        auto thisStoredReplicaType = GetStoredReplicaType();
        auto otherStoredReplicaType = other.GetStoredReplicaType();
        return std::tuple(thisStoredReplicaType, GetId()) != std::tuple(otherStoredReplicaType, other.GetId());
    }

    bool operator< (TStoredChunkReplicaPtrWithReplicaInfo other) const
    {
        auto thisStoredReplicaType = GetStoredReplicaType();
        auto otherStoredReplicaType = other.GetStoredReplicaType();
        return std::tuple(thisStoredReplicaType, GetId()) < std::tuple(otherStoredReplicaType, other.GetId());
    }
    bool operator<=(TStoredChunkReplicaPtrWithReplicaInfo other) const
    {
        auto thisStoredReplicaType = GetStoredReplicaType();
        auto otherStoredReplicaType = other.GetStoredReplicaType();
        return std::tuple(thisStoredReplicaType, GetId()) <= std::tuple(otherStoredReplicaType, other.GetId());
    }
    bool operator> (TStoredChunkReplicaPtrWithReplicaInfo other) const
    {
        auto thisStoredReplicaType = GetStoredReplicaType();
        auto otherStoredReplicaType = other.GetStoredReplicaType();
        return std::tuple(thisStoredReplicaType, GetId()) > std::tuple(otherStoredReplicaType, other.GetId());
    }

    bool operator>=(TStoredChunkReplicaPtrWithReplicaInfo other) const
    {
        auto thisStoredReplicaType = GetStoredReplicaType();
        auto otherStoredReplicaType = other.GetStoredReplicaType();
        return std::tuple(thisStoredReplicaType, GetId()) >= std::tuple(otherStoredReplicaType, other.GetId());
    }

    template <class C>
    void Save(C& context) const
    {
        using NYT::Save;
        auto type = GetStoredReplicaType();
        Save(context, type);
        Save(context, GetReplicaIndex());
        Save(context, GetReplicaState());

        switch (type) {
                case EStoredReplicaType::ChunkLocation: {
                    SaveWith<NCellMaster::TRawNonversionedObjectPtrSerializer>(context, AsChunkLocationPtr());
                    break;
                }
                case EStoredReplicaType::OffshoreMedia: {
                    SaveWith<NCellMaster::TRawNonversionedObjectPtrSerializer>(context, AsMediumPtr());
                    break;
                }
            }
        
    }
    template <class C>
    void Load(C& context)
    {
        using NYT::Load;

        if (context.GetVersion() < NCellMaster::EMasterReign::RefactoringAroundChunkStoredReplicas) {
            auto chunkLocation = Load<TChunkLocationPtrWithReplicaInfo>(context);
            *this = TStoredChunkReplicaPtrWithReplicaInfo(chunkLocation.GetPtr(), chunkLocation.GetReplicaIndex(), chunkLocation.GetReplicaState());
        } else {
            auto type = Load<EStoredReplicaType>(context);
            int index = Load<ui8>(context);
            auto state = Load<EChunkReplicaState>(context);
            switch (type) {
                case EStoredReplicaType::ChunkLocation: {
                    auto* ptr = LoadWith<NCellMaster::TRawNonversionedObjectPtrSerializer, TChunkLocation*>(context);
                    *this = TStoredChunkReplicaPtrWithReplicaInfo(ptr, index,state);
                    break;
                }
                case EStoredReplicaType::OffshoreMedia: {
                    auto* ptr = LoadWith<NCellMaster::TRawNonversionedObjectPtrSerializer, TMedium*>(context);
                    *this = TStoredChunkReplicaPtrWithReplicaInfo(ptr, index,state);
                    break;
                }
            }
        }
    }

    TStoredChunkReplicaPtrWithReplicaInfo ToGenericState() const
    {
        auto type = GetStoredReplicaType();
        switch (type) {
            case EStoredReplicaType::ChunkLocation:
                return TStoredChunkReplicaPtrWithReplicaInfo(AsChunkLocationPtr(), GetReplicaIndex());
            case EStoredReplicaType::OffshoreMedia:
                return TStoredChunkReplicaPtrWithReplicaInfo(AsMediumPtr(), GetReplicaIndex());
        }
    }

    EChunkReplicaState GetReplicaState() const
    {
        return static_cast<EChunkReplicaState>(Value_ & 0x3);
    }

    // todo: -> ui8
    int GetReplicaIndex() const
    {
        return static_cast<int>((Value_ >> (56)) & 0xff);
    }

    int GetEffectiveMediumIndex() const
    {
        auto type = GetStoredReplicaType();
        switch (type) {
        case EStoredReplicaType::ChunkLocation:
            return AsChunkLocationPtr()->GetEffectiveMediumIndex();
        case EStoredReplicaType::OffshoreMedia:
            return AsMediumPtr()->GetIndex();
        }
    }

    NNodeTrackerClient::TChunkLocationIndex GetChunkLocationIndex() const
    {
        switch (GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            auto* location = AsChunkLocationPtr();
            if (!NObjectServer::IsObjectAlive(location)) {
                return NNodeTrackerClient::InvalidChunkLocationIndex;
            }
            return location->GetIndex();
        }
        case EStoredReplicaType::OffshoreMedia:
            return NNodeTrackerClient::InvalidChunkLocationIndex;
        }
    }
    NChunkClient::TChunkLocationUuid GetLocationUuid() const
    {
        switch (GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            auto* location = AsChunkLocationPtr();
            if (!NObjectServer::IsObjectAlive(location)) {
                return NChunkClient::InvalidChunkLocationUuid;
            }
            return location->GetUuid();
        }
        case EStoredReplicaType::OffshoreMedia:
            return NChunkClient::InvalidChunkLocationUuid;
        }
    }
    NNodeTrackerClient::TNodeId GetNodeId() const
    {
        switch (GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation: {
            auto* location = AsChunkLocationPtr();
            if (!NObjectServer::IsObjectAlive(location)) {
                return NNodeTrackerClient::InvalidNodeId;
            }
            auto node = location->GetNode();
            if (!NObjectServer::IsObjectAlive(node)) {
                return NNodeTrackerClient::InvalidNodeId;
            }
            return node->GetId();
        }
        case EStoredReplicaType::OffshoreMedia:
            return NNodeTrackerClient::OffshoreNodeId;
        }
    }

private:
    static_assert(sizeof(uintptr_t) == 8, "Pointer type must be of size 8.");

    // Use compact 8-byte representation with index occupying the highest 8 bits.
    uintptr_t Value_;

    EStoredReplicaType GetStoredReplicaType() const
    {
        return static_cast<EStoredReplicaType>((Value_ >> (48)) & 0xff);
    }

    NObjectClient::TObjectId GetId() const
    {
        switch (GetStoredReplicaType()) {
        case EStoredReplicaType::ChunkLocation:
            return AsChunkLocationPtr()->GetId();
        case EStoredReplicaType::OffshoreMedia:
            return AsMediumPtr()->GetId();
        }
    }

    friend void FormatValue(TStringBuilderBase* builder, TStoredChunkReplicaPtrWithReplicaInfo value, TStringBuf spec);
    friend void ToProto(ui64* protoValue, TStoredChunkReplicaPtrWithReplicaInfo value);
};

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TStoredChunkReplicaPtrWithReplicaInfo, 8);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TStoredChunkReplicaPtrWithReplicaInfo value, TStringBuf spec);

//! Serializes node id, replica index, medium index.
void ToProto(ui64* protoValue, TStoredChunkReplicaPtrWithReplicaInfo value);

////////////////////////////////////////////////////////////////////////////////

class TStoredReplica
{
public:
    TStoredReplica();
    TStoredReplica(const TChunkLocationPtrWithReplicaInfo& chunkLocation);
    TStoredReplica(const TMediumPtrWithReplicaInfo& medium);

    std::strong_ordering operator<=> (const TStoredReplica& other) const = default;

    bool IsChunkLocation() const;
    TChunkLocationPtrWithReplicaInfo& AsChunkLocation();
    const TChunkLocationPtrWithReplicaInfo& AsChunkLocation() const;

    bool IsMedium() const;
    TMediumPtrWithReplicaInfo& AsMedium();
    const TMediumPtrWithReplicaInfo& AsMedium() const;

    TStoredReplica ToGenericState() const;

    int GetEffectiveMediumIndex() const;
    int GetReplicaIndex() const;

    EChunkReplicaState GetReplicaState() const;

    NNodeTrackerClient::TChunkLocationIndex GetChunkLocationIndex() const;
    NChunkClient::TChunkLocationUuid GetLocationUuid() const;
    NNodeTrackerClient::TNodeId GetNodeId() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
private:
    std::variant<TChunkLocationPtrWithReplicaInfo, TMediumPtrWithReplicaInfo> ReplicaInfo_;

    friend void FormatValue(TStringBuilderBase* builder, TStoredReplica value, TStringBuf spec);
    friend void ToProto(ui64* protoValue, TStoredReplica value);
};

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TStoredReplica, 16);

using TStoredReplicaList = TCompactVector<TStoredChunkReplicaPtrWithReplicaInfo, TypicalReplicaCount>;
using TChunkToStoredReplicaList = THashMap<TChunkId, TErrorOr<TStoredReplicaList>>;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TStoredReplica value, TStringBuf spec);

//! Serializes node id, replica index, medium index.
void ToProto(ui64* protoValue, TStoredReplica value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
