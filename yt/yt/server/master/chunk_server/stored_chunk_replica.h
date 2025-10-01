#pragma once

#include "public.h"
// COMPAT(cherepashka): drop after RefactoringAroundChunkStoredReplicas will be removed.
#include "chunk_replica.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EStoredReplicaType, ui8,
    ((ChunkLocation)   (0))
    ((OffshoreMedia)   (1))
);

static_assert(
    static_cast<int>(TEnumTraits<EStoredReplicaType>::GetMaxValue()) < (1LL << 3),
    "Stored replica type must fit into 2 bits.");

//! A compact representation for |(variant(TChunkLocation*, TMedium*), replica_index, replica_state)|.
class TStoredChunkReplicaPtrWithReplicaInfo
{
public:
    TStoredChunkReplicaPtrWithReplicaInfo();
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

    bool HasPtr() const;

    bool IsChunkLocationPtr() const;
    bool IsMediumPtr() const;

    TChunkLocation* AsChunkLocationPtr() const;
    TMedium* AsMediumPtr() const;

    // size_t GetHash() const;

    bool operator== (TStoredChunkReplicaPtrWithReplicaInfo other) const
    {
        auto thisStoredReplicaType = GetStoredReplicaType();
        auto otherStoredReplicaType = other.GetStoredReplicaType();
        return std::tuple(thisStoredReplicaType, GetId()) == std::tuple(otherStoredReplicaType, other.GetId());
    }
    // bool operator!=(TStoredChunkReplicaPtrWithReplicaInfo other) const
    // {
    //     auto thisStoredReplicaType = GetStoredReplicaType();
    //     auto otherStoredReplicaType = other.GetStoredReplicaType();
    //     return std::tuple(thisStoredReplicaType, GetId()) != std::tuple(otherStoredReplicaType, other.GetId());
    // }

    bool operator< (TStoredChunkReplicaPtrWithReplicaInfo other) const
    {
        auto thisStoredReplicaType = GetStoredReplicaType();
        auto otherStoredReplicaType = other.GetStoredReplicaType();
        return std::tuple(thisStoredReplicaType, GetId()) < std::tuple(otherStoredReplicaType, other.GetId());
    }
    bool operator<= (TStoredChunkReplicaPtrWithReplicaInfo other) const
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

    bool operator>= (TStoredChunkReplicaPtrWithReplicaInfo other) const
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

    TStoredChunkReplicaPtrWithReplicaInfo ToGenericState() const;

    EChunkReplicaState GetReplicaState() const;

    // todo: -> ui8
    int GetReplicaIndex() const;
    int GetEffectiveMediumIndex() const;
    NNodeTrackerClient::TChunkLocationIndex GetChunkLocationIndex() const;
    NChunkClient::TChunkLocationUuid GetLocationUuid() const;
    NNodeTrackerClient::TNodeId GetNodeId() const;

private:
    static_assert(sizeof(uintptr_t) == 8, "Pointer type must be of size 8.");

    // Use compact 8-byte representation.
    // |replica_index, replica_type, (variant(TChunkLocation*, TMedium*), replica_state)|
    // |       8 bits,       8 bits,
    uintptr_t Value_;

    EStoredReplicaType GetStoredReplicaType() const;

    NObjectClient::TObjectId GetId() const;

    friend void FormatValue(TStringBuilderBase* builder, TStoredChunkReplicaPtrWithReplicaInfo value, TStringBuf spec);
    friend void ToProto(ui64* protoValue, TStoredChunkReplicaPtrWithReplicaInfo value);
};

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TStoredChunkReplicaPtrWithReplicaInfo, 8);

////////////////////////////////////////////////////////////////////////////////

using TStoredChunkReplicaPtrWithReplicaInfoList = TCompactVector<TStoredChunkReplicaPtrWithReplicaInfo, TypicalReplicaCount>;
using TChunkToStoredChunkReplicaPtrWithReplicaInfoList = THashMap<TChunkId, TErrorOr<TStoredChunkReplicaPtrWithReplicaInfoList>>;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TStoredChunkReplicaPtrWithReplicaInfo value, TStringBuf spec);

//! Serializes node id, replica index, medium index.
void ToProto(ui64* protoValue, TStoredChunkReplicaPtrWithReplicaInfo value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
