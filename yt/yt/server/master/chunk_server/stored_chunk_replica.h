#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/assert_sizeof.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EStoredReplicaType, ui8,
    ((ChunkLocation)   (0))
    ((OffshoreMedia)   (1))
);

static_assert(
    static_cast<int>(TEnumTraits<EStoredReplicaType>::GetMaxValue()) < (1LL << 8),
    "Stored replica type must fit into 8 bits.");

//! Custom class for variant chunk replica. It is similar to TAugmentedPtr, but due to variety of stored pointer has to be separate class.
//! It stores compact representation for |(variant(TChunkLocation*, TMedium*), replica_index, replica_state)|.
class TStoredChunkReplicaPtrWithReplicaInfo
{
public:
    TStoredChunkReplicaPtrWithReplicaInfo() = default;
    template <class T>
    TStoredChunkReplicaPtrWithReplicaInfo(T* ptr, int index, EChunkReplicaState replicaState = EChunkReplicaState::Generic)
        requires ((std::is_same_v<T, TChunkLocation> || std::is_same_v<T, TMedium>));

    TStoredChunkReplicaPtrWithReplicaInfo(const TStoredChunkReplicaPtrWithReplicaInfo& other) = default;
    TStoredChunkReplicaPtrWithReplicaInfo(TStoredChunkReplicaPtrWithReplicaInfo&& other) = default;

    TStoredChunkReplicaPtrWithReplicaInfo& operator=(const TStoredChunkReplicaPtrWithReplicaInfo& other) = default;

    bool HasPtr() const;

    bool IsChunkLocationPtr() const;
    bool IsMediumPtr() const;

    TChunkLocation* AsChunkLocationPtr() const;
    TMedium* AsMediumPtr() const;

    bool operator==(TStoredChunkReplicaPtrWithReplicaInfo other) const;
    bool operator<(TStoredChunkReplicaPtrWithReplicaInfo other) const;
    bool operator<=(TStoredChunkReplicaPtrWithReplicaInfo other) const;
    bool operator>(TStoredChunkReplicaPtrWithReplicaInfo other) const;
    bool operator>=(TStoredChunkReplicaPtrWithReplicaInfo other) const;

    template <class C>
    void Save(C& context) const;

    template <class C>
    void Load(C& context);

    TStoredChunkReplicaPtrWithReplicaInfo ToGenericState() const;

    EChunkReplicaState GetReplicaState() const;

    int GetReplicaIndex() const;
    int GetEffectiveMediumIndex() const;
    NNodeTrackerClient::TChunkLocationIndex GetChunkLocationIndex() const;
    NChunkClient::TChunkLocationUuid GetLocationUuid() const;
    NNodeTrackerClient::TNodeId GetNodeId() const;

private:
    static_assert(sizeof(uintptr_t) == 8, "Pointer type must be of size 8.");

    // Use compact 8-byte representation.
    // |replica_index, replica_type, (variant(TChunkLocation*, TMedium*), replica_state|
    // |       8 bits,       8 bits,                             46 bits,        2 bits|
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

#define STORED_CHUNK_REPLICA_INL_H_
#include "stored_chunk_replica-inl.h"
#undef STORED_CHUNK_REPLICA_INL_H_
