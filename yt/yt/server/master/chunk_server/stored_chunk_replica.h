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

//! Custom class for variant stored chunk replica. It is similar to TAugmentedPtr, but due to variety of stored pointer has to be separate class.
//! It stores compact representation for |(variant(TChunkLocation*, TMedium*), replica_index, replica_state)|.
class TAugmentedStoredChunkReplicaPtr
{
public:
    TAugmentedStoredChunkReplicaPtr() = default;
    template <class T>
    TAugmentedStoredChunkReplicaPtr(T* ptr, int index, EChunkReplicaState replicaState = EChunkReplicaState::Generic)
        requires ((std::is_same_v<T, TChunkLocation> || std::is_same_v<T, TMedium>));

    TAugmentedStoredChunkReplicaPtr(const TAugmentedStoredChunkReplicaPtr& other) = default;
    TAugmentedStoredChunkReplicaPtr(TAugmentedStoredChunkReplicaPtr&& other) = default;

    TAugmentedStoredChunkReplicaPtr& operator=(const TAugmentedStoredChunkReplicaPtr& other) = default;
    TAugmentedStoredChunkReplicaPtr& operator=(TAugmentedStoredChunkReplicaPtr&& other) = default;

    bool HasPtr() const;

    bool IsChunkLocationPtr() const;
    bool IsMediumPtr() const;

    TChunkLocation* AsChunkLocationPtr() const;
    TMedium* AsMediumPtr() const;

    bool operator==(TAugmentedStoredChunkReplicaPtr other) const;
    bool operator<(TAugmentedStoredChunkReplicaPtr other) const;
    bool operator<=(TAugmentedStoredChunkReplicaPtr other) const;
    bool operator>(TAugmentedStoredChunkReplicaPtr other) const;
    bool operator>=(TAugmentedStoredChunkReplicaPtr other) const;

    template <class C>
    void Save(C& context) const;

    template <class C>
    void Load(C& context);

    TAugmentedStoredChunkReplicaPtr ToGenericState() const;

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

    friend void FormatValue(TStringBuilderBase* builder, TAugmentedStoredChunkReplicaPtr value, TStringBuf spec);
    friend void ToProto(ui64* protoValue, TAugmentedStoredChunkReplicaPtr value);
};

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TAugmentedStoredChunkReplicaPtr, 8);

////////////////////////////////////////////////////////////////////////////////

using TStoredChunkReplicaList = TCompactVector<TAugmentedStoredChunkReplicaPtr, TypicalReplicaCount>;
using TChunkToStoredChunkReplicaList = THashMap<TChunkId, TErrorOr<TStoredChunkReplicaList>>;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TAugmentedStoredChunkReplicaPtr value, TStringBuf spec);

void ToProto(ui64* protoValue, TAugmentedStoredChunkReplicaPtr value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define STORED_CHUNK_REPLICA_INL_H_
#include "stored_chunk_replica-inl.h"
#undef STORED_CHUNK_REPLICA_INL_H_
