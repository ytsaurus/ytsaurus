#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/misc/assert_sizeof.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EStoredReplicaType, ui8,
    ((ChunkLocation)   (0))
    ((OffshoreMedia)   (1))
);

template <EStoredReplicaType type>
struct TStoredReplicaTraits;

static_assert(
    static_cast<int>(TEnumTraits<EStoredReplicaType>::GetMaxValue()) < (1LL << 8),
    "Stored replica type must fit into 8 bits.");

////////////////////////////////////////////////////////////////////////////////

// Stores compact representation for |(variant(TChunkLocation*, TMedium*), replica_index, replica_state)|.
// Similar to TAugmentedPtr but not limited to a single fixed pointer type.
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

    explicit operator bool() const;

    EStoredReplicaType GetStoredReplicaType() const;
    template <EStoredReplicaType type>
    const typename TStoredReplicaTraits<type>::Type* As() const;

    bool operator==(TAugmentedStoredChunkReplicaPtr other) const;
    bool operator<(TAugmentedStoredChunkReplicaPtr other) const;
    bool operator<=(TAugmentedStoredChunkReplicaPtr other) const;
    bool operator>(TAugmentedStoredChunkReplicaPtr other) const;
    bool operator>=(TAugmentedStoredChunkReplicaPtr other) const;

    TAugmentedStoredChunkReplicaPtr ToGenericState() const;

    EChunkReplicaState GetReplicaState() const;

    int GetReplicaIndex() const;
    int GetEffectiveMediumIndex() const;
    NChunkClient::TChunkLocationUuid GetLocationUuid() const;
    NNodeTrackerClient::TNodeId GetNodeId() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

protected:
    static_assert(sizeof(uintptr_t) == 8, "Pointer type must be of size 8.");

    // Use compact 8-byte representation.
    // |replica_index, replica_type, (variant(TChunkLocation*, TMedium*), replica_state|
    // |       8 bits,       8 bits,                             46 bits,        2 bits|
    uintptr_t Value_;

    NObjectClient::TObjectId GetId() const;

    friend void FormatValue(TStringBuilderBase* builder, TAugmentedStoredChunkReplicaPtr value, TStringBuf spec);
    friend void ToProto(ui64* protoValue, TAugmentedStoredChunkReplicaPtr value);
};

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TAugmentedStoredChunkReplicaPtr, 8);

////////////////////////////////////////////////////////////////////////////////

class TAugmentedLocationChunkReplicaPtr
    : public TAugmentedStoredChunkReplicaPtr
{
public:
    NNodeTrackerClient::TChunkLocationIndex GetChunkLocationIndex() const;

    TChunkLocation* AsChunkLocationPtr() const;
};

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TAugmentedLocationChunkReplicaPtr, 8);

////////////////////////////////////////////////////////////////////////////////

class TAugmentedMediumChunkReplicaPtr
    : public TAugmentedStoredChunkReplicaPtr
{
public:
    TMedium* AsMediumPtr() const;
};

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TAugmentedMediumChunkReplicaPtr, 8);

////////////////////////////////////////////////////////////////////////////////

using TStoredChunkReplicaList = TCompactVector<TAugmentedStoredChunkReplicaPtr, TypicalReplicaCount>;
using TChunkToStoredChunkReplicaList = THashMap<TChunkId, TErrorOr<TStoredChunkReplicaList>>;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TAugmentedStoredChunkReplicaPtr value, TStringBuf spec);
void FormatValue(TStringBuilderBase* builder, TAugmentedLocationChunkReplicaPtr value, TStringBuf spec);
void FormatValue(TStringBuilderBase* builder, TAugmentedMediumChunkReplicaPtr value, TStringBuf spec);

void ToProto(ui64* protoValue, TAugmentedStoredChunkReplicaPtr value);
void ToProto(ui64* protoValue, TAugmentedLocationChunkReplicaPtr value);
void ToProto(ui64* protoValue, TAugmentedMediumChunkReplicaPtr value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define STORED_CHUNK_REPLICA_INL_H_
#include "stored_chunk_replica-inl.h"
#undef STORED_CHUNK_REPLICA_INL_H_
