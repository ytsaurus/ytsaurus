#pragma once

#include "private.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A compact representation for |(T*, index1, index2, replica_state)|.
//! Compact indices are stored in the upper 16 bits of the pointer value.
//! Extra indices are stored as plain integer fields.
//! NB: Storing replica state requires T* to be aligned to at least 4 bytes.
template <class T, bool WithReplicaState, int CompactIndexCount, int ExtraIndexCount, template <class> class TAugmentationAccessor>
class TAugmentedPtr
    : public TAugmentationAccessor<TAugmentedPtr<T, WithReplicaState, CompactIndexCount, ExtraIndexCount, TAugmentationAccessor>>
{
private:
    static constexpr int TotalIndexCount = CompactIndexCount + ExtraIndexCount;

    static_assert(0 <= CompactIndexCount && CompactIndexCount <= 2);
    static_assert(0 <= ExtraIndexCount && ExtraIndexCount <= 1);
    static_assert(1 <= TotalIndexCount && TotalIndexCount <= 2);

    friend class TAugmentationAccessor<TAugmentedPtr<T, WithReplicaState, CompactIndexCount, ExtraIndexCount, TAugmentationAccessor>>;
    friend class TAugmentedPtr<T, !WithReplicaState, CompactIndexCount, ExtraIndexCount, TAugmentationAccessor>;

public:
    TAugmentedPtr();

    TAugmentedPtr(T* ptr, int index)
        requires (!WithReplicaState && TotalIndexCount == 1);

    TAugmentedPtr(T* ptr, int index, EChunkReplicaState replicaState = EChunkReplicaState::Generic)
        requires (WithReplicaState && TotalIndexCount == 1);

    TAugmentedPtr(T* ptr, int firstIndex, int secondIndex)
        requires (!WithReplicaState && TotalIndexCount == 2);

    TAugmentedPtr(T* ptr, int firstIndex, int secondIndex, EChunkReplicaState = EChunkReplicaState::Generic)
        requires (WithReplicaState && TotalIndexCount == 2);

    explicit TAugmentedPtr(TAugmentedPtr<T, true, CompactIndexCount, ExtraIndexCount, TAugmentationAccessor> other)
        requires (!WithReplicaState);

    explicit TAugmentedPtr(TAugmentedPtr<T, false, CompactIndexCount, ExtraIndexCount, TAugmentationAccessor> other)
        requires WithReplicaState;

    TAugmentedPtr(
        TAugmentedPtr<T, false, CompactIndexCount, ExtraIndexCount, TAugmentationAccessor> other,
        EChunkReplicaState state)
        requires WithReplicaState;


    T* GetPtr() const;

    size_t GetHash() const;

    bool operator==(TAugmentedPtr other) const;
    bool operator< (TAugmentedPtr other) const;
    bool operator<=(TAugmentedPtr other) const;
    bool operator> (TAugmentedPtr other) const;
    bool operator>=(TAugmentedPtr other) const;

    // COMPAT(achulkov2): There is nothing wrong with implementing Save/Load for extra indexes, however,
    // forbidding them helps us double check that is no place where medium index is currently serialized
    // as part of this class. At some point this limitation should be removed.
    template <class C>
    void Save(C& context) const
        requires (ExtraIndexCount == 0);
    template <class C>
    void Load(C& context)
        requires (ExtraIndexCount == 0);

    TAugmentedPtr ToGenericState() const
        requires WithReplicaState;

    EChunkReplicaState GetReplicaState() const
        requires WithReplicaState;

private:
    static_assert(sizeof(uintptr_t) == 8, "Pointer type must be of size 8.");

    //! Uses compact 8-byte representation with indices occupying the highest 16 bits.
    uintptr_t Value_;

    //! Extra full-sized index. Zero-cost if not requested.
    [[no_unique_address]] std::conditional_t<ExtraIndexCount >= 1, int, std::monostate> ExtraIndex1_{};

    template <int Index>
    int GetIndex() const
        requires (Index <= TotalIndexCount);
};

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TAugmentedPtrReplicaIndexAccessor
{
public:
    int GetReplicaIndex() const;
};

template <class TImpl>
class TAugmentedPtrMediumIndexAccessor
{
public:
    int GetMediumIndex() const;
};

template <class TImpl>
class TAugmentedPtrReplicaAndMediumIndexAccessor
{
public:
    int GetReplicaIndex() const;
    int GetMediumIndex() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
using TPtrWithMediumIndex = TAugmentedPtr<
    T,
    /*WithReplicaState*/ false,
    /*CompactIndexCount*/ 0,
    /*ExtraIndexCount*/ 1,
    NDetail::TAugmentedPtrMediumIndexAccessor>;

template <class T>
using TPtrWithReplicaIndex = TAugmentedPtr<
    T,
    /*WithReplicaState*/ false,
    /*CompactIndexCount*/ 1,
    /*ExtraIndexCount*/ 0,
    NDetail::TAugmentedPtrReplicaIndexAccessor>;

template <class T>
using TPtrWithReplicaInfo = TAugmentedPtr<
    T,
    /*WithReplicaState*/ true,
    /*CompactIndexCount*/ 1,
    /*ExtraIndexCount*/ 0,
    NDetail::TAugmentedPtrReplicaIndexAccessor>;

template <class T>
using TPtrWithReplicaAndMediumIndex = TAugmentedPtr<
    T,
    /*WithReplicaState*/ false,
    /*CompactIndexCount*/ 1,
    /*ExtraIndexCount*/ 1,
    NDetail::TAugmentedPtrReplicaAndMediumIndexAccessor>;

template <class T>
using TPtrWithReplicaInfoAndMediumIndex = TAugmentedPtr<
    T,
    /*WithReplicaState*/ true,
    /*CompactIndexCount*/ 1,
    /*ExtraIndexCount*/ 1,
    NDetail::TAugmentedPtrReplicaAndMediumIndexAccessor>;

////////////////////////////////////////////////////////////////////////////////

using TNodePtrWithReplicaIndex = TPtrWithReplicaIndex<NNodeTrackerServer::TNode>;
using TNodePtrWithReplicaIndexList = TCompactVector<TNodePtrWithReplicaIndex, TypicalReplicaCount>;
using TNodePtrWithReplicaInfo = TPtrWithReplicaInfo<NNodeTrackerServer::TNode>;
using TNodePtrWithReplicaInfoList = TCompactVector<TNodePtrWithReplicaInfo, TypicalReplicaCount>;
using TNodePtrWithReplicaAndMediumIndex = TPtrWithReplicaAndMediumIndex<NNodeTrackerServer::TNode>;
using TNodePtrWithReplicaAndMediumIndexList = TCompactVector<TNodePtrWithReplicaAndMediumIndex, TypicalReplicaCount>;
using TNodePtrWithReplicaInfoAndMediumIndex = TPtrWithReplicaInfoAndMediumIndex<NNodeTrackerServer::TNode>;
using TNodePtrWithReplicaInfoAndMediumIndexList = TCompactVector<TNodePtrWithReplicaInfoAndMediumIndex, TypicalReplicaCount>;

using TChunkLocationPtrWithReplicaIndex = TPtrWithReplicaIndex<TChunkLocation>;
using TChunkLocationPtrWithReplicaIndexList = TCompactVector<TChunkLocationPtrWithReplicaIndex, TypicalReplicaCount>;
using TChunkLocationPtrWithReplicaInfo = TPtrWithReplicaInfo<TChunkLocation>;
using TChunkLocationPtrWithReplicaInfoList = TCompactVector<TChunkLocationPtrWithReplicaInfo, TypicalReplicaCount>;
using TChunkToLocationPtrWithReplicaInfoList = THashMap<TChunkId, TErrorOr<TChunkLocationPtrWithReplicaInfoList>>;
using TChunkLocationPtrWithReplicaAndMediumIndex = TPtrWithReplicaAndMediumIndex<TChunkLocation>;
using TChunkLocationPtrWithReplicaAndMediumIndexList = TCompactVector<TChunkLocationPtrWithReplicaAndMediumIndex, TypicalReplicaCount>;

using TChunkPtrWithReplicaInfo = TPtrWithReplicaInfo<TChunk>;
using TChunkPtrWithReplicaIndex = TPtrWithReplicaIndex<TChunk>;
using TChunkPtrWithReplicaAndMediumIndex = TPtrWithReplicaAndMediumIndex<TChunk>;
using TChunkPtrWithMediumIndex = TPtrWithMediumIndex<TChunk>;

using TChunkReplicaIndexList = TCompactVector<int, ChunkReplicaIndexBound>;

using TChunkRepairQueue = std::list<TChunkPtrWithMediumIndex>;
using TChunkRepairQueueIterator = TChunkRepairQueue::iterator;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithReplicaIndex value, TStringBuf spec);

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithReplicaInfo value, TStringBuf spec);

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithReplicaAndMediumIndex value, TStringBuf spec);

void FormatValue(TStringBuilderBase* builder, TChunkLocationPtrWithReplicaIndex value, TStringBuf spec);

void FormatValue(TStringBuilderBase* builder, TChunkLocationPtrWithReplicaInfo value, TStringBuf spec);

//! Serializes node id, replica index, medium index.
void ToProto(ui64* protoValue, TNodePtrWithReplicaAndMediumIndex value);
// COMPAT(babenko)
//! Serializes node id, replica index; omits medium index.
void ToProto(ui32* protoValue, TNodePtrWithReplicaAndMediumIndex value);
//! Serializes node id, replica index.
void ToProto(ui32* protoValue, TNodePtrWithReplicaIndex value);
//! Serializes node id, replica index, medium index.
void ToProto(ui64* protoValue, TChunkLocationPtrWithReplicaIndex value);
void ToProto(ui64* protoValue, TChunkLocationPtrWithReplicaInfo value);
//! Serializes node id, replica index.
void ToProto(ui32* protoValue, TChunkLocationPtrWithReplicaIndex value);
void ToProto(ui32* protoValue, TChunkLocationPtrWithReplicaInfo value);

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TChunkIdWithIndex ToChunkIdWithIndex(TChunkPtrWithReplicaIndex chunkWithIndex);
NChunkClient::TChunkIdWithIndexes ToChunkIdWithIndexes(TChunkPtrWithReplicaAndMediumIndex chunkWithIndexes);

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaChunkReplica
{
    NChunkClient::TChunkId ChunkId = NObjectClient::NullObjectId;
    int ReplicaIndex = NChunkClient::GenericChunkReplicaIndex;
    NNodeTrackerClient::TNodeId NodeId = NNodeTrackerClient::InvalidNodeId;
    NNodeTrackerClient::TChunkLocationIndex LocationIndex = NNodeTrackerClient::InvalidChunkLocationIndex;
    // Not persisted, used for getting StoredReplicas attribute.
    EChunkReplicaState ReplicaState = EChunkReplicaState::Generic;

    std::strong_ordering operator<=>(const TSequoiaChunkReplica& other) const = default;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

void FromProto(TSequoiaChunkReplica* replica, const NProto::TSequoiaReplicaInfo& protoReplica);
void ToProto(NProto::TSequoiaReplicaInfo* protoReplica, const TSequoiaChunkReplica& replica);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TProtoTraits<NChunkServer::TNodePtrWithReplicaAndMediumIndex>
{
    using TSerialized = ui64;
};

template <>
struct TProtoTraits<NChunkServer::TChunkLocationPtrWithReplicaIndex>
{
    using TSerialized = ui64;
};

template <>
struct TProtoTraits<NChunkServer::TChunkLocationPtrWithReplicaInfo>
{
    using TSerialized = ui64;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
struct TSerializerTraits<NChunkServer::TPtrWithReplicaInfo<T>, C>
{
    struct TSerializer
    {
        static void Save(C& context, const NChunkServer::TPtrWithReplicaInfo<T>& replica);
        static void Load(C& context, NChunkServer::TPtrWithReplicaInfo<T>& replica);
    };

    struct TComparer
    {
        static bool Compare(
            const NChunkServer::TPtrWithReplicaInfo<T>& lhs,
            const NChunkServer::TPtrWithReplicaInfo<T>& rhs);
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CHUNK_REPLICA_INL_H_
#include "chunk_replica-inl.h"
#undef CHUNK_REPLICA_INL_H_
