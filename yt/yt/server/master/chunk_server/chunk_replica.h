#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A compact representation for |(T*, index1, index2, replica_state)|.
template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
class TAugmentedPtr
    : public TAugmentationAccessor<TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>>
{
private:
    static_assert(1 <= IndexCount && IndexCount <= 2);
    friend class TAugmentationAccessor<TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>>;
    friend class TAugmentedPtr<T, !WithReplicaState, IndexCount, TAugmentationAccessor>;

public:
    TAugmentedPtr();

    TAugmentedPtr(T* ptr, int index)
        requires (!WithReplicaState && IndexCount == 1);

    TAugmentedPtr(T* ptr, int index, EChunkReplicaState replicaState = EChunkReplicaState::Generic)
        requires (WithReplicaState && IndexCount == 1);

    TAugmentedPtr(T* ptr, int firstIndex, int secondIndex)
        requires (!WithReplicaState && IndexCount == 2);

    TAugmentedPtr(T* ptr, int firstIndex, int secondIndex, EChunkReplicaState = EChunkReplicaState::Generic)
        requires (WithReplicaState && IndexCount == 2);

    explicit TAugmentedPtr(TAugmentedPtr<T, true, IndexCount, TAugmentationAccessor> other)
        requires (!WithReplicaState);

    explicit TAugmentedPtr(TAugmentedPtr<T, false, IndexCount, TAugmentationAccessor> other)
        requires WithReplicaState;

    TAugmentedPtr(
        TAugmentedPtr<T, false, IndexCount, TAugmentationAccessor> other,
        EChunkReplicaState state)
        requires WithReplicaState;


    T* GetPtr() const;

    size_t GetHash() const;

    bool operator==(TAugmentedPtr other) const;
    bool operator!=(TAugmentedPtr other) const;

    bool operator< (TAugmentedPtr other) const;
    bool operator<=(TAugmentedPtr other) const;
    bool operator> (TAugmentedPtr other) const;
    bool operator>=(TAugmentedPtr other) const;

    template <class C>
    void Save(C& context) const;
    template <class C>
    void Load(C& context);

    TAugmentedPtr ToGenericState() const
        requires WithReplicaState;

    EChunkReplicaState GetReplicaState() const
        requires WithReplicaState;

private:
    static_assert(sizeof (uintptr_t) == 8, "Pointer type must be of size 8.");

    // Use compact 8-byte representation with index occupying the highest 8 bits.
    uintptr_t Value_;

    template <int Index>
    int GetIndex() const
        requires (Index <= IndexCount);
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

// COMPAT(kvk1920): Remove when reign will be greater than ChunkLocationsInReplica.
//! A compact representation for:
//! * a pointer to T
//! * replica index (5 bits)
//! * medium index (7 bits)
//! * replica state (2 bits)
//! - all fit into a single 8-byte pointer.
template <class T>
class TCompatPtrWithIndexes
{
public:
    TCompatPtrWithIndexes();
    TCompatPtrWithIndexes(
        T* ptr,
        int replicaIndex,
        int mediumIndex,
        EChunkReplicaState state = EChunkReplicaState::Generic);

    explicit operator bool() const;

    T* GetPtr() const;
    int GetReplicaIndex() const;
    int GetMediumIndex() const;
    EChunkReplicaState GetState() const;

    TCompatPtrWithIndexes<T> ToGenericState() const;

    size_t GetHash() const;

    bool operator == (TCompatPtrWithIndexes other) const;
    bool operator != (TCompatPtrWithIndexes other) const;

    bool operator <  (TCompatPtrWithIndexes other) const;
    bool operator <= (TCompatPtrWithIndexes other) const;
    bool operator >  (TCompatPtrWithIndexes other) const;
    bool operator >= (TCompatPtrWithIndexes other) const;

    template <class C>
    void Save(C& context) const;
    template <class C>
    void Load(C& context);

private:
    static_assert(sizeof (uintptr_t) == 8, "Pointer type must be of size 8.");

    // Use compact 8-byte representation with indexes occupying the highest 12 bits.
    uintptr_t Value_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
using TPtrWithMediumIndex = TAugmentedPtr<
    T,
    /*WithReplicaState*/ false,
    /*IndexCount*/ 1,
    NDetail::TAugmentedPtrMediumIndexAccessor>;

template <class T>
using TPtrWithReplicaIndex = TAugmentedPtr<
    T,
    /*WithReplicaState*/ false,
    /*IndexCount*/ 1,
    NDetail::TAugmentedPtrReplicaIndexAccessor>;

template <class T>
using TPtrWithReplicaInfo = TAugmentedPtr<
    T,
    /*WithReplicaState*/ true,
    /*IndexCount*/ 1,
    NDetail::TAugmentedPtrReplicaIndexAccessor>;

template <class T>
using TPtrWithReplicaAndMediumIndex = TAugmentedPtr<
    T,
    /*WithReplicaState*/ false,
    /*IndexCount*/ 2,
    NDetail::TAugmentedPtrReplicaAndMediumIndexAccessor>;

template <class T>
using TPtrWithReplicaInfoAndMediumIndex = TAugmentedPtr<
    T,
    /*WithReplicaState*/ true,
    /*IndexCount*/ 2,
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
using TNodePtrWithMediumIndex = TPtrWithMediumIndex<NNodeTrackerServer::TNode>;
using TNodePtrWithMediumIndexList = TCompactVector<TNodePtrWithMediumIndex, TypicalReplicaCount>;

using TChunkLocationPtrWithReplicaIndex = TPtrWithReplicaIndex<TChunkLocation>;
using TChunkLocationPtrWithReplicaIndexList = TCompactVector<TChunkLocationPtrWithReplicaIndex, TypicalReplicaCount>;
using TChunkLocationPtrWithReplicaInfo = TPtrWithReplicaInfo<TChunkLocation>;
using TChunkLocationPtrWithReplicaInfoList = TCompactVector<TChunkLocationPtrWithReplicaInfo, TypicalReplicaCount>;
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
TString ToString(TChunkPtrWithReplicaIndex value);

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithReplicaInfo value, TStringBuf spec);
TString ToString(TChunkPtrWithReplicaInfo value);

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithReplicaAndMediumIndex value, TStringBuf spec);
TString ToString(TChunkPtrWithReplicaAndMediumIndex value);

void FormatValue(TStringBuilderBase* builder, TChunkLocationPtrWithReplicaIndex value, TStringBuf spec);
TString ToString(TChunkLocationPtrWithReplicaIndex value);

void FormatValue(TStringBuilderBase* builder, TChunkLocationPtrWithReplicaInfo value, TStringBuf spec);
TString ToString(TChunkLocationPtrWithReplicaInfo value);

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
    NChunkClient::TChunkId ChunkId;
    int ReplicaIndex;
    NNodeTrackerClient::TNodeId NodeId;
    NChunkClient::TChunkLocationUuid LocationUuid;

    bool operator==(TSequoiaChunkReplica other) const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

void FromProto(TSequoiaChunkReplica* replica, const NProto::TSequoiaReplicaInfo& protoReplica);
void ToProto(NProto::TSequoiaReplicaInfo* protoReplica, const TSequoiaChunkReplica& replica);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

namespace NYT {

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
