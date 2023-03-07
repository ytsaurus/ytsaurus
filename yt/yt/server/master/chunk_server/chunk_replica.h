#pragma once

#include "public.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A compact representation for |(T*, index)| pair.
template <class T>
class TPtrWithIndex
{
public:
    TPtrWithIndex();
    TPtrWithIndex(T* ptr, int index);

    T* GetPtr() const;
    int GetIndex() const;

    size_t GetHash() const;

    bool operator==(TPtrWithIndex other) const;
    bool operator!=(TPtrWithIndex other) const;

    bool operator< (TPtrWithIndex other) const;
    bool operator<=(TPtrWithIndex other) const;
    bool operator> (TPtrWithIndex other) const;
    bool operator>=(TPtrWithIndex other) const;

    template <class C>
    void Save(C& context) const;
    template <class C>
    void Load(C& context);

private:
    static_assert(sizeof (uintptr_t) == 8, "Pointer type must be of size 8.");

    // Use compact 8-byte representation with index occupying the highest 8 bits.
    uintptr_t Value_;
};

////////////////////////////////////////////////////////////////////////////////

//! A compact representation for:
//! * a pointer to T
//! * replica index (5 bits)
//! * medium index (7 bits)
//! * replica state (2 bits)
//! - all fit into a single 8-byte pointer.
template <class T>
class TPtrWithIndexes
{
public:
    TPtrWithIndexes();
    TPtrWithIndexes(
        T* ptr,
        int replicaIndex,
        int mediumIndex,
        EChunkReplicaState state = EChunkReplicaState::Generic);

    T* GetPtr() const;
    int GetReplicaIndex() const;
    int GetMediumIndex() const;
    EChunkReplicaState GetState() const;

    TPtrWithIndexes<T> ToGenericState() const;

    size_t GetHash() const;

    bool operator == (TPtrWithIndexes other) const;
    bool operator != (TPtrWithIndexes other) const;

    bool operator <  (TPtrWithIndexes other) const;
    bool operator <= (TPtrWithIndexes other) const;
    bool operator >  (TPtrWithIndexes other) const;
    bool operator >= (TPtrWithIndexes other) const;

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

void FormatValue(TStringBuilderBase* builder, TNodePtrWithIndexes value, TStringBuf spec);
TString ToString(TNodePtrWithIndexes value);

void FormatValue(TStringBuilderBase* builder, TChunkPtrWithIndexes value, TStringBuf spec);
TString ToString(TChunkPtrWithIndexes value);

//! Serializes node id, replica index, medium index.
void ToProto(ui64* protoValue, TNodePtrWithIndexes value);
//! Serializes node id, replica index.
void ToProto(ui32* protoValue, TNodePtrWithIndexes value);

TChunkId EncodeChunkId(TChunkPtrWithIndexes chunkWithIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

#define CHUNK_REPLICA_INL_H_
#include "chunk_replica-inl.h"
#undef CHUNK_REPLICA_INL_H_
