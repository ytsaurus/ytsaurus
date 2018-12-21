#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server/public.h>

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

//! A compact representation for a triplet of T*, an 8-bit number and a 4-bit
//! number - all fit into a single 8-byte pointer.
template <class T>
class TPtrWithIndexes
{
public:
    TPtrWithIndexes();
    TPtrWithIndexes(T* ptr, int replicaIndex, int mediumIndex);

    T* GetPtr() const;
    int GetReplicaIndex() const;
    int GetMediumIndex() const;

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

    // Use compact 8-byte representation with indexes occupying the highest 8 bits.
    uintptr_t Value_;
};

////////////////////////////////////////////////////////////////////////////////

TString ToString(TNodePtrWithIndexes value);
TString ToString(TChunkPtrWithIndexes value);

void ToProto(ui32* protoValue, TNodePtrWithIndexes value);

TChunkId EncodeChunkId(TChunkPtrWithIndexes chunkWithIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

////////////////////////////////////////////////////////////////////////////////

#define CHUNK_REPLICA_INL_H_
#include "chunk_replica-inl.h"
#undef CHUNK_REPLICA_INL_H_
