#pragma once

#include "public.h"

#include <core/misc/small_vector.h>

#include <server/node_tracker_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

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

    bool operator == (TPtrWithIndex other) const;
    bool operator != (TPtrWithIndex other) const;

    bool operator <  (TPtrWithIndex other) const;
    bool operator <= (TPtrWithIndex other) const;
    bool operator >  (TPtrWithIndex other) const;
    bool operator >= (TPtrWithIndex other) const;

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

typedef TPtrWithIndex<NNodeTrackerServer::TNode> TNodePtrWithIndex;
typedef SmallVector<TNodePtrWithIndex, TypicalReplicaCount> TNodePtrWithIndexList;

typedef TPtrWithIndex<TChunk> TChunkPtrWithIndex;

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(TNodePtrWithIndex value);
Stroka ToString(TChunkPtrWithIndex value);

void ToProto(ui32* protoValue, TNodePtrWithIndex value);

TChunkId EncodeChunkId(TChunkPtrWithIndex chunkWithIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

#define CHUNK_REPLICA_INL_H_
#include "chunk_replica-inl.h"
#undef CHUNK_REPLICA_INL_H_
