#pragma once

#include "public.h"

#include <ytlib/misc/small_vector.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A compact representation for |(T*, index)| pair.
template <class T>
class TWithIndex
{
public:
    TWithIndex();
    explicit TWithIndex(TDataNode* node, int index = 0);

    T* GetPtr() const;
    int GetIndex() const;

    size_t GetHash() const;

    bool operator == (TWithIndex other) const;
    bool operator != (TWithIndex other) const;

    bool operator <  (TWithIndex other) const;
    bool operator <= (TWithIndex other) const;
    bool operator >  (TWithIndex other) const;
    bool operator >= (TWithIndex other) const;

private:
#ifdef __x86_64__
    static_assert(sizeof (void*) == 8, "Pointer type must be of size 8.");
    // Use compact 8-byte representation with index occupying the highest 4 bits.
    ui64 Value;
#else
    // Use simple unpacked representation.
    T* Ptr;
    int Index;
#endif

};

////////////////////////////////////////////////////////////////////////////////

typedef TWithIndex<TDataNode> TDataNodeWithIndex;
typedef TWithIndex<TChunk> TChunkWithIndex;
typedef TSmallVector<TDataNodeWithIndex, TypicalReplicationFactor> TDataNodeWithIndexList;

Stroka ToString(TDataNodeWithIndex value);
Stroka ToString(TChunkWithIndex value);

void ToProto(ui32* protoValue, TDataNodeWithIndex value);

// TODO(babenko): eliminate this hack when new serialization API is ready
template <class T>
void SaveObjectRef(const NCellMaster::TSaveContext& context, TWithIndex<T> value);
template <class T>
void LoadObjectRef(const NCellMaster::TLoadContext& context, TWithIndex<T>& value);

template <class T>
bool CompareObjectsForSerialization(TWithIndex<T> lhs, TWithIndex<T> rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

#define CHUNK_REPLICA_INL_H_
#include "chunk_replica-inl.h"
#undef CHUNK_REPLICA_INL_H_