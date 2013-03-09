#pragma once

#include "public.h"

#include <ytlib/misc/small_vector.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A compact representation of |(node, index)| pair.
class TChunkReplica
{
public:
    TChunkReplica();
    explicit TChunkReplica(TDataNode* node, int index = 0);

    TDataNode* GetNode() const;
    int GetIndex() const;

    size_t GetHash() const;

    bool operator == (TChunkReplica other) const;
    bool operator != (TChunkReplica other) const;

    bool operator <  (TChunkReplica other) const;
    bool operator <= (TChunkReplica other) const;
    bool operator >  (TChunkReplica other) const;
    bool operator >= (TChunkReplica other) const;

private:
#ifdef __x86_64__
    static_assert(sizeof (void*) == 8, "Pointer type must be of size 8.");
    // Use compact 8-byte representation with index occupying the highest 4 bits.
    ui64 Value;
#else
    // Use simple unpacked representation.
    TDataNode* Node;
    int Index;
#endif

};

typedef TSmallVector<TChunkReplica, TypicalReplicationFactor> TChunkReplicaList;

Stroka ToString(TChunkReplica replica);

void ToProto(ui32* value, TChunkReplica replica);

// TODO(babenko): eliminate this hack when new serialization API is ready
void SaveObjectRef(const NCellMaster::TSaveContext& context, TChunkReplica value);
void LoadObjectRef(const NCellMaster::TLoadContext& context, TChunkReplica& value);

bool CompareObjectsForSerialization(TChunkReplica lhs, TChunkReplica rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

//! A hasher for TChunkReplica.
template <>
struct hash<NYT::NChunkServer::TChunkReplica>
{
    size_t operator()(NYT::NChunkServer::TChunkReplica value) const
    {
        return value.GetHash();
    }
};

////////////////////////////////////////////////////////////////////////////////
