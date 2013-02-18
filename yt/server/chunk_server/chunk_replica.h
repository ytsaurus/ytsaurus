#pragma once

#include "public.h"

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
#ifdef _linux_
    static_assert(sizeof (void*) == 8, "Pointer type must be of size 8.");
    // Use compact 8-byte representation with index occupying the highest 4 bits.
    ui64 Value;
#else
    // Use simple unpacked representation.
    TDataNode* Node;
    int Index;
#endif

};

// TODO(babenko): eliminate this hack when new serialization API is ready
void SaveObjectRef(TOutputStream* output, TChunkReplica value);
void LoadObjectRef(TInputStream* input, TChunkReplica& value, const NCellMaster::TLoadContext& context);
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
